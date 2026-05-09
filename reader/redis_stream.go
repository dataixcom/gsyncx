package reader

import (
	"context"
	"fmt"
	"time"

	"github.com/dataixcom/gsyncx"
	"github.com/redis/go-redis/v9"
)

type RedisStreamConfig struct {
	Addr          string        `json:"addr"`
	Password      string        `json:"password,omitempty"`
	DB            int           `json:"db,omitempty"`
	Stream        string        `json:"stream"`
	ConsumerGroup string        `json:"consumer_group,omitempty"`
	ConsumerName  string        `json:"consumer_name,omitempty"`
	Count         int64         `json:"count,omitempty"`
	Block         time.Duration `json:"block,omitempty"`
	BatchSize     int           `json:"batch_size,omitempty"`
	AutoCreate    bool          `json:"auto_create,omitempty"`
	StartID       string        `json:"start_id,omitempty"`
}

type RedisStreamReader struct {
	client *redis.Client
	config *RedisStreamConfig
	logger gsyncx.SyncLogger
}

func NewRedisStreamReader(config *RedisStreamConfig, logger gsyncx.SyncLogger) (*RedisStreamReader, error) {
	if config == nil {
		return nil, fmt.Errorf("redis stream config is required")
	}
	if config.Stream == "" {
		return nil, fmt.Errorf("stream name is required")
	}

	client := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	if config.ConsumerGroup == "" {
		config.ConsumerGroup = "gsyncx-consumer-group"
	}
	if config.ConsumerName == "" {
		config.ConsumerName = fmt.Sprintf("gsyncx-consumer-%d", time.Now().UnixNano())
	}
	if config.Count <= 0 {
		config.Count = 100
	}
	if config.Block <= 0 {
		config.Block = 5 * time.Second
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}

	if config.AutoCreate {
		if err := createConsumerGroup(ctx, client, config); err != nil {
			return nil, fmt.Errorf("failed to create consumer group: %w", err)
		}
	}

	return &RedisStreamReader{
		client: client,
		config: config,
		logger: gsyncx.ResolveLogger(logger),
	}, nil
}

func NewRedisStreamReaderWithClient(client *redis.Client, config *RedisStreamConfig, logger gsyncx.SyncLogger) (*RedisStreamReader, error) {
	if config == nil {
		return nil, fmt.Errorf("redis stream config is required")
	}
	if config.Stream == "" {
		return nil, fmt.Errorf("stream name is required")
	}
	if client == nil {
		return nil, fmt.Errorf("redis client is required")
	}

	if config.ConsumerGroup == "" {
		config.ConsumerGroup = "gsyncx-consumer-group"
	}
	if config.ConsumerName == "" {
		config.ConsumerName = fmt.Sprintf("gsyncx-consumer-%d", time.Now().UnixNano())
	}
	if config.Count <= 0 {
		config.Count = 100
	}
	if config.Block <= 0 {
		config.Block = 5 * time.Second
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}

	return &RedisStreamReader{
		client: client,
		config: config,
		logger: gsyncx.ResolveLogger(logger),
	}, nil
}

func createConsumerGroup(ctx context.Context, client *redis.Client, config *RedisStreamConfig) error {
	startID := config.StartID
	if startID == "" {
		startID = "0"
	}

	err := client.XGroupCreateMkStream(ctx, config.Stream, config.ConsumerGroup, startID).Err()
	if err != nil {
		if isBusyGroupError(err) {
			return nil
		}
		return fmt.Errorf("failed to create consumer group: %w", err)
	}
	return nil
}

func isBusyGroupError(err error) bool {
	return err != nil && (redis.HasErrorPrefix(err, "BUSYGROUP") ||
		fmt.Sprintf("%v", err) == "BUSYGROUP Consumer Group name already exists")
}

func (r *RedisStreamReader) Read(ctx context.Context, cfg *gsyncx.SyncConfig) (<-chan []gsyncx.Record, <-chan error) {
	recordCh := make(chan []gsyncx.Record, cfg.Parallelism)
	errCh := make(chan error, 1)

	go func() {
		defer close(recordCh)
		defer close(errCh)

		totalRead := int64(0)

		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
			}

			streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    r.config.ConsumerGroup,
				Consumer: r.config.ConsumerName,
				Streams:  []string{r.config.Stream, ">"},
				Count:    r.config.Count,
				Block:    r.config.Block,
			}).Result()

			if err != nil {
				if err == context.DeadlineExceeded || err == context.Canceled {
					errCh <- ctx.Err()
					return
				}
				if isTimeoutError(err) {
					continue
				}
				r.logger.Error("redis stream read error",
					gsyncx.F("error", err),
				)
				errCh <- fmt.Errorf("redis stream read failed: %w", err)
				return
			}

			if len(streams) == 0 || len(streams[0].Messages) == 0 {
				continue
			}

			records := make([]gsyncx.Record, 0, len(streams[0].Messages))
			var lastID string

			for _, msg := range streams[0].Messages {
				record := gsyncx.Record{
					Data: convertMessageToData(msg),
					Meta: gsyncx.RecordMeta{
						SourcePK: msg.ID,
						Stage:    gsyncx.StageRead,
					},
				}
				records = append(records, record)
				lastID = msg.ID
			}

			select {
			case recordCh <- records:
				totalRead += int64(len(records))
				r.logger.Debug("redis stream read batch",
					gsyncx.F("count", len(records)),
					gsyncx.F("total", totalRead),
					gsyncx.F("last_id", lastID),
				)
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()

	return recordCh, errCh
}

func (r *RedisStreamReader) Count(ctx context.Context, cfg *gsyncx.SyncConfig) (int64, error) {
	info, err := r.client.XInfoStream(ctx, r.config.Stream).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get stream info: %w", err)
	}
	return info.Length, nil
}

func (r *RedisStreamReader) GetSplitKeys(ctx context.Context, cfg *gsyncx.SyncConfig) ([]gsyncx.SplitKey, error) {
	return nil, fmt.Errorf("redis stream reader does not support split keys")
}

func (r *RedisStreamReader) Ack(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	n, err := r.client.XAck(ctx, r.config.Stream, r.config.ConsumerGroup, ids...).Result()
	if err != nil {
		return fmt.Errorf("failed to ack messages: %w", err)
	}
	r.logger.Debug("ack messages",
		gsyncx.F("requested", len(ids)),
		gsyncx.F("acked", n),
	)
	return nil
}

func (r *RedisStreamReader) Pending(ctx context.Context) (*redis.XPending, error) {
	return r.client.XPending(ctx, r.config.Stream, r.config.ConsumerGroup).Result()
}

func (r *RedisStreamReader) Claim(ctx context.Context, minIdleTime time.Duration, ids ...string) ([]redis.XMessage, error) {
	return r.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   r.config.Stream,
		Group:    r.config.ConsumerGroup,
		Consumer: r.config.ConsumerName,
		MinIdle:  minIdleTime,
		Messages: ids,
	}).Result()
}

func (r *RedisStreamReader) Close() error {
	return r.client.Close()
}

func (r *RedisStreamReader) SourceType() gsyncx.ReaderType {
	return gsyncx.ReaderTypeRedisStream
}

func (r *RedisStreamReader) GetClient() *redis.Client {
	return r.client
}

func convertMessageToData(msg redis.XMessage) map[string]interface{} {
	data := make(map[string]interface{})
	for k, v := range msg.Values {
		data[k] = v
	}
	data["_stream_id"] = msg.ID
	return data
}

func isTimeoutError(err error) bool {
	return err != nil && (redis.HasErrorPrefix(err, "TIMEOUT") ||
		fmt.Sprintf("%T", err) == "*net.OpError")
}
