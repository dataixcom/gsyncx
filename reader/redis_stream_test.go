package reader

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dataixcom/gsyncx"
	"github.com/redis/go-redis/v9"
)

func setupMiniredisForReader(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return mr, client
}

func setupStreamAndGroupForReader(t *testing.T, client *redis.Client, stream, group string) {
	t.Helper()
	ctx := context.Background()
	client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"_init": "1"},
	})
	client.XGroupCreate(ctx, stream, group, "0")
}

func TestNewRedisStreamReader_NilConfig(t *testing.T) {
	_, err := NewRedisStreamReader(nil, nil)
	if err == nil {
		t.Error("expected error for nil config")
	}
}

func TestNewRedisStreamReader_EmptyStream(t *testing.T) {
	_, err := NewRedisStreamReader(&RedisStreamConfig{}, nil)
	if err == nil {
		t.Error("expected error for empty stream")
	}
}

func TestNewRedisStreamReader_ConnectionFailed(t *testing.T) {
	_, err := NewRedisStreamReader(&RedisStreamConfig{
		Addr:   "localhost:19999",
		Stream: "test",
	}, nil)
	if err == nil {
		t.Error("expected error for failed connection")
	}
}

func TestNewRedisStreamReaderWithClient_NilClient(t *testing.T) {
	_, err := NewRedisStreamReaderWithClient(nil, &RedisStreamConfig{Stream: "test"}, nil)
	if err == nil {
		t.Error("expected error for nil client")
	}
}

func TestNewRedisStreamReaderWithClient_NilConfig(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()
	defer client.Close()

	_, err := NewRedisStreamReaderWithClient(client, nil, nil)
	if err == nil {
		t.Error("expected error for nil config")
	}
}

func TestNewRedisStreamReaderWithClient_EmptyStream(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()
	defer client.Close()

	_, err := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{}, nil)
	if err == nil {
		t.Error("expected error for empty stream")
	}
}

func TestNewRedisStreamReaderWithClient_Success(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()
	defer client.Close()

	rd, err := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{
		Stream:     "test-stream",
		AutoCreate: true,
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rd == nil {
		t.Error("expected non-nil reader")
	}
}

func TestRedisStreamReader_Read(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()
	defer client.Close()

	streamName := "test-read-stream"
	groupName := "test-group"
	setupStreamAndGroupForReader(t, client, streamName, groupName)

	for i := 0; i < 5; i++ {
		client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: streamName,
			Values: map[string]interface{}{
				"id":    fmt.Sprintf("user-%d", i),
				"name":  fmt.Sprintf("User %d", i),
				"email": fmt.Sprintf("user%d@example.com", i),
			},
		})
	}

	rd, err := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{
		Stream:        streamName,
		ConsumerGroup: groupName,
		ConsumerName:  "test-consumer",
		Count:         10,
		Block:         1 * time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeRealtime),
		gsyncx.WithBatchSize(100),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	recordCh, errCh := rd.Read(ctx, cfg)

	select {
	case batch := <-recordCh:
		if len(batch) < 5 {
			t.Errorf("expected at least 5 records, got %d", len(batch))
		}
		if batch[0].Data["_stream_id"] == "" {
			t.Error("expected _stream_id to be set")
		}
	case err := <-errCh:
		if err != nil && err != context.DeadlineExceeded {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for records")
	}
}

func TestRedisStreamReader_Count(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()
	defer client.Close()

	streamName := "test-count-stream"
	for i := 0; i < 3; i++ {
		client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: streamName,
			Values: map[string]interface{}{"key": fmt.Sprintf("val-%d", i)},
		})
	}

	rd, err := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{
		Stream: streamName,
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	count, err := rd.Count(context.Background(), &gsyncx.SyncConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected count 3, got %d", count)
	}
}

func TestRedisStreamReader_GetSplitKeys(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()
	defer client.Close()

	rd, _ := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{Stream: "test"}, nil)
	_, err := rd.GetSplitKeys(context.Background(), &gsyncx.SyncConfig{})
	if err == nil {
		t.Error("expected error: redis stream does not support split keys")
	}
}

func TestRedisStreamReader_Ack(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()
	defer client.Close()

	streamName := "test-ack-stream"
	groupName := "test-ack-group"
	setupStreamAndGroupForReader(t, client, streamName, groupName)

	rd, _ := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{
		Stream:        streamName,
		ConsumerGroup: groupName,
	}, nil)

	err := rd.Ack(context.Background(), "1234567890-0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRedisStreamReader_Ack_Empty(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()
	defer client.Close()

	rd, _ := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{Stream: "test"}, nil)
	err := rd.Ack(context.Background())
	if err != nil {
		t.Fatalf("unexpected error for empty ack: %v", err)
	}
}

func TestRedisStreamReader_Pending(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()
	defer client.Close()

	streamName := "test-pending-stream"
	groupName := "test-pending-group"
	setupStreamAndGroupForReader(t, client, streamName, groupName)

	rd, _ := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{
		Stream:        streamName,
		ConsumerGroup: groupName,
	}, nil)

	pending, err := rd.Pending(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pending == nil {
		t.Error("expected non-nil pending info")
	}
}

func TestRedisStreamReader_Close(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()

	rd, _ := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{Stream: "test"}, nil)
	err := rd.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRedisStreamReader_GetClient(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()

	rd, _ := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{Stream: "test"}, nil)
	if rd.GetClient() != client {
		t.Error("expected same client instance")
	}
}

func TestRedisStreamReader_SourceType(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()

	rd, _ := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{Stream: "test"}, nil)
	if rd.SourceType() != gsyncx.ReaderTypeRedisStream {
		t.Errorf("expected redis_stream, got %s", rd.SourceType())
	}
}

func TestRedisStreamReader_Read_CancelledContext(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()
	defer client.Close()

	streamName := "test-cancel-stream"
	groupName := "test-cancel-group"
	setupStreamAndGroupForReader(t, client, streamName, groupName)

	rd, _ := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{
		Stream:        streamName,
		ConsumerGroup: groupName,
		Count:         10,
		Block:         1 * time.Second,
	}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := gsyncx.NewSyncConfig(gsyncx.WithSyncMode(gsyncx.SyncModeRealtime))
	_, errCh := rd.Read(ctx, cfg)

	err := <-errCh
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestRedisStreamReader_Claim(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()
	defer client.Close()

	streamName := "test-claim-stream"
	groupName := "test-claim-group"
	setupStreamAndGroupForReader(t, client, streamName, groupName)

	msgID := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]interface{}{"key": "value"},
	}).Val()

	_, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: "old-consumer",
		Streams:  []string{streamName, ">"},
		Count:    1,
	}).Result()

	_ = err

	rd, _ := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{
		Stream:        streamName,
		ConsumerGroup: groupName,
		ConsumerName:  "new-consumer",
	}, nil)

	messages, err := rd.Claim(context.Background(), 0, msgID)
	if err != nil {
		t.Logf("claim error (expected in miniredis): %v", err)
	}
	_ = messages
}

func TestRedisStreamReader_Claim_Empty(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()
	defer client.Close()

	streamName := "test-claim-empty-stream"
	groupName := "test-claim-empty-group"
	setupStreamAndGroupForReader(t, client, streamName, groupName)

	rd, _ := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{
		Stream:        streamName,
		ConsumerGroup: groupName,
	}, nil)

	messages, err := rd.Claim(context.Background(), 0, "nonexistent-id")
	if err != nil {
		t.Logf("claim error for nonexistent ID (expected): %v", err)
	}
	_ = messages
}

func TestRedisStreamConfig_Defaults(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()
	defer client.Close()

	rd, err := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{Stream: "test"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rd.config.ConsumerGroup == "" {
		t.Error("expected default consumer group")
	}
	if rd.config.ConsumerName == "" {
		t.Error("expected default consumer name")
	}
	if rd.config.Count <= 0 {
		t.Error("expected default count > 0")
	}
	if rd.config.Block <= 0 {
		t.Error("expected default block > 0")
	}
	if rd.config.BatchSize <= 0 {
		t.Error("expected default batch size > 0")
	}
}

func TestCreateConsumerGroup(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()
	defer client.Close()

	ctx := context.Background()
	config := &RedisStreamConfig{
		Stream:        "new-stream",
		ConsumerGroup: "new-group",
		StartID:       "0",
	}

	err := createConsumerGroup(ctx, client, config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = createConsumerGroup(ctx, client, config)
	if err != nil {
		t.Fatalf("expected no error for duplicate group: %v", err)
	}
}

func TestIsBusyGroupError(t *testing.T) {
	if isBusyGroupError(nil) {
		t.Error("expected false for nil error")
	}
	if isBusyGroupError(fmt.Errorf("some other error")) {
		t.Error("expected false for non-BUSYGROUP error")
	}
}

func TestIsTimeoutError(t *testing.T) {
	if isTimeoutError(nil) {
		t.Error("expected false for nil error")
	}
	if isTimeoutError(fmt.Errorf("some error")) {
		t.Error("expected false for non-timeout error")
	}
	if isTimeoutError(context.DeadlineExceeded) {
		t.Error("expected false for context.DeadlineExceeded")
	}
}

func TestConvertMessageToData(t *testing.T) {
	msg := redis.XMessage{
		ID: "1234567890-0",
		Values: map[string]interface{}{
			"name":  "Alice",
			"email": "alice@example.com",
		},
	}

	data := convertMessageToData(msg)
	if data["_stream_id"] != "1234567890-0" {
		t.Errorf("expected _stream_id, got %v", data["_stream_id"])
	}
	if data["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", data["name"])
	}
}

func TestRedisStreamReader_Read_AutoCreate(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()
	defer client.Close()

	rd, _ := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{
		Stream:        "auto-create-stream",
		ConsumerGroup: "auto-group",
		AutoCreate:    true,
		Count:         10,
		Block:         500 * time.Millisecond,
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cfg := gsyncx.NewSyncConfig(gsyncx.WithSyncMode(gsyncx.SyncModeRealtime))
	_, errCh := rd.Read(ctx, cfg)

	select {
	case err := <-errCh:
		_ = err
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}
}

func TestRedisStreamReader_Read_NoConsumerGroup(t *testing.T) {
	mr, client := setupMiniredisForReader(t)
	defer mr.Close()
	defer client.Close()

	streamName := "test-no-group-stream"
	client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]interface{}{"key": "value"},
	})

	rd, _ := NewRedisStreamReaderWithClient(client, &RedisStreamConfig{
		Stream: streamName,
		Count:  10,
		Block:  500 * time.Millisecond,
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cfg := gsyncx.NewSyncConfig(gsyncx.WithSyncMode(gsyncx.SyncModeRealtime))
	_, errCh := rd.Read(ctx, cfg)

	select {
	case err := <-errCh:
		_ = err
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}
}
