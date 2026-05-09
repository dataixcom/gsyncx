package performance

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/dataixcom/gsyncx"
)

type PerformanceOptimizer struct {
	config *gsyncx.SyncConfig
	logger gsyncx.SyncLogger
	stats  *SyncStats
	mu     sync.Mutex
}

type SyncStats struct {
	TotalRead    int64 `json:"total_read"`
	TotalWritten int64 `json:"total_written"`
	TotalFailed  int64 `json:"total_failed"`
	BatchCount   int64 `json:"batch_count"`
	AvgBatchSize int64 `json:"avg_batch_size"`
	MinBatchTime int64 `json:"min_batch_time_ms"`
	MaxBatchTime int64 `json:"max_batch_time_ms"`
	AvgBatchTime int64 `json:"avg_batch_time_ms"`
}

func NewPerformanceOptimizer(config *gsyncx.SyncConfig, logger gsyncx.SyncLogger) *PerformanceOptimizer {
	return &PerformanceOptimizer{
		config: config,
		logger: gsyncx.ResolveLogger(logger),
		stats:  &SyncStats{},
	}
}

func (o *PerformanceOptimizer) OptimizeBatchSize(currentBatchSize int, avgProcessingTimeMs int64) int {
	if avgProcessingTimeMs == 0 {
		return currentBatchSize
	}

	targetTimeMs := int64(1000)

	if avgProcessingTimeMs < targetTimeMs/2 {
		newBatchSize := int(float64(currentBatchSize) * 1.5)
		maxBatchSize := 10000
		if newBatchSize > maxBatchSize {
			newBatchSize = maxBatchSize
		}
		return newBatchSize
	}

	if avgProcessingTimeMs > targetTimeMs*2 {
		newBatchSize := int(float64(currentBatchSize) * 0.7)
		minBatchSize := 100
		if newBatchSize < minBatchSize {
			newBatchSize = minBatchSize
		}
		return newBatchSize
	}

	return currentBatchSize
}

func (o *PerformanceOptimizer) RecordBatch(count int, processingTimeMs int64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	atomic.AddInt64(&o.stats.TotalRead, int64(count))
	atomic.AddInt64(&o.stats.BatchCount, 1)

	if o.stats.MinBatchTime == 0 || processingTimeMs < o.stats.MinBatchTime {
		o.stats.MinBatchTime = processingTimeMs
	}
	if processingTimeMs > o.stats.MaxBatchTime {
		o.stats.MaxBatchTime = processingTimeMs
	}

	totalBatches := atomic.LoadInt64(&o.stats.BatchCount)
	o.stats.AvgBatchTime = (o.stats.AvgBatchTime*(totalBatches-1) + processingTimeMs) / totalBatches
	o.stats.AvgBatchSize = atomic.LoadInt64(&o.stats.TotalRead) / totalBatches
}

func (o *PerformanceOptimizer) GetStats() *SyncStats {
	o.mu.Lock()
	defer o.mu.Unlock()

	stats := *o.stats
	return &stats
}

func (o *PerformanceOptimizer) CalculateOptimalParallelism(totalRecords int64) int {
	if totalRecords <= 0 {
		return 1
	}

	batchSize := int64(1000)
	if o.config != nil && o.config.BatchSize > 0 {
		batchSize = int64(o.config.BatchSize)
	}

	parallelism := int(math.Ceil(float64(totalRecords) / float64(batchSize)))

	maxParallelism := 16
	if parallelism > maxParallelism {
		parallelism = maxParallelism
	}

	return parallelism
}

type ShardManager struct {
	shards    map[int]*Shard
	mu        sync.RWMutex
	nextShard int
}

type Shard struct {
	ID       int                   `json:"id"`
	MinValue interface{}           `json:"min_value"`
	MaxValue interface{}           `json:"max_value"`
	Status   gsyncx.SyncStatus    `json:"status"`
	Progress *gsyncx.SyncProgress `json:"progress,omitempty"`
}

func NewShardManager() *ShardManager {
	return &ShardManager{
		shards: make(map[int]*Shard),
	}
}

func (m *ShardManager) CreateShards(splitKeys []gsyncx.SplitKey) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, sk := range splitKeys {
		m.shards[i] = &Shard{
			ID:       i,
			MinValue: sk.MinValue,
			MaxValue: sk.MaxValue,
			Status:   gsyncx.StatusPending,
		}
	}

	return nil
}

func (m *ShardManager) GetNextShard() (*Shard, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, shard := range m.shards {
		if shard.Status == gsyncx.StatusPending {
			shard.Status = gsyncx.StatusRunning
			return shard, true
		}
	}

	return nil, false
}

func (m *ShardManager) MarkShardComplete(shardID int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	shard, ok := m.shards[shardID]
	if !ok {
		return fmt.Errorf("shard %d not found", shardID)
	}

	shard.Status = gsyncx.StatusCompleted
	return nil
}

func (m *ShardManager) MarkShardFailed(shardID int, _ error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	shard, ok := m.shards[shardID]
	if !ok {
		return fmt.Errorf("shard %d not found", shardID)
	}

	shard.Status = gsyncx.StatusFailed
	return nil
}

func (m *ShardManager) GetShardCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.shards)
}

func (m *ShardManager) GetPendingCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, shard := range m.shards {
		if shard.Status == gsyncx.StatusPending {
			count++
		}
	}
	return count
}

func (m *ShardManager) GetCompletedCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, shard := range m.shards {
		if shard.Status == gsyncx.StatusCompleted {
			count++
		}
	}
	return count
}

func (m *ShardManager) GetAllShards() []*Shard {
	m.mu.RLock()
	defer m.mu.RUnlock()

	shards := make([]*Shard, 0, len(m.shards))
	for _, shard := range m.shards {
		shards = append(shards, shard)
	}
	return shards
}
