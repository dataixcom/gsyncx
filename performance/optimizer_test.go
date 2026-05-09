package performance

import (
	"testing"

	"github.com/dataixcom/gsyncx"
)

func TestNewPerformanceOptimizer(t *testing.T) {
	cfg := gsyncx.NewSyncConfig()
	opt := NewPerformanceOptimizer(cfg, nil)
	if opt == nil {
		t.Error("expected non-nil optimizer")
	}
}

func TestPerformanceOptimizer_OptimizeBatchSize_Increase(t *testing.T) {
	opt := NewPerformanceOptimizer(nil, nil)
	result := opt.OptimizeBatchSize(1000, 200)
	if result <= 1000 {
		t.Errorf("expected batch size to increase when processing is fast, got %d", result)
	}
}

func TestPerformanceOptimizer_OptimizeBatchSize_Decrease(t *testing.T) {
	opt := NewPerformanceOptimizer(nil, nil)
	result := opt.OptimizeBatchSize(1000, 5000)
	if result >= 1000 {
		t.Errorf("expected batch size to decrease when processing is slow, got %d", result)
	}
}

func TestPerformanceOptimizer_OptimizeBatchSize_Stable(t *testing.T) {
	opt := NewPerformanceOptimizer(nil, nil)
	result := opt.OptimizeBatchSize(1000, 800)
	if result != 1000 {
		t.Errorf("expected batch size to remain stable, got %d", result)
	}
}

func TestPerformanceOptimizer_OptimizeBatchSize_ZeroTime(t *testing.T) {
	opt := NewPerformanceOptimizer(nil, nil)
	result := opt.OptimizeBatchSize(1000, 0)
	if result != 1000 {
		t.Errorf("expected batch size to remain unchanged when time is 0, got %d", result)
	}
}

func TestPerformanceOptimizer_RecordBatch(t *testing.T) {
	opt := NewPerformanceOptimizer(nil, nil)

	opt.RecordBatch(100, 50)
	opt.RecordBatch(200, 100)

	stats := opt.GetStats()
	if stats.TotalRead != 300 {
		t.Errorf("expected total read 300, got %d", stats.TotalRead)
	}
	if stats.BatchCount != 2 {
		t.Errorf("expected batch count 2, got %d", stats.BatchCount)
	}
	if stats.MinBatchTime != 50 {
		t.Errorf("expected min batch time 50, got %d", stats.MinBatchTime)
	}
	if stats.MaxBatchTime != 100 {
		t.Errorf("expected max batch time 100, got %d", stats.MaxBatchTime)
	}
}

func TestPerformanceOptimizer_CalculateOptimalParallelism(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(gsyncx.WithBatchSize(1000))
	opt := NewPerformanceOptimizer(cfg, nil)

	result := opt.CalculateOptimalParallelism(10000)
	if result < 1 {
		t.Errorf("expected at least 1 parallelism, got %d", result)
	}
}

func TestPerformanceOptimizer_CalculateOptimalParallelism_Zero(t *testing.T) {
	cfg := gsyncx.NewSyncConfig()
	opt := NewPerformanceOptimizer(cfg, nil)

	result := opt.CalculateOptimalParallelism(0)
	if result != 1 {
		t.Errorf("expected 1 parallelism for 0 records, got %d", result)
	}
}

func TestShardManager(t *testing.T) {
	mgr := NewShardManager()

	splitKeys := []gsyncx.SplitKey{
		{FieldName: "id", MinValue: 1, MaxValue: 100, TotalRows: 100},
		{FieldName: "id", MinValue: 101, MaxValue: 200, TotalRows: 100},
	}

	err := mgr.CreateShards(splitKeys)
	if err != nil {
		t.Fatalf("failed to create shards: %v", err)
	}

	if mgr.GetShardCount() != 2 {
		t.Errorf("expected 2 shards, got %d", mgr.GetShardCount())
	}
	if mgr.GetPendingCount() != 2 {
		t.Errorf("expected 2 pending, got %d", mgr.GetPendingCount())
	}

	shard, ok := mgr.GetNextShard()
	if !ok {
		t.Error("expected to get next shard")
	}
	if shard.Status != gsyncx.StatusRunning {
		t.Errorf("expected running status, got %s", shard.Status)
	}
	if mgr.GetPendingCount() != 1 {
		t.Errorf("expected 1 pending after getting one, got %d", mgr.GetPendingCount())
	}

	err = mgr.MarkShardComplete(shard.ID)
	if err != nil {
		t.Fatalf("failed to mark shard complete: %v", err)
	}
	if mgr.GetCompletedCount() != 1 {
		t.Errorf("expected 1 completed, got %d", mgr.GetCompletedCount())
	}
}

func TestShardManager_MarkShardFailed(t *testing.T) {
	mgr := NewShardManager()
	_ = mgr.CreateShards([]gsyncx.SplitKey{
		{FieldName: "id", MinValue: 1, MaxValue: 100, TotalRows: 100},
	})

	shard, _ := mgr.GetNextShard()
	err := mgr.MarkShardFailed(shard.ID, nil)
	if err != nil {
		t.Fatalf("failed to mark shard failed: %v", err)
	}

	shards := mgr.GetAllShards()
	if shards[0].Status != gsyncx.StatusFailed {
		t.Errorf("expected failed status, got %s", shards[0].Status)
	}
}

func TestShardManager_NoMoreShards(t *testing.T) {
	mgr := NewShardManager()
	_, ok := mgr.GetNextShard()
	if ok {
		t.Error("expected no shards available")
	}
}

func TestShardManager_ShardNotFound(t *testing.T) {
	mgr := NewShardManager()
	err := mgr.MarkShardComplete(999)
	if err == nil {
		t.Error("expected error for nonexistent shard")
	}

	err = mgr.MarkShardFailed(999, nil)
	if err == nil {
		t.Error("expected error for nonexistent shard")
	}
}
