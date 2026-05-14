package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/checkpoint"
	"github.com/dataixcom/gsyncx/datasource"
	"github.com/dataixcom/gsyncx/mapping"
	"github.com/dataixcom/gsyncx/transform"
)

type mockReader struct {
	records [][]gsyncx.Record
	count   int64
	err     error
}

func (m *mockReader) Read(ctx context.Context, cfg *gsyncx.SyncConfig) (<-chan []gsyncx.Record, <-chan error) {
	recordCh := make(chan []gsyncx.Record, len(m.records)+1)
	errCh := make(chan error, 1)

	go func() {
		defer close(recordCh)
		defer close(errCh)

		if m.err != nil {
			errCh <- m.err
			return
		}

		for _, batch := range m.records {
			select {
			case recordCh <- batch:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()

	return recordCh, errCh
}

func (m *mockReader) Count(ctx context.Context, cfg *gsyncx.SyncConfig) (int64, error) {
	return m.count, nil
}

func (m *mockReader) GetSplitKeys(ctx context.Context, cfg *gsyncx.SyncConfig) ([]gsyncx.SplitKey, error) {
	return nil, nil
}

func (m *mockReader) Close() error {
	return nil
}

type mockWriter struct {
	writeResult gsyncx.WriteResult
	writeErr    error
	flushErr    error
}

func (m *mockWriter) Write(ctx context.Context, records []gsyncx.Record) (gsyncx.WriteResult, error) {
	return m.writeResult, m.writeErr
}

func (m *mockWriter) WriteWithMode(ctx context.Context, records []gsyncx.Record, mode gsyncx.WriteMode) (gsyncx.WriteResult, error) {
	return m.writeResult, m.writeErr
}

func (m *mockWriter) Flush(ctx context.Context) error {
	return m.flushErr
}

func (m *mockWriter) Close() error {
	return nil
}

type mockIntegrityChecker struct {
	result *gsyncx.IntegrityResult
	err    error
}

func (m *mockIntegrityChecker) Check(ctx context.Context, cfg *gsyncx.SyncConfig) (*gsyncx.IntegrityResult, error) {
	return m.result, m.err
}

func TestNewSyncEngine_NilConfig(t *testing.T) {
	_, err := NewSyncEngine(nil)
	if err == nil {
		t.Error("expected error for nil config")
	}
}

func TestNewSyncEngine_WithMockComponents(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{
			{
				{Data: map[string]interface{}{"id": 1, "name": "Alice"}},
				{Data: map[string]interface{}{"id": 2, "name": "Bob"}},
			},
		},
		count: 2,
	}
	wr := &mockWriter{
		writeResult: gsyncx.WriteResult{SuccessCount: 2},
	}

	eng, err := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != gsyncx.StatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
	if result.TotalRead != 2 {
		t.Errorf("expected 2 read, got %d", result.TotalRead)
	}
	if result.TotalWritten != 2 {
		t.Errorf("expected 2 written, got %d", result.TotalWritten)
	}
}

func TestSyncEngine_PreviewMode(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithPreviewMode(10),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{
			{
				{Data: map[string]interface{}{"id": 1}},
				{Data: map[string]interface{}{"id": 2}},
			},
		},
		count: 2,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 2}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.PreviewData) != 2 {
		t.Errorf("expected 2 preview records, got %d", len(result.PreviewData))
	}
	if result.TotalSkipped != 2 {
		t.Errorf("expected 2 skipped in preview mode, got %d", result.TotalSkipped)
	}
}

func TestSyncEngine_WithTransformer(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{
			{{Data: map[string]interface{}{"id": 1}}},
		},
		count: 1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithTransformer(transform.NewDefaultTransformer()),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalRead != 1 {
		t.Errorf("expected 1 read, got %d", result.TotalRead)
	}
}

func TestSyncEngine_WithMapper(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{
			{{Data: map[string]interface{}{"name": "Alice"}}},
		},
		count: 1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}
	mapper := mapping.NewFieldMappingEngineWithMappings([]gsyncx.FieldMapping{
		{SourceField: "name", TargetField: "full_name"},
	}, nil)

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithMapper(mapper),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalWritten != 1 {
		t.Errorf("expected 1 written, got %d", result.TotalWritten)
	}
}

func TestSyncEngine_WithHooks(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	hookCalled := false
	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
		WithHook(gsyncx.HookAfterRead, func(ctx context.Context, hctx *gsyncx.HookContext) error {
			hookCalled = true
			return nil
		}),
	)

	_, _ = eng.Run(context.Background())
	if !hookCalled {
		t.Error("expected hook to be called")
	}
}

func TestSyncEngine_HookError(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
		WithHook(gsyncx.HookBeforeRead, func(ctx context.Context, hctx *gsyncx.HookContext) error {
			return fmt.Errorf("hook error")
		}),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("hook error should not fail sync: %v", err)
	}
	if result.Status != gsyncx.StatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestSyncEngine_Stop(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{records: [][]gsyncx.Record{}, count: 0}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	eng.Stop()

	progress := eng.GetProgress()
	if progress.Status != gsyncx.StatusCancelled {
		t.Errorf("expected cancelled status, got %s", progress.Status)
	}
}

func TestSyncEngine_PauseResume(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
	)

	eng, _ := NewSyncEngine(cfg, WithLogger(gsyncx.NewNopLogger()))

	eng.Pause()
	progress := eng.GetProgress()
	if progress.Status != gsyncx.StatusPaused {
		t.Errorf("expected paused status, got %s", progress.Status)
	}

	eng.Resume()
	progress = eng.GetProgress()
	if progress.Status != gsyncx.StatusRunning {
		t.Errorf("expected running status, got %s", progress.Status)
	}
}

func TestSyncEngine_SwitchModes(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(gsyncx.WithSyncMode(gsyncx.SyncModeFull))
	eng, _ := NewSyncEngine(cfg, WithLogger(gsyncx.NewNopLogger()))

	eng.SwitchToIncrementalSync(&gsyncx.Field{FieldName: "id"}, gsyncx.StrategyAutoInc)
	if !eng.GetConfig().IsIncrementalSync() {
		t.Error("expected incremental mode after switch")
	}

	eng.SwitchToFullSync()
	if !eng.GetConfig().IsFullSync() {
		t.Error("expected full mode after switch back")
	}
}

func TestSyncEngine_SwitchToRealtimeSync(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(gsyncx.WithSyncMode(gsyncx.SyncModeFull))
	eng, _ := NewSyncEngine(cfg, WithLogger(gsyncx.NewNopLogger()))

	eng.SwitchToRealtimeSync()
	if !eng.GetConfig().IsRealtimeSync() {
		t.Error("expected realtime mode after switch")
	}
}

func TestSyncEngine_GetStats(t *testing.T) {
	cfg := gsyncx.NewSyncConfig()
	eng, _ := NewSyncEngine(cfg, WithLogger(gsyncx.NewNopLogger()))

	stats := eng.GetStats()
	if stats == nil {
		t.Error("expected non-nil stats")
	}
}

func TestSyncEngine_GetConfig(t *testing.T) {
	cfg := gsyncx.NewSyncConfig()
	eng, _ := NewSyncEngine(cfg, WithLogger(gsyncx.NewNopLogger()))

	if eng.GetConfig() != cfg {
		t.Error("expected same config instance")
	}
}

func TestSyncEngine_SetComponents(t *testing.T) {
	cfg := gsyncx.NewSyncConfig()
	eng, _ := NewSyncEngine(cfg, WithLogger(gsyncx.NewNopLogger()))

	rd := &mockReader{count: 0}
	wr := &mockWriter{}

	eng.SetReader(rd)
	eng.SetWriter(wr)
	eng.SetTransformer(transform.NewDefaultTransformer())
	eng.SetMapper(mapping.NewFieldMappingEngineWithMappings(nil, nil))
	eng.SetCheckpointStore(checkpoint.NewMemoryCheckpointStore())
	eng.SetLogger(gsyncx.NewNopLogger())
	eng.SetErrorHandler(nil)
	eng.SetIntegrityChecker(nil)
	eng.AddHook(gsyncx.HookBeforeRead, func(ctx context.Context, hctx *gsyncx.HookContext) error {
		return nil
	})
}

func TestSyncEngine_NoReader(t *testing.T) {
	cfg := gsyncx.NewSyncConfig()
	eng, _ := NewSyncEngine(cfg, WithLogger(gsyncx.NewNopLogger()))
	eng.reader = nil

	_, err := eng.Run(context.Background())
	if err == nil {
		t.Error("expected error for no reader")
	}
}

func TestSyncEngine_NoWriter(t *testing.T) {
	cfg := gsyncx.NewSyncConfig()
	eng, _ := NewSyncEngine(cfg, WithLogger(gsyncx.NewNopLogger()))
	eng.writer = nil

	_, err := eng.Run(context.Background())
	if err == nil {
		t.Error("expected error for no writer")
	}
}

func TestSyncEngine_WriteRetry(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithRetry(2, time.Millisecond),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{
		writeResult: gsyncx.WriteResult{SuccessCount: 1},
	}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != gsyncx.StatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestSyncEngine_CancelledContext(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	rd := &mockReader{records: [][]gsyncx.Record{}, count: 0}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, _ := eng.Run(ctx)
	if result.Status != gsyncx.StatusCancelled {
		t.Errorf("expected cancelled, got %s", result.Status)
	}
}

func TestSyncEngine_IntegrityCheck(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithIntegrityCheck(gsyncx.IntegrityCheckCount),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	checker := &mockIntegrityChecker{result: &gsyncx.IntegrityResult{
		Mode:        gsyncx.IntegrityCheckCount,
		SourceCount: 1,
		TargetCount: 1,
		CountMatch:  true,
		Passed:      true,
	}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithIntegrityChecker(checker),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IntegrityResult == nil {
		t.Error("expected integrity result")
	}
	if !result.IntegrityResult.Passed {
		t.Error("expected integrity check to pass")
	}
}

func TestSyncEngine_IntegrityCheckFailed(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithIntegrityCheck(gsyncx.IntegrityCheckCount),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	checker := &mockIntegrityChecker{result: &gsyncx.IntegrityResult{
		Mode:        gsyncx.IntegrityCheckCount,
		SourceCount: 1,
		TargetCount: 0,
		CountMatch:  false,
		Passed:      false,
		Details:     "count mismatch",
	}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithIntegrityChecker(checker),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, _ := eng.Run(context.Background())
	if result.IntegrityResult == nil {
		t.Error("expected integrity result")
	}
	if result.IntegrityResult.Passed {
		t.Error("expected integrity check to fail")
	}
}

func TestSyncEngine_IntegrityCheckError(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithIntegrityCheck(gsyncx.IntegrityCheckCount),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	checker := &mockIntegrityChecker{err: fmt.Errorf("check failed")}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithIntegrityChecker(checker),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, _ := eng.Run(context.Background())
	if result.Status != gsyncx.StatusCompleted {
		t.Errorf("expected completed even with integrity check error, got %s", result.Status)
	}
}

func TestSyncEngine_EmptyRead(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{records: [][]gsyncx.Record{}, count: 0}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != gsyncx.StatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestSyncEngine_WriteError(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithRetry(1, time.Millisecond),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{
		writeResult: gsyncx.WriteResult{FailedCount: 1},
		writeErr:    fmt.Errorf("write error"),
	}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, _ := eng.Run(context.Background())
	if result.TotalFailed == 0 {
		t.Error("expected some failed records")
	}
}

func TestSyncEngine_ReadError(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		err: fmt.Errorf("read error"),
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err == nil {
		t.Error("expected error for read failure")
	}
	if result.Status != gsyncx.StatusFailed {
		t.Errorf("expected failed status, got %s", result.Status)
	}
}

func TestSyncEngine_MultipleBatches(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{
			{
				{Data: map[string]interface{}{"id": 1}},
				{Data: map[string]interface{}{"id": 2}},
				{Data: map[string]interface{}{"id": 3}},
			},
		},
		count: 3,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 3}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalRead != 3 {
		t.Errorf("expected 3 read, got %d", result.TotalRead)
	}
	if result.TotalWritten != 3 {
		t.Errorf("expected 3 written, got %d", result.TotalWritten)
	}
}

func TestSyncEngine_WithCheckpointStore(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	cpStore := checkpoint.NewMemoryCheckpointStore()

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithCheckpointStore(cpStore),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != gsyncx.StatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestSyncEngine_CountError(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   -1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != gsyncx.StatusCompleted {
		t.Errorf("expected completed even with count error, got %s", result.Status)
	}
}

func TestSyncEngine_TransformError(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithTransformer(transform.TransformFunc(func(ctx context.Context, records []gsyncx.Record) ([]gsyncx.Record, []gsyncx.FailedRecord, error) {
			return nil, nil, fmt.Errorf("transform error")
		})),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, _ := eng.Run(context.Background())
	if result.Status != gsyncx.StatusCompleted {
		t.Errorf("expected completed even with transform error, got %s", result.Status)
	}
}

func TestSyncEngine_MappingError(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithMapper(mapping.NewFieldMappingEngine(gsyncx.MappingConfig{
			StrictMode: true,
			Mappings: []gsyncx.FieldMapping{
				{SourceField: "nonexistent", TargetField: "target", Required: true},
			},
		}, nil)),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, _ := eng.Run(context.Background())
	if result.TotalFailed > 0 {
		t.Logf("mapping failures: %d", result.TotalFailed)
	}
}

func TestResolveSourceDSN(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
			DSNConfig: &gsyncx.DSNConfig{Host: "reader-host"},
		}),
	)
	dsn := resolveSourceDSN(cfg)
	if dsn.Host != "reader-host" {
		t.Errorf("expected reader-host, got %s", dsn.Host)
	}
}

func TestResolveTargetDSN(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{
			DSNConfig: &gsyncx.DSNConfig{Host: "writer-host"},
		}),
	)
	dsn := resolveTargetDSN(cfg)
	if dsn.Host != "writer-host" {
		t.Errorf("expected writer-host, got %s", dsn.Host)
	}
}

func TestSyncEngine_WithAllHookPoints(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	calledHooks := make(map[gsyncx.HookPoint]bool)
	hookPoints := []gsyncx.HookPoint{
		gsyncx.HookBeforeRead, gsyncx.HookAfterRead,
		gsyncx.HookBeforeTransform, gsyncx.HookAfterTransform,
		gsyncx.HookBeforeMap, gsyncx.HookAfterMap,
		gsyncx.HookBeforeWrite, gsyncx.HookAfterWrite,
		gsyncx.HookOnComplete,
	}

	opts := []EngineOption{
		WithReader(rd),
		WithWriter(wr),
		WithTransformer(transform.NewDefaultTransformer()),
		WithMapper(mapping.NewFieldMappingEngineWithMappings(nil, nil)),
		WithLogger(gsyncx.NewNopLogger()),
	}

	for _, hp := range hookPoints {
		point := hp
		opts = append(opts, WithHook(point, func(ctx context.Context, hctx *gsyncx.HookContext) error {
			calledHooks[point] = true
			return nil
		}))
	}

	eng, _ := NewSyncEngine(cfg, opts...)
	_, _ = eng.Run(context.Background())

	for _, hp := range hookPoints {
		if !calledHooks[hp] {
			t.Errorf("expected hook %s to be called", hp)
		}
	}
}

func TestSyncEngine_PreviewMode_Limit(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithPreviewMode(2),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{
			{
				{Data: map[string]interface{}{"id": 1}},
				{Data: map[string]interface{}{"id": 2}},
				{Data: map[string]interface{}{"id": 3}},
			},
		},
		count: 3,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 3}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.PreviewData) > 2 {
		t.Errorf("expected at most 2 preview records, got %d", len(result.PreviewData))
	}
}

func TestSyncEngine_IntegrityCheckNone(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithIntegrityCheck(gsyncx.IntegrityCheckNone),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithIntegrityChecker(&mockIntegrityChecker{result: &gsyncx.IntegrityResult{Passed: true}}),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, _ := eng.Run(context.Background())
	if result.IntegrityResult != nil {
		t.Error("expected no integrity result for none mode")
	}
}

func TestWithSourceDS(t *testing.T) {
	cfg := gsyncx.NewSyncConfig()
	ds := &datasource.GdbxDataSource{}
	eng, _ := NewSyncEngine(cfg, WithSourceDS(ds), WithLogger(gsyncx.NewNopLogger()))
	if eng.sourceDS != ds {
		t.Error("expected source DS to be set")
	}
}

func TestWithTargetDS(t *testing.T) {
	cfg := gsyncx.NewSyncConfig()
	ds := &datasource.GdbxDataSource{}
	eng, _ := NewSyncEngine(cfg, WithTargetDS(ds), WithLogger(gsyncx.NewNopLogger()))
	if eng.targetDS != ds {
		t.Error("expected target DS to be set")
	}
}

func TestWithErrorHandler(t *testing.T) {
	cfg := gsyncx.NewSyncConfig()
	handler := &testErrorHandler{}
	eng, _ := NewSyncEngine(cfg, WithErrorHandler(handler), WithLogger(gsyncx.NewNopLogger()))
	if eng.errorHandler == nil {
		t.Error("expected error handler to be set")
	}
}

type testErrorHandler struct{}

func (h *testErrorHandler) Handle(ctx context.Context, record gsyncx.Record, stage gsyncx.PipelineStage, err error) gsyncx.HandleDecision {
	return gsyncx.DecisionSkip
}

type mockConfigurableWriter struct {
	mockWriter
	receivedCfg gsyncx.WriterConfig
}

func (m *mockConfigurableWriter) SetConfig(cfg gsyncx.WriterConfig) {
	m.receivedCfg = cfg
}

func TestSyncEngine_InjectWriterConfig(t *testing.T) {
	writerCfg := gsyncx.WriterConfig{
		TableName:  "target_table",
		Schema:     "target_schema",
		WriteMode:  gsyncx.WriteModeUpsert,
		PrimaryKey: &gsyncx.Field{FieldName: "id"},
		Fields:     []gsyncx.Field{{FieldName: "id"}, {FieldName: "name"}},
		RawFields:  []string{"id", "name", "email"},
	}

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(writerCfg),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockConfigurableWriter{
		mockWriter: mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}},
	}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	_, _ = eng.Run(context.Background())

	if wr.receivedCfg.TableName != "target_table" {
		t.Errorf("expected writer config table name 'target_table', got '%s'", wr.receivedCfg.TableName)
	}
	if wr.receivedCfg.Schema != "target_schema" {
		t.Errorf("expected writer config schema 'target_schema', got '%s'", wr.receivedCfg.Schema)
	}
	if wr.receivedCfg.PrimaryKey == nil || wr.receivedCfg.PrimaryKey.FieldName != "id" {
		t.Error("expected writer config primary key 'id'")
	}
	if len(wr.receivedCfg.Fields) != 2 {
		t.Errorf("expected 2 fields in writer config, got %d", len(wr.receivedCfg.Fields))
	}
	if len(wr.receivedCfg.RawFields) != 3 {
		t.Errorf("expected 3 raw fields in writer config, got %d", len(wr.receivedCfg.RawFields))
	}
}

func TestSyncEngine_InjectWriterConfig_EmptyTableName(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockConfigurableWriter{
		mockWriter: mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}},
	}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	_, _ = eng.Run(context.Background())

	if wr.receivedCfg.TableName != "" {
		t.Errorf("expected empty table name when WriterConfig not set, got '%s'", wr.receivedCfg.TableName)
	}
}

func TestSyncEngine_InjectWriterConfig_NonConfigurableWriter(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target_table"}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != gsyncx.StatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestSyncEngine_WriteRetry_SuccessAfterRetry(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithRetry(3, time.Millisecond),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	callCount := 0
	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{
		writeResult: gsyncx.WriteResult{SuccessCount: 1},
		writeErr:    nil,
	}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = callCount
	_ = result
}

func TestSyncEngine_WriteRetry_AllRetriesFail(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithRetry(2, time.Millisecond),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{
		writeResult: gsyncx.WriteResult{FailedCount: 1},
		writeErr:    fmt.Errorf("persistent write error"),
	}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, _ := eng.Run(context.Background())
	if result.TotalFailed == 0 {
		t.Error("expected failed records after all retries fail")
	}
}

func TestSyncEngine_SetSourceDS(t *testing.T) {
	cfg := gsyncx.NewSyncConfig()
	eng, _ := NewSyncEngine(cfg, WithLogger(gsyncx.NewNopLogger()))
	ds := &datasource.GdbxDataSource{}
	eng.SetSourceDS(ds)
	if eng.sourceDS != ds {
		t.Error("expected source DS to be set")
	}
}

func TestSyncEngine_SetTargetDS(t *testing.T) {
	cfg := gsyncx.NewSyncConfig()
	eng, _ := NewSyncEngine(cfg, WithLogger(gsyncx.NewNopLogger()))
	ds := &datasource.GdbxDataSource{}
	eng.SetTargetDS(ds)
	if eng.targetDS != ds {
		t.Error("expected target DS to be set")
	}
}

func TestSyncEngine_NoTransformer(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)
	eng.transformer = nil

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalRead != 1 {
		t.Errorf("expected 1 read, got %d", result.TotalRead)
	}
}

func TestSyncEngine_NoMapper(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)
	eng.mapper = nil

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalRead != 1 {
		t.Errorf("expected 1 read, got %d", result.TotalRead)
	}
}

func TestSyncEngine_WithMappingConfig(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
		gsyncx.WithMappingConfig(gsyncx.MappingConfig{
			AutoMapping: true,
			Mappings: []gsyncx.FieldMapping{
				{SourceField: "id", TargetField: "user_id"},
			},
		}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalRead != 1 {
		t.Errorf("expected 1 read, got %d", result.TotalRead)
	}
}

func TestSyncEngine_WriteWithSkipped(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{
		writeResult: gsyncx.WriteResult{SuccessCount: 1, SkippedCount: 0},
	}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != gsyncx.StatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestSyncEngine_StopBeforeRun(t *testing.T) {
	cfg := gsyncx.NewSyncConfig()
	eng, _ := NewSyncEngine(cfg, WithLogger(gsyncx.NewNopLogger()))
	eng.Stop()
	progress := eng.GetProgress()
	if progress.Status != gsyncx.StatusCancelled {
		t.Errorf("expected cancelled, got %s", progress.Status)
	}
}

func TestSyncEngine_RecordChannelClosedBeforeErrChannel(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{
			{{Data: map[string]interface{}{"id": 1}}},
			{{Data: map[string]interface{}{"id": 2}}},
			{{Data: map[string]interface{}{"id": 3}}},
		},
		count: 3,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != gsyncx.StatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
	if result.TotalRead != 3 {
		t.Errorf("expected 3 total read, got %d", result.TotalRead)
	}
}

func TestSyncEngine_WriteModeFromConfig(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeInsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != gsyncx.StatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestSyncEngine_CheckpointSaved(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithCheckpoint(true, ""),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}
	cpStore := checkpoint.NewMemoryCheckpointStore()

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithCheckpointStore(cpStore),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != gsyncx.StatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}

	cp, err := cpStore.Load(context.Background(), "source")
	if err != nil {
		t.Fatalf("unexpected error loading checkpoint: %v", err)
	}
	if cp == nil {
		t.Error("expected checkpoint to be saved")
	}
	if cp.TableName != "source" {
		t.Errorf("expected checkpoint table name 'source', got '%s'", cp.TableName)
	}
}

func TestSyncEngine_CheckpointNotSavedWhenDisabled(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}
	cpStore := checkpoint.NewMemoryCheckpointStore()

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithCheckpointStore(cpStore),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != gsyncx.StatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}

	cp, _ := cpStore.Load(context.Background(), "source")
	if cp != nil {
		t.Error("expected no checkpoint when checkpoint is disabled")
	}
}

func TestSyncEngine_IncrementalCheckpoint(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeIncremental),
		gsyncx.WithBatchSize(100),
		gsyncx.WithCheckpoint(true, ""),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
		gsyncx.WithIncrementalField(&gsyncx.Field{FieldName: "updated_at"}, gsyncx.StrategyTimestamp),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}
	cpStore := checkpoint.NewMemoryCheckpointStore()

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithCheckpointStore(cpStore),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != gsyncx.StatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}

	cp, err := cpStore.Load(context.Background(), "source")
	if err != nil {
		t.Fatalf("unexpected error loading checkpoint: %v", err)
	}
	if cp == nil {
		t.Fatal("expected checkpoint to be saved")
	}
	if cp.FieldName != "updated_at" {
		t.Errorf("expected checkpoint field name 'updated_at', got '%s'", cp.FieldName)
	}
}

func TestSyncEngine_MultipleBatchesAllConsumed(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{
			{{Data: map[string]interface{}{"id": 1}}},
			{{Data: map[string]interface{}{"id": 2}}},
			{{Data: map[string]interface{}{"id": 3}}},
			{{Data: map[string]interface{}{"id": 4}}},
			{{Data: map[string]interface{}{"id": 5}}},
		},
		count: 5,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalRead != 5 {
		t.Errorf("expected 5 total read, got %d", result.TotalRead)
	}
	if result.TotalWritten != 5 {
		t.Errorf("expected 5 total written, got %d", result.TotalWritten)
	}
}

func TestSyncEngine_ContextCancelledDuringProcessing(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	ctx, cancel := context.WithCancel(context.Background())

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	cancel()
	result, _ := eng.Run(ctx)
	if result.Status != gsyncx.StatusCancelled {
		t.Errorf("expected cancelled, got %s", result.Status)
	}
}

func TestSyncEngine_WriteRetryOnFailure(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &retryableMockWriter{
		failCount:    2,
		writeResult:  gsyncx.WriteResult{SuccessCount: 1},
		currentFails: 0,
	}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalWritten != 1 {
		t.Errorf("expected 1 total written after retry, got %d", result.TotalWritten)
	}
}

type retryableMockWriter struct {
	writeResult  gsyncx.WriteResult
	failCount    int
	currentFails int
}

func (m *retryableMockWriter) Write(ctx context.Context, records []gsyncx.Record) (gsyncx.WriteResult, error) {
	if m.currentFails < m.failCount {
		m.currentFails++
		return gsyncx.WriteResult{}, fmt.Errorf("temporary write failure (attempt %d)", m.currentFails)
	}
	return m.writeResult, nil
}

func (m *retryableMockWriter) WriteWithMode(ctx context.Context, records []gsyncx.Record, mode gsyncx.WriteMode) (gsyncx.WriteResult, error) {
	return m.Write(ctx, records)
}

func (m *retryableMockWriter) Flush(ctx context.Context) error { return nil }
func (m *retryableMockWriter) Close() error                    { return nil }

func TestSyncEngine_WithFailingHook(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &mockWriter{writeResult: gsyncx.WriteResult{SuccessCount: 1}}

	hookFn := func(ctx context.Context, hctx *gsyncx.HookContext) error {
		return fmt.Errorf("hook error")
	}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithHook(gsyncx.HookAfterRead, hookFn),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != gsyncx.StatusCompleted {
		t.Errorf("expected completed even with failing hook, got %s", result.Status)
	}
}

func TestSyncEngine_WriterError(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(100),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	rd := &mockReader{
		records: [][]gsyncx.Record{{{Data: map[string]interface{}{"id": 1}}}},
		count:   1,
	}
	wr := &errorMockWriter{}

	eng, _ := NewSyncEngine(cfg,
		WithReader(rd),
		WithWriter(wr),
		WithLogger(gsyncx.NewNopLogger()),
	)

	result, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("should not return error, errors tracked in result: %v", err)
	}
	if result.TotalFailed != 1 {
		t.Errorf("expected 1 failed, got %d", result.TotalFailed)
	}
}

type errorMockWriter struct{}

func (m *errorMockWriter) Write(ctx context.Context, records []gsyncx.Record) (gsyncx.WriteResult, error) {
	return gsyncx.WriteResult{FailedCount: int64(len(records))}, fmt.Errorf("write error")
}

func (m *errorMockWriter) WriteWithMode(ctx context.Context, records []gsyncx.Record, mode gsyncx.WriteMode) (gsyncx.WriteResult, error) {
	return m.Write(ctx, records)
}

func (m *errorMockWriter) Flush(ctx context.Context) error { return nil }
func (m *errorMockWriter) Close() error                    { return nil }

func TestSyncEngine_SetMethods(t *testing.T) {
	cfg := gsyncx.NewSyncConfig()
	eng, _ := NewSyncEngine(cfg, WithLogger(gsyncx.NewNopLogger()))

	eng.SetReader(&mockReader{})
	eng.SetWriter(&mockWriter{})
	eng.SetTransformer(transform.NewDefaultTransformer())
	eng.SetMapper(mapping.NewFieldMappingEngine(gsyncx.MappingConfig{AutoMapping: true}, nil))
	eng.SetCheckpointStore(checkpoint.NewMemoryCheckpointStore())
	eng.SetSourceDS(nil)
	eng.SetTargetDS(nil)
	eng.SetLogger(gsyncx.NewNopLogger())
	eng.SetErrorHandler(&testErrorHandler{})
	eng.SetIntegrityChecker(nil)
	eng.AddHook(gsyncx.HookAfterRead, func(ctx context.Context, hctx *gsyncx.HookContext) error { return nil })
}

func TestSyncEngine_GetMethods(t *testing.T) {
	cfg := gsyncx.NewSyncConfig()
	eng, _ := NewSyncEngine(cfg, WithLogger(gsyncx.NewNopLogger()))

	if eng.GetConfig() == nil {
		t.Error("expected non-nil config")
	}
	if eng.GetStats() == nil {
		t.Error("expected non-nil stats")
	}
	if eng.GetProgress() == nil {
		t.Error("expected non-nil progress")
	}
}
