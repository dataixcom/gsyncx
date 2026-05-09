package gsyncx

import (
	"testing"
)

func TestSyncProgress(t *testing.T) {
	progress := &SyncProgress{
		Status:        StatusRunning,
		TotalRecords:  100,
		SyncedRecords: 50,
		FailedRecords: 2,
		CurrentStage:  StageWrite,
	}

	if progress.Status != StatusRunning {
		t.Errorf("expected StatusRunning, got %s", progress.Status)
	}
	if progress.CurrentStage != StageWrite {
		t.Errorf("expected StageWrite, got %s", progress.CurrentStage)
	}
}

func TestRecord(t *testing.T) {
	record := Record{
		Data: map[string]interface{}{
			"id":   1,
			"name": "test",
		},
		Meta: RecordMeta{
			SourcePK: 1,
			Stage:    StageRead,
		},
	}

	if record.Data["id"] != 1 {
		t.Error("expected id=1")
	}
	if record.Meta.Stage != StageRead {
		t.Error("expected StageRead")
	}
}

func TestPipelineStage_Constants(t *testing.T) {
	if StageRead != "read" {
		t.Errorf("expected StageRead='read', got %s", StageRead)
	}
	if StageTransform != "transform" {
		t.Errorf("expected StageTransform='transform', got %s", StageTransform)
	}
	if StageMap != "map" {
		t.Errorf("expected StageMap='map', got %s", StageMap)
	}
	if StageWrite != "write" {
		t.Errorf("expected StageWrite='write', got %s", StageWrite)
	}
}

func TestHookPoint_Constants(t *testing.T) {
	if HookBeforeRead != "before_read" {
		t.Errorf("expected HookBeforeRead='before_read', got %s", HookBeforeRead)
	}
	if HookOnError != "on_error" {
		t.Errorf("expected HookOnError='on_error', got %s", HookOnError)
	}
	if HookOnComplete != "on_complete" {
		t.Errorf("expected HookOnComplete='on_complete', got %s", HookOnComplete)
	}
}

func TestFieldMapping(t *testing.T) {
	fm := FieldMapping{
		SourceField: "src_name",
		TargetField: "name",
		Transform:  "trim",
		Required:   true,
	}

	if fm.SourceField != "src_name" {
		t.Errorf("expected src_name, got %s", fm.SourceField)
	}
	if !fm.Required {
		t.Error("expected required=true")
	}
}

func TestSyncResult(t *testing.T) {
	result := SyncResult{
		Status:       StatusCompleted,
		TotalRead:    100,
		TotalWritten: 98,
		TotalFailed:  2,
	}

	if result.Status != StatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestFailedRecord(t *testing.T) {
	fr := FailedRecord{
		Record: Record{Data: map[string]any{"id": 1}},
		Stage:  StageWrite,
	}

	if fr.Stage != StageWrite {
		t.Errorf("expected StageWrite, got %s", fr.Stage)
	}
}

func TestIntegrityResult(t *testing.T) {
	ir := &IntegrityResult{
		Mode:        IntegrityCheckCount,
		SourceCount: 100,
		TargetCount: 100,
		CountMatch:  true,
		Passed:      true,
	}

	if ir.Mode != IntegrityCheckCount {
		t.Errorf("expected IntegrityCheckCount, got %s", ir.Mode)
	}
	if !ir.Passed {
		t.Error("expected passed=true")
	}
	if !ir.CountMatch {
		t.Error("expected count match=true")
	}
}

func TestSyncConfig_IsFullSync_And_IsIncrementalSync(t *testing.T) {
	cfg := NewSyncConfig(WithSyncMode(SyncModeFull))
	if !cfg.IsFullSync() {
		t.Error("expected IsFullSync=true")
	}
	if cfg.IsIncrementalSync() {
		t.Error("expected IsIncrementalSync=false")
	}

	cfg.SwitchToIncrementalSync(&Field{FieldName: "id"}, StrategyAutoInc)
	if cfg.IsFullSync() {
		t.Error("expected IsFullSync=false after switch")
	}
	if !cfg.IsIncrementalSync() {
		t.Error("expected IsIncrementalSync=true after switch")
	}

	cfg.SwitchToFullSync()
	if !cfg.IsFullSync() {
		t.Error("expected IsFullSync=true after switch back")
	}
}

func TestWriteResult(t *testing.T) {
	wr := WriteResult{
		SuccessCount: 100,
		FailedCount:  2,
		SkippedCount: 3,
	}

	if wr.SuccessCount != 100 {
		t.Errorf("expected 100, got %d", wr.SuccessCount)
	}
}

func TestCheckpoint(t *testing.T) {
	cp := &Checkpoint{
		TableName:    "users",
		FieldName:    "id",
		LastValue:    100,
		BatchNum:     5,
		BatchOffset:  50,
	}

	if cp.TableName != "users" {
		t.Errorf("expected users, got %s", cp.TableName)
	}
	if cp.BatchNum != 5 {
		t.Errorf("expected 5, got %d", cp.BatchNum)
	}
}
