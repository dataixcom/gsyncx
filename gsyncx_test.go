package gsyncx

import (
	"testing"
	"time"
)

func TestNewSyncConfig(t *testing.T) {
	cfg := NewSyncConfig(
		WithSyncMode(SyncModeFull),
		WithBatchSize(500),
		WithParallelism(8),
		WithCheckpoint(true, "/tmp/checkpoint"),
		WithContinueOnError(true),
		WithRetry(3, 0),
	)

	if cfg.SyncMode != SyncModeFull {
		t.Errorf("expected SyncModeFull, got %s", cfg.SyncMode)
	}
	if cfg.BatchSize != 500 {
		t.Errorf("expected batch size 500, got %d", cfg.BatchSize)
	}
	if cfg.Parallelism != 8 {
		t.Errorf("expected parallelism 8, got %d", cfg.Parallelism)
	}
	if !cfg.CheckpointEnabled {
		t.Error("expected checkpoint enabled")
	}
	if cfg.CheckpointPath != "/tmp/checkpoint" {
		t.Errorf("expected checkpoint path /tmp/checkpoint, got %s", cfg.CheckpointPath)
	}
}

func TestNewSyncConfigDefaults(t *testing.T) {
	cfg := NewSyncConfig()

	if cfg.SyncMode != SyncModeFull {
		t.Errorf("expected default SyncModeFull, got %s", cfg.SyncMode)
	}
	if cfg.BatchSize != 1000 {
		t.Errorf("expected default batch size 1000, got %d", cfg.BatchSize)
	}
	if cfg.Parallelism != 4 {
		t.Errorf("expected default parallelism 4, got %d", cfg.Parallelism)
	}
	if cfg.RetryMaxAttempts != 3 {
		t.Errorf("expected default retry max attempts 3, got %d", cfg.RetryMaxAttempts)
	}
	if !cfg.ContinueOnError {
		t.Error("expected default continue on error true")
	}
}

func TestWithLogger(t *testing.T) {
	logger := NewNopLogger()
	cfg := NewSyncConfig(WithLogger(logger))
	if cfg.GetLogger() != logger {
		t.Error("expected custom logger to be set")
	}
}

func TestSyncConfig_GetLogger_Default(t *testing.T) {
	cfg := NewSyncConfig()
	if cfg.GetLogger() == nil {
		t.Error("expected non-nil default logger")
	}
}

func TestSyncConfig_IsFullSync(t *testing.T) {
	cfg := NewSyncConfig(WithSyncMode(SyncModeFull))
	if !cfg.IsFullSync() {
		t.Error("expected IsFullSync to be true")
	}
	if cfg.IsIncrementalSync() {
		t.Error("expected IsIncrementalSync to be false")
	}
}

func TestSyncConfig_IsIncrementalSync(t *testing.T) {
	cfg := NewSyncConfig(WithSyncMode(SyncModeIncremental))
	if !cfg.IsIncrementalSync() {
		t.Error("expected IsIncrementalSync to be true")
	}
	if cfg.IsFullSync() {
		t.Error("expected IsFullSync to be false")
	}
}

func TestSyncConfig_SwitchToFullSync(t *testing.T) {
	cfg := NewSyncConfig(
		WithSyncMode(SyncModeIncremental),
		WithIncrementalField(&Field{FieldName: "updated_at"}, StrategyTimestamp),
		WithLastSyncTime(time.Now()),
		WithLastSyncValue(100),
		WithIncrementalCondition("{field} > {last_sync_value}"),
	)

	cfg.SwitchToFullSync()

	if cfg.SyncMode != SyncModeFull {
		t.Errorf("expected SyncModeFull after switch, got %s", cfg.SyncMode)
	}
	if cfg.IncrementalField != nil {
		t.Error("expected incremental field to be nil after switch")
	}
	if cfg.IncrementalStrategy != "" {
		t.Error("expected incremental strategy to be empty after switch")
	}
	if cfg.IncrementalCondition != "" {
		t.Error("expected incremental condition to be empty after switch")
	}
	if !cfg.LastSyncTime.IsZero() {
		t.Error("expected last sync time to be zero after switch")
	}
	if cfg.LastSyncValue != nil {
		t.Error("expected last sync value to be nil after switch")
	}
}

func TestSyncConfig_SwitchToIncrementalSync(t *testing.T) {
	cfg := NewSyncConfig(WithSyncMode(SyncModeFull))
	field := &Field{FieldName: "id"}

	cfg.SwitchToIncrementalSync(field, StrategyAutoInc)

	if cfg.SyncMode != SyncModeIncremental {
		t.Errorf("expected SyncModeIncremental after switch, got %s", cfg.SyncMode)
	}
	if cfg.IncrementalField != field {
		t.Error("expected incremental field to be set")
	}
	if cfg.IncrementalStrategy != StrategyAutoInc {
		t.Errorf("expected StrategyAutoInc, got %s", cfg.IncrementalStrategy)
	}
}

func TestWithIncrementalCondition(t *testing.T) {
	cfg := NewSyncConfig(WithIncrementalCondition("{field} > {last_sync_value}"))
	if cfg.IncrementalCondition != "{field} > {last_sync_value}" {
		t.Errorf("expected incremental condition, got %s", cfg.IncrementalCondition)
	}
}

func TestWithIntegrityCheck(t *testing.T) {
	cfg := NewSyncConfig(WithIntegrityCheck(IntegrityCheckCount))
	if cfg.IntegrityCheck != IntegrityCheckCount {
		t.Errorf("expected IntegrityCheckCount, got %s", cfg.IntegrityCheck)
	}
}

func TestWithAutoMapping(t *testing.T) {
	cfg := NewSyncConfig(WithAutoMapping(true))
	if !cfg.AutoMapping {
		t.Error("expected auto mapping to be enabled")
	}
	if !cfg.MappingConfig.AutoMapping {
		t.Error("expected mapping config auto mapping to be enabled")
	}
}

func TestWithPreviewMode(t *testing.T) {
	cfg := NewSyncConfig(WithPreviewMode(100))
	if !cfg.PreviewMode {
		t.Error("expected preview mode enabled")
	}
	if cfg.PreviewLimit != 100 {
		t.Errorf("expected preview limit 100, got %d", cfg.PreviewLimit)
	}
}

func TestWithErrorThreshold(t *testing.T) {
	cfg := NewSyncConfig(WithErrorThreshold(50))
	if cfg.ErrorThreshold != 50 {
		t.Errorf("expected error threshold 50, got %d", cfg.ErrorThreshold)
	}
}

func TestWithTransformConfig(t *testing.T) {
	cfg := NewSyncConfig(WithTransformConfig(TransformConfig{
		Script:     "function transform(t) return t end",
		ScriptLang: "lua",
	}))
	if cfg.TransformConfig.Script == "" {
		t.Error("expected transform script to be set")
	}
}

func TestWithMappingConfig(t *testing.T) {
	cfg := NewSyncConfig(WithMappingConfig(MappingConfig{
		Mappings: []FieldMapping{
			{SourceField: "src", TargetField: "dst"},
		},
	}))
	if len(cfg.MappingConfig.Mappings) != 1 {
		t.Error("expected mapping to be set")
	}
}

func TestAnyToInt64(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
		want  int64
	}{
		{"int64", int64(42), 42},
		{"int", int(42), 42},
		{"int32", int32(42), 42},
		{"float64", float64(42.5), 42},
		{"float32", float32(42.5), 42},
		{"string", "42", 42},
		{"empty string", "", 0},
		{"nil", nil, 0},
		{"bool", true, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AnyToInt64(tt.input)
			if result != tt.want {
				t.Errorf("AnyToInt64(%v) = %d, want %d", tt.input, result, tt.want)
			}
		})
	}
}

func TestIncrementalHelper_BuildCondition_Timestamp(t *testing.T) {
	helper := NewIncrementalHelper(StrategyTimestamp, "updated_at")
	cfg := NewSyncConfig(
		WithSyncMode(SyncModeIncremental),
		WithLastSyncTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
	)

	condition := helper.BuildCondition(cfg)
	if condition == "" {
		t.Error("expected non-empty condition for timestamp strategy")
	}
}

func TestIncrementalHelper_BuildCondition_Timestamp_ZeroTime(t *testing.T) {
	helper := NewIncrementalHelper(StrategyTimestamp, "updated_at")
	cfg := NewSyncConfig(WithSyncMode(SyncModeIncremental))

	condition := helper.BuildCondition(cfg)
	if condition != "" {
		t.Error("expected empty condition when last sync time is zero")
	}
}

func TestIncrementalHelper_BuildCondition_AutoInc(t *testing.T) {
	helper := NewIncrementalHelper(StrategyAutoInc, "id")
	cfg := NewSyncConfig(
		WithSyncMode(SyncModeIncremental),
		WithLastSyncValue(1000),
	)

	condition := helper.BuildCondition(cfg)
	if condition == "" {
		t.Error("expected non-empty condition for autoinc strategy")
	}
}

func TestIncrementalHelper_BuildCondition_AutoInc_NilValue(t *testing.T) {
	helper := NewIncrementalHelper(StrategyAutoInc, "id")
	cfg := NewSyncConfig(WithSyncMode(SyncModeIncremental))

	condition := helper.BuildCondition(cfg)
	if condition != "" {
		t.Error("expected empty condition when last sync value is nil")
	}
}

func TestIncrementalHelper_BuildCondition_Custom(t *testing.T) {
	helper := NewIncrementalHelper(StrategyCustom, "id")
	cfg := NewSyncConfig(
		WithSyncMode(SyncModeIncremental),
		WithIncrementalCondition("{field} > {last_sync_value}"),
		WithLastSyncValue(100),
	)

	condition := helper.BuildCondition(cfg)
	if condition == "" {
		t.Error("expected non-empty condition for custom strategy")
	}
	if condition != "id > 100" {
		t.Errorf("expected 'id > 100', got '%s'", condition)
	}
}

func TestIncrementalHelper_BuildCondition_Custom_Empty(t *testing.T) {
	helper := NewIncrementalHelper(StrategyCustom, "id")
	cfg := NewSyncConfig(WithSyncMode(SyncModeIncremental))

	condition := helper.BuildCondition(cfg)
	if condition != "" {
		t.Error("expected empty condition when no custom condition set")
	}
}

func TestIncrementalHelper_BuildCondition_Custom_WithTimestamp(t *testing.T) {
	helper := NewIncrementalHelper(StrategyCustom, "updated_at")
	cfg := NewSyncConfig(
		WithSyncMode(SyncModeIncremental),
		WithIncrementalCondition("{field} >= '{last_sync_time}'"),
		WithLastSyncTime(time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)),
	)

	condition := helper.BuildCondition(cfg)
	if condition == "" {
		t.Error("expected non-empty condition")
	}
}

func TestIncrementalHelper_UnknownStrategy(t *testing.T) {
	helper := NewIncrementalHelper("unknown", "id")
	cfg := NewSyncConfig(WithSyncMode(SyncModeIncremental))

	condition := helper.BuildCondition(cfg)
	if condition != "" {
		t.Error("expected empty condition for unknown strategy")
	}
}

func TestIntegrityCheckMode_Constants(t *testing.T) {
	tests := []struct {
		name  string
		mode  IntegrityCheckMode
		value string
	}{
		{"none", IntegrityCheckNone, "none"},
		{"count", IntegrityCheckCount, "count"},
		{"checksum", IntegrityCheckChecksum, "checksum"},
		{"full", IntegrityCheckFull, "full"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.mode) != tt.value {
				t.Errorf("expected %s, got %s", tt.value, tt.mode)
			}
		})
	}
}

func TestIncrementalStrategy_Constants(t *testing.T) {
	if StrategyCustom != "custom" {
		t.Errorf("expected 'custom', got %s", StrategyCustom)
	}
	if StrategyTimestamp != "timestamp" {
		t.Errorf("expected 'timestamp', got %s", StrategyTimestamp)
	}
	if StrategyAutoInc != "autoinc" {
		t.Errorf("expected 'autoinc', got %s", StrategyAutoInc)
	}
	if StrategyVersion != "version" {
		t.Errorf("expected 'version', got %s", StrategyVersion)
	}
}

func TestSyncStatistics_Increment(t *testing.T) {
	s := &SyncStatistics{}

	s.IncReadOK(8)
	s.IncReadFailed(2)
	s.IncTransformOK(8)
	s.IncTransformFailed(0)
	s.IncMappingOK(8)
	s.IncMappingFailed(0)
	s.IncWriteOK(7)
	s.IncWriteFailed(1)
	s.IncSkippedTotal(0)

	if s.ReadOK != 8 {
		t.Errorf("expected ReadOK 8, got %d", s.ReadOK)
	}
	if s.ReadFailed != 2 {
		t.Errorf("expected ReadFailed 2, got %d", s.ReadFailed)
	}
	if s.WriteOK != 7 {
		t.Errorf("expected WriteOK 7, got %d", s.WriteOK)
	}
	if s.WriteFailed != 1 {
		t.Errorf("expected WriteFailed 1, got %d", s.WriteFailed)
	}
}

func TestHandleDecision_Constants(t *testing.T) {
	if DecisionRetry != 0 {
		t.Errorf("expected DecisionRetry=0, got %d", DecisionRetry)
	}
	if DecisionSkip != 1 {
		t.Errorf("expected DecisionSkip=1, got %d", DecisionSkip)
	}
	if DecisionAbort != 2 {
		t.Errorf("expected DecisionAbort=2, got %d", DecisionAbort)
	}
}
