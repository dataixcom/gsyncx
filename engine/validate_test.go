package engine

import (
	"testing"

	"github.com/dataixcom/gsyncx"
)

func TestNewConfigValidator_NilConfig(t *testing.T) {
	v := NewConfigValidator(nil, nil)
	err := v.Validate()
	if err == nil {
		t.Error("expected error for nil config")
	}
}

func TestNewConfigValidator_ValidFullSync(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	v := NewConfigValidator(cfg, nil)
	err := v.Validate()
	if err != nil {
		t.Errorf("expected valid config, got error: %v", err)
	}
}

func TestNewConfigValidator_ValidIncrementalSync(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeIncremental),
		gsyncx.WithIncrementalField(&gsyncx.Field{FieldName: "updated_at"}, gsyncx.StrategyTimestamp),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	v := NewConfigValidator(cfg, nil)
	err := v.Validate()
	if err != nil {
		t.Errorf("expected valid config, got error: %v", err)
	}
}

func TestConfigValidator_NoSource(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	v := NewConfigValidator(cfg, nil)
	err := v.Validate()
	if err == nil {
		t.Error("expected error for missing source")
	}
}

func TestConfigValidator_NoTarget(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{WriteMode: gsyncx.WriteModeUpsert}),
	)

	v := NewConfigValidator(cfg, nil)
	err := v.Validate()
	if err == nil {
		t.Error("expected error for missing target")
	}
}

func TestConfigValidator_IncrementalWithoutField(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeIncremental),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	v := NewConfigValidator(cfg, nil)
	err := v.Validate()
	if err == nil {
		t.Error("expected error for incremental without field")
	}
}

func TestConfigValidator_UnsupportedSyncMode(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)
	cfg.SyncMode = "unsupported"

	v := NewConfigValidator(cfg, nil)
	err := v.Validate()
	if err == nil {
		t.Error("expected error for unsupported sync mode")
	}
}

func TestConfigValidator_DuplicateMappingTarget(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "source"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
		gsyncx.WithMappingConfig(gsyncx.MappingConfig{
			Mappings: []gsyncx.FieldMapping{
				{SourceField: "a", TargetField: "dup"},
				{SourceField: "b", TargetField: "dup"},
			},
		}),
	)

	v := NewConfigValidator(cfg, nil)
	err := v.Validate()
	if err == nil {
		t.Error("expected error for duplicate target field")
	}
}

func TestConfigValidator_SQLReader(t *testing.T) {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{SQL: "SELECT * FROM users"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "target", WriteMode: gsyncx.WriteModeUpsert}),
	)

	v := NewConfigValidator(cfg, nil)
	err := v.Validate()
	if err != nil {
		t.Errorf("expected valid config with SQL reader, got error: %v", err)
	}
}

func TestConfigValidator_SetCheckpointStore(t *testing.T) {
	v := NewConfigValidator(nil, nil)
	v.SetCheckpointStore(nil)
}
