package engine

import (
	"fmt"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/checkpoint"
)

type ConfigValidator struct {
	config  *gsyncx.SyncConfig
	logger  gsyncx.SyncLogger
	cpStore gsyncx.CheckpointStore
}

func NewConfigValidator(config *gsyncx.SyncConfig, logger gsyncx.SyncLogger) *ConfigValidator {
	return &ConfigValidator{
		config:  config,
		logger:  gsyncx.ResolveLogger(logger),
		cpStore: checkpoint.NewMemoryCheckpointStore(),
	}
}

func (v *ConfigValidator) Validate() error {
	if v.config == nil {
		return fmt.Errorf("sync config is required")
	}

	if err := v.validateReaderConfig(); err != nil {
		return fmt.Errorf("reader config validation failed: %w", err)
	}

	if err := v.validateWriterConfig(); err != nil {
		return fmt.Errorf("writer config validation failed: %w", err)
	}

	if err := v.validateSyncMode(); err != nil {
		return fmt.Errorf("sync mode validation failed: %w", err)
	}

	if err := v.validateMappingConfig(); err != nil {
		return fmt.Errorf("mapping config validation failed: %w", err)
	}

	return nil
}

func (v *ConfigValidator) validateReaderConfig() error {
	cfg := &v.config.ReaderConfig

	if v.config.SyncMode == gsyncx.SyncModeRealtime {
		return nil
	}

	if cfg.TableName == "" && cfg.SQL == "" {
		return fmt.Errorf("either table name or SQL must be specified")
	}

	if v.config.SyncMode == gsyncx.SyncModeIncremental {
		if v.config.IncrementalField == nil {
			return fmt.Errorf("incremental field is required for incremental sync mode")
		}
	}

	return nil
}

func (v *ConfigValidator) validateWriterConfig() error {
	cfg := &v.config.WriterConfig

	if cfg.TableName == "" {
		return fmt.Errorf("target table name is required")
	}

	return nil
}

func (v *ConfigValidator) validateSyncMode() error {
	switch v.config.SyncMode {
	case gsyncx.SyncModeFull, gsyncx.SyncModeIncremental, gsyncx.SyncModeRealtime:
		return nil
	default:
		return fmt.Errorf("unsupported sync mode: %s", v.config.SyncMode)
	}
}

func (v *ConfigValidator) validateMappingConfig() error {
	if len(v.config.MappingConfig.Mappings) == 0 {
		return nil
	}

	seen := make(map[string]bool)
	for _, m := range v.config.MappingConfig.Mappings {
		if m.SourceField == "" {
			return fmt.Errorf("mapping source field cannot be empty")
		}
		if m.TargetField == "" {
			return fmt.Errorf("mapping target field cannot be empty for source %s", m.SourceField)
		}
		if seen[m.TargetField] {
			return fmt.Errorf("duplicate target field in mapping: %s", m.TargetField)
		}
		seen[m.TargetField] = true
	}

	return nil
}

func (v *ConfigValidator) SetCheckpointStore(store gsyncx.CheckpointStore) {
	v.cpStore = store
}
