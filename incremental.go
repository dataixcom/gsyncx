package gsyncx

import (
	"fmt"
	"strings"

	"github.com/dataixcom/gdbx/base/config"
)

type IncrementalHelper struct {
	strategy  IncrementalStrategy
	fieldName string
}

func NewIncrementalHelper(strategy IncrementalStrategy, fieldName string) *IncrementalHelper {
	return &IncrementalHelper{
		strategy:  strategy,
		fieldName: fieldName,
	}
}

func (h *IncrementalHelper) BuildCondition(cfg *SyncConfig) string {
	switch h.strategy {
	case StrategyTimestamp:
		return h.buildTimestampCondition(cfg)
	case StrategyAutoInc, StrategyVersion:
		return h.buildValueCondition(cfg)
	case StrategyCustom:
		return h.buildCustomCondition(cfg)
	default:
		return ""
	}
}

func (h *IncrementalHelper) buildTimestampCondition(cfg *SyncConfig) string {
	if cfg.LastSyncTime.IsZero() {
		return ""
	}
	if err := SanitizeIdentifier(h.fieldName); err != nil {
		return ""
	}
	var dbType config.DBType
	if cfg.ReaderConfig.DSNConfig != nil {
		dbType = cfg.ReaderConfig.DSNConfig.DBType
	}
	escapedField := FormatFieldName(h.fieldName, dbType)
	w := NewWhereClauseBuilder().AndGt(escapedField, cfg.LastSyncTime)
	condSQL, _, _ := w.Build(dbType)
	return condSQL
}

func (h *IncrementalHelper) buildValueCondition(cfg *SyncConfig) string {
	if cfg.LastSyncValue == nil {
		return ""
	}
	if err := SanitizeIdentifier(h.fieldName); err != nil {
		return ""
	}
	var dbType config.DBType
	if cfg.ReaderConfig.DSNConfig != nil {
		dbType = cfg.ReaderConfig.DSNConfig.DBType
	}
	escapedField := FormatFieldName(h.fieldName, dbType)
	w := NewWhereClauseBuilder().AndGt(escapedField, cfg.LastSyncValue)
	condSQL, _, _ := w.Build(dbType)
	return condSQL
}

func (h *IncrementalHelper) buildCustomCondition(cfg *SyncConfig) string {
	if cfg.IncrementalCondition == "" {
		return ""
	}

	condition := cfg.IncrementalCondition
	condition = strings.ReplaceAll(condition, "{last_sync_time}", cfg.LastSyncTime.Format("2006-01-02 15:04:05"))
	condition = strings.ReplaceAll(condition, "{last_sync_value}", fmt.Sprintf("%v", cfg.LastSyncValue))

	if h.fieldName != "" {
		var dbType config.DBType
		if cfg.ReaderConfig.DSNConfig != nil {
			dbType = cfg.ReaderConfig.DSNConfig.DBType
		}
		escapedField := FormatFieldName(h.fieldName, dbType)
		condition = strings.ReplaceAll(condition, "{field}", escapedField)
	}

	return condition
}

func AnyToInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case float64:
		return int64(val)
	case float32:
		return int64(val)
	case string:
		v := strings.TrimSpace(val)
		if v == "" {
			return 0
		}
		var result int64
		fmt.Sscanf(v, "%d", &result)
		return result
	default:
		return 0
	}
}
