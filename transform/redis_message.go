package transform

import (
	"context"
	"encoding/json"

	"github.com/dataixcom/gsyncx"
)

type RedisMessageTransformer struct {
	fieldMapping map[string]string
	logger       gsyncx.SyncLogger
}

func NewRedisMessageTransformer(fieldMapping map[string]string, logger gsyncx.SyncLogger) *RedisMessageTransformer {
	return &RedisMessageTransformer{
		fieldMapping: fieldMapping,
		logger:       gsyncx.ResolveLogger(logger),
	}
}

func (t *RedisMessageTransformer) Transform(ctx context.Context, records []gsyncx.Record) ([]gsyncx.Record, []gsyncx.FailedRecord, error) {
	result := make([]gsyncx.Record, 0, len(records))
	var failed []gsyncx.FailedRecord

	for i, record := range records {
		transformed, err := t.transformRecord(record)
		if err != nil {
			failed = append(failed, gsyncx.FailedRecord{
				Record: record,
				Error:  err,
				Stage:  gsyncx.StageTransform,
			})
			continue
		}
		result = append(result, transformed)

		select {
		case <-ctx.Done():
			return result, failed, ctx.Err()
		default:
		}

		if i > 0 && i%1000 == 0 {
			t.logger.Debug("transform progress",
				gsyncx.F("processed", i),
				gsyncx.F("total", len(records)),
			)
		}
	}

	return result, failed, nil
}

func (t *RedisMessageTransformer) transformRecord(record gsyncx.Record) (gsyncx.Record, error) {
	newData := make(map[string]interface{})

	for sourceField, targetField := range t.fieldMapping {
		if value, ok := record.Data[sourceField]; ok {
			newData[targetField] = value
		}
	}

	for k, v := range record.Data {
		if _, mapped := t.fieldMapping[k]; !mapped {
			if _, exists := newData[k]; !exists {
				newData[k] = v
			}
		}
	}

	if payload, ok := record.Data["payload"]; ok {
		if payloadStr, ok := payload.(string); ok {
			var parsed map[string]interface{}
			if err := json.Unmarshal([]byte(payloadStr), &parsed); err == nil {
				for k, v := range parsed {
					if _, exists := newData[k]; !exists {
						newData[k] = v
					}
				}
			}
		}
	}

	return gsyncx.Record{Data: newData, Meta: record.Meta}, nil
}
