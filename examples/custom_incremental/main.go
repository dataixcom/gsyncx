package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/engine"
)

func main() {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeIncremental),
		gsyncx.WithIncrementalField(
			&gsyncx.Field{FieldName: "updated_at"},
			gsyncx.StrategyCustom,
		),
		gsyncx.WithIncrementalCondition("{field} >= '{last_sync_time}' AND status = 'active'"),
		gsyncx.WithLastSyncTime(time.Now().Add(-7*24*time.Hour)),
		gsyncx.WithBatchSize(500),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
			DSNConfig: &gsyncx.DSNConfig{
				DBType:   gsyncx.DBMySQL,
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "password",
				Schema:   "source_db",
			},
			TableName: "orders",
		}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{
			DSNConfig: &gsyncx.DSNConfig{
				DBType:   gsyncx.DBMySQL,
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "password",
				Schema:   "target_db",
			},
			TableName: "orders",
			WriteMode: gsyncx.WriteModeUpsert,
		}),
	)

	result, err := engine.RunSync(context.Background(), cfg, nil)
	if err != nil {
		log.Fatalf("custom incremental sync failed: %v", err)
	}

	fmt.Printf("Custom incremental sync completed: status=%s read=%d written=%d failed=%d\n",
		result.Status, result.TotalRead, result.TotalWritten, result.TotalFailed)

	fmt.Println("\n--- Custom Incremental Condition Placeholders ---")
	fmt.Println("{field}            - The incremental field name (escaped for DB)")
	fmt.Println("{last_sync_time}   - Last sync timestamp (formatted as datetime)")
	fmt.Println("{last_sync_value}  - Last sync value (for autoinc/version strategies)")
}
