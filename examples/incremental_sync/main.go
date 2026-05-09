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
		gsyncx.WithBatchSize(500),
		gsyncx.WithParallelism(2),
		gsyncx.WithCheckpoint(true, "./checkpoints"),
		gsyncx.WithIncrementalField(
			&gsyncx.Field{FieldName: "updated_at"},
			gsyncx.StrategyTimestamp,
		),
		gsyncx.WithLastSyncTime(time.Now().Add(-24*time.Hour)),
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
		log.Fatalf("incremental sync failed: %v", err)
	}

	fmt.Printf("Incremental sync completed: status=%s read=%d written=%d failed=%d\n",
		result.Status, result.TotalRead, result.TotalWritten, result.TotalFailed)
}
