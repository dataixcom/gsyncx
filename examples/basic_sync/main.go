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
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(1000),
		gsyncx.WithParallelism(4),
		gsyncx.WithContinueOnError(true),
		gsyncx.WithRetry(3, time.Second),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
			DSNConfig: &gsyncx.DSNConfig{
				DBType:   gsyncx.DBMySQL,
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "password",
				Schema:   "source_db",
			},
			TableName: "users",
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
			TableName: "users",
			WriteMode: gsyncx.WriteModeUpsert,
		}),
	)

	result, err := engine.RunSync(context.Background(), cfg, nil)
	if err != nil {
		log.Fatalf("sync failed: %v", err)
	}

	fmt.Printf("Sync completed: status=%s read=%d written=%d failed=%d duration=%v\n",
		result.Status, result.TotalRead, result.TotalWritten, result.TotalFailed, result.Duration)
}
