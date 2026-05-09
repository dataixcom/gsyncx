package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/engine"
)

func main() {
	customLogger := gsyncx.NewFuncLogger(
		func(msg string, fields ...gsyncx.LogField) {
			fmt.Fprintf(os.Stdout, "[INFO] %s %v\n", msg, fields)
		},
		func(msg string, fields ...gsyncx.LogField) {
			fmt.Fprintf(os.Stderr, "[WARN] %s %v\n", msg, fields)
		},
		func(msg string, fields ...gsyncx.LogField) {
			fmt.Fprintf(os.Stderr, "[ERROR] %s %v\n", msg, fields)
		},
		func(msg string, fields ...gsyncx.LogField) {
			fmt.Fprintf(os.Stdout, "[DEBUG] %s %v\n", msg, fields)
		},
	)

	gsyncx.SetDefaultLogger(customLogger)

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(1000),
		gsyncx.WithLogger(customLogger),
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

	result, err := engine.RunSync(context.Background(), cfg, customLogger)
	if err != nil {
		log.Fatalf("sync failed: %v", err)
	}

	fmt.Printf("Sync with custom logger: status=%s written=%d\n",
		result.Status, result.TotalWritten)
}
