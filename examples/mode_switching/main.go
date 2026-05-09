package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/datasource"
	"github.com/dataixcom/gsyncx/engine"
	"github.com/dataixcom/gsyncx/reader"
	"github.com/dataixcom/gsyncx/writer"
)

func main() {
	logger := gsyncx.NewSyncLogger()

	sourceDS, err := datasource.NewGdbxDataSource(gsyncx.DSNConfig{
		DBType:   gsyncx.DBMySQL,
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "password",
		Schema:   "source_db",
	})
	if err != nil {
		log.Fatalf("failed to create source datasource: %v", err)
	}

	targetDS, err := datasource.NewGdbxDataSource(gsyncx.DSNConfig{
		DBType:   gsyncx.DBMySQL,
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "password",
		Schema:   "target_db",
	})
	if err != nil {
		log.Fatalf("failed to create target datasource: %v", err)
	}

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(1000),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "users"}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{TableName: "users", WriteMode: gsyncx.WriteModeUpsert}),
	)

	eng, err := engine.NewSyncEngine(cfg,
		engine.WithSourceDS(sourceDS),
		engine.WithTargetDS(targetDS),
		engine.WithReader(reader.NewDatabaseReader(sourceDS, logger)),
		engine.WithWriter(writer.NewDatabaseWriter(targetDS, logger)),
		engine.WithLogger(logger),
	)
	if err != nil {
		log.Fatalf("failed to create engine: %v", err)
	}

	fmt.Println("Starting full sync...")
	result, err := eng.Run(context.Background())
	if err != nil {
		log.Fatalf("full sync failed: %v", err)
	}
	fmt.Printf("Full sync completed: status=%s read=%d written=%d\n",
		result.Status, result.TotalRead, result.TotalWritten)

	eng.SwitchToIncrementalSync(
		&gsyncx.Field{FieldName: "updated_at"},
		gsyncx.StrategyTimestamp,
	)
	eng.GetConfig().LastSyncTime = time.Now().Add(-24 * time.Hour)

	fmt.Println("\nSwitched to incremental sync...")
	result, err = eng.Run(context.Background())
	if err != nil {
		log.Fatalf("incremental sync failed: %v", err)
	}
	fmt.Printf("Incremental sync completed: status=%s read=%d written=%d\n",
		result.Status, result.TotalRead, result.TotalWritten)

	eng.SwitchToFullSync()
	fmt.Println("\nSwitched back to full sync mode")
	fmt.Printf("Current mode: %s\n", eng.GetConfig().SyncMode)
}
