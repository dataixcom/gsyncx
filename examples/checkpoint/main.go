package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/checkpoint"
	"github.com/dataixcom/gsyncx/datasource"
	"github.com/dataixcom/gsyncx/engine"
	"github.com/dataixcom/gsyncx/reader"
	"github.com/dataixcom/gsyncx/writer"
)

func main() {
	sourceDSN := gsyncx.DSNConfig{
		DBType:   gsyncx.DBMySQL,
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "password",
		Schema:   "source_db",
	}
	targetDSN := gsyncx.DSNConfig{
		DBType:   gsyncx.DBMySQL,
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "password",
		Schema:   "target_db",
	}

	sourceDS, err := datasource.NewGdbxDataSource(sourceDSN)
	if err != nil {
		log.Fatalf("failed to create source datasource: %v", err)
	}
	targetDS, err := datasource.NewGdbxDataSource(targetDSN)
	if err != nil {
		log.Fatalf("failed to create target datasource: %v", err)
	}

	fileStore, err := checkpoint.NewFileCheckpointStore("./checkpoints")
	if err != nil {
		log.Fatalf("failed to create checkpoint store: %v", err)
	}

	logger := gsyncx.NewSyncLogger()

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeIncremental),
		gsyncx.WithBatchSize(500),
		gsyncx.WithIncrementalField(
			&gsyncx.Field{FieldName: "id"},
			gsyncx.StrategyAutoInc,
		),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
			TableName: "large_table",
		}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{
			TableName: "large_table",
			WriteMode: gsyncx.WriteModeUpsert,
		}),
	)

	eng, err := engine.NewSyncEngine(cfg,
		engine.WithSourceDS(sourceDS),
		engine.WithTargetDS(targetDS),
		engine.WithReader(reader.NewDatabaseReader(sourceDS, logger)),
		engine.WithWriter(writer.NewDatabaseWriter(targetDS, logger)),
		engine.WithCheckpointStore(fileStore),
		engine.WithLogger(logger),
	)
	if err != nil {
		log.Fatalf("failed to create engine: %v", err)
	}

	result, err := eng.Run(context.Background())
	if err != nil {
		log.Fatalf("checkpoint sync failed: %v", err)
	}

	fmt.Printf("Checkpoint sync completed: status=%s read=%d written=%d\n",
		result.Status, result.TotalRead, result.TotalWritten)

	cp, err := fileStore.Load(context.Background(), "large_table")
	if err != nil {
		log.Printf("failed to load checkpoint: %v", err)
	} else if cp != nil {
		fmt.Printf("Checkpoint saved: field=%s last_value=%v\n", cp.FieldName, cp.LastValue)
	}
}
