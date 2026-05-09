package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/datasource"
	"github.com/dataixcom/gsyncx/engine"
	"github.com/dataixcom/gsyncx/mapping"
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

	logger := gsyncx.NewSyncLogger()

	mappings := []gsyncx.FieldMapping{
		{SourceField: "user_name", TargetField: "name", Transform: "trim"},
		{SourceField: "user_email", TargetField: "email", Transform: "to_lower"},
		{SourceField: "user_age", TargetField: "age"},
		{SourceField: "created_at", TargetField: "synced_at"},
	}

	fieldMapper := mapping.NewFieldMappingEngineWithMappings(mappings, logger)

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(500),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
			TableName: "source_users",
		}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{
			TableName: "target_users",
			WriteMode: gsyncx.WriteModeUpsert,
		}),
	)

	eng, err := engine.NewSyncEngine(cfg,
		engine.WithSourceDS(sourceDS),
		engine.WithTargetDS(targetDS),
		engine.WithReader(reader.NewDatabaseReader(sourceDS, logger)),
		engine.WithWriter(writer.NewDatabaseWriter(targetDS, logger)),
		engine.WithMapper(fieldMapper),
		engine.WithLogger(logger),
	)
	if err != nil {
		log.Fatalf("failed to create engine: %v", err)
	}

	result, err := eng.Run(context.Background())
	if err != nil {
		log.Fatalf("mapping sync failed: %v", err)
	}

	fmt.Printf("Field mapping sync completed: status=%s read=%d written=%d\n",
		result.Status, result.TotalRead, result.TotalWritten)
}
