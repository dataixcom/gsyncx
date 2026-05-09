package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/datasource"
	"github.com/dataixcom/gsyncx/engine"
	"github.com/dataixcom/gsyncx/mapping"
	"github.com/dataixcom/gsyncx/reader"
	"github.com/dataixcom/gsyncx/transform"
	"github.com/dataixcom/gsyncx/writer"
)

func main() {
	logger := gsyncx.NewSyncLogger()

	redisConfig := &reader.RedisStreamConfig{
		Addr:          "localhost:6379",
		Password:      "",
		DB:            0,
		Stream:        "data-sync-stream",
		ConsumerGroup: "gsyncx-sync-group",
		ConsumerName:  "gsyncx-consumer-1",
		Count:         100,
		Block:         5 * time.Second,
		BatchSize:     100,
		AutoCreate:    true,
		StartID:       "0",
	}

	streamReader, err := reader.NewRedisStreamReader(redisConfig, logger)
	if err != nil {
		log.Fatalf("failed to create redis stream reader: %v", err)
	}
	defer streamReader.Close()

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

	fieldMapping := map[string]string{
		"user_id":  "id",
		"username": "name",
		"email":    "email",
	}
	transformer := transform.NewRedisMessageTransformer(fieldMapping, logger)

	dbWriter := writer.NewDatabaseWriter(targetDS, logger)

	mapper := mapping.NewFieldMappingEngine(gsyncx.MappingConfig{
		Mappings: []gsyncx.FieldMapping{
			{SourceField: "id", TargetField: "user_id"},
			{SourceField: "name", TargetField: "user_name", Transform: "trim"},
			{SourceField: "email", TargetField: "user_email", Transform: "to_lower"},
		},
		AutoMapping: true,
	}, logger)

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeRealtime),
		gsyncx.WithBatchSize(100),
		gsyncx.WithContinueOnError(true),
		gsyncx.WithRetry(3, time.Second),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{
			TableName: "synced_users",
			WriteMode: gsyncx.WriteModeUpsert,
		}),
	)

	eng, err := engine.NewSyncEngine(cfg,
		engine.WithReader(streamReader),
		engine.WithWriter(dbWriter),
		engine.WithTransformer(transformer),
		engine.WithMapper(mapper),
		engine.WithLogger(logger),
	)
	if err != nil {
		log.Fatalf("failed to create sync engine: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		fmt.Printf("\nReceived signal: %v, shutting down...\n", sig)
		eng.Stop()
		cancel()
	}()

	fmt.Println("Starting Redis Stream realtime sync...")
	fmt.Println("Press Ctrl+C to stop")

	result, err := eng.Run(ctx)
	if err != nil {
		log.Printf("sync error: %v", err)
	}

	if result != nil {
		fmt.Printf("\nSync result: status=%s read=%d written=%d failed=%d duration=%v\n",
			result.Status, result.TotalRead, result.TotalWritten, result.TotalFailed, result.Duration)
	}
}
