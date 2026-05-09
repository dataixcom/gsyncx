package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/task"
)

func main() {
	logger := gsyncx.NewSyncLogger()

	fmt.Println("========================================")
	fmt.Println("  gsyncx Task Module Examples")
	fmt.Println("========================================")

	example1LoadFromFile(logger)
	example2ParseFromBytes(logger)
	example3ValidateConfig()
	example4ExecutorWithHooks(logger)
	example5CustomFactory(logger)
	example6ConvenienceFunctions(logger)
	example7StatusMonitoring()
	example8GenerateConfig()
}

func example1LoadFromFile(logger gsyncx.SyncLogger) {
	fmt.Println("\n--- Example 1: Load Config from File ---")

	configPath := findConfigFile("mysql_full_sync.json")
	if configPath == "" {
		fmt.Println("  Config file not found, skipping")
		return
	}

	cfg, err := task.LoadTaskConfig(configPath)
	if err != nil {
		log.Printf("  Failed to load config: %v", err)
		return
	}

	fmt.Printf("  Job ID:   %s\n", cfg.JobID)
	fmt.Printf("  Job Name: %s\n", cfg.JobName)
	fmt.Printf("  Reader:   %s -> %s\n", cfg.Reader.Type, cfg.Reader.TableName)
	fmt.Printf("  Writer:   %s -> %s\n", cfg.Writer.Type, cfg.Writer.TableName)
	if cfg.Mapping != nil {
		fmt.Printf("  Mappings: %d field mappings\n", len(cfg.Mapping.Mappings))
		for _, m := range cfg.Mapping.Mappings {
			fmt.Printf("    %s -> %s", m.SourceField, m.TargetField)
			if m.Transform != "" {
				fmt.Printf(" (transform: %s)", m.Transform)
			}
			fmt.Println()
		}
	}
	if cfg.Setting != nil {
		fmt.Printf("  Setting:  mode=%s batch=%d\n", cfg.Setting.SyncMode, cfg.Setting.BatchSize)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Printf("  Validation: FAILED - %v\n", err)
	} else {
		fmt.Println("  Validation: PASSED")
	}
}

func example2ParseFromBytes(logger gsyncx.SyncLogger) {
	fmt.Println("\n--- Example 2: Parse Config from JSON Bytes ---")

	configJSON := `{
		"job_id": "inline-sync-001",
		"job_name": "Inline Config Sync",
		"reader": {
			"type": "database",
			"dsn_config": {
				"db_type": "mysql",
				"host": "localhost",
				"port": 3306,
				"user": "root",
				"password": "password",
				"schema": "source_db"
			},
			"table_name": "products",
			"primary_key": {"field_name": "id"}
		},
		"writer": {
			"type": "database",
			"dsn_config": {
				"db_type": "mysql",
				"host": "localhost",
				"port": 3306,
				"user": "root",
				"password": "password",
				"schema": "target_db"
			},
			"table_name": "products",
			"write_mode": "upsert",
			"primary_key": {"field_name": "id"}
		},
		"mapping": {
			"mappings": [
				{"source_field": "product_name", "target_field": "name", "transform": "trim"},
				{"source_field": "price", "target_field": "unit_price", "transform": "tostring"}
			],
			"auto_mapping": true
		},
		"setting": {
			"sync_mode": "full",
			"batch_size": 500
		}
	}`

	cfg, err := task.ParseTaskConfig([]byte(configJSON))
	if err != nil {
		log.Printf("  Failed to parse config: %v", err)
		return
	}

	fmt.Printf("  Parsed: job_id=%s reader=%s writer=%s\n",
		cfg.JobID, cfg.Reader.Type, cfg.Writer.Type)

	if err := cfg.Validate(); err != nil {
		fmt.Printf("  Validation: FAILED - %v\n", err)
	} else {
		fmt.Println("  Validation: PASSED")
	}

	exported, _ := cfg.ToJSON()
	fmt.Printf("  Exported config size: %d bytes\n", len(exported))
}

func example3ValidateConfig() {
	fmt.Println("\n--- Example 3: Config Validation ---")

	validConfig := `{
		"job_id": "valid-001",
		"job_name": "Valid Config",
		"reader": {
			"type": "database",
			"dsn_config": {"db_type": "mysql", "host": "localhost"},
			"table_name": "users"
		},
		"writer": {
			"type": "database",
			"dsn_config": {"db_type": "mysql", "host": "localhost"},
			"table_name": "users"
		}
	}`

	invalidConfigs := map[string]string{
		"missing_job_id": `{
			"job_name": "No ID",
			"reader": {"type": "database"},
			"writer": {"type": "database"}
		}`,
		"missing_reader_type": `{
			"job_id": "test-002",
			"job_name": "No Reader Type",
			"reader": {},
			"writer": {"type": "database", "table_name": "t", "dsn_config": {"db_type": "mysql", "host": "localhost"}}
		}`,
		"missing_writer_table": `{
			"job_id": "test-003",
			"job_name": "No Writer Table",
			"reader": {"type": "database", "table_name": "t", "dsn_config": {"db_type": "mysql", "host": "localhost"}},
			"writer": {"type": "database", "dsn_config": {"db_type": "mysql", "host": "localhost"}}
		}`,
		"empty_transform": `{
			"job_id": "test-004",
			"job_name": "Empty Transform",
			"reader": {"type": "database", "table_name": "t", "dsn_config": {"db_type": "mysql", "host": "localhost"}},
			"writer": {"type": "database", "table_name": "t", "dsn_config": {"db_type": "mysql", "host": "localhost"}},
			"transform": {}
		}`,
		"invalid_mapping": `{
			"job_id": "test-005",
			"job_name": "Invalid Mapping",
			"reader": {"type": "database", "table_name": "t", "dsn_config": {"db_type": "mysql", "host": "localhost"}},
			"writer": {"type": "database", "table_name": "t", "dsn_config": {"db_type": "mysql", "host": "localhost"}},
			"mapping": {"mappings": [{"source_field": "", "target_field": "b"}]}
		}`,
	}

	cfg, _ := task.ParseTaskConfig([]byte(validConfig))
	if err := cfg.Validate(); err != nil {
		fmt.Printf("  Valid config: UNEXPECTED ERROR - %v\n", err)
	} else {
		fmt.Println("  Valid config: PASSED")
	}

	for name, jsonStr := range invalidConfigs {
		cfg, _ := task.ParseTaskConfig([]byte(jsonStr))
		err := cfg.Validate()
		if err != nil {
			errMsg := err.Error()
			if len(errMsg) > 60 {
				errMsg = errMsg[:60] + "..."
			}
			fmt.Printf("  %-20s: correctly rejected (%s)\n", name, errMsg)
		} else {
			fmt.Printf("  %-20s: should have been rejected!\n", name)
		}
	}
}

func example4ExecutorWithHooks(logger gsyncx.SyncLogger) {
	fmt.Println("\n--- Example 4: TaskExecutor with Hooks ---")

	cfg := &task.TaskConfig{
		JobID:   "hook-demo-001",
		JobName: "Hook Demo",
		Reader:  task.ReaderConfig{Type: "mock"},
		Writer:  task.WriterConfig{Type: "mock"},
	}

	readCount := 0
	writeCount := 0

	executor := task.NewTaskExecutor(cfg,
		task.WithTaskLogger(logger),
		WithMockReaderFactory(),
		WithMockWriterFactory(),
		task.WithTaskHook(gsyncx.HookAfterRead, func(ctx context.Context, hctx *gsyncx.HookContext) error {
			readCount += len(hctx.Records)
			fmt.Printf("  [Hook] AfterRead: %d records\n", len(hctx.Records))
			return nil
		}),
		task.WithTaskHook(gsyncx.HookAfterWrite, func(ctx context.Context, hctx *gsyncx.HookContext) error {
			writeCount += len(hctx.Records)
			fmt.Printf("  [Hook] AfterWrite: %d records\n", len(hctx.Records))
			return nil
		}),
		task.WithTaskHook(gsyncx.HookOnComplete, func(ctx context.Context, hctx *gsyncx.HookContext) error {
			fmt.Printf("  [Hook] OnComplete: status=%s\n", hctx.Progress.Status)
			return nil
		}),
	)

	result, err := executor.Execute(context.Background())
	if err != nil {
		fmt.Printf("  Execute error: %v\n", err)
		return
	}

	fmt.Printf("  Result: status=%s duration=%v read=%d write=%d\n",
		result.Status, result.Duration, readCount, writeCount)
}

func example5CustomFactory(logger gsyncx.SyncLogger) {
	fmt.Println("\n--- Example 5: Custom Factory Registration ---")

	cfg := &task.TaskConfig{
		JobID:   "custom-factory-001",
		JobName: "Custom Factory Demo",
		Reader:  task.ReaderConfig{Type: "csv"},
		Writer:  task.WriterConfig{Type: "console"},
	}

	executor := task.NewTaskExecutor(cfg, task.WithTaskLogger(logger))

	executor.RegisterReader("csv", func(cfg *task.TaskConfig) (gsyncx.Reader, error) {
		fmt.Println("  Custom CSV Reader created")
		return &mockReader{}, nil
	})

	executor.RegisterWriter("console", func(cfg *task.TaskConfig) (gsyncx.Writer, error) {
		fmt.Println("  Custom Console Writer created")
		return &mockWriter{}, nil
	})

	result, err := executor.Execute(context.Background())
	if err != nil {
		fmt.Printf("  Execute error: %v\n", err)
		return
	}

	fmt.Printf("  Result: status=%s job=%s\n", result.Status, result.JobName)
}

func example6ConvenienceFunctions(logger gsyncx.SyncLogger) {
	fmt.Println("\n--- Example 6: Convenience Functions ---")

	configJSON := `{
		"job_id": "convenience-001",
		"job_name": "Convenience Function Demo",
		"reader": {"type": "mock"},
		"writer": {"type": "mock"}
	}`

	result, err := task.ExecuteTaskFromBytes(context.Background(), []byte(configJSON),
		task.WithTaskLogger(logger),
		WithMockReaderFactory(),
		WithMockWriterFactory(),
	)
	if err != nil {
		fmt.Printf("  ExecuteTaskFromBytes error: %v\n", err)
		return
	}

	fmt.Printf("  ExecuteTaskFromBytes: status=%s job=%s\n", result.Status, result.JobName)

	configPath := findConfigFile("mysql_incremental_sync.json")
	if configPath != "" {
		fmt.Printf("  ExecuteTask: config found at %s\n", configPath)
		fmt.Println("  (skipping actual execution - requires database connection)")
	} else {
		fmt.Println("  ExecuteTask: config file not found, skipping")
	}
}

func example7StatusMonitoring() {
	fmt.Println("\n--- Example 7: Status Monitoring ---")

	cfg := &task.TaskConfig{
		JobID:   "monitor-001",
		JobName: "Status Monitor Demo",
		Reader:  task.ReaderConfig{Type: "mock"},
		Writer:  task.WriterConfig{Type: "mock"},
	}

	executor := task.NewTaskExecutor(cfg,
		WithMockReaderFactory(),
		WithMockWriterFactory(),
	)

	fmt.Printf("  Before execute: status=%s\n", executor.GetStatus())

	result, _ := executor.Execute(context.Background())

	fmt.Printf("  After execute:  status=%s\n", executor.GetStatus())

	fmt.Printf("  Result details:\n")
	fmt.Printf("    JobID:    %s\n", result.JobID)
	fmt.Printf("    JobName:  %s\n", result.JobName)
	fmt.Printf("    Status:   %s\n", result.Status)
	fmt.Printf("    Start:    %s\n", result.StartTime.Format(time.RFC3339))
	fmt.Printf("    End:      %s\n", result.EndTime.Format(time.RFC3339))
	fmt.Printf("    Duration: %v\n", result.Duration)

	executor2 := task.NewTaskExecutor(cfg,
		WithMockReaderFactory(),
		WithMockWriterFactory(),
	)
	go func() {
		time.Sleep(50 * time.Millisecond)
		fmt.Printf("  Mid-execution status: %s\n", executor2.GetStatus())
	}()
	executor2.Execute(context.Background())
	fmt.Printf("  Final status: %s\n", executor2.GetStatus())

	executor3 := task.NewTaskExecutor(cfg)
	executor3.Stop()
	fmt.Printf("  After Stop(): status=%s\n", executor3.GetStatus())
}

func example8GenerateConfig() {
	fmt.Println("\n--- Example 8: Generate Config Programmatically ---")

	cfg := &task.TaskConfig{
		JobID:   "generated-001",
		JobName: "Generated Config",
		Version: "1.0.0",
		Reader: task.ReaderConfig{
			Type:      "database",
			TableName: "orders",
			DSNConfig: &task.DSNConfig{
				DBType:   "mysql",
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "password",
				Schema:   "source_db",
			},
			PrimaryKey: &task.FieldConfig{FieldName: "id"},
		},
		Writer: task.WriterConfig{
			Type:      "database",
			TableName: "orders_archive",
			WriteMode: "upsert",
			DSNConfig: &task.DSNConfig{
				DBType:   "mysql",
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "password",
				Schema:   "archive_db",
			},
			PrimaryKey: &task.FieldConfig{FieldName: "id"},
			BatchSize:  500,
		},
		Mapping: &task.MappingConfig{
			Mappings: []task.FieldMappingConfig{
				{SourceField: "order_no", TargetField: "archive_no", Required: true},
				{SourceField: "total", TargetField: "amount", Transform: "tostring"},
				{SourceField: "status", TargetField: "order_status", Default: "archived"},
			},
			DefaultValues: map[string]interface{}{
				"archived_at": "NOW()",
			},
			AutoMapping:   true,
			IgnoreMissing: true,
		},
		Setting: &task.SettingConfig{
			SyncMode:         gsyncx.SyncModeFull,
			BatchSize:        1000,
			Parallelism:      4,
			RetryMaxAttempts: 3,
			RetryDelay:       5 * time.Second,
			ContinueOnError:  true,
			IntegrityCheck:   "count",
		},
		Metadata: map[string]string{
			"author":  "gsyncx",
			"purpose": "archive",
		},
	}

	data, err := cfg.ToJSON()
	if err != nil {
		fmt.Printf("  Failed to generate JSON: %v\n", err)
		return
	}

	fmt.Println("  Generated JSON config:")
	var pretty map[string]interface{}
	json.Unmarshal(data, &pretty)
	prettyData, _ := json.MarshalIndent(pretty, "  ", "  ")
	fmt.Printf("  %s\n", string(prettyData[:min(len(prettyData), 500)]))
	if len(prettyData) > 500 {
		fmt.Println("  ... (truncated)")
	}

	if err := cfg.Validate(); err != nil {
		fmt.Printf("  Validation: FAILED - %v\n", err)
	} else {
		fmt.Println("  Validation: PASSED")
	}
}

func findConfigFile(name string) string {
	_, thisFile, _, _ := runtime.Caller(0)
	examplesDir := filepath.Dir(filepath.Dir(thisFile))
	configPath := filepath.Join(examplesDir, "task_configs", name)
	if _, err := os.Stat(configPath); err == nil {
		return configPath
	}
	return ""
}

func WithMockReaderFactory() task.TaskExecutorOption {
	return task.WithReaderFactory("mock", func(cfg *task.TaskConfig) (gsyncx.Reader, error) {
		return &mockReader{}, nil
	})
}

func WithMockWriterFactory() task.TaskExecutorOption {
	return task.WithWriterFactory("mock", func(cfg *task.TaskConfig) (gsyncx.Writer, error) {
		return &mockWriter{}, nil
	})
}

type mockReader struct{}

func (r *mockReader) Read(ctx context.Context, cfg *gsyncx.SyncConfig) (<-chan []gsyncx.Record, <-chan error) {
	recordCh := make(chan []gsyncx.Record, 1)
	errCh := make(chan error, 1)
	go func() {
		defer close(recordCh)
		defer close(errCh)
		recordCh <- []gsyncx.Record{
			{Data: map[string]interface{}{"id": 1, "name": "Alice", "email": "alice@example.com"}},
			{Data: map[string]interface{}{"id": 2, "name": "Bob", "email": "bob@example.com"}},
		}
	}()
	return recordCh, errCh
}

func (r *mockReader) Count(ctx context.Context, cfg *gsyncx.SyncConfig) (int64, error) {
	return 2, nil
}

func (r *mockReader) GetSplitKeys(ctx context.Context, cfg *gsyncx.SyncConfig) ([]gsyncx.SplitKey, error) {
	return nil, nil
}

func (r *mockReader) Close() error {
	return nil
}

type mockWriter struct{}

func (w *mockWriter) Write(ctx context.Context, records []gsyncx.Record) (gsyncx.WriteResult, error) {
	return gsyncx.WriteResult{SuccessCount: int64(len(records))}, nil
}

func (w *mockWriter) WriteWithMode(ctx context.Context, records []gsyncx.Record, mode gsyncx.WriteMode) (gsyncx.WriteResult, error) {
	return gsyncx.WriteResult{SuccessCount: int64(len(records))}, nil
}

func (w *mockWriter) Flush(ctx context.Context) error { return nil }

func (w *mockWriter) Close() error { return nil }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
