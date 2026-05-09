package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/checkpoint"
	"github.com/dataixcom/gsyncx/datasource"
	"github.com/dataixcom/gsyncx/engine"
	"github.com/dataixcom/gsyncx/mapping"
	"github.com/dataixcom/gsyncx/reader"
	"github.com/dataixcom/gsyncx/writer"
)

func main() {
	logger := gsyncx.NewSyncLogger()

	fmt.Println("========================================================")
	fmt.Println("  gsyncx 数据读取-映射-写入 完整示例")
	fmt.Println("========================================================")

	example1BasicPipeline(logger)
	example2FieldMappingPipeline(logger)
	example3IncrementalPipeline(logger)
	example4TaskConfigPipeline(logger)
	example5HookMonitoringPipeline(logger)
}

func example1BasicPipeline(logger gsyncx.SyncLogger) {
	fmt.Println("\n--- 示例1: 基础数据读取-写入管道 ---")
	fmt.Println("  场景: 从源表读取全量数据，直接写入目标表")

	sourceDSN := gsyncx.DSNConfig{
		DBType:   gsyncx.DBMySQL,
		Host:     "10.1.1.100",
		Port:     33068,
		User:     "root",
		Password: "Dataix@2025",
		Schema:   "phoenix-dev",
		MaxIdle:  5,
		MaxOpen:  20,
	}
	targetDSN := gsyncx.DSNConfig{
		DBType:   gsyncx.DBMySQL,
		Host:     "10.1.1.100",
		Port:     33068,
		User:     "root",
		Password: "Dataix@2025",
		Schema:   "dx-test-go",
		MaxIdle:  5,
		MaxOpen:  20,
	}

	sourceDS, err := datasource.NewGdbxDataSource(sourceDSN)
	if err != nil {
		log.Fatalf("  创建源数据源失败: %v", err)
	}
	defer sourceDS.Close()

	targetDS, err := datasource.NewGdbxDataSource(targetDSN)
	if err != nil {
		log.Fatalf("  创建目标数据源失败: %v", err)
	}
	defer targetDS.Close()

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(1000),
		gsyncx.WithParallelism(4),
		gsyncx.WithContinueOnError(true),
		gsyncx.WithRetry(3, 5*time.Second),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
			TableName: "t_sys_user",
			PrimaryKey: &gsyncx.Field{FieldName: "id"},
			WhereClause: "status = 'active'",
		}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{
			TableName:      "t_sys_user",
			WriteMode:      gsyncx.WriteModeUpsert,
			PrimaryKey:     &gsyncx.Field{FieldName: "id"},
			UseTransaction: true,
		}),
	)

	eng, err := engine.NewSyncEngine(cfg,
		engine.WithSourceDS(sourceDS),
		engine.WithTargetDS(targetDS),
		engine.WithReader(reader.NewDatabaseReader(sourceDS, logger)),
		engine.WithWriter(writer.NewDatabaseWriter(targetDS, logger)),
		engine.WithLogger(logger),
	)
	if err != nil {
		log.Fatalf("  创建同步引擎失败: %v", err)
	}

	result, err := eng.Run(context.Background())
	if err != nil {
		log.Fatalf("  同步执行失败: %v", err)
	}

	printResult("基础管道", result)
}

func example2FieldMappingPipeline(logger gsyncx.SyncLogger) {
	fmt.Println("\n--- 示例2: 字段映射数据管道 ---")
	fmt.Println("  场景: 从源表读取指定字段，按映射关系写入目标表的不同字段")

	sourceDSN := gsyncx.DSNConfig{
		DBType:   gsyncx.DBMySQL,
		Host:     "10.1.1.100",
		Port:     33068,
		User:     "root",
		Password: "Dataix@2025",
		Schema:   "phoenix-dev",
		MaxIdle:  5,
		MaxOpen:  20,
	}
	targetDSN := gsyncx.DSNConfig{
		DBType:   gsyncx.DBMySQL,
		Host:     "10.1.1.100",
		Port:     33068,
		User:     "root",
		Password: "Dataix@2025",
		Schema:   "dx-test-go",
		MaxIdle:  5,
		MaxOpen:  20,
	}

	sourceDS, err := datasource.NewGdbxDataSource(sourceDSN)
	if err != nil {
		log.Fatalf("  创建源数据源失败: %v", err)
	}
	defer sourceDS.Close()

	targetDS, err := datasource.NewGdbxDataSource(targetDSN)
	if err != nil {
		log.Fatalf("  创建目标数据源失败: %v", err)
	}
	defer targetDS.Close()

	mappings := []gsyncx.FieldMapping{
		{SourceField: "id", TargetField: "id", Required: true},
		{SourceField: "tid", TargetField: "tid"},
		{SourceField: "account", TargetField: "account"},
		{SourceField: "password", TargetField: "password"},
		{SourceField: "nick_name", TargetField: "display_name"},
		{SourceField: "email", TargetField: "contact_email", Transform: "trim"},
		{SourceField: "phone", TargetField: "contact_phone"},
		{SourceField: "create_time", TargetField: "registered_at"},
	}

	fieldMapper := mapping.NewFieldMappingEngine(gsyncx.MappingConfig{
		Mappings:      mappings,
		AutoMapping:   true,
		IgnoreMissing: true,
		DefaultValues: map[string]interface{}{
			"sync_source": "phoenix-dev",
			"synced_at":   "NOW()",
		},
	}, logger)

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(500),
		gsyncx.WithParallelism(2),
		gsyncx.WithRetry(3, time.Second),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
			TableName:   "t_sys_user",
			PrimaryKey:  &gsyncx.Field{FieldName: "id"},
			WhereClause: "status = 'active'",
			Fields: []gsyncx.Field{
				{FieldName: "id"},
				{FieldName: "tid"},
				{FieldName: "account"},
				{FieldName: "password"},
				{FieldName: "nick_name"},
				{FieldName: "email"},
				{FieldName: "phone"},
				{FieldName: "create_time"},
			},
		}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{
			TableName:      "t_sys_user",
			WriteMode:      gsyncx.WriteModeUpsert,
			PrimaryKey:     &gsyncx.Field{FieldName: "id"},
			UseTransaction: true,
		}),
		gsyncx.WithMappingConfig(gsyncx.MappingConfig{
			Mappings:      mappings,
			AutoMapping:   true,
			IgnoreMissing: true,
			DefaultValues: map[string]interface{}{
				"sync_source": "phoenix-dev",
				"synced_at":   "NOW()",
			},
		}),
		gsyncx.WithAutoMapping(true),
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
		log.Fatalf("  创建同步引擎失败: %v", err)
	}

	result, err := eng.Run(context.Background())
	if err != nil {
		log.Fatalf("  同步执行失败: %v", err)
	}

	printResult("字段映射管道", result)
}

func example3IncrementalPipeline(logger gsyncx.SyncLogger) {
	fmt.Println("\n--- 示例3: 增量同步数据管道 ---")
	fmt.Println("  场景: 基于时间戳字段增量读取，带断点续传")

	sourceDSN := gsyncx.DSNConfig{
		DBType:   gsyncx.DBMySQL,
		Host:     "10.1.1.100",
		Port:     33068,
		User:     "root",
		Password: "Dataix@2025",
		Schema:   "phoenix-dev",
		MaxIdle:  5,
		MaxOpen:  20,
	}
	targetDSN := gsyncx.DSNConfig{
		DBType:   gsyncx.DBMySQL,
		Host:     "10.1.1.100",
		Port:     33068,
		User:     "root",
		Password: "Dataix@2025",
		Schema:   "dx-test-go",
		MaxIdle:  5,
		MaxOpen:  20,
	}

	sourceDS, err := datasource.NewGdbxDataSource(sourceDSN)
	if err != nil {
		log.Fatalf("  创建源数据源失败: %v", err)
	}
	defer sourceDS.Close()

	targetDS, err := datasource.NewGdbxDataSource(targetDSN)
	if err != nil {
		log.Fatalf("  创建目标数据源失败: %v", err)
	}
	defer targetDS.Close()

	cpStore := checkpoint.NewMemoryCheckpointStore()

	mappings := []gsyncx.FieldMapping{
		{SourceField: "id", TargetField: "id", Required: true},
		{SourceField: "account", TargetField: "account"},
		{SourceField: "nick_name", TargetField: "display_name"},
		{SourceField: "update_time", TargetField: "last_modified"},
	}

	fieldMapper := mapping.NewFieldMappingEngineWithMappings(mappings, logger)

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeIncremental),
		gsyncx.WithBatchSize(500),
		gsyncx.WithParallelism(2),
		gsyncx.WithRetry(3, 3*time.Second),
		gsyncx.WithContinueOnError(true),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
			TableName:  "t_sys_user",
			PrimaryKey: &gsyncx.Field{FieldName: "id"},
		}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{
			TableName:      "t_sys_user",
			WriteMode:      gsyncx.WriteModeUpsert,
			PrimaryKey:     &gsyncx.Field{FieldName: "id"},
			UseTransaction: true,
		}),
		gsyncx.WithIncrementalField(
			&gsyncx.Field{FieldName: "update_time"},
			gsyncx.StrategyTimestamp,
		),
		gsyncx.WithCheckpoint(true, ""),
		gsyncx.WithIntegrityCheck(gsyncx.IntegrityCheckCount),
	)

	eng, err := engine.NewSyncEngine(cfg,
		engine.WithSourceDS(sourceDS),
		engine.WithTargetDS(targetDS),
		engine.WithReader(reader.NewDatabaseReader(sourceDS, logger)),
		engine.WithWriter(writer.NewDatabaseWriter(targetDS, logger)),
		engine.WithMapper(fieldMapper),
		engine.WithCheckpointStore(cpStore),
		engine.WithLogger(logger),
	)
	if err != nil {
		log.Fatalf("  创建同步引擎失败: %v", err)
	}

	result, err := eng.Run(context.Background())
	if err != nil {
		log.Fatalf("  同步执行失败: %v", err)
	}

	printResult("增量同步管道", result)
}

func example4TaskConfigPipeline(logger gsyncx.SyncLogger) {
	fmt.Println("\n--- 示例4: 基于JSON配置文件的数据管道 ---")
	fmt.Println("  场景: 从JSON配置文件加载任务定义，执行读取-映射-写入")

	configPath := findConfigFile("mysql_full_sync_user.json")
	if configPath == "" {
		fmt.Println("  配置文件未找到，跳过此示例")
		fmt.Println("  提示: 请确保 examples/task_configs/mysql_full_sync_user.json 存在")
		return
	}

	fmt.Printf("  配置文件: %s\n", configPath)

	fmt.Println("  (跳过实际执行 - 需要数据库连接)")
	fmt.Println("  配置文件定义了完整的 reader -> mapping -> writer 管道:")
	fmt.Println("    Reader:  t_sys_user @ phoenix-dev (where status='active')")
	fmt.Println("    Mapping: id->id, tid->tid, account->account, password->password")
	fmt.Println("    Writer:  t_sys_user @ dx-test-go (upsert mode)")
	fmt.Println("    Setting: full sync, batch=1000, parallelism=4, integrity=count")
}

func example5HookMonitoringPipeline(logger gsyncx.SyncLogger) {
	fmt.Println("\n--- 示例5: 带Hook监控的数据管道 ---")
	fmt.Println("  场景: 在管道各阶段插入Hook，实时监控数据流转")

	sourceDSN := gsyncx.DSNConfig{
		DBType:   gsyncx.DBMySQL,
		Host:     "10.1.1.100",
		Port:     33068,
		User:     "root",
		Password: "Dataix@2025",
		Schema:   "phoenix-dev",
		MaxIdle:  5,
		MaxOpen:  20,
	}
	targetDSN := gsyncx.DSNConfig{
		DBType:   gsyncx.DBMySQL,
		Host:     "10.1.1.100",
		Port:     33068,
		User:     "root",
		Password: "Dataix@2025",
		Schema:   "dx-test-go",
		MaxIdle:  5,
		MaxOpen:  20,
	}

	sourceDS, err := datasource.NewGdbxDataSource(sourceDSN)
	if err != nil {
		log.Fatalf("  创建源数据源失败: %v", err)
	}
	defer sourceDS.Close()

	targetDS, err := datasource.NewGdbxDataSource(targetDSN)
	if err != nil {
		log.Fatalf("  创建目标数据源失败: %v", err)
	}
	defer targetDS.Close()

	var totalRead, totalMapped, totalWritten int64

	mappings := []gsyncx.FieldMapping{
		{SourceField: "id", TargetField: "id", Required: true},
		{SourceField: "account", TargetField: "account"},
		{SourceField: "password", TargetField: "password"},
	}

	fieldMapper := mapping.NewFieldMappingEngineWithMappings(mappings, logger)

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(1000),
		gsyncx.WithParallelism(4),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
			TableName:   "t_sys_user",
			PrimaryKey:  &gsyncx.Field{FieldName: "id"},
			WhereClause: "status = 'active'",
		}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{
			TableName:      "t_sys_user",
			WriteMode:      gsyncx.WriteModeUpsert,
			PrimaryKey:     &gsyncx.Field{FieldName: "id"},
			UseTransaction: true,
		}),
	)

	eng, err := engine.NewSyncEngine(cfg,
		engine.WithSourceDS(sourceDS),
		engine.WithTargetDS(targetDS),
		engine.WithReader(reader.NewDatabaseReader(sourceDS, logger)),
		engine.WithWriter(writer.NewDatabaseWriter(targetDS, logger)),
		engine.WithMapper(fieldMapper),
		engine.WithLogger(logger),
		engine.WithHook(gsyncx.HookAfterRead, func(ctx context.Context, hctx *gsyncx.HookContext) error {
			totalRead += int64(len(hctx.Records))
			fmt.Printf("  [Hook] 读取完成: %d 条记录 (累计: %d)\n", len(hctx.Records), totalRead)
			return nil
		}),
		engine.WithHook(gsyncx.HookAfterMap, func(ctx context.Context, hctx *gsyncx.HookContext) error {
			totalMapped += int64(len(hctx.Records))
			fmt.Printf("  [Hook] 映射完成: %d 条记录 (累计: %d)\n", len(hctx.Records), totalMapped)
			return nil
		}),
		engine.WithHook(gsyncx.HookAfterWrite, func(ctx context.Context, hctx *gsyncx.HookContext) error {
			totalWritten += int64(len(hctx.Records))
			fmt.Printf("  [Hook] 写入完成: %d 条记录 (累计: %d)\n", len(hctx.Records), totalWritten)
			return nil
		}),
		engine.WithHook(gsyncx.HookOnComplete, func(ctx context.Context, hctx *gsyncx.HookContext) error {
			fmt.Printf("  [Hook] 同步完成: status=%s\n", hctx.Progress.Status)
			return nil
		}),
	)
	if err != nil {
		log.Fatalf("  创建同步引擎失败: %v", err)
	}

	result, err := eng.Run(context.Background())
	if err != nil {
		log.Fatalf("  同步执行失败: %v", err)
	}

	printResult("Hook监控管道", result)
}

func printResult(name string, result *gsyncx.SyncResult) {
	fmt.Printf("\n  [%s] 同步结果:\n", name)
	fmt.Printf("    状态:     %s\n", result.Status)
	fmt.Printf("    读取总数: %d\n", result.TotalRead)
	fmt.Printf("    写入成功: %d\n", result.TotalWritten)
	fmt.Printf("    写入失败: %d\n", result.TotalFailed)
	fmt.Printf("    跳过数量: %d\n", result.TotalSkipped)
	fmt.Printf("    耗时:     %v\n", result.Duration)
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
