package gsyncx_test

import (
	"context"
	"fmt"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/checkpoint"
	"github.com/dataixcom/gsyncx/engine"
	"github.com/dataixcom/gsyncx/mapping"
	"github.com/dataixcom/gsyncx/sqlparser"
	"github.com/dataixcom/gsyncx/transform"
	"github.com/dataixcom/gsyncx/validator"
)

func ExampleNewSyncConfig() {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithBatchSize(1000),
		gsyncx.WithParallelism(4),
		gsyncx.WithCheckpoint(true, "/tmp/gsyncx/checkpoints"),
		gsyncx.WithContinueOnError(true),
		gsyncx.WithRetry(3, 0),
	)

	fmt.Printf("mode=%s batch=%d parallel=%d checkpoint=%v\n",
		cfg.SyncMode, cfg.BatchSize, cfg.Parallelism, cfg.CheckpointEnabled)
}

func ExampleWithLogger() {
	logger := gsyncx.NewNopLogger()
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithLogger(logger),
	)

	if cfg.GetLogger() == logger {
		fmt.Println("custom logger injected")
	}
}

func ExampleNewSyncLogger() {
	logger := gsyncx.NewSyncLogger()
	logger.Info("application started",
		gsyncx.F("version", "1.0.0"),
		gsyncx.F("port", 8080),
	)
}

func ExampleNewNopLogger() {
	logger := gsyncx.NewNopLogger()
	logger.Info("this message will be suppressed")
	fmt.Println("nop logger does not output")
}

func ExampleNewFuncLogger() {
	logger := gsyncx.NewFuncLogger(
		func(msg string, fields ...gsyncx.LogField) {
			fmt.Printf("[INFO] %s\n", msg)
		},
		func(msg string, fields ...gsyncx.LogField) {
			fmt.Printf("[WARN] %s\n", msg)
		},
		func(msg string, fields ...gsyncx.LogField) {
			fmt.Printf("[ERROR] %s\n", msg)
		},
		func(msg string, fields ...gsyncx.LogField) {
			fmt.Printf("[DEBUG] %s\n", msg)
		},
	)
	logger.Info("custom handler message")
}

func ExampleNewMemoryCheckpointStore() {
	store := checkpoint.NewMemoryCheckpointStore()

	cp := &gsyncx.Checkpoint{
		TableName: "users",
		FieldName: "updated_at",
	}

	_ = store.Save(context.Background(), cp)

	loaded, _ := store.Load(context.Background(), "users")
	fmt.Printf("checkpoint table=%s field=%s\n", loaded.TableName, loaded.FieldName)
}

func ExampleNewFieldMappingEngine() {
	mappings := []gsyncx.FieldMapping{
		{SourceField: "src_name", TargetField: "name", Transform: "trim"},
		{SourceField: "src_email", TargetField: "email", Transform: "to_lower"},
	}

	eng := mapping.NewFieldMappingEngineWithMappings(mappings, nil)

	records := []gsyncx.Record{
		{Data: map[string]interface{}{
			"src_name":  "  John  ",
			"src_email": "JOHN@EXAMPLE.COM",
		}},
	}

	result, _, _ := eng.Map(records)
	fmt.Printf("name=%s email=%s\n",
		result[0].Data["name"], result[0].Data["email"])
}

func ExampleNewSQLParser() {
	parser := sqlparser.NewSQLParser()

	parsed, err := parser.Parse("SELECT id, name FROM users WHERE age > 18 ORDER BY id LIMIT 100")
	if err != nil {
		fmt.Printf("parse error: %v\n", err)
		return
	}

	fmt.Printf("tables=%v fields=%v\n", parsed.Tables, parsed.Fields)
}

func ExampleNewDataValidator() {
	v := validator.NewDataValidator(nil)
	v.AddFieldValidator(validator.FieldValidator{
		FieldName: "email",
		Required:  true,
		Type:      "string",
	})
	v.AddFieldValidator(validator.FieldValidator{
		FieldName: "age",
		Type:      "int",
	})

	record := gsyncx.Record{Data: map[string]interface{}{
		"email": "test@example.com",
		"age":   25,
	}}

	if err := v.ValidateRecord(record); err != nil {
		fmt.Printf("validation failed: %v\n", err)
	} else {
		fmt.Println("validation passed")
	}
}

func ExampleNewConfigValidator() {
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithSyncMode(gsyncx.SyncModeFull),
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
			TableName: "source_table",
		}),
		gsyncx.WithWriterConfig(gsyncx.WriterConfig{
			TableName: "target_table",
			WriteMode: gsyncx.WriteModeUpsert,
		}),
	)

	v := engine.NewConfigValidator(cfg, nil)
	if err := v.Validate(); err != nil {
		fmt.Printf("config validation failed: %v\n", err)
	} else {
		fmt.Println("config validation passed")
	}
}

func ExampleDSNConfig() {
	dsn := gsyncx.DSNConfig{
		DBType:   gsyncx.DBMySQL,
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "password",
		Schema:   "mydb",
		MaxIdle:  5,
		MaxOpen:  20,
	}

	fmt.Println(dsn.BuildDSN())
}

func ExampleWhereClauseBuilder() {
	w := gsyncx.NewWhereClauseBuilder()
	w.AndEq("status", "active")
	w.AndGt("age", 18)
	w.AndBetween("created_at", "2024-01-01", "2024-12-31")

	sql, args, _ := w.Build(gsyncx.DBMySQL)
	fmt.Printf("SQL: %s\nArgs: %v\n", sql, args)
}

func ExampleField() {
	f := gsyncx.Field{
		FieldName: "user_name",
		AliasName: "name",
	}

	fmt.Printf("field=%s alias=%s\n", f.GetFieldName(), f.GetAliasName())
}

func ExampleFormatFieldName() {
	fmt.Println(gsyncx.FormatFieldName("name", gsyncx.DBMySQL))
	fmt.Println(gsyncx.FormatFieldName("name", gsyncx.DBPostgres))
	fmt.Println(gsyncx.FormatFieldName("name", gsyncx.DBOracle))
}

func ExampleNewScriptManager() {
	mgr := transform.NewScriptManager(nil)

	script := `
function transform(table)
    local result = {}
    for k, v in pairs(table) do
        result[k] = string.upper(v)
    end
    return result
end
`

	_ = mgr.LoadScript("uppercase", script, transform.ScriptLangLua)
	fmt.Printf("scripts=%v\n", mgr.ListScripts())
}

func ExampleSetDefaultLogger() {
	gsyncx.SetDefaultLogger(gsyncx.NewProductionSyncLogger())
	fmt.Println("default logger set to production mode")
}

func ExampleResolveLogger() {
	customLogger := gsyncx.NewNopLogger()
	logger := gsyncx.ResolveLogger(nil, customLogger)
	_ = logger
	fmt.Println("resolved to first non-nil logger")
}
