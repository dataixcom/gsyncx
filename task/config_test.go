package task

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dataixcom/gsyncx"
)

func TestParseTaskConfig(t *testing.T) {
	configJSON := `{
		"job_id": "test-001",
		"job_name": "Test Job",
		"reader": {
			"type": "database",
			"dsn_config": {
				"db_type": "mysql",
				"host": "localhost",
				"port": 3306,
				"user": "root",
				"password": "pass",
				"schema": "test_db"
			},
			"table_name": "users"
		},
		"writer": {
			"type": "database",
			"dsn_config": {
				"db_type": "mysql",
				"host": "localhost",
				"port": 3306,
				"user": "root",
				"password": "pass",
				"schema": "target_db"
			},
			"table_name": "users_copy"
		}
	}`

	cfg, err := ParseTaskConfig([]byte(configJSON))
	if err != nil {
		t.Fatalf("failed to parse config: %v", err)
	}
	if cfg.JobID != "test-001" {
		t.Errorf("expected job_id test-001, got %s", cfg.JobID)
	}
	if cfg.JobName != "Test Job" {
		t.Errorf("expected job_name Test Job, got %s", cfg.JobName)
	}
	if cfg.Reader.Type != "database" {
		t.Errorf("expected reader type database, got %s", cfg.Reader.Type)
	}
	if cfg.Writer.Type != "database" {
		t.Errorf("expected writer type database, got %s", cfg.Writer.Type)
	}
}

func TestParseTaskConfig_InvalidJSON(t *testing.T) {
	_, err := ParseTaskConfig([]byte(`{invalid json`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestParseTaskConfig_EmptyJSON(t *testing.T) {
	cfg, err := ParseTaskConfig([]byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.JobID != "" {
		t.Error("expected empty job_id")
	}
}

func TestParseTaskConfig_WithAllSections(t *testing.T) {
	configJSON := `{
		"job_id": "full-001",
		"job_name": "Full Config Test",
		"version": "1.0.0",
		"reader": {
			"type": "database",
			"dsn_config": {
				"db_type": "mysql",
				"host": "localhost",
				"port": 3306,
				"user": "root",
				"password": "pass",
				"schema": "test_db"
			},
			"table_name": "users",
			"primary_key": {"field_name": "id"},
			"where_clause": "status = 'active'"
		},
		"transform": {
			"type": "script",
			"script": "function transform(records) return records end",
			"script_lang": "lua"
		},
		"mapping": {
			"mappings": [
				{"source_field": "user_name", "target_field": "name"},
				{"source_field": "email", "target_field": "email", "transform": "to_lower"}
			],
			"auto_mapping": true,
			"ignore_missing": true,
			"strict_mode": false
		},
		"writer": {
			"type": "database",
			"dsn_config": {
				"db_type": "mysql",
				"host": "localhost",
				"port": 3306,
				"user": "root",
				"password": "pass",
				"schema": "target_db"
			},
			"table_name": "users_copy",
			"write_mode": "upsert",
			"primary_key": {"field_name": "id"},
			"batch_size": 500
		},
		"setting": {
			"sync_mode": "full",
			"batch_size": 1000,
			"parallelism": 4,
			"retry_max_attempts": 3,
			"continue_on_error": true,
			"integrity_check": "count"
		},
		"metadata": {
			"author": "test",
			"env": "dev"
		}
	}`

	cfg, err := ParseTaskConfig([]byte(configJSON))
	if err != nil {
		t.Fatalf("failed to parse config: %v", err)
	}
	if cfg.Transform == nil {
		t.Error("expected non-nil transform")
	}
	if cfg.Mapping == nil {
		t.Error("expected non-nil mapping")
	}
	if cfg.Setting == nil {
		t.Error("expected non-nil setting")
	}
	if len(cfg.Metadata) != 2 {
		t.Errorf("expected 2 metadata entries, got %d", len(cfg.Metadata))
	}
	if cfg.Reader.PrimaryKey == nil || cfg.Reader.PrimaryKey.FieldName != "id" {
		t.Error("expected primary key id")
	}
	if cfg.Writer.WriteMode != "upsert" {
		t.Errorf("expected write_mode upsert, got %s", cfg.Writer.WriteMode)
	}
}

func TestParseTaskConfig_RedisStream(t *testing.T) {
	configJSON := `{
		"job_id": "redis-001",
		"job_name": "Redis Stream Test",
		"reader": {
			"type": "redis_stream",
			"redis": {
				"addr": "localhost:6379",
				"stream": "test-stream",
				"consumer_group": "test-group",
				"auto_create": true
			}
		},
		"transform": {
			"type": "redis_message",
			"field_mapping": {
				"user_id": "id",
				"event": "type"
			}
		},
		"writer": {
			"type": "database",
			"dsn_config": {
				"db_type": "mysql",
				"host": "localhost",
				"port": 3306,
				"user": "root",
				"schema": "test_db"
			},
			"table_name": "events"
		}
	}`

	cfg, err := ParseTaskConfig([]byte(configJSON))
	if err != nil {
		t.Fatalf("failed to parse config: %v", err)
	}
	if cfg.Reader.Type != "redis_stream" {
		t.Errorf("expected redis_stream, got %s", cfg.Reader.Type)
	}
	if cfg.Reader.Redis == nil {
		t.Error("expected non-nil redis config")
	}
	if cfg.Reader.Redis.Stream != "test-stream" {
		t.Errorf("expected stream test-stream, got %s", cfg.Reader.Redis.Stream)
	}
	if cfg.Transform.Type != "redis_message" {
		t.Errorf("expected redis_message, got %s", cfg.Transform.Type)
	}
	if len(cfg.Transform.FieldMapping) != 2 {
		t.Errorf("expected 2 field mappings, got %d", len(cfg.Transform.FieldMapping))
	}
}

func TestLoadTaskConfig(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "test_config.json")

	cfg := &TaskConfig{
		JobID:   "load-test-001",
		JobName: "Load Test",
		Reader:  ReaderConfig{Type: "database", TableName: "users"},
		Writer:  WriterConfig{Type: "database", TableName: "users_copy"},
	}

	data, _ := json.MarshalIndent(cfg, "", "  ")
	os.WriteFile(configPath, data, 0644)

	loaded, err := LoadTaskConfig(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}
	if loaded.JobID != "load-test-001" {
		t.Errorf("expected job_id load-test-001, got %s", loaded.JobID)
	}
}

func TestLoadTaskConfig_FileNotFound(t *testing.T) {
	_, err := LoadTaskConfig("nonexistent.json")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestTaskConfig_ToJSON(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "json-test-001",
		JobName: "JSON Test",
		Reader:  ReaderConfig{Type: "database", TableName: "users"},
		Writer:  WriterConfig{Type: "database", TableName: "users_copy"},
	}

	data, err := cfg.ToJSON()
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	var parsed TaskConfig
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}
	if parsed.JobID != "json-test-001" {
		t.Errorf("expected job_id json-test-001, got %s", parsed.JobID)
	}
}

func TestTaskConfig_Validate_Valid(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "valid-001",
		JobName: "Valid Config",
		Reader: ReaderConfig{
			Type:      "database",
			TableName: "users",
			DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"},
		},
		Writer: WriterConfig{
			Type:      "database",
			TableName: "users_copy",
			DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"},
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestTaskConfig_Validate_MissingJobID(t *testing.T) {
	cfg := &TaskConfig{
		JobName: "No ID",
		Reader:  ReaderConfig{Type: "database"},
		Writer:  WriterConfig{Type: "database"},
	}

	if err := cfg.Validate(); err == nil {
		t.Error("expected error for missing job_id")
	}
}

func TestTaskConfig_Validate_MissingJobName(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "test-001",
		Reader:  ReaderConfig{Type: "database"},
		Writer:  WriterConfig{Type: "database"},
	}

	if err := cfg.Validate(); err == nil {
		t.Error("expected error for missing job_name")
	}
}

func TestTaskConfig_Validate_InvalidReader(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "test-001",
		JobName: "Test",
		Reader:  ReaderConfig{Type: ""},
		Writer:  WriterConfig{Type: "database", TableName: "t", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
	}

	if err := cfg.Validate(); err == nil {
		t.Error("expected error for empty reader type")
	}
}

func TestTaskConfig_Validate_InvalidWriter(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "test-001",
		JobName: "Test",
		Reader:  ReaderConfig{Type: "database", TableName: "t", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
		Writer:  WriterConfig{Type: ""},
	}

	if err := cfg.Validate(); err == nil {
		t.Error("expected error for empty writer type")
	}
}

func TestReaderConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  ReaderConfig
		wantErr bool
	}{
		{
			"valid database reader",
			ReaderConfig{Type: "database", TableName: "users", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
			false,
		},
		{
			"valid sql reader",
			ReaderConfig{Type: "sql", SQL: "SELECT * FROM users", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
			false,
		},
		{
			"valid redis_stream reader",
			ReaderConfig{Type: "redis_stream", Redis: &RedisReaderConfig{Stream: "test-stream"}},
			false,
		},
		{
			"missing type",
			ReaderConfig{Type: ""},
			true,
		},
		{
			"database reader missing dsn",
			ReaderConfig{Type: "database", TableName: "users"},
			true,
		},
		{
			"database reader missing host",
			ReaderConfig{Type: "database", TableName: "users", DSNConfig: &DSNConfig{DBType: "mysql"}},
			true,
		},
		{
			"database reader missing db_type",
			ReaderConfig{Type: "database", TableName: "users", DSNConfig: &DSNConfig{Host: "localhost"}},
			true,
		},
		{
			"database reader missing table and sql",
			ReaderConfig{Type: "database", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
			true,
		},
		{
			"redis_stream missing redis config",
			ReaderConfig{Type: "redis_stream"},
			true,
		},
		{
			"redis_stream missing stream",
			ReaderConfig{Type: "redis_stream", Redis: &RedisReaderConfig{}},
			true,
		},
		{
			"unknown type passes",
			ReaderConfig{Type: "custom"},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWriterConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  WriterConfig
		wantErr bool
	}{
		{
			"valid database writer",
			WriterConfig{Type: "database", TableName: "users", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
			false,
		},
		{
			"missing type",
			WriterConfig{Type: ""},
			true,
		},
		{
			"database writer missing dsn",
			WriterConfig{Type: "database", TableName: "users"},
			true,
		},
		{
			"database writer missing host",
			WriterConfig{Type: "database", TableName: "users", DSNConfig: &DSNConfig{DBType: "mysql"}},
			true,
		},
		{
			"database writer missing db_type",
			WriterConfig{Type: "database", TableName: "users", DSNConfig: &DSNConfig{Host: "localhost"}},
			true,
		},
		{
			"database writer missing table",
			WriterConfig{Type: "database", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
			true,
		},
		{
			"unknown type passes",
			WriterConfig{Type: "custom"},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTransformConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  TransformConfig
		wantErr bool
	}{
		{"with script", TransformConfig{Script: "function transform(r) return r end"}, false},
		{"with script_path", TransformConfig{ScriptPath: "/path/to/script.lua"}, false},
		{"with field_mapping", TransformConfig{FieldMapping: map[string]string{"a": "b"}}, false},
		{"empty config", TransformConfig{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMappingConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  MappingConfig
		wantErr bool
	}{
		{
			"valid mappings",
			MappingConfig{Mappings: []FieldMappingConfig{{SourceField: "a", TargetField: "b"}}},
			false,
		},
		{
			"empty source_field",
			MappingConfig{Mappings: []FieldMappingConfig{{SourceField: "", TargetField: "b"}}},
			true,
		},
		{
			"empty target_field",
			MappingConfig{Mappings: []FieldMappingConfig{{SourceField: "a", TargetField: ""}}},
			true,
		},
		{
			"empty mappings passes",
			MappingConfig{},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDSNConfig_ToGdbxDSN(t *testing.T) {
	dsn := &DSNConfig{
		DBType:   "mysql",
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "pass",
		Schema:   "test_db",
		MaxIdle:  5,
		MaxOpen:  20,
	}

	gdbxDSN := dsn.ToGdbxDSN()
	if gdbxDSN.Host != "localhost" {
		t.Errorf("expected host localhost, got %s", gdbxDSN.Host)
	}
	if gdbxDSN.Port != 3306 {
		t.Errorf("expected port 3306, got %d", gdbxDSN.Port)
	}
	if gdbxDSN.Schema != "test_db" {
		t.Errorf("expected schema test_db, got %s", gdbxDSN.Schema)
	}
}

func TestReaderConfig_ToGdbxReaderConfig(t *testing.T) {
	r := &ReaderConfig{
		TableName:   "users",
		Schema:      "public",
		SQL:         "SELECT * FROM users",
		WhereClause: "status = 'active'",
		RawFields:   []string{"id", "name"},
		DSNConfig: &DSNConfig{
			DBType: "mysql",
			Host:   "localhost",
		},
		PrimaryKey: &FieldConfig{FieldName: "id"},
		Fields:     []FieldConfig{{FieldName: "id", Alias: "user_id"}},
	}

	cfg := r.ToGdbxReaderConfig()
	if cfg.TableName != "users" {
		t.Errorf("expected table users, got %s", cfg.TableName)
	}
	if cfg.Schema != "public" {
		t.Errorf("expected schema public, got %s", cfg.Schema)
	}
	if cfg.SQL != "SELECT * FROM users" {
		t.Errorf("expected SQL, got %s", cfg.SQL)
	}
	if cfg.WhereClause != "status = 'active'" {
		t.Errorf("expected where clause, got %s", cfg.WhereClause)
	}
	if len(cfg.RawFields) != 2 {
		t.Errorf("expected 2 raw fields, got %d", len(cfg.RawFields))
	}
	if cfg.PrimaryKey == nil || cfg.PrimaryKey.FieldName != "id" {
		t.Error("expected primary key id")
	}
	if len(cfg.Fields) != 1 {
		t.Errorf("expected 1 field, got %d", len(cfg.Fields))
	}
	if cfg.Fields[0].AliasName != "user_id" {
		t.Errorf("expected alias user_id, got %s", cfg.Fields[0].AliasName)
	}
	if cfg.DSNConfig == nil || cfg.DSNConfig.Host != "localhost" {
		t.Error("expected DSN config with host localhost")
	}
}

func TestReaderConfig_ToGdbxReaderConfig_NilDSN(t *testing.T) {
	r := &ReaderConfig{TableName: "users"}
	cfg := r.ToGdbxReaderConfig()
	if cfg.DSNConfig != nil {
		t.Error("expected nil DSN config")
	}
}

func TestWriterConfig_ToGdbxWriterConfig(t *testing.T) {
	w := &WriterConfig{
		TableName:      "users_copy",
		Schema:         "public",
		WriteMode:      "insert",
		BatchSize:      500,
		UseTransaction: true,
		RawFields:      []string{"id", "name"},
		DSNConfig: &DSNConfig{
			DBType: "mysql",
			Host:   "localhost",
		},
		PrimaryKey: &FieldConfig{FieldName: "id"},
		Fields:     []FieldConfig{{FieldName: "id", Alias: "user_id"}},
	}

	cfg := w.ToGdbxWriterConfig()
	if cfg.TableName != "users_copy" {
		t.Errorf("expected table users_copy, got %s", cfg.TableName)
	}
	if cfg.WriteMode != gsyncx.WriteModeInsert {
		t.Errorf("expected insert mode, got %s", cfg.WriteMode)
	}
	if cfg.BatchSize != 500 {
		t.Errorf("expected batch size 500, got %d", cfg.BatchSize)
	}
	if !cfg.UseTransaction {
		t.Error("expected use transaction true")
	}
}

func TestWriterConfig_ToGdbxWriterConfig_DefaultWriteMode(t *testing.T) {
	w := &WriterConfig{TableName: "users"}
	cfg := w.ToGdbxWriterConfig()
	if cfg.WriteMode != gsyncx.WriteModeUpsert {
		t.Errorf("expected default upsert mode, got %s", cfg.WriteMode)
	}
}

func TestMappingConfig_ToGdbxMappingConfig(t *testing.T) {
	m := &MappingConfig{
		Mappings: []FieldMappingConfig{
			{SourceField: "a", TargetField: "b", Transform: "to_lower", Required: true},
			{SourceField: "c", TargetField: "d", Default: "N/A"},
		},
		DefaultValues:  map[string]interface{}{"key": "value"},
		IgnoreMissing:  true,
		StrictMode:     true,
		ValidateOnLoad: true,
		AutoMapping:    true,
	}

	cfg := m.ToGdbxMappingConfig()
	if len(cfg.Mappings) != 2 {
		t.Errorf("expected 2 mappings, got %d", len(cfg.Mappings))
	}
	if cfg.Mappings[0].SourceField != "a" {
		t.Errorf("expected source field a, got %s", cfg.Mappings[0].SourceField)
	}
	if cfg.Mappings[0].Transform != "to_lower" {
		t.Errorf("expected transform to_lower, got %s", cfg.Mappings[0].Transform)
	}
	if !cfg.Mappings[0].Required {
		t.Error("expected required true")
	}
	if !cfg.IgnoreMissing {
		t.Error("expected ignore missing true")
	}
	if !cfg.StrictMode {
		t.Error("expected strict mode true")
	}
	if !cfg.AutoMapping {
		t.Error("expected auto mapping true")
	}
}

func TestMappingConfig_ToGdbxMappingConfig_Empty(t *testing.T) {
	m := &MappingConfig{}
	cfg := m.ToGdbxMappingConfig()
	if len(cfg.Mappings) != 0 {
		t.Errorf("expected 0 mappings, got %d", len(cfg.Mappings))
	}
}

func TestTransformConfig_ToGdbxTransformConfig(t *testing.T) {
	tc := &TransformConfig{
		Script:     "function transform(r) return r end",
		ScriptPath: "/path/to/script.lua",
		ScriptLang: "lua",
		Timeout:    30 * time.Second,
		MaxMemory:  1024 * 1024,
		Env:        map[string]string{"key": "value"},
	}

	cfg := tc.ToGdbxTransformConfig()
	if cfg.Script != tc.Script {
		t.Errorf("expected script %s, got %s", tc.Script, cfg.Script)
	}
	if cfg.ScriptPath != tc.ScriptPath {
		t.Errorf("expected script path %s, got %s", tc.ScriptPath, cfg.ScriptPath)
	}
	if cfg.Timeout != tc.Timeout {
		t.Errorf("expected timeout %v, got %v", tc.Timeout, cfg.Timeout)
	}
}

func TestSettingConfig_ApplyToSyncConfig(t *testing.T) {
	s := &SettingConfig{
		SyncMode:         gsyncx.SyncModeFull,
		BatchSize:        1000,
		Parallelism:      4,
		RetryMaxAttempts: 3,
		RetryDelay:       5 * time.Second,
		ContinueOnError:  true,
		ErrorThreshold:   100,
		PreviewMode:      true,
		PreviewLimit:     10,
		IntegrityCheck:   "count",
		CheckpointEnabled: true,
		CheckpointPath:   "./checkpoints",
		IncrementalField: &IncrementalFieldConfig{FieldName: "updated_at", Strategy: "timestamp"},
		LastSyncTime:     "2024-01-01T00:00:00Z",
		LastSyncValue:    100,
	}

	cfg := gsyncx.NewSyncConfig()
	s.ApplyToSyncConfig(cfg)

	if cfg.SyncMode != gsyncx.SyncModeFull {
		t.Errorf("expected full mode, got %s", cfg.SyncMode)
	}
	if cfg.BatchSize != 1000 {
		t.Errorf("expected batch size 1000, got %d", cfg.BatchSize)
	}
	if cfg.Parallelism != 4 {
		t.Errorf("expected parallelism 4, got %d", cfg.Parallelism)
	}
	if !cfg.ContinueOnError {
		t.Error("expected continue on error true")
	}
	if !cfg.PreviewMode {
		t.Error("expected preview mode true")
	}
	if cfg.PreviewLimit != 10 {
		t.Errorf("expected preview limit 10, got %d", cfg.PreviewLimit)
	}
	if !cfg.CheckpointEnabled {
		t.Error("expected checkpoint enabled true")
	}
	if cfg.CheckpointPath != "./checkpoints" {
		t.Errorf("expected checkpoint path ./checkpoints, got %s", cfg.CheckpointPath)
	}
	if cfg.IncrementalField == nil || cfg.IncrementalField.FieldName != "updated_at" {
		t.Error("expected incremental field updated_at")
	}
	if cfg.LastSyncValue != 100 {
		t.Errorf("expected last sync value 100, got %v", cfg.LastSyncValue)
	}
}

func TestSettingConfig_ApplyToSyncConfig_Nil(t *testing.T) {
	cfg := gsyncx.NewSyncConfig()
	var s *SettingConfig = nil
	s.ApplyToSyncConfig(cfg)
}

func TestSettingConfig_ApplyToSyncConfig_Empty(t *testing.T) {
	s := &SettingConfig{}
	cfg := gsyncx.NewSyncConfig()
	originalMode := cfg.SyncMode
	s.ApplyToSyncConfig(cfg)
	if cfg.SyncMode != originalMode {
		t.Error("expected sync mode to remain unchanged")
	}
}

func TestSettingConfig_ApplyToSyncConfig_InvalidLastSyncTime(t *testing.T) {
	s := &SettingConfig{
		LastSyncTime: "invalid-time-format",
	}
	cfg := gsyncx.NewSyncConfig()
	s.ApplyToSyncConfig(cfg)
	if !cfg.LastSyncTime.IsZero() {
		t.Error("expected zero time for invalid format")
	}
}

func TestTaskStatus_String(t *testing.T) {
	statuses := []TaskStatus{TaskStatusPending, TaskStatusRunning, TaskStatusCompleted, TaskStatusFailed, TaskStatusCancelled}
	for _, s := range statuses {
		if string(s) == "" {
			t.Errorf("expected non-empty string for status %v", s)
		}
	}
}

func TestTaskResult_Fields(t *testing.T) {
	now := time.Now()
	result := &TaskResult{
		JobID:      "result-001",
		JobName:    "Result Test",
		Status:     TaskStatusCompleted,
		StartTime:  now,
		EndTime:    now.Add(5 * time.Second),
		Duration:   5 * time.Second,
		ConfigPath: "/path/to/config.json",
	}

	if result.JobID != "result-001" {
		t.Errorf("expected job_id result-001, got %s", result.JobID)
	}
	if result.Status != TaskStatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
	if result.Duration != 5*time.Second {
		t.Errorf("expected 5s duration, got %v", result.Duration)
	}
}

func TestFieldConfig(t *testing.T) {
	fc := FieldConfig{FieldName: "id", Alias: "user_id", Type: "int"}
	if fc.FieldName != "id" {
		t.Errorf("expected field name id, got %s", fc.FieldName)
	}
	if fc.Alias != "user_id" {
		t.Errorf("expected alias user_id, got %s", fc.Alias)
	}
}

func TestRedisReaderConfig(t *testing.T) {
	rc := RedisReaderConfig{
		Addr:          "localhost:6379",
		Stream:        "test-stream",
		ConsumerGroup: "test-group",
		AutoCreate:    true,
	}
	if rc.Addr != "localhost:6379" {
		t.Errorf("expected addr localhost:6379, got %s", rc.Addr)
	}
	if !rc.AutoCreate {
		t.Error("expected auto create true")
	}
}

func TestLoadTaskConfig_InvalidContent(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "invalid.json")
	os.WriteFile(configPath, []byte("not json"), 0644)

	_, err := LoadTaskConfig(configPath)
	if err == nil {
		t.Error("expected error for invalid JSON content")
	}
}

func TestTaskConfig_Validate_WithTransform(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "test-001",
		JobName: "Test",
		Reader:  ReaderConfig{Type: "database", TableName: "t", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
		Writer:  WriterConfig{Type: "database", TableName: "t", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
		Transform: &TransformConfig{},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for empty transform config")
	}
}

func TestTaskConfig_Validate_WithMapping(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "test-001",
		JobName: "Test",
		Reader:  ReaderConfig{Type: "database", TableName: "t", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
		Writer:  WriterConfig{Type: "database", TableName: "t", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
		Mapping: &MappingConfig{
			Mappings: []FieldMappingConfig{{SourceField: "", TargetField: "b"}},
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("expected error for empty source field in mapping")
	}
}

func TestTaskConfig_Validate_WithValidTransform(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "test-001",
		JobName: "Test",
		Reader:  ReaderConfig{Type: "database", TableName: "t", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
		Writer:  WriterConfig{Type: "database", TableName: "t", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
		Transform: &TransformConfig{Script: "function transform(r) return r end"},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestTaskConfig_Validate_WithValidMapping(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "test-001",
		JobName: "Test",
		Reader:  ReaderConfig{Type: "database", TableName: "t", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
		Writer:  WriterConfig{Type: "database", TableName: "t", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
		Mapping: &MappingConfig{
			Mappings: []FieldMappingConfig{{SourceField: "a", TargetField: "b"}},
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestNewTaskExecutor(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "executor-001",
		JobName: "Executor Test",
		Reader:  ReaderConfig{Type: "database", TableName: "users"},
		Writer:  WriterConfig{Type: "database", TableName: "users_copy"},
	}

	executor := NewTaskExecutor(cfg)
	if executor == nil {
		t.Error("expected non-nil executor")
	}
	if executor.GetStatus() != TaskStatusPending {
		t.Errorf("expected pending status, got %s", executor.GetStatus())
	}
}

func TestNewTaskExecutor_WithOptions(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "executor-002",
		JobName: "Executor Test With Options",
		Reader:  ReaderConfig{Type: "database", TableName: "users"},
		Writer:  WriterConfig{Type: "database", TableName: "users_copy"},
	}

	executor := NewTaskExecutor(cfg,
		WithTaskLogger(gsyncx.NewNopLogger()),
		WithTaskConfigPath("/path/to/config.json"),
		WithReaderFactory("custom", func(cfg *TaskConfig) (gsyncx.Reader, error) {
			return nil, nil
		}),
		WithWriterFactory("custom", func(cfg *TaskConfig) (gsyncx.Writer, error) {
			return nil, nil
		}),
		WithTransformerFactory("custom", func(cfg *TaskConfig) (gsyncx.Transformer, error) {
			return nil, nil
		}),
		WithMapperFactory("custom", func(cfg *TaskConfig) (gsyncx.Mapper, error) {
			return nil, nil
		}),
	)

	if executor == nil {
		t.Error("expected non-nil executor")
	}
	if _, ok := executor.readerFactories["custom"]; !ok {
		t.Error("expected custom reader factory")
	}
	if _, ok := executor.writerFactories["custom"]; !ok {
		t.Error("expected custom writer factory")
	}
	if _, ok := executor.transformerFactories["custom"]; !ok {
		t.Error("expected custom transformer factory")
	}
	if _, ok := executor.mapperFactories["custom"]; !ok {
		t.Error("expected custom mapper factory")
	}
}

func TestTaskExecutor_Stop(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "stop-001",
		JobName: "Stop Test",
		Reader:  ReaderConfig{Type: "database", TableName: "users"},
		Writer:  WriterConfig{Type: "database", TableName: "users_copy"},
	}

	executor := NewTaskExecutor(cfg)
	executor.Stop()

	if executor.GetStatus() != TaskStatusCancelled {
		t.Errorf("expected cancelled status, got %s", executor.GetStatus())
	}
}

func TestTaskExecutor_GetResult_BeforeExecution(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "result-001",
		JobName: "Result Test",
		Reader:  ReaderConfig{Type: "database", TableName: "users"},
		Writer:  WriterConfig{Type: "database", TableName: "users_copy"},
	}

	executor := NewTaskExecutor(cfg)
	result := executor.GetResult()
	if result != nil {
		t.Error("expected nil result before execution")
	}
}

func TestTaskExecutor_RegisterReader(t *testing.T) {
	cfg := &TaskConfig{JobID: "reg-001", JobName: "Register Test"}
	executor := NewTaskExecutor(cfg)

	executor.RegisterReader("custom_reader", func(cfg *TaskConfig) (gsyncx.Reader, error) {
		return nil, nil
	})

	if _, ok := executor.readerFactories["custom_reader"]; !ok {
		t.Error("expected custom_reader factory to be registered")
	}
}

func TestTaskExecutor_RegisterWriter(t *testing.T) {
	cfg := &TaskConfig{JobID: "reg-002", JobName: "Register Test"}
	executor := NewTaskExecutor(cfg)

	executor.RegisterWriter("custom_writer", func(cfg *TaskConfig) (gsyncx.Writer, error) {
		return nil, nil
	})

	if _, ok := executor.writerFactories["custom_writer"]; !ok {
		t.Error("expected custom_writer factory to be registered")
	}
}

func TestTaskExecutor_RegisterTransformer(t *testing.T) {
	cfg := &TaskConfig{JobID: "reg-003", JobName: "Register Test"}
	executor := NewTaskExecutor(cfg)

	executor.RegisterTransformer("custom_transform", func(cfg *TaskConfig) (gsyncx.Transformer, error) {
		return nil, nil
	})

	if _, ok := executor.transformerFactories["custom_transform"]; !ok {
		t.Error("expected custom_transform factory to be registered")
	}
}

func TestTaskExecutor_RegisterMapper(t *testing.T) {
	cfg := &TaskConfig{JobID: "reg-004", JobName: "Register Test"}
	executor := NewTaskExecutor(cfg)

	executor.RegisterMapper("custom_mapper", func(cfg *TaskConfig) (gsyncx.Mapper, error) {
		return nil, nil
	})

	if _, ok := executor.mapperFactories["custom_mapper"]; !ok {
		t.Error("expected custom_mapper factory to be registered")
	}
}

func TestTaskExecutor_Execute_ValidationFailed(t *testing.T) {
	cfg := &TaskConfig{
		JobName: "No ID",
		Reader:  ReaderConfig{Type: "database"},
		Writer:  WriterConfig{Type: "database"},
	}

	executor := NewTaskExecutor(cfg)
	result, err := executor.Execute(context.Background())

	if err == nil {
		t.Error("expected error for invalid config")
	}
	if result.Status != TaskStatusFailed {
		t.Errorf("expected failed status, got %s", result.Status)
	}
	if result.Error == "" {
		t.Error("expected non-empty error message")
	}
}

func TestTaskExecutor_Execute_UnsupportedReader(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "unsupported-001",
		JobName: "Unsupported Reader",
		Reader:  ReaderConfig{Type: "unsupported_type", TableName: "users"},
		Writer:  WriterConfig{Type: "database", TableName: "users_copy", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
	}

	executor := NewTaskExecutor(cfg)
	result, err := executor.Execute(context.Background())

	if err == nil {
		t.Error("expected error for unsupported reader type")
	}
	if result.Status != TaskStatusFailed {
		t.Errorf("expected failed status, got %s", result.Status)
	}
}

func TestTaskExecutor_Execute_UnsupportedWriter(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "unsupported-002",
		JobName: "Unsupported Writer",
		Reader:  ReaderConfig{Type: "database", TableName: "users", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
		Writer:  WriterConfig{Type: "unsupported_type", TableName: "users_copy"},
	}

	executor := NewTaskExecutor(cfg)
	result, err := executor.Execute(context.Background())

	if err == nil {
		t.Error("expected error for unsupported writer type")
	}
	if result.Status != TaskStatusFailed {
		t.Errorf("expected failed status, got %s", result.Status)
	}
}

func TestTaskExecutor_Execute_CancelledContext(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "cancel-001",
		JobName: "Cancel Test",
		Reader:  ReaderConfig{Type: "database", TableName: "users", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
		Writer:  WriterConfig{Type: "database", TableName: "users_copy", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	executor := NewTaskExecutor(cfg)
	result, _ := executor.Execute(ctx)

	if result.Status != TaskStatusFailed && result.Status != TaskStatusCancelled {
		t.Errorf("expected failed or cancelled status, got %s", result.Status)
	}
}

func TestTaskExecutor_Execute_WithCustomReader(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "custom-001",
		JobName: "Custom Reader Test",
		Reader:  ReaderConfig{Type: "mock"},
		Writer:  WriterConfig{Type: "mock"},
	}

	executor := NewTaskExecutor(cfg,
		WithReaderFactory("mock", func(cfg *TaskConfig) (gsyncx.Reader, error) {
			return &mockTaskReader{}, nil
		}),
		WithWriterFactory("mock", func(cfg *TaskConfig) (gsyncx.Writer, error) {
			return &mockTaskWriter{}, nil
		}),
	)

	result, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != TaskStatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
	if result.StartTime.IsZero() {
		t.Error("expected non-zero start time")
	}
	if result.EndTime.IsZero() {
		t.Error("expected non-zero end time")
	}
}

func TestTaskExecutor_Execute_WithHook(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "hook-001",
		JobName: "Hook Test",
		Reader:  ReaderConfig{Type: "mock"},
		Writer:  WriterConfig{Type: "mock"},
	}

	hookCalled := false
	executor := NewTaskExecutor(cfg,
		WithReaderFactory("mock", func(cfg *TaskConfig) (gsyncx.Reader, error) {
			return &mockTaskReader{}, nil
		}),
		WithWriterFactory("mock", func(cfg *TaskConfig) (gsyncx.Writer, error) {
			return &mockTaskWriter{}, nil
		}),
		WithTaskHook(gsyncx.HookAfterRead, func(ctx context.Context, hctx *gsyncx.HookContext) error {
			hookCalled = true
			return nil
		}),
	)

	_, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hookCalled {
		t.Error("expected hook to be called")
	}
}

func TestExecuteTaskFromBytes(t *testing.T) {
	configJSON := `{
		"job_id": "bytes-001",
		"job_name": "Bytes Test",
		"reader": {"type": "mock"},
		"writer": {"type": "mock"}
	}`

	result, err := ExecuteTaskFromBytes(context.Background(), []byte(configJSON),
		WithReaderFactory("mock", func(cfg *TaskConfig) (gsyncx.Reader, error) {
			return &mockTaskReader{}, nil
		}),
		WithWriterFactory("mock", func(cfg *TaskConfig) (gsyncx.Writer, error) {
			return &mockTaskWriter{}, nil
		}),
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != TaskStatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestExecuteTaskFromBytes_InvalidJSON(t *testing.T) {
	_, err := ExecuteTaskFromBytes(context.Background(), []byte("invalid"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestTaskExecutor_Execute_ResultTiming(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "timing-001",
		JobName: "Timing Test",
		Reader:  ReaderConfig{Type: "mock"},
		Writer:  WriterConfig{Type: "mock"},
	}

	executor := NewTaskExecutor(cfg,
		WithReaderFactory("mock", func(cfg *TaskConfig) (gsyncx.Reader, error) {
			return &mockTaskReader{}, nil
		}),
		WithWriterFactory("mock", func(cfg *TaskConfig) (gsyncx.Writer, error) {
			return &mockTaskWriter{}, nil
		}),
	)

	result, _ := executor.Execute(context.Background())
	if result.StartTime.After(result.EndTime) {
		t.Error("start time should be before end time")
	}
	if result.Duration < 0 {
		t.Error("duration should be non-negative")
	}
}

func TestTaskExecutor_Execute_DatabaseReaderCreationError(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "db-err-001",
		JobName: "DB Error Test",
		Reader: ReaderConfig{
			Type:      "database",
			TableName: "users",
			DSNConfig: &DSNConfig{
				DBType: "mysql",
				Host:   "nonexistent-host",
				Port:   9999,
				User:   "invalid",
				Schema: "invalid",
			},
		},
		Writer: WriterConfig{Type: "database", TableName: "users", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
	}

	executor := NewTaskExecutor(cfg)
	result, err := executor.Execute(context.Background())

	if err == nil {
		t.Error("expected error for invalid database connection")
	}
	if result.Status != TaskStatusFailed {
		t.Errorf("expected failed status, got %s", result.Status)
	}
}

func TestTaskExecutor_Execute_DatabaseWriterCreationError(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "db-err-002",
		JobName: "DB Writer Error Test",
		Reader: ReaderConfig{
			Type:      "database",
			TableName: "users",
			DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"},
		},
		Writer: WriterConfig{
			Type:      "database",
			TableName: "users",
			DSNConfig: &DSNConfig{
				DBType: "mysql",
				Host:   "nonexistent-host",
				Port:   9999,
				User:   "invalid",
				Schema: "invalid",
			},
		},
	}

	executor := NewTaskExecutor(cfg)
	result, err := executor.Execute(context.Background())

	if err == nil {
		t.Error("expected error for invalid database connection")
	}
	if result.Status != TaskStatusFailed {
		t.Errorf("expected failed status, got %s", result.Status)
	}
}

func TestTaskExecutor_Execute_WithSetting(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "setting-001",
		JobName: "Setting Test",
		Reader:  ReaderConfig{Type: "mock"},
		Writer:  WriterConfig{Type: "mock"},
		Setting: &SettingConfig{
			SyncMode:        gsyncx.SyncModeFull,
			BatchSize:       500,
			ContinueOnError: true,
		},
	}

	executor := NewTaskExecutor(cfg,
		WithReaderFactory("mock", func(cfg *TaskConfig) (gsyncx.Reader, error) {
			return &mockTaskReader{}, nil
		}),
		WithWriterFactory("mock", func(cfg *TaskConfig) (gsyncx.Writer, error) {
			return &mockTaskWriter{}, nil
		}),
	)

	result, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != TaskStatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestTaskExecutor_Execute_WithMapping(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "mapping-001",
		JobName: "Mapping Test",
		Reader:  ReaderConfig{Type: "mock"},
		Writer:  WriterConfig{Type: "mock"},
		Mapping: &MappingConfig{
			Mappings: []FieldMappingConfig{
				{SourceField: "name", TargetField: "full_name"},
			},
			AutoMapping: true,
		},
	}

	executor := NewTaskExecutor(cfg,
		WithReaderFactory("mock", func(cfg *TaskConfig) (gsyncx.Reader, error) {
			return &mockTaskReader{}, nil
		}),
		WithWriterFactory("mock", func(cfg *TaskConfig) (gsyncx.Writer, error) {
			return &mockTaskWriter{}, nil
		}),
	)

	result, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != TaskStatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestTaskExecutor_Execute_WithTransform(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "transform-001",
		JobName: "Transform Test",
		Reader:  ReaderConfig{Type: "mock"},
		Writer:  WriterConfig{Type: "mock"},
		Transform: &TransformConfig{
			Script: "function transform(records) return records end",
		},
	}

	executor := NewTaskExecutor(cfg,
		WithReaderFactory("mock", func(cfg *TaskConfig) (gsyncx.Reader, error) {
			return &mockTaskReader{}, nil
		}),
		WithWriterFactory("mock", func(cfg *TaskConfig) (gsyncx.Writer, error) {
			return &mockTaskWriter{}, nil
		}),
	)

	result, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != TaskStatusCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestTaskExecutor_Execute_RedisStreamReaderCreationError(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "redis-err-001",
		JobName: "Redis Error Test",
		Reader: ReaderConfig{
			Type: "redis_stream",
			Redis: &RedisReaderConfig{
				Addr:   "nonexistent:6379",
				Stream: "test-stream",
			},
		},
		Writer: WriterConfig{Type: "database", TableName: "t", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
	}

	executor := NewTaskExecutor(cfg)
	result, err := executor.Execute(context.Background())

	if err == nil {
		t.Error("expected error for invalid redis connection")
	}
	if result.Status != TaskStatusFailed {
		t.Errorf("expected failed status, got %s", result.Status)
	}
}

func TestTaskExecutor_Execute_SQLReaderNoDSN(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "sql-nodsn-001",
		JobName: "SQL No DSN Test",
		Reader:  ReaderConfig{Type: "sql", SQL: "SELECT 1"},
		Writer:  WriterConfig{Type: "database", TableName: "t", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
	}

	executor := NewTaskExecutor(cfg)
	result, err := executor.Execute(context.Background())

	if err == nil {
		t.Error("expected error for missing DSN config")
	}
	if result.Status != TaskStatusFailed {
		t.Errorf("expected failed status, got %s", result.Status)
	}
}

func TestTaskExecutor_Execute_DatabaseReaderNoDSN(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "db-nodsn-001",
		JobName: "DB No DSN Test",
		Reader:  ReaderConfig{Type: "database", TableName: "users"},
		Writer:  WriterConfig{Type: "database", TableName: "t", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
	}

	executor := NewTaskExecutor(cfg)
	result, err := executor.Execute(context.Background())

	if err == nil {
		t.Error("expected error for missing DSN config")
	}
	if result.Status != TaskStatusFailed {
		t.Errorf("expected failed status, got %s", result.Status)
	}
}

func TestTaskExecutor_Execute_DatabaseWriterNoDSN(t *testing.T) {
	cfg := &TaskConfig{
		JobID:   "db-writer-nodsn-001",
		JobName: "DB Writer No DSN Test",
		Reader:  ReaderConfig{Type: "database", TableName: "users", DSNConfig: &DSNConfig{DBType: "mysql", Host: "localhost"}},
		Writer:  WriterConfig{Type: "database", TableName: "users"},
	}

	executor := NewTaskExecutor(cfg)
	result, err := executor.Execute(context.Background())

	if err == nil {
		t.Error("expected error for missing DSN config")
	}
	if result.Status != TaskStatusFailed {
		t.Errorf("expected failed status, got %s", result.Status)
	}
}

func TestTaskExecutor_GetProgress(t *testing.T) {
	cfg := &TaskConfig{JobID: "progress-001", JobName: "Progress Test"}
	executor := NewTaskExecutor(cfg)
	progress := executor.GetProgress()
	if progress != nil {
		t.Error("expected nil progress (not implemented)")
	}
}

func TestFieldMappingConfig(t *testing.T) {
	fm := FieldMappingConfig{
		SourceField: "src",
		TargetField: "dst",
		Transform:   "to_upper",
		Default:     "N/A",
		Required:    true,
		TypeCheck:   "string",
	}
	if fm.SourceField != "src" {
		t.Errorf("expected src, got %s", fm.SourceField)
	}
	if !fm.Required {
		t.Error("expected required true")
	}
}

func TestIncrementalFieldConfig(t *testing.T) {
	ifc := IncrementalFieldConfig{
		FieldName: "updated_at",
		Strategy:  "timestamp",
	}
	if ifc.FieldName != "updated_at" {
		t.Errorf("expected updated_at, got %s", ifc.FieldName)
	}
	if ifc.Strategy != "timestamp" {
		t.Errorf("expected timestamp, got %s", ifc.Strategy)
	}
}

type mockTaskReader struct{}

func (m *mockTaskReader) Read(ctx context.Context, cfg *gsyncx.SyncConfig) (<-chan []gsyncx.Record, <-chan error) {
	recordCh := make(chan []gsyncx.Record, 1)
	errCh := make(chan error, 1)

	go func() {
		defer close(recordCh)
		defer close(errCh)
		recordCh <- []gsyncx.Record{
			{Data: map[string]interface{}{"id": 1, "name": "test"}},
		}
	}()

	return recordCh, errCh
}

func (m *mockTaskReader) Count(ctx context.Context, cfg *gsyncx.SyncConfig) (int64, error) {
	return 1, nil
}

func (m *mockTaskReader) GetSplitKeys(ctx context.Context, cfg *gsyncx.SyncConfig) ([]gsyncx.SplitKey, error) {
	return nil, nil
}

func (m *mockTaskReader) Close() error {
	return nil
}

type mockTaskWriter struct{}

func (m *mockTaskWriter) Write(ctx context.Context, records []gsyncx.Record) (gsyncx.WriteResult, error) {
	return gsyncx.WriteResult{SuccessCount: int64(len(records))}, nil
}

func (m *mockTaskWriter) WriteWithMode(ctx context.Context, records []gsyncx.Record, mode gsyncx.WriteMode) (gsyncx.WriteResult, error) {
	return gsyncx.WriteResult{SuccessCount: int64(len(records))}, nil
}

func (m *mockTaskWriter) Flush(ctx context.Context) error {
	return nil
}

func (m *mockTaskWriter) Close() error {
	return nil
}

func TestExampleConfigFiles(t *testing.T) {
	configFiles := []string{
		"../examples/task_configs/mysql_full_sync.json",
		"../examples/task_configs/mysql_incremental_sync.json",
		"../examples/task_configs/redis_realtime_sync.json",
	}

	for _, file := range configFiles {
		t.Run(filepath.Base(file), func(t *testing.T) {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatalf("failed to read config file: %v", err)
			}

			cfg, err := ParseTaskConfig(data)
			if err != nil {
				t.Fatalf("failed to parse config: %v", err)
			}

			if cfg.JobID == "" {
				t.Error("expected non-empty job_id")
			}
			if cfg.JobName == "" {
				t.Error("expected non-empty job_name")
			}
			if cfg.Reader.Type == "" {
				t.Error("expected non-empty reader type")
			}
			if cfg.Writer.Type == "" {
				t.Error("expected non-empty writer type")
			}

			if err := cfg.Validate(); err != nil {
				t.Errorf("config validation failed: %v", err)
			}
		})
	}
}
