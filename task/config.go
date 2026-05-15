package task

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/dataixcom/gsyncx"
)

type TaskConfig struct {
	JobID     string            `json:"job_id"`
	JobName   string            `json:"job_name"`
	Version   string            `json:"version,omitempty"`
	Reader    ReaderConfig      `json:"reader"`
	Transform *TransformConfig  `json:"transform,omitempty"`
	Mapping   *MappingConfig    `json:"mapping,omitempty"`
	Writer    WriterConfig      `json:"writer"`
	Setting   *SettingConfig    `json:"setting,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type ReaderConfig struct {
	Type        string                 `json:"type"`
	DSNConfig   *DSNConfig             `json:"dsn_config,omitempty"`
	TableName   string                 `json:"table_name,omitempty"`
	Schema      string                 `json:"schema,omitempty"`
	SQL         string                 `json:"sql,omitempty"`
	WhereClause string                 `json:"where_clause,omitempty"`
	PrimaryKey  *FieldConfig           `json:"primary_key,omitempty"`
	Fields      []FieldConfig          `json:"fields,omitempty"`
	RawFields   []string               `json:"raw_fields,omitempty"`
	Redis       *RedisReaderConfig     `json:"redis,omitempty"`
	Args        map[string]interface{} `json:"args,omitempty"`
}

type RedisReaderConfig struct {
	Addr          string        `json:"addr"`
	Password      string        `json:"password,omitempty"`
	DB            int           `json:"db,omitempty"`
	Stream        string        `json:"stream"`
	ConsumerGroup string        `json:"consumer_group,omitempty"`
	ConsumerName  string        `json:"consumer_name,omitempty"`
	Count         int64         `json:"count,omitempty"`
	Block         time.Duration `json:"block,omitempty"`
	BatchSize     int           `json:"batch_size,omitempty"`
	AutoCreate    bool          `json:"auto_create,omitempty"`
	StartID       string        `json:"start_id,omitempty"`
}

type WriterConfig struct {
	Type           string                 `json:"type"`
	DSNConfig      *DSNConfig             `json:"dsn_config,omitempty"`
	TableName      string                 `json:"table_name,omitempty"`
	Schema         string                 `json:"schema,omitempty"`
	WriteMode      string                 `json:"write_mode,omitempty"`
	PrimaryKey     *FieldConfig           `json:"primary_key,omitempty"`
	Fields         []FieldConfig          `json:"fields,omitempty"`
	RawFields      []string               `json:"raw_fields,omitempty"`
	BatchSize      int                    `json:"batch_size,omitempty"`
	UseTransaction bool                   `json:"use_transaction,omitempty"`
	Args           map[string]interface{} `json:"args,omitempty"`
}

type TransformConfig struct {
	Type         string            `json:"type,omitempty"`
	Script       string            `json:"script,omitempty"`
	ScriptPath   string            `json:"script_path,omitempty"`
	ScriptLang   string            `json:"script_lang,omitempty"`
	Timeout      time.Duration     `json:"timeout,omitempty"`
	MaxMemory    int64             `json:"max_memory,omitempty"`
	Env          map[string]string `json:"env,omitempty"`
	FieldMapping map[string]string `json:"field_mapping,omitempty"`
}

type MappingConfig struct {
	Mappings       []FieldMappingConfig   `json:"mappings,omitempty"`
	DefaultValues  map[string]interface{} `json:"default_values,omitempty"`
	IgnoreMissing  bool                   `json:"ignore_missing,omitempty"`
	StrictMode     bool                   `json:"strict_mode,omitempty"`
	ValidateOnLoad bool                   `json:"validate_on_load,omitempty"`
	AutoMapping    bool                   `json:"auto_mapping,omitempty"`
}

type FieldMappingConfig struct {
	SourceField string      `json:"source_field"`
	TargetField string      `json:"target_field"`
	Transform   string      `json:"transform,omitempty"`
	Default     interface{} `json:"default,omitempty"`
	Required    bool        `json:"required,omitempty"`
	TypeCheck   string      `json:"type_check,omitempty"`
}

type FieldConfig struct {
	FieldName string `json:"field_name"`
	Alias     string `json:"alias,omitempty"`
	Type      string `json:"type,omitempty"`
}

type DSNConfig struct {
	DBType   string `json:"db_type"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password,omitempty"`
	Schema   string `json:"schema"`
	MaxIdle  int    `json:"max_idle,omitempty"`
	MaxOpen  int    `json:"max_open,omitempty"`
}

type SettingConfig struct {
	SyncMode          gsyncx.SyncMode         `json:"sync_mode,omitempty"`
	BatchSize         int                     `json:"batch_size,omitempty"`
	Parallelism       int                     `json:"parallelism,omitempty"`
	RetryMaxAttempts  int                     `json:"retry_max_attempts,omitempty"`
	RetryDelay        time.Duration           `json:"retry_delay,omitempty"`
	ContinueOnError   bool                    `json:"continue_on_error,omitempty"`
	ErrorThreshold    int                     `json:"error_threshold,omitempty"`
	PreviewMode       bool                    `json:"preview_mode,omitempty"`
	PreviewLimit      int                     `json:"preview_limit,omitempty"`
	IntegrityCheck    string                  `json:"integrity_check,omitempty"`
	CheckpointEnabled bool                    `json:"checkpoint_enabled,omitempty"`
	CheckpointPath    string                  `json:"checkpoint_path,omitempty"`
	IncrementalField  *IncrementalFieldConfig `json:"incremental_field,omitempty"`
	LastSyncTime      string                  `json:"last_sync_time,omitempty"`
	LastSyncValue     interface{}             `json:"last_sync_value,omitempty"`
}

type IncrementalFieldConfig struct {
	FieldName string `json:"field_name"`
	Strategy  string `json:"strategy"`
}

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

type TaskResult struct {
	JobID      string             `json:"job_id"`
	JobName    string             `json:"job_name"`
	Status     TaskStatus         `json:"status"`
	SyncResult *gsyncx.SyncResult `json:"sync_result,omitempty"`
	Error      string             `json:"error,omitempty"`
	StartTime  time.Time          `json:"start_time"`
	EndTime    time.Time          `json:"end_time"`
	Duration   time.Duration      `json:"duration"`
	ConfigPath string             `json:"config_path,omitempty"`
}

func LoadTaskConfig(path string) (*TaskConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read task config file: %w", err)
	}
	return ParseTaskConfig(data)
}

func ParseTaskConfig(data []byte) (*TaskConfig, error) {
	var cfg TaskConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse task config: %w", err)
	}
	return &cfg, nil
}

func (c *TaskConfig) ToJSON() ([]byte, error) {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal task config: %w", err)
	}
	return data, nil
}

func (c *TaskConfig) Validate() error {
	if c.JobID == "" {
		return fmt.Errorf("job_id is required")
	}
	if c.JobName == "" {
		return fmt.Errorf("job_name is required")
	}
	if err := c.Reader.Validate(); err != nil {
		return fmt.Errorf("reader config invalid: %w", err)
	}
	if err := c.Writer.Validate(); err != nil {
		return fmt.Errorf("writer config invalid: %w", err)
	}
	if c.Transform != nil {
		if err := c.Transform.Validate(); err != nil {
			return fmt.Errorf("transform config invalid: %w", err)
		}
	}
	if c.Mapping != nil {
		if err := c.Mapping.Validate(); err != nil {
			return fmt.Errorf("mapping config invalid: %w", err)
		}
	}
	return nil
}

func (r *ReaderConfig) Validate() error {
	if r.Type == "" {
		return fmt.Errorf("reader type is required")
	}
	switch r.Type {
	case "database", "sql":
		if r.DSNConfig == nil {
			return fmt.Errorf("dsn_config is required for database reader")
		}
		if r.DSNConfig.Host == "" {
			return fmt.Errorf("dsn_config.host is required")
		}
		if r.DSNConfig.DBType == "" {
			return fmt.Errorf("dsn_config.db_type is required")
		}
		if r.TableName == "" && r.SQL == "" {
			return fmt.Errorf("table_name or sql is required for database reader")
		}
	case "redis_stream":
		if r.Redis == nil {
			return fmt.Errorf("redis config is required for redis_stream reader")
		}
		if r.Redis.Stream == "" {
			return fmt.Errorf("redis.stream is required")
		}
	default:
	}
	return nil
}

func (w *WriterConfig) Validate() error {
	if w.Type == "" {
		return fmt.Errorf("writer type is required")
	}
	switch w.Type {
	case "database":
		if w.DSNConfig == nil {
			return fmt.Errorf("dsn_config is required for database writer")
		}
		if w.DSNConfig.Host == "" {
			return fmt.Errorf("dsn_config.host is required")
		}
		if w.DSNConfig.DBType == "" {
			return fmt.Errorf("dsn_config.db_type is required")
		}
		if w.TableName == "" {
			return fmt.Errorf("table_name is required for database writer")
		}
	default:
	}
	return nil
}

func (t *TransformConfig) Validate() error {
	if t.Script == "" && t.ScriptPath == "" && len(t.FieldMapping) == 0 {
		return fmt.Errorf("at least one of script, script_path, or field_mapping is required")
	}
	return nil
}

func (m *MappingConfig) Validate() error {
	for i, fm := range m.Mappings {
		if fm.SourceField == "" {
			return fmt.Errorf("mapping[%d].source_field is required", i)
		}
		if fm.TargetField == "" {
			return fmt.Errorf("mapping[%d].target_field is required", i)
		}
	}
	return nil
}

func (d *DSNConfig) ToGdbxDSN() gsyncx.DSNConfig {
	dbType, _ := gsyncx.ParseDBType(d.DBType)
	return gsyncx.DSNConfig{
		DBType:   dbType,
		Host:     d.Host,
		Port:     d.Port,
		User:     d.User,
		Password: d.Password,
		Schema:   d.Schema,
		MaxIdle:  d.MaxIdle,
		MaxOpen:  d.MaxOpen,
	}
}

func (r *ReaderConfig) ToGdbxReaderConfig() gsyncx.ReaderConfig {
	cfg := gsyncx.ReaderConfig{
		TableName:   r.TableName,
		SQL:         r.SQL,
		WhereClause: r.WhereClause,
		RawFields:   r.RawFields,
	}
	if r.DSNConfig != nil {
		dsn := r.DSNConfig.ToGdbxDSN()
		cfg.DSNConfig = &dsn
		cfg.Schema = r.DSNConfig.Schema
	}
	if r.PrimaryKey != nil {
		pk := gsyncx.Field{FieldName: r.PrimaryKey.FieldName}
		cfg.PrimaryKey = &pk
	}
	if len(r.Fields) > 0 {
		fields := make([]gsyncx.Field, 0, len(r.Fields))
		for _, f := range r.Fields {
			fields = append(fields, gsyncx.Field{FieldName: f.FieldName, AliasName: f.Alias})
		}
		cfg.Fields = fields
	}
	return cfg
}

func (w *WriterConfig) ToGdbxWriterConfig() gsyncx.WriterConfig {
	cfg := gsyncx.WriterConfig{
		TableName:      w.TableName,
		BatchSize:      w.BatchSize,
		UseTransaction: w.UseTransaction,
		RawFields:      w.RawFields,
	}
	if w.DSNConfig != nil {
		dsn := w.DSNConfig.ToGdbxDSN()
		cfg.DSNConfig = &dsn
		cfg.Schema = w.DSNConfig.Schema
	}
	if w.WriteMode != "" {
		cfg.WriteMode = gsyncx.WriteMode(w.WriteMode)
	} else {
		cfg.WriteMode = gsyncx.WriteModeUpsert
	}
	if w.PrimaryKey != nil {
		pk := gsyncx.Field{FieldName: w.PrimaryKey.FieldName}
		cfg.PrimaryKey = &pk
	}
	if len(w.Fields) > 0 {
		fields := make([]gsyncx.Field, 0, len(w.Fields))
		for _, f := range w.Fields {
			fields = append(fields, gsyncx.Field{FieldName: f.FieldName, AliasName: f.Alias})
		}
		cfg.Fields = fields
	}
	return cfg
}

func (m *MappingConfig) ToGdbxMappingConfig() gsyncx.MappingConfig {
	cfg := gsyncx.MappingConfig{
		DefaultValues:  m.DefaultValues,
		IgnoreMissing:  m.IgnoreMissing,
		StrictMode:     m.StrictMode,
		ValidateOnLoad: m.ValidateOnLoad,
		AutoMapping:    m.AutoMapping,
	}
	if len(m.Mappings) > 0 {
		mappings := make([]gsyncx.FieldMapping, 0, len(m.Mappings))
		for _, fm := range m.Mappings {
			mappings = append(mappings, gsyncx.FieldMapping{
				SourceField: fm.SourceField,
				TargetField: fm.TargetField,
				Transform:   fm.Transform,
				Default:     fm.Default,
				Required:    fm.Required,
				TypeCheck:   fm.TypeCheck,
			})
		}
		cfg.Mappings = mappings
	}
	return cfg
}

func (t *TransformConfig) ToGdbxTransformConfig() gsyncx.TransformConfig {
	return gsyncx.TransformConfig{
		Script:     t.Script,
		ScriptPath: t.ScriptPath,
		ScriptLang: t.ScriptLang,
		Timeout:    t.Timeout,
		MaxMemory:  t.MaxMemory,
		Env:        t.Env,
	}
}

func (s *SettingConfig) ApplyToSyncConfig(cfg *gsyncx.SyncConfig) {
	if s == nil {
		return
	}
	if s.SyncMode != "" {
		cfg.SyncMode = s.SyncMode
	}
	if s.BatchSize > 0 {
		cfg.BatchSize = s.BatchSize
	}
	if s.Parallelism > 0 {
		cfg.Parallelism = s.Parallelism
	}
	if s.RetryMaxAttempts > 0 {
		cfg.RetryMaxAttempts = s.RetryMaxAttempts
	}
	if s.RetryDelay > 0 {
		cfg.RetryDelay = s.RetryDelay
	}
	cfg.ContinueOnError = s.ContinueOnError
	if s.ErrorThreshold > 0 {
		cfg.ErrorThreshold = s.ErrorThreshold
	}
	cfg.PreviewMode = s.PreviewMode
	if s.PreviewLimit > 0 {
		cfg.PreviewLimit = s.PreviewLimit
	}
	if s.IntegrityCheck != "" {
		cfg.IntegrityCheck = gsyncx.IntegrityCheckMode(s.IntegrityCheck)
	}
	cfg.CheckpointEnabled = s.CheckpointEnabled
	if s.CheckpointPath != "" {
		cfg.CheckpointPath = s.CheckpointPath
	}
	if s.IncrementalField != nil {
		cfg.IncrementalField = &gsyncx.Field{FieldName: s.IncrementalField.FieldName}
		cfg.IncrementalStrategy = gsyncx.IncrementalStrategy(s.IncrementalField.Strategy)
	}
	if s.LastSyncTime != "" {
		if t, err := time.Parse(time.RFC3339, s.LastSyncTime); err == nil {
			cfg.LastSyncTime = t
		}
	}
	if s.LastSyncValue != nil {
		cfg.LastSyncValue = s.LastSyncValue
	}
}
