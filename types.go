package gsyncx

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type SyncMode string

const (
	SyncModeFull        SyncMode = "full"
	SyncModeIncremental SyncMode = "incremental"
	SyncModeRealtime    SyncMode = "realtime"
)

type SyncStatus string

const (
	StatusPending   SyncStatus = "pending"
	StatusRunning   SyncStatus = "running"
	StatusPaused    SyncStatus = "paused"
	StatusCompleted SyncStatus = "completed"
	StatusFailed    SyncStatus = "failed"
	StatusCancelled SyncStatus = "cancelled"
)

type IncrementalStrategy string

const (
	StrategyTimestamp IncrementalStrategy = "timestamp"
	StrategyAutoInc   IncrementalStrategy = "autoinc"
	StrategyVersion   IncrementalStrategy = "version"
	StrategyCustom    IncrementalStrategy = "custom"
)

type WriteMode string

const (
	WriteModeInsert WriteMode = "insert"
	WriteModeUpdate WriteMode = "update"
	WriteModeUpsert WriteMode = "upsert"
)

type PipelineStage string

const (
	StageRead      PipelineStage = "read"
	StageTransform PipelineStage = "transform"
	StageMap       PipelineStage = "map"
	StageWrite     PipelineStage = "write"
)

type IntegrityCheckMode string

const (
	IntegrityCheckNone     IntegrityCheckMode = "none"
	IntegrityCheckCount    IntegrityCheckMode = "count"
	IntegrityCheckChecksum IntegrityCheckMode = "checksum"
	IntegrityCheckFull     IntegrityCheckMode = "full"
)

type SyncConfig struct {
	SourceDSN            DSNConfig           `json:"source_dsn"`
	TargetDSN            DSNConfig           `json:"target_dsn"`
	ReaderConfig         ReaderConfig        `json:"reader_config"`
	WriterConfig         WriterConfig        `json:"writer_config"`
	TransformConfig      TransformConfig     `json:"transform_config,omitempty"`
	MappingConfig        MappingConfig       `json:"mapping_config,omitempty"`
	SyncMode             SyncMode            `json:"sync_mode"`
	BatchSize            int                 `json:"batch_size"`
	Parallelism          int                 `json:"parallelism"`
	CheckpointEnabled    bool                `json:"checkpoint_enabled"`
	CheckpointPath       string              `json:"checkpoint_path,omitempty"`
	CheckpointDir        string              `json:"checkpoint_dir,omitempty"`
	IncrementalField     *Field              `json:"incremental_field,omitempty"`
	IncrementalStrategy  IncrementalStrategy `json:"incremental_strategy,omitempty"`
	IncrementalCondition string              `json:"incremental_condition,omitempty"`
	LastSyncTime         time.Time           `json:"last_sync_time,omitempty"`
	LastSyncValue        interface{}         `json:"last_sync_value,omitempty"`
	RetryMaxAttempts     int                 `json:"retry_max_attempts,omitempty"`
	RetryDelay           time.Duration       `json:"retry_delay,omitempty"`
	ErrorThreshold       int                 `json:"error_threshold,omitempty"`
	ContinueOnError      bool                `json:"continue_on_error,omitempty"`
	PreviewMode          bool                `json:"preview_mode,omitempty"`
	PreviewLimit         int                 `json:"preview_limit,omitempty"`
	IntegrityCheck       IntegrityCheckMode  `json:"integrity_check,omitempty"`
	AutoMapping          bool                `json:"auto_mapping,omitempty"`
	logger               SyncLogger
}

func (c *SyncConfig) GetLogger() SyncLogger {
	if c.logger != nil {
		return c.logger
	}
	return defaultLogger
}

func (c *SyncConfig) IsFullSync() bool {
	return c.SyncMode == SyncModeFull
}

func (c *SyncConfig) IsIncrementalSync() bool {
	return c.SyncMode == SyncModeIncremental
}

func (c *SyncConfig) IsRealtimeSync() bool {
	return c.SyncMode == SyncModeRealtime
}

func (c *SyncConfig) SwitchToFullSync() {
	c.SyncMode = SyncModeFull
	c.IncrementalField = nil
	c.IncrementalStrategy = ""
	c.IncrementalCondition = ""
	c.LastSyncTime = time.Time{}
	c.LastSyncValue = nil
}

func (c *SyncConfig) SwitchToIncrementalSync(field *Field, strategy IncrementalStrategy) {
	c.SyncMode = SyncModeIncremental
	c.IncrementalField = field
	c.IncrementalStrategy = strategy
}

func (c *SyncConfig) SwitchToRealtimeSync() {
	c.SyncMode = SyncModeRealtime
}

type ReaderConfig struct {
	DSNConfig   *DSNConfig          `json:"dsn_config,omitempty"`
	TableName   string              `json:"table_name"`
	Schema      string              `json:"schema,omitempty"`
	PrimaryKey  *Field              `json:"primary_key,omitempty"`
	Fields      []Field             `json:"fields,omitempty"`
	RawFields   []string            `json:"raw_fields,omitempty"`
	WhereClause string              `json:"where_clause,omitempty"`
	Conditions  *WhereClauseBuilder `json:"-"`
	SQL         string              `json:"sql,omitempty"`
	Args        map[string]any      `json:"args,omitempty"`
}

type WriterConfig struct {
	DSNConfig      *DSNConfig `json:"dsn_config,omitempty"`
	TableName      string     `json:"table_name"`
	Schema         string     `json:"schema,omitempty"`
	PrimaryKey     *Field     `json:"primary_key,omitempty"`
	Fields         []Field    `json:"fields,omitempty"`
	RawFields      []string   `json:"raw_fields,omitempty"`
	SQL            string     `json:"sql,omitempty"`
	Condition      string     `json:"condition,omitempty"`
	WriteMode      WriteMode  `json:"write_mode"`
	UseTransaction bool       `json:"use_transaction,omitempty"`
	BatchSize      int        `json:"batch_size,omitempty"`
}

type TransformConfig struct {
	Script     string            `json:"script,omitempty"`
	ScriptPath string            `json:"script_path,omitempty"`
	ScriptLang string            `json:"script_lang,omitempty"`
	Timeout    time.Duration     `json:"timeout,omitempty"`
	MaxMemory  int64             `json:"max_memory,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
}

type MappingConfig struct {
	Mappings       []FieldMapping `json:"mappings,omitempty"`
	DefaultValues  map[string]any `json:"default_values,omitempty"`
	IgnoreMissing  bool           `json:"ignore_missing,omitempty"`
	StrictMode     bool           `json:"strict_mode,omitempty"`
	ValidateOnLoad bool           `json:"validate_on_load,omitempty"`
	AutoMapping    bool           `json:"auto_mapping,omitempty"`
}

type FieldMapping struct {
	SourceField string      `json:"source_field"`
	TargetField string      `json:"target_field"`
	Transform   string      `json:"transform,omitempty"`
	Default     interface{} `json:"default,omitempty"`
	Required    bool        `json:"required,omitempty"`
	TypeCheck   string      `json:"type_check,omitempty"`
}

type IntegrityResult struct {
	Mode          IntegrityCheckMode `json:"mode"`
	SourceCount   int64              `json:"source_count"`
	TargetCount   int64              `json:"target_count"`
	CountMatch    bool               `json:"count_match"`
	ChecksumMatch bool               `json:"checksum_match,omitempty"`
	Passed        bool               `json:"passed"`
	Details       string             `json:"details,omitempty"`
}

type SyncProgress struct {
	Status         SyncStatus    `json:"status"`
	TotalRecords   int64         `json:"total_records"`
	SyncedRecords  int64         `json:"synced_records"`
	FailedRecords  int64         `json:"failed_records"`
	SkippedRecords int64         `json:"skipped_records"`
	Percent        float64       `json:"percent"`
	StartTime      time.Time     `json:"start_time"`
	EndTime        time.Time     `json:"end_time,omitempty"`
	ErrorMessage   string        `json:"error_message,omitempty"`
	SourceTable    string        `json:"source_table,omitempty"`
	TargetTable    string        `json:"target_table,omitempty"`
	CurrentStage   PipelineStage `json:"current_stage,omitempty"`
}

type SyncResult struct {
	Status          SyncStatus       `json:"status"`
	TotalRead       int64            `json:"total_read"`
	TotalWritten    int64            `json:"total_written"`
	TotalFailed     int64            `json:"total_failed"`
	TotalSkipped    int64            `json:"total_skipped"`
	StartTime       time.Time        `json:"start_time"`
	EndTime         time.Time        `json:"end_time"`
	Duration        time.Duration    `json:"duration"`
	Error           error            `json:"-"`
	PreviewData     []Record         `json:"preview_data,omitempty"`
	IntegrityResult *IntegrityResult `json:"integrity_result,omitempty"`
}

type Record struct {
	Data map[string]interface{} `json:"data"`
	Meta RecordMeta             `json:"meta"`
}

type RecordMeta struct {
	SourcePK   interface{}   `json:"source_pk,omitempty"`
	SourceTS   time.Time     `json:"source_ts,omitempty"`
	Checkpoint *Checkpoint   `json:"checkpoint,omitempty"`
	Stage      PipelineStage `json:"stage,omitempty"`
	Error      error         `json:"-"`
	RetryCount int           `json:"retry_count,omitempty"`
}

type Checkpoint struct {
	TableName    string      `json:"table_name"`
	FieldName    string      `json:"field_name"`
	LastValue    interface{} `json:"last_value"`
	LastSyncTime time.Time   `json:"last_sync_time"`
	BatchNum     int         `json:"batch_num"`
	BatchOffset  int         `json:"batch_offset"`
}

type SyncStatistics struct {
	ReadOK          int64      `json:"read_ok"`
	ReadFailed      int64      `json:"read_failed"`
	TransformOK     int64      `json:"transform_ok"`
	TransformFailed int64      `json:"transform_failed"`
	MappingOK       int64      `json:"mapping_ok"`
	MappingFailed   int64      `json:"mapping_failed"`
	WriteOK         int64      `json:"write_ok"`
	WriteFailed     int64      `json:"write_failed"`
	SkippedTotal    int64      `json:"skipped_total"`
	StartTime       time.Time  `json:"start_time"`
	EndTime         *time.Time `json:"end_time,omitempty"`
	mu              sync.RWMutex
}

func (s *SyncStatistics) IncReadOK(n int64)          { atomic.AddInt64(&s.ReadOK, n) }
func (s *SyncStatistics) IncReadFailed(n int64)      { atomic.AddInt64(&s.ReadFailed, n) }
func (s *SyncStatistics) IncTransformOK(n int64)     { atomic.AddInt64(&s.TransformOK, n) }
func (s *SyncStatistics) IncTransformFailed(n int64) { atomic.AddInt64(&s.TransformFailed, n) }
func (s *SyncStatistics) IncMappingOK(n int64)       { atomic.AddInt64(&s.MappingOK, n) }
func (s *SyncStatistics) IncMappingFailed(n int64)   { atomic.AddInt64(&s.MappingFailed, n) }
func (s *SyncStatistics) IncWriteOK(n int64)         { atomic.AddInt64(&s.WriteOK, n) }
func (s *SyncStatistics) IncWriteFailed(n int64)     { atomic.AddInt64(&s.WriteFailed, n) }
func (s *SyncStatistics) IncSkippedTotal(n int64)    { atomic.AddInt64(&s.SkippedTotal, n) }

type SplitKey struct {
	FieldName string      `json:"field_name"`
	MinValue  interface{} `json:"min_value,omitempty"`
	MaxValue  interface{} `json:"max_value,omitempty"`
	TotalRows int64       `json:"total_rows"`
}

type WriteResult struct {
	SuccessCount  int64          `json:"success_count"`
	FailedCount   int64          `json:"failed_count"`
	SkippedCount  int64          `json:"skipped_count"`
	FailedRecords []FailedRecord `json:"failed_records,omitempty"`
}

type FailedRecord struct {
	Record Record        `json:"record"`
	Error  error         `json:"-"`
	Stage  PipelineStage `json:"stage"`
}

type HookPoint string

const (
	HookBeforeRead      HookPoint = "before_read"
	HookAfterRead       HookPoint = "after_read"
	HookBeforeTransform HookPoint = "before_transform"
	HookAfterTransform  HookPoint = "after_transform"
	HookBeforeMap       HookPoint = "before_map"
	HookAfterMap        HookPoint = "after_map"
	HookBeforeWrite     HookPoint = "before_write"
	HookAfterWrite      HookPoint = "after_write"
	HookOnError         HookPoint = "on_error"
	HookOnSkip          HookPoint = "on_skip"
	HookOnRetry         HookPoint = "on_retry"
	HookOnComplete      HookPoint = "on_complete"
)

type HookContext struct {
	Point    HookPoint      `json:"point"`
	Stage    PipelineStage  `json:"stage"`
	Records  []Record       `json:"-"`
	Config   *SyncConfig    `json:"-"`
	Progress *SyncProgress  `json:"-"`
	Error    error          `json:"-"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type HookFunc func(ctx context.Context, hctx *HookContext) error

type Reader interface {
	Read(ctx context.Context, cfg *SyncConfig) (<-chan []Record, <-chan error)
	Count(ctx context.Context, cfg *SyncConfig) (int64, error)
	GetSplitKeys(ctx context.Context, cfg *SyncConfig) ([]SplitKey, error)
	Close() error
}

type ReaderType string

const (
	ReaderTypeDatabase    ReaderType = "database"
	ReaderTypeSQL         ReaderType = "sql"
	ReaderTypeRedisStream ReaderType = "redis_stream"
)

type TypedReader interface {
	Reader
	SourceType() ReaderType
}

type IncrementalReader interface {
	ReadWithIncremental(ctx context.Context, cfg *SyncConfig, field string, lastValue interface{}) (<-chan []Record, <-chan error)
}

type Writer interface {
	Write(ctx context.Context, records []Record) (WriteResult, error)
	WriteWithMode(ctx context.Context, records []Record, mode WriteMode) (WriteResult, error)
	Flush(ctx context.Context) error
	Close() error
}

type WriterType string

const (
	WriterTypeDatabase WriterType = "database"
)

type TypedWriter interface {
	Writer
	TargetType() WriterType
}

type Transformer interface {
	Transform(ctx context.Context, records []Record) ([]Record, []FailedRecord, error)
}

type Mapper interface {
	Map(records []Record) ([]Record, []FailedRecord, error)
}

type CheckpointStore interface {
	Save(ctx context.Context, cp *Checkpoint) error
	Load(ctx context.Context, tableName string) (*Checkpoint, error)
	Delete(ctx context.Context, tableName string) error
	SaveProgress(ctx context.Context, progress *SyncProgress) error
}

type IntegrityChecker interface {
	Check(ctx context.Context, cfg *SyncConfig) (*IntegrityResult, error)
}

type ErrorHandler interface {
	Handle(ctx context.Context, record Record, stage PipelineStage, err error) HandleDecision
}

type HandleDecision int

const (
	DecisionRetry HandleDecision = iota
	DecisionSkip
	DecisionAbort
)
