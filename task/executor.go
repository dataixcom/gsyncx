package task

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/datasource"
	"github.com/dataixcom/gsyncx/engine"
	"github.com/dataixcom/gsyncx/mapping"
	"github.com/dataixcom/gsyncx/reader"
	"github.com/dataixcom/gsyncx/transform"
	"github.com/dataixcom/gsyncx/writer"
)

type ReaderFactory func(cfg *TaskConfig) (gsyncx.Reader, error)
type WriterFactory func(cfg *TaskConfig) (gsyncx.Writer, error)
type TransformerFactory func(cfg *TaskConfig) (gsyncx.Transformer, error)
type MapperFactory func(cfg *TaskConfig) (gsyncx.Mapper, error)

type TaskExecutor struct {
	config              *TaskConfig
	logger              gsyncx.SyncLogger
	configPath          string
	status              TaskStatus
	result              *TaskResult
	mu                  sync.RWMutex
	cancelFunc          context.CancelFunc
	readerFactories     map[string]ReaderFactory
	writerFactories     map[string]WriterFactory
	transformerFactories map[string]TransformerFactory
	mapperFactories     map[string]MapperFactory
	hooks               map[gsyncx.HookPoint][]gsyncx.HookFunc
}

type TaskExecutorOption func(*TaskExecutor)

func WithTaskLogger(logger gsyncx.SyncLogger) TaskExecutorOption {
	return func(e *TaskExecutor) { e.logger = logger }
}

func WithTaskConfigPath(path string) TaskExecutorOption {
	return func(e *TaskExecutor) { e.configPath = path }
}

func WithReaderFactory(readerType string, factory ReaderFactory) TaskExecutorOption {
	return func(e *TaskExecutor) { e.readerFactories[readerType] = factory }
}

func WithWriterFactory(writerType string, factory WriterFactory) TaskExecutorOption {
	return func(e *TaskExecutor) { e.writerFactories[writerType] = factory }
}

func WithTransformerFactory(transformType string, factory TransformerFactory) TaskExecutorOption {
	return func(e *TaskExecutor) { e.transformerFactories[transformType] = factory }
}

func WithMapperFactory(mapperType string, factory MapperFactory) TaskExecutorOption {
	return func(e *TaskExecutor) { e.mapperFactories[mapperType] = factory }
}

func WithTaskHook(point gsyncx.HookPoint, fn gsyncx.HookFunc) TaskExecutorOption {
	return func(e *TaskExecutor) {
		e.hooks[point] = append(e.hooks[point], fn)
	}
}

func NewTaskExecutor(cfg *TaskConfig, opts ...TaskExecutorOption) *TaskExecutor {
	e := &TaskExecutor{
		config:              cfg,
		logger:              gsyncx.NewNopLogger(),
		status:              TaskStatusPending,
		readerFactories:     make(map[string]ReaderFactory),
		writerFactories:     make(map[string]WriterFactory),
		transformerFactories: make(map[string]TransformerFactory),
		mapperFactories:     make(map[string]MapperFactory),
		hooks:               make(map[gsyncx.HookPoint][]gsyncx.HookFunc),
	}

	e.registerBuiltinFactories()

	for _, opt := range opts {
		opt(e)
	}

	e.logger = gsyncx.ResolveLogger(e.logger)

	return e
}

func (e *TaskExecutor) registerBuiltinFactories() {
	e.readerFactories["database"] = e.createDatabaseReader
	e.readerFactories["sql"] = e.createSQLReader
	e.readerFactories["redis_stream"] = e.createRedisStreamReader

	e.writerFactories["database"] = e.createDatabaseWriter

	e.transformerFactories["default"] = e.createDefaultTransformer
	e.transformerFactories["script"] = e.createScriptTransformer
	e.transformerFactories["redis_message"] = e.createRedisMessageTransformer

	e.mapperFactories["default"] = e.createDefaultMapper
}

func (e *TaskExecutor) Execute(ctx context.Context) (*TaskResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	e.cancelFunc = cancel
	defer cancel()

	e.mu.Lock()
	e.status = TaskStatusRunning
	e.result = &TaskResult{
		JobID:      e.config.JobID,
		JobName:    e.config.JobName,
		Status:     TaskStatusRunning,
		StartTime:  time.Now(),
		ConfigPath: e.configPath,
	}
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		e.result.EndTime = time.Now()
		e.result.Duration = e.result.EndTime.Sub(e.result.StartTime)
		e.mu.Unlock()
	}()

	if err := e.config.Validate(); err != nil {
		e.setStatus(TaskStatusFailed)
		e.result.Error = err.Error()
		return e.result, fmt.Errorf("config validation failed: %w", err)
	}

	syncConfig := e.buildSyncConfig()

	rd, err := e.createReader()
	if err != nil {
		e.setStatus(TaskStatusFailed)
		e.result.Error = err.Error()
		return e.result, fmt.Errorf("failed to create reader: %w", err)
	}

	wr, err := e.createWriter()
	if err != nil {
		e.setStatus(TaskStatusFailed)
		e.result.Error = err.Error()
		return e.result, fmt.Errorf("failed to create writer: %w", err)
	}

	engineOpts := []engine.EngineOption{
		engine.WithReader(rd),
		engine.WithWriter(wr),
		engine.WithLogger(e.logger),
	}

	tr, err := e.createTransformer()
	if err == nil && tr != nil {
		engineOpts = append(engineOpts, engine.WithTransformer(tr))
	}

	mp, err := e.createMapper()
	if err == nil && mp != nil {
		engineOpts = append(engineOpts, engine.WithMapper(mp))
	}

	for point, fns := range e.hooks {
		for _, fn := range fns {
			engineOpts = append(engineOpts, engine.WithHook(point, fn))
		}
	}

	eng, err := engine.NewSyncEngine(syncConfig, engineOpts...)
	if err != nil {
		e.setStatus(TaskStatusFailed)
		e.result.Error = err.Error()
		return e.result, fmt.Errorf("failed to create sync engine: %w", err)
	}

	syncResult, err := eng.Run(ctx)
	if err != nil {
		e.setStatus(TaskStatusFailed)
		e.result.Error = err.Error()
		e.result.SyncResult = syncResult
		return e.result, err
	}

	e.mu.Lock()
	e.result.SyncResult = syncResult
	if syncResult.Status == gsyncx.StatusCompleted {
		e.result.Status = TaskStatusCompleted
		e.status = TaskStatusCompleted
	} else if syncResult.Status == gsyncx.StatusCancelled {
		e.result.Status = TaskStatusCancelled
		e.status = TaskStatusCancelled
	} else {
		e.result.Status = TaskStatusFailed
		e.status = TaskStatusFailed
	}
	e.mu.Unlock()

	return e.result, nil
}

func (e *TaskExecutor) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
	e.status = TaskStatusCancelled
	if e.result != nil {
		e.result.Status = TaskStatusCancelled
	}
}

func (e *TaskExecutor) GetStatus() TaskStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.status
}

func (e *TaskExecutor) GetResult() *TaskResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.result
}

func (e *TaskExecutor) GetProgress() *gsyncx.SyncProgress {
	return nil
}

func (e *TaskExecutor) RegisterReader(readerType string, factory ReaderFactory) {
	e.readerFactories[readerType] = factory
}

func (e *TaskExecutor) RegisterWriter(writerType string, factory WriterFactory) {
	e.writerFactories[writerType] = factory
}

func (e *TaskExecutor) RegisterTransformer(transformType string, factory TransformerFactory) {
	e.transformerFactories[transformType] = factory
}

func (e *TaskExecutor) RegisterMapper(mapperType string, factory MapperFactory) {
	e.mapperFactories[mapperType] = factory
}

func (e *TaskExecutor) setStatus(status TaskStatus) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.status = status
	if e.result != nil {
		e.result.Status = status
	}
}

func (e *TaskExecutor) buildSyncConfig() *gsyncx.SyncConfig {
	opts := []gsyncx.Option{
		gsyncx.WithReaderConfig(e.config.Reader.ToGdbxReaderConfig()),
		gsyncx.WithWriterConfig(e.config.Writer.ToGdbxWriterConfig()),
	}

	if e.config.Setting != nil {
		if e.config.Setting.SyncMode != "" {
			opts = append(opts, gsyncx.WithSyncMode(e.config.Setting.SyncMode))
		}
		if e.config.Setting.BatchSize > 0 {
			opts = append(opts, gsyncx.WithBatchSize(e.config.Setting.BatchSize))
		}
		if e.config.Setting.Parallelism > 0 {
			opts = append(opts, gsyncx.WithParallelism(e.config.Setting.Parallelism))
		}
		if e.config.Setting.RetryMaxAttempts > 0 {
			opts = append(opts, gsyncx.WithRetry(e.config.Setting.RetryMaxAttempts, e.config.Setting.RetryDelay))
		}
		if e.config.Setting.ContinueOnError {
			opts = append(opts, gsyncx.WithContinueOnError(true))
		}
		if e.config.Setting.PreviewMode {
			opts = append(opts, gsyncx.WithPreviewMode(e.config.Setting.PreviewLimit))
		}
		if e.config.Setting.IntegrityCheck != "" {
			opts = append(opts, gsyncx.WithIntegrityCheck(gsyncx.IntegrityCheckMode(e.config.Setting.IntegrityCheck)))
		}
		if e.config.Setting.CheckpointEnabled {
			opts = append(opts, gsyncx.WithCheckpoint(true, e.config.Setting.CheckpointPath))
		}
		if e.config.Setting.IncrementalField != nil {
			opts = append(opts, gsyncx.WithIncrementalField(
				&gsyncx.Field{FieldName: e.config.Setting.IncrementalField.FieldName},
				gsyncx.IncrementalStrategy(e.config.Setting.IncrementalField.Strategy),
			))
		}
	}

	if e.config.Mapping != nil {
		opts = append(opts, gsyncx.WithMappingConfig(e.config.Mapping.ToGdbxMappingConfig()))
		if e.config.Mapping.AutoMapping {
			opts = append(opts, gsyncx.WithAutoMapping(true))
		}
	}

	opts = append(opts, gsyncx.WithLogger(e.logger))

	return gsyncx.NewSyncConfig(opts...)
}

func (e *TaskExecutor) createReader() (gsyncx.Reader, error) {
	factory, ok := e.readerFactories[e.config.Reader.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported reader type: %s", e.config.Reader.Type)
	}
	return factory(e.config)
}

func (e *TaskExecutor) createWriter() (gsyncx.Writer, error) {
	factory, ok := e.writerFactories[e.config.Writer.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported writer type: %s", e.config.Writer.Type)
	}
	return factory(e.config)
}

func (e *TaskExecutor) createTransformer() (gsyncx.Transformer, error) {
	if e.config.Transform == nil {
		return transform.NewDefaultTransformer(), nil
	}

	transformType := e.config.Transform.Type
	if transformType == "" {
		if len(e.config.Transform.FieldMapping) > 0 {
			transformType = "redis_message"
		} else if e.config.Transform.Script != "" || e.config.Transform.ScriptPath != "" {
			transformType = "script"
		} else {
			transformType = "default"
		}
	}

	factory, ok := e.transformerFactories[transformType]
	if !ok {
		return transform.NewDefaultTransformer(), nil
	}
	return factory(e.config)
}

func (e *TaskExecutor) createMapper() (gsyncx.Mapper, error) {
	if e.config.Mapping == nil {
		return mapping.NewFieldMappingEngineWithMappings(nil, nil), nil
	}

	factory, ok := e.mapperFactories["default"]
	if !ok {
		return mapping.NewFieldMappingEngineWithMappings(nil, nil), nil
	}
	return factory(e.config)
}

func (e *TaskExecutor) createDatabaseReader(cfg *TaskConfig) (gsyncx.Reader, error) {
	if cfg.Reader.DSNConfig == nil {
		return nil, fmt.Errorf("dsn_config is required for database reader")
	}
	ds, err := datasource.NewGdbxDataSource(cfg.Reader.DSNConfig.ToGdbxDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to create datasource: %w", err)
	}
	return reader.NewDatabaseReader(ds, e.logger), nil
}

func (e *TaskExecutor) createSQLReader(cfg *TaskConfig) (gsyncx.Reader, error) {
	if cfg.Reader.DSNConfig == nil {
		return nil, fmt.Errorf("dsn_config is required for sql reader")
	}
	ds, err := datasource.NewGdbxDataSource(cfg.Reader.DSNConfig.ToGdbxDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to create datasource: %w", err)
	}
	return reader.NewSQLReader(ds, e.logger), nil
}

func (e *TaskExecutor) createRedisStreamReader(cfg *TaskConfig) (gsyncx.Reader, error) {
	if cfg.Reader.Redis == nil {
		return nil, fmt.Errorf("redis config is required for redis_stream reader")
	}
	rd, err := reader.NewRedisStreamReader(&reader.RedisStreamConfig{
		Addr:          cfg.Reader.Redis.Addr,
		Password:      cfg.Reader.Redis.Password,
		DB:            cfg.Reader.Redis.DB,
		Stream:        cfg.Reader.Redis.Stream,
		ConsumerGroup: cfg.Reader.Redis.ConsumerGroup,
		ConsumerName:  cfg.Reader.Redis.ConsumerName,
		Count:         cfg.Reader.Redis.Count,
		Block:         cfg.Reader.Redis.Block,
		BatchSize:     cfg.Reader.Redis.BatchSize,
		AutoCreate:    cfg.Reader.Redis.AutoCreate,
		StartID:       cfg.Reader.Redis.StartID,
	}, e.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create redis stream reader: %w", err)
	}
	return rd, nil
}

func (e *TaskExecutor) createDatabaseWriter(cfg *TaskConfig) (gsyncx.Writer, error) {
	if cfg.Writer.DSNConfig == nil {
		return nil, fmt.Errorf("dsn_config is required for database writer")
	}
	ds, err := datasource.NewGdbxDataSource(cfg.Writer.DSNConfig.ToGdbxDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to create datasource: %w", err)
	}
	return writer.NewDatabaseWriterWithConfig(ds, cfg.Writer.ToGdbxWriterConfig(), e.logger), nil
}

func (e *TaskExecutor) createDefaultTransformer(cfg *TaskConfig) (gsyncx.Transformer, error) {
	return transform.NewDefaultTransformer(), nil
}

func (e *TaskExecutor) createScriptTransformer(cfg *TaskConfig) (gsyncx.Transformer, error) {
	scriptMgr := transform.NewScriptManager(e.logger)
	lang := transform.ScriptLangLua
	if cfg.Transform.ScriptLang != "" {
		lang = transform.ScriptLanguage(cfg.Transform.ScriptLang)
	}
	return transform.NewScriptTransformer(scriptMgr, cfg.Transform.Script, lang, e.logger), nil
}

func (e *TaskExecutor) createRedisMessageTransformer(cfg *TaskConfig) (gsyncx.Transformer, error) {
	var fieldMapping map[string]string
	if cfg.Transform != nil {
		fieldMapping = cfg.Transform.FieldMapping
	}
	return transform.NewRedisMessageTransformer(fieldMapping, e.logger), nil
}

func (e *TaskExecutor) createDefaultMapper(cfg *TaskConfig) (gsyncx.Mapper, error) {
	if cfg.Mapping == nil {
		return mapping.NewFieldMappingEngineWithMappings(nil, nil), nil
	}
	return mapping.NewFieldMappingEngine(cfg.Mapping.ToGdbxMappingConfig(), e.logger), nil
}

func ExecuteTask(ctx context.Context, configPath string, opts ...TaskExecutorOption) (*TaskResult, error) {
	cfg, err := LoadTaskConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load task config: %w", err)
	}

	allOpts := append([]TaskExecutorOption{WithTaskConfigPath(configPath)}, opts...)
	executor := NewTaskExecutor(cfg, allOpts...)
	return executor.Execute(ctx)
}

func ExecuteTaskFromBytes(ctx context.Context, data []byte, opts ...TaskExecutorOption) (*TaskResult, error) {
	cfg, err := ParseTaskConfig(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse task config: %w", err)
	}

	executor := NewTaskExecutor(cfg, opts...)
	return executor.Execute(ctx)
}
