package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/checkpoint"
	"github.com/dataixcom/gsyncx/datasource"
	"github.com/dataixcom/gsyncx/mapping"
	"github.com/dataixcom/gsyncx/reader"
	"github.com/dataixcom/gsyncx/transform"
	"github.com/dataixcom/gsyncx/writer"
)

type EngineOption func(*SyncEngine)

func WithSourceDS(ds *datasource.GdbxDataSource) EngineOption {
	return func(e *SyncEngine) { e.sourceDS = ds }
}

func WithTargetDS(ds *datasource.GdbxDataSource) EngineOption {
	return func(e *SyncEngine) { e.targetDS = ds }
}

func WithReader(r gsyncx.Reader) EngineOption {
	return func(e *SyncEngine) { e.reader = r }
}

func WithWriter(w gsyncx.Writer) EngineOption {
	return func(e *SyncEngine) { e.writer = w }
}

func WithTransformer(t gsyncx.Transformer) EngineOption {
	return func(e *SyncEngine) { e.transformer = t }
}

func WithMapper(m gsyncx.Mapper) EngineOption {
	return func(e *SyncEngine) { e.mapper = m }
}

func WithCheckpointStore(store gsyncx.CheckpointStore) EngineOption {
	return func(e *SyncEngine) { e.cpStore = store }
}

func WithLogger(logger gsyncx.SyncLogger) EngineOption {
	return func(e *SyncEngine) { e.logger = logger }
}

func WithErrorHandler(handler gsyncx.ErrorHandler) EngineOption {
	return func(e *SyncEngine) { e.errorHandler = handler }
}

func WithHook(point gsyncx.HookPoint, fn gsyncx.HookFunc) EngineOption {
	return func(e *SyncEngine) {
		e.hooks[point] = append(e.hooks[point], fn)
	}
}

func WithIntegrityChecker(checker gsyncx.IntegrityChecker) EngineOption {
	return func(e *SyncEngine) { e.integrityChecker = checker }
}

type SyncEngine struct {
	config           *gsyncx.SyncConfig
	sourceDS         *datasource.GdbxDataSource
	targetDS         *datasource.GdbxDataSource
	reader           gsyncx.Reader
	writer           gsyncx.Writer
	transformer      gsyncx.Transformer
	mapper           gsyncx.Mapper
	cpStore          gsyncx.CheckpointStore
	errorHandler     gsyncx.ErrorHandler
	integrityChecker gsyncx.IntegrityChecker
	logger           gsyncx.SyncLogger
	hooks            map[gsyncx.HookPoint][]gsyncx.HookFunc
	stats            *gsyncx.SyncStatistics
	progress         *gsyncx.SyncProgress
	mu               sync.Mutex
	cancelFunc       context.CancelFunc
	paused           atomic.Bool
}

func NewSyncEngine(config *gsyncx.SyncConfig, opts ...EngineOption) (*SyncEngine, error) {
	if config == nil {
		return nil, fmt.Errorf("sync config is required")
	}

	engine := &SyncEngine{
		config:      config,
		transformer: transform.NewDefaultTransformer(),
		mapper:      mapping.NewFieldMappingEngineWithMappings(nil, nil),
		logger:      gsyncx.ResolveLogger(config.GetLogger()),
		hooks:       make(map[gsyncx.HookPoint][]gsyncx.HookFunc),
		stats:       &gsyncx.SyncStatistics{},
		progress: &gsyncx.SyncProgress{
			Status:      gsyncx.StatusPending,
			StartTime:   time.Now(),
			SourceTable: config.ReaderConfig.TableName,
			TargetTable: config.WriterConfig.TableName,
		},
	}

	for _, opt := range opts {
		opt(engine)
	}

	engine.logger = gsyncx.ResolveLogger(engine.logger)

	if engine.sourceDS == nil {
		dsn := resolveSourceDSN(config)
		if dsn.BuildDSN() != "" {
			ds, err := datasource.NewGdbxDataSource(dsn)
			if err != nil {
				return nil, fmt.Errorf("failed to create source datasource: %w", err)
			}
			ds.SetLogger(engine.logger)
			engine.sourceDS = ds
		}
	}

	if engine.targetDS == nil {
		dsn := resolveTargetDSN(config)
		if dsn.BuildDSN() != "" {
			ds, err := datasource.NewGdbxDataSource(dsn)
			if err != nil {
				return nil, fmt.Errorf("failed to create target datasource: %w", err)
			}
			ds.SetLogger(engine.logger)
			engine.targetDS = ds
		}
	}

	if engine.reader == nil && engine.sourceDS != nil {
		engine.reader = reader.NewDatabaseReader(engine.sourceDS, engine.logger)
	}

	if engine.writer == nil && engine.targetDS != nil {
		engine.writer = writer.NewDatabaseWriterWithConfig(engine.targetDS, engine.config.WriterConfig, engine.logger)
	}

	if engine.mapper == nil && len(config.MappingConfig.Mappings) > 0 {
		engine.mapper = mapping.NewFieldMappingEngine(config.MappingConfig, engine.logger)
	}

	if engine.cpStore == nil {
		engine.cpStore = checkpoint.NewMemoryCheckpointStore()
	}

	return engine, nil
}

func NewSyncEngineFromConfig(config *gsyncx.SyncConfig, logger gsyncx.SyncLogger) (*SyncEngine, error) {
	logger = gsyncx.ResolveLogger(logger)

	sourceDS, err := datasource.NewGdbxDataSource(resolveSourceDSN(config))
	if err != nil {
		return nil, fmt.Errorf("failed to create source datasource: %w", err)
	}
	sourceDS.SetLogger(logger)

	targetDS, err := datasource.NewGdbxDataSource(resolveTargetDSN(config))
	if err != nil {
		return nil, fmt.Errorf("failed to create target datasource: %w", err)
	}
	targetDS.SetLogger(logger)

	var rd gsyncx.Reader
	if config.ReaderConfig.SQL != "" {
		rd = reader.NewSQLReader(sourceDS, logger)
	} else {
		rd = reader.NewDatabaseReader(sourceDS, logger)
	}

	wr := writer.NewDatabaseWriterWithConfig(targetDS, config.WriterConfig, logger)

	var tr gsyncx.Transformer = transform.NewDefaultTransformer()
	if config.TransformConfig.Script != "" {
		scriptMgr := transform.NewScriptManager(logger)
		lang := transform.ScriptLanguage(config.TransformConfig.ScriptLang)
		if lang == "" {
			lang = transform.ScriptLangLua
		}
		tr = transform.NewScriptTransformer(scriptMgr, config.TransformConfig.Script, lang, logger)
	}

	var mp gsyncx.Mapper
	if len(config.MappingConfig.Mappings) > 0 || config.MappingConfig.AutoMapping {
		mp = mapping.NewFieldMappingEngine(config.MappingConfig, logger)
	}

	var cpStore gsyncx.CheckpointStore = checkpoint.NewMemoryCheckpointStore()
	checkpointDir := config.CheckpointDir
	if checkpointDir == "" {
		checkpointDir = config.CheckpointPath
	}
	if checkpointDir != "" {
		fileStore, err := checkpoint.NewFileCheckpointStore(checkpointDir)
		if err != nil {
			return nil, fmt.Errorf("failed to create checkpoint store: %w", err)
		}
		cpStore = fileStore
	}

	return &SyncEngine{
		config:      config,
		sourceDS:    sourceDS,
		targetDS:    targetDS,
		reader:      rd,
		writer:      wr,
		transformer: tr,
		mapper:      mp,
		cpStore:     cpStore,
		logger:      logger,
		hooks:       make(map[gsyncx.HookPoint][]gsyncx.HookFunc),
		stats:       &gsyncx.SyncStatistics{},
		progress: &gsyncx.SyncProgress{
			Status:      gsyncx.StatusPending,
			StartTime:   time.Now(),
			SourceTable: config.ReaderConfig.TableName,
			TargetTable: config.WriterConfig.TableName,
		},
	}, nil
}

func RunSync(ctx context.Context, config *gsyncx.SyncConfig, logger gsyncx.SyncLogger) (*gsyncx.SyncResult, error) {
	engine, err := NewSyncEngineFromConfig(config, logger)
	if err != nil {
		return nil, err
	}
	return engine.Run(ctx)
}

func (e *SyncEngine) Run(ctx context.Context) (*gsyncx.SyncResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	e.cancelFunc = cancel
	defer cancel()

	e.mu.Lock()
	e.progress.Status = gsyncx.StatusRunning
	e.progress.StartTime = time.Now()
	e.mu.Unlock()

	log := e.logger.WithModule("engine")

	log.Info("sync task started",
		gsyncx.F("sync_mode", e.config.SyncMode),
		gsyncx.F("source_table", e.config.ReaderConfig.TableName),
		gsyncx.F("target_table", e.config.WriterConfig.TableName),
		gsyncx.F("batch_size", e.config.BatchSize),
		gsyncx.F("checkpoint_enabled", e.config.CheckpointEnabled),
		gsyncx.F("preview_mode", e.config.PreviewMode),
	)

	result := &gsyncx.SyncResult{
		StartTime: time.Now(),
	}

	if e.reader == nil {
		log.Error("reader is not configured")
		return nil, fmt.Errorf("reader is not configured")
	}
	if e.writer == nil {
		log.Error("writer is not configured")
		return nil, fmt.Errorf("writer is not configured")
	}

	e.injectWriterConfig()

	e.fireHooks(ctx, gsyncx.HookBeforeRead, nil)

	totalCount, err := e.reader.Count(ctx, e.config)
	if err != nil {
		log.Warn("failed to get total count", gsyncx.F("error", err))
	} else {
		e.mu.Lock()
		e.progress.TotalRecords = totalCount
		e.mu.Unlock()
		log.Info("total records counted", gsyncx.F("total", totalCount))
	}

	recordCh, errCh := e.reader.Read(ctx, e.config)

	log.Info("reader started", gsyncx.F("source_table", e.config.ReaderConfig.TableName))

	var totalRead, totalWrite, totalFailed, totalSkipped int64

loop:
	for {
		select {
		case <-ctx.Done():
			result.Status = gsyncx.StatusCancelled
			break loop
		case batch, ok := <-recordCh:
			if !ok {
				recordCh = nil
				break
			}

			for e.paused.Load() {
				time.Sleep(100 * time.Millisecond)
				select {
				case <-ctx.Done():
					result.Status = gsyncx.StatusCancelled
					break loop
				default:
				}
			}

			totalRead += int64(len(batch))
			e.stats.IncReadOK(int64(len(batch)))

			log.Debug("batch read completed",
				gsyncx.F("batch_size", len(batch)),
				gsyncx.F("total_read", totalRead),
			)

			e.fireHooks(ctx, gsyncx.HookAfterRead, batch)

			if e.config.PreviewMode && len(result.PreviewData) < e.config.PreviewLimit {
				remaining := e.config.PreviewLimit - len(result.PreviewData)
				if remaining > len(batch) {
					remaining = len(batch)
				}
				result.PreviewData = append(result.PreviewData, batch[:remaining]...)
			}

			e.mu.Lock()
			e.progress.CurrentStage = gsyncx.StageTransform
			e.mu.Unlock()

			var transformFailed []gsyncx.FailedRecord
			if e.transformer != nil {
				e.fireHooks(ctx, gsyncx.HookBeforeTransform, batch)
				inputCount := len(batch)
				var transformed []gsyncx.Record
				transformed, transformFailed, err = e.transformer.Transform(ctx, batch)
				if err != nil {
					log.Warn("transform failed",
						gsyncx.F("error", err),
						gsyncx.F("batch_size", inputCount),
					)
				} else {
					batch = transformed
					log.Debug("transform completed",
						gsyncx.F("input_count", inputCount),
						gsyncx.F("output_count", len(batch)),
						gsyncx.F("failed_count", len(transformFailed)),
					)
				}
				e.fireHooks(ctx, gsyncx.HookAfterTransform, batch)
			}

			totalFailed += int64(len(transformFailed))
			e.stats.IncTransformFailed(int64(len(transformFailed)))
			e.stats.IncTransformOK(int64(len(batch)))

			e.mu.Lock()
			e.progress.CurrentStage = gsyncx.StageMap
			e.mu.Unlock()

			var mapFailed []gsyncx.FailedRecord
			if e.mapper != nil {
				e.fireHooks(ctx, gsyncx.HookBeforeMap, batch)
				inputCount := len(batch)
				var mapped []gsyncx.Record
				mapped, mapFailed, err = e.mapper.Map(batch)
				if err != nil {
					log.Warn("mapping failed",
						gsyncx.F("error", err),
					)
				} else {
					batch = mapped
					log.Debug("mapping completed",
						gsyncx.F("input_count", inputCount),
						gsyncx.F("output_count", len(batch)),
						gsyncx.F("failed_count", len(mapFailed)),
					)
				}
				e.fireHooks(ctx, gsyncx.HookAfterMap, batch)
			}

			totalFailed += int64(len(mapFailed))
			e.stats.IncMappingFailed(int64(len(mapFailed)))
			e.stats.IncMappingOK(int64(len(batch)))

			if e.config.PreviewMode {
				totalSkipped += int64(len(batch))
				e.stats.IncSkippedTotal(int64(len(batch)))
			} else {
				e.mu.Lock()
				e.progress.CurrentStage = gsyncx.StageWrite
				e.mu.Unlock()

				e.fireHooks(ctx, gsyncx.HookBeforeWrite, batch)

				writeResult, writeErr := e.writeWithRetry(ctx, batch)
				if writeErr != nil {
					totalFailed += int64(len(batch))
					e.stats.IncWriteFailed(int64(len(batch)))
					log.Warn("write failed",
						gsyncx.F("error", writeErr),
						gsyncx.F("batch_size", len(batch)),
					)
				} else {
					totalWrite += writeResult.SuccessCount
					totalFailed += writeResult.FailedCount
					totalSkipped += writeResult.SkippedCount
					e.stats.IncWriteOK(writeResult.SuccessCount)
					e.stats.IncWriteFailed(writeResult.FailedCount)
					log.Debug("write completed",
						gsyncx.F("success_count", writeResult.SuccessCount),
						gsyncx.F("failed_count", writeResult.FailedCount),
						gsyncx.F("total_written", totalWrite),
					)
				}

				e.fireHooks(ctx, gsyncx.HookAfterWrite, batch)
			}

			e.mu.Lock()
			e.progress.SyncedRecords = totalWrite
			e.progress.FailedRecords = totalFailed
			e.progress.SkippedRecords = totalSkipped
			if e.progress.TotalRecords > 0 {
				e.progress.Percent = float64(totalWrite+totalFailed) / float64(e.progress.TotalRecords) * 100
			}
			e.mu.Unlock()

			if e.config.CheckpointEnabled && e.cpStore != nil {
				e.saveCheckpoint(ctx, totalRead, totalWrite)
			}

		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				break
			}
			if err != nil {
				result.Status = gsyncx.StatusFailed
				result.Error = err
				result.EndTime = time.Now()
				return result, err
			}
			errCh = nil
		}

		if recordCh == nil && errCh == nil {
			break loop
		}
	}

	if result.Status == "" {
		result.Status = gsyncx.StatusCompleted
	}

	result.TotalRead = totalRead
	result.TotalWritten = totalWrite
	result.TotalFailed = totalFailed
	result.TotalSkipped = totalSkipped
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	log.Info("sync task completed",
		gsyncx.F("status", result.Status),
		gsyncx.F("total_read", totalRead),
		gsyncx.F("total_written", totalWrite),
		gsyncx.F("total_failed", totalFailed),
		gsyncx.F("total_skipped", totalSkipped),
		gsyncx.F("duration", result.Duration.String()),
	)

	if e.config.IntegrityCheck != "" && e.config.IntegrityCheck != gsyncx.IntegrityCheckNone {
		if e.integrityChecker != nil {
			ir, err := e.integrityChecker.Check(ctx, e.config)
			if err != nil {
				log.Warn("integrity check failed", gsyncx.F("error", err))
			} else {
				result.IntegrityResult = ir
				if !ir.Passed {
					log.Warn("integrity check did not pass",
						gsyncx.F("mode", ir.Mode),
						gsyncx.F("details", ir.Details),
					)
				} else {
					log.Info("integrity check passed",
						gsyncx.F("mode", ir.Mode),
					)
				}
			}
		}
	}

	e.mu.Lock()
	e.progress.Status = result.Status
	e.progress.EndTime = time.Now()
	e.mu.Unlock()

	e.fireHooks(ctx, gsyncx.HookOnComplete, nil)

	return result, nil
}

func (e *SyncEngine) writeWithRetry(ctx context.Context, records []gsyncx.Record) (gsyncx.WriteResult, error) {
	maxAttempts := e.config.RetryMaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return gsyncx.WriteResult{}, ctx.Err()
		default:
		}

		writeResult, err := e.writer.Write(ctx, records)
		if err == nil {
			return writeResult, nil
		}

		lastErr = err
		e.fireHooks(ctx, gsyncx.HookOnRetry, records)

		if attempt < maxAttempts-1 {
			delay := e.config.RetryDelay
			if delay <= 0 {
				delay = time.Second
			}
			select {
			case <-ctx.Done():
				return gsyncx.WriteResult{}, ctx.Err()
			case <-time.After(delay):
			}
		}
	}

	return gsyncx.WriteResult{FailedCount: int64(len(records))}, lastErr
}

func (e *SyncEngine) fireHooks(ctx context.Context, point gsyncx.HookPoint, records []gsyncx.Record) {
	hooks, ok := e.hooks[point]
	if !ok {
		return
	}

	hctx := &gsyncx.HookContext{
		Point:    point,
		Records:  records,
		Config:   e.config,
		Progress: e.progress,
	}

	for _, fn := range hooks {
		if err := fn(ctx, hctx); err != nil {
			e.logger.WithModule("engine").Warn("hook execution failed",
				gsyncx.F("hook_point", point),
				gsyncx.F("error", err),
			)
		}
	}
}

func (e *SyncEngine) Stop() {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
	e.mu.Lock()
	e.progress.Status = gsyncx.StatusCancelled
	e.mu.Unlock()
}

func (e *SyncEngine) Pause() {
	e.paused.Store(true)
	e.mu.Lock()
	e.progress.Status = gsyncx.StatusPaused
	e.mu.Unlock()
}

func (e *SyncEngine) Resume() {
	e.paused.Store(false)
	e.mu.Lock()
	e.progress.Status = gsyncx.StatusRunning
	e.mu.Unlock()
}

func (e *SyncEngine) GetProgress() *gsyncx.SyncProgress {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.progress
}

func (e *SyncEngine) GetConfig() *gsyncx.SyncConfig {
	return e.config
}

func (e *SyncEngine) GetStats() *gsyncx.SyncStatistics {
	return e.stats
}

func (e *SyncEngine) SetReader(r gsyncx.Reader)                       { e.reader = r }
func (e *SyncEngine) SetWriter(w gsyncx.Writer)                       { e.writer = w }
func (e *SyncEngine) SetTransformer(t gsyncx.Transformer)             { e.transformer = t }
func (e *SyncEngine) SetMapper(m gsyncx.Mapper)                       { e.mapper = m }
func (e *SyncEngine) SetCheckpointStore(store gsyncx.CheckpointStore) { e.cpStore = store }
func (e *SyncEngine) SetSourceDS(ds *datasource.GdbxDataSource)       { e.sourceDS = ds }
func (e *SyncEngine) SetTargetDS(ds *datasource.GdbxDataSource)       { e.targetDS = ds }
func (e *SyncEngine) SetLogger(logger gsyncx.SyncLogger)              { e.logger = gsyncx.ResolveLogger(logger) }
func (e *SyncEngine) SetErrorHandler(handler gsyncx.ErrorHandler)     { e.errorHandler = handler }
func (e *SyncEngine) SetIntegrityChecker(checker gsyncx.IntegrityChecker) {
	e.integrityChecker = checker
}

func (e *SyncEngine) AddHook(point gsyncx.HookPoint, fn gsyncx.HookFunc) {
	e.hooks[point] = append(e.hooks[point], fn)
}

func (e *SyncEngine) SwitchToFullSync() {
	e.config.SwitchToFullSync()
	e.logger.WithModule("engine").Info("switched to full sync mode")
}

func (e *SyncEngine) SwitchToIncrementalSync(field *gsyncx.Field, strategy gsyncx.IncrementalStrategy) {
	e.config.SwitchToIncrementalSync(field, strategy)
	e.logger.WithModule("engine").Info("switched to incremental sync mode",
		gsyncx.F("field", field.GetFieldName()),
		gsyncx.F("strategy", strategy),
	)
}

func (e *SyncEngine) SwitchToRealtimeSync() {
	e.config.SwitchToRealtimeSync()
	e.logger.WithModule("engine").Info("switched to realtime sync mode")
}

func resolveSourceDSN(config *gsyncx.SyncConfig) gsyncx.DSNConfig {
	if config.ReaderConfig.DSNConfig != nil {
		return *config.ReaderConfig.DSNConfig
	}
	return config.SourceDSN
}

func resolveTargetDSN(config *gsyncx.SyncConfig) gsyncx.DSNConfig {
	if config.WriterConfig.DSNConfig != nil {
		return *config.WriterConfig.DSNConfig
	}
	return config.TargetDSN
}

type writerConfigInjector interface {
	SetConfig(cfg gsyncx.WriterConfig)
}

func (e *SyncEngine) injectWriterConfig() {
	if e.config.WriterConfig.TableName == "" {
		return
	}
	if injector, ok := e.writer.(writerConfigInjector); ok {
		injector.SetConfig(e.config.WriterConfig)
	}
}

func (e *SyncEngine) saveCheckpoint(ctx context.Context, totalRead, totalWrite int64) {
	tableName := e.config.ReaderConfig.TableName
	if tableName == "" {
		return
	}

	cp := &gsyncx.Checkpoint{
		TableName:    tableName,
		FieldName:    "",
		LastSyncTime: time.Now(),
		BatchNum:     int(totalWrite / int64(e.config.BatchSize)),
	}

	if e.config.IncrementalField != nil {
		cp.FieldName = e.config.IncrementalField.GetFieldName()
	}

	if err := e.cpStore.Save(ctx, cp); err != nil {
		e.logger.WithModule("engine").Warn("failed to save checkpoint",
			gsyncx.F("table", tableName),
			gsyncx.F("error", err),
		)
	}

	if err := e.cpStore.SaveProgress(ctx, e.progress); err != nil {
		e.logger.WithModule("engine").Warn("failed to save progress",
			gsyncx.F("error", err),
		)
	}
}
