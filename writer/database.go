package writer

import (
	"context"
	"fmt"
	"sync"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/datasource"
)

type DatabaseWriter struct {
	ds     *datasource.GdbxDataSource
	cfg    gsyncx.WriterConfig
	logger gsyncx.SyncLogger
}

func NewDatabaseWriter(ds *datasource.GdbxDataSource, logger gsyncx.SyncLogger) *DatabaseWriter {
	return &DatabaseWriter{ds: ds, logger: gsyncx.ResolveLogger(logger).WithModule("writer")}
}

func NewDatabaseWriterWithConfig(ds *datasource.GdbxDataSource, cfg gsyncx.WriterConfig, logger gsyncx.SyncLogger) *DatabaseWriter {
	return &DatabaseWriter{ds: ds, cfg: cfg, logger: gsyncx.ResolveLogger(logger).WithModule("writer")}
}

func (w *DatabaseWriter) SetConfig(cfg gsyncx.WriterConfig) {
	w.cfg = cfg
}

func (w *DatabaseWriter) TargetType() gsyncx.WriterType {
	return gsyncx.WriterTypeDatabase
}

func (w *DatabaseWriter) Write(ctx context.Context, records []gsyncx.Record) (gsyncx.WriteResult, error) {
	if len(records) == 0 {
		return gsyncx.WriteResult{}, nil
	}
	mode := w.cfg.WriteMode
	if mode == "" {
		mode = gsyncx.WriteModeUpsert
	}
	return w.WriteWithMode(ctx, records, mode)
}

func (w *DatabaseWriter) WriteWithMode(ctx context.Context, records []gsyncx.Record, mode gsyncx.WriteMode) (gsyncx.WriteResult, error) {
	if len(records) == 0 {
		return gsyncx.WriteResult{}, nil
	}

	if w.ds == nil {
		return gsyncx.WriteResult{FailedCount: int64(len(records))}, fmt.Errorf("datasource is nil")
	}

	w.logger.Debug("write started",
		gsyncx.F("mode", mode),
		gsyncx.F("table", w.cfg.TableName),
		gsyncx.F("record_count", len(records)),
	)

	result := gsyncx.WriteResult{}

	switch mode {
	case gsyncx.WriteModeInsert:
		affected, err := w.batchInsert(ctx, records)
		if err != nil {
			result.FailedCount = int64(len(records))
			return result, fmt.Errorf("batch insert failed: %w", err)
		}
		result.SuccessCount = affected
	case gsyncx.WriteModeUpsert:
		affected, err := w.batchUpsert(ctx, records)
		if err != nil {
			result.FailedCount = int64(len(records))
			return result, fmt.Errorf("batch upsert failed: %w", err)
		}
		result.SuccessCount = affected
	default:
		affected, err := w.batchUpsert(ctx, records)
		if err != nil {
			result.FailedCount = int64(len(records))
			return result, err
		}
		result.SuccessCount = affected
	}

	return result, nil
}

func (w *DatabaseWriter) Flush(_ context.Context) error {
	return nil
}

func (w *DatabaseWriter) Close() error {
	if w.ds != nil {
		return w.ds.Close()
	}
	return nil
}

func (w *DatabaseWriter) batchInsert(ctx context.Context, records []gsyncx.Record) (int64, error) {
	data := make([]map[string]any, 0, len(records))
	for _, r := range records {
		data = append(data, r.Data)
	}

	cfg := gsyncx.BatchInsertConfig{
		TableName: w.cfg.TableName,
		Schema:    w.cfg.Schema,
		Fields:    w.cfg.Fields,
		RawFields: w.cfg.RawFields,
		Data:      data,
		BatchSize: len(data),
	}

	if w.ds != nil {
		cfg.DBType = w.ds.GetDBType()
	}

	affected, err := w.ds.BatchInsert(ctx, cfg)
	if err != nil {
		w.logger.Error("batch insert failed",
			gsyncx.F("table", w.cfg.TableName),
			gsyncx.F("error", err),
			gsyncx.F("record_count", len(records)),
		)
		return 0, err
	}

	w.logger.Info("batch insert completed",
		gsyncx.F("table", w.cfg.TableName),
		gsyncx.F("affected", affected),
		gsyncx.F("records", len(records)),
	)

	return affected, nil
}

func (w *DatabaseWriter) batchUpsert(ctx context.Context, records []gsyncx.Record) (int64, error) {
	data := make([]map[string]any, 0, len(records))
	for _, r := range records {
		data = append(data, r.Data)
	}

	cfg := gsyncx.BatchUpsertConfig{
		TableName:  w.cfg.TableName,
		Schema:     w.cfg.Schema,
		PrimaryKey: w.cfg.PrimaryKey,
		Fields:     w.cfg.Fields,
		RawFields:  w.cfg.RawFields,
		Data:       data,
		BatchSize:  len(data),
	}

	if w.ds != nil {
		cfg.DBType = w.ds.GetDBType()
	}

	affected, err := w.ds.BatchUpsert(ctx, cfg)
	if err != nil {
		w.logger.Error("batch upsert failed",
			gsyncx.F("table", w.cfg.TableName),
			gsyncx.F("error", err),
			gsyncx.F("record_count", len(records)),
		)
		return 0, err
	}

	w.logger.Info("batch upsert completed",
		gsyncx.F("table", w.cfg.TableName),
		gsyncx.F("affected", affected),
		gsyncx.F("records", len(records)),
	)

	return affected, nil
}

type BatchWriter struct {
	ds        *datasource.GdbxDataSource
	cfg       gsyncx.WriterConfig
	batchSize int
	buffer    []gsyncx.Record
	logger    gsyncx.SyncLogger
}

func NewBatchWriter(ds *datasource.GdbxDataSource, batchSize int, logger gsyncx.SyncLogger) *BatchWriter {
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &BatchWriter{
		ds:        ds,
		batchSize: batchSize,
		buffer:    make([]gsyncx.Record, 0, batchSize),
		logger:    gsyncx.ResolveLogger(logger).WithModule("writer"),
	}
}

func NewBatchWriterWithConfig(ds *datasource.GdbxDataSource, cfg gsyncx.WriterConfig, batchSize int, logger gsyncx.SyncLogger) *BatchWriter {
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &BatchWriter{
		ds:        ds,
		cfg:       cfg,
		batchSize: batchSize,
		buffer:    make([]gsyncx.Record, 0, batchSize),
		logger:    gsyncx.ResolveLogger(logger).WithModule("writer"),
	}
}

func (w *BatchWriter) SetConfig(cfg gsyncx.WriterConfig) {
	w.cfg = cfg
}

func (w *BatchWriter) TargetType() gsyncx.WriterType {
	return gsyncx.WriterTypeDatabase
}

func (w *BatchWriter) Write(ctx context.Context, records []gsyncx.Record) (gsyncx.WriteResult, error) {
	result := gsyncx.WriteResult{}

	for _, record := range records {
		w.buffer = append(w.buffer, record)

		if len(w.buffer) >= w.batchSize {
			writeResult, err := w.flush(ctx)
			if err != nil {
				return result, err
			}
			result.SuccessCount += writeResult.SuccessCount
			result.FailedCount += writeResult.FailedCount
		}
	}

	return result, nil
}

func (w *BatchWriter) WriteWithMode(ctx context.Context, records []gsyncx.Record, mode gsyncx.WriteMode) (gsyncx.WriteResult, error) {
	return w.Write(ctx, records)
}

func (w *BatchWriter) Flush(ctx context.Context) error {
	_, err := w.flush(ctx)
	return err
}

func (w *BatchWriter) Close() error {
	return nil
}

func (w *BatchWriter) flush(ctx context.Context) (gsyncx.WriteResult, error) {
	if len(w.buffer) == 0 {
		return gsyncx.WriteResult{}, nil
	}

	records := w.buffer
	w.buffer = make([]gsyncx.Record, 0, w.batchSize)

	dbWriter := &DatabaseWriter{ds: w.ds, cfg: w.cfg, logger: w.logger}
	return dbWriter.Write(ctx, records)
}

type DatabaseWriterConfig struct {
	DS  *datasource.GdbxDataSource
	Cfg gsyncx.WriterConfig
}

type WriterFactory func(config interface{}, logger gsyncx.SyncLogger) (gsyncx.Writer, error)

type WriterRegistry struct {
	factories map[gsyncx.WriterType]WriterFactory
	mu        sync.RWMutex
}

var globalWriterRegistry = &WriterRegistry{
	factories: make(map[gsyncx.WriterType]WriterFactory),
}

func init() {
	globalWriterRegistry.Register(gsyncx.WriterTypeDatabase, func(config interface{}, logger gsyncx.SyncLogger) (gsyncx.Writer, error) {
		switch v := config.(type) {
		case *DatabaseWriterConfig:
			return NewDatabaseWriterWithConfig(v.DS, v.Cfg, logger), nil
		case *datasource.GdbxDataSource:
			return NewDatabaseWriter(v, logger), nil
		default:
			return nil, fmt.Errorf("expected *DatabaseWriterConfig or *datasource.GdbxDataSource, got %T", config)
		}
	})
}

func (r *WriterRegistry) Register(writerType gsyncx.WriterType, factory WriterFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[writerType] = factory
}

func (r *WriterRegistry) Create(writerType gsyncx.WriterType, config interface{}, logger gsyncx.SyncLogger) (gsyncx.Writer, error) {
	r.mu.RLock()
	factory, ok := r.factories[writerType]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unsupported writer type: %s", writerType)
	}
	return factory(config, logger)
}

func (r *WriterRegistry) Has(writerType gsyncx.WriterType) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.factories[writerType]
	return ok
}

func (r *WriterRegistry) Types() []gsyncx.WriterType {
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := make([]gsyncx.WriterType, 0, len(r.factories))
	for t := range r.factories {
		types = append(types, t)
	}
	return types
}

func RegisterWriter(writerType gsyncx.WriterType, factory WriterFactory) {
	globalWriterRegistry.Register(writerType, factory)
}

func CreateWriter(writerType gsyncx.WriterType, config interface{}, logger gsyncx.SyncLogger) (gsyncx.Writer, error) {
	return globalWriterRegistry.Create(writerType, config, logger)
}

func RegisteredWriterTypes() []gsyncx.WriterType {
	return globalWriterRegistry.Types()
}
