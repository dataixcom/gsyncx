package reader

import (
	"context"
	"fmt"
	"sync"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/datasource"
	"github.com/dataixcom/gsyncx/sqlparser"
)

type DatabaseReader struct {
	ds     *datasource.GdbxDataSource
	logger gsyncx.SyncLogger
}

func NewDatabaseReader(ds *datasource.GdbxDataSource, logger gsyncx.SyncLogger) *DatabaseReader {
	return &DatabaseReader{ds: ds, logger: gsyncx.ResolveLogger(logger)}
}

func (r *DatabaseReader) SourceType() gsyncx.ReaderType {
	return gsyncx.ReaderTypeDatabase
}

func (r *DatabaseReader) Read(ctx context.Context, cfg *gsyncx.SyncConfig) (<-chan []gsyncx.Record, <-chan error) {
	recordCh := make(chan []gsyncx.Record, cfg.Parallelism)
	errCh := make(chan error, 1)

	if r.ds == nil {
		close(recordCh)
		errCh <- fmt.Errorf("datasource is not configured")
		close(errCh)
		return recordCh, errCh
	}

	go func() {
		defer close(recordCh)
		defer close(errCh)

		batchSize := cfg.BatchSize
		if batchSize <= 0 {
			batchSize = 1000
		}

		offset := 0
		totalRead := int64(0)

		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
			}

			selectCfg := r.buildSelectConfig(cfg, batchSize, offset)

			rows, err := r.ds.Query(ctx, selectCfg)
			if err != nil {
				errCh <- fmt.Errorf("read batch at offset %d failed: %w", offset, err)
				return
			}

			if len(rows) == 0 {
				errCh <- nil
				return
			}

			records := make([]gsyncx.Record, 0, len(rows))
			for _, row := range rows {
				record := gsyncx.Record{Data: row}
				if pk := cfg.ReaderConfig.PrimaryKey; pk != nil {
					if pkVal, ok := row[pk.GetFieldName()]; ok {
						record.Meta.SourcePK = pkVal
					}
				}
				records = append(records, record)
			}

			select {
			case recordCh <- records:
				totalRead += int64(len(records))
				r.logger.Debug("read batch",
					gsyncx.F("offset", offset),
					gsyncx.F("count", len(records)),
					gsyncx.F("total", totalRead),
				)
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}

			if len(rows) < batchSize {
				errCh <- nil
				return
			}

			offset += batchSize
		}
	}()

	return recordCh, errCh
}

func (r *DatabaseReader) Count(ctx context.Context, cfg *gsyncx.SyncConfig) (int64, error) {
	if r.ds == nil {
		return 0, fmt.Errorf("datasource is not configured")
	}
	selectCfg := r.buildSelectConfig(cfg, 0, 0)
	return r.ds.Count(ctx, selectCfg)
}

func (r *DatabaseReader) GetSplitKeys(ctx context.Context, cfg *gsyncx.SyncConfig) ([]gsyncx.SplitKey, error) {
	if r.ds == nil {
		return nil, fmt.Errorf("datasource is not configured")
	}
	if cfg.ReaderConfig.PrimaryKey == nil {
		return nil, fmt.Errorf("primary key is required for split key calculation")
	}

	pkField := cfg.ReaderConfig.PrimaryKey.GetFieldName()
	dbType := r.ds.GetDBType()
	escapedTable := gsyncx.FormatTableName(cfg.ReaderConfig.TableName, dbType)
	escapedPK := gsyncx.FormatFieldName(pkField, dbType)

	minMaxSQL := fmt.Sprintf("SELECT MIN(%s) as min_val, MAX(%s) as max_val, COUNT(*) as total FROM %s",
		escapedPK, escapedPK, escapedTable)

	rows, err := r.ds.RawQuery(ctx, minMaxSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to get split key range: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	row := rows[0]
	totalRows := gsyncx.AnyToInt64(row["total"])

	if totalRows == 0 {
		return nil, nil
	}

	parallelism := cfg.Parallelism
	if parallelism <= 0 {
		parallelism = 4
	}

	if totalRows < int64(parallelism*1000) {
		return []gsyncx.SplitKey{{
			FieldName: pkField,
			MinValue:  row["min_val"],
			MaxValue:  row["max_val"],
			TotalRows: totalRows,
		}}, nil
	}

	return []gsyncx.SplitKey{{
		FieldName: pkField,
		MinValue:  row["min_val"],
		MaxValue:  row["max_val"],
		TotalRows: totalRows,
	}}, nil
}

func (r *DatabaseReader) Close() error {
	if r.ds != nil {
		return r.ds.Close()
	}
	return nil
}

func (r *DatabaseReader) buildSelectConfig(cfg *gsyncx.SyncConfig, limit, offset int) gsyncx.SelectConfig {
	selectCfg := gsyncx.SelectConfig{
		TableName:  cfg.ReaderConfig.TableName,
		Schema:     cfg.ReaderConfig.Schema,
		Fields:     cfg.ReaderConfig.Fields,
		RawFields:  cfg.ReaderConfig.RawFields,
		Conditions: cfg.ReaderConfig.Conditions,
		Limit:      limit,
		Offset:     offset,
	}

	var dbType gsyncx.DBType
	if r.ds != nil {
		dbType = r.ds.GetDBType()
	}

	if cfg.ReaderConfig.PrimaryKey != nil {
		selectCfg.OrderBy = gsyncx.FormatFieldName(cfg.ReaderConfig.PrimaryKey.GetFieldName(), dbType) + " ASC"
	}

	if cfg.ReaderConfig.WhereClause != "" {
		selectCfg.RawConditions = cfg.ReaderConfig.WhereClause
	}

	if cfg.SyncMode == gsyncx.SyncModeIncremental && cfg.IncrementalField != nil {
		helper := gsyncx.NewIncrementalHelper(cfg.IncrementalStrategy, cfg.IncrementalField.GetFieldName())
		incCondition := helper.BuildCondition(cfg)
		if incCondition != "" {
			if selectCfg.RawConditions != "" {
				selectCfg.RawConditions = selectCfg.RawConditions + " AND " + incCondition
			} else {
				selectCfg.RawConditions = incCondition
			}
		}
	}

	return selectCfg
}

type SQLReader struct {
	ds     *datasource.GdbxDataSource
	logger gsyncx.SyncLogger
}

func NewSQLReader(ds *datasource.GdbxDataSource, logger gsyncx.SyncLogger) *SQLReader {
	return &SQLReader{ds: ds, logger: gsyncx.ResolveLogger(logger)}
}

func (r *SQLReader) SourceType() gsyncx.ReaderType {
	return gsyncx.ReaderTypeSQL
}

func (r *SQLReader) Read(ctx context.Context, cfg *gsyncx.SyncConfig) (<-chan []gsyncx.Record, <-chan error) {
	recordCh := make(chan []gsyncx.Record, cfg.Parallelism)
	errCh := make(chan error, 1)

	go func() {
		defer close(recordCh)
		defer close(errCh)

		sqlStr := cfg.ReaderConfig.SQL
		if sqlStr == "" {
			errCh <- fmt.Errorf("SQLReader requires SQL in ReaderConfig")
			return
		}

		batchSize := cfg.BatchSize
		if batchSize <= 0 {
			batchSize = 1000
		}

		parser := sqlparser.NewSQLParser()
		parsed, err := parser.Parse(sqlStr)
		if err != nil {
			errCh <- fmt.Errorf("failed to parse SQL: %w", err)
			return
		}

		if parsed.OrderBy == "" {
			if len(parsed.Fields) > 0 {
				parsed.OrderBy = parsed.Fields[0] + " ASC"
			}
		}

		offset := 0
		totalRead := int64(0)

		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
			}

			batchSQL := parser.SetLimit(parser.Rebuild(parsed), batchSize)
			if offset > 0 {
				batchSQL = fmt.Sprintf("%s OFFSET %d", batchSQL, offset)
			}

			rows, err := r.ds.RawQuery(ctx, batchSQL)
			if err != nil {
				errCh <- fmt.Errorf("sql read at offset %d failed: %w", offset, err)
				return
			}

			if len(rows) == 0 {
				errCh <- nil
				return
			}

			records := make([]gsyncx.Record, 0, len(rows))
			for _, row := range rows {
				record := gsyncx.Record{Data: row}
				if pk := cfg.ReaderConfig.PrimaryKey; pk != nil {
					if pkVal, ok := row[pk.GetFieldName()]; ok {
						record.Meta.SourcePK = pkVal
					}
				}
				records = append(records, record)
			}

			select {
			case recordCh <- records:
				totalRead += int64(len(records))
				r.logger.Debug("sql read batch",
					gsyncx.F("offset", offset),
					gsyncx.F("count", len(records)),
					gsyncx.F("total", totalRead),
				)
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}

			if len(rows) < batchSize {
				errCh <- nil
				return
			}

			offset += batchSize
		}
	}()

	return recordCh, errCh
}

func (r *SQLReader) Count(ctx context.Context, cfg *gsyncx.SyncConfig) (int64, error) {
	sqlStr := cfg.ReaderConfig.SQL
	if sqlStr == "" {
		return 0, fmt.Errorf("SQLReader requires SQL in ReaderConfig")
	}

	countSQL := fmt.Sprintf("SELECT COUNT(*) as cnt FROM (%s) AS count_subquery", sqlStr)
	rows, err := r.ds.RawQuery(ctx, countSQL)
	if err != nil {
		return 0, fmt.Errorf("sql count failed: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return gsyncx.AnyToInt64(rows[0]["cnt"]), nil
}

func (r *SQLReader) GetSplitKeys(ctx context.Context, cfg *gsyncx.SyncConfig) ([]gsyncx.SplitKey, error) {
	return nil, fmt.Errorf("SQLReader does not support split keys")
}

func (r *SQLReader) Close() error {
	if r.ds != nil {
		return r.ds.Close()
	}
	return nil
}

func (r *SQLReader) ReadWithIncremental(ctx context.Context, cfg *gsyncx.SyncConfig, incField string, lastValue interface{}) (<-chan []gsyncx.Record, <-chan error) {
	if cfg.ReaderConfig.SQL == "" {
		errCh := make(chan error, 1)
		recordCh := make(chan []gsyncx.Record)
		close(recordCh)
		errCh <- fmt.Errorf("SQLReader requires SQL in ReaderConfig")
		close(errCh)
		return recordCh, errCh
	}

	escapedField := gsyncx.FormatFieldName(incField, r.ds.GetDBType())
	condition := fmt.Sprintf("%s > '%v'", escapedField, lastValue)

	modifiedSQL := cfg.ReaderConfig.SQL
	if containsWhere(modifiedSQL) {
		modifiedSQL = modifiedSQL + " AND " + condition
	} else {
		modifiedSQL = modifiedSQL + " WHERE " + condition
	}

	modifiedCfg := *cfg
	modifiedCfg.ReaderConfig.SQL = modifiedSQL

	return r.Read(ctx, &modifiedCfg)
}

func containsWhere(sql string) bool {
	upper := ""
	for _, c := range sql {
		if c >= 'A' && c <= 'Z' {
			upper += string(c)
		} else if c >= 'a' && c <= 'z' {
			upper += string(c - 32)
		} else {
			upper += " "
		}
	}
	return len(upper) >= 6 && (upper[:6] == " WHERE" || containsSubstring(upper, " WHERE "))
}

func containsSubstring(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

type ReaderFactory func(config interface{}, logger gsyncx.SyncLogger) (gsyncx.Reader, error)

type ReaderRegistry struct {
	factories map[gsyncx.ReaderType]ReaderFactory
	mu        sync.RWMutex
}

var globalReaderRegistry = &ReaderRegistry{
	factories: make(map[gsyncx.ReaderType]ReaderFactory),
}

func init() {
	globalReaderRegistry.Register(gsyncx.ReaderTypeDatabase, func(config interface{}, logger gsyncx.SyncLogger) (gsyncx.Reader, error) {
		ds, ok := config.(*datasource.GdbxDataSource)
		if !ok {
			return nil, fmt.Errorf("expected *datasource.GdbxDataSource, got %T", config)
		}
		return NewDatabaseReader(ds, logger), nil
	})

	globalReaderRegistry.Register(gsyncx.ReaderTypeSQL, func(config interface{}, logger gsyncx.SyncLogger) (gsyncx.Reader, error) {
		ds, ok := config.(*datasource.GdbxDataSource)
		if !ok {
			return nil, fmt.Errorf("expected *datasource.GdbxDataSource, got %T", config)
		}
		return NewSQLReader(ds, logger), nil
	})

	globalReaderRegistry.Register(gsyncx.ReaderTypeRedisStream, func(config interface{}, logger gsyncx.SyncLogger) (gsyncx.Reader, error) {
		cfg, ok := config.(*RedisStreamConfig)
		if !ok {
			return nil, fmt.Errorf("expected *reader.RedisStreamConfig, got %T", config)
		}
		return NewRedisStreamReader(cfg, logger)
	})
}

func (r *ReaderRegistry) Register(readerType gsyncx.ReaderType, factory ReaderFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[readerType] = factory
}

func (r *ReaderRegistry) Create(readerType gsyncx.ReaderType, config interface{}, logger gsyncx.SyncLogger) (gsyncx.Reader, error) {
	r.mu.RLock()
	factory, ok := r.factories[readerType]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unsupported reader type: %s", readerType)
	}
	return factory(config, logger)
}

func (r *ReaderRegistry) Has(readerType gsyncx.ReaderType) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.factories[readerType]
	return ok
}

func (r *ReaderRegistry) Types() []gsyncx.ReaderType {
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := make([]gsyncx.ReaderType, 0, len(r.factories))
	for t := range r.factories {
		types = append(types, t)
	}
	return types
}

func RegisterReader(readerType gsyncx.ReaderType, factory ReaderFactory) {
	globalReaderRegistry.Register(readerType, factory)
}

func CreateReader(readerType gsyncx.ReaderType, config interface{}, logger gsyncx.SyncLogger) (gsyncx.Reader, error) {
	return globalReaderRegistry.Create(readerType, config, logger)
}

func RegisteredReaderTypes() []gsyncx.ReaderType {
	return globalReaderRegistry.Types()
}
