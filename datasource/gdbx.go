package datasource

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/dataixcom/gdbx/base/builder"
	"github.com/dataixcom/gdbx/base/config"
	"github.com/dataixcom/gdbx/base/pool"
	"github.com/dataixcom/gdbx/base/template"
	"github.com/dataixcom/gsyncx"
)

type GdbxDataSource struct {
	template *template.SQLExecutorTemplate
	builder  *builder.SQLBuilder
	db       *sql.DB
	dbType   gsyncx.DBType
	dsn      gsyncx.DSNConfig
	logger   gsyncx.SyncLogger
}

func NewGdbxDataSource(dsn gsyncx.DSNConfig) (*GdbxDataSource, error) {
	tmpl, err := template.NewSQLExecutorTemplate(config.DSNConfig(dsn))
	if err != nil {
		return nil, fmt.Errorf("failed to create gdbx executor: %w", err)
	}

	b := builder.NewSQLBuilder(config.DBType(dsn.DBType))

	db, err := pool.GetDBPool(config.DSNConfig(dsn))
	if err != nil {
		return nil, fmt.Errorf("failed to get db pool: %w", err)
	}

	return &GdbxDataSource{
		template: tmpl,
		builder:  b,
		db:       db,
		dbType:   dsn.DBType,
		dsn:      dsn,
		logger:   gsyncx.NewNopLogger(),
	}, nil
}

func NewGdbxDataSourceWithDB(db *sql.DB, dbType gsyncx.DBType) *GdbxDataSource {
	dsn := gsyncx.DSNConfig{DBType: dbType}
	tmpl, _ := template.NewSQLExecutorTemplate(config.DSNConfig(dsn))
	b := builder.NewSQLBuilder(config.DBType(dbType))
	return &GdbxDataSource{
		template: tmpl,
		builder:  b,
		db:       db,
		dbType:   dbType,
		dsn:      dsn,
		logger:   gsyncx.NewNopLogger(),
	}
}

func (ds *GdbxDataSource) SetLogger(logger gsyncx.SyncLogger) {
	if logger != nil {
		ds.logger = logger.WithModule("datasource")
	}
}

func (ds *GdbxDataSource) Query(ctx context.Context, cfg gsyncx.SelectConfig) ([]map[string]interface{}, error) {
	if ds.template == nil {
		return nil, fmt.Errorf("datasource not initialized")
	}
	ds.logSelectSQL("query", cfg)
	start := time.Now()
	result, err := ds.template.ExecuteQuery(config.SelectConfig(cfg))
	elapsed := time.Since(start)
	if err != nil {
		ds.logger.Error("sql query failed",
			gsyncx.F("table", cfg.TableName),
			gsyncx.F("error", err),
			gsyncx.F("duration", elapsed.String()),
		)
		return nil, err
	}
	ds.logger.Debug("sql query completed",
		gsyncx.F("table", cfg.TableName),
		gsyncx.F("rows", len(result)),
		gsyncx.F("duration", elapsed.String()),
	)
	return result, nil
}

func (ds *GdbxDataSource) Count(ctx context.Context, cfg gsyncx.SelectConfig) (int64, error) {
	if ds.template == nil {
		return 0, fmt.Errorf("datasource not initialized")
	}
	ds.logSelectSQL("count", cfg)
	start := time.Now()
	result, err := ds.template.ExecuteCount(config.SelectConfig(cfg))
	elapsed := time.Since(start)
	if err != nil {
		ds.logger.Error("sql count failed",
			gsyncx.F("table", cfg.TableName),
			gsyncx.F("error", err),
			gsyncx.F("duration", elapsed.String()),
		)
		return 0, err
	}
	ds.logger.Debug("sql count completed",
		gsyncx.F("table", cfg.TableName),
		gsyncx.F("count", result),
		gsyncx.F("duration", elapsed.String()),
	)
	return result, nil
}

func (ds *GdbxDataSource) BatchInsert(ctx context.Context, cfg gsyncx.BatchInsertConfig) (int64, error) {
	if ds.template == nil {
		return 0, fmt.Errorf("datasource not initialized")
	}
	ds.logBatchInsertSQL(cfg)
	start := time.Now()
	result, err := ds.template.ExecuteBatchInsert(config.BatchInsertConfig(cfg))
	elapsed := time.Since(start)
	if err != nil {
		ds.logger.Error("sql batch insert failed",
			gsyncx.F("table", cfg.TableName),
			gsyncx.F("error", err),
			gsyncx.F("duration", elapsed.String()),
		)
		return 0, err
	}
	ds.logger.Debug("sql batch insert completed",
		gsyncx.F("table", cfg.TableName),
		gsyncx.F("affected", result),
		gsyncx.F("duration", elapsed.String()),
	)
	return result, nil
}

func (ds *GdbxDataSource) BatchUpsert(ctx context.Context, cfg gsyncx.BatchUpsertConfig) (int64, error) {
	if ds.template == nil {
		return 0, fmt.Errorf("datasource not initialized")
	}
	ds.logBatchUpsertSQL(cfg)
	start := time.Now()
	result, err := ds.template.ExecuteBatchUpsert(config.BatchUpsertConfig(cfg))
	elapsed := time.Since(start)
	if err != nil {
		ds.logger.Error("sql batch upsert failed",
			gsyncx.F("table", cfg.TableName),
			gsyncx.F("error", err),
			gsyncx.F("duration", elapsed.String()),
		)
		return 0, err
	}
	ds.logger.Debug("sql batch upsert completed",
		gsyncx.F("table", cfg.TableName),
		gsyncx.F("affected", result),
		gsyncx.F("duration", elapsed.String()),
	)
	return result, nil
}

func (ds *GdbxDataSource) RawQuery(ctx context.Context, query string, args ...interface{}) ([]map[string]interface{}, error) {
	if ds.template == nil {
		return nil, fmt.Errorf("datasource not initialized")
	}
	ds.logRawSQL("raw_query", query, args...)
	start := time.Now()
	result, err := ds.template.ExecuteRawQuery(query, args...)
	elapsed := time.Since(start)
	if err != nil {
		ds.logger.Error("sql raw query failed",
			gsyncx.F("sql", query),
			gsyncx.F("error", err),
			gsyncx.F("duration", elapsed.String()),
		)
		return nil, err
	}
	ds.logger.Debug("sql raw query completed",
		gsyncx.F("sql", query),
		gsyncx.F("rows", len(result)),
		gsyncx.F("duration", elapsed.String()),
	)
	return result, nil
}

func (ds *GdbxDataSource) RawExec(ctx context.Context, query string, args ...interface{}) (int64, error) {
	if ds.template == nil {
		return 0, fmt.Errorf("datasource not initialized")
	}
	ds.logRawSQL("raw_exec", query, args...)
	start := time.Now()
	result, err := ds.template.ExecuteRawExec(query, args...)
	elapsed := time.Since(start)
	if err != nil {
		ds.logger.Error("sql raw exec failed",
			gsyncx.F("sql", query),
			gsyncx.F("error", err),
			gsyncx.F("duration", elapsed.String()),
		)
		return 0, err
	}
	ds.logger.Debug("sql raw exec completed",
		gsyncx.F("sql", query),
		gsyncx.F("affected", result),
		gsyncx.F("duration", elapsed.String()),
	)
	return result, nil
}

func (ds *GdbxDataSource) Ping(ctx context.Context) error {
	if ds.db == nil {
		return fmt.Errorf("database connection is nil")
	}
	return ds.db.PingContext(ctx)
}

func (ds *GdbxDataSource) Close() error {
	if ds.db == nil {
		return fmt.Errorf("database connection is nil")
	}
	return ds.db.Close()
}

func (ds *GdbxDataSource) GetDBType() gsyncx.DBType {
	return ds.dbType
}

func (ds *GdbxDataSource) logSelectSQL(op string, cfg gsyncx.SelectConfig) {
	if ds.builder == nil {
		return
	}
	sqlStr, args, err := ds.builder.BuildSelect(config.SelectConfig(cfg))
	if err != nil {
		ds.logger.Warn("failed to build select sql for logging",
			gsyncx.F("op", op),
			gsyncx.F("error", err),
		)
		return
	}
	ds.logger.Debug("sql operation",
		gsyncx.F("op", op),
		gsyncx.F("table", cfg.TableName),
		gsyncx.F("sql", sqlStr),
		gsyncx.F("args", gsyncx.MaskSensitiveMap(args)),
	)
}

func (ds *GdbxDataSource) logBatchInsertSQL(cfg gsyncx.BatchInsertConfig) {
	if ds.builder == nil {
		return
	}
	sqlStr, args, err := ds.builder.BuildBatchInsert(config.BatchInsertConfig(cfg))
	if err != nil {
		ds.logger.Warn("failed to build batch insert sql for logging",
			gsyncx.F("error", err),
		)
		return
	}
	ds.logger.Debug("sql operation",
		gsyncx.F("op", "batch_insert"),
		gsyncx.F("table", cfg.TableName),
		gsyncx.F("sql", sqlStr),
		gsyncx.F("args", gsyncx.MaskSensitiveMap(args)),
	)
}

func (ds *GdbxDataSource) logBatchUpsertSQL(cfg gsyncx.BatchUpsertConfig) {
	if ds.builder == nil {
		return
	}
	sqlStr, args, err := ds.builder.BuildBatchUpsert(config.BatchUpsertConfig(cfg))
	if err != nil {
		ds.logger.Warn("failed to build batch upsert sql for logging",
			gsyncx.F("error", err),
		)
		return
	}
	ds.logger.Debug("sql operation",
		gsyncx.F("op", "batch_upsert"),
		gsyncx.F("table", cfg.TableName),
		gsyncx.F("sql", sqlStr),
		gsyncx.F("args", gsyncx.MaskSensitiveMap(args)),
	)
}

func (ds *GdbxDataSource) logRawSQL(op string, query string, args ...interface{}) {
	maskedArgs := make([]interface{}, len(args))
	for i, arg := range args {
		maskedArgs[i] = arg
	}
	ds.logger.Debug("sql operation",
		gsyncx.F("op", op),
		gsyncx.F("sql", query),
		gsyncx.F("args", maskedArgs),
	)
}
