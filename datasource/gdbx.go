package datasource

import (
	"context"
	"database/sql"
	"fmt"

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
	}, nil
}

func (ds *GdbxDataSource) Query(ctx context.Context, cfg gsyncx.SelectConfig) ([]map[string]interface{}, error) {
	if ds.template == nil {
		return nil, fmt.Errorf("datasource not initialized")
	}
	return ds.template.ExecuteQuery(config.SelectConfig(cfg))
}

func (ds *GdbxDataSource) Count(ctx context.Context, cfg gsyncx.SelectConfig) (int64, error) {
	if ds.template == nil {
		return 0, fmt.Errorf("datasource not initialized")
	}
	return ds.template.ExecuteCount(config.SelectConfig(cfg))
}

func (ds *GdbxDataSource) BatchInsert(ctx context.Context, cfg gsyncx.BatchInsertConfig) (int64, error) {
	if ds.template == nil {
		return 0, fmt.Errorf("datasource not initialized")
	}
	return ds.template.ExecuteBatchInsert(config.BatchInsertConfig(cfg))
}

func (ds *GdbxDataSource) BatchUpsert(ctx context.Context, cfg gsyncx.BatchUpsertConfig) (int64, error) {
	if ds.template == nil {
		return 0, fmt.Errorf("datasource not initialized")
	}
	return ds.template.ExecuteBatchUpsert(config.BatchUpsertConfig(cfg))
}

func (ds *GdbxDataSource) RawQuery(ctx context.Context, query string, args ...interface{}) ([]map[string]interface{}, error) {
	if ds.template == nil {
		return nil, fmt.Errorf("datasource not initialized")
	}
	return ds.template.ExecuteRawQuery(query, args...)
}

func (ds *GdbxDataSource) RawExec(ctx context.Context, query string, args ...interface{}) (int64, error) {
	if ds.template == nil {
		return 0, fmt.Errorf("datasource not initialized")
	}
	return ds.template.ExecuteRawExec(query, args...)
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
