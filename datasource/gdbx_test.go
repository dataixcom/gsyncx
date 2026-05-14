package datasource

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dataixcom/gsyncx"
)

func TestGdbxDataSource_GetDBType(t *testing.T) {
	tests := []struct {
		name   string
		dbType gsyncx.DBType
	}{
		{"mysql", gsyncx.DBMySQL},
		{"postgres", gsyncx.DBPostgres},
		{"oracle", gsyncx.DBOracle},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := &GdbxDataSource{dbType: tt.dbType}
			if ds.GetDBType() != tt.dbType {
				t.Errorf("expected %s, got %s", tt.dbType, ds.GetDBType())
			}
		})
	}
}

func TestGdbxDataSource_Ping_NilDB(t *testing.T) {
	ds := &GdbxDataSource{dbType: gsyncx.DBMySQL}
	err := ds.Ping(context.Background())
	if err == nil {
		t.Error("expected error for nil db")
	}
}

func TestGdbxDataSource_Close_NilDB(t *testing.T) {
	ds := &GdbxDataSource{dbType: gsyncx.DBMySQL}
	err := ds.Close()
	if err == nil {
		t.Error("expected error for nil db")
	}
}

func TestGdbxDataSource_Query_NilTemplate(t *testing.T) {
	ds := &GdbxDataSource{dbType: gsyncx.DBMySQL}
	_, err := ds.Query(context.Background(), gsyncx.SelectConfig{TableName: "users"})
	if err == nil {
		t.Error("expected error for nil template")
	}
}

func TestGdbxDataSource_Count_NilTemplate(t *testing.T) {
	ds := &GdbxDataSource{dbType: gsyncx.DBMySQL}
	_, err := ds.Count(context.Background(), gsyncx.SelectConfig{TableName: "users"})
	if err == nil {
		t.Error("expected error for nil template")
	}
}

func TestGdbxDataSource_BatchInsert_NilTemplate(t *testing.T) {
	ds := &GdbxDataSource{dbType: gsyncx.DBMySQL}
	_, err := ds.BatchInsert(context.Background(), gsyncx.BatchInsertConfig{
		TableName: "users",
		Data:      []map[string]any{{"id": 1}},
	})
	if err == nil {
		t.Error("expected error for nil template")
	}
}

func TestGdbxDataSource_BatchUpsert_NilTemplate(t *testing.T) {
	ds := &GdbxDataSource{dbType: gsyncx.DBMySQL}
	_, err := ds.BatchUpsert(context.Background(), gsyncx.BatchUpsertConfig{
		TableName: "users",
		Data:      []map[string]any{{"id": 1}},
	})
	if err == nil {
		t.Error("expected error for nil template")
	}
}

func TestGdbxDataSource_RawQuery_NilTemplate(t *testing.T) {
	ds := &GdbxDataSource{dbType: gsyncx.DBMySQL}
	_, err := ds.RawQuery(context.Background(), "SELECT 1")
	if err == nil {
		t.Error("expected error for nil template")
	}
}

func TestGdbxDataSource_RawExec_NilTemplate(t *testing.T) {
	ds := &GdbxDataSource{dbType: gsyncx.DBMySQL}
	_, err := ds.RawExec(context.Background(), "DELETE FROM users WHERE 1=0")
	if err == nil {
		t.Error("expected error for nil template")
	}
}

func TestGdbxDataSource_Query_CancelledContext(t *testing.T) {
	ds := &GdbxDataSource{dbType: gsyncx.DBMySQL}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ds.Query(ctx, gsyncx.SelectConfig{TableName: "users"})
	if err == nil {
		t.Error("expected error for cancelled context with nil template")
	}
}

func TestGdbxDataSource_BatchInsert_EmptyData(t *testing.T) {
	ds := &GdbxDataSource{dbType: gsyncx.DBMySQL}
	_, err := ds.BatchInsert(context.Background(), gsyncx.BatchInsertConfig{
		TableName: "users",
		Data:      []map[string]any{},
	})
	if err == nil {
		t.Error("expected error for nil template")
	}
}

func TestGdbxDataSource_BatchUpsert_EmptyData(t *testing.T) {
	ds := &GdbxDataSource{dbType: gsyncx.DBMySQL}
	_, err := ds.BatchUpsert(context.Background(), gsyncx.BatchUpsertConfig{
		TableName: "users",
		Data:      []map[string]any{},
	})
	if err == nil {
		t.Error("expected error for nil template")
	}
}

func TestNewGdbxDataSource_InvalidDSN(t *testing.T) {
	ds, err := NewGdbxDataSource(gsyncx.DSNConfig{
		DBType:   gsyncx.DBMySQL,
		Host:     "invalid-host-that-does-not-exist",
		Port:     3306,
		User:     "root",
		Password: "invalid",
	})
	if err != nil {
		t.Logf("NewGdbxDataSource returned error for invalid DSN: %v", err)
		return
	}
	err = ds.Ping(context.Background())
	if err == nil {
		t.Error("expected error when pinging invalid DSN")
	}
}

func TestGdbxDataSource_Ping_WithDB(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Skipf("sqlite3 driver not available: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Skipf("sqlite3 not available (CGO required): %v", err)
	}
	ds := NewGdbxDataSourceWithDB(db, gsyncx.DBMySQL)
	if err := ds.Ping(context.Background()); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestGdbxDataSource_Close_WithDB(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Skipf("sqlite3 driver not available: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("sqlite3 not available (CGO required): %v", err)
	}
	ds := NewGdbxDataSourceWithDB(db, gsyncx.DBMySQL)
	if err := ds.Close(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
