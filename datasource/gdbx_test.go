package datasource

import (
	"context"
	"testing"

	"github.com/dataixcom/gsyncx"
)

func TestGdbxDataSource_GetDBType(t *testing.T) {
	ds := &GdbxDataSource{dbType: gsyncx.DBMySQL}
	if ds.GetDBType() != gsyncx.DBMySQL {
		t.Errorf("expected DBMySQL, got %s", ds.GetDBType())
	}
}

func TestGdbxDataSource_PingClose_NilDB(t *testing.T) {
	ds := &GdbxDataSource{dbType: gsyncx.DBMySQL}

	err := ds.Ping(context.Background())
	if err == nil {
		t.Error("expected error for nil db")
	}

	err = ds.Close()
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
