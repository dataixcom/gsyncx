package reader

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/datasource"
	_ "github.com/mattn/go-sqlite3"
)

func TestNewDatabaseReader(t *testing.T) {
	r := NewDatabaseReader(nil, nil)
	if r == nil {
		t.Error("expected non-nil reader")
	}
}

func TestNewSQLReader(t *testing.T) {
	r := NewSQLReader(nil, nil)
	if r == nil {
		t.Error("expected non-nil reader")
	}
}

func TestDatabaseReader_SourceType(t *testing.T) {
	r := NewDatabaseReader(nil, nil)
	if r.SourceType() != gsyncx.ReaderTypeDatabase {
		t.Errorf("expected database, got %s", r.SourceType())
	}
}

func TestSQLReader_SourceType(t *testing.T) {
	r := NewSQLReader(nil, nil)
	if r.SourceType() != gsyncx.ReaderTypeSQL {
		t.Errorf("expected sql, got %s", r.SourceType())
	}
}

func TestDatabaseReader_Close(t *testing.T) {
	r := NewDatabaseReader(nil, nil)
	if err := r.Close(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSQLReader_Close(t *testing.T) {
	r := NewSQLReader(nil, nil)
	if err := r.Close(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDatabaseReader_Count_NilDS(t *testing.T) {
	r := NewDatabaseReader(nil, nil)
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "users"}),
	)
	_, err := r.Count(context.Background(), cfg)
	if err == nil {
		t.Error("expected error for nil datasource")
	}
}

func TestDatabaseReader_Read_NilDS(t *testing.T) {
	r := NewDatabaseReader(nil, nil)
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "users"}),
		gsyncx.WithBatchSize(10),
	)
	recordCh, errCh := r.Read(context.Background(), cfg)
	if recordCh == nil {
		t.Error("expected non-nil record channel")
	}
	err := <-errCh
	if err == nil {
		t.Error("expected error for nil datasource")
	}
}

func TestSQLReader_Count_NilDS(t *testing.T) {
	r := NewSQLReader(nil, nil)
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{SQL: "SELECT 1"}),
	)
	_, err := r.Count(context.Background(), cfg)
	if err == nil {
		t.Error("expected error for nil datasource")
	}
}

func TestSQLReader_Read_NilDS(t *testing.T) {
	r := NewSQLReader(nil, nil)
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{SQL: "SELECT 1"}),
		gsyncx.WithBatchSize(10),
	)
	recordCh, errCh := r.Read(context.Background(), cfg)
	if recordCh == nil {
		t.Error("expected non-nil record channel")
	}
	err := <-errCh
	if err == nil {
		t.Error("expected error for nil datasource")
	}
}

func TestReaderRegistry(t *testing.T) {
	registry := &ReaderRegistry{factories: make(map[gsyncx.ReaderType]ReaderFactory)}
	registry.Register(gsyncx.ReaderType("custom"), func(config interface{}, logger gsyncx.SyncLogger) (gsyncx.Reader, error) {
		return nil, nil
	})
	if !registry.Has(gsyncx.ReaderType("custom")) {
		t.Error("expected custom reader to be registered")
	}
	types := registry.Types()
	if len(types) != 1 {
		t.Errorf("expected 1 type, got %d", len(types))
	}
}

func TestReaderRegistry_Create_Unsupported(t *testing.T) {
	registry := &ReaderRegistry{factories: make(map[gsyncx.ReaderType]ReaderFactory)}
	_, err := registry.Create(gsyncx.ReaderType("unsupported"), nil, nil)
	if err == nil {
		t.Error("expected error for unsupported reader type")
	}
}

func TestRegisterReader(t *testing.T) {
	RegisterReader("test_custom_reader", func(config interface{}, logger gsyncx.SyncLogger) (gsyncx.Reader, error) {
		return nil, nil
	})
	types := RegisteredReaderTypes()
	found := false
	for _, rt := range types {
		if rt == "test_custom_reader" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected test_custom_reader in registered types")
	}
}

func TestCreateReader_Database(t *testing.T) {
	_, err := CreateReader(gsyncx.ReaderTypeDatabase, nil, nil)
	if err != nil {
		t.Logf("create database reader with nil: %v", err)
	}
}

func TestCreateReader_SQL(t *testing.T) {
	_, err := CreateReader(gsyncx.ReaderTypeSQL, nil, nil)
	if err != nil {
		t.Logf("create sql reader with nil: %v", err)
	}
}

func TestCreateReader_Unsupported(t *testing.T) {
	_, err := CreateReader(gsyncx.ReaderType("unsupported"), nil, nil)
	if err == nil {
		t.Error("expected error for unsupported reader type")
	}
}

func setupSQLiteDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Skipf("sqlite3 driver not available: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("sqlite3 not available (CGO required): %v", err)
	}
	return db
}

func TestDatabaseReader_Read_WithDB(t *testing.T) {
	db := setupSQLiteDB(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	_, err = db.Exec("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	ds := datasource.NewGdbxDataSourceWithDB(db, gsyncx.DBMySQL)
	r := NewDatabaseReader(ds, nil)

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
			TableName:  "users",
			PrimaryKey: &gsyncx.Field{FieldName: "id"},
		}),
		gsyncx.WithBatchSize(10),
	)

	recordCh, errCh := r.Read(context.Background(), cfg)

	var records []gsyncx.Record
	for batch := range recordCh {
		records = append(records, batch...)
	}

	readErr := <-errCh
	if readErr != nil {
		t.Fatalf("unexpected error: %v", readErr)
	}

	if len(records) != 2 {
		t.Errorf("expected 2 records, got %d", len(records))
	}
}

func TestDatabaseReader_Count_WithDB(t *testing.T) {
	db := setupSQLiteDB(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	ds := datasource.NewGdbxDataSourceWithDB(db, gsyncx.DBMySQL)
	r := NewDatabaseReader(ds, nil)

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "users"}),
	)

	count, err := r.Count(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}
}

func TestSQLReader_Read_WithDB(t *testing.T) {
	db := setupSQLiteDB(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	ds := datasource.NewGdbxDataSourceWithDB(db, gsyncx.DBMySQL)
	r := NewSQLReader(ds, nil)

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
			SQL: "SELECT * FROM users",
		}),
		gsyncx.WithBatchSize(10),
	)

	recordCh, errCh := r.Read(context.Background(), cfg)

	var records []gsyncx.Record
	for batch := range recordCh {
		records = append(records, batch...)
	}

	readErr := <-errCh
	if readErr != nil {
		t.Fatalf("unexpected error: %v", readErr)
	}

	if len(records) != 1 {
		t.Errorf("expected 1 record, got %d", len(records))
	}
}

func TestSQLReader_Count_WithDB(t *testing.T) {
	db := setupSQLiteDB(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	ds := datasource.NewGdbxDataSourceWithDB(db, gsyncx.DBMySQL)
	r := NewSQLReader(ds, nil)

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
			SQL: "SELECT * FROM users",
		}),
	)

	count, err := r.Count(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}
}

func TestSQLReader_ReadWithIncremental_WithDB(t *testing.T) {
	db := setupSQLiteDB(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	ds := datasource.NewGdbxDataSourceWithDB(db, gsyncx.DBMySQL)
	r := NewSQLReader(ds, nil)

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
			SQL: "SELECT * FROM users",
		}),
		gsyncx.WithBatchSize(10),
	)

	recordCh, errCh := r.ReadWithIncremental(context.Background(), cfg, "id", 0)

	var records []gsyncx.Record
	for batch := range recordCh {
		records = append(records, batch...)
	}

	readErr := <-errCh
	if readErr != nil {
		t.Fatalf("unexpected error: %v", readErr)
	}

	if len(records) != 1 {
		t.Errorf("expected 1 record, got %d", len(records))
	}
}

func TestDatabaseReader_Close_WithDB(t *testing.T) {
	db := setupSQLiteDB(t)
	ds := datasource.NewGdbxDataSourceWithDB(db, gsyncx.DBMySQL)
	r := NewDatabaseReader(ds, nil)

	if err := r.Close(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSQLReader_Close_WithDB(t *testing.T) {
	db := setupSQLiteDB(t)
	ds := datasource.NewGdbxDataSourceWithDB(db, gsyncx.DBMySQL)
	r := NewSQLReader(ds, nil)

	if err := r.Close(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDatabaseReader_Read_EmptyTable(t *testing.T) {
	db := setupSQLiteDB(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE empty_table (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	ds := datasource.NewGdbxDataSourceWithDB(db, gsyncx.DBMySQL)
	r := NewDatabaseReader(ds, nil)

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "empty_table"}),
		gsyncx.WithBatchSize(10),
	)

	recordCh, errCh := r.Read(context.Background(), cfg)

	var recordCount int
	for range recordCh {
		recordCount++
	}

	readErr := <-errCh
	if readErr != nil {
		t.Fatalf("unexpected error: %v", readErr)
	}
	if recordCount != 0 {
		t.Errorf("expected 0 records from empty table, got %d", recordCount)
	}
}

func TestDatabaseReader_Read_CancelledContext(t *testing.T) {
	db := setupSQLiteDB(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	ds := datasource.NewGdbxDataSourceWithDB(db, gsyncx.DBMySQL)
	r := NewDatabaseReader(ds, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "users"}),
		gsyncx.WithBatchSize(10),
	)

	recordCh, errCh := r.Read(ctx, cfg)

	for range recordCh {
	}

	<-errCh
}
