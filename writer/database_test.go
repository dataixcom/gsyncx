package writer

import (
	"context"
	"testing"

	"github.com/dataixcom/gsyncx"
	"github.com/dataixcom/gsyncx/datasource"
)

func TestNewDatabaseWriter(t *testing.T) {
	w := NewDatabaseWriter(nil, nil)
	if w == nil {
		t.Error("expected non-nil writer")
	}
}

func TestNewDatabaseWriterWithConfig(t *testing.T) {
	cfg := gsyncx.WriterConfig{
		TableName:  "users",
		Schema:     "public",
		WriteMode:  gsyncx.WriteModeUpsert,
		PrimaryKey: &gsyncx.Field{FieldName: "id"},
		Fields:     []gsyncx.Field{{FieldName: "id"}, {FieldName: "name"}},
		RawFields:  []string{"id", "name", "email"},
	}
	w := NewDatabaseWriterWithConfig(nil, cfg, nil)
	if w == nil {
		t.Fatal("expected non-nil writer")
	}
	if w.cfg.TableName != "users" {
		t.Errorf("expected table name 'users', got '%s'", w.cfg.TableName)
	}
	if w.cfg.Schema != "public" {
		t.Errorf("expected schema 'public', got '%s'", w.cfg.Schema)
	}
	if w.cfg.PrimaryKey == nil || w.cfg.PrimaryKey.FieldName != "id" {
		t.Error("expected primary key field 'id'")
	}
	if len(w.cfg.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(w.cfg.Fields))
	}
	if len(w.cfg.RawFields) != 3 {
		t.Errorf("expected 3 raw fields, got %d", len(w.cfg.RawFields))
	}
}

func TestDatabaseWriter_SetConfig(t *testing.T) {
	w := NewDatabaseWriter(nil, nil)
	if w.cfg.TableName != "" {
		t.Error("expected empty table name initially")
	}

	cfg := gsyncx.WriterConfig{
		TableName:  "orders",
		Schema:     "sales",
		WriteMode:  gsyncx.WriteModeInsert,
		PrimaryKey: &gsyncx.Field{FieldName: "order_id"},
		Fields:     []gsyncx.Field{{FieldName: "order_id"}, {FieldName: "amount"}},
		RawFields:  []string{"order_id", "amount", "status"},
	}
	w.SetConfig(cfg)

	if w.cfg.TableName != "orders" {
		t.Errorf("expected table name 'orders', got '%s'", w.cfg.TableName)
	}
	if w.cfg.Schema != "sales" {
		t.Errorf("expected schema 'sales', got '%s'", w.cfg.Schema)
	}
	if w.cfg.PrimaryKey == nil || w.cfg.PrimaryKey.FieldName != "order_id" {
		t.Error("expected primary key field 'order_id'")
	}
	if len(w.cfg.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(w.cfg.Fields))
	}
	if len(w.cfg.RawFields) != 3 {
		t.Errorf("expected 3 raw fields, got %d", len(w.cfg.RawFields))
	}
}

func TestDatabaseWriter_Write_EmptyRecords(t *testing.T) {
	w := NewDatabaseWriter(nil, nil)
	result, err := w.Write(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.SuccessCount != 0 {
		t.Errorf("expected 0 success count, got %d", result.SuccessCount)
	}
}

func TestDatabaseWriter_WriteWithMode_EmptyRecords(t *testing.T) {
	w := NewDatabaseWriter(nil, nil)
	result, err := w.WriteWithMode(context.Background(), nil, gsyncx.WriteModeUpsert)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.SuccessCount != 0 {
		t.Errorf("expected 0 success count, got %d", result.SuccessCount)
	}
}

func TestDatabaseWriter_WriteWithMode_InsertMode(t *testing.T) {
	w := NewDatabaseWriter(nil, nil)
	records := []gsyncx.Record{
		{Data: map[string]interface{}{"id": 1, "name": "Alice"}},
	}
	_, err := w.WriteWithMode(context.Background(), records, gsyncx.WriteModeInsert)
	if err == nil {
		t.Error("expected error for nil datasource with insert mode")
	}
}

func TestDatabaseWriter_WriteWithMode_UpsertMode(t *testing.T) {
	w := NewDatabaseWriter(nil, nil)
	records := []gsyncx.Record{
		{Data: map[string]interface{}{"id": 1, "name": "Alice"}},
	}
	_, err := w.WriteWithMode(context.Background(), records, gsyncx.WriteModeUpsert)
	if err == nil {
		t.Error("expected error for nil datasource with upsert mode")
	}
}

func TestDatabaseWriter_WriteWithMode_DefaultMode(t *testing.T) {
	w := NewDatabaseWriter(nil, nil)
	records := []gsyncx.Record{
		{Data: map[string]interface{}{"id": 1, "name": "Alice"}},
	}
	_, err := w.WriteWithMode(context.Background(), records, gsyncx.WriteModeUpdate)
	if err == nil {
		t.Error("expected error for nil datasource with default mode")
	}
}

func TestDatabaseWriter_Flush(t *testing.T) {
	w := NewDatabaseWriter(nil, nil)
	err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDatabaseWriter_Close(t *testing.T) {
	w := NewDatabaseWriter(nil, nil)
	err := w.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDatabaseWriter_Write_DelegatesToUpsert(t *testing.T) {
	w := NewDatabaseWriter(nil, nil)
	records := []gsyncx.Record{
		{Data: map[string]interface{}{"id": 1}},
	}
	_, err := w.Write(context.Background(), records)
	if err == nil {
		t.Error("expected error for nil datasource")
	}
}

func TestNewBatchWriter(t *testing.T) {
	w := NewBatchWriter(nil, 100, nil)
	if w == nil {
		t.Error("expected non-nil batch writer")
	}
}

func TestNewBatchWriterWithConfig(t *testing.T) {
	cfg := gsyncx.WriterConfig{
		TableName:  "products",
		Schema:     "inventory",
		WriteMode:  gsyncx.WriteModeUpsert,
		PrimaryKey: &gsyncx.Field{FieldName: "sku"},
		Fields:     []gsyncx.Field{{FieldName: "sku"}, {FieldName: "price"}},
		RawFields:  []string{"sku", "price", "stock"},
	}
	w := NewBatchWriterWithConfig(nil, cfg, 100, nil)
	if w == nil {
		t.Fatal("expected non-nil batch writer")
	}
	if w.cfg.TableName != "products" {
		t.Errorf("expected table name 'products', got '%s'", w.cfg.TableName)
	}
	if w.cfg.PrimaryKey == nil || w.cfg.PrimaryKey.FieldName != "sku" {
		t.Error("expected primary key field 'sku'")
	}
	if len(w.cfg.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(w.cfg.Fields))
	}
	if len(w.cfg.RawFields) != 3 {
		t.Errorf("expected 3 raw fields, got %d", len(w.cfg.RawFields))
	}
}

func TestBatchWriter_SetConfig(t *testing.T) {
	w := NewBatchWriter(nil, 100, nil)
	if w.cfg.TableName != "" {
		t.Error("expected empty table name initially")
	}

	cfg := gsyncx.WriterConfig{
		TableName:  "orders",
		Schema:     "sales",
		WriteMode:  gsyncx.WriteModeInsert,
		PrimaryKey: &gsyncx.Field{FieldName: "order_id"},
		Fields:     []gsyncx.Field{{FieldName: "order_id"}, {FieldName: "amount"}},
		RawFields:  []string{"order_id", "amount", "status"},
	}
	w.SetConfig(cfg)

	if w.cfg.TableName != "orders" {
		t.Errorf("expected table name 'orders', got '%s'", w.cfg.TableName)
	}
	if w.cfg.PrimaryKey == nil || w.cfg.PrimaryKey.FieldName != "order_id" {
		t.Error("expected primary key field 'order_id'")
	}
}

func TestNewBatchWriter_DefaultBatchSize(t *testing.T) {
	w := NewBatchWriter(nil, 0, nil)
	if w.batchSize != 1000 {
		t.Errorf("expected default batch size 1000, got %d", w.batchSize)
	}
}

func TestNewBatchWriter_NegativeBatchSize(t *testing.T) {
	w := NewBatchWriter(nil, -1, nil)
	if w.batchSize != 1000 {
		t.Errorf("expected default batch size 1000 for negative input, got %d", w.batchSize)
	}
}

func TestBatchWriter_Write_EmptyRecords(t *testing.T) {
	w := NewBatchWriter(nil, 100, nil)
	result, err := w.Write(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.SuccessCount != 0 {
		t.Errorf("expected 0 success count, got %d", result.SuccessCount)
	}
}

func TestBatchWriter_Write_BelowBatchSize(t *testing.T) {
	w := NewBatchWriter(nil, 100, nil)
	records := []gsyncx.Record{
		{Data: map[string]interface{}{"id": 1}},
		{Data: map[string]interface{}{"id": 2}},
	}
	result, err := w.Write(context.Background(), records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.SuccessCount != 0 {
		t.Errorf("expected 0 success (not flushed yet), got %d", result.SuccessCount)
	}
}

func TestBatchWriter_Flush_Empty(t *testing.T) {
	w := NewBatchWriter(nil, 100, nil)
	err := w.Flush(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBatchWriter_Close(t *testing.T) {
	w := NewBatchWriter(nil, 100, nil)
	err := w.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBatchWriter_Flush_WithRecords(t *testing.T) {
	w := NewBatchWriter(nil, 100, nil)
	records := []gsyncx.Record{
		{Data: map[string]interface{}{"id": 1}},
	}
	_, _ = w.Write(context.Background(), records)

	err := w.Flush(context.Background())
	if err == nil {
		t.Error("expected error for nil datasource during flush")
	}
}

func TestBatchWriter_Write_TriggerFlush(t *testing.T) {
	w := NewBatchWriter(nil, 2, gsyncx.NewNopLogger())

	records := []gsyncx.Record{
		{Data: map[string]interface{}{"id": 1}},
		{Data: map[string]interface{}{"id": 2}},
	}
	_, err := w.Write(context.Background(), records)
	if err != nil {
		t.Logf("write with trigger flush: %v", err)
	}
}

func TestDatabaseWriter_BatchInsert_NilDS(t *testing.T) {
	w := NewDatabaseWriter(nil, nil)
	records := []gsyncx.Record{
		{Data: map[string]interface{}{"id": 1}},
	}
	_, err := w.WriteWithMode(context.Background(), records, gsyncx.WriteModeInsert)
	if err == nil {
		t.Error("expected error for nil datasource")
	}
}

func TestDatabaseWriter_BatchUpsert_NilDS(t *testing.T) {
	w := NewDatabaseWriter(nil, nil)
	records := []gsyncx.Record{
		{Data: map[string]interface{}{"id": 1}},
	}
	_, err := w.WriteWithMode(context.Background(), records, gsyncx.WriteModeUpsert)
	if err == nil {
		t.Error("expected error for nil datasource")
	}
}

func TestDatabaseWriter_TargetType(t *testing.T) {
	w := NewDatabaseWriter(nil, nil)
	if w.TargetType() != gsyncx.WriterTypeDatabase {
		t.Errorf("expected database, got %s", w.TargetType())
	}
}

func TestBatchWriter_TargetType(t *testing.T) {
	w := NewBatchWriter(nil, 100, nil)
	if w.TargetType() != gsyncx.WriterTypeDatabase {
		t.Errorf("expected database, got %s", w.TargetType())
	}
}

func TestWriterRegistry(t *testing.T) {
	registry := &WriterRegistry{factories: make(map[gsyncx.WriterType]WriterFactory)}

	registry.Register(gsyncx.WriterType("custom"), func(config interface{}, logger gsyncx.SyncLogger) (gsyncx.Writer, error) {
		return nil, nil
	})

	if !registry.Has(gsyncx.WriterType("custom")) {
		t.Error("expected custom writer to be registered")
	}

	if registry.Has(gsyncx.WriterType("nonexistent")) {
		t.Error("expected nonexistent writer to not be registered")
	}

	types := registry.Types()
	if len(types) != 1 {
		t.Errorf("expected 1 type, got %d", len(types))
	}
}

func TestWriterRegistry_Create_Unsupported(t *testing.T) {
	registry := &WriterRegistry{factories: make(map[gsyncx.WriterType]WriterFactory)}

	_, err := registry.Create(gsyncx.WriterType("unsupported"), nil, nil)
	if err == nil {
		t.Error("expected error for unsupported writer type")
	}
}

func TestRegisterWriter(t *testing.T) {
	RegisterWriter(gsyncx.WriterType("test_custom"), func(config interface{}, logger gsyncx.SyncLogger) (gsyncx.Writer, error) {
		return nil, nil
	})

	types := RegisteredWriterTypes()
	found := false
	for _, t := range types {
		if t == gsyncx.WriterType("test_custom") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected test_custom to be in registered writer types")
	}
}

func TestCreateWriter(t *testing.T) {
	RegisterWriter(gsyncx.WriterType("test_create"), func(config interface{}, logger gsyncx.SyncLogger) (gsyncx.Writer, error) {
		return nil, nil
	})

	wr, err := CreateWriter(gsyncx.WriterType("test_create"), nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if wr != nil {
		t.Error("expected nil writer from test factory")
	}
}

func TestCreateWriter_Unsupported(t *testing.T) {
	_, err := CreateWriter(gsyncx.WriterType("definitely_not_supported"), nil, nil)
	if err == nil {
		t.Error("expected error for unsupported writer type")
	}
}

func TestBatchWriter_WriteWithMode(t *testing.T) {
	w := NewBatchWriter(nil, 100, nil)
	records := []gsyncx.Record{
		{Data: map[string]interface{}{"id": 1}},
	}
	result, err := w.WriteWithMode(context.Background(), records, gsyncx.WriteModeInsert)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.SuccessCount != 0 {
		t.Errorf("expected 0 success (below batch size), got %d", result.SuccessCount)
	}
}

func TestCreateWriter_DatabaseWriterConfig(t *testing.T) {
	writerCfg := gsyncx.WriterConfig{
		TableName:  "test_table",
		Schema:     "test_schema",
		WriteMode:  gsyncx.WriteModeUpsert,
		PrimaryKey: &gsyncx.Field{FieldName: "id"},
		Fields:     []gsyncx.Field{{FieldName: "id"}, {FieldName: "name"}},
		RawFields:  []string{"id", "name"},
	}

	wr, err := CreateWriter(gsyncx.WriterTypeDatabase, &DatabaseWriterConfig{
		DS:  nil,
		Cfg: writerCfg,
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dbWr, ok := wr.(*DatabaseWriter)
	if !ok {
		t.Fatal("expected *DatabaseWriter")
	}
	if dbWr.cfg.TableName != "test_table" {
		t.Errorf("expected table name 'test_table', got '%s'", dbWr.cfg.TableName)
	}
	if dbWr.cfg.Schema != "test_schema" {
		t.Errorf("expected schema 'test_schema', got '%s'", dbWr.cfg.Schema)
	}
	if dbWr.cfg.PrimaryKey == nil || dbWr.cfg.PrimaryKey.FieldName != "id" {
		t.Error("expected primary key field 'id'")
	}
	if len(dbWr.cfg.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(dbWr.cfg.Fields))
	}
	if len(dbWr.cfg.RawFields) != 2 {
		t.Errorf("expected 2 raw fields, got %d", len(dbWr.cfg.RawFields))
	}
}

func TestCreateWriter_DatabaseWriterWithDSOnly(t *testing.T) {
	wr, err := CreateWriter(gsyncx.WriterTypeDatabase, &datasource.GdbxDataSource{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dbWr, ok := wr.(*DatabaseWriter)
	if !ok {
		t.Fatal("expected *DatabaseWriter")
	}
	if dbWr.cfg.TableName != "" {
		t.Errorf("expected empty table name for DS-only creation, got '%s'", dbWr.cfg.TableName)
	}
}

func TestCreateWriter_DatabaseWriterInvalidConfig(t *testing.T) {
	_, err := CreateWriter(gsyncx.WriterTypeDatabase, "invalid", nil)
	if err == nil {
		t.Error("expected error for invalid config type")
	}
}

func TestDatabaseWriterWithConfig_ConfigPropagatedToBatchInsert(t *testing.T) {
	cfg := gsyncx.WriterConfig{
		TableName:  "users",
		Schema:     "public",
		PrimaryKey: &gsyncx.Field{FieldName: "id"},
		Fields:     []gsyncx.Field{{FieldName: "id"}, {FieldName: "name"}},
		RawFields:  []string{"id", "name", "email"},
	}
	w := NewDatabaseWriterWithConfig(nil, cfg, nil)

	if w.cfg.TableName != "users" {
		t.Errorf("expected table name 'users', got '%s'", w.cfg.TableName)
	}
	if w.cfg.Schema != "public" {
		t.Errorf("expected schema 'public', got '%s'", w.cfg.Schema)
	}
	if w.cfg.PrimaryKey == nil || w.cfg.PrimaryKey.FieldName != "id" {
		t.Error("expected primary key 'id'")
	}
	if len(w.cfg.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(w.cfg.Fields))
	}
	if len(w.cfg.RawFields) != 3 {
		t.Errorf("expected 3 raw fields, got %d", len(w.cfg.RawFields))
	}

	records := []gsyncx.Record{
		{Data: map[string]interface{}{"id": 1, "name": "test"}},
	}
	_, err := w.WriteWithMode(context.Background(), records, gsyncx.WriteModeInsert)
	if err == nil {
		t.Error("expected error for nil datasource")
	}
}

func TestDatabaseWriterWithConfig_ConfigPropagatedToBatchUpsert(t *testing.T) {
	cfg := gsyncx.WriterConfig{
		TableName:  "orders",
		Schema:     "sales",
		PrimaryKey: &gsyncx.Field{FieldName: "order_id"},
		Fields:     []gsyncx.Field{{FieldName: "order_id"}, {FieldName: "amount"}},
		RawFields:  []string{"order_id", "amount"},
	}
	w := NewDatabaseWriterWithConfig(nil, cfg, nil)

	if w.cfg.TableName != "orders" {
		t.Errorf("expected table name 'orders', got '%s'", w.cfg.TableName)
	}
	if w.cfg.PrimaryKey == nil || w.cfg.PrimaryKey.FieldName != "order_id" {
		t.Error("expected primary key 'order_id'")
	}

	records := []gsyncx.Record{
		{Data: map[string]interface{}{"order_id": 1, "amount": 100}},
	}
	_, err := w.Write(context.Background(), records)
	if err == nil {
		t.Error("expected error for nil datasource")
	}
}

func TestBatchWriterWithConfig_ConfigPropagated(t *testing.T) {
	cfg := gsyncx.WriterConfig{
		TableName:  "products",
		Schema:     "inventory",
		PrimaryKey: &gsyncx.Field{FieldName: "sku"},
		Fields:     []gsyncx.Field{{FieldName: "sku"}, {FieldName: "price"}},
		RawFields:  []string{"sku", "price", "stock"},
	}
	w := NewBatchWriterWithConfig(nil, cfg, 100, nil)

	if w.cfg.TableName != "products" {
		t.Errorf("expected table name 'products', got '%s'", w.cfg.TableName)
	}
	if w.cfg.PrimaryKey == nil || w.cfg.PrimaryKey.FieldName != "sku" {
		t.Error("expected primary key 'sku'")
	}
}
