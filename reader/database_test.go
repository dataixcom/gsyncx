package reader

import (
	"context"
	"testing"

	"github.com/dataixcom/gsyncx"
)

func TestNewDatabaseReader(t *testing.T) {
	r := NewDatabaseReader(nil, nil)
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

func TestDatabaseReader_Read_NilDS(t *testing.T) {
	r := NewDatabaseReader(nil, nil)
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "users"}),
	)
	_, errCh := r.Read(context.Background(), cfg)
	err := <-errCh
	if err == nil {
		t.Error("expected error for nil datasource")
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

func TestDatabaseReader_GetSplitKeys_NilDS(t *testing.T) {
	r := NewDatabaseReader(nil, nil)
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "users"}),
	)
	_, err := r.GetSplitKeys(context.Background(), cfg)
	if err == nil {
		t.Error("expected error for nil datasource")
	}
}

func TestDatabaseReader_GetSplitKeys_NoPK(t *testing.T) {
	r := NewDatabaseReader(nil, nil)
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "users"}),
	)
	_, err := r.GetSplitKeys(context.Background(), cfg)
	if err == nil {
		t.Error("expected error for missing primary key")
	}
}

func TestDatabaseReader_Close_NilDS(t *testing.T) {
	r := NewDatabaseReader(nil, nil)
	if err := r.Close(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDatabaseReader_Read_CancelledContext(t *testing.T) {
	r := NewDatabaseReader(nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "users"}),
	)

	_, errCh := r.Read(ctx, cfg)
	err := <-errCh
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestDatabaseReader_buildSelectConfig(t *testing.T) {
	r := NewDatabaseReader(nil, nil)

	t.Run("full sync with where clause", func(t *testing.T) {
		cfg := gsyncx.NewSyncConfig(
			gsyncx.WithSyncMode(gsyncx.SyncModeFull),
			gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
				TableName:   "users",
				WhereClause: "status = 'active'",
			}),
		)
		selectCfg := r.buildSelectConfig(cfg, 100, 0)
		if selectCfg.TableName != "users" {
			t.Errorf("expected users, got %s", selectCfg.TableName)
		}
		if selectCfg.Limit != 100 {
			t.Errorf("expected limit 100, got %d", selectCfg.Limit)
		}
		if selectCfg.RawConditions != "status = 'active'" {
			t.Errorf("expected where clause, got %s", selectCfg.RawConditions)
		}
	})

	t.Run("with schema", func(t *testing.T) {
		cfg := gsyncx.NewSyncConfig(
			gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
				TableName: "users",
				Schema:    "public",
			}),
		)
		selectCfg := r.buildSelectConfig(cfg, 100, 0)
		if selectCfg.Schema != "public" {
			t.Errorf("expected public, got %s", selectCfg.Schema)
		}
	})

	t.Run("with primary key", func(t *testing.T) {
		cfg := gsyncx.NewSyncConfig(
			gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
				TableName:  "users",
				PrimaryKey: &gsyncx.Field{FieldName: "id"},
			}),
		)
		selectCfg := r.buildSelectConfig(cfg, 100, 0)
		if selectCfg.OrderBy == "" {
			t.Error("expected order by clause with primary key")
		}
	})

	t.Run("with raw fields", func(t *testing.T) {
		cfg := gsyncx.NewSyncConfig(
			gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
				TableName: "users",
				RawFields: []string{"id", "name"},
			}),
		)
		selectCfg := r.buildSelectConfig(cfg, 100, 0)
		if len(selectCfg.RawFields) != 2 {
			t.Errorf("expected 2 raw fields, got %d", len(selectCfg.RawFields))
		}
	})

	t.Run("incremental sync", func(t *testing.T) {
		cfg := gsyncx.NewSyncConfig(
			gsyncx.WithSyncMode(gsyncx.SyncModeIncremental),
			gsyncx.WithIncrementalField(&gsyncx.Field{FieldName: "updated_at"}, gsyncx.StrategyTimestamp),
			gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "users"}),
		)
		selectCfg := r.buildSelectConfig(cfg, 100, 0)
		if selectCfg.TableName != "users" {
			t.Errorf("expected users, got %s", selectCfg.TableName)
		}
	})

	t.Run("incremental with existing where", func(t *testing.T) {
		cfg := gsyncx.NewSyncConfig(
			gsyncx.WithSyncMode(gsyncx.SyncModeIncremental),
			gsyncx.WithIncrementalField(&gsyncx.Field{FieldName: "updated_at"}, gsyncx.StrategyTimestamp),
			gsyncx.WithReaderConfig(gsyncx.ReaderConfig{
				TableName:   "users",
				WhereClause: "status = 'active'",
			}),
		)
		selectCfg := r.buildSelectConfig(cfg, 100, 0)
		if selectCfg.RawConditions == "" {
			t.Error("expected combined conditions")
		}
	})

	t.Run("default batch size", func(t *testing.T) {
		cfg := gsyncx.NewSyncConfig(
			gsyncx.WithReaderConfig(gsyncx.ReaderConfig{TableName: "users"}),
		)
		cfg.BatchSize = 0
		selectCfg := r.buildSelectConfig(cfg, 0, 0)
		if selectCfg.Limit != 0 {
			t.Errorf("expected limit 0, got %d", selectCfg.Limit)
		}
	})
}

func TestNewSQLReader(t *testing.T) {
	r := NewSQLReader(nil, nil)
	if r == nil {
		t.Error("expected non-nil reader")
	}
}

func TestSQLReader_SourceType(t *testing.T) {
	r := NewSQLReader(nil, nil)
	if r.SourceType() != gsyncx.ReaderTypeSQL {
		t.Errorf("expected sql, got %s", r.SourceType())
	}
}

func TestSQLReader_Read_NoSQL(t *testing.T) {
	r := NewSQLReader(nil, nil)
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{SQL: ""}),
	)

	_, errCh := r.Read(context.Background(), cfg)
	err := <-errCh
	if err == nil {
		t.Error("expected error for empty SQL")
	}
}

func TestSQLReader_Count_NoSQL(t *testing.T) {
	r := NewSQLReader(nil, nil)
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{SQL: ""}),
	)
	_, err := r.Count(context.Background(), cfg)
	if err == nil {
		t.Error("expected error for empty SQL")
	}
}

func TestSQLReader_GetSplitKeys(t *testing.T) {
	r := NewSQLReader(nil, nil)
	cfg := gsyncx.NewSyncConfig()
	_, err := r.GetSplitKeys(context.Background(), cfg)
	if err == nil {
		t.Error("expected error: SQLReader does not support split keys")
	}
}

func TestSQLReader_Close_NilDS(t *testing.T) {
	r := NewSQLReader(nil, nil)
	if err := r.Close(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSQLReader_ReadWithIncremental_NoSQL(t *testing.T) {
	r := NewSQLReader(nil, nil)
	cfg := gsyncx.NewSyncConfig(
		gsyncx.WithReaderConfig(gsyncx.ReaderConfig{SQL: ""}),
	)
	_, errCh := r.ReadWithIncremental(context.Background(), cfg, "id", 100)
	err := <-errCh
	if err == nil {
		t.Error("expected error for empty SQL in ReadWithIncremental")
	}
}

func TestContainsWhere(t *testing.T) {
	tests := []struct {
		sql  string
		want bool
	}{
		{"SELECT * FROM users WHERE id > 10", true},
		{"SELECT * FROM users", false},
		{"select * from users where id > 10", true},
		{"SELECT * FROM users WHERE  id > 10", true},
	}

	for _, tt := range tests {
		result := containsWhere(tt.sql)
		if result != tt.want {
			t.Errorf("containsWhere(%q) = %v, want %v", tt.sql, result, tt.want)
		}
	}
}

func TestContainsSubstring(t *testing.T) {
	if !containsSubstring("hello world test", "world") {
		t.Error("expected true for existing substring")
	}
	if containsSubstring("hello test", "world") {
		t.Error("expected false for missing substring")
	}
}

func TestReaderRegistry_Operations(t *testing.T) {
	registry := &ReaderRegistry{factories: make(map[gsyncx.ReaderType]ReaderFactory)}

	registry.Register("type_a", func(config interface{}, logger gsyncx.SyncLogger) (gsyncx.Reader, error) {
		return nil, nil
	})
	registry.Register("type_b", func(config interface{}, logger gsyncx.SyncLogger) (gsyncx.Reader, error) {
		return nil, nil
	})

	if !registry.Has("type_a") {
		t.Error("expected type_a to be registered")
	}
	if !registry.Has("type_b") {
		t.Error("expected type_b to be registered")
	}
	if registry.Has("type_c") {
		t.Error("expected type_c to not be registered")
	}

	types := registry.Types()
	if len(types) != 2 {
		t.Errorf("expected 2 types, got %d", len(types))
	}

	_, err := registry.Create("type_a", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = registry.Create("type_c", nil, nil)
	if err == nil {
		t.Error("expected error for unsupported type")
	}
}

func TestGlobalReaderRegistry(t *testing.T) {
	types := RegisteredReaderTypes()
	if len(types) == 0 {
		t.Error("expected at least one registered reader type")
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
