package gsyncx

import (
	"testing"
)

func TestFormatFieldName(t *testing.T) {
	tests := []struct {
		name   string
		field  string
		dbType DBType
		want   string
	}{
		{"mysql", "name", DBMySQL, "`name`"},
		{"postgres", "name", DBPostgres, `"name"`},
		{"oracle", "name", DBOracle, `"NAME"`},
		{"empty", "", DBMySQL, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatFieldName(tt.field, tt.dbType)
			if result != tt.want {
				t.Errorf("FormatFieldName(%s, %v) = %s, want %s", tt.field, tt.dbType, result, tt.want)
			}
		})
	}
}

func TestFormatTableName(t *testing.T) {
	result := FormatTableName("users", DBMySQL)
	if result != "`users`" {
		t.Errorf("expected `users`, got %s", result)
	}
}

func TestSanitizeIdentifier(t *testing.T) {
	err := SanitizeIdentifier("valid_name")
	if err != nil {
		t.Errorf("expected valid identifier, got error: %v", err)
	}

	err = SanitizeIdentifier("")
	if err == nil {
		t.Error("expected error for empty identifier")
	}
}

func TestNewWhereClauseBuilder(t *testing.T) {
	w := NewWhereClauseBuilder()
	if w == nil {
		t.Error("expected non-nil WhereClauseBuilder")
	}
}

func TestWhereClauseBuilder_AndEq(t *testing.T) {
	w := NewWhereClauseBuilder()
	w.AndEq("status", "active")

	sql, args, err := w.Build(DBMySQL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Error("expected non-empty SQL")
	}
	if len(args) == 0 {
		t.Error("expected args")
	}
}

func TestWhereClauseBuilder_AndGt(t *testing.T) {
	w := NewWhereClauseBuilder()
	w.AndGt("age", 18)

	sql, args, err := w.Build(DBMySQL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Error("expected non-empty SQL")
	}
	if len(args) == 0 {
		t.Error("expected args")
	}
}

func TestWhereClauseBuilder_AndBetween(t *testing.T) {
	w := NewWhereClauseBuilder()
	w.AndBetween("created_at", "2024-01-01", "2024-12-31")

	sql, args, err := w.Build(DBMySQL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Error("expected non-empty SQL")
	}
	if len(args) < 2 {
		t.Errorf("expected at least 2 args, got %d", len(args))
	}
}

func TestWhereClauseBuilder_MultipleConditions(t *testing.T) {
	w := NewWhereClauseBuilder()
	w.AndEq("status", "active")
	w.AndGt("age", 18)

	sql, _, err := w.Build(DBMySQL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql == "" {
		t.Error("expected non-empty SQL with multiple conditions")
	}
}

func TestWhereClauseBuilder_Empty(t *testing.T) {
	w := NewWhereClauseBuilder()
	sql, args, err := w.Build(DBMySQL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "" {
		t.Errorf("expected empty SQL for empty builder, got %s", sql)
	}
	if len(args) != 0 {
		t.Errorf("expected 0 args for empty builder, got %d", len(args))
	}
}

func TestParseDBType(t *testing.T) {
	tests := []struct {
		input  string
		dbType DBType
		wantOK bool
	}{
		{"mysql", DBMySQL, true},
		{"postgres", DBPostgres, true},
		{"oracle", DBOracle, true},
		{"unknown", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := ParseDBType(tt.input)
			if (err == nil) != tt.wantOK {
				t.Errorf("ParseDBType(%s) error = %v, wantOK %v", tt.input, err, tt.wantOK)
			}
			if err == nil && result != tt.dbType {
				t.Errorf("ParseDBType(%s) = %v, want %v", tt.input, result, tt.dbType)
			}
		})
	}
}

func TestField_GetFieldName(t *testing.T) {
	f := Field{FieldName: "user_name", AliasName: "name"}
	if f.GetFieldName() != "user_name" {
		t.Errorf("expected user_name, got %s", f.GetFieldName())
	}
}

func TestField_GetAliasName(t *testing.T) {
	f := Field{FieldName: "user_name", AliasName: "name"}
	if f.GetAliasName() != "name" {
		t.Errorf("expected name, got %s", f.GetAliasName())
	}
}

func TestField_GetAliasName_Empty(t *testing.T) {
	f := Field{FieldName: "user_name"}
	result := f.GetAliasName()
	if result != "user_name" && result != "" {
		t.Errorf("expected user_name or empty, got %s", result)
	}
}

func TestDSNConfig_BuildDSN(t *testing.T) {
	dsn := DSNConfig{
		DBType:   DBMySQL,
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "password",
		Schema:   "testdb",
	}

	result := dsn.BuildDSN()
	if result == "" {
		t.Error("expected non-empty DSN")
	}
}

func TestJoinFields(t *testing.T) {
	fields := []string{"id", "name", "email"}
	result := JoinFields(fields, DBMySQL)
	if result == "" {
		t.Error("expected non-empty joined fields")
	}
}
