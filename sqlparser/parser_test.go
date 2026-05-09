package sqlparser

import (
	"testing"
)

func TestNewSQLParser(t *testing.T) {
	parser := NewSQLParser()
	if parser == nil {
		t.Error("expected non-nil parser")
	}
}

func TestSQLParser_Parse_Select(t *testing.T) {
	parser := NewSQLParser()

	parsed, err := parser.Parse("SELECT id, name FROM users WHERE age > 18 ORDER BY id LIMIT 100")
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	if len(parsed.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(parsed.Fields))
	}
	if len(parsed.Tables) != 1 || parsed.Tables[0] != "users" {
		t.Errorf("expected table [users], got %v", parsed.Tables)
	}
	if parsed.WhereClause == "" {
		t.Error("expected where clause")
	}
	if parsed.OrderBy == "" {
		t.Error("expected order by clause")
	}
	if parsed.Limit == nil || *parsed.Limit != 100 {
		t.Errorf("expected limit 100, got %v", parsed.Limit)
	}
}

func TestSQLParser_Parse_SelectAll(t *testing.T) {
	parser := NewSQLParser()

	parsed, err := parser.Parse("SELECT * FROM orders")
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	if len(parsed.Tables) != 1 || parsed.Tables[0] != "orders" {
		t.Errorf("expected table [orders], got %v", parsed.Tables)
	}
	if parsed.WhereClause != "" {
		t.Errorf("expected empty where clause, got %s", parsed.WhereClause)
	}
}

func TestSQLParser_Parse_WithLimitOffset(t *testing.T) {
	parser := NewSQLParser()

	parsed, err := parser.Parse("SELECT id FROM users LIMIT 50 OFFSET 100")
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	if parsed.Limit == nil || *parsed.Limit != 50 {
		t.Errorf("expected limit 50, got %v", parsed.Limit)
	}
	if parsed.Offset == nil || *parsed.Offset != 100 {
		t.Errorf("expected offset 100, got %v", parsed.Offset)
	}
}

func TestSQLParser_Parse_WithGroupBy(t *testing.T) {
	parser := NewSQLParser()

	parsed, err := parser.Parse("SELECT category, COUNT(*) FROM products GROUP BY category")
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	if parsed.GroupBy == "" {
		t.Error("expected group by clause")
	}
}

func TestSQLParser_Parse_WithHaving(t *testing.T) {
	parser := NewSQLParser()

	parsed, err := parser.Parse("SELECT category, COUNT(*) FROM products GROUP BY category HAVING COUNT(*) > 5")
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	if parsed.Having == "" {
		t.Error("expected having clause")
	}
}

func TestSQLParser_Parse_CompleteQuery(t *testing.T) {
	parser := NewSQLParser()

	parsed, err := parser.Parse("SELECT id, name FROM users WHERE status = 'active' GROUP BY name HAVING COUNT(*) > 1 ORDER BY id LIMIT 10 OFFSET 5")
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	if parsed.SelectClause == "" {
		t.Error("expected select clause")
	}
	if parsed.FromClause == "" {
		t.Error("expected from clause")
	}
	if parsed.WhereClause == "" {
		t.Error("expected where clause")
	}
	if parsed.GroupBy == "" {
		t.Error("expected group by clause")
	}
	if parsed.Having == "" {
		t.Error("expected having clause")
	}
	if parsed.OrderBy == "" {
		t.Error("expected order by clause")
	}
	if parsed.Limit == nil {
		t.Error("expected limit")
	}
	if parsed.Offset == nil {
		t.Error("expected offset")
	}
}

func TestSQLParser_Parse_InvalidSQL(t *testing.T) {
	parser := NewSQLParser()

	_, err := parser.Parse("")
	if err == nil {
		t.Error("expected error for empty SQL")
	}

	_, err = parser.Parse("NOT A SQL")
	if err == nil {
		t.Error("expected error for invalid SQL")
	}
}

func TestSQLParser_Parse_Join(t *testing.T) {
	parser := NewSQLParser()

	parsed, err := parser.Parse("SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id")
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	if len(parsed.Tables) < 1 {
		t.Errorf("expected at least 1 table, got %d", len(parsed.Tables))
	}
}

func TestSQLParser_Rebuild(t *testing.T) {
	parser := NewSQLParser()

	parsed, err := parser.Parse("SELECT id, name FROM users WHERE status = 'active'")
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	rebuilt := parser.Rebuild(parsed)
	if rebuilt == "" {
		t.Error("expected non-empty rebuilt SQL")
	}
}

func TestSQLParser_Rebuild_WithAllClauses(t *testing.T) {
	parser := NewSQLParser()

	parsed, err := parser.Parse("SELECT id, name FROM users WHERE status = 'active' GROUP BY name HAVING COUNT(*) > 1 ORDER BY id LIMIT 10")
	if err != nil {
		t.Fatalf("failed to parse SQL: %v", err)
	}

	rebuilt := parser.Rebuild(parsed)
	if rebuilt == "" {
		t.Error("expected non-empty rebuilt SQL")
	}
}

func TestSQLParser_SetLimit(t *testing.T) {
	parser := NewSQLParser()

	sql := "SELECT id, name FROM users ORDER BY id"
	result := parser.SetLimit(sql, 100)
	if result == "" {
		t.Error("expected non-empty SQL with limit")
	}
}

func TestSQLParser_SetLimit_InvalidSQL(t *testing.T) {
	parser := NewSQLParser()

	result := parser.SetLimit("NOT SQL", 100)
	if result != "NOT SQL" {
		t.Errorf("expected original SQL for invalid input, got %s", result)
	}
}

func TestSQLParser_AddWhereCondition(t *testing.T) {
	parser := NewSQLParser()

	sql := "SELECT id FROM users WHERE status = 'active'"
	result := parser.AddWhereCondition(sql, "age > 18")
	if result == "" {
		t.Error("expected non-empty SQL")
	}
}

func TestSQLParser_AddWhereCondition_NoExistingWhere(t *testing.T) {
	parser := NewSQLParser()

	sql := "SELECT id FROM users"
	result := parser.AddWhereCondition(sql, "age > 18")
	if result == "" {
		t.Error("expected non-empty SQL")
	}
}

func TestSQLParser_AddWhereCondition_InvalidSQL(t *testing.T) {
	parser := NewSQLParser()

	result := parser.AddWhereCondition("NOT SQL", "age > 18")
	if result != "NOT SQL" {
		t.Errorf("expected original SQL for invalid input, got %s", result)
	}
}

func TestSQLParser_AddOrderBy(t *testing.T) {
	parser := NewSQLParser()

	sql := "SELECT id FROM users ORDER BY name"
	result := parser.AddOrderBy(sql, "id")
	if result == "" {
		t.Error("expected non-empty SQL")
	}
}

func TestSQLParser_AddOrderBy_NoExistingOrder(t *testing.T) {
	parser := NewSQLParser()

	sql := "SELECT id FROM users"
	result := parser.AddOrderBy(sql, "id")
	if result == "" {
		t.Error("expected non-empty SQL")
	}
}

func TestSQLParser_AddOrderBy_InvalidSQL(t *testing.T) {
	parser := NewSQLParser()

	result := parser.AddOrderBy("NOT SQL", "id")
	if result != "NOT SQL" {
		t.Errorf("expected original SQL for invalid input, got %s", result)
	}
}

func TestSQLParser_ExtractTables(t *testing.T) {
	parser := NewSQLParser()

	tables := parser.ExtractTables("SELECT id FROM users, orders WHERE users.id = orders.user_id")
	if len(tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(tables))
	}
}

func TestSQLParser_ExtractTables_InvalidSQL(t *testing.T) {
	parser := NewSQLParser()

	tables := parser.ExtractTables("NOT SQL")
	if tables != nil {
		t.Errorf("expected nil for invalid SQL, got %v", tables)
	}
}

func TestSQLParser_ExtractFields(t *testing.T) {
	parser := NewSQLParser()

	fields := parser.ExtractFields("SELECT id, name, email FROM users")
	if len(fields) != 3 {
		t.Errorf("expected 3 fields, got %d", len(fields))
	}
}

func TestSQLParser_ExtractFields_WithAlias(t *testing.T) {
	parser := NewSQLParser()

	fields := parser.ExtractFields("SELECT id AS user_id, name AS user_name FROM users")
	if len(fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(fields))
	}
	if fields[0] != "user_id" {
		t.Errorf("expected user_id, got %s", fields[0])
	}
}

func TestSQLParser_ExtractFields_InvalidSQL(t *testing.T) {
	parser := NewSQLParser()

	fields := parser.ExtractFields("NOT SQL")
	if fields != nil {
		t.Errorf("expected nil for invalid SQL, got %v", fields)
	}
}

func TestParseFields(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"id, name, email", 3},
		{"*", 1},
		{"id AS user_id", 1},
		{"COUNT(*)", 1},
	}

	for _, tt := range tests {
		result := parseFields(tt.input)
		if len(result) != tt.expected {
			t.Errorf("parseFields(%q) returned %d fields, expected %d", tt.input, len(result), tt.expected)
		}
	}
}

func TestParseTables(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"users", 1},
		{"users, orders", 2},
		{"users u", 1},
		{"users u, orders o", 2},
	}

	for _, tt := range tests {
		result := parseTables(tt.input)
		if len(result) != tt.expected {
			t.Errorf("parseTables(%q) returned %d tables, expected %d", tt.input, len(result), tt.expected)
		}
	}
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"100", 100},
		{"  50  ", 50},
		{"0", 0},
		{"invalid", 0},
	}

	for _, tt := range tests {
		result := parseInt(tt.input)
		if result != tt.expected {
			t.Errorf("parseInt(%q) = %d, expected %d", tt.input, result, tt.expected)
		}
	}
}
