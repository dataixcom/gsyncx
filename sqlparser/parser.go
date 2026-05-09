package sqlparser

import (
	"fmt"
	"regexp"
	"strings"
)

type SQLParser struct{}

func NewSQLParser() *SQLParser {
	return &SQLParser{}
}

type ParsedSQL struct {
	SelectClause string   `json:"select_clause"`
	FromClause   string   `json:"from_clause"`
	WhereClause  string   `json:"where_clause,omitempty"`
	GroupBy      string   `json:"group_by,omitempty"`
	Having       string   `json:"having,omitempty"`
	OrderBy      string   `json:"order_by,omitempty"`
	Limit        *int     `json:"limit,omitempty"`
	Offset       *int     `json:"offset,omitempty"`
	Tables       []string `json:"tables"`
	Fields       []string `json:"fields"`
	IsSubQuery   bool     `json:"is_sub_query,omitempty"`
}

var (
	selectRegex = regexp.MustCompile(`(?i)^\s*SELECT\s+(.+?)\s+FROM\s+(.+?)(?:\s+WHERE\s+(.+?))?(?:\s+GROUP\s+BY\s+(.+?))?(?:\s+HAVING\s+(.+?))?(?:\s+ORDER\s+BY\s+(.+?))?(?:\s+LIMIT\s+(\d+))?(?:\s+OFFSET\s+(\d+))?\s*$`)
)

func (p *SQLParser) Parse(sql string) (*ParsedSQL, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, fmt.Errorf("empty SQL")
	}

	matches := selectRegex.FindStringSubmatch(sql)
	if matches == nil {
		return nil, fmt.Errorf("failed to parse SQL: unsupported format")
	}

	result := &ParsedSQL{
		SelectClause: strings.TrimSpace(matches[1]),
		FromClause:   strings.TrimSpace(matches[2]),
	}

	if matches[3] != "" {
		result.WhereClause = strings.TrimSpace(matches[3])
	}
	if matches[4] != "" {
		result.GroupBy = strings.TrimSpace(matches[4])
	}
	if matches[5] != "" {
		result.Having = strings.TrimSpace(matches[5])
	}
	if matches[6] != "" {
		result.OrderBy = strings.TrimSpace(matches[6])
	}
	if matches[7] != "" {
		limit := parseInt(matches[7])
		result.Limit = &limit
	}
	if matches[8] != "" {
		offset := parseInt(matches[8])
		result.Offset = &offset
	}

	result.Fields = parseFields(result.SelectClause)
	result.Tables = parseTables(result.FromClause)

	return result, nil
}

func (p *SQLParser) AddWhereCondition(sql, condition string) string {
	parsed, err := p.Parse(sql)
	if err != nil {
		return sql
	}

	if parsed.WhereClause != "" {
		parsed.WhereClause = parsed.WhereClause + " AND " + condition
	} else {
		parsed.WhereClause = condition
	}

	return p.Rebuild(parsed)
}

func (p *SQLParser) AddOrderBy(sql, orderBy string) string {
	parsed, err := p.Parse(sql)
	if err != nil {
		return sql
	}

	if parsed.OrderBy != "" {
		parsed.OrderBy = parsed.OrderBy + ", " + orderBy
	} else {
		parsed.OrderBy = orderBy
	}

	return p.Rebuild(parsed)
}

func (p *SQLParser) SetLimit(sql string, limit int) string {
	parsed, err := p.Parse(sql)
	if err != nil {
		return sql
	}

	parsed.Limit = &limit
	return p.Rebuild(parsed)
}

func (p *SQLParser) Rebuild(parsed *ParsedSQL) string {
	var sb strings.Builder

	sb.WriteString("SELECT ")
	sb.WriteString(parsed.SelectClause)
	sb.WriteString(" FROM ")
	sb.WriteString(parsed.FromClause)

	if parsed.WhereClause != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(parsed.WhereClause)
	}
	if parsed.GroupBy != "" {
		sb.WriteString(" GROUP BY ")
		sb.WriteString(parsed.GroupBy)
	}
	if parsed.Having != "" {
		sb.WriteString(" HAVING ")
		sb.WriteString(parsed.Having)
	}
	if parsed.OrderBy != "" {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(parsed.OrderBy)
	}
	if parsed.Limit != nil {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", *parsed.Limit))
	}
	if parsed.Offset != nil {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", *parsed.Offset))
	}

	return sb.String()
}

func (p *SQLParser) ExtractTables(sql string) []string {
	parsed, err := p.Parse(sql)
	if err != nil {
		return nil
	}
	return parsed.Tables
}

func (p *SQLParser) ExtractFields(sql string) []string {
	parsed, err := p.Parse(sql)
	if err != nil {
		return nil
	}
	return parsed.Fields
}

func parseFields(selectClause string) []string {
	fields := strings.Split(selectClause, ",")
	result := make([]string, 0, len(fields))
	for _, f := range fields {
		trimmed := strings.TrimSpace(f)
		if trimmed != "" {
			if asIdx := strings.LastIndex(strings.ToUpper(trimmed), " AS "); asIdx >= 0 {
				trimmed = strings.TrimSpace(trimmed[asIdx+4:])
			}
			result = append(result, trimmed)
		}
	}
	return result
}

func parseTables(fromClause string) []string {
	tables := strings.Split(fromClause, ",")
	result := make([]string, 0, len(tables))
	for _, t := range tables {
		trimmed := strings.TrimSpace(t)
		if trimmed != "" {
			if spaceIdx := strings.Index(trimmed, " "); spaceIdx >= 0 {
				trimmed = strings.TrimSpace(trimmed[:spaceIdx])
			}
			result = append(result, trimmed)
		}
	}
	return result
}

func parseInt(s string) int {
	var result int
	fmt.Sscanf(strings.TrimSpace(s), "%d", &result)
	return result
}
