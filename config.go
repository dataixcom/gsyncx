package gsyncx

import (
	"github.com/dataixcom/gdbx/base/config"
)

type DBType = config.DBType

const (
	DBMySQL    DBType = config.MySQL
	DBOracle   DBType = config.Oracle
	DBPostgres DBType = config.Postgres
)

type DSNConfig = config.DSNConfig

type Field = config.Field

type SelectConfig = config.SelectConfig

type InsertConfig = config.InsertConfig

type BatchInsertConfig = config.BatchInsertConfig

type BatchUpsertConfig = config.BatchUpsertConfig

type UpdateConfig = config.UpdateConfig

type DeleteConfig = config.DeleteConfig

type PageConfig = config.PageConfig

type WhereClauseBuilder = config.WhereClauseBuilder

type LogicalOperator = config.LogicalOperator

type ComparisonOperator = config.ComparisonOperator

type Condition = config.Condition

type JoinTable = config.JoinTable

const (
	AND LogicalOperator = config.AND
	OR  LogicalOperator = config.OR
)

const (
	Eq         ComparisonOperator = config.Eq
	Neq        ComparisonOperator = config.Neq
	Gt         ComparisonOperator = config.Gt
	Gte        ComparisonOperator = config.Gte
	Lt         ComparisonOperator = config.Lt
	Lte        ComparisonOperator = config.Lte
	Like       ComparisonOperator = config.Like
	NotLike    ComparisonOperator = config.NotLike
	In         ComparisonOperator = config.In
	NotIn      ComparisonOperator = config.NotIn
	Between    ComparisonOperator = config.Between
	NotBetween ComparisonOperator = config.NotBetween
	IsNull     ComparisonOperator = config.IsNull
	IsNotNull  ComparisonOperator = config.IsNotNull
)

func NewWhereClauseBuilder() *WhereClauseBuilder {
	return config.NewWhereClauseBuilder()
}

func FormatFieldName(name string, dbType DBType) string {
	return config.FormatFieldName(name, dbType)
}

func FormatTableName(name string, dbType DBType) string {
	return config.FormatTableName(name, dbType)
}

func SanitizeIdentifier(name string) error {
	return config.SanitizeIdentifier(name)
}

func JoinFields(fields []string, dbType DBType) string {
	return config.JoinFields(fields, dbType)
}

func ParseDBType(s string) (DBType, error) {
	return config.ParseDBType(s)
}
