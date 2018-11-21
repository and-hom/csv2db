package common

import (
	"database/sql"
	"reflect"
	"bytes"
	"github.com/Sirupsen/logrus"
	"fmt"
	"errors"
)

type DbTool interface {
	TableName(schema, table string) TableName

	Exists(tableName TableName) (bool, error)
	LoadSchema(tableName TableName) (Schema, error)
	CreateTable(tableName TableName, tabSchema Schema) error
	DeleteFromTable(tableName TableName) error
	TruncateTable(tableName TableName) error
	DropTable(tableName TableName) error
	InsertQuery(tableName TableName, tabSchema InsertSchema) (string, []string, error)
}

type CommonDbTool struct {
	Db                *sql.DB
	DbToGoTypeMapping map[string]reflect.Kind
	GoTypeToDbMapping map[reflect.Kind]string
	DefaultSchema     string
	EscapeF           func(string) string
}

func (this CommonDbTool) TableName(schema, table string) TableName {
	schemaNotEmpty := this.NvlSchema(schema)
	return TableName{
		Table:this.Escape(table),
		Schema:this.Escape(schemaNotEmpty),
		TablePlain:table,
		SchemaPlain:schemaNotEmpty,
	}
}

func (this CommonDbTool) RegisterType(goType reflect.Kind, dbPrimaryType string, dbTypes ...string) {
	this.DbToGoTypeMapping[dbPrimaryType] = goType
	for _, dbType := range dbTypes {
		this.DbToGoTypeMapping[dbType] = goType
	}
	this.GoTypeToDbMapping[goType] = dbPrimaryType
}

func (this CommonDbTool) CreateTable(tableName TableName, tabSchema Schema) error {
	if len(tabSchema.Types) == 0 {
		return errors.New("Can not create table without any column")
	}
	sb := bytes.NewBufferString("CREATE TABLE ")
	sb.WriteString(tableName.Schema)
	sb.WriteString(".")
	sb.WriteString(tableName.Table)
	sb.WriteString("(")

	first := true
	for name, colDef := range tabSchema.Types {
		if first {
			first = false
		} else {
			sb.WriteString(", ")
		}
		sqlType, registered := this.GoTypeToDbMapping[colDef.GoType]
		if !registered {
			return fmt.Errorf("No registered SQL type for go type %v", colDef.GoType)
		}
		sb.WriteString(this.Escape(name))
		sb.WriteString(" ")
		sb.WriteString(sqlType)
		if !colDef.Nullable {
			sb.WriteString(" NOT NULL")
		}
	}
	sb.WriteString(")")

	logrus.Debug(sb.String())

	_, err := this.Db.Exec(sb.String())
	return err
}

func (this CommonDbTool) DropTable(tableName TableName) error {
	_, err := this.Db.Exec(fmt.Sprintf("DROP TABLE %s.%s", tableName.Schema, tableName.Table))
	return err
}

func (this CommonDbTool) TruncateTable(tableName TableName) error {
	_, err := this.Db.Exec(fmt.Sprintf("TRUNCATE TABLE %s.%s", tableName.Schema, tableName.Table))
	return err
}

func (this CommonDbTool) DeleteFromTable(tableName TableName) error {
	_, err := this.Db.Exec(fmt.Sprintf("DELETE FROM %s.%s", tableName.Schema, tableName.Table))
	return err
}

func (this CommonDbTool) NvlSchema(schema string) string {
	if schema == "" {
		return this.DefaultSchema
	}
	return schema
}

func (this CommonDbTool) Escape(v string) string {
	if this.EscapeF == nil {
		return v
	}
	return this.EscapeF(v)
}

type TableName struct {
	Table       string
	Schema      string
	TablePlain  string
	SchemaPlain string
}