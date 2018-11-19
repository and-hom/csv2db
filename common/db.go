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
	Exists(schema, table string) (bool, error)
	LoadSchema(schema, table string) (Schema, error)
	CreateTable(schema, table string, tabSchema Schema) error
	DeleteFromTable(schema, table string) error
	TruncateTable(schema, table string) error
	DropTable(schema, table string) error
	InsertQuery(schema, table string, tabSchema InsertSchema) (string, []string, error)
}

type CommonDbTool struct {
	Db                *sql.DB
	DbToGoTypeMapping map[string]reflect.Kind
	GoTypeToDbMapping map[reflect.Kind]string
	DefaultSchema string
}

func (this CommonDbTool) RegisterType(goType reflect.Kind, dbPrimaryType string, dbTypes ...string) {
	this.DbToGoTypeMapping[dbPrimaryType] = goType
	for _, dbType := range dbTypes {
		this.DbToGoTypeMapping[dbType] = goType
	}
	this.GoTypeToDbMapping[goType] = dbPrimaryType
}

func (this CommonDbTool) CreateTable(schema, table string, tabSchema Schema) error {
	if len(tabSchema.Types) == 0 {
		return errors.New("Can not create table without any column")
	}
	sb := bytes.NewBufferString("CREATE TABLE ")
	sb.WriteString(this.NvlSchema(schema))
	sb.WriteString(".")
	sb.WriteString(table)
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
		sb.WriteString(name)
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

func (this CommonDbTool) DropTable(schema, table string) error {
	_, err := this.Db.Exec(fmt.Sprintf("DROP TABLE %s.%s", this.NvlSchema(schema), table))
	return err
}

func (this CommonDbTool) TruncateTable(schema, table string) error {
	_, err := this.Db.Exec(fmt.Sprintf("TRUNCATE TABLE %s.%s", this.NvlSchema(schema), table))
	return err
}

func (this CommonDbTool) DeleteFromTable(schema, table string) error {
	_, err := this.Db.Exec(fmt.Sprintf("DELETE FROM %s.%s", this.NvlSchema(schema), table))
	return err
}

func (this CommonDbTool) NvlSchema(schema string) string {
	if schema == "" {
		return this.DefaultSchema
	}
	return schema
}
