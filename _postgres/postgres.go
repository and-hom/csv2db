package _postgres

import (
	"database/sql"
	"github.com/and-hom/csv2db/common"
	"errors"
	"reflect"
	"github.com/Sirupsen/logrus"
	"bytes"
	"fmt"
	"strings"
)

func MakeDbTool(db *sql.DB) common.DbTool {
	tool := pgDbTool{
		db:db,
		dbToGoTypeMapping:make(map[string]reflect.Kind),
		goTypeToDbMapping:make(map[reflect.Kind]string),
	}
	tool.registerType(reflect.Int64, "bigint", "bigserial")
	tool.registerType(reflect.Int32, "integer", "serial")
	tool.registerType(reflect.Int8, "smallint", "smallserial")
	tool.registerType(reflect.Float64, "double precision", "numeric")
	tool.registerType(reflect.Float32, "real")
	tool.registerType(reflect.Bool, "bool")
	tool.registerType(reflect.String, "character varying", "text", "character", "json", "jsonb", "uuid", "xml",
		"date", "time", "timestamp",
		"date with time zone", "time with time zone", "timestamp with time zone", )

	return tool
}

type pgDbTool struct {
	db                *sql.DB
	dbToGoTypeMapping map[string]reflect.Kind
	goTypeToDbMapping map[reflect.Kind]string
}

func (this pgDbTool) registerType(goType reflect.Kind, dbPrimaryType string, dbTypes ...string) {
	this.dbToGoTypeMapping[dbPrimaryType] = goType
	for _, dbType := range dbTypes {
		this.dbToGoTypeMapping[dbType] = goType
	}
	this.goTypeToDbMapping[goType] = dbPrimaryType
}

func (this pgDbTool) Exists(schema, table string) (bool, error) {
	query := `SELECT EXISTS (
				   SELECT 1
				   FROM   pg_catalog.pg_class c
				   JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
				   WHERE  n.nspname = $1
				   AND    c.relname = $2
				   AND    c.relkind = 'r'
				)`
	logrus.Debug(query)
	rows, err := this.db.Query(query, nvlSchema(schema), table)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if rows.Next() {
		result := false
		err := rows.Scan(&result)
		return result, err
	}
	return false, errors.New("Empty query for select exists")
}

func (this pgDbTool) LoadSchema(schema, table string) (common.Schema, error) {
	rows, err := this.db.Query(`SELECT
					    f.attname AS name,
					    not f.attnotnull AS nullable,
					    pg_catalog.format_type(f.atttypid,f.atttypmod) AS type

					    FROM pg_attribute f
					    JOIN pg_class c ON c.oid = f.attrelid
					    JOIN pg_type t ON t.oid = f.atttypid
					    LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum
					    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
					    LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY (p.conkey)
					    LEFT JOIN pg_class AS g ON p.confrelid = g.oid
					WHERE c.relkind = 'r'::char
					    AND n.nspname = $1
					    AND c.relname = $2
					    AND f.attnum > 0 ORDER BY f.attnum ASC`, nvlSchema(schema), table)
	if err != nil {
		return common.Schema{}, err
	}
	defer rows.Close()

	colMap := make(map[string]common.ColDef)
	i := 0
	for rows.Next() {
		colName := ""
		dataType := ""
		colDef := common.ColDef{OrderIndex:i}
		i += 1

		err := rows.Scan(&colName, &colDef.Nullable, &dataType)
		if err != nil {
			return common.Schema{}, err
		}

		var typeOk = false
		colDef.GoType, typeOk = this.dbToGoTypeMapping[dataType]
		if !typeOk {
			logrus.Warnf("Can not detect go type for column type %s - skip column", dataType)
			continue
		}
		colMap[colName] = colDef
	}
	return common.Schema{Types:colMap}, nil
}

func (this pgDbTool) CreateTable(schema, table string, tabSchema common.Schema) error {
	if len(tabSchema.Types) == 0 {
		return errors.New("Can not create table without any column")
	}
	sb := bytes.NewBufferString("CREATE TABLE ")
	sb.WriteString(nvlSchema(schema))
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
		sqlType, registered := this.goTypeToDbMapping[colDef.GoType]
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

	_, err := this.db.Exec(sb.String())
	return err
}

func (this pgDbTool) DropTable(schema, table string) error {
	_, err := this.db.Exec(fmt.Sprintf("DROP TABLE %s.%s", nvlSchema(schema), table))
	return err
}

func (this pgDbTool) TruncateTable(schema, table string) error {
	_, err := this.db.Exec(fmt.Sprintf("TRUNCATE TABLE %s.%s", nvlSchema(schema), table))
	return err
}

func (this pgDbTool) DeleteFromTable(schema, table string) error {
	_, err := this.db.Exec(fmt.Sprintf("DELETE FROM %s.%s", nvlSchema(schema), table))
	return err
}

func (this pgDbTool) InsertQuery(schema, table string, insertSchema common.InsertSchema) (string, []string, error) {
	if len(insertSchema.Types) == 0 {
		return "", []string{}, errors.New("Can not insert 0 columns")
	}
	names := make([]string, 0, len(insertSchema.Types))
	params := make([]string, 0, len(insertSchema.Types))
	i := 1
	for name, _ := range insertSchema.Types {
		names = append(names, name)
		params = append(params, fmt.Sprintf("$%d", i))
		i += 1
	}

	sb := bytes.NewBufferString("INSERT INTO ")
	sb.WriteString(nvlSchema(schema))
	sb.WriteString(".")
	sb.WriteString(table)
	sb.WriteString("(")
	sb.WriteString(strings.Join(names, ","))
	sb.WriteString(") VALUES (")
	sb.WriteString(strings.Join(params, ","))
	sb.WriteString(")")
	return sb.String(), names, nil
}

func nvlSchema(schema string) string {
	if schema == "" {
		return "public"
	}
	return schema
}
