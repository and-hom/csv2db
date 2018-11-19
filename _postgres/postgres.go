package _postgres

import (
	"database/sql"
	"github.com/and-hom/csv2db/common"
	"errors"
	"reflect"
	"github.com/Sirupsen/logrus"
	_ "github.com/lib/pq"
	"bytes"
	"strings"
	"fmt"
)

func MakeDbTool(db *sql.DB) common.DbTool {
	tool := pgDbTool{common.CommonDbTool{
		Db:db,
		DbToGoTypeMapping:make(map[string]reflect.Kind),
		GoTypeToDbMapping:make(map[reflect.Kind]string),
		DefaultSchema:"public",
	}, }
	tool.RegisterType(reflect.Int64, "bigint", "bigserial")
	tool.RegisterType(reflect.Int32, "integer", "serial")
	tool.RegisterType(reflect.Int8, "smallint", "smallserial")
	tool.RegisterType(reflect.Float64, "double precision", "numeric")
	tool.RegisterType(reflect.Float32, "real")
	tool.RegisterType(reflect.Bool, "bool")
	tool.RegisterType(reflect.String, "character varying", "text", "character", "json", "jsonb", "uuid", "xml",
		"date", "time", "timestamp",
		"date with time zone", "time with time zone", "timestamp with time zone", )

	return tool
}

type pgDbTool struct {
	common.CommonDbTool
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
	rows, err := this.Db.Query(query, this.NvlSchema(schema), table)
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
	rows, err := this.Db.Query(`SELECT
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
					    AND f.attnum > 0 ORDER BY f.attnum ASC`, this.NvlSchema(schema), table)
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
		colDef.GoType, typeOk = this.DbToGoTypeMapping[dataType]
		if !typeOk {
			logrus.Warnf("Can not detect go type for column type %s - skip column", dataType)
			continue
		}
		colMap[colName] = colDef
	}
	return common.Schema{Types:colMap}, nil
}

func (this pgDbTool) CreateTable(schema, table string, tabSchema common.Schema) error {
	return this.CommonDbTool.CreateTable(this.NvlSchema(schema), table, tabSchema)
}

func (this pgDbTool) DropTable(schema, table string) error {
	return this.CommonDbTool.DropTable(this.NvlSchema(schema), table)
}

func (this pgDbTool) TruncateTable(schema, table string) error {
	return this.CommonDbTool.TruncateTable(this.NvlSchema(schema), table)
}

func (this pgDbTool) DeleteFromTable(schema, table string) error {
	return this.CommonDbTool.DeleteFromTable(this.NvlSchema(schema), table)
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
	sb.WriteString(this.NvlSchema(schema))
	sb.WriteString(".")
	sb.WriteString(table)
	sb.WriteString("(")
	sb.WriteString(strings.Join(names, ","))
	sb.WriteString(") VALUES (")
	sb.WriteString(strings.Join(params, ","))
	sb.WriteString(")")
	return sb.String(), names, nil
}
