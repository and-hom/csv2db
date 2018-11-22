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
	"github.com/and-hom/csv2db/common/inserter"
)

func MakeDbTool(db *sql.DB) common.DbTool {
	tool := pgDbTool{common.CommonDbTool{
		Db:db,
		DbToGoTypeMapping:make(map[string]reflect.Kind),
		GoTypeToDbMapping:make(map[reflect.Kind]string),
		DefaultSchema:"public",
		EscapeF:func(s string) string {
			return "\"" + s + "\""
		},
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

func (this pgDbTool) Exists(tableName common.TableName) (bool, error) {
	query := `SELECT EXISTS (
				   SELECT 1
				   FROM   pg_catalog.pg_class c
				   JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
				   WHERE  n.nspname = $1
				   AND    c.relname = $2
				   AND    c.relkind = 'r'
				)`
	logrus.Debug(query)
	rows, err := this.Db.Query(query, tableName.SchemaPlain, tableName.TablePlain)
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

func (this pgDbTool) LoadSchema(tableName common.TableName) (common.Schema, error) {
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
					    AND f.attnum > 0 ORDER BY f.attnum ASC`,
		tableName.SchemaPlain, tableName.TablePlain)
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

func (this pgDbTool) InsertQuery(tableName common.TableName, insertSchema common.InsertSchema) (string, []string, error) {
	return this.InsertQueryMultiple(tableName, insertSchema, 1)
}

func (this pgDbTool) InsertQueryMultiple(tableName common.TableName, insertSchema common.InsertSchema, rows int) (string, []string, error) {
	rowParamCount := len(insertSchema.Types)
	if rowParamCount == 0 {
		return "", []string{}, errors.New("Can not insert 0 columns")
	}
	names := make([]string, 0, rowParamCount)
	escapedNames := make([]string, 0, rowParamCount)
	i := 1
	for name, _ := range insertSchema.Types {
		names = append(names, name)
		escapedNames = append(escapedNames, this.Escape(name))
		i += 1
	}

	sb := bytes.NewBufferString("INSERT INTO ")
	sb.WriteString(tableName.Schema)
	sb.WriteString(".")
	sb.WriteString(tableName.Table)
	sb.WriteString("(")
	sb.WriteString(strings.Join(escapedNames, ","))
	sb.WriteString(") VALUES ")
	for i := 0; i < rows; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(")
		for j := 0; j < rowParamCount; j++ {
			if j > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(fmt.Sprintf("$%d", i * rowParamCount + j + 1))
		}
		sb.WriteString(")")
	}
	return sb.String(), names, nil
}

func (this pgDbTool) CreateInserter(tableName common.TableName, insertSchema common.InsertSchema) (common.Inserter, error) {
	return inserter.CreateBufferedTxInserter(this.Db, this, tableName, insertSchema, 1000 / len(insertSchema.Types))
}
