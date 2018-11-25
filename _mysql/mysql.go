package main

import (
	"database/sql"
	"github.com/and-hom/csv2db/common"
	"errors"
	"reflect"
	"github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
	"bytes"
	"strings"
	"log"
	"github.com/and-hom/csv2db/common/inserter"
	"github.com/xo/dburl"
)

func MakeDbTool(dbUrl *dburl.URL) (common.DbTool, *sql.DB, error) {
	db, err := sql.Open(dbUrl.Driver, dbUrl.DSN)
	if err != nil {
		log.Fatalf("Can not connect to database: %v", err)
		return nil, nil, err
	}

	rows, err := db.Query("SELECT DATABASE()")
	if err != nil || !rows.Next() {
		log.Fatalf("Can not determine current schema: %v", err)
		return nil, nil, err
	}
	defaultSchema := ""
	err = rows.Scan(&defaultSchema)
	if err != nil {
		log.Fatalf("Can not determine current schema: %v", err)
		return nil, nil, err
	}
	tool := myDbTool{common.CommonDbTool{
		Db:db,
		DbToGoTypeMapping:make(map[string]reflect.Kind),
		GoTypeToDbMapping:make(map[reflect.Kind]string),
		DefaultSchema:defaultSchema,
		EscapeF:func(s string) string {
			return "`" + s + "`"
		},
	}, }
	tool.RegisterType(reflect.Int64, "bigint")
	tool.RegisterType(reflect.Int32, "int", "mediumint")
	tool.RegisterType(reflect.Int16, "smallint")
	tool.RegisterType(reflect.Int8, "tinyint")

	tool.RegisterType(reflect.Float64, "double", "double precision")
	tool.RegisterType(reflect.Float32, "float", "real")

	tool.RegisterType(reflect.String, "text", "varchar", "char", "json", "enum", "date", "time", "timestamp", )

	return tool, db, nil
}

type myDbTool struct {
	common.CommonDbTool
}

func (this myDbTool) Exists(tableName common.TableName) (bool, error) {
	query := `SELECT COUNT(*)
			FROM information_schema.tables
			WHERE table_schema = ?
			AND table_name = ?`
	logrus.Debug(query)
	rows, err := this.Db.Query(query, tableName.SchemaPlain, tableName.TablePlain)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if rows.Next() {
		result := 0
		err := rows.Scan(&result)
		return result > 0, err
	}
	return false, errors.New("Empty query for select exists")
}

func (this myDbTool) LoadSchema(tableName common.TableName) (common.Schema, error) {
	rows, err := this.Db.Query(`SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE
  					FROM INFORMATION_SCHEMA.COLUMNS
  					WHERE table_schema = ?
  					AND table_name = ?
  					ORDER BY ORDINAL_POSITION ASC`, tableName.SchemaPlain, tableName.TablePlain)
	if err != nil {
		return common.Schema{}, err
	}
	defer rows.Close()

	colMap := make(map[string]common.ColDef)
	i := 0
	for rows.Next() {
		colName := ""
		dataType := ""
		nullableStr := ""
		colDef := common.ColDef{OrderIndex:i}
		i += 1

		err := rows.Scan(&colName, &nullableStr, &dataType)
		if err != nil {
			return common.Schema{}, err
		}
		colDef.Nullable = nullableStr == "YES"

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

func (this myDbTool) InsertQuery(tableName common.TableName, insertSchema common.InsertSchema) (string, []string, error) {
	return this.InsertQueryMultiple(tableName, insertSchema, 1)
}

func (this myDbTool) InsertQueryMultiple(tableName common.TableName, insertSchema common.InsertSchema, rows int) (string, []string, error) {
	if len(insertSchema.Types) == 0 {
		return "", []string{}, errors.New("Can not insert 0 columns")
	}
	names := make([]string, 0, len(insertSchema.Types))
	escapedNames := make([]string, 0, len(insertSchema.Types))
	params := make([]string, 0, len(insertSchema.Types))
	i := 1
	for name, _ := range insertSchema.Types {
		names = append(names, name)
		escapedNames = append(escapedNames, this.Escape(name))
		params = append(params, "?")
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
		sb.WriteString(strings.Join(params, ","))
		sb.WriteString(")")
	}
	return sb.String(), names, nil
}

func (this myDbTool) CreateInserter(tableName common.TableName, insertSchema common.InsertSchema) (common.Inserter, error) {
	columnsCount := len(insertSchema.Types)
	maxRecordsPerBatch := 1
	if columnsCount > 0 {
		maxRecordsPerBatch = 1000 / columnsCount
	}
	return inserter.CreateBufferedTxInserter(this.Db, this, tableName, insertSchema, maxRecordsPerBatch)
}

