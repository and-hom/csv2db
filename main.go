package main

import (
	"os"
	"bufio"
	"encoding/csv"
	"database/sql"
	_ "github.com/lib/pq"
	"gopkg.in/urfave/cli.v1"
	log "github.com/Sirupsen/logrus"
	"github.com/and-hom/csv2db/common"
	"io"
	"reflect"
	"github.com/and-hom/csv2db/postgres"
)

const CONN_STRING_FLAG = "connection-string"
const DB_TYPE_FLAG = "db-type"
const TABLE_FLAG = "table"
const TABLE_MODE_FLAG = "table-mode"
const INPUT_FILE_FLAG = "input-file"
const HEADER_FLAG = "has-header"

func main() {
	log.SetLevel(log.DebugLevel)

	app := cli.NewApp()
	app.Name = "csv2db"
	app.Usage = "Import your CSV to database as a table"
	app.Action = mainAction

	app.Flags = []cli.Flag{
		cli.StringFlag{Name:CONN_STRING_FLAG, Usage:"Connection string"},
		cli.StringFlag{Name:DB_TYPE_FLAG, Usage:"Database type"},
		cli.StringFlag{Name:TABLE_FLAG, Usage:"Table name"},
		cli.StringFlag{Name:TABLE_MODE_FLAG, Usage:"Create table"},
		cli.StringFlag{Name:INPUT_FILE_FLAG, Usage:"Input CSV file"},
		cli.BoolFlag{Name:HEADER_FLAG, Usage:"True if first line is header"},
		cli.StringFlag{Name:"encoding", Usage:"Input file encoding", Value:"UTF-8"},
		cli.StringFlag{Name:"separator", Usage:"CSV separator", Value:","},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func mainAction(c *cli.Context) error {
	dbType := c.String(DB_TYPE_FLAG)
	connString := c.String(CONN_STRING_FLAG)
	db, err := sql.Open(dbType, connString)
	if err != nil {
		log.Fatalf("Can not connect to database: %v", err)
	}
	defer db.Close()
	log.Debugf("Connected to %s %s", dbType, connString)

	schema := "public"
	table := c.String(TABLE_FLAG)
	scholdCreateTable := c.String(TABLE_MODE_FLAG) == "create"
	dbTool := postgres.MakeDbTool(db)

	fileName := c.String(INPUT_FILE_FLAG)
	header := c.Bool(HEADER_FLAG)
	csvFile, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Can not open CSV file %s: %v", fileName, err)
	}
	defer csvFile.Close()
	fileReader := bufio.NewReader(csvFile)
	csvReader := csv.NewReader(fileReader)

	tableExists, err := dbTool.Exists(schema, table)
	if err != nil {
		return err
	}

	var insertSchema common.InsertSchema
	if !header && !tableExists {
		log.Warn("Can not detect column names. Use CSV header with flag %s or create table in the database", HEADER_FLAG)
	}
	first := true
	var st *sql.Stmt
	var columnNames []string
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Can not read CSV: %v", err)
		}

		if first {
			first = false

			if header {
				csvSchema := common.Schema{}
				dbTableSchema := common.Schema{}

				parseSchema(line, &csvSchema)
				if tableExists {
					err = dbTool.LoadSchema(schema, table, &dbTableSchema)
					if err != nil {
						return err
					}
					log.Debug("csv schema ", csvSchema)
					log.Debug("db schema ", dbTableSchema)
					insertSchema = common.CreateCsvToDbSchema(csvSchema, dbTableSchema)
				} else if scholdCreateTable {
					err = dbTool.CreateTable(schema, table, csvSchema)
					if err != nil {
						return err
					}
					insertSchema = csvSchema.ToInsertSchema()
				} else {
					log.Fatalf("Table %s.%s does not exists. Please set table mode to create or create table manually", schema, table)
				}
			}

			log.Debug("insert schema ", insertSchema)

			var query string
			query, columnNames, err = dbTool.InsertQuery(schema, table, insertSchema)
			if err != nil {
				return err
			}
			log.Debug("QUERY IS ", query)
			st, err = db.Prepare(query)
			if err != nil {
				return err
			}
			defer st.Close()

			if header {
				continue
			}
		}

		args := prepare(insertSchema, columnNames, line)
		log.Debug(args)
		_, err = st.Exec(args...)
		if err != nil {
			return err
		}
	}

	return nil
}

func prepare(insertSchema common.InsertSchema, columnNames []string, line []string) []interface{} {
	result := make([]interface{}, 0, len(insertSchema.Types))
	for _, name := range columnNames {
		typeDef, found := insertSchema.Types[name]
		if !found {
			log.Fatalf("Can not find column %s in insert schema: %v", name, insertSchema)
		}
		valStr := line[typeDef.OrderIndex]
		value, err := typeDef.ValMapper(valStr)
		if err != nil {
			log.Fatalf("Can not convert value %s at column %d to %v(nullable=%v)",
				valStr, typeDef.OrderIndex, typeDef.GoType, typeDef.Nullable)
		}
		result = append(result, value)
	}
	return result
}

func parseSchema(header []string, schema *common.Schema) {
	colDefs := make(map[string]common.ColDef)
	for i, col := range header {
		colDefs[col] = common.ColDef{GoType:reflect.String, Nullable:false, OrderIndex:i, }
	}
	schema.Types = colDefs
}