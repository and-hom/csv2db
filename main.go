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
	"strings"
	"github.com/and-hom/csv2db/_postgres"
	"fmt"
	"golang.org/x/net/html/charset"
	"github.com/pkg/errors"
)

const CONN_STRING_FLAG = "connection-string, conn"
const DB_TYPE_FLAG = "db-type, db"
const TABLE_FLAG = "table, t"
const TABLE_MODE_FLAG = "table-mode, m"
const INPUT_FILE_FLAG = "input-file, i"
const HEADER_FLAG = "has-header, hh"
const DELIMITER_FLAG = "delimiter, d"
const ENCODING_FLAG = "encoding, e"

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
		cli.StringFlag{Name:TABLE_MODE_FLAG, Usage:"Table mode flag. Available values are: " + strings.Join(modes, ", ")},
		cli.StringFlag{Name:INPUT_FILE_FLAG, Usage:"Input CSV file. Use -- to read from stdin"},
		cli.BoolFlag{Name:HEADER_FLAG, Usage:"True if first line is header"},
		cli.StringFlag{Name:ENCODING_FLAG, Usage:"Input file encoding", Value:"UTF-8"},
		cli.StringFlag{Name:DELIMITER_FLAG, Usage:"CSV cell delimiter", Value:","},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func mainAction(c *cli.Context) error {
	conf := LoadConfig(c)
	fmt.Println(conf.String())
	log.Infof("Run with config: \n%s", conf.String())
	return CsvToDb{Config:conf}.Perform()
}

func LoadConfig(c *cli.Context) Config {
	tableParts := strings.Split(c.String(flagName(TABLE_FLAG)), ".")
	schemaName := ""
	if len(tableParts) > 1 {
		schemaName = tableParts[0]
	}
	cliConfig := Config{
		DbType:DbType(c.String(flagName(DB_TYPE_FLAG))),
		DbConnString:c.String(flagName(CONN_STRING_FLAG)),

		Schema:schemaName,
		Table:tableParts[len(tableParts) - 1],

		FileName : c.String(flagName(INPUT_FILE_FLAG)),
		HasHeader : c.Bool(flagName(HEADER_FLAG)),
		Delimiter : c.String(flagName(DELIMITER_FLAG)),
		Encoding : c.String(flagName(ENCODING_FLAG)),
	}
	cliConfig.SetMode(c.String(TABLE_MODE_FLAG))
	cliConfig.Validate()
	return cliConfig
}

func flagName(flagName string) string {
	s := strings.Split(flagName, ",")
	return strings.TrimSpace(s[0])
}

type CsvToDb struct {
	Config Config
}

func (this CsvToDb) Perform() error {
	db, err := sql.Open(string(this.Config.DbType), this.Config.DbConnString)
	if err != nil {
		log.Fatalf("Can not connect to database: %v", err)
	}
	defer db.Close()
	log.Debugf("Connected to %s %s", this.Config.DbType, this.Config.DbType)

	dbTool := _postgres.MakeDbTool(db)

	var reader *os.File
	if this.Config.FileName == "--" {
		reader = os.Stdin
	} else {
		reader, err = os.Open(this.Config.FileName)
		if err != nil {
			log.Fatalf("Can not open CSV file %s: %v", this.Config.FileName, err)
			return err
		}
	}
	defer reader.Close()

	var encodedReader io.Reader
	if this.Config.Encoding == "UTF-8" {
		encodedReader = reader
	} else {
		encodedReader, err = charset.NewReader(reader, this.Config.Encoding)
		if err != nil {
			log.Fatalf("Can not decode file dfrom charset %s: %v", this.Config.Encoding, err)
			return err
		}
	}
	fileReader := bufio.NewReader(encodedReader)
	csvReader := csv.NewReader(fileReader)
	csvReader.Comma = ([]rune(this.Config.Delimiter))[0]

	tableExists, err := dbTool.Exists(this.Config.Schema, this.Config.Table)
	if err != nil {
		return err
	}

	if tableExists {
		if this.Config.DropAndCreateIfExists {
			err := dbTool.DropTable(this.Config.Schema, this.Config.Table)
			if err != nil {
				log.Fatalf("Can not drop table %s.%s: %v", this.Config.Schema, this.Config.Table, err)
				return err
			}
		} else if this.Config.TruncatePrevious {
			err := dbTool.TruncateTable(this.Config.Schema, this.Config.Table)
			if err != nil {
				log.Fatalf("Can truncate table %s.%s: %v", this.Config.Schema, this.Config.Table, err)
				return err
			}
		} else if this.Config.DeletePrevious {
			err := dbTool.DeleteFromTable(this.Config.Schema, this.Config.Table)
			if err != nil {
				log.Fatalf("Can not delete all from table %s.%s: %v", this.Config.Schema, this.Config.Table, err)
				return err
			}
		}
	}

	var insertSchema common.InsertSchema
	if !this.Config.HasHeader && !tableExists {
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
			csvSchema := this.parseCsvSchema(line)
			log.Debug("CSV schema is " + csvSchema.String())

			if tableExists {
				dbTableSchema, err := dbTool.LoadSchema(this.Config.Schema, this.Config.Table)
				if err != nil {
					return err
				}
				log.Debug("DB schema is " + dbTableSchema.String())

				if this.Config.HasHeader {
					insertSchema = common.CreateCsvToDbSchemaByName(csvSchema, dbTableSchema)
				} else {
					insertSchema = common.CreateCsvToDbSchemaByIdx(csvSchema, dbTableSchema)
				}
			} else {
				if this.Config.CreateIfMissing || this.Config.DropAndCreateIfExists {
					err = dbTool.CreateTable(this.Config.Schema, this.Config.Table, csvSchema)
					if err != nil {
						log.Fatalf("Can not create table %s.%s: %v", this.Config.Schema, this.Config.Table, err)
						return err
					}

				} else {
					msg := fmt.Sprintf("Table %s.%s does not exists. Please set table mode to create or create table manually",
						this.Config.Schema, this.Config.Table)
					log.Fatal(msg)
					return errors.New(msg)
				}

				insertSchema = csvSchema.ToInsertSchema()
			}
			log.Debug("Insert schema is " + insertSchema.String())

			var query string
			query, columnNames, err = dbTool.InsertQuery(this.Config.Schema, this.Config.Table, insertSchema)
			if err != nil {
				return err
			}
			log.Debug("Insert query is ", query)

			st, err = db.Prepare(query)
			if err != nil {
				return err
			}
			defer st.Close()

			if this.Config.HasHeader {
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

func (this CsvToDb) parseCsvSchema(line []string) common.Schema {
	if this.Config.HasHeader {
		return parseSchema(line)
	} else {
		return nColsSchema(len(line))
	}
}

func parseSchema(header []string) common.Schema {
	schema := common.Schema{Types:make(map[string]common.ColDef)}
	for i, col := range header {
		schema.Types[col] = common.ColDef{
			GoType:reflect.String,
			Nullable:false,
			OrderIndex:i,
		}
	}
	return schema
}

func nColsSchema(colsCount int) common.Schema {
	schema := common.Schema{Types:make(map[string]common.ColDef)}
	for i := 0; i < colsCount; i++ {
		schema.Types[fmt.Sprintf("col%d", i)] = common.ColDef{
			GoType:reflect.String,
			Nullable:false,
			OrderIndex:i,
		}
	}
	return schema
}