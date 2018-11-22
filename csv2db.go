package main

import (
	"os"
	"bufio"
	"encoding/csv"
	"database/sql"
	_ "github.com/lib/pq"
	log "github.com/Sirupsen/logrus"
	"github.com/and-hom/csv2db/common"
	"io"
	"reflect"
	"fmt"
	"golang.org/x/net/html/charset"
	"github.com/pkg/errors"
	"github.com/and-hom/csv2db/_postgres"
	"github.com/and-hom/csv2db/_mysql"
	"github.com/xo/dburl"
)

type CsvToDb struct {
	Config       Config
	dbTool       common.DbTool
	tableExists  bool
	insertSchema common.InsertSchema
	tableName    common.TableName
	inserter     common.Inserter
}

func (this *CsvToDb) Perform() error {
	dbUrl, err := dburl.Parse(this.Config.DbUrl)
	if err != nil {
		log.Fatalf("Can not parse DB url: %v", err)
	}
	db, err := sql.Open(dbUrl.Driver, dbUrl.DSN)
	if err != nil {
		log.Fatalf("Can not connect to database: %v", err)
	}
	defer db.Close()
	log.Debugf("Connected to %s", this.Config.DbUrl)

	this.dbTool = this.makeDbTool(db, dbUrl)
	this.tableName = this.dbTool.TableName(this.Config.Schema, this.Config.Table)

	csvReader, closer, err := this.createReader()
	if err != nil {
		return err
	}
	defer closer.Close()

	this.tableExists, err = this.dbTool.Exists(this.tableName)
	if err != nil {
		return err
	}

	if this.tableExists {
		if err = this.onTableExists(); err != nil {
			return err
		}

	}

	if !this.Config.HasHeader && !this.tableExists {
		log.Warn("Can not detect column names - using col1...colN column names. Use CSV header with flag %s or create table in the database", HEADER_FLAG)
	}

	first := true

	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Can not read CSV: %v", err)
		}

		if first {
			first = false
			if this.initInsertSchema(line); err != nil {
				log.Fatalf("Can not create insert schema: %v", err)
				return err
			}

			err = this.initializeInserter(db)
			if err != nil {
				return err
			}
			defer this.inserter.Close()

			if this.Config.HasHeader {
				continue
			}
		}

		err = this.inserter.Add(line...)
		if err != nil {
			return err
		}
	}

	return nil
}

func (this *CsvToDb) makeDbTool(db *sql.DB, dbUrl *dburl.URL) common.DbTool {
	switch dbUrl.Driver {
	case postgres:
		return _postgres.MakeDbTool(db)
	case mysql:
		return _mysql.MakeDbTool(db)
	default:
		log.Fatalf("Unsupported db type %s", dbUrl.Driver)
		return nil
	}
}

func (this *CsvToDb) initializeInserter(db *sql.DB) error {
	var err error
	this.inserter, err = common.CreateTxInserter(db, this.dbTool, this.tableName, this.insertSchema)
	return err
}

func (this *CsvToDb) initInsertSchema(line []string) error {
	csvSchema := this.parseCsvSchema(line)
	log.Debug("CSV schema is " + common.ObjectToJson(csvSchema, false))

	if this.tableExists {
		dbTableSchema, err := this.dbTool.LoadSchema(this.tableName)
		if err != nil {
			return err
		}
		log.Debug("DB schema is " + common.ObjectToJson(dbTableSchema, false))
		this.insertSchema = this.createInsertSchema(csvSchema, dbTableSchema)
	} else {
		if this.Config.TableMode.CreateIfMissing() || this.Config.TableMode.DropAndCreateIfExists() {
			err := this.dbTool.CreateTable(this.tableName, csvSchema)
			if err != nil {
				log.Fatalf("Can not create table %s.%s: %v", this.tableName, err)
				return err
			}
		} else {
			msg := fmt.Sprintf("Table %s.%s does not exists. Please set table mode to create or create table manually",
				this.tableName)
			log.Fatal(msg)
			return errors.New(msg)
		}
		this.insertSchema = csvSchema.ToInsertSchema()
	}
	log.Debug("Insert schema is " + common.ObjectToJson(this.insertSchema, false))
	return nil
}

func (this *CsvToDb) createInsertSchema(csvSchema, dbTableSchema common.Schema) common.InsertSchema {
	if this.Config.HasHeader {
		return common.CreateCsvToDbSchemaByName(csvSchema, dbTableSchema)
	} else {
		return common.CreateCsvToDbSchemaByIdx(csvSchema, dbTableSchema)
	}
}
func (this *CsvToDb) createReader() (*csv.Reader, io.Closer, error) {
	var reader *os.File
	var err error
	if this.Config.FileName == "--" {
		reader = os.Stdin
	} else {
		reader, err = os.Open(this.Config.FileName)
		if err != nil {
			log.Fatalf("Can not open CSV file %s: %v", this.Config.FileName, err)
			return nil, nil, err
		}
	}

	var encodedReader io.Reader
	if this.Config.Encoding == "UTF-8" {
		encodedReader = reader
	} else {
		encodedReader, err = charset.NewReader(reader, this.Config.Encoding)
		if err != nil {
			log.Fatalf("Can not decode file dfrom charset %s: %v", this.Config.Encoding, err)
			return nil, nil, err
		}
	}
	fileReader := bufio.NewReader(encodedReader)
	csvReader := csv.NewReader(fileReader)
	csvReader.Comma = ([]rune(this.Config.Delimiter))[0]
	return csvReader, reader, nil
}

func (this *CsvToDb) parseCsvSchema(line []string) common.Schema {
	if this.Config.HasHeader {
		return parseSchema(line)
	} else {
		return nColsSchema(len(line))
	}
}

func (this *CsvToDb)onTableExists() error {
	if this.Config.TableMode.DropAndCreateIfExists() {
		err := this.dbTool.DropTable(this.tableName)
		if err != nil {
			log.Fatalf("Can not drop table %s.%s: %v", this.tableName, err)
			return err
		}
		this.tableExists = false
	} else if this.Config.TableMode.TruncatePrevious() {
		err := this.dbTool.TruncateTable(this.tableName)
		if err != nil {
			log.Fatalf("Can truncate table %s.%s: %v", this.tableName, err)
			return err
		}
	} else if this.Config.TableMode.DeletePrevious() {
		err := this.dbTool.DeleteFromTable(this.tableName)
		if err != nil {
			log.Fatalf("Can not delete all from table %s.%s: %v", this.tableName, err)
			return err
		}
	}
	return nil
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
