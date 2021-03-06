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
	"fmt"
	"golang.org/x/net/html/charset"
	"github.com/pkg/errors"
	"github.com/and-hom/csv2db/_postgres"
	"github.com/and-hom/csv2db/_mysql"
	"github.com/xo/dburl"
	"time"
	"github.com/machinebox/progress"
)

const MIN_SIZE_BYTES_TO_SHOW_PROGRESS = 100

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
	initializeCredentialsIfMissing(dbUrl)

	db, err := sql.Open(dbUrl.Driver, dbUrl.DSN)
	if err != nil {
		log.Fatalf("Can not connect to database: %v", err)
	}
	defer db.Close()
	log.Debugf("Connected to %s", this.Config.DbUrl)

	this.dbTool = this.makeDbTool(db, dbUrl)
	this.tableName = this.dbTool.TableName(this.Config.Schema, this.Config.Table)

	csvReader, closer, size, progressFunc, err := this.createReader()
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

	progressBar := InitProgressBar(progressFunc, size)
	if size > MIN_SIZE_BYTES_TO_SHOW_PROGRESS {
		progressBar.Start()
	}

	first := true
	var started time.Time

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

			this.inserter, err = this.dbTool.CreateInserter(this.tableName, this.insertSchema)
			if err != nil {
				return err
			}
			defer this.inserter.Close()

			started = time.Now()

			if this.Config.HasHeader {
				continue
			}
		}

		err = this.inserter.Add(line...)
		if err != nil {
			log.Errorf("Can not insert: %v", err)
			return err
		}
	}

	progressBar.Stop()
	log.Infof("Performed in %s", time.Since(started).String())

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

func (this *CsvToDb) initInsertSchema(line []string) error {
	csvSchema := this.parseCsvSchema(line)
	log.Debugf("CSV schema is:\n%s\n", csvSchema.ToAsciiTable())

	if this.tableExists {
		dbTableSchema, err := this.dbTool.LoadSchema(this.tableName)
		if err != nil {
			return err
		}
		log.Debugf("DB schema is:\n%s\n", dbTableSchema.ToAsciiTable())
		this.insertSchema = this.createInsertSchema(csvSchema, dbTableSchema)
	} else {
		if this.Config.TableMode.CreateIfMissing() || this.Config.TableMode.DropAndCreateIfExists() {
			err := this.dbTool.CreateTable(this.tableName, csvSchema)
			if err != nil {
				log.Fatalf("Can not create table %s.%s: %v", this.tableName, err)
				return err
			}
		} else {
			msg := fmt.Sprintf("Table %s does not exists. Please set table mode to create or create table manually",
				this.tableName.String())
			log.Fatal(msg)
			return errors.New(msg)
		}
		this.insertSchema = csvSchema.ToInsertSchema()
	}
	log.Infof("Insert schema is:\n%s\n", this.insertSchema.ToAsciiTable())
	return nil
}

func (this *CsvToDb) createInsertSchema(csvSchema, dbTableSchema common.Schema) common.InsertSchema {
	if this.Config.HasHeader {
		return common.CreateCsvToDbSchemaByName(csvSchema, dbTableSchema)
	} else {
		return common.CreateCsvToDbSchemaByIdx(csvSchema, dbTableSchema)
	}
}
func (this *CsvToDb) createReader() (*csv.Reader, io.Closer, int64, func() int64, error) {
	var reader *os.File
	var err error
	size := int64(0)
	if this.Config.FileName == "--" {
		reader = os.Stdin
	} else {
		reader, err = os.Open(this.Config.FileName)
		if err != nil {
			log.Fatalf("Can not open CSV file %s: %v", this.Config.FileName, err)
			return nil, nil, 0, return0, err
		}
		info, err := reader.Stat()
		if err != nil {
			log.Warnf("Can not get file stat %s: %v", this.Config.FileName, err)
		} else {
			size = info.Size()
		}
	}

	var encodedReader io.Reader
	if this.Config.Encoding == "UTF-8" {
		encodedReader = reader
	} else {
		encodedReader, err = charset.NewReader(reader, this.Config.Encoding)
		if err != nil {
			log.Fatalf("Can not decode file dfrom charset %s: %v", this.Config.Encoding, err)
			return nil, nil, 0, return0, err
		}
	}
	progressReader := progress.NewReader(encodedReader)
	fileReader := bufio.NewReader(progressReader)
	csvReader := csv.NewReader(fileReader)
	csvReader.Comma = ([]rune(this.Config.Delimiter))[0]
	return csvReader, reader, size, progressReader.N, nil
}

func return0() int64 {
	return int64(0)
}

func (this *CsvToDb) parseCsvSchema(line []string) common.Schema {
	if this.Config.HasHeader {
		return common.ParseSchema(line)
	} else {
		return common.NColsSchema(len(line))
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
