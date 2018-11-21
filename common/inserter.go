package common

import (
	"database/sql"
	"io"
	"log"
	"github.com/Sirupsen/logrus"
)

type Inserter interface {
	io.Closer
	Add(...string) error
}

type basicInserter struct {
	stmt         *sql.Stmt
	columnNames  []string
	insertSchema InsertSchema
}

func (this basicInserter) Add(args ...string) error {
	objArgs := prepareInsertArguments(this.insertSchema, this.columnNames, args)
	_, err := this.stmt.Exec(objArgs...)
	return err
}

func (this basicInserter) Close() error {
	return this.stmt.Close()
}

func CreateBasicInserter(db CanPrepareStatement, dbTool DbTool, tableName TableName, insertSchema InsertSchema) (Inserter, error) {
	query, columnNames, err := dbTool.InsertQuery(tableName, insertSchema)
	if err != nil {
		return nil, err
	}
	logrus.Debug("Insert query is ", query)

	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}

	return &basicInserter{
		stmt:stmt,
		columnNames:columnNames,
		insertSchema:insertSchema,
	}, nil
}

type txInserter struct {
	basicInserter
	tx *sql.Tx
}

func CreateTxInserter(db *sql.DB, dbTool DbTool, tableName TableName, insertSchema InsertSchema) (Inserter, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	bInsPtr, err := CreateBasicInserter(tx, dbTool, tableName, insertSchema)
	if err != nil {
		return nil, err
	}

	bIns := *(bInsPtr.(*basicInserter))
	return &txInserter{
		basicInserter:bIns,
		tx:tx,
	}, nil
}

func (this txInserter) Close() error {
	err := this.basicInserter.Close()
	if err != nil {
		logrus.Error("Can not insert: ", err)
		return this.tx.Rollback()
	}
	return this.tx.Commit()
}

func prepareInsertArguments(insertSchema InsertSchema, columnNames []string, line []string) []interface{} {
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

type CanPrepareStatement interface {
	Prepare(query string) (*sql.Stmt, error)
}