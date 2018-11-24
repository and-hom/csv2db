package inserter

import (
	"database/sql"
	"github.com/and-hom/csv2db/common"
	"github.com/Sirupsen/logrus"
)

type BasicInserter struct {
	stmt         *sql.Stmt
	columnNames  []string
	insertSchema common.InsertSchema
}

func (this BasicInserter) Add(args ...string) error {
	objArgs := common.PrepareInsertArguments(this.insertSchema, this.columnNames, args)
	_, err := this.stmt.Exec(objArgs...)
	return err
}

func (this BasicInserter) Close() error {
	return this.stmt.Close()
}

func InitBasicInserter(stmt *sql.Stmt, columnNames []string, insertSchema common.InsertSchema) (BasicInserter, error) {
	return BasicInserter{
		stmt:stmt,
		columnNames:columnNames,
		insertSchema:insertSchema,
	}, nil
}

func CreateBasicInserter(db common.CanPrepareStatement,
			dbTool common.DbTool,
			tableName common.TableName,
			insertSchema common.InsertSchema) (common.Inserter, error) {
	query, columnNames, err := dbTool.InsertQuery(tableName, insertSchema)
	if err != nil {
		return nil, err
	}
	logrus.Debug("Insert query is ", query)

	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}

	return &BasicInserter{
		stmt:stmt,
		columnNames:columnNames,
		insertSchema:insertSchema,
	}, nil
}