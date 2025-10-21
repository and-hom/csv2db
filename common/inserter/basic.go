package inserter

import (
	"database/sql"
	"github.com/and-hom/csv2db/common"
	"github.com/sirupsen/logrus"
)

type BasicInserter struct {
	stmt         *sql.Stmt
	insertSchema common.InsertSchema
}

func (this BasicInserter) Add(args ...string) error {
	objArgs := common.PrepareInsertArguments(this.insertSchema, args)
	_, err := this.stmt.Exec(objArgs...)
	return err
}

func (this BasicInserter) Close() error {
	return this.stmt.Close()
}

func InitBasicInserter(stmt *sql.Stmt, insertSchema common.InsertSchema) (BasicInserter, error) {
	return BasicInserter{
		stmt:stmt,
		insertSchema:insertSchema,
	}, nil
}

func CreateBasicInserter(db common.CanPrepareStatement,
			dbTool common.DbTool,
			tableName common.TableName,
			insertSchema common.InsertSchema) (common.Inserter, error) {
	query, err := dbTool.InsertQuery(tableName, insertSchema)
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
		insertSchema:insertSchema,
	}, nil
}