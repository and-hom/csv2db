package inserter

import (
	"database/sql"
	"github.com/and-hom/csv2db/common"
	"github.com/Sirupsen/logrus"
)

type BasicInserter struct {
	Stmt         *sql.Stmt
	ColumnNames  []string
	InsertSchema common.InsertSchema
}

func (this BasicInserter) Add(args ...string) error {
	objArgs := common.PrepareInsertArguments(this.InsertSchema, this.ColumnNames, args)
	_, err := this.Stmt.Exec(objArgs...)
	return err
}

func (this BasicInserter) Close() error {
	return this.Stmt.Close()
}

func CreateBasicInserter(db common.CanPrepareStatement, dbTool common.DbTool, tableName common.TableName, insertSchema common.InsertSchema) (common.Inserter, error) {
	query, ColumnNames, err := dbTool.InsertQuery(tableName, insertSchema)
	if err != nil {
		return nil, err
	}
	logrus.Debug("Insert query is ", query)

	Stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}

	return &BasicInserter{
		Stmt:Stmt,
		ColumnNames:ColumnNames,
		InsertSchema:insertSchema,
	}, nil
}