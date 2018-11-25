package main

import (
	"github.com/and-hom/csv2db/common"
	"github.com/lib/pq"
	"github.com/and-hom/csv2db/common/inserter"
	"database/sql"
	"github.com/Sirupsen/logrus"
)

func CreateCopyInserter(db *sql.DB, dbTool common.DbTool, tableName common.TableName, insertSchema common.InsertSchema) (common.Inserter, error) {
	columnNames := make([]string, 0, len(insertSchema.Types))
	for name, _ := range insertSchema.Types {
		columnNames = append(columnNames, name)
	}

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	query := pq.CopyIn(tableName.TablePlain, columnNames...)
	logrus.Debug("Query is " + query)
	stmt, err := tx.Prepare(query)
	if err != nil {
		return nil, err
	}

	return inserter.InitTxInserter(stmt, columnNames, insertSchema, tx)
}
