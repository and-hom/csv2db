package inserter

import (
	"database/sql"
	"github.com/and-hom/csv2db/common"
	"github.com/Sirupsen/logrus"
)

type TxInserter struct {
	BasicInserter
	Tx *sql.Tx
}

func CreateTxInserter(db *sql.DB, dbTool common.DbTool, tableName common.TableName, insertSchema common.InsertSchema) (common.Inserter, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	bInsPtr, err := CreateBasicInserter(tx, dbTool, tableName, insertSchema)
	if err != nil {
		return nil, err
	}

	bIns := *(bInsPtr.(*BasicInserter))
	return &TxInserter{
		BasicInserter:bIns,
		Tx:tx,
	}, nil
}

func (this TxInserter) Close() error {
	err := this.BasicInserter.Close()
	if err != nil {
		logrus.Error("Can not insert: ", err)
		return this.Tx.Rollback()
	}
	return this.Tx.Commit()
}
