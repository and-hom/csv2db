package inserter

import (
	"database/sql"
	"github.com/and-hom/csv2db/common"
	"github.com/sirupsen/logrus"
)

type bufferedTxInserter struct {
	stmt             *sql.Stmt
	insertSchema     common.InsertSchema
	tx               *sql.Tx
	dbTool           common.DbTool
	tableName        common.TableName
	db               *sql.DB
	buffer           []interface{}
	counter          int
	prevStmtRowCount int
	batchSize        int
}

func (this *bufferedTxInserter) Add(args ...string) error {
	objArgs := common.PrepareInsertArguments(this.insertSchema, args)
	this.buffer = append(this.buffer, objArgs...)
	this.counter += 1
	if this.counter > this.batchSize {
		return this.flush()
	}
	return nil
}

func (this *bufferedTxInserter) initTx() error {
	var err error = nil
	if this.tx == nil {
		this.tx, err = this.db.Begin()
	}
	return err
}

func (this *bufferedTxInserter) prepareStmt() error {
	if this.stmt == nil {
		return this.prepareStmtForce()
	} else if (this.counter != this.prevStmtRowCount) {
		if err := this.stmt.Close(); err != nil {
			return err
		}
		return this.prepareStmtForce()
	}
	return nil
}

func (this *bufferedTxInserter) prepareStmtForce() error {
	logrus.Debugf("Preparing statement for %d args", len(this.buffer))
	query, err := this.dbTool.InsertQueryMultiple(this.tableName, this.insertSchema, this.counter)
	if err != nil {
		return err
	}
	logrus.Debug("Insert query is: ", query)

	if this.stmt, err = this.tx.Prepare(query); err != nil {
		return err
	}
	this.prevStmtRowCount = this.counter
	return nil
}

func (this *bufferedTxInserter) flush() error {
	if err := this.initTx(); err != nil {
		return err
	}
	if err := this.prepareStmt(); err != nil {
		return err
	}

	_, err := this.stmt.Exec(this.buffer...)
	this.counter = 0
	this.buffer = this.buffer[:0]
	return err
}

func (this *bufferedTxInserter) Close() error {
	var err error

	if len(this.buffer) > 0 {
		if err = this.flush(); err != nil {
			return this.closeTx(err)
		}
	}

	if this.stmt != nil {
		if err = this.stmt.Close(); err != nil {
			return this.closeTx(err)
		}
	}

	if this.tx != nil {
		return this.tx.Commit()
	}

	return nil
}

func (this *bufferedTxInserter) closeTx(err error) error {
	if err != nil {
		if this.tx != nil {
			this.tx.Rollback()
		}
		return err
	}
	return nil
}

func CreateBufferedTxInserter(db *sql.DB, dbTool common.DbTool, tableName common.TableName, insertSchema common.InsertSchema, batchSize int) (common.Inserter, error) {
	return &bufferedTxInserter{
		insertSchema:insertSchema,
		db:db,
		dbTool:dbTool,
		tableName:tableName,
		buffer:make([]interface{}, 0),
		counter: 0,
		batchSize:batchSize,
	}, nil
}
