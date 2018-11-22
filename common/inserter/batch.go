package inserter

import (
	"database/sql"
	"github.com/and-hom/csv2db/common"
	"github.com/Sirupsen/logrus"
)

type bufferedTxInserter struct {
	BasicInserter
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
	objArgs := common.PrepareInsertArguments(this.InsertSchema, this.ColumnNames, args)
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
	if this.Stmt == nil {
		return this.prepareStmtForce()
	} else if (this.counter != this.prevStmtRowCount) {
		if err := this.Stmt.Close(); err != nil {
			return err
		}
		return this.prepareStmtForce()
	}
	return nil
}

func (this *bufferedTxInserter) prepareStmtForce() error {
	logrus.Debugf("Preparing statement for %s args", len(this.buffer))
	query, _, err := this.dbTool.InsertQueryMultiple(this.tableName, this.InsertSchema, this.counter)
	if err != nil {
		return err
	}

	if this.Stmt, err = this.tx.Prepare(query); err != nil {
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

	_, err := this.Stmt.Exec(this.buffer...)
	this.counter = 0
	this.buffer = this.buffer[:0]
	return err
}

func (this *bufferedTxInserter) Close() error {
	if this.Stmt != nil {
		defer this.Stmt.Close()
	}
	if len(this.buffer) > 0 {
		return this.flush()
	}
	return nil
}

func CreateBufferedTxInserter(db *sql.DB, dbTool common.DbTool, tableName common.TableName, insertSchema common.InsertSchema, batchSize int) (common.Inserter, error) {
	_, columnNames, err := dbTool.InsertQuery(tableName, insertSchema)
	if err != nil {
		return nil, err
	}
	return &bufferedTxInserter{
		BasicInserter:BasicInserter{ColumnNames:columnNames, InsertSchema:insertSchema},
		db:db,
		dbTool:dbTool,
		tableName:tableName,
		buffer:make([]interface{}, 0),
		counter: 0,
		batchSize:batchSize,
	}, nil
}
