package main

import (
	"github.com/and-hom/csv2db/common"
	log "github.com/Sirupsen/logrus"
)

type DbType string

const postgres DbType = "postgres"
const mysql DbType = "mysql"
const oracle DbType = "oracle"

const MODE_CREATE = "create"
const MODE_DELETE_ALL = "delete-all"
const MODE_TRUNCATE = "truncate"
const MODE_DROP_AND_CREATE = "drop-and-create"

var modes = []string{
	MODE_CREATE,
	MODE_DELETE_ALL,
	MODE_TRUNCATE,
	MODE_DROP_AND_CREATE,
}

type Config struct {
	DbType                DbType
	DbConnString          string

	Schema                string
	Table                 string
	CreateIfMissing       bool
	DropAndCreateIfExists bool
	DeletePrevious        bool
	TruncatePrevious      bool

	FileName              string
	HasHeader             bool
	Delimiter             string
	Encoding              string
}

func (this Config) String() string {
	return common.ObjectToJson(this)
}

func (this Config) SetMode(mode string) {
	this.CreateIfMissing = (mode == MODE_CREATE)
	this.DropAndCreateIfExists = (mode == MODE_DROP_AND_CREATE)
	this.DeletePrevious = (mode == MODE_DELETE_ALL)
	this.TruncatePrevious = (mode == MODE_TRUNCATE)
}

func (this Config) Validate() {
	if len(this.Delimiter) > 1 {
		log.Fatalf("CSV delimiter should be a single char: %s", this.Delimiter)
	} else if (len(this.Delimiter) == 0) {
		log.Fatalf("Should set CSV delimiter")
	}
}

