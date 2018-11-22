package main

import (
	"os"
	"gopkg.in/urfave/cli.v1"
	log "github.com/Sirupsen/logrus"
	"strings"
)

const DB_URL_FLAG = "url"
const TABLE_FLAG = "table, t"
const TABLE_MODE_FLAG = "table-mode, m"
const INPUT_FILE_FLAG = "input-file, i"
const HEADER_FLAG = "has-header, hh"
const DELIMITER_FLAG = "delimiter, d"
const ENCODING_FLAG = "encoding, e"
const STORE_PRESET_FLAG = "store-preset, s"
const PRESET_FLAG = "preset, p"

func main() {
	log.SetLevel(log.DebugLevel)

	app := cli.NewApp()
	app.Name = "csv2db"
	app.Usage = "Import your CSV to database as a table"
	app.Action = mainAction

	app.Flags = []cli.Flag{
		cli.StringFlag{Name:DB_URL_FLAG, Usage:`Database url by https://github.com/xo/dburl project. For example:
			mysql:		mysql://csv2db:csv2db@localhost:5432/csv2db
			postgres:	  postgres://csv2db:csv2db@localhost:5432/csv2db
		`},
		cli.StringFlag{Name:TABLE_FLAG, Usage:"Table name"},
		cli.StringFlag{Name:TABLE_MODE_FLAG, Usage:"Table mode flag. Available values are: " + strings.Join(modes, ", ")},
		cli.StringFlag{Name:INPUT_FILE_FLAG, Usage:"Input CSV file. Use -- to read from stdin"},
		cli.BoolFlag{Name:HEADER_FLAG, Usage:"True if first line is header"},
		cli.StringFlag{Name:ENCODING_FLAG, Usage:"Input file encoding", Value:"UTF-8"},
		cli.StringFlag{Name:DELIMITER_FLAG, Usage:"CSV cell delimiter", Value:","},
		cli.StringFlag{Name:PRESET_FLAG, Usage:"Use preset from configuration", Value:DEFAULT_PRESET},
		cli.StringFlag{Name:STORE_PRESET_FLAG, Usage:"Create new preset using current parameters"},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func mainAction(c *cli.Context) error {
	conf := LoadConfig(c)
	log.Infof("Run with config: \n%s", conf.String())
	return (&CsvToDb{Config:conf}).Perform()
}

func flagName(flagName string) string {
	s := strings.Split(flagName, ",")
	return strings.TrimSpace(s[0])
}