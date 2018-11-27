package common

import (
	"reflect"
	"github.com/Sirupsen/logrus"
	"strconv"
	"bytes"
	"encoding/json"
	"github.com/olekukonko/tablewriter"
	"fmt"
)

type ColDef struct {
	GoType     reflect.Kind
	Nullable   bool
	OrderIndex int
}

func NewSchema() Schema {
	return Schema{types:make(map[string]ColDef), OrderedDbColumns:make([]string, 0)}
}

type Schema struct {
	types            map[string]ColDef
	OrderedDbColumns []string
}

func (this *Schema) Add(name string, colDef ColDef) {
	this.types[name] = colDef
	this.OrderedDbColumns = append(this.OrderedDbColumns, name)
}

func (this *Schema) Get(name string) (ColDef, bool) {
	typeDef, ok := this.types[name]
	return typeDef, ok
}

func (this *Schema)GetByIdx(index int) (string, ColDef, bool) {
	if index >= len(this.OrderedDbColumns) {
		return "", ColDef{}, false
	}
	name := this.OrderedDbColumns[index]
	return name, this.types[name], true
}

func (this *Schema) Len() int {
	return len(this.OrderedDbColumns)
}

func (this *Schema) ToInsertSchema() InsertSchema {
	insertSchema := NewInsertSchema()
	for _, name := range this.OrderedDbColumns {
		insertSchema.Add(name, this.types[name])
	}
	return insertSchema
}

func (this *Schema) ToJson() string {
	return ObjectToJson(this, true)
}

func (this *Schema)ToAsciiTable() string {
	return schemaToAsciiTable(this.types)
}

/// Take type and nullable from DB cchema
func CreateCsvToDbSchemaByName(csvSchema, dbSchema Schema) InsertSchema {
	insertSchema := NewInsertSchema()
	for name, csvDef := range csvSchema.types {
		dbDef, found := dbSchema.types[name]
		if !found {
			logrus.Warnf("Can not find DB defenition for CSV column %s - use not null string type", name)
			continue
		}

		insertSchema.Add(name, ColDef{
			GoType: dbDef.GoType,
			Nullable:dbDef.Nullable,
			OrderIndex:csvDef.OrderIndex,
		})
	}
	return insertSchema
}

func CreateCsvToDbSchemaByIdx(csvSchema, dbSchema Schema) InsertSchema {
	insertSchema := NewInsertSchema()
	for _, csvDef := range csvSchema.types {
		name, dbDef, found := dbSchema.GetByIdx(csvDef.OrderIndex)
		if !found {
			logrus.Warnf("Can not find DB defenition for CSV column #%d - use not null string type", csvDef.OrderIndex)
			continue
		}

		valMapper := createValMapper(dbDef.GoType)
		if dbDef.Nullable {
			valMapper = NullableMapper{Source:valMapper}.Apply
		}
		insertSchema.Add(name, ColDef{
			GoType: dbDef.GoType,
			Nullable:dbDef.Nullable,
			OrderIndex:csvDef.OrderIndex,
		})
	}
	return insertSchema
}

func NewInsertSchema() InsertSchema {
	return InsertSchema{types:make(map[string]InsertColDef), OrderedDbColumns:make([]string, 0)}
}

func ObjectToJson(object interface{}, pretty bool) string {
	buf := bytes.Buffer{}
	f := json.NewEncoder(&buf)
	if pretty {
		f.SetIndent("", "    ")
	}
	err := f.Encode(object)
	if err != nil {
		logrus.Fatalf("Can not convert config to string: %v", err)
	}
	return buf.String()
}

func schemaToAsciiTable(cols map[string]ColDef) string {
	data := make([][]string, len(cols))
	for name, def := range cols {
		data[def.OrderIndex] = []string{
			name, strconv.FormatInt(int64(def.OrderIndex), 10), def.GoType.String(), strconv.FormatBool(def.Nullable),
		}
	}

	buf := bytes.Buffer{}
	table := tablewriter.NewWriter(&buf)
	table.SetHeader([]string{"Name", "Order index", "Go type", "Nullable"})
	table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
	table.SetCenterSeparator("|")
	table.AppendBulk(data)
	table.SetBorders(tablewriter.Border{Bottom:true, Left:true, Right:true, Top:false})
	table.Render()

	return buf.String()
}

func ParseSchema(header []string) Schema {
	schema := Schema{types:make(map[string]ColDef), OrderedDbColumns:make([]string, len(header))}
	for i, name := range header {
		schema.types[name] = ColDef{
			GoType:reflect.String,
			Nullable:false,
			OrderIndex:i,
		}
		schema.OrderedDbColumns[i] = name
	}
	return schema
}

func NColsSchema(colsCount int) Schema {
	schema := Schema{types:make(map[string]ColDef), OrderedDbColumns:make([]string, colsCount)}
	for i := 0; i < colsCount; i++ {
		name := fmt.Sprintf("col%d", i)
		schema.types[name] = ColDef{
			GoType:reflect.String,
			Nullable:false,
			OrderIndex:i,
		}
		schema.OrderedDbColumns[i] = name
	}
	return schema
}
