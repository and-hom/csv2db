package common

import (
	"reflect"
	"github.com/Sirupsen/logrus"
	"strconv"
	"bytes"
	"encoding/json"
	"github.com/olekukonko/tablewriter"
)

type Schema struct {
	Types map[string]ColDef
}

type ColDef struct {
	GoType     reflect.Kind
	Nullable   bool
	OrderIndex int
}

func (this Schema) ToInsertSchema() InsertSchema {
	insertSchema := NewInsertSchema()
	for name, colDef := range this.Types {
		insertSchema.Add(name, colDef)
	}
	return insertSchema
}

/// Take type and nullable from DB cchema
func CreateCsvToDbSchemaByName(csvSchema, dbSchema Schema) InsertSchema {
	insertSchema := NewInsertSchema()
	for name, csvDef := range csvSchema.Types {
		dbDef, found := dbSchema.Types[name]
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
	for _, csvDef := range csvSchema.Types {
		name, dbDef, found := getByIdx(dbSchema, csvDef.OrderIndex)
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

func getByIdx(schema Schema, index int) (string, ColDef, bool) {
	for name, colDef := range schema.Types {
		if colDef.OrderIndex == index {
			return name, colDef, true
		}
	}
	return "", ColDef{}, false
}

func NewInsertSchema() InsertSchema {
	return InsertSchema{types:make(map[string]InsertColDef), OrderedDbColumns:make([]string, 0)}
}

type InsertSchema struct {
	types            map[string]InsertColDef
	OrderedDbColumns []string
}

func (this *InsertSchema) Get(name string) (InsertColDef, bool) {
	typeDef, ok := this.types[name]
	return typeDef, ok
}

func (this *InsertSchema) Len() int {
	return len(this.OrderedDbColumns)
}

func (this *InsertSchema) ForEach(func()) interface{} {
	return len(this.OrderedDbColumns)
}

func (this *InsertSchema) Add(name string, colDef ColDef) {
	valMapper := createValMapper(colDef.GoType)
	if colDef.Nullable {
		valMapper = NullableMapper{Source:valMapper}.Apply
	}

	this.types[name] = InsertColDef{
		ValMapper:valMapper,
		ColDef:colDef,
	}
	this.OrderedDbColumns = append(this.OrderedDbColumns, name)
}

type InsertColDef struct {
	ColDef
	ValMapper ValMapper `json:"-"`
}

func createValMapper(goType reflect.Kind) ValMapper {
	switch goType {
	case reflect.Int64:
		return Int64ValMapper
	case reflect.Int32:
		return Int32ValMapper
	case reflect.Int8:
		return Int8ValMapper
	case reflect.Float64:
		return Float64ValMapper
	case reflect.Float32:
		return Float32ValMapper
	case reflect.String:
		return StringValMapper
	case reflect.Bool:
		return BoolValMapper
	default:
		logrus.Fatalf("Unsupported go type %v - can not create value mapper", goType)
		return nil
	}
}

type ValMapper func(val string) (interface{}, error)

type NullableMapper struct {
	Source ValMapper
}

func (this NullableMapper) Apply(val string) (interface{}, error) {
	if val == "" {
		return nil, nil
	}
	return this.Source(val)
}

func StringValMapper(val string) (interface{}, error) {
	return val, nil
}

func Int64ValMapper(val string) (interface{}, error) {
	return strconv.ParseInt(val, 10, 64)
}

func Int32ValMapper(val string) (interface{}, error) {
	return strconv.ParseInt(val, 10, 32)
}

func Int8ValMapper(val string) (interface{}, error) {
	return strconv.ParseInt(val, 10, 8)
}

func Float64ValMapper(val string) (interface{}, error) {
	return strconv.ParseFloat(val, 64)
}

func Float32ValMapper(val string) (interface{}, error) {
	return strconv.ParseFloat(val, 32)
}

func BoolValMapper(val string) (interface{}, error) {
	return strconv.ParseBool(val)
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

func InsertSchemaToAsciiTable(schema InsertSchema) string {
	colDefs := make(map[string]ColDef, len(schema.types))
	for name, def := range schema.types {
		colDefs[name] = def.ColDef
	}
	return schemaToAsciiTable(colDefs)
}

func SchemaToAsciiTable(schema Schema) string {
	return schemaToAsciiTable(schema.Types)
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
