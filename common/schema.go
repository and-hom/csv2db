package common

import (
	"reflect"
	"github.com/Sirupsen/logrus"
	"strconv"
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
	insertSchema := InsertSchema{Types:make(map[string]InsertColDef)}

	for name, colDef := range this.Types {
		valMapper := createValMapper(colDef.GoType)
		if colDef.Nullable {
			valMapper = NullableMapper{Source:valMapper}.Apply
		}

		insertSchema.Types[name] = InsertColDef{
			ValMapper:valMapper,
			ColDef:colDef,
		}
	}
	return insertSchema
}

func CreateCsvToDbSchema(csvSchema, dbSchema Schema) InsertSchema {
	insertSchema := InsertSchema{Types:make(map[string]InsertColDef)}
	for name, csvDef := range csvSchema.Types {
		dbDef, found := dbSchema.Types[name]
		if !found {
			logrus.Warnf("Can not find DB defenition for CSV column %s - use not null string type", name)
			continue
		}

		valMapper := createValMapper(dbDef.GoType)
		if dbDef.Nullable {
			valMapper = NullableMapper{Source:valMapper}.Apply
		}
		insertSchema.Types[name] = InsertColDef{
			ValMapper:valMapper,
			ColDef:ColDef{
				GoType: dbDef.GoType,
				Nullable:dbDef.Nullable,
				OrderIndex:csvDef.OrderIndex,
			},
		}
	}
	return insertSchema
}

type InsertSchema struct {
	Types map[string]InsertColDef
}

type InsertColDef struct {
	ColDef
	ValMapper ValMapper
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
