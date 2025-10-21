package common

import (
	"strconv"
	"reflect"
	"github.com/sirupsen/logrus"
)

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

