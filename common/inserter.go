package common

import (
	"database/sql"
	"io"
	"log"
)

type Inserter interface {
	io.Closer
	Add(...string) error
}

func PrepareInsertArguments(insertSchema InsertSchema, columnNames []string, line []string) []interface{} {
	result := make([]interface{}, 0, len(insertSchema.Types))
	for _, name := range columnNames {
		typeDef, found := insertSchema.Types[name]
		if !found {
			log.Fatalf("Can not find column %s in insert schema: %v", name, insertSchema)
		}
		valStr := line[typeDef.OrderIndex]
		value, err := typeDef.ValMapper(valStr)
		if err != nil {
			log.Fatalf("Can not convert value %s at column %d to %v(nullable=%v)",
				valStr, typeDef.OrderIndex, typeDef.GoType, typeDef.Nullable)
		}
		result = append(result, value)
	}
	return result
}

type CanPrepareStatement interface {
	Prepare(query string) (*sql.Stmt, error)
}