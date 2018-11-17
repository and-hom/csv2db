package common

type DbTool interface {
	Exists(schema, table string) (bool, error)
	LoadSchema(schema, table string, tabSchema *Schema) error
	CreateTable(schema, table string, tabSchema Schema) error
	InsertQuery(schema, table string, tabSchema InsertSchema) (string, []string, error)
}
