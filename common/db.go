package common

type DbTool interface {
	Exists(schema, table string) (bool, error)
	LoadSchema(schema, table string) (Schema, error)
	CreateTable(schema, table string, tabSchema Schema) error
	DeleteFromTable(schema, table string) error
	TruncateTable(schema, table string) error
	DropTable(schema, table string) error
	InsertQuery(schema, table string, tabSchema InsertSchema) (string, []string, error)
}
