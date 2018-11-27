package common

type InsertColDef struct {
	ColDef
	ValMapper ValMapper `json:"-"`
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

func (this *InsertSchema) ToAsciiTable() string {
	colDefs := make(map[string]ColDef, len(this.types))
	for name, def := range this.types {
		colDefs[name] = def.ColDef
	}
	return schemaToAsciiTable(colDefs)
}
