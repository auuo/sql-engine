package rows

import "sql-engine/util/pointer"

type DataType int

const (
	Int DataType = iota
	Float
	Boolean
	String
)

var DataTypeName = map[DataType]string{
	Int:     "bigint",
	Float:   "double",
	Boolean: "boolean",
	String:  "string",
}

type StructField struct {
	Name     string
	DataType DataType
}

type Dataset struct {
	Data   []Row
	Schema []StructField
}

func (d *Dataset) String() string {
	str := ""
	for _, row := range d.Data {
		for i, field := range d.Schema {
			str += field.Name + ": "
			str += pointer.PointerContent(row.IndexOf(i))
			if i != len(d.Schema) - 1 {
				str += ", "
			}
		}
		str += "\n"
	}
	return str
}