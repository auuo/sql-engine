package source

import (
	"io/ioutil"
	"sql-engine/config"
	"sql-engine/expression"
	"sql-engine/rows"
	"sql-engine/util/pointer"
)

type fileSystemSource struct {
	path string
}

func newFilesystem(args []string, _ config.SQLConf) Source {
	return &fileSystemSource{path: args[0]}
}

func (f *fileSystemSource) GetSchema() []rows.StructField {
	names := []string{"name", "size", "modify_time", "is_dir"}
	types := []rows.DataType{rows.String, rows.Int, rows.Int, rows.Boolean}
	return buildSchema(names, types)
}

func (f *fileSystemSource) Execute([]expression.Expression) [][]interface{} {
	var result [][]interface{}
	dir, err := ioutil.ReadDir(f.path)
	if err != nil {
		panic(err)
	}
	for _, info := range dir {
		result = append(result, []interface{}{
			pointer.String(info.Name()),
			pointer.Int64(info.Size()),
			pointer.Int64(info.ModTime().Unix()),
			pointer.Bool(info.IsDir()),
		})
	}
	return result
}
