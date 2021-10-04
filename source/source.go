package source

import (
	"os/exec"
	"sql-engine/config"
	"sql-engine/expression"
	"sql-engine/rows"
	"strings"
)

type Source interface {
	GetSchema() []rows.StructField
	Execute(pushDownPredicate []expression.Expression) [][]interface{}
}

var sourceFactory = map[string]func([]string, config.SQLConf) Source{
	"hdfs": newHdfs,
	"fs":   newFilesystem,
}

func NewSource(conf config.SQLConf, input string) Source {
	if strings.HasPrefix(input, "hdfs://") {
		return &hdfsSource{path: input}
	}
	inputs := strings.Fields(input)
	if len(inputs) == 1 {
		return &fileSystemSource{path: input}
	}
	if source, ok := sourceFactory[inputs[0]]; ok {
		return source(inputs[1:], conf)
	}
	panic("nonsupport data source: " + input)
}

func checkCmdError(err error) {
	if err == nil {
		return
	}
	if e, ok := err.(*exec.ExitError); ok {
		panic(err.Error() + ", " + string(e.Stderr))
	}
	panic(err)
}

func buildParams(args []string) map[string]bool {
	var params = make(map[string]bool)
	for i := 0; i < len(args)-1; i++ {
		params[args[i]] = true
	}
	return params
}

func buildSchema(names []string, types []rows.DataType) []rows.StructField {
	if len(names) != len(types) {
		panic("schema name and type length don't equal")
	}
	var res []rows.StructField
	for i, name := range names {
		res = append(res, rows.StructField{
			Name:     name,
			DataType: types[i],
		})
	}
	return res
}