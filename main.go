package main

import (
	"fmt"
	"sql-engine/config"
	"sql-engine/parser"
)

func main() {
	sql := `select is_dir, sum(size) from '/Users/youbo/Downloads' group by is_dir`
	dataSet := parser.ExecuteSql(sql, config.SQLConf{})
	fmt.Println(dataSet.String())
}
