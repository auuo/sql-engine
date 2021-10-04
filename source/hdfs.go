package source

import (
	"bufio"
	"bytes"
	"os/exec"
	"regexp"
	"sql-engine/config"
	"sql-engine/expression"
	"sql-engine/rows"
	"sql-engine/util/pointer"
	"strings"
)

type hdfsSource struct {
	path string
	du   bool
	s    bool
}

func newHdfs(args []string, _ config.SQLConf) Source {
	params := buildParams(args)
	source := &hdfsSource{path: args[len(args)-1]}
	if params["-du"] {
		source.du = true
		if params["-s"] {
			source.s = true
		}
	}
	return source
}

func (h *hdfsSource) GetSchema() []rows.StructField {
	names := []string{"size", "name"}
	types := []rows.DataType{rows.Int, rows.String}
	if !h.du {
		names = []string{"owner", "size", "modify_date", "modify_time", "name"}
		types = []rows.DataType{rows.String, rows.Int, rows.String, rows.String, rows.String}
	}
	return buildSchema(names, types)
}

func (h *hdfsSource) Execute([]expression.Expression) [][]interface{} {
	var result [][]interface{}
	if h.du {
		var cmd *exec.Cmd
		if h.s {
			cmd = exec.Command("hadoop", "fs", "-du", "-s", h.path)
		} else {
			cmd = exec.Command("hadoop", "fs", "-du", h.path)
		}
		output, err := cmd.Output()
		if err != nil && strings.Contains(err.Error(), "No such file or directory") {
			return result
		}
		checkCmdError(err)
		scanner := bufio.NewScanner(bytes.NewReader(output))
		for scanner.Scan() {
			split := strings.Fields(scanner.Text())
			result = append(result, []interface{}{
				pointer.Int64From(split[0]),
				pointer.String(split[2]),
			})
		}
		return result
	}
	cmd := exec.Command("hadoop", "fs", "-ls", h.path)
	output, err := cmd.Output()
	if err != nil && strings.Contains(err.Error(), "No such file or directory") {
		return result
	}
	checkCmdError(err)
	regex := regexp.MustCompile("Found \\d+ items")
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		if regex.MatchString(scanner.Text()) {
			continue
		}
		split := strings.Fields(scanner.Text())
		result = append(result, []interface{}{
			pointer.String(split[2]),
			pointer.Int64From(split[4]),
			pointer.String(split[5]),
			pointer.String(split[6]),
			pointer.String(split[7]),
		})
	}
	return result
}
