package plan

import (
	"fmt"
	"sql-engine/expression"
	"sql-engine/rows"
	"strconv"
	"strings"
)

func (p *Project) GetSchema() []rows.StructField {
	if p.schemaCache != nil {
		return p.schemaCache
	}
	options := p.Child.GetSchema()
	var result []rows.StructField
	genField := genFields()
	for _, expr := range p.ProjectList {
		if star, ok := expr.(*expression.Star); ok {
			for _, field := range options {
				if star.Table == "" || strings.HasPrefix(field.Name, star.Table+".") {
					result = append(result, genField(field, ""))
				}
			}
		} else {
			field := expr.GetSchema(options)
			result = append(result, genField(field, expr.Print()))
		}
	}
	p.schemaCache = result
	return result
}

func (f *Filter) GetSchema() []rows.StructField {
	schema := f.Child.GetSchema()
	conditionSchema := f.Condition.GetSchema(schema)
	if conditionSchema.DataType != rows.Boolean {
		panic("filter condition must be boolean")
	}
	return schema
}

func (r *Relation) GetSchema() []rows.StructField {
	return r.DataSource.GetSchema()
}

func (u *Union) GetSchema() []rows.StructField {
	first := u.Children[0].GetSchema()
	for i := 1; i < len(u.Children); i++ {
		other := u.Children[i].GetSchema()
		if len(first) != len(other) {
			panic("union all length is not match")
		}
		// 比较每一个字段类型
		for i, field := range first {
			otherField := other[i]
			if field.DataType != otherField.DataType {
				panic(fmt.Sprintf(
					"union all field data type is not match, %s and %s",
					rows.DataTypeName[field.DataType],
					rows.DataTypeName[otherField.DataType]))
			}
		}
	}
	return first
}

func (a *Aggregate) GetSchema() []rows.StructField {
	if a.schemaCache != nil {
		return a.schemaCache
	}
	options := a.Child.GetSchema()
	var result []rows.StructField
	genField := genFields()
	for _, expr := range a.AggregateExprs {
		field := expr.GetSchema(options)
		result = append(result, genField(field, expr.Print()))
	}
	// 检查 group 语句的 select 中是否有非分组字段
	groupKeyOptions := a.GetGroupSchema()
	names := make(map[string]bool)
	for _, option := range groupKeyOptions {
		names[option.Name] = true
	}
	var checkField func(expression.Expression)
	checkField = func(expr expression.Expression) {
		// 命中分组字段，不继续检查，直接返回
		if names[expr.Print()] {
			return
		}
		// 聚合函数不需要检查, attribute 不应该存在
		switch t := expr.(type) {
		case expression.AggFunction:
			return
		case *expression.Attribute:
			panic(fmt.Sprintf("can't found '%s' in group by", t.Name))
		}
		for _, e := range expr.GetChildren() {
			checkField(*e)
		}
	}
	for _, expr := range a.AggregateExprs {
		checkField(expr)
	}
	a.schemaCache = result
	return result
}

func (a *Aggregate) GetGroupSchema() []rows.StructField {
	var result []rows.StructField
	options := a.Child.GetSchema()
	for _, expr := range a.GroupExprs {
		s := expr.GetSchema(options)
		//if s.Name == "" {
		//	s.Name = expr.Print()
		//}
		// 直接使用 print 的值
		s.Name = expr.Print()
		result = append(result, s)
	}
	return result
}

// 对于 table.field 的字段更改为 field.
// 重名字段时增加后缀 '_$num'
func (s *Subquery) GetSchema() []rows.StructField {
	if s.schemaCache != nil {
		return s.schemaCache
	}
	var result []rows.StructField
	genField := genFields()
	for _, field := range s.Child.GetSchema() {
		newField := genField(field, "")
		newField.Name = s.Alias + "." + newField.Name
		result = append(result, newField)
	}
	s.schemaCache = result
	return result
}

func (s *Sort) GetSchema() []rows.StructField {
	return s.Child.GetSchema()
}

func (l *Limit) GetSchema() []rows.StructField {
	return l.Child.GetSchema()
}

// 用于生成新字段，规范化字段名
// 自动防止字段名重复
func genFields() func(field rows.StructField, bakName string) rows.StructField {
	nameSet := make(map[string]bool)
	i := 1
	return func(field rows.StructField, bakName string) rows.StructField {
		name := field.Name
		if strings.Contains(name, ".") {
			name = strings.Split(name, ".")[1]
		}
		if name == "" {
			name = bakName
		}
		if _, ok := nameSet[name]; ok {
			name += "_$" + strconv.Itoa(i)
			i += 1
		}
		nameSet[name] = true
		return rows.StructField{Name: name, DataType: field.DataType}
	}
}
