package expression

import (
	"fmt"
	"reflect"
	"regexp"
	"sql-engine/rows"
	"sql-engine/util/pointer"
	"strings"
)

var FuncMap = map[string]Function{
	"count":          &Count{},
	"concat":         &Concat{},
	"min":            &Min{},
	"max":            &Max{},
	"sum":            &Sum{},
	"length":         &Length{},
	"substr":         &SubStr{},
	"regexp_extract": &RegexpExtract{},
}

func NewFuncByName(name string, args []Expression) Expression {
	ptrType := reflect.TypeOf(FuncMap[name])
	ptrValue := reflect.New(ptrType.Elem())
	ptrValue.Elem().FieldByName("Args").Set(reflect.ValueOf(args))
	return ptrValue.Interface().(Expression)
}

type Function Expression

type AggFunction interface {
	Expression
	SetGroupData(group []rows.Row)
}

type baseAgg struct {
	RowGroup []rows.Row
}

func (b *baseAgg) SetGroupData(group []rows.Row) {
	b.RowGroup = group
}

type Count struct {
	baseAgg
	Args []Expression
}

func (c *Count) Eval(_ rows.Row) interface{} {
	arg := c.Args[0]
	if l, ok := arg.(*Literal); ok {
		if l.IsNull {
			return pointer.Int64(0)
		}
		return pointer.Int64(int64(len(c.RowGroup)))
	}
	var count int64 = 0
	for _, row := range c.baseAgg.RowGroup {
		if !pointer.IsNil(arg.Eval(row)) {
			count += 1
		}
	}
	return pointer.Int64(count)
}

func (c *Count) Print() (s string) {
	return fmt.Sprintf("count(%s)", c.Args[0].Print())
}

func (c *Count) GetSchema(option []rows.StructField) rows.StructField {
	if len(c.Args) != 1 {
		panic("just support one param in count")
	}
	c.Args[0].GetSchema(option)
	return rows.StructField{DataType: rows.Int}
}

func (c *Count) GetChildren() []*Expression {
	return []*Expression{&c.Args[0]}
}

type Concat struct {
	Args []Expression
}

func (c *Concat) Eval(row rows.Row) interface{} {
	sb := strings.Builder{}
	for _, arg := range c.Args {
		v := arg.Eval(row)
		if pointer.IsNil(v) {
			return (*string)(nil)
		}
		sb.WriteString(*castAsString(v))
	}
	return pointer.String(sb.String())
}

func (c *Concat) Print() (s string) {
	sb := strings.Builder{}
	sb.WriteString("concat(")
	for i, arg := range c.Args {
		sb.WriteString(arg.Print())
		if i != len(c.Args)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString(")")
	return sb.String()
}

func (c *Concat) GetSchema(_ []rows.StructField) rows.StructField {
	return rows.StructField{DataType: rows.String}
}

func (c *Concat) GetChildren() []*Expression {
	var result []*Expression
	for i := range c.Args {
		result = append(result, &c.Args[i])
	}
	return result
}

type Min struct {
	baseAgg
	Args []Expression
}

func (m *Min) Eval(_ rows.Row) interface{} {
	var minValue interface{}
	for _, r := range m.RowGroup {
		newValue := m.Args[0].Eval(r)
		// 记录的值为 nil
		if pointer.IsNil(minValue) {
			minValue = newValue
			continue
		}
		if !pointer.IsNil(newValue) {
			if v1, v2, ok := pointer.BothString(newValue, minValue); ok {
				if *v1 < *v2 {
					minValue = newValue
				}
			} else if v1, v2, ok := pointer.BothInt64(newValue, minValue); ok {
				if *v1 < *v2 {
					minValue = newValue
				}
			} else if v1, v2, ok := pointer.BothFloat64(newValue, minValue); ok {
				if *v1 < *v2 {
					minValue = newValue
				}
			} else if v1, v2, ok := pointer.BothBool(newValue, minValue); ok {
				if !*v1 && *v2 {
					minValue = newValue
				}
			}
		}
	}
	return minValue
}

func (m *Min) Print() (s string) {
	return fmt.Sprintf("min(%s)", m.Args[0].Print())
}

func (m *Min) GetSchema(option []rows.StructField) rows.StructField {
	if len(m.Args) != 1 {
		panic("just support one param in min")
	}
	m.Args[0].GetSchema(option)
	return rows.StructField{DataType: m.Args[0].GetSchema(option).DataType}
}

func (m *Min) GetChildren() []*Expression {
	return []*Expression{&m.Args[0]}
}

type Max struct {
	baseAgg
	Args []Expression
}

func (m *Max) Eval(_ rows.Row) interface{} {
	var maxValue interface{}
	for _, r := range m.RowGroup {
		newValue := m.Args[0].Eval(r)
		// 记录的值为 nil
		if pointer.IsNil(maxValue) {
			maxValue = newValue
			continue
		}
		if !pointer.IsNil(newValue) {
			if v1, v2, ok := pointer.BothString(newValue, maxValue); ok {
				if *v1 > *v2 {
					maxValue = newValue
				}
			} else if v1, v2, ok := pointer.BothInt64(newValue, maxValue); ok {
				if *v1 > *v2 {
					maxValue = newValue
				}
			} else if v1, v2, ok := pointer.BothFloat64(newValue, maxValue); ok {
				if *v1 > *v2 {
					maxValue = newValue
				}
			} else if v1, v2, ok := pointer.BothBool(newValue, maxValue); ok {
				if *v1 && !*v2 {
					maxValue = newValue
				}
			}
		}
	}
	return maxValue
}

func (m *Max) Print() (s string) {
	return fmt.Sprintf("max(%s)", m.Args[0].Print())
}

func (m *Max) GetSchema(option []rows.StructField) rows.StructField {
	if len(m.Args) != 1 {
		panic("just support one param in max")
	}
	m.Args[0].GetSchema(option)
	return rows.StructField{DataType: m.Args[0].GetSchema(option).DataType}
}

func (m *Max) GetChildren() []*Expression {
	return []*Expression{&m.Args[0]}
}

type Sum struct {
	baseAgg
	Args []Expression
}

func (s *Sum) Eval(_ rows.Row) interface{} {
	var sum float64 = 0
	var dataType *rows.DataType
	for i, row := range s.RowGroup {
		r := s.Args[0].Eval(row)
		// 第一条计算下数据类型
		if i == 0 {
			if _, ok := r.(*int64); ok {
				t := rows.Int
				dataType = &t
			} else {
				t := rows.Float
				dataType = &t
			}
		}
		f := castAsFloat(r)
		if f != nil {
			sum += *f
		}
	}
	if dataType == nil {
		return nil
	}
	if *dataType == rows.Int {
		return pointer.Int64(int64(sum))
	}
	return pointer.Float64(sum)
}

func (s *Sum) Print() string {
	return fmt.Sprintf("Sum(%s)", s.Args[0].Print())
}

func (s *Sum) GetSchema(option []rows.StructField) rows.StructField {
	if len(s.Args) != 1 {
		panic("just support one param in sum")
	}
	schema := s.Args[0].GetSchema(option)
	if schema.DataType != rows.Int && schema.DataType != rows.Float {
		panic("sum only support 'bigint' and 'double'")
	}
	return rows.StructField{
		DataType: schema.DataType,
	}
}

func (s *Sum) GetChildren() []*Expression {
	return []*Expression{&s.Args[0]}
}

type Length struct {
	Args []Expression
}

func (l *Length) Eval(row rows.Row) interface{} {
	result := castAsString(l.Args[0].Eval(row))
	if result == nil {
		return nil
	}
	return pointer.Int64(int64(len(*result)))
}

func (l *Length) Print() (s string) {
	return fmt.Sprintf("length(%s)", l.Args[0].Print())
}

func (l *Length) GetSchema(option []rows.StructField) rows.StructField {
	if len(l.Args) != 1 {
		panic("just support one param in length")
	}
	l.Args[0].GetSchema(option)
	return rows.StructField{DataType: rows.Int}
}

func (l *Length) GetChildren() []*Expression {
	return []*Expression{&l.Args[0]}
}

type SubStr struct {
	Args []Expression
}

func (s *SubStr) Eval(row rows.Row) interface{} {
	result := castAsString(s.Args[0].Eval(row))
	if result == nil {
		return nil
	}
	startPos := castAsInt(s.Args[1].Eval(row))
	if startPos == nil || *startPos < 1 || int(*startPos) > len(*result) {
		return nil
	}
	if len(s.Args) == 2 {
		return pointer.String(string([]byte(*result)[*startPos-1:]))
	}
	length := castAsInt(s.Args[2].Eval(row))
	if length == nil || *length < 1 || int(*length) > len(*result)-int(*startPos)+1 {
		return nil
	}
	return pointer.String(string([]byte(*result)[*startPos-1 : *startPos-1+*length]))
}

func (s *SubStr) Print() string {
	sb := strings.Builder{}
	sb.WriteString("substr(")
	for i, arg := range s.Args {
		sb.WriteString(arg.Print())
		if i != len(s.Args)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString(")")
	return sb.String()
}

func (s *SubStr) GetSchema(_ []rows.StructField) rows.StructField {
	if len(s.Args) == 0 || len(s.Args) > 3 {
		panic("substr need two or three params, substr(str, startPos, len)")
	}
	return rows.StructField{DataType: rows.String}
}

func (s *SubStr) GetChildren() []*Expression {
	var result []*Expression
	for i := range s.Args {
		result = append(result, &s.Args[i])
	}
	return result
}

type RegexpExtract struct {
	Args        []Expression
	regxMap		map[string]*regexp.Regexp
}

func (r *RegexpExtract) Eval(row rows.Row) interface{} {
	// Compile each regex once.
	pattern := castAsString(r.Args[1].Eval(row))
	if pattern == nil {
		return nil
	}
	if r.regxMap == nil { r.regxMap = make(map[string]*regexp.Regexp) }
	if _, ok := r.regxMap[*pattern]; !ok {
		if p, err := regexp.Compile(*pattern); err != nil {
			r.regxMap[*pattern] = nil
		} else {
			r.regxMap[*pattern] = p
		}
	}
	p := r.regxMap[*pattern]
	if p == nil {
		return nil
	}
	subject := castAsString(r.Args[0].Eval(row))
	index := castAsInt(r.Args[2].Eval(row))
	if subject == nil || index == nil {
		return nil
	}
	submatch := p.FindStringSubmatch(*subject)
	if *index < int64(len(submatch)) {
		return &submatch[*index]
	}
	return nil
}

func (r *RegexpExtract) Print() string {
	return fmt.Sprintf("regexp_extract(%s, %s, %s)", r.Args[0].Print(), r.Args[1].Print(), r.Args[2].Print())
}

func (r *RegexpExtract) GetSchema(option []rows.StructField) rows.StructField {
	if len(r.Args) != 3 {
		panic("regexp_extract(subject, pattern, index) needs three args")
	}
	subject := r.Args[0].GetSchema(option)
	pattern := r.Args[1].GetSchema(option)
	index := r.Args[2].GetSchema(option)
	if subject.DataType != rows.String || pattern.DataType != rows.String || index.DataType != rows.Int {
		panic("data type error, need (string, string, int)")
	}
	return rows.StructField{DataType: rows.String}
}

func (r *RegexpExtract) GetChildren() []*Expression {
	var res []*Expression
	for i := range r.Args {
		res = append(res, &r.Args[i])
	}
	return res
}
