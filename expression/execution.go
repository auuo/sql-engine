package expression

import (
	"sql-engine/rows"
	"sql-engine/util/pointer"
	"strconv"
	"strings"
)

func (b *BinaryExpr) Eval(_ rows.Row) interface{} {
	return nil
}

func (a *And) Eval(row rows.Row) interface{} {
	v1 := castAsBool(a.Left.Eval(row))
	if v1 == nil {
		return (*bool)(nil)
	}
	if !*v1 {
		return pointer.Bool(false)
	}
	v2 := castAsBool(a.Right.Eval(row))
	if v2 == nil {
		return (*bool)(nil)
	}
	if !*v2 {
		return pointer.Bool(false)
	}
	return pointer.Bool(true)
}

func (o *Or) Eval(row rows.Row) interface{} {
	// 第一个如果为真返回真，否则返回第二个的值（无论是否是 nil）
	if v1 := castAsBool(o.Left.Eval(row)); v1 != nil && *v1 {
		return pointer.Bool(true)
	}
	return castAsBool(o.Right.Eval(row))
}

func (eq *EqualTo) Eval(row rows.Row) interface{} {
	return equal(eq.Left.Eval(row), eq.Right.Eval(row))
}

func (neq *NotEqualTo) Eval(row rows.Row) interface{} {
	if v := equal(neq.Left.Eval(row), neq.Right.Eval(row)); v == nil {
		return v
	} else {
		return pointer.Bool(!*v)
	}
}

func (in *In) Eval(row rows.Row) interface{} {
	value := in.Value.Eval(row)
	if pointer.IsNil(value) {
		return (*bool)(nil)
	}
	for _, expr := range in.List {
		try := expr.Eval(row)
		if pointer.IsNil(try) {
			continue
		}
		if r := equal(value, try); r != nil && *r {
			return r
		}
	}
	return pointer.Bool(false)
}

func (lt *LessThan) Eval(row rows.Row) interface{} {
	return upcastingCompare(lt.Left.Eval(row), lt.Right.Eval(row), func(v1 *string, v2 *string) *bool {
		return pointer.Bool(*v1 < *v2)
	}, func(v1 *float64, v2 *float64) *bool {
		return pointer.Bool(*v1 < *v2)
	})
}

func (le *LessThanOrEqual) Eval(row rows.Row) interface{} {
	return upcastingCompare(le.Left.Eval(row), le.Right.Eval(row), func(v1 *string, v2 *string) *bool {
		return pointer.Bool(*v1 <= *v2)
	}, func(v1 *float64, v2 *float64) *bool {
		return pointer.Bool(*v1 <= *v2)
	})
}

func (gt *GreaterThan) Eval(row rows.Row) interface{} {
	return upcastingCompare(gt.Left.Eval(row), gt.Right.Eval(row), func(v1 *string, v2 *string) *bool {
		return pointer.Bool(*v1 > *v2)
	}, func(v1 *float64, v2 *float64) *bool {
		return pointer.Bool(*v1 > *v2)
	})
}

func (ge *GreaterThanOrEqual) Eval(row rows.Row) interface{} {
	return upcastingCompare(ge.Left.Eval(row), ge.Right.Eval(row), func(v1 *string, v2 *string) *bool {
		return pointer.Bool(*v1 >= *v2)
	}, func(v1 *float64, v2 *float64) *bool {
		return pointer.Bool(*v1 >= *v2)
	})
}

func (a *Add) Eval(row rows.Row) interface{} {
	return upcastingCalculate(a.Left.Eval(row), a.Right.Eval(row), func(v1 *int64, v2 *int64) *int64 {
		return pointer.Int64(*v1 + *v2)
	}, func(v1 *float64, v2 *float64) *float64 {
		return pointer.Float64(*v1 + *v2)
	})
}

func (s *Subtract) Eval(row rows.Row) interface{} {
	return upcastingCalculate(s.Left.Eval(row), s.Right.Eval(row), func(v1 *int64, v2 *int64) *int64 {
		return pointer.Int64(*v1 - *v2)
	}, func(v1 *float64, v2 *float64) *float64 {
		return pointer.Float64(*v1 - *v2)
	})
}

func (m *Multiply) Eval(row rows.Row) interface{} {
	return upcastingCalculate(m.Left.Eval(row), m.Right.Eval(row), func(v1 *int64, v2 *int64) *int64 {
		return pointer.Int64(*v1 * *v2)
	}, func(v1 *float64, v2 *float64) *float64 {
		return pointer.Float64(*v1 * *v2)
	})
}

func (d *Divide) Eval(row rows.Row) interface{} {
	left := castAsFloat(d.Left.Eval(row))
	if left == nil {
		return nil
	}
	right := castAsFloat(d.Right.Eval(row))
	if right == nil {
		return nil
	}
	return pointer.Float64(*left / *right)
}

func (r *Remainder) Eval(row rows.Row) interface{} {
	return upcastingCalculate(r.Left.Eval(row), r.Right.Eval(row), func(v1 *int64, v2 *int64) *int64 {
		return pointer.Int64(*v1 % *v2)
	}, func(v1 *float64, v2 *float64) *float64 {
		// go 不支持 float 取模，强转为 int
		return pointer.Float64(*castAsFloat(*castAsInt(v1) % *castAsInt(v2)))
	})
}

func (i *If) Eval(row rows.Row) interface{} {
	r := castAsBool(i.Predicate.Eval(row))
	if r != nil && *r {
		return i.TrueValue.Eval(row)
	}
	return i.FalseValue.Eval(row)
}

func (cast *Cast) Eval(row rows.Row) interface{} {
	r := cast.Child.Eval(row)
	if pointer.IsNil(r) {
		return nullByType(cast.DataType)
	}
	switch cast.DataType {
	case rows.Int:
		return castAsInt(r)
	case rows.Float:
		return castAsFloat(r)
	case rows.String:
		return castAsString(r)
	case rows.Boolean:
		return castAsBool(r)
	}
	return nil
}

func (c *CaseWhen) Eval(row rows.Row) interface{} {
	for _, branch := range c.Branches {
		r := castAsBool(branch.One.Eval(row))
		if r != nil && *r {
			return branch.Two.Eval(row)
		}
	}
	if c.ElseValue != nil {
		return c.ElseValue.Eval(row)
	}
	return nil
}

func (a *Alias) Eval(row rows.Row) interface{} {
	return a.Child.Eval(row)
}

func (l *Literal) Eval(_ rows.Row) interface{} {
	if l.IsNull {
		return nil
	}
	switch l.Type {
	case rows.String:
		return pointer.String(l.Value.(string))
	case rows.Int:
		return pointer.Int64(l.Value.(int64))
	case rows.Boolean:
		return pointer.Bool(l.Value.(bool))
	case rows.Float:
		return pointer.Float64(l.Value.(float64))
	}
	return nil
}

func (a *Attribute) Eval(row rows.Row) interface{} {
	return row.IndexOf(a.idx)
}

func (isNull *IsNull) Eval(row rows.Row) interface{} {
	return pointer.Bool(pointer.IsNil(isNull.Child.Eval(row)))
}

func (s *Star) Eval(_ rows.Row) interface{} {
	// 应该由 project 来处理
	panic("you should not enter here")
}

func (l *Like) Eval(row rows.Row) interface{} {
	likeBody := castAsString(l.Right.Eval(row))
	if likeBody == nil {
		return nil
	}
	left := castAsString(l.Left.Eval(row))
	if left == nil {
		return nil
	}
	// contains
	if strings.HasPrefix(*likeBody, "%") && strings.HasSuffix(*likeBody, "%") {
		return pointer.Bool(strings.Contains(*left, string([]rune(*likeBody)[1:len(*likeBody)-1])))
	} else if strings.HasPrefix(*likeBody, "%") {
		return pointer.Bool(strings.HasSuffix(*left, string([]rune(*likeBody)[1:])))
	} else if strings.HasSuffix(*likeBody, "%") {
		return pointer.Bool(strings.HasPrefix(*left, string([]rune(*likeBody)[:len(*likeBody)-1])))
	}
	if strings.Contains(*likeBody, "%") {
		idx := strings.Index(*likeBody, "%")
		return pointer.Bool(strings.HasPrefix(*left, string([]rune(*likeBody)[:idx])) &&
			strings.HasSuffix(*left, string([]rune(*likeBody)[idx+1:])))
	}
	return pointer.Bool(*left == *likeBody)
}

func (n *Not) Eval(row rows.Row) interface{} {
	value := castAsBool(n.Child.Eval(row))
	if value == nil {
		return nil
	}
	return pointer.Bool(!*value)
}

func nullByType(t rows.DataType) interface{} {
	switch t {
	case rows.Boolean:
		return (*bool)(nil)
	case rows.Int:
		return (*int64)(nil)
	case rows.String:
		return (*string)(nil)
	case rows.Float:
		return (*float64)(nil)
	}
	return nil
}

func castAsInt(e interface{}) *int64 {
	if v, ok := e.(*int64); ok {
		return v
	} else if v, ok := e.(*string); ok {
		if parseInt, err := strconv.ParseInt(*v, 10, 64); err != nil {
			return (*int64)(nil)
		} else {
			return &parseInt
		}
	} else if v, ok := e.(*float64); ok {
		return pointer.Int64(int64(*v))
	} else if v, ok := e.(*bool); ok {
		if *v {
			return pointer.Int64(1)
		} else {
			return pointer.Int64(0)
		}
	}
	return (*int64)(nil)
}

func castAsFloat(e interface{}) *float64 {
	if v, ok := e.(*int64); ok {
		return pointer.Float64(float64(*v))
	} else if v, ok := e.(*string); ok {
		if result, err := strconv.ParseFloat(*v, 64); err != nil {
			return (*float64)(nil)
		} else {
			return &result
		}
	} else if v, ok := e.(*float64); ok {
		return v
	} else if v, ok := e.(*bool); ok {
		if *v {
			return pointer.Float64(1)
		} else {
			return pointer.Float64(0)
		}
	}
	return (*float64)(nil)
}

func castAsString(e interface{}) *string {
	if v, ok := e.(*int64); ok {
		return pointer.String(strconv.FormatInt(*v, 10))
	} else if v, ok := e.(*string); ok {
		return v
	} else if v, ok := e.(*float64); ok {
		return pointer.String(strconv.FormatFloat(*v, 'f', -1, 64))
	} else if v, ok := e.(*bool); ok {
		if *v {
			return pointer.String("true")
		} else {
			return pointer.String("false")
		}
	}
	return (*string)(nil)
}

func castAsBool(e interface{}) *bool {
	if v, ok := e.(*int64); ok {
		return pointer.Bool(*v != 0)
	} else if v, ok := e.(*string); ok {
		if *v == "true" {
			return pointer.Bool(true)
		} else if *v == "false" {
			return pointer.Bool(false)
		} else {
			return (*bool)(nil)
		}
	} else if v, ok := e.(*float64); ok {
		return pointer.Bool(*v != 0)
	} else if v, ok := e.(*bool); ok {
		return v
	}
	return (*bool)(nil)
}

// 向上转型计算, 如果都是 int 直接计算, 否则转为 float 后计算
func upcastingCalculate(left, right interface{},
	intFunc func(*int64, *int64) *int64,
	floatFunc func(*float64, *float64) *float64) interface{} {
	if pointer.IsNil(left) {
		return nil
	}
	if pointer.IsNil(right) {
		return nil
	}
	// 只要有一个不是 *int64, 都需要转换为 float 进行计算
	if v1, v2, ok := pointer.BothInt64(left, right); ok {
		return intFunc(v1, v2)
	}
	v1 := castAsFloat(left)
	v2 := castAsFloat(right)
	if v1 == nil || v2 == nil {
		return nil
	}
	return floatFunc(v1, v2)
}

func equal(v1 interface{}, v2 interface{}) *bool {
	if pointer.IsNil(v1) || pointer.IsNil(v2) {
		return (*bool)(nil)
	}
	// 都是 string 直接比较
	if left, right, ok := pointer.BothString(v1, v2); ok {
		return pointer.Bool(*left == *right)
	}
	// 都是 int 直接比较
	if left, right, ok := pointer.BothInt64(v1, v2); ok {
		return pointer.Bool(*left == *right)
	}
	left := castAsFloat(v1)
	right := castAsFloat(v2)
	if left == nil || right == nil {
		return (*bool)(nil)
	}
	return pointer.Bool(*left == *right)
}

func upcastingCompare(left, right interface{},
	stringFunc func(*string, *string) *bool,
	floatFunc func(*float64, *float64) *bool) *bool {
	if pointer.IsNil(left) || pointer.IsNil(right) {
		return (*bool)(nil)
	}
	if v1, v2, ok := pointer.BothString(left, right); ok {
		return stringFunc(v1, v2)
	}
	v1 := castAsFloat(left)
	v2 := castAsFloat(right)
	return floatFunc(v1, v2)
}
