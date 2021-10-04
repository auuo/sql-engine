package expression

import (
	"sql-engine/rows"
)

type Expression interface {
	Eval(row rows.Row) interface{}
	Print() string
	GetSchema(option []rows.StructField) rows.StructField
	GetChildren() []*Expression // 使用指针，让外部有替换的能力
}

type BinaryOp interface {
	Expression
	SetLeft(expr Expression)
	SetRight(expr Expression)
}

type BinaryExpr struct {
	Left  Expression
	Right Expression
}

func (b *BinaryExpr) SetLeft(expr Expression) {
	b.Left = expr
}

func (b *BinaryExpr) SetRight(expr Expression) {
	b.Right = expr
}

func (b *BinaryExpr) GetChildren() []*Expression {
	return []*Expression{&b.Left, &b.Right}
}

type ExprTuple struct {
	One, Two Expression
}

type (
	And struct {
		BinaryExpr
	}

	Or struct {
		BinaryExpr
	}

	EqualTo struct {
		BinaryExpr
	}

	NotEqualTo struct {
		BinaryExpr
	}

	LessThan struct {
		BinaryExpr
	}

	LessThanOrEqual struct {
		BinaryExpr
	}

	GreaterThan struct {
		BinaryExpr
	}

	GreaterThanOrEqual struct {
		BinaryExpr
	}
)

type (
	Add struct {
		BinaryExpr
	}

	Subtract struct {
		BinaryExpr
	}

	Multiply struct {
		BinaryExpr
	}

	Divide struct {
		BinaryExpr
	}

	Remainder struct {
		BinaryExpr
	}
)

type (
	If struct {
		Predicate  Expression
		TrueValue  Expression
		FalseValue Expression
	}

	Cast struct {
		Child    Expression
		DataType rows.DataType
	}

	CaseWhen struct {
		Branches  []ExprTuple
		ElseValue Expression // 可为空
	}

	Alias struct {
		Child Expression
		Name  string
	}

	Literal struct {
		Value  interface{}
		Type   rows.DataType
		IsNull bool
	}

	Attribute struct {
		Name string
		idx  int // 经过编译后得到的 index
	}

	IsNull struct {
		Child Expression
	}

	Star struct {
		Table string
	}

	In struct {
		Value Expression
		List  []Expression
	}

	Like struct {
		Left  Expression
		Right Expression
	}

	Not struct {
		Child Expression
	}
)

func (l *Like) GetChildren() []*Expression {
	return []*Expression{&l.Left, &l.Right}
}

func (i *If) GetChildren() []*Expression {
	return []*Expression{&i.Predicate, &i.TrueValue, &i.FalseValue}
}

func (cast *Cast) GetChildren() []*Expression {
	return []*Expression{&cast.Child}
}

func (a *Alias) GetChildren() []*Expression {
	return []*Expression{&a.Child}
}

func (l *Literal) GetChildren() []*Expression {
	return []*Expression{}
}

func (a *Attribute) GetChildren() []*Expression {
	return []*Expression{}
}

func (c *CaseWhen) GetChildren() []*Expression {
	var result []*Expression
	for _, expr := range c.Branches {
		result = append(result, &expr.One, &expr.Two)
	}
	if c.ElseValue != nil {
		result = append(result, &c.ElseValue)
	}
	return result
}

func (isNull *IsNull) GetChildren() []*Expression {
	return []*Expression{&isNull.Child}
}

func (s *Star) GetChildren() []*Expression {
	return []*Expression{}
}

func (in *In) GetChildren() []*Expression {
	result := []*Expression{&in.Value}
	for i := range in.List {
		result = append(result, &in.List[i])
	}
	return result
}

func (n *Not) GetChildren() []*Expression {
	return []*Expression{&n.Child}
}

func Transform(expr Expression, fn func(expr Expression) Expression) Expression {
	children := expr.GetChildren()
	for _, child := range children {
		*child = Transform(*child, fn)
	}
	return fn(expr)
}

type ExprProxy struct {
	Expr        Expression
	RowGroup    []rows.Row
	GroupSchema []rows.StructField
	idx         int // 编译后生成，如果为 -1 表示该 expr 不是 group by 后的表达式，否则表示 group by 的下标
}

func (e *ExprProxy) Eval(row rows.Row) interface{} {
	// 如果被代理对象是 group 的 key, 则从 row 中直接取
	// 如果被代理对象是聚合类, 则将 RowGroup 传入计算
	// 否则让被代理对象自行求值
	if e.idx != -1 {
		return row.IndexOf(e.idx)
	}
	if agg, ok := e.Expr.(AggFunction); ok {
		agg.SetGroupData(e.RowGroup)
	}
	// 被代理类的子类还是一个代理, 将 group 数据继续传递
	for _, expr := range e.Expr.GetChildren() {
		if proxy, ok := (*expr).(*ExprProxy); ok {
			proxy.RowGroup = e.RowGroup
		}
	}
	return e.Expr.Eval(row)
}

func (e *ExprProxy) Print() (s string) {
	return e.Expr.Print()
}

func (e *ExprProxy) GetSchema(option []rows.StructField) rows.StructField {
	e.idx = -1
	for i, field := range e.GroupSchema {
		if field.Name == e.Print() {
			e.idx = i
			break
		}
	}
	return e.Expr.GetSchema(option)
}

func (e *ExprProxy) GetChildren() []*Expression {
	return e.Expr.GetChildren()
}
