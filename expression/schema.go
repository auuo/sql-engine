package expression

import (
	"sql-engine/rows"
	"strings"
)

func (b *BinaryExpr) GetSchema(_ []rows.StructField) rows.StructField { return rows.StructField{} }

func (a *And) GetSchema(_ []rows.StructField) rows.StructField {
	return rows.StructField{DataType: rows.Boolean}
}

func (o *Or) GetSchema(_ []rows.StructField) rows.StructField {
	return rows.StructField{DataType: rows.Boolean}
}

func (eq *EqualTo) GetSchema(_ []rows.StructField) rows.StructField {
	return rows.StructField{DataType: rows.Boolean}
}

func (neq *NotEqualTo) GetSchema(_ []rows.StructField) rows.StructField {
	return rows.StructField{DataType: rows.Boolean}
}

func (in *In) GetSchema(_ []rows.StructField) rows.StructField {
	return rows.StructField{DataType: rows.Boolean}
}

func (lt *LessThan) GetSchema(_ []rows.StructField) rows.StructField {
	return rows.StructField{DataType: rows.Boolean}
}

func (le *LessThanOrEqual) GetSchema(_ []rows.StructField) rows.StructField {
	return rows.StructField{DataType: rows.Boolean}
}

func (gt *GreaterThan) GetSchema(_ []rows.StructField) rows.StructField {
	return rows.StructField{DataType: rows.Boolean}
}

func (ge *GreaterThanOrEqual) GetSchema(_ []rows.StructField) rows.StructField {
	return rows.StructField{DataType: rows.Boolean}
}

func (a *Add) GetSchema(option []rows.StructField) rows.StructField {
	left := a.Left.GetSchema(option)
	right := a.Right.GetSchema(option)
	t := rows.Int
	if left.DataType != rows.Int && right.DataType != rows.Int {
		t = rows.Float
	}
	return rows.StructField{DataType: t}
}

func (s *Subtract) GetSchema(option []rows.StructField) rows.StructField {
	left := s.Left.GetSchema(option)
	right := s.Right.GetSchema(option)
	t := rows.Int
	if left.DataType != rows.Int && right.DataType != rows.Int {
		t = rows.Float
	}
	return rows.StructField{DataType: t}
}

func (m *Multiply) GetSchema(option []rows.StructField) rows.StructField {
	left := m.Left.GetSchema(option)
	right := m.Right.GetSchema(option)
	t := rows.Int
	if left.DataType != rows.Int && right.DataType != rows.Int {
		t = rows.Float
	}
	return rows.StructField{DataType: t}
}

func (d *Divide) GetSchema(_ []rows.StructField) rows.StructField {
	return rows.StructField{DataType: rows.Float}
}

func (r *Remainder) GetSchema(option []rows.StructField) rows.StructField {
	left := r.Left.GetSchema(option)
	right := r.Right.GetSchema(option)
	t := rows.Int
	if left.DataType != rows.Int && right.DataType != rows.Int {
		t = rows.Float
	}
	return rows.StructField{DataType: t}
}

func (i *If) GetSchema(option []rows.StructField) rows.StructField {
	s := i.TrueValue.GetSchema(option)
	return rows.StructField{DataType: s.DataType}
}

func (cast *Cast) GetSchema(_ []rows.StructField) rows.StructField {
	return rows.StructField{DataType: cast.DataType}
}

func (c *CaseWhen) GetSchema(option []rows.StructField) rows.StructField {
	s := c.Branches[0].Two.GetSchema(option)
	return rows.StructField{DataType: s.DataType}
}

func (a *Alias) GetSchema(option []rows.StructField) rows.StructField {
	return rows.StructField{
		Name:     a.Name,
		DataType: a.Child.GetSchema(option).DataType,
	}
}

func (l *Literal) GetSchema(_ []rows.StructField) rows.StructField {
	return rows.StructField{DataType: l.Type}
}

func (a *Attribute) GetSchema(option []rows.StructField) rows.StructField {
	name := a.Name
	for i, field := range option {
		if field.Name == name {
			a.idx = i
			return rows.StructField{
				Name:     a.Name,
				DataType: field.DataType,
			}
		}
	}
	// name 是带了表名的，必须匹配上
	if strings.Contains(name, ".") {
		panic("can't find '" + name + "'")
	}
	// 尝试将 field 的表名去掉后匹配
	var matched []rows.StructField
	idx := -1
	for i, field := range option {
		if strings.Contains(field.Name, ".") {
			if strings.Split(field.Name, ".")[1] == name {
				matched = append(matched, field)
				if idx == -1 {
					idx = i
				}
			}
		}
	}
	if len(matched) == 0 {
		panic("can't find '" + name + "'")
	}
	if len(matched) > 1 {
		panic("field '" + name + "' is ambiguous")
	}
	a.idx = idx
	return rows.StructField{
		Name:     a.Name,
		DataType: matched[0].DataType,
	}
}

func (isNull *IsNull) GetSchema(_ []rows.StructField) rows.StructField {
	return rows.StructField{DataType: rows.Boolean}
}

func (s *Star) GetSchema(_ []rows.StructField) rows.StructField {
	// 应该由 project 实现
	return rows.StructField{}
}

func (l *Like) GetSchema(option []rows.StructField) rows.StructField {
	if l.Left.GetSchema(option).DataType != rows.String {
		panic("attr like xxx must be string")
	}
	if l.Right.GetSchema(option).DataType != rows.String {
		panic("like xxx must be string")
	}
	return rows.StructField{DataType: rows.Boolean}
}

func (n *Not) GetSchema(option []rows.StructField) rows.StructField {
	return n.Child.GetSchema(option)
}