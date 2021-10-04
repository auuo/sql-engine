package expression

import (
	"fmt"
	"sql-engine/rows"
)

func (b *BinaryExpr) Print() string { return "" }

func (a *And) Print() string {
	return fmt.Sprintf("(%s and %s)", a.Left.Print(), a.Right.Print())
}

func (o *Or) Print() string {
	return fmt.Sprintf("(%s or %s)", o.Left.Print(), o.Right.Print())
}

func (eq *EqualTo) Print() string {
	return fmt.Sprintf("%s = %s", eq.Left.Print(), eq.Right.Print())
}

func (neq *NotEqualTo) Print() string {
	return fmt.Sprintf("%s != %s", neq.Left.Print(), neq.Right.Print())
}

func (in *In) Print() string {
	s := fmt.Sprintf("In(%s, [", in.Value.Print())
	for i, e := range in.List {
		s += e.Print()
		if i != len(in.List) - 1 {
			s += ", "
		}
	}
	return s + "])"
}

func (lt *LessThan) Print() string {
	return fmt.Sprintf("%s < %s", lt.Left.Print(), lt.Right.Print())
}

func (le *LessThanOrEqual) Print() string {
	return fmt.Sprintf("%s <= %s", le.Left.Print(), le.Right.Print())
}

func (gt *GreaterThan) Print() string {
	return fmt.Sprintf("%s > %s", gt.Left.Print(), gt.Right.Print())
}

func (ge *GreaterThanOrEqual) Print() string {
	return fmt.Sprintf("%s >= %s", ge.Left.Print(), ge.Right.Print())
}

func (a *Add) Print() string {
	return fmt.Sprintf("(%s + %s)", a.Left.Print(), a.Right.Print())
}

func (s *Subtract) Print() string {
	return fmt.Sprintf("(%s - %s)", s.Left.Print(), s.Right.Print())
}

func (m *Multiply) Print() string {
	return fmt.Sprintf("(%s * %s)", m.Left.Print(), m.Right.Print())
}

func (d *Divide) Print() string {
	return fmt.Sprintf("(%s / %s)", d.Left.Print(), d.Right.Print())
}

func (r *Remainder) Print() string {
	return fmt.Sprintf("(%s % %s)", r.Left.Print(), r.Right.Print())
}

func (i *If) Print() string {
	return fmt.Sprintf("if(%s, %s, %s)", i.Predicate.Print(), i.TrueValue.Print(), i.FalseValue.Print())
}

func (cast *Cast) Print() string {
	return fmt.Sprintf("Cast(%s as %s)", cast.Child.Print(), rows.DataTypeName[cast.DataType])
}

func (c *CaseWhen) Print() string {
	return fmt.Sprintf("CaseWhen(...)")
}

func (a *Alias) Print() string {
	return fmt.Sprintf("%s as %s", a.Child.Print(), a.Name)
}

func (l *Literal) Print() string {
	if l.Type == rows.String {
		return fmt.Sprintf("'%v'", l.Value)
	}
	return fmt.Sprintf("%v", l.Value)
}

func (a *Attribute) Print() string {
	return fmt.Sprintf("#%s", a.Name)
}

func (isNull *IsNull) Print() string {
	return fmt.Sprintf("IsNull(%s)", isNull.Child.Print())
}

func (s *Star) Print() string {
	if s.Table != "" {
		return s.Table + ".*"
	} else {
		return "*"
	}
}

func (l *Like) Print() string {
	return fmt.Sprintf("%s like %s", l.Left.Print(), l.Right.Print())
}

func (n *Not) Print() string {
	return fmt.Sprintf("not(%s)", n.Child.Print())
}