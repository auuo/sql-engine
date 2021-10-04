package plan

import (
	"fmt"
	"strings"
)

func (p *Project) Print(level int) {
	PrintBlank(level)
	fmt.Print("Project[")
	for i, e := range p.ProjectList {
		fmt.Printf(e.Print())
		if i != len(p.ProjectList)-1 {
			fmt.Printf(", ")
		}
	}
	fmt.Println("]")
	p.Child.Print(level + 1)
}

func (f *Filter) Print(level int) {
	PrintBlank(level)
	fmt.Printf("Filter[%s]\n", f.Condition.Print())
	f.Child.Print(level + 1)
}

func (r *Relation) Print(level int) {
	PrintBlank(level)
	fmt.Printf("Relation(input = '%s', alias = '%s')\n", r.Input, r.Alias)
}

func (u *Union) Print(level int) {
	PrintBlank(level)
	fmt.Println("Union:")
	for _, child := range u.Children {
		child.Print(level + 1)
	}
}

func (a *Aggregate) Print(level int) {
	PrintBlank(level)
	fmt.Print("Aggregate([")
	for i, expr := range a.GroupExprs {
		fmt.Print(expr.Print())
		if i != len(a.GroupExprs)-1 {
			fmt.Print(", ")
		}
	}
	fmt.Print("], [")
	for i, expr := range a.AggregateExprs {
		fmt.Print(expr.Print())
		if i != len(a.AggregateExprs)-1 {
			fmt.Print(", ")
		}
	}
	fmt.Println("])")
	a.Child.Print(level + 1)
}

func (s *Subquery) Print(level int) {
	PrintBlank(level)
	fmt.Printf("SubqueryAlias %s\n", s.Alias)
	s.Child.Print(level + 1)
}

func (s *Sort) Print(level int) {
	PrintBlank(level)
	fmt.Print("Sort(")
	for i, order := range s.Order {
		fmt.Print(order.Expr.Print())
		if order.Reverse {
			fmt.Print(" desc")
		}
		if i != len(s.Order)-1 {
			fmt.Print(", ")
		}
	}
	fmt.Println(")")
	s.Child.Print(level + 1)
}

func (l *Limit) Print(level int) {
	PrintBlank(level)
	fmt.Printf("Limit(%d)\n", l.Count)
	l.Child.Print(level + 1)
}

func PrintBlank(level int) {
	blank := strings.Repeat("    ", level)
	fmt.Print(blank)
	fmt.Print("- ")
}
