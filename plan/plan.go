package plan

import (
	"sql-engine/expression"
	"sql-engine/rows"
	"sql-engine/source"
)

type Plan interface {
	Execute() rows.Dataset
	Print(level int)
	GetSchema() []rows.StructField
	GetChildren() []*Plan
}

type Project struct {
	ProjectList []expression.Expression
	Child       Plan
	schemaCache []rows.StructField
}

type Filter struct {
	Condition expression.Expression
	Child     Plan
}

type Relation struct {
	Input             string
	Alias             string
	DataSource        source.Source
	PushDownPredicate []expression.Expression
}

type Union struct {
	Children []Plan
}

type Aggregate struct {
	Child          Plan
	GroupExprs     []expression.Expression // group by 后的表达式
	AggregateExprs []expression.Expression // select 中的[聚合]表达式
	schemaCache    []rows.StructField
}

type Subquery struct {
	Child       Plan
	Alias       string
	schemaCache []rows.StructField
}

type Sort struct {
	Child Plan
	Order []SortOrder
}

type Limit struct {
	Child Plan
	Count int
}

func (p *Project) GetChildren() []*Plan {
	return []*Plan{&p.Child}
}

func (f *Filter) GetChildren() []*Plan {
	return []*Plan{&f.Child}
}

func (r *Relation) GetChildren() []*Plan {
	return []*Plan{}
}

func (u *Union) GetChildren() []*Plan {
	var result []*Plan
	for i := range u.Children {
		result = append(result, &u.Children[i])
	}
	return result
}

func (a *Aggregate) GetChildren() []*Plan {
	return []*Plan{&a.Child}
}

func (s *Subquery) GetChildren() []*Plan {
	return []*Plan{&s.Child}
}

func (s *Sort) GetChildren() []*Plan {
	return []*Plan{&s.Child}
}

func (l *Limit) GetChildren() []*Plan {
	return []*Plan{&l.Child}
}

func Transform(plan Plan, fn func(p Plan) Plan) Plan {
	children := plan.GetChildren()
	for _, child := range children {
		*child = Transform(*child, fn)
	}
	return fn(plan)
}
