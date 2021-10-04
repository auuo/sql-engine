package plan

import (
	"reflect"
	"sql-engine/expression"
	"sql-engine/rows"
	"strings"
)

// 如果 select 里存在聚合函数，那么替换为 agg
type PureAggregateReplace struct{}

func (PureAggregateReplace) Apply(plan Plan) Plan {
	return Transform(plan, func(p Plan) Plan {
		if project, ok := p.(*Project); ok {
			// project 里存在聚合函数
			existAgg := false
			for _, e := range project.ProjectList {
				expression.Transform(e, func(expr expression.Expression) expression.Expression {
					if _, ok := expr.(expression.AggFunction); ok {
						existAgg = true
					}
					return expr
				})
			}
			if !existAgg {
				return p
			}
			var one int64 = 1
			return &Aggregate{
				Child: project.Child,
				GroupExprs: []expression.Expression{&expression.Literal{
					Value: one,
					Type:  rows.Int,
				}},
				AggregateExprs: project.ProjectList,
			}
		}
		return p
	})
}

// 只能在 group 中使用聚合函数
type CheckAggregateUse struct {}

func (CheckAggregateUse) Apply(plan Plan) Plan {
	return Transform(plan, func(p Plan) Plan {
		var exprs []expression.Expression
		switch t := p.(type) {
		case *Project:
			exprs = append(exprs, t.ProjectList...)
		case *Filter:
			exprs = append(exprs, t.Condition)
		case *Sort:
			for _, order := range t.Order {
				exprs = append(exprs, order.Expr)
			}
		}
		for _, expr := range exprs {
			expression.Transform(expr, func(e expression.Expression) expression.Expression {
				if agg, ok := e.(expression.AggFunction); ok {
					panic("just use in group by: " + strings.Split(reflect.TypeOf(agg).String(), ".")[1])
				}
				return e
			})
		}
		return p
	})
}

// 检测 schema，主要确保引用正确字段和表达式类型正确
type CheckSchema struct {}

func (CheckSchema) Apply(plan Plan) Plan {
	// GetSchema 中会自行检查
	plan.GetSchema()

	var checkExpr func(expression.Expression, []rows.StructField)
	checkExpr = func(expr expression.Expression, option []rows.StructField) {
		expr.GetSchema(option)
		for _, e := range expr.GetChildren() {
			checkExpr(*e, option)
		}
	}

	return Transform(plan, func(p Plan) Plan {
		if project, ok := p.(*Project); ok {
			option := project.Child.GetSchema()
			for _, expr := range project.ProjectList {
				checkExpr(expr, option)
			}
		}
		if filter, ok := p.(*Filter); ok {
			checkExpr(filter.Condition, filter.Child.GetSchema())
		}
		if agg, ok := p.(*Aggregate); ok {
			option := agg.Child.GetSchema()
			for _, expr := range append(agg.GroupExprs, agg.AggregateExprs...) {
				checkExpr(expr, option)
			}
		}
		return p
	})
}

// 检测 group by select 部分的表达式，不应该存在 select * group by
type CheckStarInAggregate struct {}

func (CheckStarInAggregate) Apply(plan Plan) Plan {
	return Transform(plan, func(p Plan) Plan {
		if agg, ok := p.(*Aggregate); ok {
			for _, expr := range agg.AggregateExprs {
				if _, ok = expr.(*expression.Star); ok {
					panic("'*' can not in group by")
				}
			}
		}
		return p
	})
}

// 对 group by 语句 select 部分的表达式附上代理.
type ProxyExprInAggregate struct {}

func (p ProxyExprInAggregate) Apply(plan Plan) Plan {
	return Transform(plan, func(p Plan) Plan {
		if agg, ok := p.(*Aggregate); ok {
			groupSchema := agg.GetGroupSchema()
			for i, aggExpr := range agg.AggregateExprs {
				agg.AggregateExprs[i] = expression.Transform(aggExpr, func(expr expression.Expression) expression.Expression {
					return &expression.ExprProxy{Expr: expr, GroupSchema: groupSchema}
				})
			}
		}
		return p
	})
}
