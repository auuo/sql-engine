package plan

import (
	"sql-engine/expression"
)

// 将过滤条件下推至 relation 中
type PushDownPredicateIntoSource struct {}

func (opt PushDownPredicateIntoSource) Apply(plan Plan) Plan {
	return Transform(plan, func(p Plan) Plan {
		if filter, ok := p.(*Filter); ok {
			if relation, ok := filter.Child.(*Relation); ok {
				conditions := opt.splitConjunctivePredicates(filter.Condition)
				relation.PushDownPredicate = conditions
			}
		}
		return p
	})
}

// 使用 and 递归拆分表达式
func (opt PushDownPredicateIntoSource) splitConjunctivePredicates(condition expression.Expression) []expression.Expression {
	if and, ok := condition.(*expression.And); ok {
		return append(opt.splitConjunctivePredicates(and.Left), opt.splitConjunctivePredicates(and.Right)...)
	} else {
		return []expression.Expression{condition}
	}
}