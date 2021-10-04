package parser

import (
	"sql-engine/config"
	"sql-engine/plan"
	"sql-engine/rows"
)

type Strategy int

const (
	Once = iota
	Repeat
)

type Batch struct {
	Strategy Strategy
	Rule     plan.Rule
}

var analysisBatches = []Batch{
	{Rule: plan.PureAggregateReplace{}},
	{Rule: plan.CheckAggregateUse{}},
	{Rule: plan.ProxyExprInAggregate{}},
	{Rule: plan.CheckSchema{}},
	{Rule: plan.CheckStarInAggregate{}},
}
var optimizeBatches = []Batch{
	{Rule: plan.PushDownPredicateIntoSource{}},
}

func ParseSql(sql string, conf config.SQLConf) plan.Plan {
	s := newScanner(sql)
	tokens, err := s.tokens()
	if err != nil {
		panic(err)
	}
	p := newParser(tokens, conf)
	return p.parse()
}

func ExecuteSql(sql string, conf config.SQLConf) rows.Dataset {
	p := ParseSql(sql, conf)
	p = AnalysePlan(p)
	p = OptimizePlan(p)
	return p.Execute()
}

func AnalysePlan(p plan.Plan) plan.Plan {
	return executeBatch(p, analysisBatches)
}

func OptimizePlan(p plan.Plan) plan.Plan {
	return executeBatch(p, optimizeBatches)
}

func executeBatch(p plan.Plan, batches []Batch) plan.Plan {
	for _, batch := range batches {
		if batch.Strategy == Once {
			p = batch.Rule.Apply(p)
		} else {
			// 先尝试 5 次
			for i := 0; i < 5; i++ {
				p = batch.Rule.Apply(p)
			}
		}
	}
	return p
}
