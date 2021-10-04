package parser

import (
	"fmt"
	"sql-engine/config"
	"sql-engine/expression"
	"sql-engine/plan"
	"sql-engine/rows"
	"sql-engine/source"
	"strconv"
)

type parser struct {
	index  int // 识别的 token 位置
	tokens []token
	conf   config.SQLConf
}

func newParser(tokens []token, conf config.SQLConf) *parser {
	return &parser{
		tokens: tokens,
		conf:   conf,
	}
}

func (p *parser) parse() plan.Plan {
	result := p.wantQuery()
	p.want(_EOF)
	return result
}

func (p *parser) wantQuery() plan.Plan {
	plans := []plan.Plan{p.wantSelect()}
	for p.got(_Union) {
		p.want(_All)
		plans = append(plans, p.wantSelect())
	}
	if len(plans) == 1 {
		return plans[0]
	}
	return &plan.Union{
		Children: plans,
	}
}

func (p *parser) wantSelect() plan.Plan {
	var rootPlan plan.Plan
	p.got(_Select)
	selectList := p.wantExpressionList(true)
	p.want(_From)
	dataSource := p.wantSource()

	project := &plan.Project{ProjectList: selectList}
	rootPlan = project
	hasFilter := p.got(_Where)
	var filter plan.Plan
	if hasFilter {
		where := p.wantExpression()
		filter = &plan.Filter{
			Condition: where,
			Child:     dataSource,
		}
		project.Child = filter
	} else {
		project.Child = dataSource
	}
	if p.got(_Group) {
		p.want(_By)
		exprs := p.wantExpressionList(false)
		agg := &plan.Aggregate{
			GroupExprs:     exprs,
			AggregateExprs: selectList,
		}
		if hasFilter {
			agg.Child = filter
		} else {
			agg.Child = dataSource
		}
		rootPlan = agg
	}
	if p.got(_Order) {
		p.want(_By)
		genOrder := func() plan.SortOrder {
			order := plan.SortOrder{Expr: p.wantExpression()}
			if p.got(_Desc) {
				order.Reverse = true
			} else {
				p.got(_Asc)
			}
			return order
		}
		orders := []plan.SortOrder{genOrder()}
		for p.got(_Comma) {
			orders = append(orders, genOrder())
		}
		rootPlan = &plan.Sort{
			Child: rootPlan,
			Order: orders,
		}
	}
	if p.got(_Limit) {
		p.want(_IntLit)
		count, _ := strconv.Atoi(p.tok().Value)
		rootPlan = &plan.Limit{
			Child: rootPlan,
			Count: count,
		}
	}
	return rootPlan
}

func (p *parser) wantSource() plan.Plan {
	if p.got(_StringLit) || p.got(_Name) {
		input := p.tok()
		alias := ""
		if p.got(_As) {
			p.want(_Name)
			alias = p.tok().Value
		}
		return &plan.Relation{
			Input: input.Value,
			Alias: alias,
			DataSource: source.NewSource(p.conf, input.Value),
		}
	}
	// 以上条件不成立则必须为子查询
	p.want(_Lparen)
	subquery := p.wantQuery()
	p.want(_Rparen)
	p.want(_As)
	p.want(_Name)
	alias := p.tok().Value
	return &plan.Subquery{
		Child: subquery,
		Alias: alias,
	}
}

func (p *parser) wantExpressionList(isProject bool) []expression.Expression {
	getExpr := func() expression.Expression {
		if isProject {
			// 可能有 *, table.* 的情况
			if p.got(_Mul) {
				return &expression.Star{}
			}
			if p.got(_Name) {
				name := p.tok().Value
				if name[len(name)-1:] == "." {
					p.want(_Mul)
					return &expression.Star{
						Table: name[:len(name)-1],
					}
				}
				p.back()
			}
		}
		e := p.wantExpression()
		if isProject {
			e = p.mayAlias(e)
		}
		return e
	}
	var result []expression.Expression
	result = append(result, getExpr())
	for p.got(_Comma) {
		result = append(result, getExpr())
	}
	return result
}

func (p *parser) wantExpression() expression.Expression {
	var opStack tokenStack
	var queue []expression.Expression
	startPos := p.peek().pos
	for {
		// 判断表达式是否结束
		needBreak := false
		switch p.peek().Type {
		case _Comma, _EOF, _From, _As:
			needBreak = true
		case _Name:
			//if len(opStack) == 0 {
			//	needBreak = true
			//}
		}
		if needBreak {
			break
		}
		if lit := p.wantLit(false); lit != nil {
			queue = append(queue, lit)
		} else if p.got(_Name) {
			queue = append(queue, &expression.Attribute{Name: p.tok().Value})
		} else if p.got(_If) {
			queue = append(queue, p.wantIf())
		} else if p.got(_Cast) {
			queue = append(queue, p.wantCast())
		} else if p.got(_Case) {
			queue = append(queue, p.wantCaseWhen())
		} else if p.got(_Function) {
			queue = append(queue, p.wantFunction())
		} else if p.got(_Lparen) {
			opStack.push(p.tok())
		} else if p.got(_Rparen) {
			hasLparen := false
			// 左括号之后的都弹入放入队列
			for opStack.size() != 0 {
				// 获取栈顶元素并弹出
				if opStack.peek().Type == _Lparen {
					hasLparen = true
					opStack.pop()
					break
				}
				// token 转为 expr 后放入队列
				queue = append(queue, opStack.popAsExpr())
			}
			if !hasLparen {
				// 可能已经是一个合法的表达式了, 此中情况应该后退一步, ')' 可能是函数的
				if opStack.size() == 0 && len(queue) == 1 {
					p.back()
					return queue[0]
				} else if opStack.size() == 0 {
					p.back()
					break
				}
				p.panicAt("expect '(', expression start", startPos)
			}
		} else if p.got(_Not) {
			if p.got(_In) {
				queue = p.parseIn(queue, startPos)
			} else if p.got(_Like) {
				queue = p.parseLike(queue, startPos)
			} else {
				p.panicAt("expect 'in', 'like' after 'not'", startPos)
			}
			//noinspection GoNilness
			queue[len(queue)-1] = &expression.Not{Child: queue[len(queue)-1]}
		} else if p.got(_In) { // todo 支持 not in
			queue = p.parseIn(queue, startPos)
		} else if p.got(_Like) {
			queue = p.parseLike(queue, startPos)
		} else if p.got(_Is) {
			neg := false
			if p.got(_Not) {
				neg = true
			}
			p.want(_Null)
			if len(queue) == 0 {
				p.panicAt("expect attribute before 'is'", startPos)
			}
			//noinspection GoNilness
			e := queue[len(queue)-1]
			queue = queue[0 : len(queue)-1]
			if attr, ok := e.(*expression.Attribute); !ok {
				p.panicAt("expect attribute before 'in'", startPos)
			} else {
				var expr expression.Expression = &expression.IsNull{Child: &expression.Attribute{Name: attr.Name}}
				if neg {
					expr = &expression.Not{Child: expr}
				}
				queue = append(queue, expr)
			}
		} else if p.got(_Add) || p.got(_Sub) || p.got(_Mul) || p.got(_Div) || p.got(_Rem) ||
			p.got(_Eql) || p.got(_Neq) || p.got(_Lss) || p.got(_Gtr) || p.got(_Leq) ||
			p.got(_Geq) || p.got(_And) || p.got(_Or) {
			for opStack.size() != 0 {
				if opStack.peek().Type == _Lparen {
					opStack.push(p.tok())
					break
				} else if opGreat(p.tok().Type, opStack.peek().Type) {
					opStack.push(p.tok())
					break
				} else {
					queue = append(queue, opStack.popAsExpr())
				}
			}
			if opStack.size() == 0 {
				opStack.push(p.tok())
			}
		} else {
			// 也可能是合法表达式
			break
		}
	}
	// 栈中剩下部分之间加入队列
	for opStack.size() != 0 {
		queue = append(queue, opStack.popAsExpr())
	}

	// 转为树结构
	var exprStack exprStack
	for _, ele := range queue {
		if expr, ok := ele.(expression.BinaryOp); ok {
			rightExpr := exprStack.pop()
			leftExpr := exprStack.pop()
			if leftExpr == nil || rightExpr == nil {
				p.panicAt("expression is illegal", startPos)
			}
			expr.SetLeft(leftExpr)
			expr.SetRight(rightExpr)
			exprStack.push(expr)
		} else {
			exprStack.push(ele)
		}
	}
	if exprStack.size() != 1 {
		p.panicAt("expression is illegal", startPos)
	}
	return exprStack.pop()
}

func (p *parser) wantFunction() expression.Expression {
	funcName := p.tok().Value
	p.want(_Lparen)
	args := p.wantExpressionList(false)
	p.want(_Rparen)
	return expression.NewFuncByName(funcName, args)
}

func (p *parser) parseLike(queue []expression.Expression, startPos pos) []expression.Expression {
	if len(queue) == 0 {
		p.panicAt("expect attribute before 'like'", startPos)
	}
	//noinspection GoNilness
	e := queue[len(queue)-1]
	queue = queue[0 : len(queue)-1]
	if attr, ok := e.(*expression.Attribute); !ok {
		p.panicAt("expect attribute before 'in'", startPos)
	} else {
		var likeBody expression.Expression
		if p.got(_StringLit) {
			likeBody = &expression.Literal{
				Value: p.tok().Value,
				Type:  rows.String,
			}
		} else if p.got(_Function) {
			likeBody = p.wantFunction()
		} else {
			p.panicAt("expect string or function after 'like'", startPos)
		}
		queue = append(queue, &expression.Like{
			Left:  attr,
			Right: likeBody,
		})
	}
	return queue
}

func (p *parser) parseIn(queue []expression.Expression, startPos pos) []expression.Expression {
	p.want(_Lparen)
	result := []expression.Expression{p.wantLit(true)}
	for p.got(_Comma) {
		result = append(result, p.wantLit(true))
	}
	p.want(_Rparen)
	if len(queue) == 0 {
		p.panicAt("expect attribute before 'in'", startPos)
	}
	//noinspection GoNilness
	e := queue[len(queue)-1]
	queue = queue[0 : len(queue)-1]
	if attr, ok := e.(*expression.Attribute); !ok {
		p.panicAt("expect attribute before 'in'", startPos)
	} else {
		queue = append(queue, &expression.In{
			Value: &expression.Attribute{Name: attr.Name},
			List:  result,
		})
	}
	return queue
}

func (p *parser) mayAlias(child expression.Expression) expression.Expression {
	if p.got(_As) {
		p.want(_Name)
		return &expression.Alias{
			Child: child,
			Name:  p.tok().Value,
		}
	}
	return child
}

func (p *parser) wantIf() *expression.If {
	p.want(_Lparen)
	predicate := p.wantExpression()
	p.want(_Comma)
	trueValue := p.wantExpression()
	p.want(_Comma)
	falseValue := p.wantExpression()
	p.want(_Rparen)
	return &expression.If{
		Predicate:  predicate,
		TrueValue:  trueValue,
		FalseValue: falseValue,
	}
}

func (p *parser) wantCast() *expression.Cast {
	p.want(_Lparen)
	expr := p.wantExpression()
	p.want(_As)
	dataType := rows.Int
	if p.got(_Double) {
		dataType = rows.Float
	} else if p.got(_String) {
		dataType = rows.String
	} else {
		p.want(_Bigint)
	}
	p.want(_Rparen)
	return &expression.Cast{
		Child:    expr,
		DataType: dataType,
	}
}

func (p *parser) wantCaseWhen() *expression.CaseWhen {
	var branches []expression.ExprTuple
	var elseValue expression.Expression = nil
	for {
		if p.got(_When) {
			when := p.wantExpression()
			p.want(_Then)
			then := p.wantExpression()
			branches = append(branches, expression.ExprTuple{
				One: when,
				Two: then,
			})
		} else if p.got(_Else) {
			elseValue = p.wantExpression()
		} else if p.got(_End) {
			break
		} else {
			p.expectPanic("'when', 'else' or 'end'", p.peek())
		}
	}
	return &expression.CaseWhen{
		Branches:  branches,
		ElseValue: elseValue,
	}
}

func (p *parser) wantLit(needPanic bool) *expression.Literal {
	if p.got(_Null) {
		return &expression.Literal{
			Value:  "null",
			Type:   rows.Int,
			IsNull: true,
		}
	}
	if p.got(_IntLit) {
		n, _ := strconv.ParseInt(p.tok().Value, 10, 64)
		return &expression.Literal{
			Value: n,
			Type:  rows.Int,
		}
	} else if p.got(_FloatLit) {
		n, _ := strconv.ParseFloat(p.tok().Value, 64)
		return &expression.Literal{
			Value: n,
			Type:  rows.Float,
		}
	} else if p.got(_StringLit) {
		return &expression.Literal{
			Value: p.tok().Value,
			Type:  rows.String,
		}
	} else if p.got(_BooleanLit) {
		r := false
		if p.tok().Value == "true" {
			r = true
		}
		return &expression.Literal{
			Value: r,
			Type:  rows.Boolean,
		}
	}
	if needPanic {
		p.expectPanic("literal", p.peek())
	}
	return nil
}

func (p *parser) got(tok tokenType) bool {
	if p.peek().Type == tok {
		p.index += 1
		return true
	}
	return false
}

func (p *parser) want(tok tokenType) {
	if !p.got(tok) {
		p.expectPanic(tokensName[tok], p.peek())
	}
}

func (p *parser) back() {
	if p.index != 0 {
		p.index -= 1
	}
}

func (p *parser) tok() token {
	if p.index < 1 || p.index > len(p.tokens) {
		return token{
			Type:  _EOF,
			Value: "EOF",
		}
	}
	return p.tokens[p.index-1]
}

func (p *parser) peek() token {
	if p.index < 0 || p.index >= len(p.tokens) {
		return token{
			Type:  _EOF,
			Value: "EOF",
		}
	}
	return p.tokens[p.index]
}

func (p *parser) expectPanic(msg string, tok token) {
	panic(fmt.Sprintf("expect %s, got (%s: %s) at (%d, %d)", msg, tok.Value, tokensName[tok.Type], tok.row, tok.col))
}

func (p *parser) panicAt(msg string, position pos) {
	panic(fmt.Sprintf("%s at (%d, %d)", msg, position.row, position.col))
}
