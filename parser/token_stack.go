package parser

import (
	"fmt"
	"sql-engine/expression"
)

type tokenStack struct {
	tokens []token
}

func (s *tokenStack) push(t token) {
	s.tokens = append(s.tokens, t)
}

func (s *tokenStack) pop() *token {
	if s.size() == 0 {
		return nil
	}
	r := s.tokens[s.size()-1]
	s.tokens = s.tokens[0 : s.size()-1]
	return &r
}

func (s *tokenStack) size() int {
	return len(s.tokens)
}

func (s *tokenStack) peek() *token {
	if s.size() == 0 {
		return nil
	}
	return &s.tokens[s.size()-1]
}

// 将 token 转为 expression 弹出，仅限有 left、right 的子节点
func (s *tokenStack) popAsExpr() expression.BinaryOp {
	t := s.pop()
	if t == nil {
		return nil
	}
	switch t.Type {
	case _Add:
		return &expression.Add{BinaryExpr: expression.BinaryExpr{}}
	case _Sub:
		return &expression.Subtract{BinaryExpr: expression.BinaryExpr{}}
	case _Mul:
		return &expression.Multiply{BinaryExpr: expression.BinaryExpr{}}
	case _Div:
		return &expression.Divide{BinaryExpr: expression.BinaryExpr{}}
	case _Rem:
		return &expression.Remainder{BinaryExpr: expression.BinaryExpr{}}
	case _Eql:
		return &expression.EqualTo{BinaryExpr: expression.BinaryExpr{}}
	case _Neq:
		return &expression.NotEqualTo{BinaryExpr: expression.BinaryExpr{}}
	case _Lss:
		return &expression.LessThan{BinaryExpr: expression.BinaryExpr{}}
	case _Gtr:
		return &expression.GreaterThan{BinaryExpr: expression.BinaryExpr{}}
	case _Leq:
		return &expression.LessThanOrEqual{BinaryExpr: expression.BinaryExpr{}}
	case _Geq:
		return &expression.GreaterThanOrEqual{BinaryExpr: expression.BinaryExpr{}}
	case _And:
		return &expression.And{BinaryExpr: expression.BinaryExpr{}}
	case _Or:
		return &expression.Or{BinaryExpr: expression.BinaryExpr{}}
	default:
		panic(fmt.Sprintf("is not a binary expression: (%s: %s)", t.Value, tokensName[t.Type]))
	}
}
