package parser

import "sql-engine/expression"

type exprStack struct {
	exprs []expression.Expression
}

func (s *exprStack) push(e expression.Expression) {
	s.exprs = append(s.exprs, e)
}

func (s *exprStack) pop() expression.Expression {
	if s.size() == 0 {
		return nil
	}
	r := s.exprs[s.size()-1]
	s.exprs = s.exprs[0 : s.size()-1]
	return r
}

func (s *exprStack) size() int {
	return len(s.exprs)
}

func (s *exprStack) peek() expression.Expression {
	if s.size() == 0 {
		return nil
	}
	return s.exprs[s.size()-1]
}