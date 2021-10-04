package parser

import (
	"errors"
	"fmt"
	"sql-engine/expression"
	"strings"
)

type scanner struct {
	source  []rune
	pos     int
	startAt int

	// 当前指针行列位置
	row int
	col int
	// 用来记录一个 token 开始的行列
	startRow int
	startCol int

	// 记录 token 的类型和值
	_type  tokenType
	_value string
}

func newScanner(text string) *scanner {
	// \t 都换为 4 个空格。去除每行行前与行尾的空格
	text = strings.ReplaceAll(text, "\t", "    ")
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}
	return &scanner{
		source: []rune(strings.Join(lines, "\n")),
		pos:    -1,
	}
}

func (s *scanner) tokens() ([]token, error) {
	var tokens []token
	for {
		t, err := s.next()
		if err != nil {
			return nil, err
		}
		if t == nil {
			break
		} else if t.Type == _Comment {
			continue
		}
		tokens = append(tokens, *t)
	}
	return tokens, nil
}

func (s *scanner) next() (*token, error) {
	// 获取下个字符，跳过空格
	char := s.getr()
	for char == ' ' || char == '\n' || char == '\t' || char == '\r' {
		char = s.getr()
	}
	s.startPos()

	if isLetter(char) {
		return s.ident()
	}

	switch char {
	case -1:
		return nil, nil
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return s.number()
	case '\'', '"':
		return s.stdString()
	case '-':
		if s.getr() == '-' {
			return s.lineComment()
		}
		s.ungetr()
		s.setTokenInfo(_Sub, "-")
	case ',':
		s.setTokenInfo(_Comma, ",")
	case '(':
		s.setTokenInfo(_Lparen, "(")
	case ')':
		s.setTokenInfo(_Rparen, ")")
	case '+':
		s.setTokenInfo(_Add, "+")
	case '*':
		s.setTokenInfo(_Mul, "*")
	case '/':
		s.setTokenInfo(_Div, "/")
	case '%':
		s.setTokenInfo(_Rem, "%")
	case '=':
		s.setTokenInfo(_Eql, "=")
	case '!':
		if s.getr() != '=' {
			s.ungetr()
			return nil, errors.New("unknown symbol: ! ")
		}
		s.setTokenInfo(_Neq, "!=")
	case '>':
		if s.getr() == '=' {
			s.setTokenInfo(_Geq, ">=")
		} else {
			s.ungetr()
			s.setTokenInfo(_Gtr, ">")
		}
	case '<':
		if s.getr() == '=' {
			s.setTokenInfo(_Leq, "<=")
		} else {
			s.ungetr()
			s.setTokenInfo(_Lss, "<")
		}
	default:
		return nil, errors.New("unknown: " + string(char))
	}
	return s.newToken(), nil
}

// 识别标识符
func (s *scanner) ident() (*token, error) {
	s.startLit()
	hasDot := false
	c := s.getr()
	for isLetter(c) || isDecimal(c)  || (!hasDot && c == '.'){
		if c == '.' {
			hasDot = true
		}
		c = s.getr()
	}
	s.ungetr()
	lit := s.stopLit()
	var t = _Name
	if tt, ok := keywordMap[strings.ToLower(string(lit))]; ok {
		t = tt
		if tt == _True || tt == _False {
			t = _BooleanLit
		}
	} else if _, ok := expression.FuncMap[strings.ToLower(string(lit))]; ok {
		t = _Function
	}
	s.setTokenInfo(t, string(lit))
	return s.newToken(), nil
}

func (s *scanner) number() (*token, error) {
	s.startLit()

	hasDot := false
	c := s.getr()
	for isDecimal(c) || (!hasDot && c == '.') {
		if c == '.' {
			hasDot = true
		}
		c = s.getr()
	}
	s.ungetr()
	num := s.stopLit()
	if num[len(num)-1] == '.' {
		return nil, errors.New(fmt.Sprintf("%s is not normal number", string(num)))
	}
	t := _IntLit
	if hasDot {
		t = _FloatLit
	}
	s.setTokenInfo(t, string(num))
	return s.newToken(), nil
}

func (s *scanner) stdString() (*token, error) {
	s.ungetr()
	quote := s.getr()
	s.startLit()
	for {
		c := s.getr()
		if c == quote {
			break
		}
		if c < 0 {
			return nil, errors.New(fmt.Sprintf("string not terminated: %s", string(s.stopLit()[1:])))
		}
	}
	str := s.stopLit()

	s.setTokenInfo(_StringLit, string(str[1:len(str)-1]))
	return s.newToken(), nil
}

func (s *scanner) lineComment() (*token, error) {
	s.ungetr()
	s.startLit()
	for {
		if c := s.getr(); c == '\n' || c < 0 {
			break
		}
	}
	s.ungetr()
	c := s.stopLit()

	s.setTokenInfo(_Comment, string(c))
	return s.newToken(), nil
}

func (s *scanner) getr() rune {
	s.pos += 1
	if s.pos >= len(s.source) {
		return -1
	}
	c := s.source[s.pos]
	if c == '\n' {
		s.row += 1
		s.col = 0
	} else {
		s.col += 1
	}
	return c
}

func (s *scanner) ungetr() {
	s.pos -= 1
	if s.pos >= len(s.source)-1 {
		return
	}
	if s.col == 0 {
		s.row -= 1
		// 更新 col, 从后往前扫描，直到文件头或者上一个换行
		idx := s.pos
		for idx == 0 || s.source[idx] == '\n' {
			idx--
		}
		s.col = s.pos - idx
	} else {
		s.col -= 1
	}
}

func (s *scanner) startLit() {
	s.startAt = s.pos
}

func (s *scanner) stopLit() []rune {
	return s.source[s.startAt : s.pos+1]
}

func (s *scanner) startPos() {
	s.startRow = s.row
	s.startCol = s.col
}

func (s *scanner) setTokenInfo(t tokenType, v string) {
	s._type = t
	s._value = v
}

func (s *scanner) newToken() *token {
	return &token{
		pos: pos{
			row: s.startRow + 1,
			col: s.startCol,
		},
		Type:  s._type,
		Value: s._value,
	}
}

func lower(c rune) rune     { return ('a' - 'A') | c }
func isDecimal(c rune) bool { return '0' <= c && c <= '9' }
func isLetter(c rune) bool {
	return 'a' <= lower(c) && lower(c) <= 'z' || c == '_'
}
