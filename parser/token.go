package parser

type tokenType int

const (
	_EOF tokenType = iota
	_Function
	_IntLit
	_FloatLit
	_StringLit
	_BooleanLit
	_Comment
	_Comma
	_Name

	_Lparen // (
	_Rparen // )
	_Add
	_Sub
	_Mul
	_Div
	_Rem // %
	_Eql // =
	_Neq // !=
	_Lss // <
	_Gtr // >
	_Leq // <=
	_Geq // >=

	// keywords
	_Select
	_From
	_Where
	_Cast
	_As
	_Left
	_Join
	_Full
	_Outer
	_Right
	_Bigint
	_Double
	_String
	_On
	_Order
	_Group
	_By
	_Case
	_When
	_Then
	_Else
	_End
	_If
	_Distinct
	_In
	_Is
	_Not
	_Null
	_Or
	_And
	_Union
	_All
	_Limit
	_Asc
	_Desc
	_True
	_False
	_Like
)

type pos struct {
	row int
	col int
}

type token struct {
	pos
	Type  tokenType
	Value string
}

// 运算符优先级
var opPriority = map[tokenType]int{
	_Or:  1,
	_And: 2,

	_Leq: 3,
	_Lss: 3,
	_Eql: 3,
	_Gtr: 3,
	_Geq: 3,
	_Neq: 3,

	_Add: 4,
	_Sub: 4,
	_Mul: 5,
	_Div: 5,
	_Rem: 5,
}

func opGreat(op1, op2 tokenType) bool {
	return opPriority[op1] > opPriority[op2]
}

var keywordMap = map[string]tokenType{
	"select":   _Select,
	"from":     _From,
	"where":    _Where,
	"cast":     _Cast,
	"as":       _As,
	"left":     _Left,
	"join":     _Join,
	"full":     _Full,
	"outer":    _Outer,
	"right":    _Right,
	"bigint":   _Bigint,
	"double":   _Double,
	"string":   _String,
	"on":       _On,
	"order":    _Order,
	"group":    _Group,
	"by":       _By,
	"case":     _Case,
	"when":     _When,
	"then":     _Then,
	"else":     _Else,
	"end":      _End,
	"if":       _If,
	"distinct": _Distinct,
	"in":       _In,
	"is":       _Is,
	"not":      _Not,
	"null":     _Null,
	"or":       _Or,
	"and":      _And,
	"union":    _Union,
	"all":      _All,
	"limit":    _Limit,
	"asc":      _Asc,
	"desc":     _Desc,
	"true":     _True,
	"false":    _False,
	"like":		_Like,
}

var tokensName = map[tokenType]string{
	_Name:      "name",
	_Function:  "function",
	_IntLit:    "intLit",
	_FloatLit:  "floatLit",
	_StringLit: "stringLit",
	_Comment:   "//",
	_Comma:     ",",
	_EOF:       "EOF",
	_Lparen:    "(",
	_Rparen:    ")",
	_Add:       "+",
	_Sub:       "-",
	_Mul:       "*",
	_Div:       "/",
	_Rem:       "%",
	_Eql:       "=",
	_Neq:       "!=",
	_Lss:       "<",
	_Gtr:       ">",
	_Leq:       "<=",
	_Geq:       ">=",
	_Select:    "select",
	_From:      "from",
	_Where:     "where",
	_Cast:      "cast",
	_As:        "as",
	_Left:      "left",
	_Join:      "join",
	_Full:      "full",
	_Outer:     "outer",
	_Right:     "right",
	_Bigint:    "bigint",
	_Double:    "double",
	_String:    "string",
	_On:        "on",
	_Order:     "order",
	_Group:     "group",
	_By:        "by",
	_Case:      "case",
	_When:      "when",
	_Then:      "then",
	_Else:      "else",
	_End:       "end",
	_If:        "if",
	_Distinct:  "distinct",
	_In:        "in",
	_Is:        "is",
	_Not:       "not",
	_Null:      "null",
	_Or:        "or",
	_And:       "and",
	_Union:     "union",
	_All:       "all",
	_Limit:     "limit",
	_Asc:       "asc",
	_Desc:      "desc",
	_True:      "true",
	_False:     "false",
	_Like:		"like",
}
