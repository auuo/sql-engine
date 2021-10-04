package plan

import (
	"sort"
	"sql-engine/expression"
	"sql-engine/rows"
	"sql-engine/util/pointer"
)

type Sorter interface {
	sort(dataset rows.Dataset) rows.Dataset
}

type SortOrder struct {
	Expr    expression.Expression
	Reverse bool
}

type genericSorter struct {
	order   []SortOrder
	dataset rows.Dataset
}

func newSorter(order []SortOrder) Sorter {
	return &genericSorter{order: order}
}

func (s *genericSorter) sort(dataset rows.Dataset) rows.Dataset {
	s.dataset = dataset
	sort.Sort(s)
	return s.dataset
}

func (s *genericSorter) Len() int {
	return len(s.dataset.Data)
}

func (s *genericSorter) Less(i, j int) bool {
	row1 := s.dataset.Data[i]
	row2 := s.dataset.Data[j]
	for _, order := range s.order {
		result1 := order.Expr.Eval(row1)
		result2 := order.Expr.Eval(row2)
		if pointer.IsNil(result1) && pointer.IsNil(result2) {
			continue
		} else if pointer.IsNil(result1) {
			// null 算作最小
			return !order.Reverse
		} else if pointer.IsNil(result2) {
			return order.Reverse
		}
		if v1, v2, ok := pointer.BothString(result1, result2); ok {
			if *v1 == *v2 {
				continue
			}
			if order.Reverse {
				return *v2 < *v1
			} else {
				return *v1 < *v2
			}
		}
		if v1, v2, ok := pointer.BothInt64(result1, result2); ok {
			if *v1 == *v2 {
				continue
			}
			if order.Reverse {
				return *v2 < *v1
			} else {
				return *v1 < *v2
			}
		}
		if v1, v2, ok := pointer.BothFloat64(result1, result2); ok {
			if *v1 == *v2 {
				continue
			}
			if order.Reverse {
				return *v2 < *v1
			} else {
				return *v1 < *v2
			}
		}
		if v1, v2, ok := pointer.BothBool(result1, result2); ok {
			if *v1 == *v2 {
				continue
			}
			if order.Reverse {
				return !*v2 && *v1
			} else {
				return !*v1 && *v2
			}
		}
	}
	return false
}

func (s *genericSorter) Swap(i, j int) {
	s.dataset.Data[i], s.dataset.Data[j] = s.dataset.Data[j], s.dataset.Data[i]
}
