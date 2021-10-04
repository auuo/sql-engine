package plan

import (
	"errors"
	"fmt"
	"sql-engine/expression"
	"sql-engine/rows"
	"strings"
	"sync"
)

func (r *Relation) Execute() rows.Dataset {
	var result []rows.Row
	for _, data := range r.DataSource.Execute(r.PushDownPredicate) {
		row := rows.New(data)
		result = append(result, row)
	}
	return rows.Dataset{
		Data:   result,
		Schema: r.DataSource.GetSchema(),
	}
}

func (s *Subquery) Execute() rows.Dataset {
	return rows.Dataset{
		Data:   s.Child.Execute().Data,
		Schema: s.GetSchema(),
	}
}

func (u *Union) Execute() rows.Dataset {
	var result []rows.Row
	type option struct {
		err  error
		data []rows.Row
	}

	ch := make(chan option, len(u.Children))
	jobParallel := make(chan int, 5)
	wg := sync.WaitGroup{}
	wg.Add(len(u.Children))
	for _, child := range u.Children {
		jobParallel <- 0
		go func(plan Plan) {
			defer func() {
				if err := recover(); err != nil {
					ch <- option{err: errors.New(fmt.Sprintf("union all execute err: %v", err))}
				}
				<-jobParallel
				wg.Done()
			}()
			ch <- option{data: plan.Execute().Data}
		}(child)
	}
	wg.Wait()
	close(ch)
	close(jobParallel)
	for i := 0; i < len(u.Children); i++ {
		if opt := <- ch; opt.err != nil {
			panic(opt.err)
		} else {
			result = append(result, opt.data...)
		}
	}
	return rows.Dataset{
		Data:   result,
		Schema: u.GetSchema(),
	}
}

func (p *Project) Execute() rows.Dataset {
	subSchema := p.Child.GetSchema()
	handle := func(row rows.Row) rows.Row {
		var data []interface{}
		for _, expr := range p.ProjectList {
			// 区分 * 和其他情况
			if star, ok := expr.(*expression.Star); ok {
				for i, field := range subSchema {
					// 如果不是单独的 '*' 那么就要匹配 table 名
					if star.Table == "" || strings.HasPrefix(field.Name, star.Table+".") {
						data = append(data, row.IndexOf(i))
					}
				}
			} else {
				r := expr.Eval(row)
				data = append(data, r)
			}
		}
		r := rows.New(data)
		return r
	}
	var result []rows.Row
	for _, row := range p.Child.Execute().Data {
		result = append(result, handle(row))
	}
	return rows.Dataset{
		Data:   result,
		Schema: p.GetSchema(),
	}
}

func (f *Filter) Execute() rows.Dataset {
	var result []rows.Row
	for _, row := range f.Child.Execute().Data {
		value := f.Condition.Eval(row)
		if b, ok := value.(*bool); !ok {
			panic(fmt.Sprintf("expect bool type, but got %t", value))
		} else if b != nil && *b {
			result = append(result, row)
		}
	}
	return rows.Dataset{
		Data:   result,
		Schema: f.GetSchema(),
	}
}

type group struct {
	groupSchema []rows.StructField // group by 后的表达式的 schema
	groupExprs  []expression.Expression // group by 后的表达式
	rowKey      *rows.Row // 这个分组的 key, 也就是 group by 后表达式的 row
	data        []rows.Row // 这个分组下的所有 row
}

func newGroup(exprs []expression.Expression, schema []rows.StructField) *group {
	return &group{
		groupExprs:  exprs,
		groupSchema: schema,
	}
}

func (g *group) add(row rows.Row) {
	g.data = append(g.data, row)
	if g.rowKey != nil {
		return
	}
	// 计算 group by key
	var data []interface{}
	for _, expr := range g.groupExprs {
		v := expr.Eval(row)
		data = append(data, v)
	}
	rowKey := rows.New(data)
	g.rowKey = &rowKey
}

// 基于排序的分组
func sortBaseGroups(dataset rows.Dataset, groupExprs []expression.Expression, groupSchema []rows.StructField) []*group {
	var sortOrder []SortOrder
	for _, expr := range groupExprs {
		sortOrder = append(sortOrder, SortOrder{Expr: expr})
	}
	sorter := newSorter(sortOrder)
	dataset = sorter.sort(dataset)
	gsorter := sorter.(*genericSorter)
	// 从前往后读, group by key 相等的为一组
	var groupData []*group
	groupData = append(groupData, newGroup(groupExprs, groupSchema))
	// 兼容 select count(1) from xx 的情况，没有 group by 可能 from 后条数为 0
	if len(dataset.Data) != 0 {
		groupData[0].add(dataset.Data[0])
	}
	for i := 1; i < len(dataset.Data); i++ {
		if gsorter.Less(i-1, i) {
			// 前一个不比当前小，新增一个组
			groupData = append(groupData, newGroup(groupExprs, groupSchema))
		}
		groupData[len(groupData)-1].add(dataset.Data[i])
	}
	return groupData
}

func (a *Aggregate) Execute() rows.Dataset {
	groupData := sortBaseGroups(a.Child.Execute(), a.GroupExprs, a.GetGroupSchema())
	// 对每一组求值
	var result []rows.Row
	for _, g := range groupData {
		var rowData []interface{}
		for _, expr := range a.AggregateExprs {
			expr.(*expression.ExprProxy).RowGroup = g.data
			// 没有 group by 的情况 row key 可能为 nil
			if g.rowKey != nil {
				rowData = append(rowData, expr.Eval(*g.rowKey))
			} else {
				rowData = append(rowData, expr.Eval(nil))
			}
		}
		r := rows.New(rowData)
		result = append(result, r)
	}
	return rows.Dataset{
		Data:   result,
		Schema: a.GetSchema(),
	}
}

func (s *Sort) Execute() rows.Dataset {
	sorter := newSorter(s.Order)
	return sorter.sort(s.Child.Execute())
}

func (l *Limit) Execute() rows.Dataset {
	dataset := l.Child.Execute()
	if l.Count >= len(dataset.Data) {
		return dataset
	}
	dataset.Data = dataset.Data[:l.Count]
	return dataset
}
