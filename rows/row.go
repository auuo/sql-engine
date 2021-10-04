package rows

type Row interface {
	IndexOf(idx int) interface{}
}

func New(data []interface{}) Row {
	return &genericRow{
		data: data,
	}
}

type genericRow struct {
	data []interface{}
}

func (r *genericRow) IndexOf(idx int) interface{} {
	return r.data[idx]
}