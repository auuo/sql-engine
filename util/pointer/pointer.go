package pointer

import (
	"reflect"
	"strconv"
)

func IsNil(v interface{}) bool {
	return v == nil || reflect.ValueOf(v).IsNil()
}

func Int64(v int64) *int64 {
	return &v
}

func Int64From(s string) *int64 {
	size, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil
	}
	return &size
}

func Bool(v bool) *bool {
	return &v
}

func Float64(v float64) *float64 {
	return &v
}

func String(v string) *string {
	return &v
}

func BothInt64(one, two interface{}) (*int64, *int64, bool) {
	v1, ok := one.(*int64)
	if !ok {
		return nil, nil, false
	}
	if v2, ok := two.(*int64); ok {
		return v1, v2, true
	}
	return nil, nil, false
}

func BothString(one, two interface{}) (*string, *string, bool) {
	v1, ok := one.(*string)
	if !ok {
		return nil, nil, false
	}
	if v2, ok := two.(*string); ok {
		return v1, v2, true
	}
	return nil, nil, false
}

func BothFloat64(one, two interface{}) (*float64, *float64, bool) {
	v1, ok := one.(*float64)
	if !ok {
		return nil, nil, false
	}
	if v2, ok := two.(*float64); ok {
		return v1, v2, true
	}
	return nil, nil, false
}

func BothBool(one, two interface{}) (*bool, *bool, bool) {
	v1, ok := one.(*bool)
	if !ok {
		return nil, nil, false
	}
	if v2, ok := two.(*bool); ok {
		return v1, v2, true
	}
	return nil, nil, false
}

func PointerContent(p interface{}) string {
	if IsNil(p) {
		return "null"
	}
	switch actual := p.(type) {
	case *string:
		return "'" + *actual + "'"
	case *bool:
		if *actual {
			return "true"
		} else {
			return "false"
		}
	case *float64:
		return strconv.FormatFloat(*actual, 'f', -1, 64)
	case *int64:
		return strconv.FormatInt(*actual, 10)
	}
	return ""
}
