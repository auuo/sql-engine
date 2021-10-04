package conv

import "strconv"

func IntDefault(str string, d int) int {
	if v, err := strconv.Atoi(str); err != nil {
		return d
	} else {
		return v
	}
}
