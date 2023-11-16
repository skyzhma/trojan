package utils

import "strconv"

func BytesToFloat64(buf []byte) float64 {
	f, _ := strconv.ParseFloat(string(buf), 64)
	return f
}

func Float64ToBytes(val float64) []byte {
	return []byte(strconv.FormatFloat(val, 'f', -1, 64))
}
