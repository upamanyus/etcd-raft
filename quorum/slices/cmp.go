package slices

import (
	"cmp"
)

func CompareUint64(a, b uint64) int {
	return cmp.Compare(a, b)
}
