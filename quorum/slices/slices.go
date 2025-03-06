package slices

import (
	"slices"
)

func SortUint64(a []uint64) {
	slices.Sort(a)
}

type Tup struct {
	ID  uint64
	Idx uint64 // corresponds to quorum's `type Index uint64`
	Ok  bool   // idx found?
	Bar int    // length of bar displayed for this tup
}

func SortFuncTup(x []Tup, f func(a, b Tup) int) {
	slices.SortFunc(x, f)
}
