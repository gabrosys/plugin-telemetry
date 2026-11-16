//go:build e2e

package measure_test

import "slices"

// stat is the min, median and max of a series of readings.
type stat struct {
	Min    uint64 `json:"min"`
	Median uint64 `json:"median"`
	Max    uint64 `json:"max"`
}

func statOf(xs []uint64) stat {
	s := slices.Sorted(slices.Values(xs))

	return stat{Min: s[0], Median: s[len(s)/2], Max: s[len(s)-1]}
}
