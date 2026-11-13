package queries

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// toFloat is CloudNativePG's DBToFloat64, ported from v1.30.1 pkg/management/postgres/utils/utils.go.
// NULL becomes NaN with ok true.
func toFloat(t any) (float64, bool) {
	switch v := t.(type) {
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case time.Time:
		return float64(v.Unix()), true
	case []byte:
		return parseFloat(string(v))
	case string:
		return parseFloat(v)
	case bool:
		if v {
			return 1, true
		}

		return 0, true
	case nil:
		return math.NaN(), true
	default:
		return math.NaN(), false
	}
}

func parseFloat(s string) (float64, bool) {
	result, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return math.NaN(), false
	}

	return result, true
}

// toUint is CloudNativePG's DBToUint64, ported from v1.30.1 pkg/management/postgres/utils/utils.go.
// NULL becomes 0 with ok true.
func toUint(t any) (uint64, bool) {
	switch v := t.(type) {
	case uint64:
		return v, true
	case int64:
		return uint64(v), true //nolint:gosec // CNPG's conversion
	case float64:
		return uint64(v), true
	case time.Time:
		return uint64(v.Unix()), true //nolint:gosec // CNPG's conversion
	case []byte:
		return parseUint(string(v))
	case string:
		return parseUint(v)
	case bool:
		if v {
			return 1, true
		}

		return 0, true
	case nil:
		return 0, true
	default:
		return 0, false
	}
}

func parseUint(s string) (uint64, bool) {
	result, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}

	return result, true
}

// toString is CloudNativePG's DBToString, ported from v1.30.1 pkg/management/postgres/utils/utils.go.
// NULL becomes the empty string.
func toString(t any) (string, bool) {
	switch v := t.(type) {
	case int64:
		return fmt.Sprintf("%v", v), true
	case float64:
		return fmt.Sprintf("%v", v), true
	case time.Time:
		return fmt.Sprintf("%v", v.Unix()), true
	case nil:
		return "", true
	case []byte:
		return string(v), true
	case string:
		return v, true
	case bool:
		if v {
			return "true", true
		}

		return "false", true
	default:
		return "", false
	}
}

// mappedValue is CNPG's MAPPEDMETRIC conversion (pkg/management/postgres/metrics/mappings.go:190-205 at v1.30.1):
// only string text found in the mapping converts.
func mappedValue(mapping map[string]float64, in any) (float64, bool) {
	text, ok := in.(string)
	if !ok {
		return math.NaN(), false
	}
	val, ok := mapping[text]
	if !ok {
		return math.NaN(), false
	}

	return val, true
}

// durationMillis is CNPG's DURATION conversion (pkg/management/postgres/metrics/mappings.go:207-232 at v1.30.1):
// Go duration text in whole milliseconds, "-1" and other text rejected.
func durationMillis(in any) (float64, bool) {
	var text string
	switch t := in.(type) {
	case []byte:
		text = string(t)
	case string:
		text = t
	default:
		return math.NaN(), false
	}
	if text == "-1" {
		return math.NaN(), false
	}
	d, err := time.ParseDuration(text)
	if err != nil {
		return math.NaN(), false
	}

	return float64(d / time.Millisecond), true
}
