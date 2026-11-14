package export

import (
	"os"
	"slices"
	"strings"
)

// ClearEnv unsets every OTEL_* variable in this process, so the SDK neither reads the files
// they name nor logs their values, and returns the names it removed, sorted.
func ClearEnv() []string {
	var removed []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(name, "OTEL_") {
			continue
		}
		// A failed unset leaves the variable set and out of the result.
		if err := os.Unsetenv(name); err == nil {
			removed = append(removed, name)
		}
	}
	slices.Sort(removed)
	return removed
}
