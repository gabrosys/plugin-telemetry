// Package memlimit sets the Go soft memory limit from the container's cgroup limit.
package memlimit

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"runtime/debug"
	"strconv"
)

const (
	cgroupMemoryMax = "/sys/fs/cgroup/memory.max"
	// percent of the cgroup limit left to the Go heap, the rest covers memory the runtime does not count.
	percent = 80
)

// setMemoryLimit applies the limit; it is a variable so tests do not change the process's limit.
var setMemoryLimit = debug.SetMemoryLimit

// Set sets the soft memory limit to 80% of the cgroup v2 memory.max, unless GOMEMLIMIT is set
// or there is no limit; it returns the limit applied, or 0. A read error other than a missing
// file returns 0 with the error, and the limit is left alone.
func Set(readFile func(string) ([]byte, error), getenv func(string) string) (int64, error) {
	if getenv("GOMEMLIMIT") != "" {
		return 0, nil
	}
	raw, err := readFile(cgroupMemoryMax)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("read %s: %w", cgroupMemoryMax, err)
	}
	total, err := strconv.ParseInt(string(bytes.TrimSpace(raw)), 10, 64)
	if err != nil || total <= 0 {
		// "max" means no limit; anything else is not a limit this package understands.
		return 0, nil //nolint:nilerr // an unreadable limit leaves the runtime default
	}
	limit := total / 100 * percent
	if r := total % 100; r > 0 {
		limit += r * percent / 100
	}
	if limit <= 0 {
		return 0, nil
	}
	setMemoryLimit(limit)
	return limit, nil
}
