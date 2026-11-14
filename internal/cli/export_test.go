package cli

// FormatSamples is formatSamples.
var FormatSamples = formatSamples

// SetMemlimit replaces the function that applies the memory limit and returns the function that restores it.
func SetMemlimit(f func(func(string) ([]byte, error), func(string) string) (int64, error)) (restore func()) {
	prev := setMemoryLimit
	setMemoryLimit = f
	return func() { setMemoryLimit = prev }
}
