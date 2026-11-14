package memlimit

// SetLimiter replaces the function that applies the limit and returns the function that restores it.
func SetLimiter(f func(int64) int64) (restore func()) {
	prev := setMemoryLimit
	setMemoryLimit = f
	return func() { setMemoryLimit = prev }
}
