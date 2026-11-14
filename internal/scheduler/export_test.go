package scheduler

// WorkerCount returns the number of live workers.
func (s *Scheduler) WorkerCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.workers)
}
