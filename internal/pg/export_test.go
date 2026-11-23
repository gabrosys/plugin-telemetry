package pg

// FreeSlots returns how many more connections the pool may open now.
func (p *Pool) FreeSlots() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxConns - p.total
}
