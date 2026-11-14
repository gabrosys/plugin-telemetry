package engine

import (
	"sync"

	"github.com/go-logr/logr"

	"github.com/gabrosys/plugin-telemetry/internal/snapshot"
)

// maxMessages bounds the distinct messages remembered per failing (query, database).
const maxMessages = 16

// errorLog logs a failure of one (query, database) once per distinct message and logs its recovery once.
// The zero value is ready to use.
type errorLog struct {
	mu      sync.Mutex                           // guards failing
	failing map[snapshot.Key]map[string]struct{} // the messages logged since each key last succeeded
}

// Failed logs err unless its message was already logged since query last succeeded on database.
func (l *errorLog) Failed(log logr.Logger, query, database string, err error) {
	if l.note(snapshot.Key{Query: query, Database: database}, err.Error()) {
		log.Error(err, "query failed", "query", query, "database", database)
	}
}

// Succeeded logs the recovery of query on database if it was failing.
func (l *errorLog) Succeeded(log logr.Logger, query, database string) {
	if l.forget(snapshot.Key{Query: query, Database: database}) {
		log.Info("query recovered", "query", query, "database", database)
	}
}

// note records msg for k and reports whether it is new. Past maxMessages it starts over.
func (l *errorLog) note(k snapshot.Key, msg string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	seen := l.failing[k]
	if _, ok := seen[msg]; ok {
		return false
	}
	if seen == nil || len(seen) >= maxMessages {
		if l.failing == nil {
			l.failing = map[snapshot.Key]map[string]struct{}{}
		}
		seen = map[string]struct{}{}
		l.failing[k] = seen
	}
	seen[msg] = struct{}{}
	return true
}

// forget clears k and reports whether it was failing.
func (l *errorLog) forget(k snapshot.Key) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.failing[k]
	delete(l.failing, k)
	return ok
}

// retain forgets the keys keep rejects.
func (l *errorLog) retain(keep func(snapshot.Key) bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for k := range l.failing {
		if !keep(k) {
			delete(l.failing, k)
		}
	}
}
