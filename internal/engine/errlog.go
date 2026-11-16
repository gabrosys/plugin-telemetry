package engine

import (
	"sync"

	"github.com/go-logr/logr"

	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/snapshot"
)

// failure is one way a (query, database) fails.
type failure struct {
	key     snapshot.Key
	outcome selfmetrics.Outcome
}

// errorLog logs when a (query, database) starts failing with an outcome, whatever the error text, and
// once when it recovers. The zero value is ready to use.
type errorLog struct {
	mu      sync.Mutex // guards failing
	failing map[failure]struct{}
}

// Failed logs err unless query on database was already failing with outcome since it last succeeded.
func (l *errorLog) Failed(log logr.Logger, query, database string, outcome selfmetrics.Outcome, err error) {
	if l.note(failure{snapshot.Key{Query: query, Database: database}, outcome}) {
		log.Error(err, "query failed", "query", query, "database", database, "outcome", string(outcome))
	}
}

// Succeeded logs the recovery of query on database if it was failing, and forgets every outcome.
func (l *errorLog) Succeeded(log logr.Logger, query, database string) {
	if l.forget(snapshot.Key{Query: query, Database: database}) {
		log.Info("query recovered", "query", query, "database", database)
	}
}

// note records f and reports whether it is new.
func (l *errorLog) note(f failure) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.failing[f]; ok {
		return false
	}
	if l.failing == nil {
		l.failing = map[failure]struct{}{}
	}
	l.failing[f] = struct{}{}
	return true
}

// forget clears every outcome of k and reports whether k was failing.
func (l *errorLog) forget(k snapshot.Key) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	was := false
	for f := range l.failing {
		if f.key == k {
			delete(l.failing, f)
			was = true
		}
	}
	return was
}

// retain forgets the keys keep rejects.
func (l *errorLog) retain(keep func(snapshot.Key) bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for f := range l.failing {
		if !keep(f.key) {
			delete(l.failing, f)
		}
	}
}
