package engine

import (
	"github.com/go-logr/logr"

	"github.com/gabrosys/plugin-telemetry/internal/latch"
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
	failing latch.Latch[failure]
}

// Failed logs err unless query on database was already failing with outcome since it last succeeded.
func (l *errorLog) Failed(log logr.Logger, query, database string, outcome selfmetrics.Outcome, err error) {
	if l.failing.Set(failure{snapshot.Key{Query: query, Database: database}, outcome}) {
		log.Error(err, "query failed", "query", query, "database", database, "outcome", string(outcome))
	}
}

// Succeeded logs the recovery of query on database if it was failing, and forgets every outcome.
func (l *errorLog) Succeeded(log logr.Logger, query, database string) {
	k := snapshot.Key{Query: query, Database: database}
	if l.failing.Retain(func(f failure) bool { return f.key != k }) {
		log.Info("query recovered", "query", query, "database", database)
	}
}

// retain forgets the keys keep rejects.
func (l *errorLog) retain(keep func(snapshot.Key) bool) {
	l.failing.Retain(func(f failure) bool { return keep(f.key) })
}
