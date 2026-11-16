package pg

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"k8s.io/utils/clock"
)

const (
	initialBackoff    = time.Second
	defaultMaxBackoff = 30 * time.Second
	closeTimeout      = 2 * time.Second
	// maxConnAge is how long a connection lives, measured from its dial. A release and each CloseIdle (every
	// plan interval) close an older one, so the pool holds a smart shutdown for about one plan interval plus
	// the queries in flight.
	maxConnAge = 10 * time.Second
)

// ErrPoolClosed is returned by AcquireWithin once the pool is closed.
var ErrPoolClosed = errors.New("connection pool is closed")

// ErrNoSlot is returned by AcquireWithin when no slot came free within maxWait.
var ErrNoSlot = errors.New("no connection slot")

// Pool hands out connections to databases of one server, with at most maxConns open in total.
type Pool struct {
	cfg        Config
	guard      string
	maxConns   int
	maxBackoff time.Duration
	clk        clock.PassiveClock
	log        logr.Logger

	// mu guards every field below.
	mu sync.Mutex
	// idleTimeout is how long a connection may sit idle before CloseIdle closes it.
	idleTimeout time.Duration
	// total counts open connections, idle or busy, and dials in flight.
	total int
	// idle is ordered from least to most recently released.
	idle   []*idleConn
	failed map[string]*backoff
	closed bool
	// changed is closed and replaced whenever a slot or an idle connection becomes available.
	changed chan struct{}
}

type idleConn struct {
	conn     *pgx.Conn
	database string
	dialed   time.Time // when the connection was opened; a reuse keeps it
	since    time.Time
}

type backoff struct {
	failures int
	retryAt  time.Time
	last     error // the connect error that started the wait, without the connection string
}

// NewPool returns a pool that opens no connection until AcquireWithin or AcquireNow. log receives the
// errors that have no caller to return to; the zero Logger discards them. maxBackoff caps the wait before
// reconnecting to a failing database; maxBackoff <= 0 keeps the default 30s.
func NewPool(c Config, maxConns int, idleTimeout time.Duration, clk clock.PassiveClock, log logr.Logger, maxBackoff time.Duration) *Pool {
	p := &Pool{
		cfg:         c,
		guard:       GuardSQL(c),
		maxConns:    max(1, maxConns),
		idleTimeout: idleTimeout,
		maxBackoff:  defaultMaxBackoff,
		clk:         clk,
		failed:      make(map[string]*backoff),
		changed:     make(chan struct{}),
		log:         log,
	}
	if maxBackoff > 0 {
		p.maxBackoff = maxBackoff
	}
	return p
}

// Guard returns GuardSQL of the pool's Config, to run after each BEGIN.
func (p *Pool) Guard() string { return p.guard }

// AcquireWithin returns a connection to database. At the cap it closes the least recently used idle
// connection of another database, or waits at most maxWait for a slot (0 or less: no limit) and then returns
// ErrNoSlot. After a failed connect it returns ErrBackoff until 1s, 2s, 4s ... up to the maximum backoff
// have passed for that database. maxWait does not bound the connect, which keeps ctx and the connect timeout.
func (p *Pool) AcquireWithin(ctx context.Context, database string, maxWait time.Duration) (*pgx.Conn, func(broken bool), error) {
	return p.acquire(ctx, database, true, maxWait)
}

// AcquireNow is AcquireWithin without the backoff or the wait limit: it connects even while database waits before
// reconnecting, and a successful connect ends that wait.
func (p *Pool) AcquireNow(ctx context.Context, database string) (*pgx.Conn, func(broken bool), error) {
	return p.acquire(ctx, database, false, 0)
}

func (p *Pool) acquire(ctx context.Context, database string, withBackoff bool, maxWait time.Duration) (*pgx.Conn, func(broken bool), error) {
	var giveUp <-chan time.Time // nil blocks forever: no limit
	if maxWait > 0 {
		timer := time.NewTimer(maxWait)
		defer timer.Stop()
		giveUp = timer.C
	}
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, nil, ErrPoolClosed
		}

		if ic := p.takeIdleLocked(database); ic != nil {
			p.mu.Unlock()
			return ic.conn, p.releaser(ctx, ic.conn, database, ic.dialed), nil
		}

		if b := p.failed[database]; withBackoff && b != nil && p.clk.Now().Before(b.retryAt) {
			last := b.last
			p.mu.Unlock()
			return nil, nil, fmt.Errorf("database %s: %w: %w", database, ErrBackoff, last)
		}

		var evicted *idleConn
		switch {
		case p.total < p.maxConns:
			p.total++
		case len(p.idle) > 0:
			evicted = p.idle[0]
			p.idle = p.idle[1:]
		default:
			wait := p.changed
			p.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-giveUp:
				// select picks at random when the context ended as well.
				if err := ctx.Err(); err != nil {
					return nil, nil, fmt.Errorf("wait for a connection slot: %w", err)
				}
				return nil, nil, fmt.Errorf("wait for a connection slot: %w", ErrNoSlot)
			case <-ctx.Done():
				return nil, nil, fmt.Errorf("wait for a connection slot: %w", ctx.Err())
			}
		}
		p.mu.Unlock()

		if evicted != nil {
			p.closeConn(ctx, evicted.conn, evicted.database)
		}
		return p.dial(ctx, database)
	}
}

// takeIdleLocked removes and returns the most recently released live connection to database.
func (p *Pool) takeIdleLocked(database string) *idleConn {
	for i := len(p.idle) - 1; i >= 0; i-- {
		ic := p.idle[i]
		if ic.database != database {
			continue
		}
		p.idle = append(p.idle[:i], p.idle[i+1:]...)
		if ic.conn.IsClosed() {
			p.total--
			p.notifyLocked()
			continue
		}
		return ic
	}
	return nil
}

func (p *Pool) dial(ctx context.Context, database string) (*pgx.Conn, func(bool), error) {
	conn, err := p.connect(ctx, database)
	if err != nil {
		// ConnectError.Config holds the password; keep only the cause.
		var ce *pgconn.ConnectError
		if errors.As(err, &ce) {
			err = ce.Unwrap()
		}
	}

	p.mu.Lock()
	if err != nil {
		p.total--
		p.notifyLocked()
		if ctx.Err() == nil {
			b := p.failed[database]
			if b == nil {
				b = &backoff{}
				p.failed[database] = b
			}
			b.retryAt = p.clk.Now().Add(BackoffDelay(b.failures, p.maxBackoff))
			b.failures++
			b.last = err
		}
		p.mu.Unlock()
		return nil, nil, fmt.Errorf("connect to database %s as %s: %w", database, p.cfg.User, err)
	}
	delete(p.failed, database)
	closed := p.closed
	if closed {
		p.total--
	}
	p.mu.Unlock()

	if closed {
		p.closeConn(ctx, conn, database)
		return nil, nil, ErrPoolClosed
	}
	return conn, p.releaser(ctx, conn, database, p.clk.Now()), nil
}

func (p *Pool) connect(ctx context.Context, database string) (*pgx.Conn, error) {
	cc, err := ConnConfig(p.cfg, database)
	if err != nil {
		return nil, err
	}
	conn, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	return conn, nil
}

// BackoffDelay is 1s doubled once per failure before the current one, capped at maxBackoff.
func BackoffDelay(failures int, maxBackoff time.Duration) time.Duration {
	delay := initialBackoff
	for range failures {
		if delay >= maxBackoff {
			break
		}
		delay *= 2
	}
	return min(delay, maxBackoff)
}

// releaser returns the function that hands conn back; only its first call has an effect. ctx is
// accepted for contextcheck; closing conn uses WithoutCancel(ctx) and the 2 s closeTimeout.
func (p *Pool) releaser(ctx context.Context, conn *pgx.Conn, database string, dialed time.Time) func(bool) {
	var once sync.Once
	return func(broken bool) {
		once.Do(func() { p.release(ctx, conn, database, dialed, broken) })
	}
}

// release hands conn back to the idle list, or closes it when it is broken, mid-transaction or older than
// maxConnAge, so that a native sidecar's pooled sessions don't hold PostgreSQL's smart shutdown open; an
// idle connection that nothing acquires is closed by CloseIdle.
func (p *Pool) release(ctx context.Context, conn *pgx.Conn, database string, dialed time.Time, broken bool) {
	p.mu.Lock()
	// A connection left inside a transaction, open or failed, would carry it into the next run.
	if broken || conn.IsClosed() || conn.PgConn().TxStatus() != 'I' || p.closed || p.clk.Since(dialed) >= maxConnAge {
		p.total--
		p.notifyLocked()
		p.mu.Unlock()
		p.closeConn(ctx, conn, database)
		return
	}
	p.idle = append(p.idle, &idleConn{conn: conn, database: database, dialed: dialed, since: p.clk.Now()})
	p.notifyLocked()
	p.mu.Unlock()
}

// SetIdleTimeout sets the idle timeout the next CloseIdle uses.
func (p *Pool) SetIdleTimeout(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.idleTimeout = d
}

// IdleTimeout returns the idle timeout in force.
func (p *Pool) IdleTimeout() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.idleTimeout
}

// CloseIdle closes the idle connections that have been unused for the idle timeout or are older than
// maxConnAge. ctx is accepted for contextcheck; each close uses WithoutCancel(ctx) and the 2 s closeTimeout.
func (p *Pool) CloseIdle(ctx context.Context) {
	p.mu.Lock()
	now := p.clk.Now()
	var stale []*idleConn
	kept := p.idle[:0]
	for _, ic := range p.idle {
		if now.Sub(ic.since) >= p.idleTimeout || now.Sub(ic.dialed) >= maxConnAge {
			stale = append(stale, ic)
			continue
		}
		kept = append(kept, ic)
	}
	p.idle = kept
	p.total -= len(stale)
	if len(stale) > 0 {
		p.notifyLocked()
	}
	p.mu.Unlock()

	for _, ic := range stale {
		p.closeConn(ctx, ic.conn, ic.database)
	}
}

// Close closes the idle connections and makes AcquireWithin fail. Connections still in use are
// closed when they are released. ctx is accepted for contextcheck;
// each close uses WithoutCancel(ctx) and the 2 s closeTimeout.
func (p *Pool) Close(ctx context.Context) {
	p.mu.Lock()
	p.closed = true
	stale := make([]*idleConn, 0, len(p.idle))
	stale = append(stale, p.idle...)
	p.idle = nil
	p.total -= len(stale)
	p.notifyLocked()
	p.mu.Unlock()

	for _, ic := range stale {
		p.closeConn(ctx, ic.conn, ic.database)
	}
}

func (p *Pool) notifyLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

// closeConn ends the session; the socket is closed even when the goodbye fails, so the error is only logged.
// A cancelled ctx must not cut the goodbye short, so it is detached with WithoutCancel.
func (p *Pool) closeConn(ctx context.Context, conn *pgx.Conn, database string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()
	if err := conn.Close(ctx); err != nil {
		p.log.V(1).Info("close connection", "database", database, "error", err.Error())
	}
}
