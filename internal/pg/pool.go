package pg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"k8s.io/utils/clock"
)

const (
	initialBackoff    = time.Second
	defaultMaxBackoff = 30 * time.Second
	closeTimeout      = 2 * time.Second
)

// ErrPoolClosed is returned by AcquireWithin once the pool is closed.
var ErrPoolClosed = errors.New("connection pool is closed")

// ErrNoSlot is returned by AcquireWithin when no slot came free within maxWait.
var ErrNoSlot = errors.New("no connection slot")

// Pool hands out connections to databases of one server, with at most maxConns open in total. It keeps
// no idle connection: release closes each one, so no session stays in a database between runs.
type Pool struct {
	cfg        Config
	guard      string
	maxConns   int
	maxBackoff time.Duration
	clk        clock.PassiveClock
	log        logr.Logger

	// mu guards every field below.
	mu sync.Mutex
	// total counts open connections and dials in flight.
	total  int
	failed map[string]*backoff
	closed bool
	// changed is closed and replaced whenever a slot becomes available.
	changed chan struct{}
}

type backoff struct {
	failures int
	retryAt  time.Time
	last     error // the connect error that started the wait, without the connection string
}

// NewPool returns a pool that opens no connection until AcquireWithin or AcquireNow. log receives the
// errors that have no caller to return to; the zero Logger discards them. maxBackoff caps the wait before
// reconnecting to a failing database; maxBackoff <= 0 keeps the default 30s.
func NewPool(c Config, maxConns int, clk clock.PassiveClock, log logr.Logger, maxBackoff time.Duration) *Pool {
	p := &Pool{
		cfg:        c,
		guard:      GuardSQL(c),
		maxConns:   max(1, maxConns),
		maxBackoff: defaultMaxBackoff,
		clk:        clk,
		failed:     make(map[string]*backoff),
		changed:    make(chan struct{}),
		log:        log,
	}
	if maxBackoff > 0 {
		p.maxBackoff = maxBackoff
	}
	return p
}

// Guard returns GuardSQL of the pool's Config, to run after each BEGIN.
func (p *Pool) Guard() string { return p.guard }

// AcquireWithin dials a connection to database. At the cap it waits at most maxWait for a slot (0 or less:
// no limit) and then returns ErrNoSlot. After a failed connect it returns ErrBackoff until 1s, 2s, 4s ... up
// to the maximum backoff have passed for that database. maxWait does not bound the connect, which keeps ctx
// and the connect timeout.
func (p *Pool) AcquireWithin(ctx context.Context, database string, maxWait time.Duration) (*pgx.Conn, func(), error) {
	return p.acquire(ctx, database, true, maxWait)
}

// AcquireNow is AcquireWithin without the backoff or the wait limit: it connects even while database waits before
// reconnecting, and a successful connect ends that wait.
func (p *Pool) AcquireNow(ctx context.Context, database string) (*pgx.Conn, func(), error) {
	return p.acquire(ctx, database, false, 0)
}

func (p *Pool) acquire(ctx context.Context, database string, withBackoff bool, maxWait time.Duration) (*pgx.Conn, func(), error) {
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

		if b := p.failed[database]; withBackoff && b != nil && p.clk.Now().Before(b.retryAt) {
			last := b.last
			p.mu.Unlock()
			return nil, nil, fmt.Errorf("database %s: %w: %w", database, ErrBackoff, last)
		}

		if p.total < p.maxConns {
			p.total++
			p.mu.Unlock()
			return p.dial(ctx, database)
		}
		wait := p.changed
		p.mu.Unlock()
		select {
		case <-wait:
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
}

// dial opens a connection in a slot acquire has taken.
func (p *Pool) dial(ctx context.Context, database string) (*pgx.Conn, func(), error) {
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
	p.mu.Unlock()

	release := p.releaser(ctx, conn, database)
	if closed {
		release()
		return nil, nil, ErrPoolClosed
	}
	return conn, release, nil
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

// releaser returns the function that closes conn and frees its slot; only its first call has an effect.
// ctx is accepted for contextcheck and reaches closeConn.
func (p *Pool) releaser(ctx context.Context, conn *pgx.Conn, database string) func() {
	var once sync.Once
	return func() {
		once.Do(func() { p.release(ctx, conn, database) })
	}
}

// release closes the connection, then frees the slot. For a connection that is still open and idle, release
// returns once the backend has exited or closeTimeout has passed. For one that pgx already closed after an
// error, or one released mid-statement, the slot is freed while the backend may still count against the
// role's limit.
func (p *Pool) release(ctx context.Context, conn *pgx.Conn, database string) {
	p.closeConn(ctx, conn, database)
	p.mu.Lock()
	p.total--
	p.notifyLocked()
	p.mu.Unlock()
}

// Close makes AcquireWithin and AcquireNow fail with ErrPoolClosed. Connections in use are closed when they
// are released.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
}

func (p *Pool) notifyLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

// closeConn ends the session within closeTimeout on WithoutCancel(ctx), so a cancelled ctx does not cut the
// goodbye short. It sends Terminate and reads until the server closes the socket, which PostgreSQL does only
// once the backend has exited, so when closeConn returns the backend has exited or closeTimeout has passed.
// A ping SyncConn sends can run up to cancelDeadlineDelay past closeTimeout. A connection pgx has already
// closed is left to pgx, which closes the socket in the background. The socket is closed whatever fails, so
// errors are only logged.
func (p *Pool) closeConn(ctx context.Context, conn *pgx.Conn, database string) {
	if conn.IsClosed() {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()
	if err := syncClose(ctx, conn.PgConn()); err != nil {
		p.log.V(1).Info("close connection", "database", database, "error", err.Error())
	}
}

// syncClose takes the socket over from pc, sends Terminate and waits until the server closes the socket or
// ctx's deadline passes, then closes the socket. When pc cannot hand the socket over, pc.Close closes it without waiting.
func syncClose(ctx context.Context, pc *pgconn.PgConn) error {
	if err := pc.SyncConn(ctx); err != nil {
		return errors.Join(fmt.Errorf("sync the connection: %w", err), pc.Close(ctx))
	}
	hc, err := pc.Hijack()
	if err != nil {
		return errors.Join(fmt.Errorf("take over the connection: %w", err), pc.Close(ctx))
	}
	err = awaitServerClose(ctx, hc)
	if cerr := hc.Conn.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
		err = errors.Join(err, fmt.Errorf("close the socket: %w", cerr))
	}
	return err
}

// awaitServerClose sends Terminate on hc and reads until the server closes the socket or ctx's deadline
// passes.
func awaitServerClose(ctx context.Context, hc *pgconn.HijackedConn) error {
	if deadline, ok := ctx.Deadline(); ok {
		if err := hc.Conn.SetDeadline(deadline); err != nil {
			return fmt.Errorf("set the close deadline: %w", err)
		}
	}
	hc.Frontend.Send(&pgproto3.Terminate{})
	if err := hc.Frontend.Flush(); err != nil {
		return fmt.Errorf("send terminate: %w", err)
	}
	if _, err := io.Copy(io.Discard, hc.Conn); err != nil {
		return fmt.Errorf("wait for the server to close the connection: %w", err)
	}
	return nil
}
