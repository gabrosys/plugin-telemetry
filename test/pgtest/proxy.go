package pgtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
)

// Proxy forwards TCP connections to a server. Once a chunk from a client contains ROLLBACK, in any case,
// it stops forwarding that client's bytes, so the rollback and everything after it never reach the server.
type Proxy struct {
	Host string
	Port uint16

	target string
	ln     net.Listener
	wg     sync.WaitGroup

	// mu guards conns, closed and errs.
	mu     sync.Mutex
	conns  []net.Conn
	closed bool
	errs   error // close errors of connections the accept loop dropped
}

// StartProxy listens on a free local port and forwards each connection to the server at host:port.
func StartProxy(ctx context.Context, host string, port uint16) (*Proxy, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	localHost, localPort, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return nil, errors.Join(fmt.Errorf("listener address: %w", err), ln.Close())
	}
	n, err := strconv.ParseUint(localPort, 10, 16)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("listener port: %w", err), ln.Close())
	}
	p := &Proxy{Host: localHost, Port: uint16(n), target: net.JoinHostPort(host, strconv.Itoa(int(port))), ln: ln}
	dialCtx := context.WithoutCancel(ctx)
	p.wg.Go(func() { p.accept(dialCtx) })
	return p, nil
}

func (p *Proxy) accept(ctx context.Context) {
	var d net.Dialer
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		server, err := d.DialContext(ctx, "tcp", p.target)
		if err != nil {
			p.mu.Lock()
			p.errs = errors.Join(p.errs, client.Close())
			p.mu.Unlock()
			continue
		}
		p.mu.Lock()
		if p.closed {
			p.errs = errors.Join(p.errs, client.Close(), server.Close())
			p.mu.Unlock()
			return
		}
		p.conns = append(p.conns, client, server)
		p.mu.Unlock()
		p.wg.Go(func() {
			if _, err := io.Copy(client, server); err != nil {
				return
			}
		})
		p.wg.Go(func() { forwardUntilRollback(server, client) })
	}
}

// forwardUntilRollback copies client to server until a chunk contains ROLLBACK, then drops what follows.
func forwardUntilRollback(server, client net.Conn) {
	rollback := []byte("ROLLBACK")
	buf := make([]byte, 32*1024)
	forward := true
	for {
		n, err := client.Read(buf)
		if n > 0 && forward {
			if bytes.Contains(bytes.ToUpper(buf[:n]), rollback) {
				forward = false
			} else if _, werr := server.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// Close stops listening, closes every forwarded connection and waits for the forwarding to end.
func (p *Proxy) Close() error {
	errs := p.ln.Close()
	p.mu.Lock()
	p.closed = true
	for _, c := range p.conns {
		if err := c.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = errors.Join(errs, err)
		}
	}
	p.mu.Unlock()
	p.wg.Wait()
	p.mu.Lock()
	errs = errors.Join(errs, p.errs)
	p.mu.Unlock()
	if errs != nil {
		return fmt.Errorf("close proxy: %w", errs)
	}
	return nil
}
