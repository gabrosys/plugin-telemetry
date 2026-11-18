package fixture_test

import (
	"errors"
	"os"
	"runtime"
	"testing"

	"github.com/gabrosys/plugin-telemetry/test/fixture"
)

// fatalTB keeps its cleanups and records Errorf and Fatalf; Fatalf stops the calling goroutine as testing.T does.
type fatalTB struct {
	testing.TB
	fatal    string
	errs     []string
	cleanups []func()
}

func (f *fatalTB) Helper()           {}
func (f *fatalTB) Cleanup(fn func()) { f.cleanups = append(f.cleanups, fn) }

func (f *fatalTB) Errorf(format string, _ ...any) { f.errs = append(f.errs, format) }

func (f *fatalTB) Fatalf(format string, _ ...any) {
	f.fatal = format
	runtime.Goexit()
}

// fatalOf runs fn on a fatalTB in its own goroutine, then its cleanups in reverse order, and returns the Fatalf
// format, or "".
func fatalOf(fn func(tb testing.TB)) string {
	tb := &fatalTB{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(tb)
	}()
	<-done
	for i := len(tb.cleanups) - 1; i >= 0; i-- {
		tb.cleanups[i]()
	}
	return tb.fatal
}

func TestOpenClosesTheRootAtCleanup(t *testing.T) {
	var root *os.Root
	t.Run("open", func(t *testing.T) {
		root = fixture.Open(t, "testdata")
		if _, err := root.ReadFile("hello.txt"); err != nil {
			t.Fatalf("read through the open root: %v", err)
		}
	})

	if _, err := root.ReadFile("hello.txt"); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("read after cleanup: got %v, want os.ErrClosed", err)
	}
}

func TestOpenFailsOnAMissingDir(t *testing.T) {
	if fatalOf(func(tb testing.TB) { fixture.Open(tb, "absent") }) == "" {
		t.Fatal("Open of a missing dir did not call Fatalf")
	}
}

func TestReadReturnsTheFile(t *testing.T) {
	if got := string(fixture.Read(t, "testdata", "hello.txt")); got != "hello\n" {
		t.Fatalf("got %q, want %q", got, "hello\n")
	}
}

func TestReadFailsOnAMissingFile(t *testing.T) {
	if fatalOf(func(tb testing.TB) { fixture.Read(tb, "testdata", "absent.txt") }) == "" {
		t.Fatal("Read of a missing file did not call Fatalf")
	}
}

func TestReadStaysInsideDir(t *testing.T) {
	if fatalOf(func(tb testing.TB) { fixture.Read(tb, "testdata", "../fixture_test.go") }) == "" {
		t.Fatal("Read of a path outside dir did not call Fatalf")
	}
}
