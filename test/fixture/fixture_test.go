package fixture_test

import (
	"errors"
	"os"
	"runtime"
	"testing"

	"github.com/gabrosys/plugin-telemetry/test/fixture"
)

// fatalTB is t with a Fatalf that records its format and stops the calling goroutine, as testing.T does.
type fatalTB struct {
	*testing.T
	fatal string
}

func (f *fatalTB) Fatalf(format string, _ ...any) {
	f.fatal = format
	runtime.Goexit()
}

// fatalOf runs fn on a fatalTB over t in its own goroutine and returns the Fatalf format, or "".
func fatalOf(t *testing.T, fn func(tb testing.TB)) string {
	tb := &fatalTB{T: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(tb)
	}()
	<-done
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
	if fatalOf(t, func(tb testing.TB) { fixture.Open(tb, "absent") }) == "" {
		t.Fatal("Open of a missing dir did not call Fatalf")
	}
}

func TestReadReturnsTheFile(t *testing.T) {
	if got := string(fixture.Read(t, "testdata", "hello.txt")); got != "hello\n" {
		t.Fatalf("got %q, want %q", got, "hello\n")
	}
}

func TestReadFailsOnAMissingFile(t *testing.T) {
	if fatalOf(t, func(tb testing.TB) { fixture.Read(tb, "testdata", "absent.txt") }) == "" {
		t.Fatal("Read of a missing file did not call Fatalf")
	}
}

func TestReadStaysInsideDir(t *testing.T) {
	if fatalOf(t, func(tb testing.TB) { fixture.Read(tb, "testdata", "../fixture_test.go") }) == "" {
		t.Fatal("Read of a path outside dir did not call Fatalf")
	}
}

func TestGoldenWritesAndReturnsGotWithUpdateGolden(t *testing.T) {
	t.Setenv("UPDATE_GOLDEN", "1")
	dir := t.TempDir()

	if got := string(fixture.Golden(t, dir, "g.golden", []byte("new\n"))); got != "new\n" {
		t.Fatalf("got %q, want %q", got, "new\n")
	}
	if got := string(fixture.Read(t, dir, "g.golden")); got != "new\n" {
		t.Fatalf("file holds %q, want %q", got, "new\n")
	}
}

func TestGoldenReturnsTheFileWithoutUpdateGolden(t *testing.T) {
	t.Setenv("UPDATE_GOLDEN", "")
	if got := string(fixture.Golden(t, "testdata", "hello.txt", []byte("other\n"))); got != "hello\n" {
		t.Fatalf("got %q, want %q", got, "hello\n")
	}
}

func TestGoldenFailsOnAMissingFile(t *testing.T) {
	t.Setenv("UPDATE_GOLDEN", "")
	if fatalOf(t, func(tb testing.TB) { fixture.Golden(tb, "testdata", "absent.golden", nil) }) == "" {
		t.Fatal("Golden of a missing file did not call Fatalf")
	}
}
