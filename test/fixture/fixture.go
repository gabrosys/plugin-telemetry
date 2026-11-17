// Package fixture reads test fixtures through an os.Root, so a name can't reach outside its directory.
package fixture

import (
	"os"
	"testing"
)

// Open opens dir as an os.Root that closes when the test ends. It fails the test if dir can't be opened.
func Open(tb testing.TB, dir string) *os.Root {
	tb.Helper()

	root, err := os.OpenRoot(dir)
	if err != nil {
		tb.Fatalf("open %s: %v", dir, err)
	}
	tb.Cleanup(func() {
		if err := root.Close(); err != nil {
			tb.Errorf("close %s: %v", dir, err)
		}
	})

	return root
}

// Read returns the file name inside dir. It fails the test if the file can't be read.
func Read(tb testing.TB, dir, name string) []byte {
	tb.Helper()

	data, err := Open(tb, dir).ReadFile(name)
	if err != nil {
		tb.Fatalf("read %s: %v", name, err)
	}

	return data
}
