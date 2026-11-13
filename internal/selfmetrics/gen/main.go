// Command gen prints the self-metric catalogue as Markdown.
package main

import (
	"fmt"
	"os"

	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
)

func main() {
	if err := selfmetrics.Render(os.Stdout, selfmetrics.Catalogue()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
