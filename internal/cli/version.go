package cli

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"

	"github.com/gabrosys/plugin-telemetry/internal/version"
)

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version, Go version and platform",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "plugin-telemetry %s %s %s/%s\n",
				version.Version, runtime.Version(), runtime.GOOS, runtime.GOARCH)
			if err != nil {
				return fmt.Errorf("write version: %w", err)
			}

			return nil
		},
	}
}
