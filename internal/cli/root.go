// Package cli builds the plugin-telemetry command tree.
package cli

import "github.com/spf13/cobra"

// NewRootCommand returns the plugin-telemetry root command with all
// subcommands attached.
func NewRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "plugin-telemetry",
		Short:         "CloudNativePG plugin that exports PostgreSQL metrics over OTLP",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	cmd.AddCommand(newVersionCommand())

	return cmd
}
