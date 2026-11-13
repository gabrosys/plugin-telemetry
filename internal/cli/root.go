// Package cli builds the plugin-telemetry command tree.
package cli

import (
	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/spf13/cobra"
)

// NewRootCommand returns the plugin-telemetry root command with all
// subcommands attached.
func NewRootCommand() *cobra.Command {
	logFlags := &log.Flags{}
	cmd := &cobra.Command{
		Use:           "plugin-telemetry",
		Short:         "CloudNativePG plugin that exports PostgreSQL metrics over OTLP",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRun: func(cmd *cobra.Command, _ []string) {
			logFlags.ConfigureLogging()
			cmd.SetContext(log.IntoContext(cmd.Context(), log.GetLogger()))
		},
	}
	logFlags.AddFlags(cmd.PersistentFlags())
	cmd.AddCommand(newVersionCommand(), newOperatorCommand(), newInstanceCommand())

	return cmd
}
