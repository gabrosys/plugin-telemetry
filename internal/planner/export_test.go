package planner

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// GatherFactsWith is GatherFacts with the settings the checks read, keyed by check.
func GatherFactsWith(ctx context.Context, conn *pgx.Conn, settings map[string]string) (Facts, error) {
	return gatherFacts(ctx, conn, settings)
}
