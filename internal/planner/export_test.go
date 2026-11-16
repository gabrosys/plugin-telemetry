package planner

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// GatherFactsWith is GatherFacts with the settings the checks read, keyed by check.
func GatherFactsWith(ctx context.Context, conn *pgx.Conn, guard string, settings map[string]string) (Facts, error) {
	return gatherFacts(ctx, conn, guard, settings)
}
