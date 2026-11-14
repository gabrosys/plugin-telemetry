package executor

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// SetCancel replaces the function that sends the cancel request at the row cap.
func (e *Executor) SetCancel(f func(context.Context, *pgx.Conn) error) { e.cancel = f }
