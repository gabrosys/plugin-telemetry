package framework

import (
	"context"
	"fmt"
	"time"
)

// WaitUntil returns at t, or ctx's error when ctx ends first.
func WaitUntil(ctx context.Context, t time.Time) error {
	timer := time.NewTimer(time.Until(t))
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait until %s: %w", t.UTC().Format(time.RFC3339Nano), ctx.Err())
	}
}
