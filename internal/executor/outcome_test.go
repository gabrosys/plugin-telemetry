package executor_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/utils/clock"

	"github.com/gabrosys/plugin-telemetry/internal/executor"
	"github.com/gabrosys/plugin-telemetry/internal/pg"
	"github.com/gabrosys/plugin-telemetry/internal/planner"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
)

var _ = Describe("Classify", func() {
	DescribeTable("maps an error to an outcome",
		func(err error, want selfmetrics.Outcome) {
			Expect(executor.Classify(err)).To(Equal(want))
		},
		Entry("statement_timeout, SQLSTATE 57014", &pgconn.PgError{Code: "57014"}, selfmetrics.OutcomeTimeout),
		Entry("lock_timeout, SQLSTATE 55P03", &pgconn.PgError{Code: "55P03"}, selfmetrics.OutcomeTimeout),
		Entry("wrapped SQLSTATE 57014", fmt.Errorf("query: %w", &pgconn.PgError{Code: "57014"}), selfmetrics.OutcomeTimeout),
		Entry("context deadline", context.DeadlineExceeded, selfmetrics.OutcomeTimeout),
		Entry("wrapped context deadline", fmt.Errorf("query: %w", context.DeadlineExceeded), selfmetrics.OutcomeTimeout),
		Entry("insufficient privilege, SQLSTATE 42501", &pgconn.PgError{Code: "42501"}, selfmetrics.OutcomeDenied),
		Entry("wrapped SQLSTATE 42501", fmt.Errorf("run query: %w", &pgconn.PgError{Code: "42501"}), selfmetrics.OutcomeDenied),
		Entry("cancelled context", context.Canceled, selfmetrics.OutcomeError),
		Entry("a plain error", errors.New("boom"), selfmetrics.OutcomeError),
		Entry("a closed pool", fmt.Errorf("acquire: %w", pg.ErrPoolClosed), selfmetrics.OutcomeSkipped),
	)
})

var _ = Describe("Run", func() {
	It("counts a run on a closed pool as skipped, with no lost connection", func(ctx SpecContext) {
		limits := settings.Limits{QueryTimeout: time.Second, LockTimeout: time.Second}
		clk := clock.RealClock{}
		pool := pg.NewPool(pg.Config{Host: "127.0.0.1", Port: 1, User: "u", QueryTimeout: time.Second, LockTimeout: time.Second},
			1, time.Minute, clk)
		pool.Close()

		res := executor.New(pool, limits, func() *semver.Version { return nil }, clk).
			Run(ctx, planner.Job{Query: "q", Database: "app", Fingerprint: "f"})
		Expect(res.Outcome).To(Equal(selfmetrics.OutcomeSkipped))
		Expect(res.Err).To(MatchError(pg.ErrPoolClosed))
		Expect(res.ConnectionLost).To(BeFalse())
	})
})
