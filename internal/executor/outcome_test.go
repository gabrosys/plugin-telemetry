package executor_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/executor"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
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
		Entry("insufficient privilege, SQLSTATE 42501", &pgconn.PgError{Code: "42501"}, selfmetrics.OutcomeError),
		Entry("cancelled context", context.Canceled, selfmetrics.OutcomeError),
		Entry("a plain error", errors.New("boom"), selfmetrics.OutcomeError),
	)
})
