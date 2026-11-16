package engine_test

import (
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/selfmetrics"
)

var _ = Describe("errorLog", func() {
	var (
		lines  []string
		logger logr.Logger
		l      *engine.ErrorLog
	)

	BeforeEach(func() {
		lines = nil
		logger = funcr.New(func(_, args string) { lines = append(lines, args) }, funcr.Options{})
		l = &engine.ErrorLog{}
	})

	It("logs ten identical failures once", func() {
		for range 10 {
			l.Failed(logger, "backends", "app", selfmetrics.OutcomeError, errors.New("connection refused"))
		}
		Expect(lines).To(HaveLen(1))
		Expect(lines[0]).To(And(
			ContainSubstring(`"error"="connection refused"`),
			ContainSubstring(`"query"="backends"`),
			ContainSubstring(`"database"="app"`),
		))
	})

	It("logs ten failures whose messages differ by an ephemeral port once", func() {
		for i := range 10 {
			l.Failed(logger, "backends", "app", selfmetrics.OutcomeError, fmt.Errorf("dial tcp 10.0.0.1:%d: connection refused", 40000+i))
		}
		Expect(lines).To(HaveLen(1))
	})

	It("logs a new line when the outcome changes", func() {
		err := errors.New("boom")
		l.Failed(logger, "backends", "app", selfmetrics.OutcomeError, err)
		l.Failed(logger, "backends", "app", selfmetrics.OutcomeTimeout, err)
		l.Failed(logger, "backends", "app", selfmetrics.OutcomeError, err)
		l.Failed(logger, "backends", "app", selfmetrics.OutcomeTimeout, err)
		Expect(lines).To(HaveLen(2))

		l.Succeeded(logger, "backends", "app")
		Expect(lines).To(HaveLen(3), "one recovery for every outcome")
		l.Failed(logger, "backends", "app", selfmetrics.OutcomeError, err)
		Expect(lines).To(HaveLen(4))
	})

	It("logs one recovery line, and the next failure again", func() {
		l.Failed(logger, "backends", "app", selfmetrics.OutcomeError, errors.New("connection refused"))
		l.Succeeded(logger, "backends", "app")
		l.Succeeded(logger, "backends", "app")
		Expect(lines).To(HaveLen(2))
		Expect(lines[1]).To(And(ContainSubstring(`"query"="backends"`), ContainSubstring(`"database"="app"`)))
		Expect(lines[1]).NotTo(ContainSubstring(`"error"`))

		l.Failed(logger, "backends", "app", selfmetrics.OutcomeError, errors.New("connection refused"))
		Expect(lines).To(HaveLen(3))
	})

	It("logs nothing for a success without a failure", func() {
		l.Succeeded(logger, "backends", "app")
		Expect(lines).To(BeEmpty())
	})

	It("keeps each query and database apart", func() {
		err := errors.New("connection refused")
		l.Failed(logger, "backends", "app", selfmetrics.OutcomeError, err)
		l.Failed(logger, "backends", "postgres", selfmetrics.OutcomeError, err)
		l.Failed(logger, "pg_database", "app", selfmetrics.OutcomeError, err)
		Expect(lines).To(HaveLen(3))

		l.Succeeded(logger, "backends", "postgres")
		Expect(lines).To(HaveLen(4))
		Expect(lines[3]).To(ContainSubstring(`"database"="postgres"`))
		l.Failed(logger, "backends", "app", selfmetrics.OutcomeError, err)
		Expect(lines).To(HaveLen(4))
	})
})
