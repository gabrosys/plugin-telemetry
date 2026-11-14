package engine_test

import (
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
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
			l.Failed(logger, "backends", "app", errors.New("connection refused"))
		}
		Expect(lines).To(HaveLen(1))
		Expect(lines[0]).To(And(
			ContainSubstring(`"error"="connection refused"`),
			ContainSubstring(`"query"="backends"`),
			ContainSubstring(`"database"="app"`),
		))
	})

	It("logs a new line when the message changes, once per distinct message", func() {
		for _, msg := range []string{"a", "a", "b", "b", "a", "b"} {
			l.Failed(logger, "backends", "app", errors.New(msg))
		}
		Expect(lines).To(HaveLen(2))
		Expect(lines[0]).To(ContainSubstring(`"error"="a"`))
		Expect(lines[1]).To(ContainSubstring(`"error"="b"`))
	})

	It("remembers a bounded number of messages", func() {
		for i := range engine.MaxMessages + 1 {
			l.Failed(logger, "backends", "app", fmt.Errorf("attempt %d", i))
		}
		Expect(lines).To(HaveLen(engine.MaxMessages + 1))
		l.Failed(logger, "backends", "app", errors.New("attempt 0"))
		Expect(lines).To(HaveLen(engine.MaxMessages + 2))
	})

	It("logs one recovery line, and the next failure again", func() {
		l.Failed(logger, "backends", "app", errors.New("connection refused"))
		l.Succeeded(logger, "backends", "app")
		l.Succeeded(logger, "backends", "app")
		Expect(lines).To(HaveLen(2))
		Expect(lines[1]).To(And(ContainSubstring(`"query"="backends"`), ContainSubstring(`"database"="app"`)))
		Expect(lines[1]).NotTo(ContainSubstring(`"error"`))

		l.Failed(logger, "backends", "app", errors.New("connection refused"))
		Expect(lines).To(HaveLen(3))
	})

	It("logs nothing for a success without a failure", func() {
		l.Succeeded(logger, "backends", "app")
		Expect(lines).To(BeEmpty())
	})

	It("keeps each query and database apart", func() {
		err := errors.New("connection refused")
		l.Failed(logger, "backends", "app", err)
		l.Failed(logger, "backends", "postgres", err)
		l.Failed(logger, "pg_database", "app", err)
		Expect(lines).To(HaveLen(3))

		l.Succeeded(logger, "backends", "postgres")
		Expect(lines).To(HaveLen(4))
		Expect(lines[3]).To(ContainSubstring(`"database"="postgres"`))
		l.Failed(logger, "backends", "app", err)
		Expect(lines).To(HaveLen(4))
	})
})
