package engine_test

import (
	"context"
	"strings"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/planner"
)

const alphaBeta = `alpha:
  query: SELECT 1 AS value
  metrics:
    - value:
        usage: GAUGE
        description: Alpha.
beta:
  query: SELECT 2 AS value
  metrics:
    - value:
        usage: GAUGE
        description: Beta.
`

const gamma = `gamma:
  query: SELECT 3 AS value
  metrics:
    - value:
        usage: GAUGE
        description: Gamma.
`

var _ = Describe("Planning", func() {
	plans := func(h *harness) func() int {
		return func() int { return h.logged(`"msg"="plan updated"`) }
	}

	It("plans nothing before the first facts", func() {
		release := make(chan struct{})
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) },
			func(ctx context.Context, round int) (planner.Facts, error) {
				if round == 0 {
					select {
					case <-release:
					case <-ctx.Done():
						return planner.Facts{}, ctx.Err()
					}
				}
				return memPrimary, nil
			})
		h.advance(func() bool { return h.logged("configuration applied") == 1 && len(h.requests()) >= 2 })
		Expect(plans(h)()).To(BeZero())

		close(release)
		h.advance(func() bool { return plans(h)() == 1 })
		Expect(h.logged(`"msg"="plan updated" "jobs"=4`)).To(Equal(1))
	})

	It("replans when the sources or the settings change, and not when they stay", func() {
		h := run(func(url string) *memConfig { return newMemConfig(httpSettings(url), httpFiles()) }, primary)
		h.advance(func() bool { return plans(h)() == 1 })

		h.reload()
		h.cfg.set(func() { h.cfg.doc += "" })
		h.reload()
		Expect(plans(h)()).To(Equal(1))

		h.cfg.set(func() { h.cfg.files["/mem/queries.yaml"] = alphaBeta + gamma })
		h.reload()
		Expect(plans(h)()).To(Equal(2))
		h.advance(h.has("cnpg_otel_query_interval_seconds", map[string]string{"query": "gamma"}, 30))

		h.cfg.set(func() { h.cfg.doc = strings.Replace(h.cfg.doc, "defaultInterval: 30s", "defaultInterval: 45s", 1) })
		h.reload()
		Expect(plans(h)()).To(Equal(3))
		h.advance(h.has("cnpg_otel_query_interval_seconds", map[string]string{"query": "alpha"}, 45))
	})

	It("replans when the facts change and not when they repeat", func() {
		var standby atomic.Bool
		h := run(func(url string) *memConfig { return newMemConfig(fastSettings(url), httpFiles()) },
			func(context.Context, int) (planner.Facts, error) {
				f := memPrimary
				f.InRecovery = standby.Load()
				return f, nil
			})
		h.advance(func() bool { return len(h.roundTimes()) >= 3 })
		Expect(plans(h)()).To(Equal(1))

		standby.Store(true)
		h.advance(func() bool { return plans(h)() == 2 })
		n := len(h.roundTimes())
		h.advance(func() bool { return len(h.roundTimes()) >= n+2 })
		Expect(plans(h)()).To(Equal(2))
	})
})
