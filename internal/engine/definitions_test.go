package engine_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/engine"
	"github.com/gabrosys/plugin-telemetry/internal/queries"
	"github.com/gabrosys/plugin-telemetry/internal/settings"
	"github.com/gabrosys/plugin-telemetry/internal/sources"
)

// secretConfig marks the sources under secret/ as sensitive.
type secretConfig struct{ engine.Config }

func (secretConfig) Sensitive(name string) bool { return len(name) > 7 && name[:7] == "secret/" }

var _ = Describe("Definitions", func() {
	It("quotes no value of a source the Config marks sensitive", func() {
		q := queries.UserQuery{Query: "SELECT 1", RunOnServer: "nope!"}
		res := sources.Result{Entries: []sources.Entry{
			{Key: "a", Source: "secret/a", Query: q},
			{Key: "b", Source: "/file/b", Query: q},
		}}

		defs := engine.Definitions(secretConfig{Config: engine.FileConfig("/x")}, settings.Defaults(), res)

		Expect(defs).To(HaveLen(2))
		Expect(defs[0].Disabled).ToNot(ContainSubstring("nope!"))
		Expect(defs[1].Disabled).To(ContainSubstring("nope!"))
	})
})
