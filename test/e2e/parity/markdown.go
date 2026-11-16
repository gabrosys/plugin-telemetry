package parity

import (
	"fmt"
	"strings"
)

const timeLayout = "2006-01-02T15:04:05.000Z07:00"

// Markdown renders checks under title as a table: series, rule, gated, CNPG before (value at time), ours,
// CNPG after, margin, verdict and detail.
func Markdown(title string, checks []Check) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n", title)
	fmt.Fprintf(&b, "Ours passes when CNPG's sample before it is at least %s older, its sample after it at least %s newer, "+
		"and our value lies between them (NaN equals NaN; histograms by count and sum). "+
		"A gated check that does not pass fails the run.\n\n", Margin, Margin)
	b.WriteString("| Series | Rule | Gated | CNPG before | Ours | CNPG after | Margin | Verdict | Detail |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|\n")
	for _, c := range checks {
		gated := "no"
		if c.Gated {
			gated = "yes"
		}
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s | %s | %s | %s | %s | %s |\n", cell(c.Series), cell(c.Rule), gated,
			sample(c.Before), sample(c.Ours), sample(c.After), Margin, c.Verdict, cell(c.Detail))
	}

	return b.String()
}

func sample(s *Series) string {
	if s == nil {
		return "absent"
	}
	at := s.At.UTC().Format(timeLayout)
	if s.Kind == KindHistogram || s.Kind == KindSummary {
		return fmt.Sprintf("count %s, sum %s at %s", number(s.Value), number(s.Sum), at)
	}

	return number(s.Value) + " at " + at
}

// cell escapes s for a table cell.
func cell(s string) string {
	return strings.NewReplacer("|", `\|`, "\n", " ").Replace(s)
}
