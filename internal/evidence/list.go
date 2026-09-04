package evidence

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ScenarioView is one registry row plus what this host can currently offer it.
type ScenarioView struct {
	Scenario
	// MissingEnv names the variables the owner needs that are not set here.
	MissingEnv []string `json:"missing_env,omitempty"`
	// Evidence is the outcome this repository can currently claim for the row.
	// It is never `passed`: no row is wired into the aggregate runner yet, and a
	// recorded outcome from a document is provenance, not evidence re-earned
	// against this candidate.
	Evidence Status `json:"evidence"`
}

// View resolves the registry against an environment without reading a value.
func View(lookup Lookup) []ScenarioView {
	out := make([]ScenarioView, 0, len(scenarios))
	for _, s := range Scenarios() {
		v := ScenarioView{Scenario: s, MissingEnv: missingEnv(s.Env, lookup), Evidence: StatusUnavailable}
		if s.Wired() {
			v.Evidence = ""
		}
		out = append(out, v)
	}
	return out
}

// RenderListJSON is the machine-readable enumeration behind `evidence list`.
func RenderListJSON(views []ScenarioView) ([]byte, error) {
	data, err := json.MarshalIndent(views, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

// RenderList is the human enumeration. It leads with the count of required
// rows that have no proof, because that number is the point of the command.
func RenderList(views []ScenarioView) string {
	var b strings.Builder
	unowned, requiredUnowned := 0, 0
	for _, v := range views {
		if !v.Owned() {
			unowned++
			if v.Required {
				requiredUnowned++
			}
		}
	}
	fmt.Fprintf(&b, "%d of %d registered acceptance scenarios shown; %d of them have no owning proof (%d of those are required).\n",
		len(views), len(scenarios), unowned, requiredUnowned)
	b.WriteString("No scenario is wired into the aggregate runner yet, so no row below is evidence for this candidate.\n\n")
	fmt.Fprintf(&b, "%-5s %-13s %-9s %-11s %s\n", "ID", "LAYER", "REQUIRED", "EVIDENCE", "OWNER / OPEN")
	for _, v := range views {
		owner := v.Owner
		if owner == "" {
			owner = "OPEN: " + v.Open
		}
		if len(v.MissingEnv) > 0 {
			owner += "  [missing env: " + strings.Join(v.MissingEnv, ", ") + "]"
		}
		fmt.Fprintf(&b, "%-5s %-13s %-9s %-11s %s\n", v.ID, v.Layer, requiredWord(v.Required), orDash(string(v.Evidence)), owner)
	}
	b.WriteString("\nLast recorded outcomes, from the source documents rather than from this run:\n")
	for _, v := range views {
		if v.Recorded == "" {
			continue
		}
		note := ""
		if v.Note != "" {
			note = " — " + v.Note
		}
		fmt.Fprintf(&b, "  %-5s %-12s %s%s\n", v.ID, v.Recorded, v.Source, note)
	}
	return b.String()
}
