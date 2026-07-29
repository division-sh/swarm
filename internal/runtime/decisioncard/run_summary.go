package decisioncard

import (
	"fmt"
	"strings"
)

// RunSummary is the decision owner's validated view of unresolved durable
// card, continuation, route, and gate obligations for one run.
type RunSummary struct {
	RunID                 string
	UnresolvedHumanTasks  int
	UnresolvedEffects     int
	UnresolvedRouteEvents int
	OpenGateObligations   int
	MalformedObligations  int
}

func (s RunSummary) Validate() error {
	if strings.TrimSpace(s.RunID) == "" {
		return fmt.Errorf("decision run summary requires run_id")
	}
	for name, count := range map[string]int{
		"unresolved_human_tasks":  s.UnresolvedHumanTasks,
		"unresolved_effects":      s.UnresolvedEffects,
		"unresolved_route_events": s.UnresolvedRouteEvents,
		"open_gate_obligations":   s.OpenGateObligations,
		"malformed_obligations":   s.MalformedObligations,
	} {
		if count < 0 {
			return fmt.Errorf("decision run summary %s cannot be negative", name)
		}
	}
	return nil
}

func (s RunSummary) BlocksCompletion() bool {
	return s.UnresolvedHumanTasks > 0 ||
		s.UnresolvedEffects > 0 ||
		s.UnresolvedRouteEvents > 0 ||
		s.OpenGateObligations > 0 ||
		s.MalformedObligations > 0
}
