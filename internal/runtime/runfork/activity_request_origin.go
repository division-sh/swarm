package runfork

import (
	"fmt"
	"slices"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (p RunForkPlan) WithActivityRequestOrigins(revision int64, origins []semanticview.ActivityRequestOrigin) (RunForkPlan, error) {
	if revision <= 0 || revision != p.historicalRevision {
		return RunForkPlan{}, fmt.Errorf("activity origins require the plan's exact historical revision")
	}
	seen := make(map[string]bool, len(origins))
	for _, origin := range origins {
		if err := origin.Validate(); err != nil {
			return RunForkPlan{}, err
		}
		cut := origin.Cuts[0]
		if cut.RunID != p.SourceRunID || cut.Revision != revision || !slices.Contains(p.historicalEventIDs, cut.RequestID) || seen[cut.RequestID] {
			return RunForkPlan{}, fmt.Errorf("activity origin contradicts the exact plan cut")
		}
		seen[cut.RequestID] = true
	}
	p.activityRequestOrigins = cloneActivityRequestOrigins(origins)
	return p, nil
}

func (p RunForkPlan) ActivityRequestOrigins() []semanticview.ActivityRequestOrigin {
	return cloneActivityRequestOrigins(p.activityRequestOrigins)
}

func cloneActivityRequestOrigins(origins []semanticview.ActivityRequestOrigin) []semanticview.ActivityRequestOrigin {
	out := slices.Clone(origins)
	for i := range out {
		out[i].Cuts = slices.Clone(out[i].Cuts)
	}
	return out
}
