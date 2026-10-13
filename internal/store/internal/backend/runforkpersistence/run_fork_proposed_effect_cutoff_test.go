package runforkpersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestRunForkProposedEffectCutoffDistinguishesCreationFromEvent(t *testing.T) {
	birth := time.Date(2026, 10, 13, 0, 0, 0, 123456000, time.UTC)
	event := birth.Add(time.Hour)
	for _, tc := range []struct {
		name string
		plan runfork.RunForkPlan
		want time.Time
	}{
		{"event", runfork.RunForkPlan{ForkPoint: runfork.RunForkPoint{Timestamp: event}}, event},
		{"event_ignores_birth", runfork.RunForkPlan{ForkPoint: runfork.RunForkPoint{Timestamp: event}, SourceRunStartedAt: &birth}, event},
		{"start", runfork.RunForkPlan{ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 1}, SourceRunStartedAt: &birth}, birth},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := runForkProposedEffectCutoff(tc.plan)
			if err != nil || !got.Equal(tc.want) {
				t.Fatalf("cutoff=%s want=%s error=%v", got, tc.want, err)
			}
			if tc.name == "start" && !birth.Add(time.Microsecond).After(got) {
				t.Fatal("start cutoff includes cards created after the original birth")
			}
		})
	}
}

func TestRunForkProposedEffectStartCutRequiresCreationEvidence(t *testing.T) {
	zero := time.Time{}
	birth := time.Now().UTC()
	for _, tc := range []struct {
		name  string
		birth *time.Time
		point runfork.RunForkPoint
	}{
		{"missing", nil, runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 1}},
		{"zero", &zero, runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 1}},
		{"event_timestamp", &birth, runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 1, Timestamp: birth}},
		{"missing_revision", &birth, runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := runForkProposedEffectCutoff(runfork.RunForkPlan{ForkPoint: tc.point, SourceRunStartedAt: tc.birth}); err == nil {
				t.Fatal("invalid creation-cut evidence was accepted")
			}
		})
	}
}
