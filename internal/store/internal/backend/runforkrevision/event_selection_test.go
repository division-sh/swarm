package runforkrevision

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
)

func TestEventHistoryExcludesOnlyRuntimeLog(t *testing.T) {
	for _, name := range []string{
		"business.created", "platform.run_started", "platform.inbound_recorded",
		"platform.agent_directive", "platform.pipeline", "platform.runtime_log.extra",
		"Platform.runtime_log", "application.runtime_log",
	} {
		if !CapturesEvent(name) {
			t.Errorf("non-runtime-log event excluded from fork history: %q", name)
		}
	}
	if CapturesEvent(string(events.EventTypePlatformRuntimeLog)) {
		t.Fatal("runtime log selected as fork evidence")
	}
	spec, ok := canonicalProjectionSpec(FamilyEvents)
	if !ok || spec.source != "events e" || !strings.Contains(spec.query, "e.event_name <> '"+excludedEventName+"'") {
		t.Fatal("broad and exact event projections do not consume the canonical exclusion")
	}
}
