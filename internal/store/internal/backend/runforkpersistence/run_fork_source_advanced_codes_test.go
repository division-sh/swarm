package runforkpersistence

import (
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func TestRunForkSourceAdvancedCodesConsumeCanonicalFamilies(t *testing.T) {
	expected := map[runforkrevision.Family]string{
		runforkrevision.FamilyEvents:                  "source_events_advanced_after_fork_point",
		runforkrevision.FamilyEntityMutations:         "source_mutations_advanced_after_fork_point",
		runforkrevision.FamilyEntityMetadata:          "source_current_state_advanced_after_fork_point",
		runforkrevision.FamilyEventDeliveries:         "source_deliveries_advanced_after_fork_point",
		runforkrevision.FamilyCommittedReplayScopes:   "source_committed_replay_scope_advanced_after_fork_point",
		runforkrevision.FamilyEventReceipts:           "source_receipts_advanced_after_fork_point",
		runforkrevision.FamilyDeadLetters:             "source_dead_letters_advanced_after_fork_point",
		runforkrevision.FamilyTimers:                  "source_timers_advanced_after_fork_point",
		runforkrevision.FamilyAgentSessions:           "source_sessions_advanced_after_fork_point",
		runforkrevision.FamilyAgentTurns:              "source_turns_advanced_after_fork_point",
		runforkrevision.FamilyAgentConversationAudits: "source_conversation_audits_advanced_after_fork_point",
		runforkrevision.FamilyReplyContexts:           "source_reply_contexts_advanced_after_fork_point",
		runforkrevision.FamilyFanOutObligations:       "source_fan_out_obligations_advanced_after_fork_point",
	}
	seen := make(map[runforkrevision.Family]bool)
	for _, family := range runforkrevision.AllFamilies() {
		if seen[family] {
			t.Fatalf("canonical family %q appears twice", family)
		}
		seen[family] = true
		t.Run(string(family), func(t *testing.T) {
			want, exists := expected[family]
			if !exists {
				t.Fatalf("new canonical family %q needs an explicit source-advancement classification", family)
			}
			got, ok := runForkSourceAdvancedCode(string(family))
			if !ok || got == "" || got != want {
				t.Fatalf("canonical family %q classification = %q/%t, want %q/true", family, got, ok, want)
			}
		})
	}
	for family := range expected {
		if !seen[family] {
			t.Fatalf("retired/noncanonical family %q retains a classification expectation", family)
		}
	}
}

func TestRunForkSourceAdvancedCodesRejectUnknownFamilies(t *testing.T) {
	for _, family := range []string{"", " ", "unknown_family", "fan_out", "Fan_Out_Obligations", "fan_out_obligations.extra"} {
		t.Run(family, func(t *testing.T) {
			if code, ok := runForkSourceAdvancedCode(family); ok || code != "" {
				t.Fatalf("unknown family %q acquired classification %q/%t", family, code, ok)
			}
		})
	}
}
