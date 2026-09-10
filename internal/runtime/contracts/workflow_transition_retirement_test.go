package contracts

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCompiledTransitionRetiredOwnershipRejectsPresence(t *testing.T) {
	for _, value := range []string{"[old-edge]", "[]", "null", "{}", "false", "''"} {
		t.Run(value, func(t *testing.T) {
			var node SystemNodeContract
			err := yaml.Unmarshal([]byte("id: worker\nowned_transitions: "+value+"\nevent_handlers: {}\n"), &node)
			if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "owned_transitions") {
				t.Fatalf("retired ownership presence accepted: %v", err)
			}
		})
	}
}

func TestCompiledTransitionJoinOutcomeActionsRemainUnsupported(t *testing.T) {
	for _, outcome := range []string{"on_complete: {action: {id: noop}}", "timeout: {after: 1h, action: {id: noop}}"} {
		t.Run(outcome, func(t *testing.T) {
			var handler SystemNodeEventHandler
			err := yaml.Unmarshal([]byte("join: {stage: waiting, members: {from: entity.ids, by: payload.id}, output: payload.result, "+outcome+"}"), &handler)
			if err == nil || !strings.Contains(err.Error(), "action") {
				t.Fatalf("join outcome action must fail strict decoding: %v", err)
			}
		})
	}
}
