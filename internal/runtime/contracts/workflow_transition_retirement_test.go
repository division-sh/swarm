package contracts

import (
	"strings"
	"testing"
)

func TestCompiledTransitionJoinOutcomeActionsRemainUnsupported(t *testing.T) {
	for _, outcome := range []string{"on_complete: {action: {id: noop}}", "on_deadline: {action: {id: noop}}"} {
		t.Run(outcome, func(t *testing.T) {
			var handler SystemNodeEventHandler
			err := decodeNodeTestYAML([]byte("join: {stage: waiting, members: {from: entity.ids, by: payload.id}, output: payload.result, "+outcome+"}"), &handler)
			if err == nil || !strings.Contains(err.Error(), "action") {
				t.Fatalf("join outcome action must fail strict decoding: %v", err)
			}
		})
	}
}
