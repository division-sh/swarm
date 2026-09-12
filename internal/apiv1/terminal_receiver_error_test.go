package apiv1

import (
	"errors"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestPublicationTerminalReceiverErrorPreservesTypedCause(t *testing.T) {
	params := eventPublicationParams{EventName: "loop.repeat", EventID: "event", RunID: "run"}
	for _, mapError := range []struct {
		name     string
		mapError func(eventPublicationParams, error) error
	}{{"event.publish", eventPublishPublishError}, {"run.start", runStartEventPublishError}} {
		t.Run(mapError.name, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				err      error
				terminal bool
			}{
				{"terminal", &pipeline.TerminalReceiverError{FlowID: "child", Stage: "done"}, true},
				{"wrapped", fmt.Errorf("planning: %w", &pipeline.TerminalReceiverError{FlowID: "child", Stage: "done"}), true},
				{"identical text is not authority", errors.New((&pipeline.TerminalReceiverError{FlowID: "child", Stage: "done"}).Error()), false},
				{"storage", errors.New("database unavailable"), false},
				{"inactive", errors.New("receiver target owner is unavailable: lifecycle is not active"), false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var app *ApplicationError
					if err := mapError.mapError(params, tc.err); !errors.As(err, &app) || app.Code != EventPublishFailedCode || app.Retryable == tc.terminal {
						t.Fatalf("publication error=%#v err=%v", app, err)
					}
					details := app.Details.(map[string]any)
					if tc.terminal && (details["reason"] != "receiver_entity_terminal" || details["flow_id"] != "child" || details["stage"] != "done") {
						t.Fatalf("terminal details=%#v", details)
					}
				})
			}
		})
	}
}
