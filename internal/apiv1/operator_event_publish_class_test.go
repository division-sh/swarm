package apiv1

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/google/uuid"
)

func TestEventPublicationClassDependsOnRunCreationNotOptionalReference(t *testing.T) {
	runID := uuid.NewString()
	referenceID := uuid.NewString()
	tests := []struct {
		name          string
		newRun        bool
		referenceID   string
		wantClass     events.EventAdmissionClass
		wantReference bool
	}{
		{"new run root", true, "", events.EventAdmissionRootIngress, false},
		{"existing run without reference", false, "", events.EventAdmissionOperatorInjected, false},
		{"existing run with reference", false, referenceID, events.EventAdmissionOperatorInjected, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event, err := eventPublicationEvent(eventPublicationParams{
				EventID: uuid.NewString(), EventName: "operator.work", RunID: runID, Emitter: "operator",
				Payload: []byte(`{}`), NewRunCreated: test.newRun, SourceEventID: test.referenceID,
			}, time.Now().UTC(), executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			if event.AdmissionClass() != test.wantClass {
				t.Fatalf("class = %q, want %q", event.AdmissionClass(), test.wantClass)
			}
			_, hasReference := event.OperatorReference()
			if hasReference != test.wantReference {
				t.Fatalf("operator reference present = %v, want %v", hasReference, test.wantReference)
			}
		})
	}
}

func TestEventPublicationRootModeComesOnlyFromProcessPosture(t *testing.T) {
	for _, posture := range []executionposture.Posture{executionposture.Live, executionposture.MockOnly} {
		t.Run(string(posture), func(t *testing.T) {
			event, err := eventPublicationEvent(eventPublicationParams{
				EventID: uuid.NewString(), EventName: "operator.work", RunID: uuid.NewString(), Emitter: "operator",
				Payload: []byte(`{}`), NewRunCreated: true,
			}, time.Now().UTC(), posture)
			if err != nil {
				t.Fatal(err)
			}
			if got := event.ExecutionMode(); got != posture.RootMode() {
				t.Fatalf("execution mode = %q, want %q", got, posture.RootMode())
			}
		})
	}
}
