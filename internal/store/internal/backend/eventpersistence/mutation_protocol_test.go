package eventpersistence

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func TestComposedEventWritersRequireMutationAttempt(t *testing.T) {
	ctx := context.Background()
	checks := map[string]func() error{
		"publication": func() error {
			_, err := commitPublicationTx(ctx, nil, nil, runtimebus.PublicationCommand{})
			return err
		},
		"fan-out publication": func() error {
			_, err := commitFanOutPublicationTx(ctx, nil, nil, runtimebus.PublicationCommand{}, fanoutobligation.OrdinalEmission{})
			return err
		},
		"directive": func() error {
			_, err := (&EventPostgresOwner{}).CommitDirectiveEventTx(ctx, nil, events.AdmittedEvent{})
			return err
		},
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) {
			if err := check(); err == nil || !strings.Contains(err.Error(), "attempt") {
				t.Fatalf("missing mutation attempt was not rejected: %v", err)
			}
		})
	}
}
