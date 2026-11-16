package selectedtest

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/sourceartifact"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type MailboxGateDomain = private.MailboxGateDomain
type MailboxGateEvidence = private.MailboxGateEvidence
type MailboxGateObservation = private.MailboxGateObservation

type MailboxGateFixture struct {
	Observation           MailboxGateObservation
	RequirePublicationRun func(context.Context, string, *sourceartifact.AdmittedSourceArtifact) error
}

// BindMailboxGateFixtureForTest consumes the existing selected construction only
// in the fixture role. Its separate producer never enters the observation ports.
func BindMailboxGateFixtureForTest(original *selected.Owner) (MailboxGateFixture, error) {
	if original == nil {
		return MailboxGateFixture{}, fmt.Errorf("original selected mailbox owner is required")
	}
	deps := original.RuntimeDeps()
	observation, err := private.BindMailboxGateObservationForTest(deps.EventStore)
	if err != nil {
		return MailboxGateFixture{}, err
	}
	owner, ok := deps.EventBusDurable.RunLifecycle.(storetest.RunFixtureStore)
	if !ok {
		return MailboxGateFixture{}, fmt.Errorf("original mailbox run fixture owner is required")
	}
	return MailboxGateFixture{
		Observation: observation,
		RequirePublicationRun: func(ctx context.Context, runID string, artifact *sourceartifact.AdmittedSourceArtifact) error {
			if artifact == nil {
				return fmt.Errorf("mailbox publication requires its actual admitted artifact")
			}
			return storetest.MaterializeRun(ctx, owner, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: artifact})
		},
	}, nil
}
