package runtimepersistence

import (
	"context"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	"github.com/google/uuid"
)

// ConversationForkSourceFixture is the fixed historical source used by the
// served fork/snapshot proof. It is not a runtime history-import capability.
type ConversationForkSourceFixture struct {
	Identity                                       agentidentity.Identity
	RunID, SessionID, EntityID, Event1ID, Event2ID string
	CreatedAt, Turn1At, Turn2At                    time.Time
	Construction                                   ConversationForkConstruction
	kind                                           conversationForkFixtureKind
}

type ConversationForkSourceResult = runforkpersistence.ConversationForkSourceResult

type conversationForkFixtureKind = runforkpersistence.ConversationForkFixtureKind

const (
	conversationForkServedSource = runforkpersistence.ConversationForkServedSource
	conversationForkNativeChat   = runforkpersistence.ConversationForkNativeChat
	conversationForkDataOnlyChat = runforkpersistence.ConversationForkDataOnlyChat
)

func SeedConversationForkSourceForTest(ctx context.Context, selected any, fixture ConversationForkSourceFixture) (ConversationForkSourceResult, error) {
	var result ConversationForkSourceResult
	bundleHash, err := validateConversationForkFixture(ctx, fixture)
	if err != nil {
		return result, err
	}
	ctx = runtimecorrelation.WithRunID(ctx, fixture.RunID)
	evidence, err := conversationForkConstructionEvidence(fixture, bundleHash)
	if err != nil {
		return result, err
	}
	native := runforkpersistence.ConversationForkSourceFixture{
		Identity: fixture.Identity, RunID: fixture.RunID, SessionID: fixture.SessionID, EntityID: fixture.EntityID, Event1ID: fixture.Event1ID, Event2ID: fixture.Event2ID,
		CreatedAt: fixture.CreatedAt, Turn1At: fixture.Turn1At, Turn2At: fixture.Turn2At, Construction: fixture.Construction, Kind: fixture.kind,
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil {
			return result, fmt.Errorf("postgres fixture store is required")
		}
		return owner.runForkPostgresOwner.SeedConversationForkSourceForTest(ctx, native, bundleHash, evidence)
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil {
			return result, fmt.Errorf("sqlite fixture store is required")
		}
		return owner.runForkSQLiteOwner.SeedConversationForkSourceForTest(ctx, native, bundleHash, evidence)
	default:
		return result, fmt.Errorf("fork fixture store %T is unsupported", selected)
	}
}

func conversationForkConstructionEvidence(fixture ConversationForkSourceFixture, bundleHash string) (mutationprotocol.Evidence, error) {
	if fixture.kind == conversationForkDataOnlyChat {
		if fixture.Construction.Child.Identity.InstancePath != "" || fixture.Construction.Parent != nil {
			return 0, fmt.Errorf("data-only conversation fixture cannot carry construction")
		}
		return mutationprotocol.RevisionOnly, nil
	}
	record, err := fixture.Construction.Child.PersistenceRecord()
	if err != nil {
		return 0, fmt.Errorf("conversation fixture requires complete construction: %w", err)
	}
	if record.Identity.RunID != fixture.RunID || record.EntityID != fixture.EntityID ||
		!record.CreatedAt.Equal(fixture.Turn1At.Add(-30*time.Second).UTC().Truncate(time.Microsecond)) || fixture.Construction.Child.Readiness.BundleHash != bundleHash {
		return 0, fmt.Errorf("conversation construction does not match fixed historical source/identity/time")
	}
	input := fixture.Construction.CreatingEvent
	if input.ID() == "" || input.ID() != fixture.Construction.Child.CreatingInput.EventID || input.RunID() != fixture.RunID || !input.CreatedAt().Equal(fixture.Construction.Child.OccurredAt) {
		return 0, fmt.Errorf("conversation construction requires its original prepared creating event")
	}
	return mutationprotocol.Story, nil
}

func seedConversationForkChatEntityForTest(ctx context.Context, selected any, run, entity string, forkAt time.Time, construction ConversationForkConstruction) error {
	_, err := SeedConversationForkSourceForTest(ctx, selected, ConversationForkSourceFixture{RunID: run, EntityID: entity, Turn1At: forkAt, Construction: construction, kind: conversationForkNativeChat})
	return err
}

func seedConversationForkDataOnlyEntityForTest(ctx context.Context, selected any, run, entity string, forkAt time.Time) error {
	_, err := SeedConversationForkSourceForTest(ctx, selected, ConversationForkSourceFixture{RunID: run, EntityID: entity, Turn1At: forkAt, kind: conversationForkDataOnlyChat})
	return err
}

func validateConversationForkFixture(ctx context.Context, fixture ConversationForkSourceFixture) (string, error) {
	if fixture.kind == conversationForkServedSource {
		identity, err := fixture.Identity.StorageFields()
		if err != nil {
			return "", err
		}
		if identity.RunID != fixture.RunID {
			return "", fmt.Errorf("fork fixture run does not match agent identity")
		}
		if fixture.SessionID == "" || fixture.EntityID == "" || fixture.Event1ID == "" || fixture.Event2ID == "" || fixture.CreatedAt.IsZero() || fixture.Turn1At.IsZero() || !fixture.Turn2At.After(fixture.Turn1At) {
			return "", fmt.Errorf("fork fixture requires exact historical identities and timestamps")
		}
	}
	for _, id := range []string{fixture.RunID, fixture.EntityID} {
		if _, err := uuid.Parse(id); err != nil {
			return "", fmt.Errorf("conversation fixture requires exact run/entity UUIDs: %w", err)
		}
	}
	if fixture.Turn1At.IsZero() {
		return "", fmt.Errorf("conversation fixture requires an exact historical cut")
	}
	source, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok || source.BundleHash() == "" {
		return "", fmt.Errorf("fork fixture requires exact source scope")
	}
	return source.BundleHash(), nil
}
