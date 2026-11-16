package runlifecyclefixture

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func ScenarioSetupOrigin() runtimerunlifecycle.RunOrigin {
	return runtimerunlifecycle.ScenarioSetupRunOrigin()
}

func ScenarioSetupOriginKind() string {
	return string(runtimerunlifecycle.OriginScenarioSetup)
}

func EventOrigin(t testing.TB, eventID, eventType string) runtimerunlifecycle.RunOrigin {
	t.Helper()
	origin, err := runtimerunlifecycle.EventRunOrigin(eventID, eventType)
	if err != nil {
		t.Fatalf("construct semantic event run origin: %v", err)
	}
	return origin
}

type CorruptSnapshot struct {
	RunID               string
	State               string
	BundleHash          string
	RootDirectory       string
	OriginKind          string
	TriggerEventID      string
	TriggerEventType    string
	ForkedFromRunID     string
	ForkedFromPointKind string
	ForkedFromRevision  int64
	ForkedFromEventID   string
	ContinuedAsRunID    string
	EventCount          int
	Failure             *runtimefailures.Envelope
	StartedAt           time.Time
	EndedAt             time.Time
}

// NormalizeCorruptSnapshot preserves hostile lifecycle fields while preparing
// failure and timestamps for the private lifecycle snapshot fault writer.
func NormalizeCorruptSnapshot(snapshot CorruptSnapshot) (CorruptSnapshot, string, any, error) {
	if snapshot.StartedAt.IsZero() {
		snapshot.StartedAt = time.Now().UTC()
	}
	if strings.TrimSpace(snapshot.OriginKind) == "" {
		return CorruptSnapshot{}, "", nil, fmt.Errorf(
			"corrupt run snapshot %s requires explicit origin_kind",
			snapshot.RunID,
		)
	}
	if state, err := runtimerunlifecycle.ParseState(snapshot.State); err == nil &&
		state.Terminal() &&
		snapshot.EndedAt.IsZero() {
		snapshot.EndedAt = snapshot.StartedAt
	}
	failure, err := marshalFixtureFailure(snapshot.Failure)
	if err != nil {
		return CorruptSnapshot{}, "", nil, fmt.Errorf(
			"marshal corrupt run failure %s: %w",
			snapshot.RunID, err,
		)
	}
	return snapshot, failure, nullableFixtureTime(snapshot.EndedAt), nil
}

func nullableFixtureTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

func marshalFixtureFailure(failure *runtimefailures.Envelope) (string, error) {
	if failure == nil {
		return "", nil
	}
	raw, err := json.Marshal(failure)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
