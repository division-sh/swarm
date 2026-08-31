package sourceartifactfixture

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

type fixtureStore struct {
	persisted sourceartifact.Persisted
	readErr   error
	writes    int
}

func (s *fixtureStore) GetSourceArtifact(context.Context, string) (sourceartifact.Persisted, error) {
	return s.persisted, s.readErr
}

func (s *fixtureStore) EnsureSourceArtifactWithData(_ context.Context, artifact *sourceartifact.AdmittedSourceArtifact, _ durabledata.Catalog) (sourceartifact.EnsureResult, error) {
	s.writes++
	persisted, err := sourceartifact.PersistedFromArtifact(artifact, time.Unix(1, 0).UTC())
	return sourceartifact.EnsureResult{Artifact: persisted, Created: true}, err
}

func TestEnsureArtifactReusesExactPersistedArtifact(t *testing.T) {
	artifact := Artifact()
	persisted, err := sourceartifact.PersistedFromArtifact(artifact, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	store := &fixtureStore{persisted: persisted}
	if err := EnsureArtifact(context.Background(), store, artifact); err != nil {
		t.Fatal(err)
	}
	if store.writes != 0 {
		t.Fatalf("writes = %d, want 0", store.writes)
	}
}

func TestEnsureArtifactWritesWhenMissing(t *testing.T) {
	store := &fixtureStore{readErr: sourceartifact.ErrNotFound}
	if err := EnsureArtifact(context.Background(), store, Artifact()); err != nil {
		t.Fatal(err)
	}
	if store.writes != 1 {
		t.Fatalf("writes = %d, want 1", store.writes)
	}
}

func TestEnsureArtifactFailsClosedOnReadError(t *testing.T) {
	want := errors.New("read failed")
	store := &fixtureStore{readErr: want}
	if err := EnsureArtifact(context.Background(), store, Artifact()); !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if store.writes != 0 {
		t.Fatalf("writes = %d, want 0", store.writes)
	}
}
