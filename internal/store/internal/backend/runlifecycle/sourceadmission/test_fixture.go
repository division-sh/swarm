package sourceadmission

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	sourceartifactstore "github.com/division-sh/swarm/internal/store/internal/sourceartifact"
)

// FixtureSourceMutation pairs fixture source operations with their native owner.
// It exposes neither SQL nor an admission setter.
type FixtureSourceMutation struct {
	attempt *mutationprotocol.Attempt
	dialect string
}

func NewFixtureSourceMutation(mutation runtimeauthoractivity.Mutation, dialect string) (FixtureSourceMutation, error) {
	attempt, ok := mutation.(*mutationprotocol.Attempt)
	if !ok || attempt == nil {
		return FixtureSourceMutation{}, errors.New("fixture run source requires a native mutation attempt")
	}
	if dialect != "postgres" && dialect != "sqlite" {
		return FixtureSourceMutation{}, fmt.Errorf("fixture run source dialect %q is unsupported", dialect)
	}
	return FixtureSourceMutation{attempt: attempt, dialect: dialect}, nil
}

func (m FixtureSourceMutation) RequirePresent(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return m.load(ctx, runID, false)
}

func (m FixtureSourceMutation) RequireActive(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return m.load(ctx, runID, true)
}

// RequireActiveAdmission observes native admission without minting or reloading.
func (m FixtureSourceMutation) RequireActiveAdmission(ctx context.Context, runID string, source runtimecorrelation.SourceArtifactFact) error {
	if m.attempt == nil {
		return errors.New("fixture run source requires a native mutation attempt")
	}
	return m.attempt.RequireActiveRunSourceAdmission(ctx, runID, source)
}

// RequireWriteFrame checks the existing native frame without lending one.
func (m FixtureSourceMutation) RequireWriteFrame(ctx context.Context) error {
	if m.attempt == nil {
		return errors.New("fixture run source requires a native mutation attempt")
	}
	return m.attempt.RequireExistingSQLFrame(ctx)
}

func (m FixtureSourceMutation) load(ctx context.Context, runID string, active bool) (runtimecorrelation.SourceArtifactFact, error) {
	if err := m.RequireWriteFrame(ctx); err != nil {
		return runtimecorrelation.SourceArtifactFact{}, err
	}
	var source runtimecorrelation.SourceArtifactFact
	err := m.attempt.WithSQL(ctx, func(_ context.Context, tx *sql.Tx) (err error) {
		// Use the already-validated frame. Only the canonical loader may mint
		// active admission; present reads remain nonminting.
		switch m.dialect {
		case "postgres":
			source, err = LoadPostgresTx(ctx, tx, runID, active, false)
		case "sqlite":
			source, err = LoadSQLiteTx(ctx, tx, runID, active, false)
		default:
			return errors.New("fixture run source dialect is required")
		}
		if err == nil && active {
			err = m.attempt.RequireActiveRunSourceAdmission(ctx, runID, source)
		}
		return err
	})
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, err
	}
	return source, nil
}

// Invalidate follows a fixture lifecycle/source write before its next reader.
func (m FixtureSourceMutation) Invalidate(ctx context.Context, runID string) error {
	if err := m.RequireWriteFrame(ctx); err != nil {
		return err
	}
	return m.attempt.WithSQL(ctx, func(_ context.Context, tx *sql.Tx) error {
		return mutationprotocol.InvalidateActiveRunSource(ctx, tx, runID)
	})
}

// DeleteSourceArtifactForRefusal corrupts artifact availability without clearing
// the warmed run-row admission: the next source read must check it freshly.
func (m FixtureSourceMutation) DeleteSourceArtifactForRefusal(ctx context.Context, source runtimecorrelation.SourceArtifactFact) error {
	if err := m.RequireWriteFrame(ctx); err != nil {
		return err
	}
	return m.attempt.WithSQL(ctx, func(_ context.Context, tx *sql.Tx) error {
		return sourceartifactstore.DeleteSourceArtifactForFixtureRefusalTx(ctx, tx, m.dialect, source)
	})
}
