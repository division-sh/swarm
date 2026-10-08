package mutationprotocol

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/google/uuid"
)

type activeRunSources map[string]runtimecorrelation.SourceArtifactFact

func runAdmissionAttempt(ctx context.Context, tx *sql.Tx) (*Attempt, error) {
	if ctx == nil {
		return nil, errors.New("run admission context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	current := ctx.Value(sqlAttemptKey{})
	if current == nil {
		return nil, nil
	}
	attempt, ok := current.(*Attempt)
	if !ok {
		return nil, errors.New("run admission context requires a native mutation attempt")
	}
	if err := attempt.requireActive(); err != nil {
		return nil, err
	}
	if attempt.tx != tx {
		return nil, errors.New("run admission belongs to another transaction")
	}
	if attempt.callerCtx == nil || attempt.runAdmissions == nil {
		return nil, errors.New("run admission requires a native mutation attempt")
	}
	if err := attempt.callerCtx.Err(); err != nil {
		return nil, err
	}
	return attempt, nil
}

// CachedActiveRunSource reuses only the matching native attempt's mutable
// run-row admission. Source-artifact availability must still be queried.
func CachedActiveRunSource(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.SourceArtifactFact, bool, error) {
	attempt, err := runAdmissionAttempt(ctx, tx)
	if err != nil || attempt == nil {
		return runtimecorrelation.SourceArtifactFact{}, false, err
	}
	fact, ok := attempt.runAdmissions[strings.TrimSpace(runID)]
	return fact, ok, nil
}

// CacheActiveRunSource is called only after canonical mutable active-run and
// source-artifact admission. Read-only results and context facts cannot mint it.
func CacheActiveRunSource(ctx context.Context, tx *sql.Tx, runID string, fact runtimecorrelation.SourceArtifactFact) error {
	attempt, err := runAdmissionAttempt(ctx, tx)
	if err != nil || attempt == nil {
		return err
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return errors.New("run admission requires run_id")
	}
	if err := fact.Validate(); err != nil {
		return err
	}
	attempt.runAdmissions[runID] = fact
	return nil
}

// InvalidateActiveRunSource follows each canonical source/lifecycle write,
// before another source consumer can reuse the old run-row admission.
func InvalidateActiveRunSource(ctx context.Context, tx *sql.Tx, runID string) error {
	attempt, err := runAdmissionAttempt(ctx, tx)
	if err != nil || attempt == nil {
		return err
	}
	runID = strings.TrimSpace(runID)
	if attempt.dialect == privateactivity.DialectPostgres {
		// PostgreSQL UUID aliases name the same row, unlike SQLite TEXT keys.
		id, err := uuid.Parse(runID)
		if err != nil {
			clear(attempt.runAdmissions)
			return nil
		}
		for admittedRun := range attempt.runAdmissions {
			admittedID, err := uuid.Parse(admittedRun)
			if err != nil || admittedID == id {
				delete(attempt.runAdmissions, admittedRun)
			}
		}
	} else {
		delete(attempt.runAdmissions, runID)
	}
	return nil
}

// RequireActiveRunSourceAdmission checks each record against this exact
// attempt's canonical mutable admission, not a public source lookup.
func (a *Attempt) RequireActiveRunSourceAdmission(ctx context.Context, runID string, fact runtimecorrelation.SourceArtifactFact) error {
	if err := a.requireActive(); err != nil {
		return err
	}
	attempt, err := runAdmissionAttempt(ctx, a.tx)
	if err != nil {
		return err
	}
	if attempt != a {
		return errors.New("run admission belongs to another mutation attempt")
	}
	admitted, ok := a.runAdmissions[strings.TrimSpace(runID)]
	if !ok || !admitted.Matches(fact) {
		return errors.New("active run source was not admitted by this mutation attempt")
	}
	return nil
}
