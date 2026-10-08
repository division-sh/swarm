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
	key, valid := physicalRunKey(attempt.dialect, runID)
	if !valid {
		return runtimecorrelation.SourceArtifactFact{}, false, nil
	}
	fact, ok := attempt.runAdmissions[key]
	return fact, ok, nil
}

// CacheActiveRunSource is called only after canonical mutable active-run and
// source-artifact admission. Read-only results and context facts cannot mint it.
func CacheActiveRunSource(ctx context.Context, tx *sql.Tx, runID string, fact runtimecorrelation.SourceArtifactFact) error {
	attempt, err := runAdmissionAttempt(ctx, tx)
	if err != nil || attempt == nil {
		return err
	}
	key, valid := physicalRunKey(attempt.dialect, runID)
	if key == "" {
		return errors.New("run admission requires run_id")
	}
	if !valid {
		return errors.New("run admission requires a physical run identity")
	}
	if err := fact.Validate(); err != nil {
		return err
	}
	attempt.runAdmissions[key] = fact
	return nil
}

// InvalidateActiveRunSource follows each canonical source/lifecycle write,
// before another source consumer can reuse the old run-row admission.
func InvalidateActiveRunSource(ctx context.Context, tx *sql.Tx, runID string) error {
	attempt, err := runAdmissionAttempt(ctx, tx)
	if err != nil || attempt == nil {
		return err
	}
	key, valid := physicalRunKey(attempt.dialect, runID)
	if attempt.dialect == privateactivity.DialectPostgres {
		// Preserve conservative invalidation for spellings outside uuid.Parse,
		// even when PG accepts them. Invalid spellings cannot retain authority.
		if _, err := uuid.Parse(strings.TrimSpace(runID)); err != nil || !valid {
			clear(attempt.runAdmissions)
			return nil
		}
	}
	delete(attempt.runAdmissions, key)
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
	key, valid := physicalRunKey(a.dialect, runID)
	admitted, ok := a.runAdmissions[key]
	if !valid || !ok || !admitted.Matches(fact) {
		return errors.New("active run source was not admitted by this mutation attempt")
	}
	return nil
}
