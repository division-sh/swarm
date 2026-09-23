package mutationprotocol

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	runtimeactivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimelifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	privatefork "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
	"github.com/google/uuid"
)

// Evidence names the mutation's ordering obligation, not a choice of finalizer order.
type Evidence uint8

const (
	Story Evidence = iota + 1
	AuthorityFence
	RevisionOnly
)

// Kind distinguishes ordinary writes from the two supported destructive cuts.
type Kind uint8

const (
	Ordinary Kind = iota + 1
	RetainedForkCleanup
	WholeParentDeletion
)

// Phase is diagnostic evidence about the last attempted lifecycle stage. It
// does not infer rollback from an unacknowledged native COMMIT.
type Phase uint8

const (
	BeforeAttempt Phase = iota + 1
	AcquireFence
	DomainWrite
	RevisionFinalize
	ActivityFinalize
	CommitAdmission
	PostCommit
)

type Baseline struct {
	sync.Mutex
	effects *privatefork.Effects
	frozen  bool
}

func NewBaseline() *Baseline { return &Baseline{effects: privatefork.NewEffects()} }

func (b *Baseline) AddFact(runID string, family privatefork.Family, key string) error {
	if b == nil || b.effects == nil {
		return errors.New("mutation baseline is required")
	}
	b.Lock()
	defer b.Unlock()
	if b.frozen {
		return errors.New("mutation baseline is already in use")
	}
	return b.effects.AddFact(runID, family, key)
}

func (b *Baseline) AddWholeFamily(runID string, family privatefork.Family) error {
	if b == nil || b.effects == nil {
		return errors.New("mutation baseline is required")
	}
	b.Lock()
	defer b.Unlock()
	if b.frozen {
		return errors.New("mutation baseline is already in use")
	}
	return b.effects.Add(runID, family)
}

// CandidateWriter is the canonical durable request writer. The protocol invokes
// it and reserves its returned candidate inside the same transaction attempt.
type CandidateWriter interface {
	WriteCompletionCandidateTx(context.Context, *sql.Tx, string, *time.Time) (runtimelifecycle.CandidateRequestResult, error)
}

// ClaimRetirement closes a settled pipeline claim before its committed
// completion candidate becomes executable.
type ClaimRetirement interface {
	RetireCommittedClaim(context.Context) error
}

type Attempt struct {
	tx              *sql.Tx
	dialect         privateactivity.Dialect
	evidence        Evidence
	kind            Kind
	story           *privateactivity.Mutation
	effects         *privatefork.Effects
	handoff         *runhandoff.CandidateHandoff
	candidates      *runhandoff.CandidateCoordinator
	active          bool
	cleanup         bool
	discardRunID    string
	discardRetained bool
	discardSelected bool
	claimRetirement ClaimRetirement
}

var _ runtimeactivity.Mutation = (*Attempt)(nil)

// WithSQL lends the native transaction only for the duration of a named
// domain writer. It must not be retained or committed by the writer.
func (a *Attempt) WithSQL(ctx context.Context, writer func(context.Context, *sql.Tx) error) error {
	if err := a.requireActive(); err != nil {
		return err
	}
	if writer == nil {
		return errors.New("mutation SQL writer is required")
	}
	return writer(ctx, a.tx)
}

func (a *Attempt) Record(ctx context.Context, draft runtimeactivity.Draft) error {
	if err := a.requireActive(); err != nil {
		return err
	}
	if a.story == nil || a.cleanup {
		return errors.New("mutation cannot record activity in this phase")
	}
	return a.story.Record(ctx, draft)
}

func (a *Attempt) PersistedOccurredAt(ctx context.Context, key string) (time.Time, bool, error) {
	if err := a.requireActive(); err != nil {
		return time.Time{}, false, err
	}
	if a.story == nil || a.cleanup {
		return time.Time{}, false, errors.New("mutation has no active activity story")
	}
	return a.story.PersistedOccurredAt(ctx, key)
}

func (a *Attempt) PersistedAuthorSafeSummary(ctx context.Context, key string) (string, bool, error) {
	if err := a.requireActive(); err != nil {
		return "", false, err
	}
	if a.story == nil || a.cleanup {
		return "", false, errors.New("mutation has no active activity story")
	}
	return a.story.PersistedAuthorSafeSummary(ctx, key)
}

func (a *Attempt) AddFact(runID string, family privatefork.Family, key string) error {
	if err := a.requireActive(); err != nil {
		return err
	}
	if a.kind == WholeParentDeletion {
		return errors.New("whole-parent deletion does not publish revision facts")
	}
	return a.effects.AddFact(runID, family, key)
}

func (a *Attempt) AddFacts(runID string, refs ...privatefork.FactRef) error {
	if err := a.requireActive(); err != nil {
		return err
	}
	if a.kind == WholeParentDeletion {
		return errors.New("whole-parent deletion does not publish revision facts")
	}
	return a.effects.AddFacts(runID, refs...)
}

func (a *Attempt) AddWholeFamily(runID string, family privatefork.Family) error {
	if err := a.requireActive(); err != nil {
		return err
	}
	if a.kind == WholeParentDeletion {
		return errors.New("whole-parent deletion does not publish revision facts")
	}
	return a.effects.Add(runID, family)
}

func (a *Attempt) RequestCompletion(ctx context.Context, writer CandidateWriter, runID string, dueAt *time.Time) (runtimelifecycle.CandidateRequestResult, error) {
	if err := a.requireActive(); err != nil {
		return runtimelifecycle.CandidateRequestResult{}, err
	}
	if a.cleanup || writer == nil || a.handoff == nil || a.candidates == nil {
		return runtimelifecycle.CandidateRequestResult{}, errors.New("completion candidate writer and reservation are required")
	}
	if a.kind == WholeParentDeletion {
		return runtimelifecycle.CandidateRequestResult{}, errors.New("whole-parent deletion cannot request completion")
	}
	result, err := writer.WriteCompletionCandidateTx(ctx, a.tx, runID, dueAt)
	if err != nil {
		return runtimelifecycle.CandidateRequestResult{}, err
	}
	if err := result.Validate(); err != nil {
		return runtimelifecycle.CandidateRequestResult{}, err
	}
	if result.RequiresRepresentation() && result.Candidate.RunID != strings.TrimSpace(runID) {
		return runtimelifecycle.CandidateRequestResult{}, errors.New("completion candidate request returned a different run")
	}
	if err := a.handoff.Prepare(a.candidates, result); err != nil {
		return runtimelifecycle.CandidateRequestResult{}, err
	}
	return result, nil
}

// RetireClaimBeforeCandidateHandoff binds the exact settled claim to this
// attempt. Retirement runs after acknowledged COMMIT, before candidate handoff.
func (a *Attempt) RetireClaimBeforeCandidateHandoff(retirement ClaimRetirement) error {
	if err := a.requireActive(); err != nil {
		return err
	}
	if retirement == nil || a.claimRetirement != nil {
		return errors.New("one claim retirement is required per attempt")
	}
	a.claimRetirement = retirement
	return nil
}

// SelectForkDiscardRetention binds the destructive kind to the canonical
// retained-execution row inside this attempt. The caller cannot choose a
// finalization order from a previously observed boolean.
func (a *Attempt) SelectForkDiscardRetention(ctx context.Context, runID string) (bool, error) {
	if err := a.requireActive(); err != nil {
		return false, err
	}
	if a.kind != RetainedForkCleanup || a.cleanup || a.discardSelected {
		return false, errors.New("selected fork discard retention can be classified only once before cleanup")
	}
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return false, fmt.Errorf("selected fork discard requires a UUID run_id: %w", err)
	}
	var retained bool
	if err := a.tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id = $1)`, runID).Scan(&retained); err != nil {
		return false, fmt.Errorf("select selected fork completion evidence: %w", err)
	}
	a.discardRunID = runID
	a.discardRetained = retained
	a.discardSelected = true
	return retained, nil
}

// BeginDestructiveCleanup finalizes the terminalization story before deleting
// the fork's state. The two destructive kinds are explicit, not a caller-chosen
// ordering flag on ordinary mutations.
func (a *Attempt) BeginDestructiveCleanup(ctx context.Context) error {
	if err := a.requireActive(); err != nil {
		return err
	}
	if a.kind != RetainedForkCleanup && a.kind != WholeParentDeletion {
		return errors.New("ordinary mutation cannot enter destructive cleanup")
	}
	if a.cleanup || a.story == nil {
		return errors.New("destructive cleanup activity was already finalized")
	}
	if a.kind == RetainedForkCleanup && !a.discardSelected {
		return errors.New("selected fork discard retention was not classified")
	}
	if err := a.story.Finalize(ctx); err != nil {
		return err
	}
	if a.kind == RetainedForkCleanup && !a.discardRetained {
		if err := a.effects.DiscardDeletedRun(a.discardRunID); err != nil {
			return err
		}
		a.kind = WholeParentDeletion
	}
	a.cleanup = true
	return nil
}

func (a *Attempt) requireActive() error {
	if a == nil || !a.active || a.tx == nil {
		return errors.New("mutation attempt is not active")
	}
	return nil
}

func (a *Attempt) finalize(ctx context.Context, phase *Phase) error {
	if err := a.requireActive(); err != nil {
		return err
	}
	if a.kind == WholeParentDeletion {
		if !a.cleanup {
			return errors.New("whole-parent deletion did not finalize activity before deletion")
		}
		return nil
	}
	if a.kind == RetainedForkCleanup && !a.cleanup {
		return errors.New("retained fork cleanup did not finalize activity before deletion")
	}
	if a.effects.HasDeclarations() {
		*phase = RevisionFinalize
		if a.dialect == privateactivity.DialectPostgres {
			if _, err := privatefork.FinalizePostgres(ctx, a.tx, a.effects); err != nil {
				return err
			}
		} else {
			if _, err := privatefork.FinalizeSQLite(ctx, a.tx, a.effects); err != nil {
				return err
			}
		}
	}
	if a.story != nil && !a.cleanup {
		*phase = ActivityFinalize
		return a.story.Finalize(ctx)
	}
	return nil
}

// Result exposes only the value from an acknowledged attempt. An unacknowledged
// commit is not treated as a proven rollback.
type Result[T any] struct {
	value        T
	acknowledged bool
	err          error
	phase        Phase
}

func (r Result[T]) Acknowledged() bool { return r.acknowledged }
func (r Result[T]) Err() error         { return r.err }
func (r Result[T]) Phase() Phase       { return r.phase }
func (r Result[T]) Value() (T, bool) {
	if !r.acknowledged {
		var zero T
		return zero, false
	}
	return r.value, true
}

type nativeRunner func(context.Context, func(context.Context, *sql.Tx) error) (bool, error)

func RunPostgres[T any](ctx context.Context, db *postgresbackend.Backend, evidence Evidence, kind Kind, baseline *Baseline, candidates *runhandoff.CandidateCoordinator, write func(context.Context, *Attempt) (T, error)) Result[T] {
	if db == nil {
		return failed[T](errors.New("PostgreSQL mutation backend is required"))
	}
	if kind != Ordinary {
		return failed[T](errors.New("destructive PostgreSQL mutation requires serializable transaction options"))
	}
	return run(ctx, privateactivity.DialectPostgres, evidence, kind, baseline, candidates, db.RunTransactionOutcome, write)
}

func RunPostgresWithOptions[T any](ctx context.Context, db *postgresbackend.Backend, opts *sql.TxOptions, evidence Evidence, kind Kind, baseline *Baseline, candidates *runhandoff.CandidateCoordinator, write func(context.Context, *Attempt) (T, error)) Result[T] {
	if db == nil {
		return failed[T](errors.New("PostgreSQL mutation backend is required"))
	}
	if kind != Ordinary && (opts == nil || opts.Isolation != sql.LevelSerializable || opts.ReadOnly) {
		return failed[T](errors.New("destructive PostgreSQL mutation requires serializable write transaction"))
	}
	return run(ctx, privateactivity.DialectPostgres, evidence, kind, baseline, candidates, func(ctx context.Context, fn func(context.Context, *sql.Tx) error) (bool, error) {
		return db.RunTransactionWithOptionsOutcome(ctx, opts, fn)
	}, write)
}

func RunSQLite[T any](ctx context.Context, db *sqlitebackend.Backend, label string, evidence Evidence, kind Kind, baseline *Baseline, candidates *runhandoff.CandidateCoordinator, write func(context.Context, *Attempt) (T, error)) Result[T] {
	if db == nil {
		return failed[T](errors.New("SQLite mutation backend is required"))
	}
	return run(ctx, privateactivity.DialectSQLite, evidence, kind, baseline, candidates, func(ctx context.Context, fn func(context.Context, *sql.Tx) error) (bool, error) {
		return db.RunTransactionOutcome(ctx, label, fn)
	}, write)
}

func RunRetainedPostgres[T any](ctx context.Context, session *postgresbackend.SessionAuthority, evidence Evidence, kind Kind, baseline *Baseline, candidates *runhandoff.CandidateCoordinator, write func(context.Context, *Attempt) (T, error)) Result[T] {
	if session == nil {
		return failed[T](errors.New("retained PostgreSQL authority is required"))
	}
	if kind != Ordinary {
		return failed[T](errors.New("destructive retained PostgreSQL mutation requires serializable transaction options"))
	}
	return run(ctx, privateactivity.DialectPostgres, evidence, kind, baseline, candidates, func(ctx context.Context, fn func(context.Context, *sql.Tx) error) (bool, error) {
		return postgresbackend.RunAuthorityTransactionOutcome(ctx, session, fn)
	}, write)
}

func RunRetainedPostgresWithOptions[T any](ctx context.Context, session *postgresbackend.SessionAuthority, opts *sql.TxOptions, evidence Evidence, kind Kind, baseline *Baseline, candidates *runhandoff.CandidateCoordinator, write func(context.Context, *Attempt) (T, error)) Result[T] {
	if session == nil {
		return failed[T](errors.New("retained PostgreSQL authority is required"))
	}
	if kind != Ordinary && (opts == nil || opts.Isolation != sql.LevelSerializable || opts.ReadOnly) {
		return failed[T](errors.New("destructive retained PostgreSQL mutation requires serializable write transaction"))
	}
	return run(ctx, privateactivity.DialectPostgres, evidence, kind, baseline, candidates, func(ctx context.Context, fn func(context.Context, *sql.Tx) error) (bool, error) {
		return postgresbackend.RunAuthorityTransactionOutcomeWithOptions(ctx, session, opts, fn)
	}, write)
}

func failed[T any](err error) Result[T] { return Result[T]{err: err, phase: BeforeAttempt} }

// Reject reports an admission failure before a transaction attempt exists.
func Reject[T any](err error) Result[T] { return failed[T](err) }

func run[T any](ctx context.Context, dialect privateactivity.Dialect, evidence Evidence, kind Kind, baseline *Baseline, candidates *runhandoff.CandidateCoordinator, native nativeRunner, write func(context.Context, *Attempt) (T, error)) Result[T] {
	if write == nil || native == nil {
		return failed[T](errors.New("selected-store mutation writer and native runner are required"))
	}
	if evidence != Story && evidence != AuthorityFence && evidence != RevisionOnly {
		return failed[T](fmt.Errorf("unsupported mutation evidence mode %d", evidence))
	}
	if kind != Ordinary && kind != RetainedForkCleanup && kind != WholeParentDeletion {
		return failed[T](fmt.Errorf("unsupported mutation kind %d", kind))
	}
	if kind == WholeParentDeletion {
		return failed[T](errors.New("whole-parent deletion must be selected from durable fork retention evidence"))
	}
	if kind != Ordinary && evidence != Story {
		return failed[T](errors.New("destructive fork mutation requires activity story"))
	}
	if baseline == nil {
		baseline = NewBaseline()
	}
	if baseline.effects == nil {
		return failed[T](errors.New("mutation baseline is required"))
	}
	baseline.Lock()
	if baseline.frozen {
		baseline.Unlock()
		return failed[T](errors.New("mutation baseline is already in use"))
	}
	baseline.frozen = true
	baseline.Unlock()
	var handoff *runhandoff.CandidateHandoff
	if candidates != nil {
		var err error
		handoff, err = runhandoff.ReserveCandidateHandoff(ctx)
		if err != nil {
			return failed[T](err)
		}
		defer handoff.Rollback()
	}
	resetEffects := baseline.effects.AttemptReset()
	var value T
	var previous *Attempt
	phase := BeforeAttempt
	acknowledged, nativeErr := native(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if previous != nil {
			previous.active = false
		}
		resetEffects()
		if handoff != nil {
			if err := handoff.ResetAttempt(); err != nil {
				return err
			}
		}
		attempt := &Attempt{tx: tx, dialect: dialect, evidence: evidence, kind: kind, effects: baseline.effects, handoff: handoff, candidates: candidates, active: true}
		previous = attempt
		defer func() { attempt.active = false }()
		phase = AcquireFence
		switch evidence {
		case Story:
			story, err := privateactivity.Begin(txctx, tx, dialect)
			if err != nil {
				return err
			}
			attempt.story = story
		case AuthorityFence:
			if err := privateactivity.FenceMutationOrder(txctx, tx, dialect); err != nil {
				return err
			}
		}
		phase = DomainWrite
		candidate, err := write(txctx, attempt)
		if err != nil {
			return err
		}
		if err := attempt.finalize(txctx, &phase); err != nil {
			return err
		}
		value = candidate
		phase = CommitAdmission
		return nil
	})
	if !acknowledged {
		if nativeErr == nil {
			nativeErr = errors.New("selected-store mutation commit was not acknowledged")
		}
		return Result[T]{err: nativeErr, phase: phase}
	}
	if previous != nil && previous.claimRetirement != nil {
		nativeErr = errors.Join(nativeErr, previous.claimRetirement.RetireCommittedClaim(context.WithoutCancel(ctx)))
	}
	if handoff != nil {
		nativeErr = errors.Join(nativeErr, handoff.Commit())
	}
	return Result[T]{value: value, acknowledged: true, err: nativeErr, phase: PostCommit}
}
