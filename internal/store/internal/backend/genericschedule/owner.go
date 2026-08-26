package genericschedule

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

type dialect uint8

const (
	sqliteDialect dialect = iota + 1
	postgresDialect
)

type persistedScheduleFamily uint8

const (
	persistedScheduleFamilyUnclassified persistedScheduleFamily = iota
	persistedScheduleFamilyNonJoin
	persistedScheduleFamilyJoin
)

type malformedActivationError struct {
	cause  error
	family persistedScheduleFamily
}

func (e *malformedActivationError) Error() string { return e.cause.Error() }
func (e *malformedActivationError) Unwrap() error { return e.cause }

func asMalformedActivation(err error) (*malformedActivationError, bool) {
	var malformed *malformedActivationError
	ok := errors.As(err, &malformed)
	return malformed, ok
}

type malformedActivationDisposition uint8

const (
	malformedActivationReject malformedActivationDisposition = iota
	malformedActivationTerminalize
)

func dispositionForMalformedActivation(malformed *malformedActivationError) malformedActivationDisposition {
	if malformed != nil && malformed.family == persistedScheduleFamilyNonJoin {
		return malformedActivationTerminalize
	}
	return malformedActivationReject
}

func classifyPersistedScheduleFamily(eventType string, payloadRaw any) persistedScheduleFamily {
	switch strings.TrimSpace(eventType) {
	case "platform.join_timeout", "platform.join_complete":
		return persistedScheduleFamilyJoin
	}

	var payload map[string]any
	if err := json.Unmarshal(jsonBytes(payloadRaw), &payload); err != nil {
		return persistedScheduleFamilyUnclassified
	}
	rawHandle, present := payload["timer_handle"]
	if !present {
		return persistedScheduleFamilyNonJoin
	}
	handle, ok := rawHandle.(map[string]any)
	if !ok {
		return persistedScheduleFamilyUnclassified
	}
	kind, ok := handle["kind"].(string)
	if !ok {
		return persistedScheduleFamilyUnclassified
	}
	switch timeridentity.TimerHandleKind(strings.TrimSpace(kind)) {
	case timeridentity.TimerHandleJoinTimeout, timeridentity.TimerHandleJoinComplete:
		return persistedScheduleFamilyJoin
	case timeridentity.TimerHandleWorkflowTimer:
		return persistedScheduleFamilyNonJoin
	default:
		return persistedScheduleFamilyUnclassified
	}
}

type PostgresOwner struct {
	backend     *postgresbackend.Backend
	schemaGuard func() error
	nowMu       sync.RWMutex
	nowFn       func() time.Time
	claims      postgresClaims
}

type SQLiteOwner struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
	nowMu       sync.RWMutex
	nowFn       func() time.Time
}

type postgresClaims struct {
	mu   sync.Mutex
	conn *sql.Conn
	keys map[string]struct{}
}

func NewPostgres(backend *postgresbackend.Backend, schemaGuard func() error) (*PostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, errors.New("generic schedule postgres backend is required")
	}
	if schemaGuard == nil {
		return nil, errors.New("generic schedule postgres schema guard is required")
	}
	return &PostgresOwner{backend: backend, schemaGuard: schemaGuard, nowFn: time.Now}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, schemaGuard func() error) (*SQLiteOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, errors.New("generic schedule sqlite backend is required")
	}
	if schemaGuard == nil {
		return nil, errors.New("generic schedule sqlite schema guard is required")
	}
	return &SQLiteOwner{backend: backend, schemaGuard: schemaGuard, nowFn: time.Now}, nil
}

func (o *PostgresOwner) SetNowFnForTest(nowFn func() time.Time) {
	if o == nil {
		return
	}
	if nowFn == nil {
		nowFn = time.Now
	}
	o.nowMu.Lock()
	o.nowFn = nowFn
	o.nowMu.Unlock()
}

func (o *SQLiteOwner) SetNowFnForTest(nowFn func() time.Time) {
	if o == nil {
		return
	}
	if nowFn == nil {
		nowFn = time.Now
	}
	o.nowMu.Lock()
	o.nowFn = nowFn
	o.nowMu.Unlock()
}

func (o *PostgresOwner) now() time.Time {
	o.nowMu.RLock()
	fn := o.nowFn
	o.nowMu.RUnlock()
	return canonicalTime(fn())
}

func (o *SQLiteOwner) now() time.Time {
	o.nowMu.RLock()
	fn := o.nowFn
	o.nowMu.RUnlock()
	return canonicalTime(fn())
}

func (o *PostgresOwner) requireSchema() error {
	if o == nil || o.schemaGuard == nil {
		return errors.New("generic schedule postgres owner is required")
	}
	return o.schemaGuard()
}

func (o *SQLiteOwner) requireSchema() error {
	if o == nil || o.schemaGuard == nil {
		return errors.New("generic schedule sqlite owner is required")
	}
	return o.schemaGuard()
}

func (o *PostgresOwner) AdmitGenericSchedule(ctx context.Context, command runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.AdmissionResult, error) {
	if err := o.requireSchema(); err != nil {
		return runtimegenericschedule.AdmissionResult{}, err
	}
	var result runtimegenericschedule.AdmissionResult
	err := o.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		effects := privaterunforkrevision.NewEffects()
		var err error
		result, err = AdmitTx(txctx, tx, true, effects, command, o.now)
		if err != nil {
			return err
		}
		_, err = privaterunforkrevision.FinalizePostgres(txctx, tx, effects)
		return err
	})
	return result, err
}

func (o *SQLiteOwner) AdmitGenericSchedule(ctx context.Context, command runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.AdmissionResult, error) {
	if err := o.requireSchema(); err != nil {
		return runtimegenericschedule.AdmissionResult{}, err
	}
	var result runtimegenericschedule.AdmissionResult
	err := o.backend.RunTransaction(ctx, "sqlite generic schedule admission", func(txctx context.Context, tx *sql.Tx) error {
		effects := privaterunforkrevision.NewEffects()
		var err error
		result, err = AdmitTx(txctx, tx, false, effects, command, o.now)
		if err != nil {
			return err
		}
		_, err = privaterunforkrevision.FinalizeSQLite(txctx, tx, effects)
		return err
	})
	return result, err
}

func (o *PostgresOwner) AdmitTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, command runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.AdmissionResult, error) {
	return AdmitTx(ctx, tx, true, effects, command, o.now)
}

func (o *SQLiteOwner) AdmitTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, command runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.AdmissionResult, error) {
	return AdmitTx(ctx, tx, false, effects, command, o.now)
}

// AdmitTx is the sole immutable generic activation admission implementation.
// The due clock is called only after scoped-key lookup proves this is not replay.
func AdmitTx(ctx context.Context, tx *sql.Tx, postgres bool, effects *privaterunforkrevision.Effects, command runtimegenericschedule.AdmissionCommand, now func() time.Time) (runtimegenericschedule.AdmissionResult, error) {
	command = command.Canonical()
	if err := command.Validate(); err != nil {
		return runtimegenericschedule.AdmissionResult{}, err
	}
	if tx == nil || effects == nil || now == nil {
		return runtimegenericschedule.AdmissionResult{}, errors.New("generic schedule admission requires transaction, revision effects, and selected-store clock")
	}
	scope, err := command.ScopeKey()
	if err != nil {
		return runtimegenericschedule.AdmissionResult{}, err
	}
	hash, err := command.ImmutableHash()
	if err != nil {
		return runtimegenericschedule.AdmissionResult{}, err
	}
	persisted, found, err := loadByKeyTx(ctx, tx, dialectFor(postgres), scope, command.ScheduleKey, postgres)
	if err != nil {
		return runtimegenericschedule.AdmissionResult{}, err
	}
	if found {
		return exactReplay(scope, command.ScheduleKey, hash, persisted)
	}
	if command.RunID != "" {
		if postgres {
			err = storerunstate.RequirePostgresActiveTx(ctx, tx, command.RunID)
		} else {
			err = storerunstate.RequireSQLiteActiveTx(ctx, tx, command.RunID)
		}
		if err != nil {
			return runtimegenericschedule.AdmissionResult{}, err
		}
	}
	admittedAt := canonicalTime(now())
	firstDue, err := command.Due.FirstDue(admittedAt)
	if err != nil {
		return runtimegenericschedule.AdmissionResult{}, err
	}
	activation := runtimegenericschedule.Activation{
		ID: uuid.NewString(), Command: command, ImmutableHash: hash,
		AdmittedAt: admittedAt, InitialDueAt: firstDue, CurrentDueAt: firstDue,
		Status: runtimegenericschedule.StatusActive,
	}
	if err := activation.Validate(); err != nil {
		return runtimegenericschedule.AdmissionResult{}, err
	}
	inserted, err := insertActivationTx(ctx, tx, dialectFor(postgres), scope, activation)
	if err != nil {
		return runtimegenericschedule.AdmissionResult{}, err
	}
	if !inserted {
		persisted, found, err = loadByKeyTx(ctx, tx, dialectFor(postgres), scope, command.ScheduleKey, postgres)
		if err != nil {
			return runtimegenericschedule.AdmissionResult{}, err
		}
		if !found {
			return runtimegenericschedule.AdmissionResult{}, errors.New("generic schedule concurrent admission winner is missing")
		}
		return exactReplay(scope, command.ScheduleKey, hash, persisted)
	}
	if command.RunID != "" {
		if err := effects.Add(command.RunID, privaterunforkrevision.FamilyTimers); err != nil {
			return runtimegenericschedule.AdmissionResult{}, err
		}
	}
	return runtimegenericschedule.AdmissionResult{Outcome: runtimegenericschedule.AdmissionCreated, Activation: activation}, nil
}

func exactReplay(scope, key, hash string, persisted runtimegenericschedule.Activation) (runtimegenericschedule.AdmissionResult, error) {
	if persisted.ImmutableHash != hash {
		return runtimegenericschedule.AdmissionResult{}, &runtimegenericschedule.ConflictError{ScopeKey: scope, ScheduleKey: key}
	}
	if err := persisted.Validate(); err != nil {
		return runtimegenericschedule.AdmissionResult{}, err
	}
	return runtimegenericschedule.AdmissionResult{Outcome: runtimegenericschedule.AdmissionExactReplay, Activation: persisted}, nil
}

func (o *PostgresOwner) LoadGenericScheduleActivation(ctx context.Context, activationID string) (runtimegenericschedule.Activation, bool, error) {
	if err := o.requireSchema(); err != nil {
		return runtimegenericschedule.Activation{}, false, err
	}
	return loadByID(ctx, o.backend, postgresDialect, activationID)
}

func (o *SQLiteOwner) LoadGenericScheduleActivation(ctx context.Context, activationID string) (runtimegenericschedule.Activation, bool, error) {
	if err := o.requireSchema(); err != nil {
		return runtimegenericschedule.Activation{}, false, err
	}
	return loadByID(ctx, o.backend, sqliteDialect, activationID)
}

func (o *PostgresOwner) ListActiveGenericScheduleActivations(ctx context.Context) ([]runtimegenericschedule.Activation, error) {
	if err := o.requireSchema(); err != nil {
		return nil, err
	}
	return o.listActive(ctx)
}

func (o *SQLiteOwner) ListActiveGenericScheduleActivations(ctx context.Context) ([]runtimegenericschedule.Activation, error) {
	if err := o.requireSchema(); err != nil {
		return nil, err
	}
	return o.listActive(ctx)
}

func (o *PostgresOwner) listActive(ctx context.Context) ([]runtimegenericschedule.Activation, error) {
	ids, err := listActiveIDs(ctx, o.backend)
	if err != nil {
		return nil, err
	}
	result := make([]runtimegenericschedule.Activation, 0, len(ids))
	for _, id := range ids {
		activation, found, loadErr := o.LoadGenericScheduleActivation(ctx, id)
		if malformed, ok := asMalformedActivation(loadErr); ok {
			if dispositionForMalformedActivation(malformed) == malformedActivationReject {
				return nil, fmt.Errorf("restore malformed mandatory or unclassified generic schedule %s: %w", id, malformed)
			}
			if err := o.failMalformed(ctx, id, malformed); err != nil {
				return nil, err
			}
			continue
		}
		if loadErr != nil {
			return nil, loadErr
		}
		if found && activation.Status == runtimegenericschedule.StatusActive {
			result = append(result, activation)
		}
	}
	return result, nil
}

func (o *SQLiteOwner) listActive(ctx context.Context) ([]runtimegenericschedule.Activation, error) {
	ids, err := listActiveIDs(ctx, o.backend)
	if err != nil {
		return nil, err
	}
	result := make([]runtimegenericschedule.Activation, 0, len(ids))
	for _, id := range ids {
		activation, found, loadErr := o.LoadGenericScheduleActivation(ctx, id)
		if malformed, ok := asMalformedActivation(loadErr); ok {
			if dispositionForMalformedActivation(malformed) == malformedActivationReject {
				return nil, fmt.Errorf("restore malformed mandatory or unclassified generic schedule %s: %w", id, malformed)
			}
			if err := o.failMalformed(ctx, id, malformed); err != nil {
				return nil, err
			}
			continue
		}
		if loadErr != nil {
			return nil, loadErr
		}
		if found && activation.Status == runtimegenericschedule.StatusActive {
			result = append(result, activation)
		}
	}
	return result, nil
}

func (o *PostgresOwner) failMalformed(ctx context.Context, activationID string, malformed error) error {
	return o.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		effects := privaterunforkrevision.NewEffects()
		runID, err := timerRunIDTx(txctx, tx, activationID)
		if err != nil {
			return err
		}
		if err := failMalformedByIDTx(txctx, tx, true, activationID, malformed, o.now()); err != nil {
			return err
		}
		if err := addTimerEffect(effects, runID); err != nil {
			return err
		}
		_, err = privaterunforkrevision.FinalizePostgres(txctx, tx, effects)
		return err
	})
}

func (o *SQLiteOwner) failMalformed(ctx context.Context, activationID string, malformed error) error {
	return o.backend.RunTransaction(ctx, "sqlite malformed generic schedule terminalization", func(txctx context.Context, tx *sql.Tx) error {
		effects := privaterunforkrevision.NewEffects()
		runID, err := timerRunIDTx(txctx, tx, activationID)
		if err != nil {
			return err
		}
		if err := failMalformedByIDTx(txctx, tx, false, activationID, malformed, o.now()); err != nil {
			return err
		}
		if err := addTimerEffect(effects, runID); err != nil {
			return err
		}
		_, err = privaterunforkrevision.FinalizeSQLite(txctx, tx, effects)
		return err
	})
}

func (o *PostgresOwner) PrepareGenericScheduleOccurrence(ctx context.Context, wakeup runtimegenericschedule.Wakeup) (runtimegenericschedule.PreparedOccurrence, error) {
	if err := o.requireSchema(); err != nil {
		return runtimegenericschedule.PreparedOccurrence{}, err
	}
	var result runtimegenericschedule.PreparedOccurrence
	err := o.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		effects := privaterunforkrevision.NewEffects()
		admittedAt, err := SelectedStoreTimeTx(txctx, tx, true, nil)
		if err != nil {
			return err
		}
		result, err = PrepareOccurrenceTx(txctx, tx, true, effects, wakeup, admittedAt)
		if err != nil {
			return err
		}
		runID := result.Activation.Command.RunID
		if strings.TrimSpace(runID) == "" {
			runID, err = timerRunIDTx(txctx, tx, wakeup.ActivationID())
			if err != nil {
				return err
			}
		}
		_, err = privaterunforkrevision.FinalizePostgres(txctx, tx, effects)
		return err
	})
	return result, err
}

func (o *SQLiteOwner) PrepareGenericScheduleOccurrence(ctx context.Context, wakeup runtimegenericschedule.Wakeup) (runtimegenericschedule.PreparedOccurrence, error) {
	if err := o.requireSchema(); err != nil {
		return runtimegenericschedule.PreparedOccurrence{}, err
	}
	var result runtimegenericschedule.PreparedOccurrence
	err := o.backend.RunTransaction(ctx, "sqlite generic schedule occurrence preparation", func(txctx context.Context, tx *sql.Tx) error {
		effects := privaterunforkrevision.NewEffects()
		admittedAt, err := SelectedStoreTimeTx(txctx, tx, false, o.now)
		if err != nil {
			return err
		}
		result, err = PrepareOccurrenceTx(txctx, tx, false, effects, wakeup, admittedAt)
		if err != nil {
			return err
		}
		runID := result.Activation.Command.RunID
		if strings.TrimSpace(runID) == "" {
			runID, err = timerRunIDTx(txctx, tx, wakeup.ActivationID())
			if err != nil {
				return err
			}
		}
		_, err = privaterunforkrevision.FinalizeSQLite(txctx, tx, effects)
		return err
	})
	return result, err
}

func PrepareOccurrenceTx(ctx context.Context, tx *sql.Tx, postgres bool, effects *privaterunforkrevision.Effects, wakeup runtimegenericschedule.Wakeup, admittedAt time.Time) (runtimegenericschedule.PreparedOccurrence, error) {
	if err := wakeup.Validate(); err != nil {
		return runtimegenericschedule.PreparedOccurrence{}, err
	}
	activation, found, err := loadByIDTx(ctx, tx, dialectFor(postgres), wakeup.ActivationID(), postgres)
	if err != nil {
		if malformed, ok := asMalformedActivation(err); ok {
			if dispositionForMalformedActivation(malformed) == malformedActivationReject {
				return runtimegenericschedule.PreparedOccurrence{}, fmt.Errorf("prepare malformed mandatory or unclassified generic schedule %s: %w", wakeup.ActivationID(), malformed)
			}
			runID, runErr := timerRunIDTx(ctx, tx, wakeup.ActivationID())
			if runErr != nil {
				return runtimegenericschedule.PreparedOccurrence{}, runErr
			}
			if failErr := failMalformedByIDTx(ctx, tx, postgres, wakeup.ActivationID(), malformed, admittedAt); failErr != nil {
				return runtimegenericschedule.PreparedOccurrence{}, failErr
			}
			if addErr := addTimerEffect(effects, runID); addErr != nil {
				return runtimegenericschedule.PreparedOccurrence{}, addErr
			}
			return runtimegenericschedule.PreparedOccurrence{Outcome: runtimegenericschedule.PrepareTerminal}, nil
		}
		return runtimegenericschedule.PreparedOccurrence{}, err
	}
	if !found {
		return runtimegenericschedule.PreparedOccurrence{Outcome: runtimegenericschedule.PrepareTerminal}, nil
	}
	if activation.Status == runtimegenericschedule.StatusCancelled {
		return runtimegenericschedule.PreparedOccurrence{Outcome: runtimegenericschedule.PrepareStaleCancelled, Activation: activation}, nil
	}
	if activation.Status != runtimegenericschedule.StatusActive || !activation.CurrentDueAt.Equal(wakeup.DueAt()) {
		return runtimegenericschedule.PreparedOccurrence{Outcome: runtimegenericschedule.PrepareTerminal, Activation: activation}, nil
	}
	if activation.Command.RunID != "" {
		if postgres {
			err = storerunstate.RequirePostgresActiveTx(ctx, tx, activation.Command.RunID)
		} else {
			err = storerunstate.RequireSQLiteActiveTx(ctx, tx, activation.Command.RunID)
		}
		if errors.Is(err, runtimerunlifecycle.ErrRunNotActive) {
			activation, err = cancelLoadedTx(ctx, tx, dialectFor(postgres), activation, "run_terminalized", admittedAt)
			if err != nil {
				return runtimegenericschedule.PreparedOccurrence{}, err
			}
			if err := addTimerEffect(effects, activation.Command.RunID); err != nil {
				return runtimegenericschedule.PreparedOccurrence{}, err
			}
			return runtimegenericschedule.PreparedOccurrence{Outcome: runtimegenericschedule.PrepareStaleCancelled, Activation: activation}, nil
		}
		if errors.Is(err, runtimerunlifecycle.ErrRunNotFound) {
			activation, err = failLoadedTx(ctx, tx, dialectFor(postgres), activation, "missing_run", "generic schedule run linkage is missing", admittedAt)
			if err != nil {
				return runtimegenericschedule.PreparedOccurrence{}, err
			}
			if err := addTimerEffect(effects, activation.Command.RunID); err != nil {
				return runtimegenericschedule.PreparedOccurrence{}, err
			}
			return runtimegenericschedule.PreparedOccurrence{Outcome: runtimegenericschedule.PrepareTerminal, Activation: activation}, nil
		}
		if err != nil {
			return runtimegenericschedule.PreparedOccurrence{}, err
		}
	}
	if activation.CurrentEventID == "" {
		activation.CurrentEventID = runtimegenericschedule.OccurrenceEventID(activation.ID, activation.CurrentDueAt)
		activation.CurrentEventAdmittedAt = canonicalTime(admittedAt)
		if activation.CurrentEventAdmittedAt.IsZero() {
			return runtimegenericschedule.PreparedOccurrence{}, errors.New("generic schedule occurrence admission clock returned zero")
		}
		if err := stampOccurrenceTx(ctx, tx, dialectFor(postgres), activation); err != nil {
			return runtimegenericschedule.PreparedOccurrence{}, err
		}
		if err := addTimerEffect(effects, activation.Command.RunID); err != nil {
			return runtimegenericschedule.PreparedOccurrence{}, err
		}
	}
	occurrence := runtimegenericschedule.Occurrence{
		ActivationID: activation.ID, DueAt: activation.CurrentDueAt,
		EventID: activation.CurrentEventID, AdmittedAt: activation.CurrentEventAdmittedAt,
	}
	result := runtimegenericschedule.PreparedOccurrence{Outcome: runtimegenericschedule.PrepareReady, Activation: activation, Occurrence: occurrence}
	return result, result.Validate()
}

func addTimerEffect(effects *privaterunforkrevision.Effects, runID string) error {
	if effects == nil {
		return errors.New("generic schedule mutation requires revision effects")
	}
	if strings.TrimSpace(runID) == "" {
		return nil
	}
	return effects.Add(runID, privaterunforkrevision.FamilyTimers)
}

func (o *PostgresOwner) CancelGenericSchedule(ctx context.Context, command runtimegenericschedule.CancelCommand) (runtimegenericschedule.CancelResult, error) {
	if err := o.requireSchema(); err != nil {
		return runtimegenericschedule.CancelResult{}, err
	}
	var result runtimegenericschedule.CancelResult
	err := o.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		effects := privaterunforkrevision.NewEffects()
		var err error
		result, err = CancelTx(txctx, tx, true, effects, command)
		if err != nil {
			return err
		}
		_, err = privaterunforkrevision.FinalizePostgres(txctx, tx, effects)
		return err
	})
	return result, err
}

func (o *SQLiteOwner) CancelGenericSchedule(ctx context.Context, command runtimegenericschedule.CancelCommand) (runtimegenericschedule.CancelResult, error) {
	if err := o.requireSchema(); err != nil {
		return runtimegenericschedule.CancelResult{}, err
	}
	var result runtimegenericschedule.CancelResult
	err := o.backend.RunTransaction(ctx, "sqlite generic schedule cancellation", func(txctx context.Context, tx *sql.Tx) error {
		effects := privaterunforkrevision.NewEffects()
		var err error
		result, err = CancelTx(txctx, tx, false, effects, command)
		if err != nil {
			return err
		}
		_, err = privaterunforkrevision.FinalizeSQLite(txctx, tx, effects)
		return err
	})
	return result, err
}

func timerRunIDTx(ctx context.Context, tx *sql.Tx, activationID string) (string, error) {
	var runID sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT CAST(run_id AS TEXT) FROM timers WHERE timer_id=$1`, strings.TrimSpace(activationID)).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("resolve generic schedule affected run: %w", err)
	}
	return strings.TrimSpace(runID.String), nil
}

func CancelTx(ctx context.Context, tx *sql.Tx, postgres bool, effects *privaterunforkrevision.Effects, command runtimegenericschedule.CancelCommand) (runtimegenericschedule.CancelResult, error) {
	command = command.Canonical()
	if err := command.Validate(); err != nil {
		return runtimegenericschedule.CancelResult{}, err
	}
	if tx == nil || effects == nil {
		return runtimegenericschedule.CancelResult{}, errors.New("generic schedule cancellation requires transaction and revision effects")
	}
	activation, found, err := loadByIDTx(ctx, tx, dialectFor(postgres), command.ActivationID, postgres)
	if err != nil {
		return runtimegenericschedule.CancelResult{}, err
	}
	if !found {
		return runtimegenericschedule.CancelResult{Outcome: runtimegenericschedule.CancelMissing}, nil
	}
	if activation.Status != runtimegenericschedule.StatusActive {
		return runtimegenericschedule.CancelResult{Outcome: runtimegenericschedule.CancelTerminal, Activation: activation}, nil
	}
	activation, err = cancelLoadedTx(ctx, tx, dialectFor(postgres), activation, command.Cause, command.CancelledAt)
	if err != nil {
		return runtimegenericschedule.CancelResult{}, err
	}
	if activation.Command.RunID != "" {
		if err := effects.Add(activation.Command.RunID, privaterunforkrevision.FamilyTimers); err != nil {
			return runtimegenericschedule.CancelResult{}, err
		}
	}
	return runtimegenericschedule.CancelResult{Outcome: runtimegenericschedule.CancelChanged, Activation: activation}, nil
}

// CancelAdmissionTx cancels the exact immutable activation selected by the
// admission command. It is the private composition form for outer workflow
// transactions that know the stable schedule key but not the server-minted ID.
func CancelAdmissionTx(ctx context.Context, tx *sql.Tx, postgres bool, effects *privaterunforkrevision.Effects, command runtimegenericschedule.AdmissionCommand, cause string, cancelledAt time.Time) (runtimegenericschedule.CancelResult, error) {
	command = command.Canonical()
	cause = strings.TrimSpace(cause)
	cancelledAt = canonicalTime(cancelledAt)
	if err := command.Validate(); err != nil {
		return runtimegenericschedule.CancelResult{}, err
	}
	if tx == nil || effects == nil || cause == "" || cancelledAt.IsZero() {
		return runtimegenericschedule.CancelResult{}, errors.New("generic schedule admission cancellation requires transaction, revision effects, typed cause, and time")
	}
	scope, err := command.ScopeKey()
	if err != nil {
		return runtimegenericschedule.CancelResult{}, err
	}
	hash, err := command.ImmutableHash()
	if err != nil {
		return runtimegenericschedule.CancelResult{}, err
	}
	activation, found, err := loadByKeyTx(ctx, tx, dialectFor(postgres), scope, command.ScheduleKey, postgres)
	if err != nil {
		return runtimegenericschedule.CancelResult{}, err
	}
	if !found {
		return runtimegenericschedule.CancelResult{Outcome: runtimegenericschedule.CancelMissing}, nil
	}
	if activation.ImmutableHash != hash {
		return runtimegenericschedule.CancelResult{}, &runtimegenericschedule.ConflictError{ScopeKey: scope, ScheduleKey: command.ScheduleKey}
	}
	if activation.Status != runtimegenericschedule.StatusActive {
		return runtimegenericschedule.CancelResult{Outcome: runtimegenericschedule.CancelTerminal, Activation: activation}, nil
	}
	activation, err = cancelLoadedTx(ctx, tx, dialectFor(postgres), activation, cause, cancelledAt)
	if err != nil {
		return runtimegenericschedule.CancelResult{}, err
	}
	if activation.Command.RunID != "" {
		if err := effects.Add(activation.Command.RunID, privaterunforkrevision.FamilyTimers); err != nil {
			return runtimegenericschedule.CancelResult{}, err
		}
	}
	return runtimegenericschedule.CancelResult{Outcome: runtimegenericschedule.CancelChanged, Activation: activation}, nil
}

func (o *PostgresOwner) CancelAdmissionTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, command runtimegenericschedule.AdmissionCommand, cause string, cancelledAt time.Time) (runtimegenericschedule.CancelResult, error) {
	return CancelAdmissionTx(ctx, tx, true, effects, command, cause, cancelledAt)
}

func (o *SQLiteOwner) CancelAdmissionTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, command runtimegenericschedule.AdmissionCommand, cause string, cancelledAt time.Time) (runtimegenericschedule.CancelResult, error) {
	return CancelAdmissionTx(ctx, tx, false, effects, command, cause, cancelledAt)
}

func CancelRunsTx(ctx context.Context, tx *sql.Tx, postgres bool, effects *privaterunforkrevision.Effects, runIDs []string, cause string, cancelledAt time.Time) ([]runtimetimercancellation.Ref, error) {
	if tx == nil || effects == nil || strings.TrimSpace(cause) == "" || canonicalTime(cancelledAt).IsZero() {
		return nil, errors.New("generic schedule run cancellation requires transaction, revision effects, cause, and time")
	}
	ids := normalizedIDs(runIDs)
	if len(ids) == 0 {
		return nil, nil
	}
	query, args := runActivationQuery(postgres, ids)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	refs := make([]runtimetimercancellation.Ref, 0)
	for rows.Next() {
		var ref runtimetimercancellation.Ref
		var due any
		if err := rows.Scan(&ref.ActivationID, &due, &ref.RunID, &ref.TaskID); err != nil {
			return nil, err
		}
		ref.Family = runtimetimercancellation.FamilyGenericSchedule
		if ref.DueAt, _, err = timeValue(due); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, ref := range refs {
		if _, err := CancelTx(ctx, tx, postgres, effects, runtimegenericschedule.CancelCommand{ActivationID: ref.ActivationID, Cause: cause, CancelledAt: cancelledAt}); err != nil {
			return nil, err
		}
	}
	return refs, nil
}

type queryRowContext interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type queryContext interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func loadByID(ctx context.Context, query queryRowContext, d dialect, activationID string) (runtimegenericschedule.Activation, bool, error) {
	return scanActivation(query.QueryRowContext(ctx, activationSelectByID(d), strings.TrimSpace(activationID)), d)
}

func loadByIDTx(ctx context.Context, tx *sql.Tx, d dialect, activationID string, lock bool) (runtimegenericschedule.Activation, bool, error) {
	query := activationSelectByID(d)
	if lock && d == postgresDialect {
		query += " FOR UPDATE"
	}
	return scanActivation(tx.QueryRowContext(ctx, query, strings.TrimSpace(activationID)), d)
}

func loadByKeyTx(ctx context.Context, tx *sql.Tx, d dialect, scope, key string, lock bool) (runtimegenericschedule.Activation, bool, error) {
	query := activationSelectColumns + ` FROM timers WHERE schedule_scope = ? AND schedule_key = ? AND task_type IN ('timer','scheduled_task','global_recurring')`
	if d == postgresDialect {
		query = activationSelectColumns + ` FROM timers WHERE schedule_scope = $1 AND schedule_key = $2 AND task_type IN ('timer','scheduled_task','global_recurring')`
		if lock {
			query += " FOR UPDATE"
		}
	}
	return scanActivation(tx.QueryRowContext(ctx, query, scope, key), d)
}

func listActiveIDs(ctx context.Context, query queryContext) ([]string, error) {
	statement := `SELECT CAST(timer_id AS TEXT) FROM timers WHERE task_type IN ('timer','scheduled_task','global_recurring') AND status = 'active' ORDER BY fire_at, timer_id`
	rows, err := query.QueryContext(ctx, statement)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]string, 0)
	for rows.Next() {
		var activationID string
		if err := rows.Scan(&activationID); err != nil {
			return nil, err
		}
		result = append(result, activationID)
	}
	return result, rows.Err()
}

const activationSelectColumns = `SELECT
CAST(timer_id AS TEXT), schedule_key, immutable_hash, COALESCE(CAST(run_id AS TEXT), ''),
COALESCE(CAST(entity_id AS TEXT), ''), COALESCE(flow_instance, ''), owner_kind, owner_agent,
COALESCE(agent_name_owner, ''), COALESCE(agent_name_source, ''), COALESCE(agent_route_presence, ''),
COALESCE(agent_flow_scope_key, ''), COALESCE(agent_flow_instance_id, ''), fire_event, fire_payload,
routing_source, execution_mode, COALESCE(reply_context_id, ''), due_basis_kind, due_basis_absolute,
COALESCE(due_basis_duration, ''), COALESCE(due_basis_cron, ''), COALESCE(task_id, ''),
immutable_hash, created_at, initial_fire_at, fire_at, COALESCE(CAST(occurrence_event_id AS TEXT), ''),
occurrence_admitted_at, status, COALESCE(cancel_cause, ''), cancelled_at, fired_at, accepted_at,
failed_at, COALESCE(failure_code, ''), COALESCE(failure_message, '')`

func activationSelectByID(d dialect) string {
	if d == postgresDialect {
		return activationSelectColumns + ` FROM timers WHERE timer_id = $1::uuid AND task_type IN ('timer','scheduled_task','global_recurring')`
	}
	return activationSelectColumns + ` FROM timers WHERE timer_id = ? AND task_type IN ('timer','scheduled_task','global_recurring')`
}

type rowScanner interface {
	Scan(...any) error
}

func scanActivation(row rowScanner, d dialect) (runtimegenericschedule.Activation, bool, error) {
	activation, err := scanActivationRow(row, d)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimegenericschedule.Activation{}, false, nil
	}
	return activation, err == nil, err
}

func scanActivationRow(row rowScanner, _ dialect) (runtimegenericschedule.Activation, error) {
	var (
		activation                                                                    runtimegenericschedule.Activation
		scheduleKey, runID, entityID, flowInstance, ownerKind, ownerID                string
		nameOwner, nameSource, routePresence, flowScopeKey, flowInstanceID            string
		eventType, executionMode, replyContext, dueKind, dueDuration, dueCron, taskID string
		immutableHash, immutableHashDuplicate, occurrenceEventID, status              string
		payloadRaw, routingRaw                                                        any
		dueAbsoluteRaw, createdRaw, initialRaw, currentRaw, occurrenceAdmittedRaw     any
		cancelCause, failureCode, failureMessage                                      string
		cancelledRaw, firedRaw, acceptedRaw, failedRaw                                any
	)
	err := row.Scan(
		&activation.ID, &scheduleKey, &immutableHash, &runID, &entityID, &flowInstance, &ownerKind, &ownerID,
		&nameOwner, &nameSource, &routePresence, &flowScopeKey, &flowInstanceID, &eventType, &payloadRaw,
		&routingRaw, &executionMode, &replyContext, &dueKind, &dueAbsoluteRaw, &dueDuration, &dueCron, &taskID,
		&immutableHashDuplicate, &createdRaw, &initialRaw, &currentRaw, &occurrenceEventID,
		&occurrenceAdmittedRaw, &status, &cancelCause, &cancelledRaw, &firedRaw, &acceptedRaw,
		&failedRaw, &failureCode, &failureMessage,
	)
	if err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	family := classifyPersistedScheduleFamily(eventType, payloadRaw)
	activation.Command.ExecutionMode = executionmode.Mode(executionMode)
	malformed := func(err error) (runtimegenericschedule.Activation, error) {
		return activation, &malformedActivationError{cause: err, family: family}
	}
	if immutableHash != immutableHashDuplicate {
		return malformed(errors.New("generic schedule immutable hash projection is inconsistent"))
	}
	payload, err := canonicaljson.Decode(jsonBytes(payloadRaw))
	if err != nil {
		return malformed(fmt.Errorf("decode generic schedule payload: %w", err))
	}
	var routing events.RoutingSource
	if err := json.Unmarshal(jsonBytes(routingRaw), &routing); err != nil {
		return malformed(fmt.Errorf("decode generic schedule routing source: %w", err))
	}
	identity := agentidentity.Identity{}
	if runtimegenericschedule.OwnerKind(ownerKind) == runtimegenericschedule.OwnerAgent {
		identity, err = agentidentity.FromStorageFields(agentidentity.StorageFields{
			AgentID: ownerID, NameOwner: nameOwner, NameSource: nameSource, RoutePresence: routePresence,
			FlowScopeKey: flowScopeKey, FlowInstanceID: flowInstanceID, FlowInstancePath: flowInstance,
		})
		if err != nil {
			return malformed(err)
		}
	}
	due, err := decodeDueBasis(dueKind, dueAbsoluteRaw, dueDuration, dueCron)
	if err != nil {
		return malformed(err)
	}
	activation.Command = runtimegenericschedule.AdmissionCommand{
		ScheduleKey: scheduleKey, RunID: runID, EntityID: entityID, FlowInstance: flowInstance,
		OwnerKind: runtimegenericschedule.OwnerKind(ownerKind), OwnerID: ownerID, AgentIdentity: identity,
		EventType: eventType, Payload: payload, RoutingSource: routing, ExecutionMode: executionmode.Mode(executionMode),
		ReplyContext: replyContext, Due: due, TaskID: taskID,
	}
	activation.ImmutableHash = immutableHash
	activation.CurrentEventID = occurrenceEventID
	activation.Status = runtimegenericschedule.Status(status)
	activation.CancelCause = cancelCause
	activation.Failure = runtimegenericschedule.Failure{Code: failureCode, Message: failureMessage}
	if activation.AdmittedAt, _, err = timeValue(createdRaw); err != nil {
		return malformed(err)
	}
	if activation.InitialDueAt, _, err = timeValue(initialRaw); err != nil {
		return malformed(err)
	}
	if activation.CurrentDueAt, _, err = timeValue(currentRaw); err != nil {
		return malformed(err)
	}
	if activation.CurrentEventAdmittedAt, _, err = timeValue(occurrenceAdmittedRaw); err != nil {
		return malformed(err)
	}
	if activation.CancelledAt, _, err = timeValue(cancelledRaw); err != nil {
		return malformed(err)
	}
	if activation.FiredAt, _, err = timeValue(firedRaw); err != nil {
		return malformed(err)
	}
	if activation.AcceptedAt, _, err = timeValue(acceptedRaw); err != nil {
		return malformed(err)
	}
	if activation.FailedAt, _, err = timeValue(failedRaw); err != nil {
		return malformed(err)
	}
	activation = activation.Canonical()
	if err := activation.Validate(); err != nil {
		return malformed(err)
	}
	return activation, nil
}

func insertActivationTx(ctx context.Context, tx *sql.Tx, d dialect, scope string, activation runtimegenericschedule.Activation) (bool, error) {
	payload, err := canonicaljson.Encode(activation.Command.Payload)
	if err != nil {
		return false, err
	}
	routing, err := json.Marshal(activation.Command.RoutingSource)
	if err != nil {
		return false, err
	}
	identity := agentidentity.StorageFields{}
	if !activation.Command.AgentIdentity.IsZero() {
		identity, err = activation.Command.AgentIdentity.StorageFields()
		if err != nil {
			return false, err
		}
	}
	dueAbsolute, dueDuration, dueCron := encodeDueBasis(activation.Command.Due)
	taskType := genericTaskType(activation.Command)
	query := `INSERT INTO timers (
		timer_id, timer_name, schedule_scope, schedule_key, immutable_hash, run_id, entity_id, flow_instance,
		fire_event, fire_payload, routing_source, execution_mode, fire_at, initial_fire_at, recurring,
		owner_agent, owner_kind, agent_name_owner, agent_name_source,
		agent_route_presence, agent_flow_scope_key, agent_flow_instance_id, reply_context_id, task_id,
		due_basis_kind, due_basis_absolute, due_basis_duration, due_basis_cron, task_type, status, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?,''), NULLIF(?,''),
		NULLIF(?,''), NULLIF(?,''), NULLIF(?,''), NULLIF(?,''), NULLIF(?,''), ?, ?, NULLIF(?,''), NULLIF(?,''), ?, 'active', ?)
	ON CONFLICT(schedule_scope, schedule_key) DO NOTHING`
	args := []any{
		activation.ID, activation.Command.ScheduleKey, scope, activation.Command.ScheduleKey, activation.ImmutableHash,
		nullString(activation.Command.RunID), nullString(activation.Command.EntityID), nullString(activation.Command.FlowInstance),
		activation.Command.EventType, string(payload), string(routing), activation.Command.ExecutionMode,
		activation.CurrentDueAt, activation.InitialDueAt,
		activation.Command.Due.Recurring(),
		activation.Command.OwnerID, activation.Command.OwnerKind, identity.NameOwner, identity.NameSource, identity.RoutePresence,
		identity.FlowScopeKey, identity.FlowInstanceID, activation.Command.ReplyContext, activation.Command.TaskID,
		activation.Command.Due.Kind, dueAbsolute, dueDuration, dueCron, taskType, activation.AdmittedAt,
	}
	if d == postgresDialect {
		query = `INSERT INTO timers (
			timer_id, timer_name, schedule_scope, schedule_key, immutable_hash, run_id, entity_id, flow_instance,
			fire_event, fire_payload, routing_source, execution_mode, fire_at, initial_fire_at, recurring,
			owner_agent, owner_kind, agent_name_owner, agent_name_source,
			agent_route_presence, agent_flow_scope_key, agent_flow_instance_id, reply_context_id, task_id,
			due_basis_kind, due_basis_absolute, due_basis_duration, due_basis_cron, task_type, status, created_at
		) VALUES ($1::uuid, $2, $3, $4, $5, NULLIF($6,'')::uuid, NULLIF($7,'')::uuid, NULLIF($8,''),
			$9, $10::jsonb, $11::jsonb, $12, $13, $14, $15, $16, $17,
			NULLIF($18,''), NULLIF($19,''), NULLIF($20,''), NULLIF($21,''), NULLIF($22,''), NULLIF($23,''),
			NULLIF($24,''), $25, $26, NULLIF($27,''), NULLIF($28,''), $29, 'active', $30)
		ON CONFLICT(schedule_scope, schedule_key) DO NOTHING`
		args[5] = activation.Command.RunID
		args[6] = activation.Command.EntityID
		args[22] = activation.Command.ReplyContext
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("insert generic schedule activation: %w", err)
	}
	rows, err := result.RowsAffected()
	return rows == 1, err
}

func stampOccurrenceTx(ctx context.Context, tx *sql.Tx, d dialect, activation runtimegenericschedule.Activation) error {
	query := `UPDATE timers SET occurrence_event_id = ?, occurrence_admitted_at = ?
		WHERE timer_id = ? AND task_type IN ('timer','scheduled_task','global_recurring') AND status = 'active'
		AND fire_at = ? AND occurrence_event_id IS NULL AND occurrence_admitted_at IS NULL`
	args := []any{activation.CurrentEventID, activation.CurrentEventAdmittedAt, activation.ID, activation.CurrentDueAt}
	if d == postgresDialect {
		query = `UPDATE timers SET occurrence_event_id = $1::uuid, occurrence_admitted_at = $2
			WHERE timer_id = $3::uuid AND task_type IN ('timer','scheduled_task','global_recurring') AND status = 'active'
			AND fire_at = $4 AND occurrence_event_id IS NULL AND occurrence_admitted_at IS NULL`
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("generic schedule occurrence stamp changed %d rows", rows)
	}
	return nil
}

func cancelLoadedTx(ctx context.Context, tx *sql.Tx, d dialect, activation runtimegenericschedule.Activation, cause string, at time.Time) (runtimegenericschedule.Activation, error) {
	at = canonicalTime(at)
	query := `UPDATE timers SET status = 'cancelled', cancel_cause = ?, cancelled_at = ?
		WHERE timer_id = ? AND task_type IN ('timer','scheduled_task','global_recurring') AND status = 'active'`
	args := []any{strings.TrimSpace(cause), at, activation.ID}
	if d == postgresDialect {
		query = `UPDATE timers SET status = 'cancelled', cancel_cause = $1, cancelled_at = $2
			WHERE timer_id = $3::uuid AND task_type IN ('timer','scheduled_task','global_recurring') AND status = 'active'`
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return runtimegenericschedule.Activation{}, fmt.Errorf("generic schedule cancellation changed %d rows: %w", rows, err)
	}
	activation.Status = runtimegenericschedule.StatusCancelled
	activation.CancelCause = strings.TrimSpace(cause)
	activation.CancelledAt = at
	return activation.Canonical(), activation.Validate()
}

func failLoadedTx(ctx context.Context, tx *sql.Tx, d dialect, activation runtimegenericschedule.Activation, code, message string, at time.Time) (runtimegenericschedule.Activation, error) {
	at = canonicalTime(at)
	query := `UPDATE timers SET status = 'failed', failure_code = ?, failure_message = ?, failed_at = ?
		WHERE timer_id = ? AND task_type IN ('timer','scheduled_task','global_recurring') AND status = 'active'`
	args := []any{strings.TrimSpace(code), strings.TrimSpace(message), at, activation.ID}
	if d == postgresDialect {
		query = `UPDATE timers SET status = 'failed', failure_code = $1, failure_message = $2, failed_at = $3
			WHERE timer_id = $4::uuid AND task_type IN ('timer','scheduled_task','global_recurring') AND status = 'active'`
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return runtimegenericschedule.Activation{}, fmt.Errorf("generic schedule failure changed %d rows: %w", rows, err)
	}
	activation.Status = runtimegenericschedule.StatusFailed
	activation.Failure = runtimegenericschedule.Failure{Code: strings.TrimSpace(code), Message: strings.TrimSpace(message)}
	activation.FailedAt = at
	return activation.Canonical(), activation.Validate()
}

func failMalformedByIDTx(ctx context.Context, tx *sql.Tx, postgres bool, activationID string, malformed error, at time.Time) error {
	activationID = strings.TrimSpace(activationID)
	at = canonicalTime(at)
	if tx == nil || activationID == "" || malformed == nil || at.IsZero() {
		return errors.New("malformed generic schedule terminalization requires transaction, activation, cause, and time")
	}
	message := "generic schedule durable activation is malformed: " + malformed.Error()
	query := `UPDATE timers SET status = 'failed', failure_code = ?, failure_message = ?, failed_at = ?
		WHERE timer_id = ? AND task_type IN ('timer','scheduled_task','global_recurring') AND status = 'active'`
	args := []any{"malformed_persisted_activation", message, at, activationID}
	if postgres {
		query = `UPDATE timers SET status = 'failed', failure_code = $1, failure_message = $2, failed_at = $3
			WHERE timer_id = $4::uuid AND task_type IN ('timer','scheduled_task','global_recurring') AND status = 'active'`
	}
	_, err := tx.ExecContext(ctx, query, args...)
	return err
}

func FailActivationTx(ctx context.Context, tx *sql.Tx, postgres bool, activation runtimegenericschedule.Activation, code, message string, at time.Time) (runtimegenericschedule.Activation, error) {
	if tx == nil {
		return runtimegenericschedule.Activation{}, errors.New("generic schedule failure requires transaction")
	}
	return failLoadedTx(ctx, tx, dialectFor(postgres), activation, code, message, at)
}

func decodeDueBasis(kind string, absoluteRaw any, duration, cronSpec string) (runtimegenericschedule.DueBasis, error) {
	switch runtimegenericschedule.DueBasisKind(strings.TrimSpace(kind)) {
	case runtimegenericschedule.DueAbsolute:
		at, present, err := timeValue(absoluteRaw)
		if err != nil || !present {
			return runtimegenericschedule.DueBasis{}, errors.New("absolute generic schedule is missing due basis timestamp")
		}
		return runtimegenericschedule.AbsoluteDue(at), nil
	case runtimegenericschedule.DueDelay:
		parsed, err := time.ParseDuration(strings.TrimSpace(duration))
		if err != nil {
			return runtimegenericschedule.DueBasis{}, err
		}
		return runtimegenericschedule.DelayDue(parsed), nil
	case runtimegenericschedule.DueCron:
		return runtimegenericschedule.CronDue(cronSpec), nil
	case runtimegenericschedule.DueEvery:
		parsed, err := time.ParseDuration(strings.TrimSpace(duration))
		if err != nil {
			return runtimegenericschedule.DueBasis{}, err
		}
		return runtimegenericschedule.EveryDue(parsed), nil
	default:
		return runtimegenericschedule.DueBasis{}, fmt.Errorf("generic schedule due basis kind %q is invalid", kind)
	}
}

func encodeDueBasis(due runtimegenericschedule.DueBasis) (any, string, string) {
	due = due.Canonical()
	switch due.Kind {
	case runtimegenericschedule.DueAbsolute:
		return due.Absolute, "", ""
	case runtimegenericschedule.DueDelay:
		return nil, due.Delay.String(), ""
	case runtimegenericschedule.DueCron:
		return nil, "", due.Cron
	case runtimegenericschedule.DueEvery:
		return nil, due.Every.String(), ""
	default:
		return nil, "", ""
	}
}

func genericTaskType(command runtimegenericschedule.AdmissionCommand) string {
	if !command.Due.Recurring() {
		return "timer"
	}
	if command.RunID == "" {
		return "global_recurring"
	}
	return "scheduled_task"
}

func nullString(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}

func jsonBytes(raw any) []byte {
	switch value := raw.(type) {
	case []byte:
		return append([]byte(nil), value...)
	case string:
		return []byte(value)
	case json.RawMessage:
		return append([]byte(nil), value...)
	default:
		encoded, _ := json.Marshal(value)
		return encoded
	}
}

func timeValue(raw any) (time.Time, bool, error) {
	if raw == nil {
		return time.Time{}, false, nil
	}
	switch value := raw.(type) {
	case time.Time:
		return canonicalTime(value), true, nil
	case string:
		value = strings.TrimSpace(value)
		if value == "" {
			return time.Time{}, false, nil
		}
		layouts := [...]string{
			time.RFC3339Nano,
			"2006-01-02 15:04:05.999999999 -0700 MST",
			"2006-01-02 15:04:05.999999 -0700 MST",
			"2006-01-02 15:04:05 -0700 MST",
			"2006-01-02 15:04:05.999999999-07:00",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05",
		}
		for _, layout := range layouts {
			if parsed, err := time.Parse(layout, value); err == nil {
				return canonicalTime(parsed), true, nil
			}
		}
		return time.Time{}, false, fmt.Errorf("invalid generic schedule time %q", value)
	case []byte:
		return timeValue(string(value))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported generic schedule time value %T", raw)
	}
}

func canonicalTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.UTC().Truncate(time.Microsecond)
}

func dialectFor(postgres bool) dialect {
	if postgres {
		return postgresDialect
	}
	return sqliteDialect
}

func normalizedIDs(values []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func runActivationQuery(postgres bool, ids []string) (string, []any) {
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
		if postgres {
			placeholders[i] = fmt.Sprintf("$%d::uuid", i+1)
		} else {
			placeholders[i] = "?"
		}
	}
	query := `SELECT CAST(timer_id AS TEXT), fire_at, CAST(run_id AS TEXT), COALESCE(timer_name, '') FROM timers
		WHERE run_id IN (` + strings.Join(placeholders, ",") + `)
		AND task_type IN ('timer','scheduled_task','global_recurring') AND status = 'active'
		ORDER BY timer_id`
	if postgres {
		query += " FOR UPDATE"
	}
	return query, args
}
