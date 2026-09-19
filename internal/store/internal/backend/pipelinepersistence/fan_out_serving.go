package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

// FanOutAdmission is installed only by private selected-store assembly. The
// startup owner retains interpretation of process, source-set and run grants.
type FanOutAdmission interface {
	ObserveFanOutGrantsTx(context.Context, *sql.Tx, []startupownership.GrantEvidence) error
	ObserveFanOutRunTx(context.Context, *sql.Tx, startupownership.GrantEvidence, string) (bool, error)
	AdmitFanOutRunTx(context.Context, *sql.Tx, startupownership.GrantEvidence, string) (bool, error)
	AdmitFanOutCleanupTx(context.Context, *sql.Tx, startupownership.GrantEvidence) error
}

// FanOutReadiness reads current durable execution evidence for global work
// observation. It never grants mutation authority or counts an owed suffix.
type FanOutReadiness interface {
	CurrentFanOutGrantsTx(context.Context, *sql.Tx) ([]startupownership.GrantEvidence, error)
}

// ErrFanOutRegistrationPending is a lawful admit-before-register observation,
// not corruption. Selection must wait rather than narrow the complete census.
var ErrFanOutRegistrationPending = errors.New("fan-out ordinary execution registration is pending")

func (s *PipelinePostgresOwner) BindFanOutReadiness(reader FanOutReadiness) error {
	if reader == nil || s.fanOutReadiness != nil {
		return errors.New("fan-out readiness must be assembled exactly once")
	}
	s.fanOutReadiness = reader
	return nil
}

func (s *PipelineSQLiteOwner) BindFanOutReadiness(reader FanOutReadiness) error {
	if reader == nil || s.fanOutReadiness != nil {
		return errors.New("fan-out readiness must be assembled exactly once")
	}
	s.fanOutReadiness = reader
	return nil
}

type fanOutServingStore struct {
	postgres  *PipelinePostgresOwner
	sqlite    *PipelineSQLiteOwner
	admission FanOutAdmission
}

type fanOutPostgresOwner struct {
	*PipelinePostgresOwner
	grant             startupownership.GrantEvidence
	admission         FanOutAdmission
	publicationGroups fanOutPublicationGroups
}

type fanOutSQLiteOwner struct {
	*PipelineSQLiteOwner
	grant             startupownership.GrantEvidence
	admission         FanOutAdmission
	publicationGroups fanOutPublicationGroups
}

func (s *PipelinePostgresOwner) NewFanOutServingStore(admission FanOutAdmission) (startupownership.FanOutServingStore, error) {
	if s == nil || s.backend == nil || admission == nil {
		return nil, errors.New("fan-out serving requires pipeline and selected-store admission")
	}
	return &fanOutServingStore{postgres: s, admission: admission}, nil
}

func (s *PipelineSQLiteOwner) NewFanOutServingStore(admission FanOutAdmission) (startupownership.FanOutServingStore, error) {
	if s == nil || s.backend == nil || admission == nil {
		return nil, errors.New("fan-out serving requires pipeline and selected-store admission")
	}
	return &fanOutServingStore{sqlite: s, admission: admission}, nil
}

func (s *fanOutServingStore) BindFanOutGrant(grant startupownership.GrantEvidence) (pipeline.FanOutObligationOwner, error) {
	if err := grant.Validate(); err != nil {
		return nil, err
	}
	if grant.State != startupownership.GrantAdmitted || grant.SelectedFork != nil {
		return nil, errors.New("fan-out serving requires an admitted ordinary runtime grant")
	}
	grant.ProbeSurfaceIDs = append([]string(nil), grant.ProbeSurfaceIDs...)
	if s.postgres != nil {
		return &fanOutPostgresOwner{PipelinePostgresOwner: s.postgres, grant: grant, admission: s.admission}, nil
	}
	return &fanOutSQLiteOwner{PipelineSQLiteOwner: s.sqlite, grant: grant, admission: s.admission}, nil
}

// All acknowledged evidence reaches admission before closing registrations are
// excluded. A closing flag can only remove serving eligibility.
func fanOutRegistrationGrants(registrations []startupownership.FanOutRegistrationObservation, activeOnly bool) []startupownership.GrantEvidence {
	grants := make([]startupownership.GrantEvidence, 0, len(registrations))
	for _, registration := range registrations {
		if !activeOnly || !registration.Closing {
			grants = append(grants, registration.Grant)
		}
	}
	return grants
}

func (s *fanOutServingStore) NextFanOutCandidate(ctx context.Context, registrations []startupownership.FanOutRegistrationObservation, exclude []fanoutobligation.IntentKey) (candidate startupownership.FanOutCandidate, found bool, err error) {
	grants := fanOutRegistrationGrants(registrations, false)
	operation := func(ctx context.Context, tx *sql.Tx) error {
		transactiontest.Mark(ctx, transactiontest.FanOutObservation)
		if err := s.admission.ObserveFanOutGrantsTx(ctx, tx, grants); err != nil {
			if errors.Is(err, ErrFanOutRegistrationPending) {
				return nil
			}
			return err
		}
		now := time.Now
		if s.sqlite != nil {
			now = s.sqlite.now
		}
		candidate, found, err = observeFanOutCandidateTx(ctx, tx, s.postgres != nil, now, fanOutRegistrationGrants(registrations, true), exclude)
		if err != nil || !found {
			return err
		}
		for _, grant := range grants {
			if grant.GrantID == candidate.GrantID {
				found, err = s.admission.ObserveFanOutRunTx(ctx, tx, grant, candidate.Key.RunID)
				return err
			}
		}
		return errors.New("fan-out candidate has no exact grant")
	}
	if s.postgres != nil {
		err = s.postgres.backend.RunReadTransaction(ctx, operation)
	} else {
		err = s.sqlite.backend.RunReadTransaction(ctx, operation)
	}
	if err != nil {
		return startupownership.FanOutCandidate{}, false, err
	}
	return candidate, found, nil
}

func observeFanOutCandidateTx(ctx context.Context, tx *sql.Tx, postgres bool, now func() time.Time, grants []startupownership.GrantEvidence, exclude []fanoutobligation.IntentKey) (candidate startupownership.FanOutCandidate, found bool, err error) {
	operation := func() error {
		if len(grants) == 0 {
			return nil
		}
		at, err := fanOutAdmissionTime(ctx, tx, postgres, now)
		if err != nil {
			return err
		}
		args := []any{at}
		// Both the shared selector and global-work projection consume this
		// exact current-grant/run join and the same temporary readiness facts.
		query := `SELECT g.grant_id, ` + fanOutObservationHeaderColumns() +
			fanOutObservationFrom(grants, &args) + ` WHERE (` + fanOutReadinessReasonSQL + `)='eligible'`
		for _, key := range exclude {
			if err := key.Validate(); err != nil {
				return err
			}
			start := len(args) + 1
			args = append(args, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath)
			query += fmt.Sprintf(` AND NOT (i.run_id=$%d AND i.triggering_delivery_id=$%d AND i.flow_path=$%d AND i.declaration_family=$%d AND i.semantic_path=$%d)`, start, start+1, start+2, start+3, start+4)
		}
		// Text identity ties must not inherit the PostgreSQL database locale.
		collation := ` COLLATE BINARY`
		if postgres {
			collation = ` COLLATE "C"`
		}
		query += ` ORDER BY COALESCE(i.last_served_at,i.created_at),i.created_at,i.run_id,i.triggering_delivery_id,i.flow_path` + collation + `,i.declaration_family` + collation + `,i.semantic_path` + collation + `,g.grant_id LIMIT 1`
		intent, scanErr := scanFanOutIntent(fanOutPrefixedRow{row: tx.QueryRowContext(ctx, query, args...), prefix: []any{&candidate.GrantID}})
		err = scanErr
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		state, err := intent.ServingAt(at)
		if err != nil {
			return err
		}
		if state != fanoutobligation.ServingEligible {
			return errors.New("fan-out candidate SQL prefilter disagrees with canonical serving state")
		}
		candidate.Key = intent.Request.Key
		candidate.CreatedAt = intent.CreatedAt
		candidate.PositionAt = intent.LastServedAt
		if candidate.PositionAt.IsZero() {
			candidate.PositionAt = intent.CreatedAt
		}
		found = true
		return nil
	}
	err = operation()
	if err != nil {
		return startupownership.FanOutCandidate{}, false, err
	}
	return candidate, found, nil
}

// SQL only prefilters candidate readiness. Every returned header is decoded by
// scanFanOutIntent and ServingAt before it can become a positive observation.
const fanOutExecutionReasonSQL = `CASE
	WHEN EXISTS (SELECT 1 FROM run_fork_selected_contract_bindings b WHERE b.fork_run_id=r.run_id) THEN 'selected_fork_reserved'
	WHEN g.grant_id IS NULL THEN 'runtime_unregistered'
	WHEN r.bundle_hash<>i.bundle_hash THEN 'run_not_owned'
	WHEN r.status<>'running' THEN 'run_' || r.status
	WHEN c.control_status IN ('paused','stopped') THEN 'run_' || c.control_status
	ELSE 'eligible' END`

const fanOutReadinessReasonSQL = `CASE
	WHEN (` + fanOutExecutionReasonSQL + `)<>'eligible' THEN (` + fanOutExecutionReasonSQL + `)
	WHEN i.status<>'open' THEN 'intent_' || i.status
	WHEN i.claim_owner IS NOT NULL AND (i.lease_expires_at IS NULL OR i.lease_expires_at>$1) THEN 'claim_in_flight'
	WHEN i.retry_ready_at>$1 THEN 'retry_wait'
	ELSE 'eligible' END`

func fanOutObservationHeaderColumns() string {
	columns := strings.Split(fanOutIntentColumns, ",")
	for i := range columns {
		columns[i] = "i." + strings.TrimSpace(columns[i])
	}
	return strings.Join(columns, ",")
}

type fanOutPrefixedRow struct {
	row    rowScanner
	prefix []any
}

func (r fanOutPrefixedRow) Scan(fields ...any) error {
	return r.row.Scan(append(r.prefix, fields...)...)
}

func fanOutCanonicalReadinessReason(intent fanoutobligation.Intent, at time.Time, executionReason string) (string, error) {
	state, err := intent.ServingAt(at)
	if err != nil {
		return "", err
	}
	if executionReason != "eligible" {
		return executionReason, nil
	}
	switch state {
	case fanoutobligation.ServingEligible:
		return "eligible", nil
	case fanoutobligation.ServingLeased:
		return "claim_in_flight", nil
	case fanoutobligation.ServingRetryWait:
		return "retry_wait", nil
	case fanoutobligation.ServingBlocked:
		return "intent_blocked", nil
	case fanoutobligation.ServingClosed:
		return "intent_closed", nil
	case fanoutobligation.ServingCanceled:
		return "intent_canceled", nil
	default:
		return "", errors.New("unknown canonical fan-out serving state")
	}
}

func fanOutObservationFrom(grants []startupownership.GrantEvidence, args *[]any) string {
	ids := make([]string, 0, len(grants))
	for _, grant := range grants {
		*args = append(*args, grant.GrantID)
		ids = append(ids, fmt.Sprintf("$%d", len(*args)))
	}
	grantFilter := "FALSE"
	if len(ids) > 0 {
		grantFilter = `g.grant_id IN (` + strings.Join(ids, ",") + `)`
	}
	return ` FROM fan_out_intents i JOIN runs r ON r.run_id=i.run_id
		LEFT JOIN run_control_state c ON c.run_id=r.run_id
		LEFT JOIN runtime_generation_grants g ON g.bundle_hash=r.bundle_hash AND ` + grantFilter + `
		AND g.state_version=(SELECT MAX(head.state_version) FROM runtime_generation_grants head WHERE head.grant_id=g.grant_id)
		AND g.state='admitted' AND g.selected_binding_id IS NULL`
}

func observeFanOutWorkTx(ctx context.Context, tx *sql.Tx, postgres bool, now func() time.Time, reader FanOutReadiness) (bool, error) {
	if reader == nil {
		return false, errors.New("fan-out work observation requires the assembled startup owner")
	}
	grants, err := reader.CurrentFanOutGrantsTx(ctx, tx)
	if err != nil {
		return false, err
	}
	_, found, err := observeFanOutCandidateTx(ctx, tx, postgres, now, grants, nil)
	return found, err
}

func (s *PipelinePostgresOwner) fanOutWorkPresence(ctx context.Context) (ready bool, err error) {
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		transactiontest.Mark(ctx, transactiontest.FanOutObservation)
		ready, err = observeFanOutWorkTx(ctx, tx, true, time.Now, s.fanOutReadiness)
		return err
	})
	return ready, err
}

func (s *PipelineSQLiteOwner) fanOutWorkPresence(ctx context.Context) (ready bool, err error) {
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		transactiontest.Mark(ctx, transactiontest.FanOutObservation)
		ready, err = observeFanOutWorkTx(ctx, tx, false, s.now, s.fanOutReadiness)
		return err
	})
	return ready, err
}

func validateFanOutCandidate(request pipeline.FanOutClaimRequest, grant startupownership.GrantEvidence) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if request.Candidate == nil || request.BundleHash != grant.BundleHash {
		return errors.New("fan-out claim requires the exact candidate and granted bundle")
	}
	return nil
}

func admitFanOutRun(ctx context.Context, tx *sql.Tx, admission FanOutAdmission, grant startupownership.GrantEvidence, runID string, newTurn bool) (bool, error) {
	owned, err := admission.AdmitFanOutRunTx(ctx, tx, grant, runID)
	if err != nil || !owned {
		return false, err
	}
	// InspectRunExecutionOwnershipTx holds the run lock, shared with pause/stop.
	return fanOutRunAcceptsTurnTx(ctx, tx, runID, newTurn)
}

func fanOutRunAcceptsTurnTx(ctx context.Context, tx *sql.Tx, runID string, newTurn bool) (bool, error) {
	var status, control string
	if err := tx.QueryRowContext(ctx, `SELECT r.status,COALESCE(c.control_status,'') FROM runs r LEFT JOIN run_control_state c ON c.run_id=r.run_id WHERE r.run_id=$1`, runID).Scan(&status, &control); err != nil {
		return false, err
	}
	return (status == "running" || (!newTurn && status == "paused")) && control != "stopped" && (!newTurn || control != "paused"), nil
}

func observeFanOutClaim(ctx context.Context, tx *sql.Tx, admission FanOutAdmission, grant startupownership.GrantEvidence, postgres bool, claim fanoutobligation.Claim, now func() time.Time) (fanoutobligation.Intent, error) {
	if err := claim.Validate(); err != nil {
		return fanoutobligation.Intent{}, err
	}
	owned, err := admission.ObserveFanOutRunTx(ctx, tx, grant, claim.Key.RunID)
	if err != nil {
		return fanoutobligation.Intent{}, err
	}
	if !owned {
		return fanoutobligation.Intent{}, fanoutobligation.ErrStaleClaim
	}
	ready, err := fanOutRunAcceptsTurnTx(ctx, tx, claim.Key.RunID, false)
	if err != nil {
		return fanoutobligation.Intent{}, err
	}
	if !ready {
		return fanoutobligation.Intent{}, fanoutobligation.ErrStaleClaim
	}
	intent, err := loadOwnedFanOutIntentTx(ctx, tx, postgres, claim, false)
	if err != nil {
		return fanoutobligation.Intent{}, err
	}
	if intent.Request.PlanRef.BundleHash != grant.BundleHash {
		return fanoutobligation.Intent{}, fanoutobligation.ErrStaleClaim
	}
	at, err := fanOutAdmissionTime(ctx, tx, postgres, now)
	if err != nil {
		return fanoutobligation.Intent{}, err
	}
	if err := intent.AdmitClaim(claim, at); err != nil {
		return fanoutobligation.Intent{}, err
	}
	return intent, nil
}

func admitFanOutClaim(ctx context.Context, tx *sql.Tx, admission FanOutAdmission, grant startupownership.GrantEvidence, postgres bool, claim fanoutobligation.Claim) error {
	if err := claim.Validate(); err != nil {
		return err
	}
	ready, err := admitFanOutRun(ctx, tx, admission, grant, claim.Key.RunID, false)
	if err != nil {
		return err
	}
	if !ready {
		return fanoutobligation.ErrStaleClaim
	}
	return requireFanOutClaimBundle(ctx, tx, postgres, claim, grant.BundleHash)
}

func requireFanOutClaimBundle(ctx context.Context, tx *sql.Tx, postgres bool, claim fanoutobligation.Claim, bundleHash string) error {
	intent, err := lockOwnedFanOutIntent(ctx, tx, postgres, claim)
	if err != nil {
		return err
	}
	if intent.Request.PlanRef.BundleHash != bundleHash {
		return fanoutobligation.ErrStaleClaim
	}
	return nil
}
