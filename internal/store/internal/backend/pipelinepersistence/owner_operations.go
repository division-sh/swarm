package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	runhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

const pipelineReplayClaimNamespace = "swarm:pipeline-replay:"

func replayClaimLockKey(eventID string) string {
	return pipelineReplayClaimNamespace + strings.TrimSpace(eventID)
}

func ReplayClaimLockKey(eventID string) string { return replayClaimLockKey(eventID) }

type pipelineClaimState struct {
	claim                         runtimepipelineobligation.Claim
	scanToken                     string
	postgresLease                 *postgresbackend.AdvisoryLockLease
	operationMu                   sync.Mutex
	testBeforeSQLiteOperationLock func()
	testAfterSQLiteOperationLock  func()
}

type pipelineScanState struct {
	mu         sync.Mutex
	scan       runtimepipelineobligation.Scan
	request    runtimepipelineobligation.ScanRequest
	phase      int
	after      *pipelineCandidate
	through    *pipelineCandidate
	bound      bool
	examined   map[int64]struct{}
	openClaims map[string]runtimepipelineobligation.Claim
	closed     bool
}

type postgresPipelineClaimRegistry struct {
	mu                          sync.Mutex
	issuer                      *runtimepipelineobligation.ClaimIssuer
	scanIssuer                  *runtimepipelineobligation.ScanIssuer
	claims                      map[string]*pipelineClaimState
	scans                       map[string]*pipelineScanState
	acquiring                   map[string]struct{}
	testBeforeClaimRegistryLock func()
	testAfterParentClaimScan    func()
	testConfigureClaimLease     func(*postgresbackend.AdvisoryLockLease)
}

type PipelineClaimStateForTest = pipelineClaimState
type PostgresPipelineClaimRegistryForTest = postgresPipelineClaimRegistry

func (s *pipelineClaimState) LeaseForTest() *postgresbackend.AdvisoryLockLease {
	if s == nil {
		return nil
	}
	return s.postgresLease
}

func (r *postgresPipelineClaimRegistry) SetHooksForTest(beforeClaimLock, afterParentScan func(), configureLease func(*postgresbackend.AdvisoryLockLease)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.testBeforeClaimRegistryLock = beforeClaimLock
	r.testAfterParentClaimScan = afterParentScan
	r.testConfigureClaimLease = configureLease
}

func (r *postgresPipelineClaimRegistry) LockForTest() func() {
	r.mu.Lock()
	return r.mu.Unlock
}

func (r *postgresPipelineClaimRegistry) ClaimCountForTest() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.claims)
}

func (r *postgresPipelineClaimRegistry) ContainsEventForTest(eventID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, state := range r.claims {
		if state != nil && state.claim.EventID() == eventID {
			return true
		}
	}
	return false
}

func (r *postgresPipelineClaimRegistry) AcquiringForTest(eventID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.acquiring[eventID]
	return ok
}

func (r *postgresPipelineClaimRegistry) AnyLeaseForTest() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, state := range r.claims {
		if state != nil && state.postgresLease != nil {
			return true
		}
	}
	return false
}

var postgresPipelineClaimRegistries sync.Map

var (
	_ runtimepipelineobligation.Store                      = (*postgresPipelineObligationStore)(nil)
	_ runtimepipelineobligation.Store                      = (*sqlitePipelineObligationStore)(nil)
	_ runtimepipelineobligation.ResumeAdmissionPreflighter = (*postgresPipelineObligationStore)(nil)
	_ runtimepipelineobligation.ResumeAdmissionPreflighter = (*sqlitePipelineObligationStore)(nil)
)

type postgresPipelineObligationStore struct{ *PipelinePostgresOwner }
type sqlitePipelineObligationStore struct{ *PipelineSQLiteOwner }

const pipelineCandidatePageSize = 32

type pipelineCandidate struct {
	eventID            string
	insertionSequence  int64
	visibilitySnapshot string
	attemptCount       int
	nextAttemptAt      time.Time
	createdAt          time.Time
	executionMode      executionmode.Mode
}

func (s *PipelinePostgresOwner) PipelineObligations() runtimepipelineobligation.Store {
	return &postgresPipelineObligationStore{PipelinePostgresOwner: s}
}

func (s *PipelineSQLiteOwner) PipelineObligations() runtimepipelineobligation.Store {
	return &sqlitePipelineObligationStore{PipelineSQLiteOwner: s}
}

func (s *postgresPipelineObligationStore) PreflightResume(ctx context.Context, request runtimepipelineobligation.ResumeAdmissionRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	mode, found, err := s.postgresResumeIncompatiblePipelineMode(ctx, request)
	if err != nil || !found {
		return err
	}
	return request.Admit(mode)
}

func (s *sqlitePipelineObligationStore) PreflightResume(ctx context.Context, request runtimepipelineobligation.ResumeAdmissionRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	mode, found, err := s.sqliteResumeIncompatiblePipelineMode(ctx, request)
	if err != nil || !found {
		return err
	}
	return request.Admit(mode)
}

func (s *PipelinePostgresOwner) postgresResumeIncompatiblePipelineMode(ctx context.Context, request runtimepipelineobligation.ResumeAdmissionRequest) (executionmode.Mode, bool, error) {
	if err := request.Validate(); err != nil {
		return "", false, err
	}
	args := []any{}
	whereRun := ""
	if runID := strings.TrimSpace(request.RunID()); runID != "" {
		whereRun = "AND route.run_id = $1::uuid"
		args = append(args, runID)
	}
	var mode executionmode.Mode
	err := s.backend.QueryRowContext(ctx, `
			SELECT e.execution_mode FROM decision_card_route_obligations route
			JOIN runs run ON run.run_id = route.run_id
			JOIN events e ON e.event_id = route.event_id
			WHERE route.status = 'pending'
			  AND run.status IN (`+runLifecycleActiveStateSQLValues+`)
			  AND e.execution_mode = 'live' `+whereRun+`
			LIMIT 1`, args...).Scan(&mode)
	if err == nil {
		return mode, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", false, err
	}
	args = diagnosticDirectReplayEventArgs()
	whereRun = ""
	if runID := strings.TrimSpace(request.RunID()); runID != "" {
		whereRun = fmt.Sprintf("AND e.run_id = $%d::uuid", len(args)+1)
		args = append(args, runID)
	}
	err = s.backend.QueryRowContext(ctx, `
		SELECT e.execution_mode FROM events e
		LEFT JOIN runs run ON run.run_id = e.run_id
		LEFT JOIN event_receipts receipt ON receipt.event_id = e.event_id
		 AND receipt.subscriber_type = 'platform' AND receipt.subscriber_id = 'pipeline'
		WHERE receipt.event_id IS NULL
		  AND (e.run_id IS NULL OR run.status IN (`+runLifecycleActiveStateSQLValues+`))
		  AND e.execution_mode = 'live' `+whereRun+`
		  AND NOT EXISTS (
			SELECT 1 FROM decision_card_route_obligations route
			WHERE route.event_id = e.event_id AND route.status <> 'completed'
		  )
		  AND `+postgresDiagnosticDirectReplayExclusionSQL("e", 1)+`
		LIMIT 1`, args...).Scan(&mode)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return mode, err == nil, err
}

func (s *PipelineSQLiteOwner) sqliteResumeIncompatiblePipelineMode(ctx context.Context, request runtimepipelineobligation.ResumeAdmissionRequest) (executionmode.Mode, bool, error) {
	if err := request.Validate(); err != nil {
		return "", false, err
	}
	args := []any{}
	whereRun := ""
	if runID := strings.TrimSpace(request.RunID()); runID != "" {
		whereRun = "AND route.run_id = ?"
		args = append(args, runID)
	}
	var mode executionmode.Mode
	err := s.backend.QueryRowContext(ctx, `
			SELECT e.execution_mode FROM decision_card_route_obligations route
			JOIN runs run ON run.run_id = route.run_id
			JOIN events e ON e.event_id = route.event_id
			WHERE route.status = 'pending'
			  AND run.status IN (`+runLifecycleActiveStateSQLValues+`)
			  AND e.execution_mode = 'live' `+whereRun+`
			LIMIT 1`, args...).Scan(&mode)
	if err == nil {
		return mode, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", false, err
	}
	args = []any{}
	whereRun = ""
	if runID := strings.TrimSpace(request.RunID()); runID != "" {
		whereRun = "AND e.run_id = ?"
		args = append(args, runID)
	}
	args = append(args, diagnosticDirectReplayEventArgs()...)
	err = s.backend.QueryRowContext(ctx, `
		SELECT e.execution_mode FROM events e
		LEFT JOIN runs run ON run.run_id = e.run_id
		LEFT JOIN event_receipts receipt ON receipt.event_id = e.event_id
		 AND receipt.subscriber_type = 'platform' AND receipt.subscriber_id = 'pipeline'
		WHERE receipt.event_id IS NULL
		  AND (e.run_id IS NULL OR run.status IN (`+runLifecycleActiveStateSQLValues+`))
		  AND e.execution_mode = 'live' `+whereRun+`
		  AND NOT EXISTS (
			SELECT 1 FROM decision_card_route_obligations route
			WHERE route.event_id = e.event_id AND route.status <> 'completed'
		  )
		  AND `+sqliteDiagnosticDirectReplayExclusionSQL("e")+`
		LIMIT 1`, args...).Scan(&mode)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return mode, err == nil, err
}

func (s *PipelinePostgresOwner) postgresPipelineClaims() *postgresPipelineClaimRegistry {
	if s == nil || s.backend == nil {
		return nil
	}
	created := &postgresPipelineClaimRegistry{
		issuer:     runtimepipelineobligation.NewClaimIssuer(),
		scanIssuer: runtimepipelineobligation.NewScanIssuer(),
		claims:     map[string]*pipelineClaimState{},
		scans:      map[string]*pipelineScanState{},
		acquiring:  map[string]struct{}{},
	}
	actual, _ := postgresPipelineClaimRegistries.LoadOrStore(s.backend, created)
	return actual.(*postgresPipelineClaimRegistry)
}

func (s *PipelinePostgresOwner) PostgresPipelineClaimsForTest() *PostgresPipelineClaimRegistryForTest {
	return s.postgresPipelineClaims()
}

func (s *PipelinePostgresOwner) PostgresPipelineClaimStateForTest(claim runtimepipelineobligation.Claim) (*PipelineClaimStateForTest, error) {
	return s.postgresPipelineClaimState(claim)
}

func (s *PipelineSQLiteOwner) SetPipelineReleaseErrorForTest(fn func() error) {
	s.testPipelineReleaseErr = fn
}

func (s *PipelinePostgresOwner) PostgresPipelineScanOpenClaimCountForTest(scan runtimepipelineobligation.Scan) (int, error) {
	state, err := s.postgresPipelineScanState(scan)
	if err != nil {
		return 0, err
	}
	return len(state.openClaims), nil
}

func (s *PipelineSQLiteOwner) pipelineScanOwner() (*runtimepipelineobligation.ScanIssuer, map[string]*pipelineScanState) {
	if s.pipelineScanIssuer == nil {
		s.pipelineScanIssuer = runtimepipelineobligation.NewScanIssuer()
	}
	if s.pipelineScans == nil {
		s.pipelineScans = map[string]*pipelineScanState{}
	}
	return s.pipelineScanIssuer, s.pipelineScans
}

func (s *PipelineSQLiteOwner) pipelineClaimOwner() (*runtimepipelineobligation.ClaimIssuer, map[string]*pipelineClaimState) {
	if s.pipelineClaimIssuer == nil {
		s.pipelineClaimIssuer = runtimepipelineobligation.NewClaimIssuer()
	}
	if s.pipelineClaims == nil {
		s.pipelineClaims = map[string]*pipelineClaimState{}
	}
	return s.pipelineClaimIssuer, s.pipelineClaims
}

func (s *PipelinePostgresOwner) RequirePipelinePublicationClaimTx(
	_ context.Context,
	tx *sql.Tx,
	eventID string,
	claim runtimepipelineobligation.Claim,
) error {
	if tx == nil {
		return errors.New("PostgreSQL event commit transaction is required")
	}
	state, err := s.postgresPipelineClaimState(claim)
	if err != nil {
		return err
	}
	if state.claim.EventID() != strings.TrimSpace(eventID) || state.claim.Purpose() != runtimepipelineobligation.PurposePublication {
		return runtimepipelineobligation.ErrWrongClaim
	}
	return nil
}

func (s *PipelineSQLiteOwner) RequirePipelinePublicationClaimTx(
	_ context.Context,
	tx *sql.Tx,
	eventID string,
	claim runtimepipelineobligation.Claim,
) error {
	if tx == nil {
		return errors.New("SQLite event commit transaction is required")
	}
	state, err := s.sqlitePipelineClaimState(claim)
	if err != nil {
		return err
	}
	if state.claim.EventID() != strings.TrimSpace(eventID) || state.claim.Purpose() != runtimepipelineobligation.PurposePublication {
		return runtimepipelineobligation.ErrWrongClaim
	}
	return nil
}

func (s *postgresPipelineObligationStore) ClaimPublication(ctx context.Context, eventID string) (runtimepipelineobligation.Claim, error) {
	return s.claimPostgresPipelineEvent(ctx, eventID, runtimepipelineobligation.PurposePublication)
}

func (s *sqlitePipelineObligationStore) ClaimPublication(ctx context.Context, eventID string) (runtimepipelineobligation.Claim, error) {
	return s.claimSQLitePipelineEvent(ctx, eventID, runtimepipelineobligation.PurposePublication)
}

func (s *postgresPipelineObligationStore) ClaimEvent(ctx context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.ClaimedWork, error) {
	claim, err := s.claimPostgresPipelineEvent(ctx, eventID, purpose)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	work, err := s.loadPostgresClaimedPipelineWork(ctx, claim)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, errors.Join(
			err,
			s.Release(context.WithoutCancel(ctx), claim),
		)
	}
	return work, nil
}

func (s *sqlitePipelineObligationStore) ClaimEvent(ctx context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.ClaimedWork, error) {
	claim, err := s.claimSQLitePipelineEvent(ctx, eventID, purpose)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	work, err := s.loadSQLiteClaimedPipelineWork(ctx, claim)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, errors.Join(
			err,
			s.Release(context.WithoutCancel(ctx), claim),
		)
	}
	return work, nil
}

func (s *postgresPipelineObligationStore) OpenScan(ctx context.Context, request runtimepipelineobligation.ScanRequest) (runtimepipelineobligation.Scan, error) {
	if err := request.Validate(); err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	if err := ctx.Err(); err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	registry := s.postgresPipelineClaims()
	scan, err := registry.scanIssuer.Issue()
	if err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	token, err := registry.scanIssuer.Token(scan)
	if err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	registry.mu.Lock()
	registry.scans[token] = &pipelineScanState{
		scan:       scan,
		request:    request,
		openClaims: map[string]runtimepipelineobligation.Claim{},
	}
	registry.mu.Unlock()
	return scan, nil
}

func (s *sqlitePipelineObligationStore) OpenScan(ctx context.Context, request runtimepipelineobligation.ScanRequest) (runtimepipelineobligation.Scan, error) {
	if err := request.Validate(); err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	if err := ctx.Err(); err != nil {
		return runtimepipelineobligation.Scan{}, err
	}
	s.pipelineClaimMu.Lock()
	issuer, scans := s.pipelineScanOwner()
	scan, err := issuer.Issue()
	if err == nil {
		token, tokenErr := issuer.Token(scan)
		if tokenErr != nil {
			err = tokenErr
		} else {
			scans[token] = &pipelineScanState{
				scan:       scan,
				request:    request,
				openClaims: map[string]runtimepipelineobligation.Claim{},
			}
		}
	}
	s.pipelineClaimMu.Unlock()
	return scan, err
}

type pipelineBatchBackend struct {
	boundary   func(context.Context, runtimepipelineobligation.ClaimQuery) (*pipelineCandidate, error)
	candidates func(context.Context, runtimepipelineobligation.ClaimQuery, *pipelineCandidate, *pipelineCandidate, map[int64]struct{}, int) ([]pipelineCandidate, error)
	claim      func(context.Context, string, runtimepipelineobligation.Purpose) (runtimepipelineobligation.Claim, error)
	associate  func(runtimepipelineobligation.Scan, runtimepipelineobligation.Claim) error
	load       func(context.Context, runtimepipelineobligation.Claim) (runtimepipelineobligation.ClaimedWork, error)
	release    func(context.Context, runtimepipelineobligation.Claim) error
}

func (s *postgresPipelineObligationStore) ClaimBatch(ctx context.Context, scan runtimepipelineobligation.Scan, limit int) (runtimepipelineobligation.ScanBatch, error) {
	state, err := s.postgresPipelineScanState(scan)
	if err != nil {
		return runtimepipelineobligation.ScanBatch{}, err
	}
	state.mu.Lock()
	if state.closed {
		state.mu.Unlock()
		return runtimepipelineobligation.ScanBatch{}, runtimepipelineobligation.ErrStaleScan
	}
	batch, err := claimPipelineBatch(ctx, state, limit, pipelineBatchBackend{
		boundary:   s.postgresPipelineBoundary,
		candidates: s.postgresPipelineCandidates,
		claim:      s.claimPostgresPipelineEvent,
		associate:  s.associatePostgresPipelineClaim,
		load:       s.loadPostgresClaimedPipelineWork,
		release:    s.Release,
	})
	state.mu.Unlock()
	if err != nil {
		err = errors.Join(err, s.CloseScan(context.WithoutCancel(ctx), scan))
	}
	return batch, err
}

func (s *sqlitePipelineObligationStore) ClaimBatch(ctx context.Context, scan runtimepipelineobligation.Scan, limit int) (runtimepipelineobligation.ScanBatch, error) {
	state, err := s.sqlitePipelineScanState(scan)
	if err != nil {
		return runtimepipelineobligation.ScanBatch{}, err
	}
	state.mu.Lock()
	if state.closed {
		state.mu.Unlock()
		return runtimepipelineobligation.ScanBatch{}, runtimepipelineobligation.ErrStaleScan
	}
	batch, err := claimPipelineBatch(ctx, state, limit, pipelineBatchBackend{
		boundary:   s.sqlitePipelineBoundary,
		candidates: s.sqlitePipelineCandidates,
		claim:      s.claimSQLitePipelineEvent,
		associate:  s.associateSQLitePipelineClaim,
		load:       s.loadSQLiteClaimedPipelineWork,
		release:    s.Release,
	})
	state.mu.Unlock()
	if err != nil {
		err = errors.Join(err, s.CloseScan(context.WithoutCancel(ctx), scan))
	}
	return batch, err
}

func claimPipelineBatch(
	ctx context.Context,
	state *pipelineScanState,
	limit int,
	backend pipelineBatchBackend,
) (batch runtimepipelineobligation.ScanBatch, err error) {
	if state == nil {
		return batch, runtimepipelineobligation.ErrStaleScan
	}
	if limit <= 0 {
		return batch, errors.New("pipeline scan batch limit must be positive")
	}
	batch.Work = make([]runtimepipelineobligation.ClaimedWork, 0, limit)
	acquired := make([]runtimepipelineobligation.Claim, 0, limit)
	defer func() {
		if err == nil {
			return
		}
		releaseCtx := context.WithoutCancel(ctx)
		for _, claim := range acquired {
			releaseErr := backend.release(releaseCtx, claim)
			if !errors.Is(releaseErr, runtimepipelineobligation.ErrStaleClaim) {
				err = errors.Join(err, releaseErr)
			}
		}
	}()
	for batch.Examined < limit {
		if err := ctx.Err(); err != nil {
			return batch, err
		}
		query, ok := state.request.QueryAt(state.phase)
		if !ok {
			batch.Exhausted = true
			return batch, nil
		}
		if !state.bound {
			state.through, err = backend.boundary(ctx, query)
			if err != nil {
				return batch, err
			}
			state.bound = true
			if state.through == nil {
				advancePipelineScanPhase(state)
				continue
			}
		}
		remaining := limit - batch.Examined
		pageLimit := min(remaining, pipelineCandidatePageSize)
		candidates, err := backend.candidates(ctx, query, state.after, state.through, state.examined, pageLimit)
		if err != nil {
			return batch, err
		}
		if len(candidates) == 0 {
			advancePipelineScanPhase(state)
			continue
		}
		for i := range candidates {
			if err := ctx.Err(); err != nil {
				return batch, err
			}
			candidate := candidates[i]
			if err := state.request.Admit(candidate.executionMode); err != nil {
				return batch, err
			}
			// Continuation advances before a claim can be released or retained.
			state.after = &candidate
			if query.Purpose == runtimepipelineobligation.PurposeDecisionRoute {
				if state.examined == nil {
					state.examined = map[int64]struct{}{}
				}
				state.examined[candidate.insertionSequence] = struct{}{}
			}
			batch.Examined++
			claim, err := backend.claim(ctx, candidate.eventID, query.Purpose)
			if errors.Is(err, runtimepipelineobligation.ErrBusy) {
				batch.LocallyBlocked = true
				continue
			}
			if errors.Is(err, runtimepipelineobligation.ErrIneligible) {
				continue
			}
			if err != nil {
				return batch, err
			}
			acquired = append(acquired, claim)
			if err := backend.associate(state.scan, claim); err != nil {
				return batch, err
			}
			work, err := backend.load(ctx, claim)
			if errors.Is(err, runtimepipelineobligation.ErrIneligible) {
				if releaseErr := backend.release(context.WithoutCancel(ctx), claim); releaseErr != nil {
					return batch, errors.Join(err, releaseErr)
				}
				continue
			}
			if err != nil {
				return batch, err
			}
			batch.Work = append(batch.Work, work)
			if i == len(candidates)-1 && len(candidates) < pageLimit {
				advancePipelineScanPhase(state)
				if _, more := state.request.QueryAt(state.phase); !more {
					batch.Exhausted = true
				}
			}
			// Processing one claimed item may change the eligibility of every
			// later candidate. The cursor preserves the examination budget,
			// while the consumer preserves mutation order between steps.
			return batch, nil
		}
		if len(candidates) < pageLimit {
			advancePipelineScanPhase(state)
		}
	}
	return batch, nil
}

func advancePipelineScanPhase(state *pipelineScanState) {
	state.phase++
	state.after = nil
	state.through = nil
	state.bound = false
	state.examined = nil
}

func (s *postgresPipelineObligationStore) CloseScan(ctx context.Context, scan runtimepipelineobligation.Scan) error {
	if ctx == nil {
		ctx = context.Background()
	}
	registry := s.postgresPipelineClaims()
	token, err := registry.scanIssuer.Token(scan)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	state := registry.scans[token]
	registry.mu.Unlock()
	if state == nil {
		return runtimepipelineobligation.ErrStaleScan
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	registry.mu.Lock()
	if registry.scans[token] != state {
		registry.mu.Unlock()
		return runtimepipelineobligation.ErrStaleScan
	}
	state.closed = true
	claims := make([]runtimepipelineobligation.Claim, 0, len(state.openClaims))
	for _, claim := range state.openClaims {
		claims = append(claims, claim)
	}
	registry.mu.Unlock()
	releaseCtx := context.WithoutCancel(ctx)
	var releaseErr error
	for _, claim := range claims {
		if err := s.releasePostgresPipelineClaim(releaseCtx, claim); err != nil &&
			!errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
			releaseErr = errors.Join(releaseErr, err)
		}
	}
	registry.mu.Lock()
	if current := registry.scans[token]; current != state {
		releaseErr = errors.Join(releaseErr, runtimepipelineobligation.ErrStaleScan)
	} else {
		if len(state.openClaims) != 0 {
			releaseErr = errors.Join(releaseErr, errors.New("close PostgreSQL pipeline scan left current claims"))
		}
		delete(registry.scans, token)
	}
	registry.mu.Unlock()
	return releaseErr
}

func (s *sqlitePipelineObligationStore) CloseScan(_ context.Context, scan runtimepipelineobligation.Scan) error {
	s.pipelineClaimMu.Lock()
	issuer, scans := s.pipelineScanOwner()
	token, err := issuer.Token(scan)
	if err != nil {
		s.pipelineClaimMu.Unlock()
		return err
	}
	state := scans[token]
	s.pipelineClaimMu.Unlock()
	if state == nil {
		return runtimepipelineobligation.ErrStaleScan
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	s.pipelineClaimMu.Lock()
	if scans[token] != state {
		s.pipelineClaimMu.Unlock()
		return runtimepipelineobligation.ErrStaleScan
	}
	state.closed = true
	claims := make([]runtimepipelineobligation.Claim, 0, len(state.openClaims))
	for _, claim := range state.openClaims {
		claims = append(claims, claim)
	}
	s.pipelineClaimMu.Unlock()
	var releaseErr error
	for _, claim := range claims {
		if err := s.releaseSQLitePipelineClaim(claim); err != nil &&
			!errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
			releaseErr = errors.Join(releaseErr, err)
		}
	}
	s.pipelineClaimMu.Lock()
	if current := scans[token]; current != state {
		releaseErr = errors.Join(releaseErr, runtimepipelineobligation.ErrStaleScan)
	} else if len(state.openClaims) == 0 {
		delete(scans, token)
	} else if releaseErr == nil {
		releaseErr = errors.New("close SQLite pipeline scan left current claims")
	}
	s.pipelineClaimMu.Unlock()
	return releaseErr
}

func (s *PipelinePostgresOwner) postgresPipelineScanState(scan runtimepipelineobligation.Scan) (*pipelineScanState, error) {
	registry := s.postgresPipelineClaims()
	if registry == nil {
		return nil, runtimepipelineobligation.ErrStaleScan
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	token, err := registry.scanIssuer.Token(scan)
	if err != nil {
		return nil, err
	}
	state := registry.scans[token]
	if state == nil {
		return nil, runtimepipelineobligation.ErrStaleScan
	}
	return state, nil
}

func (s *PipelineSQLiteOwner) sqlitePipelineScanState(scan runtimepipelineobligation.Scan) (*pipelineScanState, error) {
	s.pipelineClaimMu.Lock()
	defer s.pipelineClaimMu.Unlock()
	issuer, scans := s.pipelineScanOwner()
	token, err := issuer.Token(scan)
	if err != nil {
		return nil, err
	}
	state := scans[token]
	if state == nil {
		return nil, runtimepipelineobligation.ErrStaleScan
	}
	return state, nil
}

func (s *PipelinePostgresOwner) associatePostgresPipelineClaim(scan runtimepipelineobligation.Scan, claim runtimepipelineobligation.Claim) error {
	registry := s.postgresPipelineClaims()
	if registry == nil {
		return runtimepipelineobligation.ErrStaleScan
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	scanToken, err := registry.scanIssuer.Token(scan)
	if err != nil {
		return err
	}
	claimToken, err := registry.issuer.Token(claim)
	if err != nil {
		return err
	}
	scanState := registry.scans[scanToken]
	claimState := registry.claims[claimToken]
	if scanState == nil || claimState == nil {
		return runtimepipelineobligation.ErrStaleScan
	}
	claimState.scanToken = scanToken
	scanState.openClaims[claimToken] = claim
	return nil
}

func (s *PipelineSQLiteOwner) associateSQLitePipelineClaim(scan runtimepipelineobligation.Scan, claim runtimepipelineobligation.Claim) error {
	s.pipelineClaimMu.Lock()
	defer s.pipelineClaimMu.Unlock()
	scanIssuer, scans := s.pipelineScanOwner()
	claimIssuer, claims := s.pipelineClaimOwner()
	scanToken, err := scanIssuer.Token(scan)
	if err != nil {
		return err
	}
	claimToken, err := claimIssuer.Token(claim)
	if err != nil {
		return err
	}
	scanState := scans[scanToken]
	claimState := claims[claimToken]
	if scanState == nil || claimState == nil {
		return runtimepipelineobligation.ErrStaleScan
	}
	claimState.scanToken = scanToken
	scanState.openClaims[claimToken] = claim
	return nil
}

func corruptPipelineScopeDisposition(eventID string, err error) (runtimepipelineobligation.Disposition, bool) {
	code := ""
	switch {
	case errors.Is(err, runtimepipelineobligation.ErrMissingScope):
		code = "committed_pipeline_scope_missing"
	case errors.Is(err, runtimepipelineobligation.ErrInvalidScope):
		code = "committed_pipeline_scope_invalid"
	default:
		return runtimepipelineobligation.Disposition{}, false
	}
	failureErr := runtimefailures.New(
		runtimefailures.ClassSchemaInvalid,
		code,
		"pipeline-obligation-store",
		"hydrate_claimed_work",
		map[string]any{"event_id": strings.TrimSpace(eventID)},
	)
	failure, _ := runtimefailures.EnvelopeFromError(failureErr)
	return runtimepipelineobligation.Quarantined(code, &failure), true
}

func CorruptPipelineScopeDisposition(eventID string, err error) (runtimepipelineobligation.Disposition, bool) {
	return corruptPipelineScopeDisposition(eventID, err)
}

func (s *postgresPipelineObligationStore) MarkDecisionProcessed(ctx context.Context, claim runtimepipelineobligation.Claim) error {
	if claim.Purpose() != runtimepipelineobligation.PurposePublication && claim.Purpose() != runtimepipelineobligation.PurposeDecisionRoute {
		return runtimepipelineobligation.ErrWrongClaim
	}
	state, err := s.lockPostgresPipelineClaim(claim)
	if err != nil {
		return err
	}
	defer state.operationMu.Unlock()
	handoff, err := runhandoff.ReserveCandidateHandoff(ctx)
	if err != nil {
		return err
	}
	defer handoff.Rollback()
	lease := state.postgresLease
	tx, err := lease.Session().BeginTx(ctx)
	if err != nil {
		return err
	}
	if err := markDecisionRouteProcessedTx(ctx, tx, claim.EventID(), true, time.Now().UTC()); err != nil {
		return errors.Join(err, postgresbackend.RollbackAuthorityTransaction(tx, lease.Session()))
	}
	runID, err := eventRunIDForCompletionCandidateTx(ctx, tx, claim.EventID(), true)
	if err != nil {
		return errors.Join(err, postgresbackend.RollbackAuthorityTransaction(tx, lease.Session()))
	}
	if runID != "" {
		effects := newRevisionEffects()
		if err := addPipelineDispositionRevisionEffects(effects, runID); err != nil {
			return errors.Join(err, postgresbackend.RollbackAuthorityTransaction(tx, lease.Session()))
		}
		if _, err := s.candidateRequests.RequestCompletionCandidateTx(ctx, tx, runID, nil, handoff); err != nil {
			return errors.Join(err, postgresbackend.RollbackAuthorityTransaction(tx, lease.Session()))
		}
		if err := postgresDeliveryAdapter.CommitPipelineHandoff(ctx, tx, claim.EventID()); err != nil {
			return errors.Join(err, postgresbackend.RollbackAuthorityTransaction(tx, lease.Session()))
		}
		if _, err := privaterunforkrevision.FinalizePostgres(ctx, tx, effects); err != nil {
			return errors.Join(err, postgresbackend.RollbackAuthorityTransaction(tx, lease.Session()))
		}
	} else {
		return errors.Join(errors.New("processed pipeline event has no owning run"), postgresbackend.RollbackAuthorityTransaction(tx, lease.Session()))
	}
	if err := tx.Commit(); err != nil {
		return errors.Join(err, postgresbackend.RollbackAuthorityTransaction(tx, lease.Session()))
	}
	endErr := lease.Session().EndTx(tx)
	handoffErr := handoff.Commit()
	return errors.Join(endErr, handoffErr)
}

func (s *sqlitePipelineObligationStore) MarkDecisionProcessed(ctx context.Context, claim runtimepipelineobligation.Claim) error {
	if claim.Purpose() != runtimepipelineobligation.PurposePublication && claim.Purpose() != runtimepipelineobligation.PurposeDecisionRoute {
		return runtimepipelineobligation.ErrWrongClaim
	}
	state, err := s.lockSQLitePipelineClaim(claim)
	if err != nil {
		return err
	}
	defer state.operationMu.Unlock()
	handoff, err := runhandoff.ReserveCandidateHandoff(ctx)
	if err != nil {
		return err
	}
	defer handoff.Rollback()
	effects := newRevisionEffects()
	err = s.runRuntimeMutation(ctx, "mark sqlite decision route processed", effects, func(txctx context.Context, tx *sql.Tx) error {
		if current, err := s.sqlitePipelineClaimState(claim); err != nil || current != state {
			if err != nil {
				return err
			}
			return runtimepipelineobligation.ErrStaleClaim
		}
		if err := markDecisionRouteProcessedTx(txctx, tx, claim.EventID(), false, s.now()); err != nil {
			return err
		}
		runID, err := eventRunIDForCompletionCandidateTx(txctx, tx, claim.EventID(), false)
		if err != nil {
			return err
		}
		if runID != "" {
			if err := addPipelineDispositionRevisionEffects(effects, runID); err != nil {
				return err
			}
			if _, err = s.candidateRequests.RequestCompletionCandidateTx(txctx, tx, runID, nil, handoff); err != nil {
				return err
			}
		} else {
			return errors.New("processed pipeline event has no owning run")
		}
		return sqliteDeliveryAdapter.CommitPipelineHandoff(txctx, tx, claim.EventID())
	})
	if err != nil {
		return err
	}
	return handoff.Commit()
}

func markDecisionRouteProcessedTx(ctx context.Context, tx pipelineExecer, eventID string, postgres bool, now time.Time) error {
	pending, receiptOutcome, err := pipelineDispositionState(ctx, tx, eventID, postgres)
	if err != nil {
		return err
	}
	if !pending || receiptOutcome != "" {
		return runtimepipelineobligation.ErrIneligible
	}
	return writeExactPlatformPipelineReceipt(ctx, tx, eventID, runtimepipelineobligation.Acknowledged("decision_route_processed"), postgres, now)
}

func (s *PipelinePostgresOwner) claimPostgresPipelineEvent(ctx context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (claim runtimepipelineobligation.Claim, err error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimepipelineobligation.Claim{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	eventID = strings.TrimSpace(eventID)
	if _, err := uuid.Parse(eventID); err != nil {
		return runtimepipelineobligation.Claim{}, fmt.Errorf("claim pipeline event: %w", err)
	}
	registry := s.postgresPipelineClaims()
	if registry == nil {
		return runtimepipelineobligation.Claim{}, errors.New("PostgreSQL pipeline claim registry is required")
	}
	if registry.testBeforeClaimRegistryLock != nil {
		registry.testBeforeClaimRegistryLock()
	}
	registry.mu.Lock()
	for _, state := range registry.claims {
		if state != nil && state.claim.EventID() == eventID {
			registry.mu.Unlock()
			return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrBusy
		}
	}
	if _, reserving := registry.acquiring[eventID]; reserving {
		registry.mu.Unlock()
		return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrBusy
	}
	registry.acquiring[eventID] = struct{}{}
	registry.mu.Unlock()
	acquisitionReserved := true
	defer func() {
		if acquisitionReserved {
			registry.mu.Lock()
			delete(registry.acquiring, eventID)
			registry.mu.Unlock()
		}
	}()
	claimSession, releaseReservation, err := s.reservePostgresPipelineClaimConnection(ctx)
	if err != nil {
		return runtimepipelineobligation.Claim{}, err
	}
	reservationOpen := true
	defer func() {
		if reservationOpen {
			err = errors.Join(err, releaseReservation())
		}
	}()
	releaseLeaseSession, retained := claimSession.Retain()
	if !retained {
		return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrStaleClaim
	}
	lease, acquired, err := postgresbackend.AcquireAdvisoryLockLeaseOnSession(ctx, claimSession, replayClaimLockKey(eventID), nil, releaseLeaseSession)
	if err != nil {
		return runtimepipelineobligation.Claim{}, err
	}
	if !acquired || lease == nil {
		return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrBusy
	}
	if registry.testConfigureClaimLease != nil {
		registry.testConfigureClaimLease(lease)
	}
	claim, err = registry.issuer.Issue(eventID, purpose)
	if err != nil {
		return runtimepipelineobligation.Claim{}, errors.Join(
			err,
			lease.Release(context.WithoutCancel(ctx)),
		)
	}
	token, err := registry.issuer.Token(claim)
	if err != nil {
		return runtimepipelineobligation.Claim{}, errors.Join(
			err,
			lease.Release(context.WithoutCancel(ctx)),
		)
	}
	state := &pipelineClaimState{claim: claim, postgresLease: lease}
	releaseCapacity := s.backend.RetainConnectionCapacity()
	registry.mu.Lock()
	installed := lease.InstallTerminalOwner(
		releaseCapacity,
		func() { registry.retirePostgresPipelineClaim(token, state) },
		func() { registry.claims[token] = state },
	)
	delete(registry.acquiring, eventID)
	acquisitionReserved = false
	registry.mu.Unlock()
	if !installed {
		releaseCapacity()
		return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrStaleClaim
	}
	reservationOpen = false
	if releaseErr := releaseReservation(); releaseErr != nil {
		return runtimepipelineobligation.Claim{}, errors.Join(
			releaseErr,
			s.releasePostgresPipelineClaim(context.WithoutCancel(ctx), claim),
		)
	}
	if purpose == runtimepipelineobligation.PurposePublication {
		return claim, nil
	}
	eligible, err := postgresPipelineEligible(ctx, lease.Session(), eventID, purpose)
	if err != nil {
		return runtimepipelineobligation.Claim{}, errors.Join(err, s.releasePostgresPipelineClaim(context.WithoutCancel(ctx), claim))
	}
	if !eligible {
		return runtimepipelineobligation.Claim{}, errors.Join(runtimepipelineobligation.ErrIneligible, s.releasePostgresPipelineClaim(context.WithoutCancel(ctx), claim))
	}
	return claim, nil
}

func (s *PipelinePostgresOwner) ClaimPostgresPipelineEventForTest(ctx context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.Claim, error) {
	return s.claimPostgresPipelineEvent(ctx, eventID, purpose)
}

func (r *postgresPipelineClaimRegistry) retirePostgresPipelineClaim(token string, state *pipelineClaimState) {
	if r == nil || state == nil {
		return
	}
	r.mu.Lock()
	if r.claims[token] == state {
		if scanState := r.scans[state.scanToken]; scanState != nil {
			delete(scanState.openClaims, token)
		}
		delete(r.claims, token)
	}
	r.mu.Unlock()
}

func (s *PipelinePostgresOwner) reservePostgresPipelineClaimConnection(ctx context.Context) (*postgresbackend.SessionAuthority, func() error, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := s.backend.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("reserve PostgreSQL pipeline claim connection: %w", err)
	}
	session, err := postgresbackend.NewSessionAuthority(conn)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return session, session.Release, nil
}

func (s *PipelineSQLiteOwner) claimSQLitePipelineEvent(ctx context.Context, eventID string, purpose runtimepipelineobligation.Purpose) (runtimepipelineobligation.Claim, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimepipelineobligation.Claim{}, err
	}
	eventID = strings.TrimSpace(eventID)
	if _, err := uuid.Parse(eventID); err != nil {
		return runtimepipelineobligation.Claim{}, fmt.Errorf("claim sqlite pipeline event: %w", err)
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	s.pipelineClaimMu.Lock()
	issuer, claims := s.pipelineClaimOwner()
	for _, state := range claims {
		if state != nil && state.claim.EventID() == eventID {
			s.pipelineClaimMu.Unlock()
			return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrBusy
		}
	}
	if purpose != runtimepipelineobligation.PurposePublication {
		eligible, err := sqlitePipelineEligible(ctx, s.backend, eventID, purpose)
		if err != nil {
			s.pipelineClaimMu.Unlock()
			return runtimepipelineobligation.Claim{}, err
		}
		if !eligible {
			s.pipelineClaimMu.Unlock()
			return runtimepipelineobligation.Claim{}, runtimepipelineobligation.ErrIneligible
		}
	}
	claim, err := issuer.Issue(eventID, purpose)
	if err == nil {
		token, tokenErr := issuer.Token(claim)
		if tokenErr != nil {
			err = tokenErr
		} else {
			claims[token] = &pipelineClaimState{claim: claim}
		}
	}
	s.pipelineClaimMu.Unlock()
	return claim, err
}

func (s *PipelinePostgresOwner) loadPostgresClaimedPipelineWork(ctx context.Context, claim runtimepipelineobligation.Claim) (runtimepipelineobligation.ClaimedWork, error) {
	state, err := s.lockPostgresPipelineClaim(claim)
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	defer state.operationMu.Unlock()
	if claim.Purpose() != runtimepipelineobligation.PurposePublication {
		eligible, err := postgresPipelineEligible(ctx, state.postgresLease.Session(), claim.EventID(), claim.Purpose())
		if err != nil {
			return runtimepipelineobligation.ClaimedWork{}, err
		}
		if !eligible {
			return runtimepipelineobligation.ClaimedWork{}, runtimepipelineobligation.ErrIneligible
		}
	}
	return loadClaimedPipelineWork(ctx, state.postgresLease.Session(), claim, true)
}

func (s *PipelineSQLiteOwner) loadSQLiteClaimedPipelineWork(ctx context.Context, claim runtimepipelineobligation.Claim) (runtimepipelineobligation.ClaimedWork, error) {
	if _, err := s.sqlitePipelineClaimState(claim); err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	return loadClaimedPipelineWork(ctx, s.backend, claim, false)
}

func (s *PipelinePostgresOwner) CommitInitialPipelineScopeTx(ctx context.Context, tx *sql.Tx, eventID string, scope runtimepipelineobligation.CommittedScope) error {
	if tx == nil {
		return errors.New("initial pipeline scope transaction is required")
	}
	if err := requireActiveRunForEvent(ctx, tx, eventID, true); err != nil {
		return err
	}
	return insertCommittedPipelineScopeTx(ctx, tx, eventID, scope, true, time.Now().UTC())
}

func (s *PipelinePostgresOwner) CommitScopeAtTx(ctx context.Context, tx *sql.Tx, eventID string, scope runtimepipelineobligation.CommittedScope, at time.Time) error {
	return insertCommittedPipelineScopeTx(ctx, tx, eventID, scope, true, at.UTC())
}

func (s *PipelineSQLiteOwner) CommitInitialPipelineScopeTx(ctx context.Context, tx *sql.Tx, eventID string, scope runtimepipelineobligation.CommittedScope) error {
	if tx == nil {
		return errors.New("initial sqlite pipeline scope transaction is required")
	}
	if err := requireActiveRunForEvent(ctx, tx, eventID, false); err != nil {
		return err
	}
	return insertCommittedPipelineScopeTx(ctx, tx, eventID, scope, false, s.now())
}

func insertCommittedPipelineScopeTx(
	ctx context.Context,
	tx pipelineExecer,
	eventID string,
	scope runtimepipelineobligation.CommittedScope,
	postgres bool,
	now time.Time,
) error {
	eventID = strings.TrimSpace(eventID)
	if tx == nil {
		return errors.New("committed pipeline scope transaction is required")
	}
	if _, err := uuid.Parse(eventID); err != nil {
		return fmt.Errorf("committed pipeline scope event id: %w", err)
	}
	parsed, err := runtimepipelineobligation.ParseCommittedScope(string(scope))
	if err != nil {
		return fmt.Errorf("committed pipeline scope: %w", err)
	}
	if parsed != scope {
		return errors.New("committed pipeline scope must be canonical")
	}
	query := `
		INSERT INTO committed_replay_scopes (event_id, run_id, scope, created_at, updated_at)
		SELECT e.event_id, e.run_id, ?, ?, ? FROM events e WHERE e.event_id = ?
		ON CONFLICT(event_id) DO NOTHING`
	args := []any{string(scope), now, now, eventID}
	if postgres {
		query = `
			INSERT INTO committed_replay_scopes (event_id, run_id, scope, created_at, updated_at)
			SELECT e.event_id, e.run_id, $2, $3, $3 FROM events e WHERE e.event_id = $1::uuid
			ON CONFLICT(event_id) DO NOTHING`
		args = []any{eventID, string(scope), now}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("insert committed pipeline scope: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read committed pipeline scope insertion: %w", err)
	}
	if rows == 1 {
		return nil
	}
	persisted, err := loadCommittedPipelineScope(ctx, tx, eventID, postgres)
	if err != nil {
		return fmt.Errorf("read committed pipeline scope duplicate: %w", err)
	}
	if persisted != scope {
		return errors.New("committed pipeline scope conflicts with persisted scope")
	}
	return nil
}

func InsertCommittedPipelineScopeTx(ctx context.Context, tx interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, eventID string, scope runtimepipelineobligation.CommittedScope, postgres bool, now time.Time) error {
	return insertCommittedPipelineScopeTx(ctx, tx, eventID, scope, postgres, now)
}

func (s *PipelinePostgresOwner) CommitInitialPipelineDispositionTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
	claim runtimepipelineobligation.Claim,
	disposition runtimepipelineobligation.Disposition,
) error {
	if tx == nil {
		return errors.New("initial pipeline disposition transaction is required")
	}
	if claim.Purpose() != runtimepipelineobligation.PurposePublication || claim.EventID() != strings.TrimSpace(eventID) {
		return runtimepipelineobligation.ErrWrongClaim
	}
	if _, err := s.postgresPipelineClaimState(claim); err != nil {
		return err
	}
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return err
	}
	if err := writePipelineDispositionTx(ctx, tx, eventID, claim.Purpose(), disposition, true, time.Now().UTC()); err != nil {
		return err
	}
	if disposition.Successful() {
		return postgresDeliveryAdapter.CommitPipelineHandoff(ctx, tx, eventID)
	}
	return nil
}

func (s *PipelineSQLiteOwner) CommitInitialPipelineDispositionTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
	claim runtimepipelineobligation.Claim,
	disposition runtimepipelineobligation.Disposition,
) error {
	if tx == nil {
		return errors.New("initial sqlite pipeline disposition transaction is required")
	}
	if claim.Purpose() != runtimepipelineobligation.PurposePublication || claim.EventID() != strings.TrimSpace(eventID) {
		return runtimepipelineobligation.ErrWrongClaim
	}
	if _, err := s.sqlitePipelineClaimState(claim); err != nil {
		return err
	}
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return err
	}
	if err := writePipelineDispositionTx(ctx, tx, eventID, claim.Purpose(), disposition, false, s.now()); err != nil {
		return err
	}
	if disposition.Successful() {
		return sqliteDeliveryAdapter.CommitPipelineHandoff(ctx, tx, eventID)
	}
	return nil
}

type pipelineQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadClaimedPipelineWork(ctx context.Context, q pipelineQueryer, claim runtimepipelineobligation.Claim, postgres bool) (runtimepipelineobligation.ClaimedWork, error) {
	var (
		records []events.PersistedReplayEvent
		outcome sql.NullString
		err     error
	)
	if postgres {
		records, err = hydratePostgresPersistedReplayEvents(ctx, q, []string{claim.EventID()})
	} else {
		records, err = hydrateSQLitePersistedReplayEvents(ctx, q, []string{claim.EventID()})
	}
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	if len(records) != 1 {
		return runtimepipelineobligation.ClaimedWork{}, runtimepipelineobligation.ErrIneligible
	}
	if postgres {
		err = q.QueryRowContext(ctx, `SELECT outcome FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, claim.EventID()).Scan(&outcome)
	} else {
		err = q.QueryRowContext(ctx, `SELECT outcome FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, claim.EventID()).Scan(&outcome)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	work := runtimepipelineobligation.ClaimedWork{
		Event:        records[0].Event,
		Claim:        claim,
		Acknowledged: strings.TrimSpace(outcome.String) == "success",
	}
	scope, err := loadCommittedPipelineScope(ctx, q, claim.EventID(), postgres)
	if disposition, corrupt := corruptPipelineScopeDisposition(claim.EventID(), err); corrupt {
		return runtimepipelineobligation.PreclassifiedWork(work, disposition)
	}
	if err != nil {
		return runtimepipelineobligation.ClaimedWork{}, err
	}
	work.Scope = scope
	if failure := records[0].ReplayFailure; failure != nil {
		reasonCode := strings.TrimSpace(failure.Detail.Code)
		if reasonCode == "" {
			reasonCode = "pipeline_recovery_event_invalid"
		}
		return runtimepipelineobligation.PreclassifiedWork(
			work,
			runtimepipelineobligation.Quarantined(reasonCode, failure),
		)
	}
	return work, nil
}

func loadCommittedPipelineScope(ctx context.Context, q rowQueryer, eventID string, postgres bool) (runtimepipelineobligation.CommittedScope, error) {
	var raw string
	query := `SELECT scope FROM committed_replay_scopes WHERE event_id = ?`
	args := []any{strings.TrimSpace(eventID)}
	if postgres {
		query = `SELECT scope FROM committed_replay_scopes WHERE event_id = $1::uuid`
	}
	if err := q.QueryRowContext(ctx, query, args...).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", runtimepipelineobligation.ErrMissingScope
		}
		return "", err
	}
	scope, err := runtimepipelineobligation.ParseCommittedScope(raw)
	if err != nil {
		return "", fmt.Errorf("%w: %v", runtimepipelineobligation.ErrInvalidScope, err)
	}
	return scope, nil
}

func LoadCommittedScope(ctx context.Context, q rowQueryer, eventID string, postgres bool) (runtimepipelineobligation.CommittedScope, error) {
	return loadCommittedPipelineScope(ctx, q, eventID, postgres)
}

func postgresPipelineEligible(ctx context.Context, q pipelineQueryer, eventID string, purpose runtimepipelineobligation.Purpose) (bool, error) {
	var eligible bool
	switch purpose {
	case runtimepipelineobligation.PurposeRecovery:
		diagnostics := diagnosticDirectReplayEventArgs()
		query := fmt.Sprintf(`
				SELECT EXISTS (
					SELECT 1
					FROM events e
				LEFT JOIN runs run ON run.run_id = e.run_id
				LEFT JOIN event_receipts receipt
				  ON receipt.event_id = e.event_id
				 AND receipt.subscriber_type = 'platform'
				 AND receipt.subscriber_id = 'pipeline'
					WHERE e.event_id = $1::uuid
					  AND (e.run_id IS NULL OR run.status IN (`+runLifecycleActiveStateSQLValues+`))
					  AND receipt.event_id IS NULL
					  AND %s
					  AND NOT EXISTS (
						SELECT 1 FROM decision_card_route_obligations route
						WHERE route.event_id = e.event_id AND route.status <> 'completed'
					  )
				)`, postgresDiagnosticDirectReplayExclusionSQL("e", 2))
		err := q.QueryRowContext(ctx, query, append([]any{eventID}, diagnostics...)...).Scan(&eligible)
		return eligible, err
	case runtimepipelineobligation.PurposeDecisionRoute:
		err := q.QueryRowContext(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM decision_card_route_obligations route
					JOIN runs run ON run.run_id = route.run_id
					WHERE route.event_id = $1::uuid
					  AND route.status = 'pending'
					  AND run.status IN (`+runLifecycleActiveStateSQLValues+`)
				)`, eventID).Scan(&eligible)
		return eligible, err
	default:
		return false, fmt.Errorf("pipeline claim purpose %q cannot hydrate work", purpose)
	}
}

func sqlitePipelineEligible(ctx context.Context, q pipelineQueryer, eventID string, purpose runtimepipelineobligation.Purpose) (bool, error) {
	var eligible bool
	switch purpose {
	case runtimepipelineobligation.PurposeRecovery:
		query := `
				SELECT EXISTS (
					SELECT 1
				FROM events e
				LEFT JOIN runs run ON run.run_id = e.run_id
				LEFT JOIN event_receipts receipt
				  ON receipt.event_id = e.event_id
				 AND receipt.subscriber_type = 'platform'
				 AND receipt.subscriber_id = 'pipeline'
					WHERE e.event_id = ?
					  AND (e.run_id IS NULL OR run.status IN (` + runLifecycleActiveStateSQLValues + `))
					  AND receipt.event_id IS NULL
					  AND ` + sqliteDiagnosticDirectReplayExclusionSQL("e") + `
					  AND NOT EXISTS (
						SELECT 1 FROM decision_card_route_obligations route
						WHERE route.event_id = e.event_id AND route.status <> 'completed'
					  )
				)`
		err := q.QueryRowContext(ctx, query, append([]any{eventID}, diagnosticDirectReplayEventArgs()...)...).Scan(&eligible)
		return eligible, err
	case runtimepipelineobligation.PurposeDecisionRoute:
		err := q.QueryRowContext(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM decision_card_route_obligations route
					JOIN runs run ON run.run_id = route.run_id
					WHERE route.event_id = ?
					  AND route.status = 'pending'
					  AND run.status IN (`+runLifecycleActiveStateSQLValues+`)
				)`, eventID).Scan(&eligible)
		return eligible, err
	default:
		return false, fmt.Errorf("pipeline claim purpose %q cannot hydrate work", purpose)
	}
}

func (s *PipelinePostgresOwner) postgresPipelineBoundary(ctx context.Context, query runtimepipelineobligation.ClaimQuery) (*pipelineCandidate, error) {
	var visibilitySnapshot string
	if err := s.backend.QueryRowContext(ctx, `SELECT pg_current_snapshot()::text`).Scan(&visibilitySnapshot); err != nil {
		return nil, fmt.Errorf("capture postgres pipeline visibility snapshot: %w", err)
	}
	visibilitySnapshot = strings.TrimSpace(visibilitySnapshot)
	if visibilitySnapshot == "" {
		return nil, errors.New("postgres pipeline visibility snapshot is empty")
	}
	candidates, err := s.postgresPipelineCandidatePage(ctx, query, nil, nil, nil, visibilitySnapshot, 1, true)
	boundary, err := pipelineBoundaryCandidate(candidates, err)
	if boundary != nil {
		boundary.visibilitySnapshot = visibilitySnapshot
	}
	return boundary, err
}

func (s *PipelinePostgresOwner) postgresPipelineCandidates(
	ctx context.Context,
	query runtimepipelineobligation.ClaimQuery,
	after *pipelineCandidate,
	through *pipelineCandidate,
	examined map[int64]struct{},
	limit int,
) ([]pipelineCandidate, error) {
	if through == nil || strings.TrimSpace(through.visibilitySnapshot) == "" {
		return nil, errors.New("postgres pipeline visibility snapshot is missing")
	}
	return s.postgresPipelineCandidatePage(ctx, query, after, through, examined, through.visibilitySnapshot, limit, false)
}

func (s *PipelinePostgresOwner) postgresPipelineCandidatePage(
	ctx context.Context,
	query runtimepipelineobligation.ClaimQuery,
	after *pipelineCandidate,
	through *pipelineCandidate,
	examined map[int64]struct{},
	visibilitySnapshot string,
	limit int,
	boundary bool,
) ([]pipelineCandidate, error) {
	if query.Purpose == runtimepipelineobligation.PurposeDecisionRoute {
		whereAfter := ""
		args := []any{}
		whereRun := ""
		if runID := strings.TrimSpace(query.RunID); runID != "" {
			whereRun = fmt.Sprintf("AND route.run_id = $%d::uuid", len(args)+1)
			args = append(args, runID)
		}
		if len(examined) > 0 {
			whereAfter = fmt.Sprintf("AND NOT (route.insertion_sequence = ANY($%d::bigint[]))", len(args)+1)
			args = append(args, pq.Array(sortedPipelineSequences(examined)))
		}
		whereVisibility := fmt.Sprintf(
			"AND pg_visible_in_snapshot(route.insertion_transaction_id, $%d::pg_snapshot)",
			len(args)+1,
		)
		args = append(args, visibilitySnapshot)
		whereThrough := ""
		if through != nil {
			whereThrough = fmt.Sprintf("AND route.insertion_sequence <= $%d", len(args)+1)
			args = append(args, through.insertionSequence)
		}
		orderBy := "route.attempt_count ASC, route.next_attempt_at ASC, route.created_at ASC, route.event_id ASC"
		if boundary {
			orderBy = "route.insertion_sequence DESC"
		}
		args = append(args, limit)
		rows, err := s.backend.QueryContext(ctx, fmt.Sprintf(`
				SELECT route.event_id::text
				     , route.insertion_sequence
				     , route.attempt_count
				     , route.next_attempt_at
				     , route.created_at
				     , e.execution_mode
				FROM decision_card_route_obligations route
				JOIN runs run ON run.run_id = route.run_id
				JOIN events e ON e.event_id = route.event_id
				WHERE route.status = 'pending'
				  AND route.next_attempt_at <= now()
				  AND run.status IN (`+runLifecycleActiveStateSQLValues+`)
				  %s
				  %s
				  %s
				  %s
				ORDER BY %s
				LIMIT $%d`, whereRun, whereAfter, whereVisibility, whereThrough, orderBy, len(args)), args...)
		if err != nil {
			return nil, err
		}
		return scanPipelineCandidates(rows, true, "postgres decision-route pipeline candidates")
	}
	args := diagnosticDirectReplayEventArgs()
	whereRun := ""
	if runID := strings.TrimSpace(query.RunID); runID != "" {
		whereRun = fmt.Sprintf("AND e.run_id = $%d::uuid", len(args)+1)
		args = append(args, runID)
	}
	whereAfter := ""
	if after != nil {
		whereAfter = fmt.Sprintf("AND (e.created_at, e.event_id) > ($%d, $%d::uuid)", len(args)+1, len(args)+2)
		args = append(args, after.createdAt, after.eventID)
	}
	whereVisibility := fmt.Sprintf(
		"AND pg_visible_in_snapshot(e.xmin::text::xid8, $%d::pg_snapshot)",
		len(args)+1,
	)
	args = append(args, visibilitySnapshot)
	whereThrough := ""
	if through != nil {
		whereThrough = fmt.Sprintf("AND e.insertion_sequence <= $%d", len(args)+1)
		args = append(args, through.insertionSequence)
	}
	orderBy := "e.created_at ASC, e.event_id ASC"
	if boundary {
		orderBy = "e.insertion_sequence DESC"
	}
	args = append(args, limit)
	rows, err := s.backend.QueryContext(ctx, fmt.Sprintf(`
			SELECT e.event_id::text, e.insertion_sequence, 0, e.created_at, e.created_at, e.execution_mode
			FROM events e
			LEFT JOIN runs run ON run.run_id = e.run_id
			LEFT JOIN event_receipts receipt
		  ON receipt.event_id = e.event_id
		 AND receipt.subscriber_type = 'platform'
		 AND receipt.subscriber_id = 'pipeline'
		WHERE receipt.event_id IS NULL
			  AND (e.run_id IS NULL OR run.status IN (`+runLifecycleActiveStateSQLValues+`))
			  %s
			  %s
			  %s
			  %s
			  AND NOT EXISTS (
				SELECT 1 FROM decision_card_route_obligations route
				WHERE route.event_id = e.event_id AND route.status <> 'completed'
			)
			  AND %s
			ORDER BY %s
			LIMIT $%d`, whereRun, whereAfter, whereVisibility, whereThrough, postgresDiagnosticDirectReplayExclusionSQL("e", 1), orderBy, len(args)), args...)
	if err != nil {
		return nil, err
	}
	return scanPipelineCandidates(rows, false, "postgres pipeline candidates")
}

func (s *PipelineSQLiteOwner) sqlitePipelineBoundary(ctx context.Context, query runtimepipelineobligation.ClaimQuery) (*pipelineCandidate, error) {
	candidates, err := s.sqlitePipelineCandidatePage(ctx, query, nil, nil, nil, 1, true)
	return pipelineBoundaryCandidate(candidates, err)
}

func (s *PipelineSQLiteOwner) sqlitePipelineCandidates(
	ctx context.Context,
	query runtimepipelineobligation.ClaimQuery,
	after *pipelineCandidate,
	through *pipelineCandidate,
	examined map[int64]struct{},
	limit int,
) ([]pipelineCandidate, error) {
	return s.sqlitePipelineCandidatePage(ctx, query, after, through, examined, limit, false)
}

func (s *PipelineSQLiteOwner) sqlitePipelineCandidatePage(
	ctx context.Context,
	query runtimepipelineobligation.ClaimQuery,
	after *pipelineCandidate,
	through *pipelineCandidate,
	examined map[int64]struct{},
	limit int,
	boundary bool,
) ([]pipelineCandidate, error) {
	if query.Purpose == runtimepipelineobligation.PurposeDecisionRoute {
		whereAfter := ""
		args := []any{time.Now().UTC()}
		whereRun := ""
		if runID := strings.TrimSpace(query.RunID); runID != "" {
			whereRun = "AND route.run_id = ?"
			args = append(args, runID)
		}
		if len(examined) > 0 {
			rawExamined, err := json.Marshal(sortedPipelineSequences(examined))
			if err != nil {
				return nil, fmt.Errorf("encode examined SQLite decision routes: %w", err)
			}
			whereAfter = "AND route.insertion_sequence NOT IN (SELECT CAST(value AS INTEGER) FROM json_each(?))"
			args = append(args, string(rawExamined))
		}
		whereThrough := ""
		if through != nil {
			whereThrough = "AND route.insertion_sequence <= ?"
			args = append(args, through.insertionSequence)
		}
		orderBy := "route.attempt_count ASC, route.next_attempt_at ASC, route.created_at ASC, route.event_id ASC"
		if boundary {
			orderBy = "route.insertion_sequence DESC"
		}
		args = append(args, limit)
		rows, err := s.backend.QueryContext(ctx, `
				SELECT route.event_id
				     , route.insertion_sequence
				     , route.attempt_count
				     , route.next_attempt_at
				     , route.created_at
				     , e.execution_mode
				FROM decision_card_route_obligations route
				JOIN runs run ON run.run_id = route.run_id
				JOIN events e ON e.event_id = route.event_id
				WHERE route.status = 'pending'
				  AND route.next_attempt_at <= ?
				  AND run.status IN (`+runLifecycleActiveStateSQLValues+`)
				  `+whereRun+`
				  `+whereAfter+`
				  `+whereThrough+`
				ORDER BY `+orderBy+`
				LIMIT ?`, args...)
		if err != nil {
			return nil, err
		}
		return scanPipelineCandidates(rows, true, "sqlite decision-route pipeline candidates")
	}
	args := make([]any, 0, len(diagnosticDirectReplayEventArgs())+4)
	whereRun := ""
	if runID := strings.TrimSpace(query.RunID); runID != "" {
		whereRun = "AND e.run_id = ?"
		args = append(args, runID)
	}
	whereAfter := ""
	if after != nil {
		whereAfter = "AND (e.created_at, e.event_id) > (?, ?)"
		args = append(args, after.createdAt, after.eventID)
	}
	whereThrough := ""
	if through != nil {
		whereThrough = "AND e.insertion_sequence <= ?"
		args = append(args, through.insertionSequence)
	}
	orderBy := "e.created_at ASC, e.event_id ASC"
	if boundary {
		orderBy = "e.insertion_sequence DESC"
	}
	args = append(args, diagnosticDirectReplayEventArgs()...)
	args = append(args, limit)
	rows, err := s.backend.QueryContext(ctx, `
			SELECT e.event_id, e.insertion_sequence, 0, e.created_at, e.created_at, e.execution_mode
			FROM events e
			LEFT JOIN runs run ON run.run_id = e.run_id
			LEFT JOIN event_receipts receipt
		  ON receipt.event_id = e.event_id
		 AND receipt.subscriber_type = 'platform'
		 AND receipt.subscriber_id = 'pipeline'
			WHERE receipt.event_id IS NULL
			  AND (e.run_id IS NULL OR run.status IN (`+runLifecycleActiveStateSQLValues+`))
			  `+whereRun+`
			  `+whereAfter+`
			  `+whereThrough+`
			  AND NOT EXISTS (
				SELECT 1 FROM decision_card_route_obligations route
				WHERE route.event_id = e.event_id AND route.status <> 'completed'
		  )
		  AND `+sqliteDiagnosticDirectReplayExclusionSQL("e")+`
		ORDER BY `+orderBy+`
		LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	return scanPipelineCandidates(rows, false, "sqlite pipeline candidates")
}

func pipelineBoundaryCandidate(candidates []pipelineCandidate, err error) (*pipelineCandidate, error) {
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	boundary := candidates[0]
	return &boundary, nil
}

func sortedPipelineSequences(sequences map[int64]struct{}) []int64 {
	sorted := make([]int64, 0, len(sequences))
	for sequence := range sequences {
		sorted = append(sorted, sequence)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted
}

func scanPipelineCandidates(rows *sql.Rows, decisionRoute bool, operation string) (out []pipelineCandidate, err error) {
	if rows == nil {
		return nil, fmt.Errorf("%s: rows are missing", operation)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()
	out = make([]pipelineCandidate, 0)
	for rows.Next() {
		var (
			candidate  pipelineCandidate
			nextRaw    any
			createdRaw any
		)
		if err := rows.Scan(
			&candidate.eventID,
			&candidate.insertionSequence,
			&candidate.attemptCount,
			&nextRaw,
			&createdRaw,
			&candidate.executionMode,
		); err != nil {
			return nil, fmt.Errorf("%s: %w", operation, err)
		}
		var ok bool
		var parseErr error
		candidate.createdAt, ok, parseErr = sqliteTimeValue(createdRaw)
		if parseErr != nil {
			return nil, fmt.Errorf("%s created_at: %w", operation, parseErr)
		}
		if !ok {
			return nil, fmt.Errorf("%s created_at is missing", operation)
		}
		candidate.nextAttemptAt, ok, parseErr = sqliteTimeValue(nextRaw)
		if parseErr != nil {
			return nil, fmt.Errorf("%s next_attempt_at: %w", operation, parseErr)
		}
		if !ok {
			return nil, fmt.Errorf("%s next_attempt_at is missing", operation)
		}
		candidate.eventID = strings.TrimSpace(candidate.eventID)
		if !decisionRoute {
			candidate.nextAttemptAt = candidate.createdAt
		}
		out = append(out, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	return out, nil
}

func (s *postgresPipelineObligationStore) Settle(ctx context.Context, claim runtimepipelineobligation.Claim, disposition runtimepipelineobligation.Disposition) (runtimepipelineobligation.SettlementOutcome, error) {
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	state, err := s.lockPostgresPipelineClaim(claim)
	if err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	defer state.operationMu.Unlock()
	handoff, err := runhandoff.ReserveCandidateHandoff(ctx)
	if err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	defer handoff.Rollback()
	lease := state.postgresLease
	tx, err := lease.Session().BeginTx(ctx)
	if err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	if err := writePipelineDispositionTx(ctx, tx, claim.EventID(), claim.Purpose(), disposition, true, time.Now().UTC()); err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, errors.Join(err, postgresbackend.RollbackAuthorityTransaction(tx, lease.Session()))
	}
	runID, err := eventRunIDForCompletionCandidateTx(ctx, tx, claim.EventID(), true)
	if err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, errors.Join(err, postgresbackend.RollbackAuthorityTransaction(tx, lease.Session()))
	}
	if runID != "" {
		effects := newRevisionEffects()
		if err := addPipelineDispositionRevisionEffects(effects, runID); err != nil {
			return runtimepipelineobligation.SettlementOutcome{}, errors.Join(err, postgresbackend.RollbackAuthorityTransaction(tx, lease.Session()))
		}
		if _, err := s.candidateRequests.RequestCompletionCandidateTx(ctx, tx, runID, nil, handoff); err != nil {
			return runtimepipelineobligation.SettlementOutcome{}, errors.Join(err, postgresbackend.RollbackAuthorityTransaction(tx, lease.Session()))
		}
		if disposition.Successful() {
			if err := postgresDeliveryAdapter.CommitPipelineHandoff(ctx, tx, claim.EventID()); err != nil {
				return runtimepipelineobligation.SettlementOutcome{}, errors.Join(err, postgresbackend.RollbackAuthorityTransaction(tx, lease.Session()))
			}
		}
		if _, err := privaterunforkrevision.FinalizePostgres(ctx, tx, effects); err != nil {
			return runtimepipelineobligation.SettlementOutcome{}, errors.Join(err, postgresbackend.RollbackAuthorityTransaction(tx, lease.Session()))
		}
	} else {
		return runtimepipelineobligation.SettlementOutcome{}, errors.Join(errors.New("settled pipeline event has no owning run"), postgresbackend.RollbackAuthorityTransaction(tx, lease.Session()))
	}
	if err := tx.Commit(); err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, errors.Join(err, postgresbackend.RollbackAuthorityTransaction(tx, lease.Session()))
	}
	outcome := runtimepipelineobligation.CommittedSettlement(disposition.Successful())
	endErr := lease.Session().EndTx(tx)
	releaseErr := s.releasePostgresPipelineClaimLocked(context.WithoutCancel(ctx), claim, state)
	handoffErr := handoff.Commit()
	return outcome, errors.Join(endErr, releaseErr, handoffErr)
}

func (s *sqlitePipelineObligationStore) Settle(ctx context.Context, claim runtimepipelineobligation.Claim, disposition runtimepipelineobligation.Disposition) (runtimepipelineobligation.SettlementOutcome, error) {
	if err := disposition.ValidateFor(claim.Purpose()); err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	state, err := s.lockSQLitePipelineClaim(claim)
	if err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	defer state.operationMu.Unlock()
	handoff, err := runhandoff.ReserveCandidateHandoff(ctx)
	if err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	defer handoff.Rollback()
	effects := newRevisionEffects()
	err = s.runRuntimeMutation(ctx, "settle sqlite pipeline obligation", effects, func(txctx context.Context, tx *sql.Tx) error {
		if current, err := s.sqlitePipelineClaimState(claim); err != nil || current != state {
			if err != nil {
				return err
			}
			return runtimepipelineobligation.ErrStaleClaim
		}
		if err := writePipelineDispositionTx(txctx, tx, claim.EventID(), claim.Purpose(), disposition, false, s.now()); err != nil {
			return err
		}
		runID, err := eventRunIDForCompletionCandidateTx(txctx, tx, claim.EventID(), false)
		if err != nil {
			return err
		}
		if runID != "" {
			if err := addPipelineDispositionRevisionEffects(effects, runID); err != nil {
				return err
			}
			if _, err = s.candidateRequests.RequestCompletionCandidateTx(txctx, tx, runID, nil, handoff); err != nil {
				return err
			}
		} else {
			return errors.New("settled pipeline event has no owning run")
		}
		if disposition.Successful() {
			return sqliteDeliveryAdapter.CommitPipelineHandoff(txctx, tx, claim.EventID())
		}
		return nil
	})
	if err != nil {
		return runtimepipelineobligation.SettlementOutcome{}, err
	}
	outcome := runtimepipelineobligation.CommittedSettlement(disposition.Successful())
	releaseErr := s.releaseSQLitePipelineClaimLocked(claim, state)
	return outcome, errors.Join(releaseErr, handoff.Commit())
}

type pipelineExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *PipelinePostgresOwner) TerminalizePipelineObligationTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
	disposition runtimepipelineobligation.Disposition,
	at time.Time,
) error {
	if tx == nil {
		return errors.New("pipeline parent terminalization transaction is required")
	}
	if err := disposition.ValidateFor(runtimepipelineobligation.PurposeRecovery); err != nil {
		return err
	}
	registry := s.postgresPipelineClaims()
	if registry == nil {
		return errors.New("PostgreSQL pipeline claim registry is required")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, state := range registry.claims {
		if state != nil && state.claim.EventID() == strings.TrimSpace(eventID) {
			return runtimepipelineobligation.ErrBusy
		}
	}
	if registry.testAfterParentClaimScan != nil {
		registry.testAfterParentClaimScan()
	}
	var acquired bool
	if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtext($1))`, replayClaimLockKey(eventID)).Scan(&acquired); err != nil {
		return fmt.Errorf("fence pipeline parent terminalization: %w", err)
	}
	if !acquired {
		return runtimepipelineobligation.ErrBusy
	}
	return terminalizeUnclaimedPipelineObligationTx(ctx, tx, eventID, disposition, true, at)
}

func (s *PipelineSQLiteOwner) TerminalizePipelineObligationTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
	disposition runtimepipelineobligation.Disposition,
	at time.Time,
) error {
	if tx == nil {
		return errors.New("sqlite pipeline parent terminalization transaction is required")
	}
	if err := disposition.ValidateFor(runtimepipelineobligation.PurposeRecovery); err != nil {
		return err
	}
	eventID = strings.TrimSpace(eventID)
	s.pipelineClaimMu.Lock()
	_, claims := s.pipelineClaimOwner()
	for _, state := range claims {
		if state != nil && state.claim.EventID() == eventID {
			s.pipelineClaimMu.Unlock()
			return runtimepipelineobligation.ErrBusy
		}
	}
	s.pipelineClaimMu.Unlock()
	return terminalizeUnclaimedPipelineObligationTx(ctx, tx, eventID, disposition, false, at)
}

func terminalizeUnclaimedPipelineObligationTx(
	ctx context.Context,
	tx pipelineExecer,
	eventID string,
	disposition runtimepipelineobligation.Disposition,
	postgres bool,
	at time.Time,
) error {
	pending, receiptOutcome, err := pipelineDispositionState(ctx, tx, eventID, postgres)
	if err != nil {
		return err
	}
	if pending && receiptOutcome == "success" && !disposition.Successful() {
		return supersedeProcessedParentDecisionRouteTx(ctx, tx, eventID, postgres, at)
	}
	exact, found, err := exactStoredPipelineDisposition(ctx, tx, eventID, disposition, postgres)
	if err != nil {
		return err
	}
	if found {
		if exact {
			return settleExactParentDecisionRouteTx(ctx, tx, eventID, disposition, postgres, at)
		}
		return runtimepipelineobligation.ErrIneligible
	}
	return writePipelineDispositionTx(ctx, tx, eventID, runtimepipelineobligation.PurposeRecovery, disposition, postgres, at)
}

func supersedeProcessedParentDecisionRouteTx(
	ctx context.Context,
	tx pipelineExecer,
	eventID string,
	postgres bool,
	at time.Time,
) error {
	query := `UPDATE decision_card_route_obligations SET status = 'superseded', superseded_at = ?, updated_at = ? WHERE event_id = ? AND status = 'pending'`
	args := []any{at, at, eventID}
	if postgres {
		query = `UPDATE decision_card_route_obligations SET status = 'superseded', superseded_at = $2, updated_at = $2 WHERE event_id = $1::uuid AND status = 'pending'`
		args = []any{eventID, at}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	return requireOnePipelineMutation(result, err, "supersede processed parent decision-route obligation")
}

func settleExactParentDecisionRouteTx(
	ctx context.Context,
	tx pipelineExecer,
	eventID string,
	disposition runtimepipelineobligation.Disposition,
	postgres bool,
	at time.Time,
) error {
	pending, _, err := pipelineDispositionState(ctx, tx, eventID, postgres)
	if err != nil || !pending {
		return err
	}
	if disposition.Successful() {
		return runtimepipelineobligation.ErrIneligible
	}
	status := "quarantined"
	query := `UPDATE decision_card_route_obligations SET status = ?, quarantined_at = ?, updated_at = ? WHERE event_id = ? AND status = 'pending'`
	args := []any{status, at, at, eventID}
	if postgres {
		query = `UPDATE decision_card_route_obligations SET status = $2, quarantined_at = $3, updated_at = $3 WHERE event_id = $1::uuid AND status = 'pending'`
		args = []any{eventID, status, at}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	return requireOnePipelineMutation(result, nil, "settle exact parent decision-route obligation")
}

func exactStoredPipelineDisposition(
	ctx context.Context,
	q pipelineExecer,
	eventID string,
	disposition runtimepipelineobligation.Disposition,
	postgres bool,
) (exact bool, found bool, err error) {
	expected, err := storedPipelineDispositionFor(disposition)
	if err != nil {
		return false, false, err
	}
	var (
		outcome     string
		reasonCode  string
		failureRaw  any
		sideEffects []byte
	)
	query := `
		SELECT outcome, COALESCE(reason_code, ''), failure, side_effects
		FROM event_receipts
		WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	args := []any{eventID}
	if postgres {
		query = `
			SELECT outcome, COALESCE(reason_code, ''), failure, side_effects
			FROM event_receipts
			WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	}
	if err := q.QueryRowContext(ctx, query, args...).Scan(&outcome, &reasonCode, &failureRaw, &sideEffects); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, false, nil
		}
		return false, false, err
	}
	actualFailure, err := decodeStoredFailure(failureRaw)
	if err != nil {
		return false, true, err
	}
	var actualSideEffects pipelineReceiptSideEffects
	if err := json.Unmarshal(sideEffects, &actualSideEffects); err != nil {
		return false, true, fmt.Errorf("decode pipeline receipt side effects: %w", err)
	}
	return strings.TrimSpace(outcome) == expected.outcome &&
			strings.TrimSpace(reasonCode) == expected.reasonCode &&
			samePipelineFailure(actualFailure, disposition.Failure()) &&
			actualSideEffects == expected.sideEffects,
		true,
		nil
}

func samePipelineFailure(left, right *runtimefailures.Envelope) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftRaw, leftErr := runtimefailures.MarshalEnvelope(*left)
	rightRaw, rightErr := runtimefailures.MarshalEnvelope(*right)
	return leftErr == nil && rightErr == nil && string(leftRaw) == string(rightRaw)
}

func writePipelineDispositionTx(ctx context.Context, tx pipelineExecer, eventID string, purpose runtimepipelineobligation.Purpose, disposition runtimepipelineobligation.Disposition, postgres bool, now time.Time) error {
	routePending, receiptOutcome, err := pipelineDispositionState(ctx, tx, eventID, postgres)
	if err != nil {
		return err
	}
	if disposition.Kind() == runtimepipelineobligation.DispositionDeferred {
		if !routePending {
			return errors.New("deferred pipeline disposition requires a pending decision-route obligation")
		}
		raw, err := json.Marshal(disposition.Failure())
		if err != nil {
			return err
		}
		query := `UPDATE decision_card_route_obligations SET attempt_count = attempt_count + 1, next_attempt_at = ?, last_failure = ?, updated_at = ? WHERE event_id = ? AND status = 'pending'`
		args := []any{disposition.RetryAt(), string(raw), now, eventID}
		if postgres {
			query = `UPDATE decision_card_route_obligations SET attempt_count = attempt_count + 1, next_attempt_at = $2, last_failure = $3::jsonb, updated_at = $4 WHERE event_id = $1::uuid AND status = 'pending'`
			args = []any{eventID, disposition.RetryAt(), string(raw), now}
		}
		result, err := tx.ExecContext(ctx, query, args...)
		return requireOnePipelineMutation(result, err, "defer decision-route obligation")
	}
	if receiptOutcome != "" {
		if !(routePending && receiptOutcome == "success" && disposition.Kind() == runtimepipelineobligation.DispositionAcknowledged) {
			return runtimepipelineobligation.ErrIneligible
		}
	} else {
		if err := writeExactPlatformPipelineReceipt(ctx, tx, eventID, disposition, postgres, now); err != nil {
			return err
		}
	}
	if !routePending {
		return nil
	}
	status := "completed"
	timeColumn := "completed_at"
	if disposition.Kind() == runtimepipelineobligation.DispositionQuarantined ||
		disposition.Kind() == runtimepipelineobligation.DispositionTerminal ||
		disposition.Kind() == runtimepipelineobligation.DispositionDeadLetter {
		status = "quarantined"
		timeColumn = "quarantined_at"
	}
	query := fmt.Sprintf(`UPDATE decision_card_route_obligations SET status = ?, %s = ?, updated_at = ? WHERE event_id = ? AND status = 'pending'`, timeColumn)
	args := []any{status, now, now, eventID}
	if postgres {
		query = fmt.Sprintf(`UPDATE decision_card_route_obligations SET status = $2, %s = $3, updated_at = $3 WHERE event_id = $1::uuid AND status = 'pending'`, timeColumn)
		args = []any{eventID, status, now}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	return requireOnePipelineMutation(result, err, "settle decision-route obligation")
}

func WritePipelineDispositionTx(ctx context.Context, tx interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, eventID string, purpose runtimepipelineobligation.Purpose, disposition runtimepipelineobligation.Disposition, postgres bool, now time.Time) error {
	return writePipelineDispositionTx(ctx, tx, eventID, purpose, disposition, postgres, now)
}

func pipelineDispositionState(ctx context.Context, tx pipelineExecer, eventID string, postgres bool) (bool, string, error) {
	var (
		routePending bool
		outcome      sql.NullString
	)
	query := `
		SELECT EXISTS (
			SELECT 1 FROM decision_card_route_obligations
			WHERE event_id = ? AND status = 'pending'
		), (
			SELECT outcome FROM event_receipts
			WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'
		)`
	args := []any{eventID, eventID}
	if postgres {
		query = `
			SELECT EXISTS (
				SELECT 1 FROM decision_card_route_obligations
				WHERE event_id = $1::uuid AND status = 'pending'
			), (
				SELECT outcome FROM event_receipts
				WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'
			)`
		args = []any{eventID}
	}
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&routePending, &outcome); err != nil {
		return false, "", err
	}
	return routePending, strings.TrimSpace(outcome.String), nil
}

func writeExactPlatformPipelineReceipt(ctx context.Context, tx pipelineExecer, eventID string, disposition runtimepipelineobligation.Disposition, postgres bool, now time.Time) error {
	stored, err := storedPipelineDispositionFor(disposition)
	if err != nil {
		return err
	}
	failureJSON, err := encodeStoredFailure(disposition.Failure())
	if err != nil {
		return err
	}
	sideEffects, err := json.Marshal(stored.sideEffects)
	if err != nil {
		return err
	}
	receiptID := uuid.NewString()
	query := `
		INSERT INTO event_receipts (
			receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, failure, side_effects, processed_at
		)
		SELECT ?, e.event_id, 'platform', 'pipeline', e.entity_id, e.flow_instance, ?, ?, ?, ?, ?
		FROM events e WHERE e.event_id = ?
		ON CONFLICT(event_id, subscriber_type, subscriber_id) DO NOTHING`
	args := []any{receiptID, stored.outcome, stored.reasonCode, failureJSON, string(sideEffects), now, eventID}
	if postgres {
		query = `
			INSERT INTO event_receipts (
				receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
				outcome, reason_code, failure, side_effects, processed_at
			)
			SELECT $1::uuid, e.event_id, 'platform', 'pipeline', e.entity_id, e.flow_instance, $2, $3, $4::jsonb, $5::jsonb, $6
			FROM events e WHERE e.event_id = $7::uuid
			ON CONFLICT(event_id, subscriber_type, subscriber_id) DO NOTHING`
	}
	result, err := tx.ExecContext(ctx, query, args...)
	return requireOnePipelineMutation(result, err, "write platform pipeline acknowledgement")
}

type storedPipelineDisposition struct {
	outcome     string
	reasonCode  string
	sideEffects pipelineReceiptSideEffects
}

func storedPipelineDispositionFor(disposition runtimepipelineobligation.Disposition) (storedPipelineDisposition, error) {
	if err := disposition.ValidateFor(runtimepipelineobligation.PurposeRecovery); err != nil {
		return storedPipelineDisposition{}, err
	}
	outcome := "success"
	managerStatus := "processed"
	if disposition.Kind() != runtimepipelineobligation.DispositionAcknowledged {
		outcome = "dead_letter"
		managerStatus = "error"
		if disposition.Kind() == runtimepipelineobligation.DispositionDeadLetter {
			managerStatus = "dead_letter"
		}
	}
	reasonCode := strings.TrimSpace(disposition.ReasonCode())
	if reasonCode == "" {
		if outcome == "success" {
			reasonCode = "pipeline_persisted"
		} else {
			reasonCode = "pipeline_error"
		}
	}
	return storedPipelineDisposition{
		outcome:     outcome,
		reasonCode:  reasonCode,
		sideEffects: pipelineReceiptSideEffects{ManagerStatus: managerStatus, ReasonCode: reasonCode},
	}, nil
}

func requireOnePipelineMutation(result sql.Result, err error, operation string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: read affected rows: %w", operation, err)
	}
	if rows != 1 {
		return fmt.Errorf("%s: affected %d rows, want 1", operation, rows)
	}
	return nil
}

func (s *PipelinePostgresOwner) postgresPipelineClaimState(claim runtimepipelineobligation.Claim) (*pipelineClaimState, error) {
	registry := s.postgresPipelineClaims()
	if registry == nil {
		return nil, runtimepipelineobligation.ErrStaleClaim
	}
	registry.mu.Lock()
	token, err := registry.issuer.Token(claim)
	if err != nil {
		registry.mu.Unlock()
		return nil, err
	}
	state := registry.claims[token]
	if state == nil {
		registry.mu.Unlock()
		return nil, runtimepipelineobligation.ErrStaleClaim
	}
	if err := registry.issuer.Verify(state.claim, claim.EventID(), claim.Purpose()); err != nil {
		registry.mu.Unlock()
		return nil, err
	}
	if state.scanToken != "" && registry.scans[state.scanToken] == nil {
		registry.mu.Unlock()
		return nil, runtimepipelineobligation.ErrStaleClaim
	}
	lease := state.postgresLease
	registry.mu.Unlock()
	if lease == nil || !lease.Current() {
		return nil, runtimepipelineobligation.ErrStaleClaim
	}
	return state, nil
}

func (s *PipelinePostgresOwner) lockPostgresPipelineClaim(claim runtimepipelineobligation.Claim) (*pipelineClaimState, error) {
	state, err := s.postgresPipelineClaimState(claim)
	if err != nil {
		return nil, err
	}
	state.operationMu.Lock()
	current, err := s.postgresPipelineClaimState(claim)
	if err != nil || current != state {
		state.operationMu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, runtimepipelineobligation.ErrStaleClaim
	}
	if state.postgresLease == nil || state.postgresLease.Session() == nil {
		state.operationMu.Unlock()
		return nil, runtimepipelineobligation.ErrStaleClaim
	}
	return state, nil
}

func (s *PipelineSQLiteOwner) sqlitePipelineClaimState(claim runtimepipelineobligation.Claim) (*pipelineClaimState, error) {
	s.pipelineClaimMu.Lock()
	defer s.pipelineClaimMu.Unlock()
	issuer, claims := s.pipelineClaimOwner()
	token, err := issuer.Token(claim)
	if err != nil {
		return nil, err
	}
	state := claims[token]
	if state == nil {
		return nil, runtimepipelineobligation.ErrStaleClaim
	}
	if err := issuer.Verify(state.claim, claim.EventID(), claim.Purpose()); err != nil {
		return nil, err
	}
	return state, nil
}

func (s *PipelineSQLiteOwner) SetSQLiteClaimOperationHooksForTest(claim runtimepipelineobligation.Claim, before, after func()) error {
	state, err := s.sqlitePipelineClaimState(claim)
	if err != nil {
		return err
	}
	state.testBeforeSQLiteOperationLock = before
	state.testAfterSQLiteOperationLock = after
	return nil
}

func (s *PipelineSQLiteOwner) lockSQLitePipelineClaim(claim runtimepipelineobligation.Claim) (*pipelineClaimState, error) {
	state, err := s.sqlitePipelineClaimState(claim)
	if err != nil {
		return nil, err
	}
	if state.testBeforeSQLiteOperationLock != nil {
		state.testBeforeSQLiteOperationLock()
	}
	state.operationMu.Lock()
	current, err := s.sqlitePipelineClaimState(claim)
	if err != nil || current != state {
		state.operationMu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, runtimepipelineobligation.ErrStaleClaim
	}
	if state.testAfterSQLiteOperationLock != nil {
		state.testAfterSQLiteOperationLock()
	}
	return state, nil
}

func (s *postgresPipelineObligationStore) Release(ctx context.Context, claim runtimepipelineobligation.Claim) error {
	return s.releasePostgresPipelineClaim(ctx, claim)
}

func (s *sqlitePipelineObligationStore) Release(_ context.Context, claim runtimepipelineobligation.Claim) error {
	return s.releaseSQLitePipelineClaim(claim)
}

func (s *PipelinePostgresOwner) releasePostgresPipelineClaim(ctx context.Context, claim runtimepipelineobligation.Claim) error {
	if ctx == nil {
		ctx = context.Background()
	}
	state, err := s.lockPostgresPipelineClaim(claim)
	if err != nil {
		return err
	}
	defer state.operationMu.Unlock()
	return s.releasePostgresPipelineClaimLocked(ctx, claim, state)
}

func (s *PipelinePostgresOwner) releasePostgresPipelineClaimLocked(ctx context.Context, claim runtimepipelineobligation.Claim, state *pipelineClaimState) error {
	registry := s.postgresPipelineClaims()
	if registry == nil || state == nil {
		return runtimepipelineobligation.ErrStaleClaim
	}
	token, err := registry.issuer.Token(claim)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	if registry.claims[token] != state {
		registry.mu.Unlock()
		return runtimepipelineobligation.ErrStaleClaim
	}
	if err := registry.issuer.Verify(state.claim, claim.EventID(), claim.Purpose()); err != nil {
		registry.mu.Unlock()
		return err
	}
	registry.mu.Unlock()
	if state.postgresLease == nil {
		return runtimepipelineobligation.ErrStaleClaim
	}
	releaseErr := state.postgresLease.ReleaseTerminal(context.WithoutCancel(ctx))
	state.postgresLease = nil
	return releaseErr
}

func (s *PipelineSQLiteOwner) releaseSQLitePipelineClaim(claim runtimepipelineobligation.Claim) error {
	state, err := s.lockSQLitePipelineClaim(claim)
	if err != nil {
		return err
	}
	defer state.operationMu.Unlock()
	return s.releaseSQLitePipelineClaimLocked(claim, state)
}

func (s *PipelineSQLiteOwner) releaseSQLitePipelineClaimLocked(claim runtimepipelineobligation.Claim, state *pipelineClaimState) error {
	s.pipelineClaimMu.Lock()
	defer s.pipelineClaimMu.Unlock()
	issuer, claims := s.pipelineClaimOwner()
	token, err := issuer.Token(claim)
	if err != nil {
		return err
	}
	if state == nil || claims[token] != state {
		return runtimepipelineobligation.ErrStaleClaim
	}
	if err := issuer.Verify(state.claim, claim.EventID(), claim.Purpose()); err != nil {
		return err
	}
	_, scans := s.pipelineScanOwner()
	if scanState := scans[state.scanToken]; scanState != nil {
		delete(scanState.openClaims, token)
	}
	delete(claims, token)
	if s.testPipelineReleaseErr != nil {
		return s.testPipelineReleaseErr()
	}
	return nil
}

func (s *postgresPipelineObligationStore) GlobalWorkPresence(ctx context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	var out runtimepipelineobligation.GlobalWorkPresence
	args := diagnosticDirectReplayEventArgs()
	err := s.backend.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1 FROM events e
			LEFT JOIN runs run ON run.run_id = e.run_id
			LEFT JOIN event_receipts receipt ON receipt.event_id = e.event_id AND receipt.subscriber_type = 'platform' AND receipt.subscriber_id = 'pipeline'
			WHERE receipt.event_id IS NULL
			  AND (e.run_id IS NULL OR run.status IN (`+runLifecycleActiveStateSQLValues+`))
			  AND NOT EXISTS (SELECT 1 FROM decision_card_route_obligations route WHERE route.event_id = e.event_id AND route.status <> 'completed')
			  AND %s
		), EXISTS (
			SELECT 1 FROM decision_card_route_obligations route JOIN runs run ON run.run_id = route.run_id
				WHERE route.status = 'pending' AND route.next_attempt_at <= now() AND run.status IN (`+runLifecycleActiveStateSQLValues+`)
		), COALESCE((
			SELECT MIN(e.created_at) FROM events e
				LEFT JOIN runs run ON run.run_id = e.run_id
				LEFT JOIN event_receipts receipt ON receipt.event_id = e.event_id AND receipt.subscriber_type = 'platform' AND receipt.subscriber_id = 'pipeline'
				WHERE receipt.event_id IS NULL
				  AND (e.run_id IS NULL OR run.status IN (`+runLifecycleActiveStateSQLValues+`))
				  AND NOT EXISTS (SELECT 1 FROM decision_card_route_obligations route WHERE route.event_id = e.event_id AND route.status <> 'completed')
				  AND %s
		), '0001-01-01'::timestamptz)`,
		postgresDiagnosticDirectReplayExclusionSQL("e", 1),
		postgresDiagnosticDirectReplayExclusionSQL("e", 1)), args...).Scan(&out.ProcessingEligible, &out.DecisionRouteDue, &out.OldestEligibleEvent)
	return out, err
}

func (s *sqlitePipelineObligationStore) GlobalWorkPresence(ctx context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	var (
		out       runtimepipelineobligation.GlobalWorkPresence
		oldestRaw any
	)
	diagnostics := diagnosticDirectReplayEventArgs()
	args := make([]any, 0, len(diagnostics)*2+1)
	args = append(args, diagnostics...)
	args = append(args, time.Now().UTC())
	args = append(args, diagnostics...)
	err := s.backend.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM events e
			LEFT JOIN runs run ON run.run_id = e.run_id
			LEFT JOIN event_receipts receipt ON receipt.event_id = e.event_id AND receipt.subscriber_type = 'platform' AND receipt.subscriber_id = 'pipeline'
			WHERE receipt.event_id IS NULL
			  AND (e.run_id IS NULL OR run.status IN (`+runLifecycleActiveStateSQLValues+`))
			  AND NOT EXISTS (SELECT 1 FROM decision_card_route_obligations route WHERE route.event_id = e.event_id AND route.status <> 'completed')
			  AND `+sqliteDiagnosticDirectReplayExclusionSQL("e")+`
		), EXISTS (
			SELECT 1 FROM decision_card_route_obligations route JOIN runs run ON run.run_id = route.run_id
				WHERE route.status = 'pending' AND route.next_attempt_at <= ? AND run.status IN (`+runLifecycleActiveStateSQLValues+`)
		), (
			SELECT MIN(e.created_at) FROM events e
				LEFT JOIN runs run ON run.run_id = e.run_id
				LEFT JOIN event_receipts receipt ON receipt.event_id = e.event_id AND receipt.subscriber_type = 'platform' AND receipt.subscriber_id = 'pipeline'
				WHERE receipt.event_id IS NULL
				  AND (e.run_id IS NULL OR run.status IN (`+runLifecycleActiveStateSQLValues+`))
				  AND NOT EXISTS (SELECT 1 FROM decision_card_route_obligations route WHERE route.event_id = e.event_id AND route.status <> 'completed')
				  AND `+sqliteDiagnosticDirectReplayExclusionSQL("e")+`
			)`, args...).Scan(
		&out.ProcessingEligible, &out.DecisionRouteDue, &oldestRaw)
	if err != nil {
		return out, err
	}
	if oldest, ok, parseErr := sqliteTimeValue(oldestRaw); parseErr != nil {
		return out, fmt.Errorf("parse oldest SQLite pipeline obligation: %w", parseErr)
	} else if ok {
		out.OldestEligibleEvent = oldest
	}
	return out, nil
}

func (s *postgresPipelineObligationStore) SummarizeRun(ctx context.Context, runID string) (runtimepipelineobligation.RunSummary, error) {
	return summarizePipelineRun(ctx, s.backend, runID, true)
}

func (s *sqlitePipelineObligationStore) SummarizeRun(ctx context.Context, runID string) (runtimepipelineobligation.RunSummary, error) {
	return summarizePipelineRun(ctx, s.backend, runID, false)
}

func (s *PipelinePostgresOwner) SummarizeRunTx(ctx context.Context, tx *sql.Tx, runID string) (runtimepipelineobligation.RunSummary, error) {
	return summarizePipelineRun(ctx, tx, runID, true)
}

func (s *PipelineSQLiteOwner) SummarizeRunTx(ctx context.Context, tx *sql.Tx, runID string) (runtimepipelineobligation.RunSummary, error) {
	return summarizePipelineRun(ctx, tx, runID, false)
}

func (s *PipelinePostgresOwner) TerminalizeRunTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	disposition runtimepipelineobligation.Disposition,
	at time.Time,
) (int, error) {
	if !disposition.Terminal() || disposition.Successful() {
		return 0, errors.New("parent run terminalization requires a terminal non-success pipeline disposition")
	}
	diagnostics := diagnosticDirectReplayEventArgs()
	args := append([]any{strings.TrimSpace(runID)}, diagnostics...)
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT e.event_id::text
		FROM events e
		LEFT JOIN event_receipts receipt
		  ON receipt.event_id = e.event_id
		 AND receipt.subscriber_type = 'platform'
		 AND receipt.subscriber_id = 'pipeline'
			WHERE e.run_id = $1::uuid
			  AND (
				receipt.event_id IS NULL
				OR (
					receipt.outcome = 'success'
					AND EXISTS (
						SELECT 1 FROM decision_card_route_obligations route
						WHERE route.event_id = e.event_id AND route.status = 'pending'
					)
				)
			  )
			  AND %s
		ORDER BY e.created_at, e.event_id`,
		postgresDiagnosticDirectReplayExclusionSQL("e", 2)), args...)
	if err != nil {
		return 0, fmt.Errorf("list PostgreSQL pipeline parent terminalization targets: %w", err)
	}
	eventIDs, err := scanOrderedEventIDs(rows, "PostgreSQL pipeline parent terminalization")
	if err != nil {
		return 0, err
	}
	for _, eventID := range eventIDs {
		if err := s.TerminalizePipelineObligationTx(ctx, tx, eventID, disposition, at); err != nil {
			return 0, fmt.Errorf("terminalize PostgreSQL pipeline event %s: %w", eventID, err)
		}
	}
	return len(eventIDs), nil
}

func (s *PipelineSQLiteOwner) TerminalizeRunTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	disposition runtimepipelineobligation.Disposition,
	at time.Time,
) (int, error) {
	if !disposition.Terminal() || disposition.Successful() {
		return 0, errors.New("parent run terminalization requires a terminal non-success pipeline disposition")
	}
	args := append([]any{strings.TrimSpace(runID)}, diagnosticDirectReplayEventArgs()...)
	rows, err := tx.QueryContext(ctx, `
		SELECT e.event_id
		FROM events e
		LEFT JOIN event_receipts receipt
		  ON receipt.event_id = e.event_id
		 AND receipt.subscriber_type = 'platform'
		 AND receipt.subscriber_id = 'pipeline'
			WHERE e.run_id = ?
			  AND (
				receipt.event_id IS NULL
				OR (
					receipt.outcome = 'success'
					AND EXISTS (
						SELECT 1 FROM decision_card_route_obligations route
						WHERE route.event_id = e.event_id AND route.status = 'pending'
					)
				)
			  )
			  AND `+sqliteDiagnosticDirectReplayExclusionSQL("e")+`
		ORDER BY e.created_at, e.event_id`, args...)
	if err != nil {
		return 0, fmt.Errorf("list SQLite pipeline parent terminalization targets: %w", err)
	}
	eventIDs, err := scanOrderedEventIDs(rows, "SQLite pipeline parent terminalization")
	if err != nil {
		return 0, err
	}
	for _, eventID := range eventIDs {
		if err := s.TerminalizePipelineObligationTx(ctx, tx, eventID, disposition, at); err != nil {
			return 0, fmt.Errorf("terminalize SQLite pipeline event %s: %w", eventID, err)
		}
	}
	return len(eventIDs), nil
}

func summarizePipelineRun(ctx context.Context, q pipelineQueryer, runID string, postgres bool) (runtimepipelineobligation.RunSummary, error) {
	out := runtimepipelineobligation.RunSummary{RunID: strings.TrimSpace(runID)}
	if _, err := uuid.Parse(out.RunID); err != nil {
		return out, fmt.Errorf("pipeline run summary: %w", err)
	}
	diagnostics := diagnosticDirectReplayEventArgs()
	diagnosticPredicate := sqliteDiagnosticDirectReplayExclusionSQL("e")
	runPlaceholder := "?"
	args := make([]any, 0, len(diagnostics)+1)
	args = append(args, diagnostics...)
	args = append(args, out.RunID)
	if postgres {
		diagnosticPredicate = postgresDiagnosticDirectReplayExclusionSQL("e", 1)
		runPlaceholder = fmt.Sprintf("$%d::uuid", len(diagnostics)+1)
	}
	query := fmt.Sprintf(`
			WITH classified AS (
				SELECT
					e.event_id,
					NOT (%s) AS diagnostic,
					receipt.event_id AS receipt_id,
					receipt.outcome AS receipt_outcome,
					route.event_id AS route_id,
					route.status AS route_status
				FROM events e
				LEFT JOIN event_receipts receipt
				  ON receipt.event_id = e.event_id
				 AND receipt.subscriber_type = 'platform'
				 AND receipt.subscriber_id = 'pipeline'
				LEFT JOIN decision_card_route_obligations route ON route.event_id = e.event_id
				WHERE e.run_id = %s
			)
			SELECT
				COALESCE(SUM(CASE WHEN NOT classified.diagnostic AND classified.receipt_id IS NULL AND (classified.route_id IS NULL OR classified.route_status = 'completed') THEN 1 ELSE 0 END), 0),
				COALESCE(SUM(CASE WHEN NOT classified.diagnostic AND classified.receipt_outcome = 'success' THEN 1 ELSE 0 END), 0),
				COALESCE(SUM(CASE WHEN NOT classified.diagnostic AND classified.receipt_id IS NOT NULL AND classified.receipt_outcome <> 'success' THEN 1 ELSE 0 END), 0),
				COALESCE(SUM(CASE WHEN NOT classified.diagnostic AND classified.route_id IS NOT NULL AND classified.route_status = 'pending' THEN 1 ELSE 0 END), 0),
				COALESCE(SUM(CASE WHEN NOT classified.diagnostic AND classified.route_id IS NOT NULL AND classified.route_status = 'pending' AND classified.receipt_outcome = 'success' THEN 1 ELSE 0 END), 0),
				COALESCE(SUM(CASE WHEN classified.diagnostic THEN 1 ELSE 0 END), 0)
			FROM classified`,
		diagnosticPredicate, runPlaceholder)
	err := q.QueryRowContext(ctx, query, args...).Scan(
		&out.Replayable, &out.Acknowledged, &out.TerminalNonSuccess, &out.Deferred,
		&out.ProcessedDeferred, &out.DiagnosticExcluded)
	if err != nil {
		return out, err
	}
	return out, out.Validate()
}
