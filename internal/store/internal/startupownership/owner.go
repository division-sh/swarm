package startupownership

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebundledelete "github.com/division-sh/swarm/internal/runtime/bundledelete"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	storeadmin "github.com/division-sh/swarm/internal/store/internal/adminpersistence"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

const runtimeSharedStoreOwnershipLock = "swarm:runtime:shared-store-owner"

type StartupPostgresOwner struct {
	backend          *postgresbackend.Backend
	schemaGuard      func() error
	agents           *storeagent.AgentPostgresOwner
	bundleDelete     *storeadmin.BundleDeletePostgresOwner
	destructiveReset *storeadmin.DestructiveResetPostgresOwner
}

type StartupSQLiteOwner struct {
	backend     *sqlitebackend.Backend
	schemaGuard func() error
	agents      *storeagent.AgentSQLiteOwner
	ownerMu     sync.Mutex
}

func NewPostgres(backend *postgresbackend.Backend, schemaGuard func() error, agents *storeagent.AgentPostgresOwner, bundleDelete *storeadmin.BundleDeletePostgresOwner, destructiveReset *storeadmin.DestructiveResetPostgresOwner) (*StartupPostgresOwner, error) {
	if backend == nil || !backend.Valid() || schemaGuard == nil || agents == nil || bundleDelete == nil || destructiveReset == nil {
		return nil, errors.New("startup/topology PostgreSQL owner requires backend, schema guard, and agent lifecycle owner")
	}
	return &StartupPostgresOwner{backend: backend, schemaGuard: schemaGuard, agents: agents, bundleDelete: bundleDelete, destructiveReset: destructiveReset}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, schemaGuard func() error, agents *storeagent.AgentSQLiteOwner) (*StartupSQLiteOwner, error) {
	if backend == nil || !backend.Valid() || schemaGuard == nil || agents == nil {
		return nil, errors.New("startup/topology SQLite owner requires backend, schema guard, and agent lifecycle owner")
	}
	return &StartupSQLiteOwner{backend: backend, schemaGuard: schemaGuard, agents: agents}, nil
}

func (s *StartupPostgresOwner) AcquireProcessCapability(ctx context.Context, req runtimestartupownership.AcquireRequest) (runtimestartupownership.ProcessCapability, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if err := s.schemaGuard(); err != nil {
		return nil, err
	}
	lease, acquired, err := postgresbackend.AcquireAdvisoryLockLease(ctx, s.backend, runtimeSharedStoreOwnershipLock)
	if err != nil {
		return nil, fmt.Errorf("acquire retained runtime store session: %w", err)
	}
	if !acquired {
		return nil, &runtimestartupownership.AcquisitionError{Failure: runtimestartupownership.AcquisitionTakeoverRequired, Detail: "selected store is held by another process"}
	}
	authority, err := runtimestartupownership.NewColdAuthority(req, "postgres_retained_session")
	if err != nil {
		return nil, errors.Join(err, lease.Release(ctx))
	}
	session := &postgresSession{owner: s, lease: lease, authority: authority}
	if err := lease.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := requireAcquirableAuthorityHead(txctx, tx, false); err != nil {
			return err
		}
		return recordAuthorityTransitionTx(txctx, tx, nil, authority, false)
	}); err != nil {
		return nil, errors.Join(err, lease.Release(ctx))
	}
	capability, err := runtimestartupownership.NewProcessCapability(session)
	if err != nil {
		return nil, errors.Join(err, lease.Release(ctx))
	}
	return capability, nil
}

func (s *StartupSQLiteOwner) AcquireProcessCapability(ctx context.Context, req runtimestartupownership.AcquireRequest) (runtimestartupownership.ProcessCapability, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if err := s.schemaGuard(); err != nil {
		return nil, err
	}
	if !s.ownerMu.TryLock() {
		return nil, &runtimestartupownership.AcquisitionError{Failure: runtimestartupownership.AcquisitionTakeoverRequired, Detail: "selected store is held by another process capability"}
	}
	authority, err := runtimestartupownership.NewColdAuthority(req, "sqlite_retained_owner")
	if err != nil {
		s.ownerMu.Unlock()
		return nil, err
	}
	session := &sqliteSession{owner: s, authority: authority}
	err = s.backend.RunTransaction(ctx, "acquire runtime process capability", func(txctx context.Context, tx *sql.Tx) error {
		if err := requireAcquirableAuthorityHead(txctx, tx, true); err != nil {
			return err
		}
		return recordAuthorityTransitionTx(txctx, tx, nil, authority, true)
	})
	if err != nil {
		s.ownerMu.Unlock()
		return nil, err
	}
	capability, err := runtimestartupownership.NewProcessCapability(session)
	if err != nil {
		s.ownerMu.Unlock()
		return nil, err
	}
	return capability, nil
}

type postgresSession struct {
	mu        sync.Mutex
	owner     *StartupPostgresOwner
	lease     *postgresbackend.AdvisoryLockLease
	authority runtimestartupownership.Authority
	released  bool
}

func (s *postgresSession) Authority() (runtimestartupownership.Authority, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released || s.authority.State != runtimestartupownership.StateActive {
		return runtimestartupownership.Authority{}, errors.New("PostgreSQL process capability is released")
	}
	return s.authority, nil
}

func (s *postgresSession) ProveCurrent(ctx context.Context) error { return s.lease.ProveCurrent(ctx) }

func (s *postgresSession) InstallTerminalOwner(owner runtimestartupownership.SessionTerminalOwner) error {
	if owner == nil || !s.lease.InstallTerminalOwner(nil, owner.SelectedStoreSessionTerminal, nil) {
		return errors.New("install PostgreSQL process capability terminal callback")
	}
	return nil
}

func (s *postgresSession) RecordGenerationGrantTransition(ctx context.Context, previous *runtimestartupownership.GrantEvidence, next runtimestartupownership.GrantEvidence) error {
	return s.lease.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return recordGrantTransitionTx(txctx, tx, previous, next, false)
	})
}

func (s *postgresSession) LoadSourceSet(ctx context.Context) (runtimeagenttopology.SourceSetPlan, bool, error) {
	var plan runtimeagenttopology.SourceSetPlan
	var exists bool
	err := s.lease.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		plan, exists, err = loadSourceSetTx(txctx, tx, false)
		return err
	})
	return plan, exists, err
}

func (s *postgresSession) CommitSourceSet(ctx context.Context, req runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error) {
	var result runtimeagenttopology.SourceSetCommitResult
	err := s.lease.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = commitSourceSetTx(txctx, tx, req, false)
		return err
	})
	return result, err
}

func (s *postgresSession) ApplyBundleDeleteFinalMutation(ctx context.Context, req runtimebundledelete.FinalMutationRequest, topology *runtimeagenttopology.SourceSetCommitRequest) (runtimebundledelete.FinalMutationResult, error) {
	var result runtimebundledelete.FinalMutationResult
	err := s.lease.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if topology == nil {
			stored, found, err := loadBundleDeleteFinalMutationTx(txctx, tx, req)
			if err != nil {
				return err
			}
			if found {
				result = stored
				return nil
			}
		}
		if topology != nil {
			if _, err := commitSourceSetTx(txctx, tx, *topology, false); err != nil {
				return err
			}
		}
		var err error
		result, err = storeadmin.ApplyBundleDeleteFinalMutationInRetainedTransaction(s.owner.bundleDelete, txctx, tx, req)
		if err == nil {
			result.SourceAuthorityOwner = "process_capability.ApplyBundleDeleteFinalMutation"
			if topology != nil {
				result.TransactionOrderProof = append([]string{"commit_agent_topology_source_set"}, result.TransactionOrderProof...)
			}
			if err = storeBundleDeleteFinalMutationTx(txctx, tx, req, result); err != nil {
				return err
			}
		}
		return err
	})
	return result, err
}

type sourceSetOperationResult struct {
	runtimeagenttopology.SourceSetCommitResult
}

type bundleDeleteFinalMutationReplayRecord struct {
	OperationID   string
	RequestHash   string
	ReplayKeyHash string
	RequestedAt   time.Time
	Result        runtimebundledelete.FinalMutationResult
}

func storeBundleDeleteFinalMutationTx(
	ctx context.Context,
	tx *sql.Tx,
	req runtimebundledelete.FinalMutationRequest,
	result runtimebundledelete.FinalMutationResult,
) error {
	if tx == nil {
		return errors.New("bundle delete replay record requires retained transaction")
	}
	requestHash := strings.TrimSpace(req.RequestHash)
	if requestHash == "" {
		return errors.New("bundle delete replay record requires request hash")
	}
	if req.RequestedAt.IsZero() {
		return errors.New("bundle delete replay record requires requested_at")
	}
	replayKeyHash := strings.TrimSpace(req.ReplayKeyHash)
	raw, err := canonicaljson.Bytes(result)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO bundle_delete_final_mutation_replays (
			operation_id, request_hash, replay_key_hash, requested_at, result, created_at
		) VALUES ($1::uuid,$2,$3,$4,$5::jsonb,$6)
	`, strings.TrimSpace(req.OperationID), requestHash, replayKeyHash, req.RequestedAt.UTC(), string(raw), time.Now().UTC())
	if err != nil {
		return fmt.Errorf("store bundle delete final mutation replay record: %w", err)
	}
	return nil
}

func loadBundleDeleteFinalMutationTx(
	ctx context.Context,
	tx *sql.Tx,
	req runtimebundledelete.FinalMutationRequest,
) (runtimebundledelete.FinalMutationResult, bool, error) {
	if tx == nil {
		return runtimebundledelete.FinalMutationResult{}, false, errors.New("bundle delete replay requires retained transaction")
	}
	var stored bundleDeleteFinalMutationReplayRecord
	var raw []byte
	err := tx.QueryRowContext(ctx, `
		SELECT operation_id::text,request_hash,replay_key_hash,requested_at,result
		FROM bundle_delete_final_mutation_replays
		WHERE operation_id = $1::uuid
	`, strings.TrimSpace(req.OperationID)).Scan(&stored.OperationID, &stored.RequestHash, &stored.ReplayKeyHash, &stored.RequestedAt, &raw)
	if err == nil {
		if err := json.Unmarshal(raw, &stored.Result); err != nil {
			return runtimebundledelete.FinalMutationResult{}, false, fmt.Errorf("decode bundle delete final mutation replay result: %w", err)
		}
		result, err := decodeBundleDeleteFinalMutation(req, stored, false)
		return result, err == nil, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return runtimebundledelete.FinalMutationResult{}, false, fmt.Errorf("load bundle delete final mutation replay record: %w", err)
	}
	replayKeyHash := strings.TrimSpace(req.ReplayKeyHash)
	if replayKeyHash == "" {
		return runtimebundledelete.FinalMutationResult{}, false, nil
	}
	if req.RequestedAt.IsZero() {
		return runtimebundledelete.FinalMutationResult{}, false, errors.New("bundle delete replay requires requested_at")
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT operation_id::text,request_hash,replay_key_hash,requested_at,result
		FROM bundle_delete_final_mutation_replays
		WHERE replay_key_hash = $1
		  AND requested_at > $2
		  AND requested_at <= $3
		ORDER BY requested_at DESC, created_at DESC
		LIMIT 2
	`, replayKeyHash, req.RequestedAt.Add(-runtimebundledelete.FinalMutationReplayWindow).UTC(), req.RequestedAt.UTC())
	if err != nil {
		return runtimebundledelete.FinalMutationResult{}, false, fmt.Errorf("find bundle delete final mutation replay authority: %w", err)
	}
	defer rows.Close()
	var candidates []bundleDeleteFinalMutationReplayRecord
	for rows.Next() {
		var candidate bundleDeleteFinalMutationReplayRecord
		var candidateRaw []byte
		if err := rows.Scan(&candidate.OperationID, &candidate.RequestHash, &candidate.ReplayKeyHash, &candidate.RequestedAt, &candidateRaw); err != nil {
			return runtimebundledelete.FinalMutationResult{}, false, fmt.Errorf("scan bundle delete final mutation replay authority: %w", err)
		}
		if err := json.Unmarshal(candidateRaw, &candidate.Result); err != nil {
			return runtimebundledelete.FinalMutationResult{}, false, fmt.Errorf("decode bundle delete final mutation replay result: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return runtimebundledelete.FinalMutationResult{}, false, fmt.Errorf("iterate bundle delete final mutation replay authority: %w", err)
	}
	if len(candidates) == 0 {
		return runtimebundledelete.FinalMutationResult{}, false, nil
	}
	if len(candidates) != 1 {
		return runtimebundledelete.FinalMutationResult{}, false, errors.New("bundle delete replay authority is ambiguous")
	}
	result, err := decodeBundleDeleteFinalMutation(req, candidates[0], true)
	return result, err == nil, err
}

func decodeBundleDeleteFinalMutation(req runtimebundledelete.FinalMutationRequest, stored bundleDeleteFinalMutationReplayRecord, requireReplayAuthority bool) (runtimebundledelete.FinalMutationResult, error) {
	if strings.TrimSpace(stored.RequestHash) == "" || strings.TrimSpace(stored.RequestHash) != strings.TrimSpace(req.RequestHash) {
		return runtimebundledelete.FinalMutationResult{}, errors.New("bundle delete replay conflicts with stored request hash")
	}
	if strings.TrimSpace(stored.ReplayKeyHash) != strings.TrimSpace(req.ReplayKeyHash) {
		return runtimebundledelete.FinalMutationResult{}, errors.New("bundle delete replay conflicts with stored replay authority")
	}
	if requireReplayAuthority {
		if stored.RequestedAt.IsZero() || req.RequestedAt.Before(stored.RequestedAt) ||
			!req.RequestedAt.Before(stored.RequestedAt.Add(runtimebundledelete.FinalMutationReplayWindow)) {
			return runtimebundledelete.FinalMutationResult{}, errors.New("bundle delete replay authority is outside its validity window")
		}
	}
	result := stored.Result
	if strings.TrimSpace(result.BundleHash) != strings.TrimSpace(req.BundleHash) ||
		strings.TrimSpace(result.OperationName) != strings.TrimSpace(req.OperationName) {
		return runtimebundledelete.FinalMutationResult{}, errors.New("bundle delete replay conflicts with stored final mutation")
	}
	return result, nil
}

func (s *postgresSession) ApplyDestructiveResetCleanup(ctx context.Context, req runtimedestructivereset.CleanupRequest, topology *runtimeagenttopology.SourceSetCommitRequest) (runtimedestructivereset.CleanupResult, error) {
	var result runtimedestructivereset.CleanupResult
	err := s.lease.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if topology != nil {
			if _, err := commitSourceSetTx(txctx, tx, *topology, false); err != nil {
				return err
			}
		}
		var err error
		result, err = storeadmin.ApplyDestructiveResetCleanupInRetainedTransaction(s.owner.destructiveReset, txctx, tx, req)
		return err
	})
	return result, err
}

func (s *postgresSession) CommitAgentLifecycleTransition(ctx context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	var result runtimemanager.AgentLifecycleTransitionResult
	err := s.lease.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = s.owner.agents.CommitAgentLifecycleTransitionTx(txctx, tx, req)
		return err
	})
	return result, err
}

func (s *postgresSession) Release(ctx context.Context) error {
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return nil
	}
	previous := s.authority
	next, err := runtimestartupownership.ReleasedAuthority(previous)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	if err := s.lease.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return recordAuthorityTransitionTx(txctx, tx, &previous, next, false)
	}); err != nil {
		return err
	}
	s.mu.Lock()
	s.authority = next
	s.released = true
	s.mu.Unlock()
	return s.lease.Release(ctx)
}

type sqliteSession struct {
	mu            sync.Mutex
	owner         *StartupSQLiteOwner
	authority     runtimestartupownership.Authority
	terminalOwner runtimestartupownership.SessionTerminalOwner
	released      bool
}

func (s *sqliteSession) Authority() (runtimestartupownership.Authority, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released || s.authority.State != runtimestartupownership.StateActive {
		return runtimestartupownership.Authority{}, errors.New("SQLite process capability is released")
	}
	return s.authority, nil
}

func (s *sqliteSession) ProveCurrent(ctx context.Context) error {
	if _, err := s.Authority(); err != nil {
		return err
	}
	var snapshot []byte
	err := s.owner.backend.QueryRowContext(ctx, `SELECT snapshot FROM runtime_startup_authority_facts WHERE authority_id = ? ORDER BY transition_ordinal DESC LIMIT 1`, s.authority.AuthorityID).Scan(&snapshot)
	if err != nil {
		s.terminal()
		return err
	}
	var persisted runtimestartupownership.Authority
	if err := json.Unmarshal(snapshot, &persisted); err != nil || persisted != s.authority {
		s.terminal()
		return errors.New("SQLite process capability durable head changed")
	}
	return nil
}

func (s *sqliteSession) InstallTerminalOwner(owner runtimestartupownership.SessionTerminalOwner) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminalOwner != nil || owner == nil {
		return errors.New("install SQLite process capability terminal callback")
	}
	s.terminalOwner = owner
	return nil
}

func (s *sqliteSession) RecordGenerationGrantTransition(ctx context.Context, previous *runtimestartupownership.GrantEvidence, next runtimestartupownership.GrantEvidence) error {
	return s.owner.backend.RunTransaction(ctx, "record runtime generation grant", func(txctx context.Context, tx *sql.Tx) error {
		return recordGrantTransitionTx(txctx, tx, previous, next, true)
	})
}

func (s *sqliteSession) LoadSourceSet(ctx context.Context) (runtimeagenttopology.SourceSetPlan, bool, error) {
	var plan runtimeagenttopology.SourceSetPlan
	var exists bool
	err := s.owner.backend.RunTransaction(ctx, "load agent topology source set", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		plan, exists, err = loadSourceSetTx(txctx, tx, true)
		return err
	})
	return plan, exists, err
}

func (s *sqliteSession) CommitSourceSet(ctx context.Context, req runtimeagenttopology.SourceSetCommitRequest) (runtimeagenttopology.SourceSetCommitResult, error) {
	var result runtimeagenttopology.SourceSetCommitResult
	err := s.owner.backend.RunTransaction(ctx, "commit agent topology source set", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = commitSourceSetTx(txctx, tx, req, true)
		return err
	})
	return result, err
}

func (s *sqliteSession) ApplyBundleDeleteFinalMutation(context.Context, runtimebundledelete.FinalMutationRequest, *runtimeagenttopology.SourceSetCommitRequest) (runtimebundledelete.FinalMutationResult, error) {
	return runtimebundledelete.FinalMutationResult{}, errors.New("bundle deletion is unsupported by the SQLite selected-store composition")
}

func (s *sqliteSession) ApplyDestructiveResetCleanup(context.Context, runtimedestructivereset.CleanupRequest, *runtimeagenttopology.SourceSetCommitRequest) (runtimedestructivereset.CleanupResult, error) {
	return runtimedestructivereset.CleanupResult{}, errors.New("destructive reset is unsupported by the SQLite selected-store composition")
}

func (s *sqliteSession) CommitAgentLifecycleTransition(ctx context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	var result runtimemanager.AgentLifecycleTransitionResult
	err := s.owner.backend.RunTransaction(ctx, "commit retained agent lifecycle transition", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = s.owner.agents.CommitAgentLifecycleTransitionTx(txctx, tx, req)
		return err
	})
	return result, err
}

func (s *sqliteSession) Release(ctx context.Context) error {
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return nil
	}
	previous := s.authority
	next, err := runtimestartupownership.ReleasedAuthority(previous)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	err = s.owner.backend.RunTransaction(ctx, "release runtime process capability", func(txctx context.Context, tx *sql.Tx) error {
		return recordAuthorityTransitionTx(txctx, tx, &previous, next, true)
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.authority = next
	s.released = true
	s.mu.Unlock()
	s.owner.ownerMu.Unlock()
	return nil
}

func (s *sqliteSession) terminal() {
	s.mu.Lock()
	owner := s.terminalOwner
	if !s.released {
		s.released = true
		s.owner.ownerMu.Unlock()
	}
	s.mu.Unlock()
	if owner != nil {
		owner.SelectedStoreSessionTerminal()
	}
}

func requireAcquirableAuthorityHead(ctx context.Context, tx *sql.Tx, sqlite bool) error {
	query := `SELECT snapshot FROM runtime_startup_authority_facts ORDER BY created_at DESC, transition_ordinal DESC LIMIT 1`
	if !sqlite {
		query += ` FOR UPDATE`
	}
	var raw []byte
	err := tx.QueryRowContext(ctx, query).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	var prior runtimestartupownership.Authority
	if err := json.Unmarshal(raw, &prior); err != nil || prior.Validate() != nil {
		return &runtimestartupownership.AcquisitionError{Failure: runtimestartupownership.AcquisitionPriorOwnerAmbiguous, Detail: "durable process authority head is invalid"}
	}
	if prior.State != runtimestartupownership.StateReleased {
		return &runtimestartupownership.AcquisitionError{Failure: runtimestartupownership.AcquisitionTakeoverRequired, Detail: "prior process authority is nonterminal"}
	}
	return nil
}

func recordAuthorityTransitionTx(ctx context.Context, tx *sql.Tx, previous *runtimestartupownership.Authority, next runtimestartupownership.Authority, sqlite bool) error {
	if err := runtimestartupownership.ValidateTransition(previous, next); err != nil {
		return err
	}
	if previous != nil {
		query := `SELECT snapshot FROM runtime_startup_authority_facts WHERE authority_id = ? ORDER BY transition_ordinal DESC LIMIT 1`
		args := []any{previous.AuthorityID}
		if !sqlite {
			query = `SELECT snapshot FROM runtime_startup_authority_facts WHERE authority_id = $1::uuid ORDER BY transition_ordinal DESC LIMIT 1 FOR UPDATE`
		}
		var raw []byte
		if err := tx.QueryRowContext(ctx, query, args...).Scan(&raw); err != nil {
			return err
		}
		var head runtimestartupownership.Authority
		if err := json.Unmarshal(raw, &head); err != nil || head != *previous {
			return errors.New("process authority predecessor changed")
		}
	}
	raw, err := canonicaljson.Bytes(next)
	if err != nil {
		return err
	}
	if sqlite {
		_, err = tx.ExecContext(ctx, `INSERT INTO runtime_startup_authority_facts (fact_id,authority_id,transition_ordinal,state_version,state,owner_id,boot_id,runtime_instance_id,backend,snapshot,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)`, uuid.NewString(), next.AuthorityID, next.TransitionOrdinal, next.StateVersion, string(next.State), next.OwnerID, next.BootID, next.RuntimeInstanceID, next.Backend, string(raw), next.RecordedAt.UTC())
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO runtime_startup_authority_facts (fact_id,authority_id,transition_ordinal,state_version,state,owner_id,boot_id,runtime_instance_id,backend,snapshot,created_at) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7::uuid,$8::uuid,$9,$10::jsonb,$11)`, uuid.NewString(), next.AuthorityID, next.TransitionOrdinal, next.StateVersion, string(next.State), next.OwnerID, next.BootID, next.RuntimeInstanceID, next.Backend, string(raw), next.RecordedAt.UTC())
	}
	return err
}

func recordGrantTransitionTx(ctx context.Context, tx *sql.Tx, previous *runtimestartupownership.GrantEvidence, next runtimestartupownership.GrantEvidence, sqlite bool) error {
	if err := next.Validate(); err != nil {
		return err
	}
	loadCurrent := `SELECT snapshot FROM runtime_generation_grants WHERE grant_id = ? ORDER BY state_version DESC LIMIT 1`
	if !sqlite {
		loadCurrent = `SELECT snapshot FROM runtime_generation_grants WHERE grant_id = $1::uuid ORDER BY state_version DESC LIMIT 1 FOR UPDATE`
	}
	var currentRaw []byte
	currentErr := tx.QueryRowContext(ctx, loadCurrent, next.GrantID).Scan(&currentRaw)
	if previous == nil {
		if next.StateVersion != 1 || next.State != runtimestartupownership.GrantPrepared {
			return errors.New("initial runtime generation grant transition is invalid")
		}
		if currentErr == nil {
			return errors.New("initial runtime generation grant already exists")
		}
		if currentErr != sql.ErrNoRows {
			return currentErr
		}
	} else if previous.GrantID != next.GrantID || next.StateVersion != previous.StateVersion+1 {
		return errors.New("runtime generation grant predecessor changed")
	} else {
		if currentErr == sql.ErrNoRows {
			return errors.New("runtime generation grant predecessor is missing")
		}
		if currentErr != nil {
			return currentErr
		}
		var current runtimestartupownership.GrantEvidence
		if err := json.Unmarshal(currentRaw, &current); err != nil {
			return fmt.Errorf("decode current runtime generation grant: %w", err)
		}
		currentCanonical, err := canonicaljson.Bytes(current)
		if err != nil {
			return err
		}
		previousCanonical, err := canonicaljson.Bytes(*previous)
		if err != nil {
			return err
		}
		if string(currentCanonical) != string(previousCanonical) {
			return errors.New("runtime generation grant predecessor is not current")
		}
	}
	raw, err := canonicaljson.Bytes(next)
	if err != nil {
		return err
	}
	if sqlite {
		_, err = tx.ExecContext(ctx, `INSERT INTO runtime_generation_grants (fact_id,grant_id,process_authority_id,process_owner_id,state_version,state,bundle_hash,bundle_source,runtime_instance_id,runtime_generation,source_set_revision,snapshot,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`, uuid.NewString(), next.GrantID, next.ProcessAuthorityID, next.ProcessOwnerID, next.StateVersion, string(next.State), next.BundleHash, next.BundleSource, next.RuntimeInstanceID, next.RuntimeGeneration, next.SourceSetRevision, string(raw), time.Now().UTC())
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO runtime_generation_grants (fact_id,grant_id,process_authority_id,process_owner_id,state_version,state,bundle_hash,bundle_source,runtime_instance_id,runtime_generation,source_set_revision,snapshot,created_at) VALUES ($1::uuid,$2::uuid,$3::uuid,$4,$5,$6,$7,$8,$9::uuid,$10,$11,$12::jsonb,$13)`, uuid.NewString(), next.GrantID, next.ProcessAuthorityID, next.ProcessOwnerID, next.StateVersion, string(next.State), next.BundleHash, next.BundleSource, next.RuntimeInstanceID, next.RuntimeGeneration, next.SourceSetRevision, string(raw), time.Now().UTC())
	}
	return err
}

func commitSourceSetTx(ctx context.Context, tx *sql.Tx, req runtimeagenttopology.SourceSetCommitRequest, sqlite bool) (runtimeagenttopology.SourceSetCommitResult, error) {
	if err := req.Validate(); err != nil {
		return runtimeagenttopology.SourceSetCommitResult{}, err
	}
	requestHash, err := canonicaljson.Hash(req)
	if err != nil {
		return runtimeagenttopology.SourceSetCommitResult{}, err
	}
	loadOperation := `SELECT request_hash,result FROM agent_topology_source_set_operations WHERE operation_id = ?`
	if !sqlite {
		loadOperation = `SELECT request_hash,result FROM agent_topology_source_set_operations WHERE operation_id = $1::uuid`
	}
	var storedHash string
	var storedResult []byte
	if err := tx.QueryRowContext(ctx, loadOperation, req.OperationID).Scan(&storedHash, &storedResult); err == nil {
		if storedHash != requestHash {
			return runtimeagenttopology.SourceSetCommitResult{}, errors.New("agent topology source-set operation conflicts with stored request")
		}
		var result runtimeagenttopology.SourceSetCommitResult
		if err := json.Unmarshal(storedResult, &result); err != nil {
			return result, err
		}
		result.Replayed = true
		return result, nil
	} else if err != sql.ErrNoRows {
		return runtimeagenttopology.SourceSetCommitResult{}, err
	}
	loadHead := `SELECT revision,plan FROM agent_topology_source_set_head WHERE singleton_id = 1`
	if !sqlite {
		loadHead += ` FOR UPDATE`
	}
	var previous runtimeagenttopology.SourceSetPlan
	var previousRaw []byte
	err = tx.QueryRowContext(ctx, loadHead).Scan(&previous.Revision, &previousRaw)
	hasPrevious := err == nil
	if err != nil && err != sql.ErrNoRows {
		return runtimeagenttopology.SourceSetCommitResult{}, err
	}
	if hasPrevious {
		if err := json.Unmarshal(previousRaw, &previous); err != nil {
			return runtimeagenttopology.SourceSetCommitResult{}, err
		}
	} else {
		previous, err = runtimeagenttopology.EmptySourceSetPlan()
		if err != nil {
			return runtimeagenttopology.SourceSetCommitResult{}, err
		}
	}
	if req.Operation == runtimeagenttopology.OperationInstallCompleteSourceSet {
		if hasPrevious {
			return runtimeagenttopology.SourceSetCommitResult{}, errors.New("complete source set is already installed")
		}
	} else if !hasPrevious || req.ExpectedRevision != previous.Revision {
		return runtimeagenttopology.SourceSetCommitResult{}, errors.New("agent topology source-set predecessor changed")
	}
	if req.Operation == runtimeagenttopology.OperationRemoveBundleSource {
		removed := req.RemovedSource.Normalize()
		if sourceSetContains(req.Plan, removed) || !sourceSetContains(previous, removed) {
			return runtimeagenttopology.SourceSetCommitResult{}, errors.New("bundle-source removal does not exactly remove the declared source")
		}
	}
	changes, err := runtimeagenttopology.Diff(previous, req.Plan)
	if err != nil {
		return runtimeagenttopology.SourceSetCommitResult{}, err
	}
	result := runtimeagenttopology.SourceSetCommitResult{Operation: req.Operation, OperationID: req.OperationID, CurrentRevision: req.Plan.Revision, Changes: changes}
	if hasPrevious {
		result.PreviousRevision = previous.Revision
	}
	planRaw, _ := canonicaljson.Bytes(req.Plan)
	resultRaw, _ := canonicaljson.Bytes(result)
	now := time.Now().UTC()
	if sqlite {
		_, err = tx.ExecContext(ctx, `INSERT INTO agent_topology_source_set_head (singleton_id,revision,plan,operation_id,updated_at) VALUES (1,?,?,?,?) ON CONFLICT(singleton_id) DO UPDATE SET revision=excluded.revision,plan=excluded.plan,operation_id=excluded.operation_id,updated_at=excluded.updated_at`, req.Plan.Revision, string(planRaw), req.OperationID, now)
		if err == nil {
			_, err = tx.ExecContext(ctx, `INSERT INTO agent_topology_source_set_operations (operation_id,operation_kind,request_hash,previous_revision,current_revision,result,created_at) VALUES (?,?,?,?,?,?,?)`, req.OperationID, string(req.Operation), requestHash, nullText(result.PreviousRevision), result.CurrentRevision, string(resultRaw), now)
		}
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO agent_topology_source_set_head (singleton_id,revision,plan,operation_id,updated_at) VALUES (1,$1,$2::jsonb,$3::uuid,$4) ON CONFLICT(singleton_id) DO UPDATE SET revision=EXCLUDED.revision,plan=EXCLUDED.plan,operation_id=EXCLUDED.operation_id,updated_at=EXCLUDED.updated_at`, req.Plan.Revision, string(planRaw), req.OperationID, now)
		if err == nil {
			_, err = tx.ExecContext(ctx, `INSERT INTO agent_topology_source_set_operations (operation_id,operation_kind,request_hash,previous_revision,current_revision,result,created_at) VALUES ($1::uuid,$2,$3,NULLIF($4,''),$5,$6::jsonb,$7)`, req.OperationID, string(req.Operation), requestHash, result.PreviousRevision, result.CurrentRevision, string(resultRaw), now)
		}
	}
	return result, err
}

func loadSourceSetTx(ctx context.Context, tx *sql.Tx, sqlite bool) (runtimeagenttopology.SourceSetPlan, bool, error) {
	query := `SELECT revision,plan FROM agent_topology_source_set_head WHERE singleton_id = 1`
	if !sqlite {
		query += ` FOR SHARE`
	}
	var plan runtimeagenttopology.SourceSetPlan
	var raw []byte
	if err := tx.QueryRowContext(ctx, query).Scan(&plan.Revision, &raw); err != nil {
		if err == sql.ErrNoRows {
			return runtimeagenttopology.SourceSetPlan{}, false, nil
		}
		return runtimeagenttopology.SourceSetPlan{}, false, err
	}
	if err := json.Unmarshal(raw, &plan); err != nil {
		return runtimeagenttopology.SourceSetPlan{}, false, err
	}
	if err := plan.Validate(); err != nil {
		return runtimeagenttopology.SourceSetPlan{}, false, err
	}
	return plan, true, nil
}

func sourceSetContains(plan runtimeagenttopology.SourceSetPlan, source runtimeagenttopology.SourceCoordinate) bool {
	for _, candidate := range plan.Sources {
		if candidate.Normalize() == source {
			return true
		}
	}
	return false
}

func nullText(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}
