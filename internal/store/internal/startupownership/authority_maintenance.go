package startupownership

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/google/uuid"
)

type authorityHeadRecord struct {
	AuthorityID            string          `json:"authority_id"`
	AuthorityGeneration    uint64          `json:"authority_generation"`
	TransitionOrdinal      uint64          `json:"transition_ordinal"`
	StateVersion           uint64          `json:"state_version"`
	State                  string          `json:"state"`
	OwnerID                string          `json:"owner_id"`
	BootID                 string          `json:"boot_id"`
	RuntimeInstanceID      string          `json:"runtime_instance_id"`
	Backend                string          `json:"backend"`
	AcquisitionID          string          `json:"acquisition_id"`
	AcquisitionRequestHash string          `json:"acquisition_request_hash"`
	AcquisitionKind        string          `json:"acquisition_kind"`
	PredecessorAuthorityID string          `json:"predecessor_authority_id,omitempty"`
	SuccessorAuthorityID   string          `json:"successor_authority_id,omitempty"`
	Snapshot               json.RawMessage `json:"snapshot"`
	CreatedAt              string          `json:"created_at"`
	createdAtValid         bool
}

func (s *StartupPostgresOwner) InspectAuthority(ctx context.Context) (runtimestartupownership.AuthorityInspection, error) {
	return inspectAuthorityHead(ctx, s.backend, "postgres_retained_session", false)
}

func (s *StartupSQLiteOwner) InspectAuthority(ctx context.Context) (runtimestartupownership.AuthorityInspection, error) {
	return inspectAuthorityHead(ctx, s.backend, "sqlite_retained_owner", true)
}

func (s *StartupPostgresOwner) RepairAuthority(ctx context.Context, req runtimestartupownership.AuthorityRepairRequest) (runtimestartupownership.AuthorityRepairResult, error) {
	if err := req.Validate(); err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, err
	}
	if err := s.schemaGuard(); err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, err
	}
	lease, acquired, err := postgresbackend.AcquireAdvisoryLockLease(ctx, s.backend, runtimeSharedStoreOwnershipLock)
	if err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, fmt.Errorf("acquire selected-store possession for authority repair: %w", err)
	}
	if !acquired {
		return runtimestartupownership.AuthorityRepairResult{}, &runtimestartupownership.AcquisitionError{
			Failure: runtimestartupownership.AcquisitionTakeoverRequired,
			Detail:  "another serve still owns this project; stop it before repairing",
		}
	}
	defer lease.Release(context.WithoutCancel(ctx))
	var result runtimestartupownership.AuthorityRepairResult
	err = lease.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var repairErr error
		result, repairErr = repairAuthorityTx(txctx, tx, req, "postgres_retained_session", false)
		return repairErr
	})
	return result, err
}

func (s *StartupSQLiteOwner) RepairAuthority(ctx context.Context, req runtimestartupownership.AuthorityRepairRequest) (runtimestartupownership.AuthorityRepairResult, error) {
	if err := req.Validate(); err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, err
	}
	if err := s.schemaGuard(); err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, err
	}
	possession, err := s.acquirePossession()
	if err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, err
	}
	defer possession.Release()
	var result runtimestartupownership.AuthorityRepairResult
	err = s.backend.RunTransaction(ctx, "repair runtime process authority", func(txctx context.Context, tx *sql.Tx) error {
		var repairErr error
		result, repairErr = repairAuthorityTx(txctx, tx, req, "sqlite_retained_owner", true)
		return repairErr
	})
	return result, err
}

type authorityQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func inspectAuthorityHead(ctx context.Context, queryer authorityQueryer, backend string, sqlite bool) (runtimestartupownership.AuthorityInspection, error) {
	record, exists, err := loadAuthorityHeadRecord(ctx, queryer, sqlite, false)
	if err != nil {
		return runtimestartupownership.AuthorityInspection{}, err
	}
	return classifyAuthorityHead(ctx, queryer, record, exists, backend, sqlite)
}

func loadAuthorityHeadRecord(ctx context.Context, queryer authorityQueryer, sqlite, lock bool) (authorityHeadRecord, bool, error) {
	query := `SELECT authority_id,authority_generation,transition_ordinal,state_version,state,owner_id,boot_id,runtime_instance_id,backend,acquisition_id,acquisition_request_hash,acquisition_kind,predecessor_authority_id,successor_authority_id,snapshot,created_at FROM runtime_startup_authority_facts ORDER BY authority_generation DESC,transition_ordinal DESC LIMIT 1`
	if !sqlite {
		query = `SELECT authority_id::text,authority_generation,transition_ordinal,state_version,state,owner_id,boot_id::text,runtime_instance_id::text,backend,acquisition_id::text,acquisition_request_hash,acquisition_kind,predecessor_authority_id::text,successor_authority_id::text,snapshot,created_at FROM runtime_startup_authority_facts ORDER BY authority_generation DESC,transition_ordinal DESC LIMIT 1`
		if lock {
			query += ` FOR UPDATE`
		}
	}
	record, exists, err := scanAuthorityRecord(queryer.QueryRowContext(ctx, query))
	if err != nil {
		return authorityHeadRecord{}, false, fmt.Errorf("inspect process authority head: %w", err)
	}
	return record, exists, nil
}

func loadAuthorityRecord(ctx context.Context, queryer authorityQueryer, authorityID string, ordinal *uint64, sqlite bool) (authorityHeadRecord, bool, error) {
	query := `SELECT authority_id,authority_generation,transition_ordinal,state_version,state,owner_id,boot_id,runtime_instance_id,backend,acquisition_id,acquisition_request_hash,acquisition_kind,predecessor_authority_id,successor_authority_id,snapshot,created_at FROM runtime_startup_authority_facts WHERE authority_id = ?`
	args := []any{authorityID}
	if ordinal == nil {
		query += ` ORDER BY transition_ordinal DESC LIMIT 1`
	} else {
		query += ` AND transition_ordinal = ?`
		args = append(args, *ordinal)
	}
	if !sqlite {
		args = []any{authorityID}
		query = `SELECT authority_id::text,authority_generation,transition_ordinal,state_version,state,owner_id,boot_id::text,runtime_instance_id::text,backend,acquisition_id::text,acquisition_request_hash,acquisition_kind,predecessor_authority_id::text,successor_authority_id::text,snapshot,created_at FROM runtime_startup_authority_facts WHERE authority_id = $1::uuid`
		if ordinal == nil {
			query += ` ORDER BY transition_ordinal DESC LIMIT 1`
		} else {
			query += ` AND transition_ordinal = $2`
			args = append(args, *ordinal)
		}
	}
	record, exists, err := scanAuthorityRecord(queryer.QueryRowContext(ctx, query, args...))
	if err != nil {
		return authorityHeadRecord{}, false, fmt.Errorf("load process authority lineage: %w", err)
	}
	return record, exists, nil
}

func scanAuthorityRecord(row *sql.Row) (authorityHeadRecord, bool, error) {
	var record authorityHeadRecord
	var predecessor, successor sql.NullString
	var raw []byte
	var createdAt any
	err := row.Scan(
		&record.AuthorityID, &record.AuthorityGeneration, &record.TransitionOrdinal,
		&record.StateVersion, &record.State, &record.OwnerID, &record.BootID,
		&record.RuntimeInstanceID, &record.Backend, &record.AcquisitionID,
		&record.AcquisitionRequestHash, &record.AcquisitionKind, &predecessor,
		&successor, &raw, &createdAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return authorityHeadRecord{}, false, nil
	}
	if err != nil {
		return authorityHeadRecord{}, false, err
	}
	record.PredecessorAuthorityID = predecessor.String
	record.SuccessorAuthorityID = successor.String
	record.Snapshot = append(json.RawMessage(nil), raw...)
	record.CreatedAt, record.createdAtValid = authorityRecordTimestamp(createdAt)
	return record, true, nil
}

func authorityRecordTimestamp(value any) (string, bool) {
	switch typed := value.(type) {
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano), true
	case string:
		for _, layout := range []string{
			time.RFC3339Nano,
			"2006-01-02 15:04:05.999999999 -0700 MST",
			"2006-01-02 15:04:05.999999 -0700 MST",
			"2006-01-02 15:04:05 -0700 MST",
			"2006-01-02 15:04:05.999999999Z07:00",
			"2006-01-02 15:04:05.999999999-07:00",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05",
		} {
			if parsed, err := time.Parse(layout, typed); err == nil {
				return parsed.UTC().Format(time.RFC3339Nano), true
			}
		}
		return typed, false
	case []byte:
		return authorityRecordTimestamp(string(typed))
	}
	return fmt.Sprint(value), false
}

func classifyAuthorityHead(ctx context.Context, queryer authorityQueryer, record authorityHeadRecord, exists bool, backend string, sqlite bool) (runtimestartupownership.AuthorityInspection, error) {
	if !exists {
		digest, err := canonicaljson.Hash(struct {
			Backend string `json:"backend"`
			Empty   bool   `json:"empty"`
		}{Backend: backend, Empty: true})
		if err != nil {
			return runtimestartupownership.AuthorityInspection{}, err
		}
		result := runtimestartupownership.AuthorityInspection{
			Status: runtimestartupownership.AuthorityInspectionEmpty, Backend: backend,
			FindingsDigest: digest, Detail: "No previous project session is recorded.",
		}
		return result, result.Validate()
	}
	digest, err := canonicaljson.Hash(record)
	if err != nil {
		return runtimestartupownership.AuthorityInspection{}, err
	}
	result := runtimestartupownership.AuthorityInspection{
		Status: runtimestartupownership.AuthorityInspectionCorrupt, Backend: backend,
		FindingsDigest: digest, Detail: "The recorded project session is inconsistent and must be repaired before serving.",
	}
	authority, lineageErr := validateAuthorityLineage(ctx, queryer, record, backend, sqlite, make(map[string]struct{}))
	if lineageErr == nil {
		result.Status = runtimestartupownership.AuthorityInspectionValid
		result.State = authority.State
		result.OwnerID = authority.OwnerID
		result.AuthorityID = authority.AuthorityID
		result.Generation = authority.AuthorityGeneration
		result.RecordedAt = authority.RecordedAt
		result.Detail = "The recorded project session is internally consistent; its current liveness is not inferred by inspection."
	}
	return result, result.Validate()
}

func validateAuthorityLineage(
	ctx context.Context,
	queryer authorityQueryer,
	record authorityHeadRecord,
	backend string,
	sqlite bool,
	visited map[string]struct{},
) (runtimestartupownership.Authority, error) {
	var authority runtimestartupownership.Authority
	if !record.createdAtValid || json.Unmarshal(record.Snapshot, &authority) != nil || authority.Validate() != nil ||
		authority.Backend != backend || !authorityMatchesRecord(authority, record) {
		return runtimestartupownership.Authority{}, errors.New("process authority record is invalid")
	}
	key := authority.AuthorityID + ":" + fmt.Sprint(authority.TransitionOrdinal)
	if _, duplicate := visited[key]; duplicate {
		return runtimestartupownership.Authority{}, errors.New("process authority lineage contains a cycle")
	}
	visited[key] = struct{}{}
	defer delete(visited, key)

	if authority.TransitionOrdinal > 1 {
		previousOrdinal := authority.TransitionOrdinal - 1
		previousRecord, exists, err := loadAuthorityRecord(ctx, queryer, authority.AuthorityID, &previousOrdinal, sqlite)
		if err != nil {
			return runtimestartupownership.Authority{}, fmt.Errorf("load process authority terminal predecessor: %w", err)
		}
		if !exists {
			return runtimestartupownership.Authority{}, errors.New("process authority terminal transition has no exact predecessor")
		}
		previous, err := validateAuthorityLineage(ctx, queryer, previousRecord, backend, sqlite, visited)
		if err != nil || runtimestartupownership.ValidateTransition(&previous, authority) != nil {
			return runtimestartupownership.Authority{}, errors.New("process authority terminal transition lineage is invalid")
		}
		return authority, nil
	}

	switch authority.AcquisitionKind {
	case runtimestartupownership.AcquisitionCold:
		if authority.AuthorityGeneration != 1 || authority.PredecessorAuthorityID != "" {
			return runtimestartupownership.Authority{}, errors.New("cold process authority lineage is invalid")
		}
		return authority, nil
	case runtimestartupownership.AcquisitionRepair:
		if err := validateAuthorityRepairRoot(ctx, queryer, authority, backend, sqlite); err != nil {
			return runtimestartupownership.Authority{}, err
		}
		return authority, nil
	case runtimestartupownership.AcquisitionCleanHandoff, runtimestartupownership.AcquisitionCrashTakeover:
	default:
		return runtimestartupownership.Authority{}, errors.New("process authority acquisition kind is invalid")
	}

	predecessorRecord, exists, err := loadAuthorityRecord(ctx, queryer, authority.PredecessorAuthorityID, nil, sqlite)
	if err != nil {
		return runtimestartupownership.Authority{}, fmt.Errorf("load process authority acquisition predecessor: %w", err)
	}
	if !exists {
		return runtimestartupownership.Authority{}, errors.New("process authority acquisition predecessor is missing")
	}
	predecessor, err := validateAuthorityLineage(ctx, queryer, predecessorRecord, backend, sqlite, visited)
	if err != nil || predecessor.AuthorityGeneration == ^uint64(0) || predecessor.AuthorityGeneration+1 != authority.AuthorityGeneration {
		return runtimestartupownership.Authority{}, errors.New("process authority acquisition generation is not contiguous")
	}
	if authority.AcquisitionKind == runtimestartupownership.AcquisitionCleanHandoff {
		if predecessor.State != runtimestartupownership.StateReleased || predecessor.SuccessorAuthorityID != "" {
			return runtimestartupownership.Authority{}, errors.New("clean process authority handoff has invalid predecessor evidence")
		}
		return authority, nil
	}
	if predecessor.State != runtimestartupownership.StateSuperseded || predecessor.SuccessorAuthorityID != authority.AuthorityID {
		return runtimestartupownership.Authority{}, errors.New("crash process authority takeover has invalid predecessor evidence")
	}
	return authority, nil
}

func validateAuthorityRepairRoot(ctx context.Context, queryer authorityQueryer, authority runtimestartupownership.Authority, backend string, sqlite bool) error {
	query := `SELECT operation_id,findings_digest,backend,repaired_authority_id,authority_generation,result FROM runtime_startup_authority_repairs WHERE repaired_authority_id = ? AND authority_generation = ?`
	if !sqlite {
		query = `SELECT operation_id::text,findings_digest,backend,repaired_authority_id::text,authority_generation,result FROM runtime_startup_authority_repairs WHERE repaired_authority_id = $1::uuid AND authority_generation = $2`
	}
	var operationID, findingsDigest, storedBackend, repairedAuthorityID string
	var generation uint64
	var raw []byte
	if err := queryer.QueryRowContext(ctx, query, authority.AuthorityID, authority.AuthorityGeneration).Scan(
		&operationID, &findingsDigest, &storedBackend, &repairedAuthorityID, &generation, &raw,
	); err != nil {
		return errors.New("process authority repair lineage is not journaled")
	}
	var result runtimestartupownership.AuthorityRepairResult
	if json.Unmarshal(raw, &result) != nil || result.Validate() != nil || storedBackend != backend ||
		result.OperationID != operationID || result.FindingsDigest != findingsDigest ||
		result.RepairedAuthorityID != repairedAuthorityID || result.AuthorityGeneration != generation {
		return errors.New("process authority repair journal is invalid")
	}
	runtimeID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm-authority-repair-runtime-v1\x00"+operationID)).String()
	predecessorID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm-authority-repair-predecessor-v1\x00"+findingsDigest)).String()
	expected, err := runtimestartupownership.NewAuthority(runtimestartupownership.AcquireRequest{
		OwnerID: "operator-authority-repair", BootID: operationID, RuntimeInstanceID: runtimeID,
	}, backend, generation, predecessorID, runtimestartupownership.AcquisitionRepair)
	if err != nil || expected.AuthorityID != authority.AuthorityID || expected.AcquisitionID != authority.AcquisitionID ||
		expected.AcquisitionRequestHash != authority.AcquisitionRequestHash || expected.PredecessorAuthorityID != authority.PredecessorAuthorityID {
		return errors.New("process authority repair binding is invalid")
	}
	return nil
}

func authorityMatchesRecord(authority runtimestartupownership.Authority, record authorityHeadRecord) bool {
	return authority.AuthorityID == record.AuthorityID &&
		authority.AuthorityGeneration == record.AuthorityGeneration &&
		authority.TransitionOrdinal == record.TransitionOrdinal &&
		authority.StateVersion == record.StateVersion && string(authority.State) == record.State &&
		authority.OwnerID == record.OwnerID && authority.BootID == record.BootID &&
		authority.RuntimeInstanceID == record.RuntimeInstanceID && authority.Backend == record.Backend &&
		authority.AcquisitionID == record.AcquisitionID && authority.AcquisitionRequestHash == record.AcquisitionRequestHash &&
		string(authority.AcquisitionKind) == record.AcquisitionKind &&
		authority.PredecessorAuthorityID == record.PredecessorAuthorityID &&
		authority.SuccessorAuthorityID == record.SuccessorAuthorityID
}

func repairAuthorityTx(ctx context.Context, tx *sql.Tx, req runtimestartupownership.AuthorityRepairRequest, backend string, sqlite bool) (runtimestartupownership.AuthorityRepairResult, error) {
	requestHash, err := canonicaljson.Hash(req)
	if err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, err
	}
	if stored, found, err := loadAuthorityRepairResult(ctx, tx, req.OperationID, sqlite); err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, err
	} else if found {
		if stored.requestHash != requestHash || stored.result.FindingsDigest != strings.TrimSpace(req.FindingsDigest) {
			return runtimestartupownership.AuthorityRepairResult{}, errors.New("authority repair operation conflicts with stored request")
		}
		return stored.result, nil
	}
	record, exists, err := loadAuthorityHeadRecord(ctx, tx, sqlite, true)
	if err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, err
	}
	inspection, err := classifyAuthorityHead(ctx, tx, record, exists, backend, sqlite)
	if err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, err
	}
	if inspection.FindingsDigest != strings.TrimSpace(req.FindingsDigest) {
		return runtimestartupownership.AuthorityRepairResult{}, errors.New("authority repair findings changed; inspect again before confirming")
	}
	if inspection.Status != runtimestartupownership.AuthorityInspectionCorrupt {
		return runtimestartupownership.AuthorityRepairResult{}, errors.New("authority repair requires corrupt durable evidence")
	}
	if record.AuthorityGeneration == ^uint64(0) {
		return runtimestartupownership.AuthorityRepairResult{}, errors.New("authority repair generation is exhausted")
	}
	repairGeneration := record.AuthorityGeneration + 1
	if repairGeneration < 2 {
		repairGeneration = 2
	}
	runtimeID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm-authority-repair-runtime-v1\x00"+req.OperationID)).String()
	predecessorID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm-authority-repair-predecessor-v1\x00"+inspection.FindingsDigest)).String()
	repair, err := runtimestartupownership.NewAuthority(runtimestartupownership.AcquireRequest{
		OwnerID: "operator-authority-repair", BootID: req.OperationID, RuntimeInstanceID: runtimeID,
	}, backend, repairGeneration, predecessorID, runtimestartupownership.AcquisitionRepair)
	if err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, err
	}
	repair.RecordedAt = time.Now().UTC()
	if err := recordAuthorityTransitionTx(ctx, tx, nil, repair, sqlite); err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, err
	}
	released, err := runtimestartupownership.ReleasedAuthority(repair)
	if err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, err
	}
	if err := recordAuthorityTransitionTx(ctx, tx, &repair, released, sqlite); err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, err
	}
	result := runtimestartupownership.AuthorityRepairResult{
		OperationID: req.OperationID, FindingsDigest: inspection.FindingsDigest,
		RepairedAuthorityID: released.AuthorityID, AuthorityGeneration: released.AuthorityGeneration,
		CompletedAt: released.RecordedAt, UserDataUntouched: true,
	}
	if err := result.Validate(); err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, err
	}
	before, err := canonicaljson.Bytes(record)
	if err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, err
	}
	resultRaw, err := canonicaljson.Bytes(result)
	if err != nil {
		return runtimestartupownership.AuthorityRepairResult{}, err
	}
	if sqlite {
		_, err = tx.ExecContext(ctx, `INSERT INTO runtime_startup_authority_repairs (operation_id,request_hash,findings_digest,backend,before_snapshot,repaired_authority_id,authority_generation,result,created_at) VALUES (?,?,?,?,?,?,?,?,?)`, req.OperationID, requestHash, inspection.FindingsDigest, backend, string(before), released.AuthorityID, released.AuthorityGeneration, string(resultRaw), released.RecordedAt)
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO runtime_startup_authority_repairs (operation_id,request_hash,findings_digest,backend,before_snapshot,repaired_authority_id,authority_generation,result,created_at) VALUES ($1::uuid,$2,$3,$4,$5::jsonb,$6::uuid,$7,$8::jsonb,$9)`, req.OperationID, requestHash, inspection.FindingsDigest, backend, string(before), released.AuthorityID, released.AuthorityGeneration, string(resultRaw), released.RecordedAt)
	}
	return result, err
}

type storedAuthorityRepair struct {
	requestHash string
	result      runtimestartupownership.AuthorityRepairResult
}

func loadAuthorityRepairResult(ctx context.Context, tx *sql.Tx, operationID string, sqlite bool) (storedAuthorityRepair, bool, error) {
	query := `SELECT request_hash,result FROM runtime_startup_authority_repairs WHERE operation_id = ?`
	if !sqlite {
		query = `SELECT request_hash,result FROM runtime_startup_authority_repairs WHERE operation_id = $1::uuid`
	}
	var stored storedAuthorityRepair
	var raw []byte
	err := tx.QueryRowContext(ctx, query, operationID).Scan(&stored.requestHash, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return storedAuthorityRepair{}, false, nil
	}
	if err != nil {
		return storedAuthorityRepair{}, false, err
	}
	if err := json.Unmarshal(raw, &stored.result); err != nil {
		return storedAuthorityRepair{}, false, fmt.Errorf("decode authority repair result: %w", err)
	}
	if err := stored.result.Validate(); err != nil {
		return storedAuthorityRepair{}, false, err
	}
	return stored, true, nil
}
