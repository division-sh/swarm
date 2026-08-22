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
	CreatedAt              time.Time       `json:"created_at"`
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
	return classifyAuthorityHead(record, exists, backend)
}

func loadAuthorityHeadRecord(ctx context.Context, queryer authorityQueryer, sqlite, lock bool) (authorityHeadRecord, bool, error) {
	query := `SELECT authority_id,authority_generation,transition_ordinal,state_version,state,owner_id,boot_id,runtime_instance_id,backend,acquisition_id,acquisition_request_hash,acquisition_kind,predecessor_authority_id,successor_authority_id,snapshot,created_at FROM runtime_startup_authority_facts ORDER BY authority_generation DESC,transition_ordinal DESC LIMIT 1`
	if !sqlite {
		query = `SELECT authority_id::text,authority_generation,transition_ordinal,state_version,state,owner_id,boot_id::text,runtime_instance_id::text,backend,acquisition_id::text,acquisition_request_hash,acquisition_kind,predecessor_authority_id::text,successor_authority_id::text,snapshot,created_at FROM runtime_startup_authority_facts ORDER BY authority_generation DESC,transition_ordinal DESC LIMIT 1`
		if lock {
			query += ` FOR UPDATE`
		}
	}
	var record authorityHeadRecord
	var predecessor, successor sql.NullString
	var raw []byte
	var createdAt any
	err := queryer.QueryRowContext(ctx, query).Scan(
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
		return authorityHeadRecord{}, false, fmt.Errorf("inspect process authority head: %w", err)
	}
	record.PredecessorAuthorityID = predecessor.String
	record.SuccessorAuthorityID = successor.String
	record.Snapshot = append(json.RawMessage(nil), raw...)
	record.CreatedAt, err = authorityRecordTime(createdAt)
	if err != nil {
		return authorityHeadRecord{}, false, err
	}
	return record, true, nil
}

func authorityRecordTime(value any) (time.Time, error) {
	switch typed := value.(type) {
	case time.Time:
		return typed.UTC(), nil
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
				return parsed.UTC(), nil
			}
		}
	case []byte:
		return authorityRecordTime(string(typed))
	}
	return time.Time{}, fmt.Errorf("inspect process authority created_at: unsupported value %T", value)
}

func classifyAuthorityHead(record authorityHeadRecord, exists bool, backend string) (runtimestartupownership.AuthorityInspection, error) {
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
	var authority runtimestartupownership.Authority
	if json.Unmarshal(record.Snapshot, &authority) == nil && authority.Validate() == nil && authorityMatchesRecord(authority, record) {
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
	inspection, err := classifyAuthorityHead(record, exists, backend)
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
