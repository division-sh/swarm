package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

const runtimeSharedStoreOwnershipLock = "swarm:runtime:shared-store-owner"

func (s *PostgresStore) AcquireRuntimeStartupOwnership(ctx context.Context, req runtimestartupownership.AcquireRequest) (runtimestartupownership.Lease, error) {
	if s == nil || s.DB == nil {
		return nil, nil
	}
	ownerID := strings.TrimSpace(req.OwnerID)
	if ownerID == "" {
		return nil, fmt.Errorf("runtime owner id is required")
	}
	lease, acquired, err := acquireAdvisoryLockLease(ctx, s.DB, runtimeSharedStoreOwnershipLock)
	if err != nil {
		return nil, fmt.Errorf("acquire shared runtime store ownership for %s: %w", ownerID, err)
	}
	if !acquired {
		return nil, fmt.Errorf("shared runtime store already owned by another runtime instance")
	}
	authority, err := runtimestartupownership.NewColdAuthority(req, "postgres_advisory_lock")
	if err != nil {
		_ = lease.Release(ctx)
		return nil, err
	}
	if err := s.RecordRuntimeStartupAuthorityTransition(ctx, nil, authority); err != nil {
		_ = lease.Release(ctx)
		return nil, err
	}
	return runtimestartupownership.NewLease(authority, s, lease.Release)
}

func (s *PostgresStore) RecordRuntimeStartupAuthorityTransition(ctx context.Context, previous *runtimestartupownership.Authority, next ...runtimestartupownership.Authority) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required for startup authority evidence")
	}
	if err := runtimestartupownership.ValidateTransitionChain(previous, next...); err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	leaseID := next[0].LeaseAuthorityID
	var persistedRaw []byte
	headErr := tx.QueryRowContext(ctx, `
		SELECT snapshot FROM runtime_startup_authority_facts
		WHERE lease_authority_id=$1::uuid
		ORDER BY transition_ordinal DESC LIMIT 1 FOR UPDATE
	`, leaseID).Scan(&persistedRaw)
	if err := validatePersistedStartupAuthorityHead(persistedRaw, headErr, previous); err != nil {
		return err
	}
	for _, authority := range next {
		raw, err := json.Marshal(authority)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO runtime_startup_authority_facts (
				fact_id,authority_id,lease_authority_id,transition_ordinal,generation,state_version,state,owner_id,boot_id,
				bundle_fingerprint,backend,handoff_id,snapshot,created_at
			) VALUES (gen_random_uuid(),$1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8::uuid,$9,$10,NULLIF($11,'')::uuid,$12::jsonb,$13)
		`, authority.AuthorityID, authority.LeaseAuthorityID, authority.TransitionOrdinal, authority.Generation, authority.StateVersion, authority.State,
			authority.OwnerID, authority.BootID, authority.BundleFingerprint, authority.Backend, authority.HandoffID, string(raw), authority.RecordedAt.UTC()); err != nil {
			return fmt.Errorf("record runtime startup authority: %w", err)
		}
	}
	return tx.Commit()
}

func validatePersistedStartupAuthorityHead(raw []byte, queryErr error, previous *runtimestartupownership.Authority) error {
	if previous == nil {
		if queryErr == nil {
			return fmt.Errorf("initial runtime startup authority conflicts with an existing lease head")
		}
		if queryErr != sql.ErrNoRows {
			return fmt.Errorf("load runtime startup authority head: %w", queryErr)
		}
		return nil
	}
	if queryErr != nil {
		if queryErr == sql.ErrNoRows {
			return fmt.Errorf("runtime startup authority transition has no persisted predecessor")
		}
		return fmt.Errorf("load runtime startup authority head: %w", queryErr)
	}
	var persisted runtimestartupownership.Authority
	if err := json.Unmarshal(raw, &persisted); err != nil {
		return fmt.Errorf("decode runtime startup authority head: %w", err)
	}
	if err := persisted.Validate(); err != nil {
		return fmt.Errorf("validate runtime startup authority head: %w", err)
	}
	persistedJSON, _ := json.Marshal(persisted)
	previousJSON, _ := json.Marshal(previous)
	if !bytes.Equal(persistedJSON, previousJSON) {
		return fmt.Errorf("runtime startup authority compare-and-set predecessor mismatch")
	}
	return nil
}
