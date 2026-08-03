package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

const runtimeSharedStoreOwnershipLock = "swarm:runtime:shared-store-owner"

func (s *PostgresStore) AcquireRuntimeStartupOwnership(ctx context.Context, req runtimestartupownership.AcquireRequest) (runtimestartupownership.Lease, error) {
	if s == nil || s.backend.db == nil {
		return nil, nil
	}
	ownerID := strings.TrimSpace(req.OwnerID)
	if ownerID == "" {
		return nil, fmt.Errorf("runtime owner id is required")
	}
	lease, acquired, err := acquireAdvisoryLockLease(ctx, s.backend.db, runtimeSharedStoreOwnershipLock)
	if err != nil {
		return nil, fmt.Errorf("acquire shared runtime store ownership for %s: %w", ownerID, err)
	}
	if !acquired {
		return nil, fmt.Errorf("shared runtime store already owned by another runtime instance")
	}
	authority, err := runtimestartupownership.NewColdAuthority(req, "postgres_advisory_lock")
	if err != nil {
		return nil, errors.Join(err, lease.Release(ctx))
	}
	ownedRecorder := postgresStartupOwnershipRecorder{store: s, lease: lease}
	if err := ownedRecorder.RecordRuntimeStartupAuthorityTransition(ctx, nil, authority); err != nil {
		return nil, errors.Join(err, lease.Release(ctx))
	}
	ownedLease, err := runtimestartupownership.NewLease(authority, ownedRecorder, lease.Release)
	if err != nil {
		return nil, errors.Join(err, lease.Release(ctx))
	}
	return ownedLease, nil
}

func (s *PostgresStore) RecordRuntimeStartupAuthorityTransition(ctx context.Context, previous *runtimestartupownership.Authority, next ...runtimestartupownership.Authority) error {
	if s == nil || s.backend.db == nil {
		return fmt.Errorf("postgres store is required for startup authority evidence")
	}
	if err := runtimestartupownership.ValidateTransitionChain(previous, next...); err != nil {
		return err
	}
	return runPostgresSessionTransaction(ctx, s.backend.db, func(txctx context.Context, tx *sql.Tx) error {
		return recordPostgresRuntimeStartupAuthorityTransitionTx(txctx, tx, previous, next...)
	})
}

func recordPostgresRuntimeStartupAuthorityTransitionTx(
	ctx context.Context,
	tx *sql.Tx,
	previous *runtimestartupownership.Authority,
	next ...runtimestartupownership.Authority,
) error {
	if err := runtimestartupownership.ValidateTransitionChain(previous, next...); err != nil {
		return err
	}
	return func() error {
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
					bundle_hash,backend,handoff_id,snapshot,created_at
				) VALUES (gen_random_uuid(),$1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8::uuid,$9,$10,NULLIF($11,'')::uuid,$12::jsonb,$13)
			`, authority.AuthorityID, authority.LeaseAuthorityID, authority.TransitionOrdinal, authority.Generation, authority.StateVersion, authority.State,
				authority.OwnerID, authority.BootID, authority.BundleHash, authority.Backend, authority.HandoffID, string(raw), authority.RecordedAt.UTC()); err != nil {
				return fmt.Errorf("record runtime startup authority: %w", err)
			}
		}
		return nil
	}()
}

type postgresStartupOwnershipRecorder struct {
	store *PostgresStore
	lease *sqlAdvisoryLockLease
}

func (r postgresStartupOwnershipRecorder) RecordRuntimeStartupAuthorityTransition(
	ctx context.Context,
	previous *runtimestartupownership.Authority,
	next ...runtimestartupownership.Authority,
) error {
	if r.store == nil || r.lease == nil {
		return errors.New("PostgreSQL startup ownership recorder is missing")
	}
	return r.lease.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return recordPostgresRuntimeStartupAuthorityTransitionTx(txctx, tx, previous, next...)
	})
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
