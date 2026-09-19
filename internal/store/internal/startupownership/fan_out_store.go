package startupownership

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/manager"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

func (s *postgresSession) FanOutServingStore() (runtimeownership.FanOutServingStore, error) {
	if _, err := s.Authority(); err != nil {
		return nil, err
	}
	if s.fanOutStore == nil {
		return nil, errors.New("retained PostgreSQL session has no assembled fan-out store")
	}
	return s.fanOutStore, nil
}

func (s *sqliteSession) FanOutServingStore() (runtimeownership.FanOutServingStore, error) {
	if _, err := s.Authority(); err != nil {
		return nil, err
	}
	if s.fanOutStore == nil {
		return nil, errors.New("retained SQLite session has no assembled fan-out store")
	}
	return s.fanOutStore, nil
}

type fanOutAdmission struct {
	session interface {
		Authority() (runtimeownership.Authority, error)
	}
	sqlite bool
}

var errFanOutSourceNotCurrent = errors.New("fan-out grant no longer owns the current source-set head")

// Exact discard does not require a live generation or source head. It does
// require the same retained process, and the pipeline owner fences the precise
// claim tuple so cleanup cannot touch a successor claim.
func (a *fanOutAdmission) AdmitFanOutCleanupTx(ctx context.Context, tx *sql.Tx, grant runtimeownership.GrantEvidence) error {
	if err := grant.Validate(); err != nil {
		return err
	}
	authority, err := a.admitProcess(ctx, tx)
	if err != nil {
		return err
	}
	if grant.ProcessAuthorityID != authority.AuthorityID || grant.ProcessOwnerID != authority.OwnerID || grant.ProcessBootID != authority.BootID {
		return errors.New("fan-out cleanup belongs to another process")
	}
	return nil
}

func (a *fanOutAdmission) admitProcess(ctx context.Context, tx *sql.Tx) (runtimeownership.Authority, error) {
	return a.proveProcess(ctx, tx, true)
}

func (a *fanOutAdmission) proveProcess(ctx context.Context, tx *sql.Tx, lock bool) (runtimeownership.Authority, error) {
	if lock {
		if err := generationauthority.FenceMutation(ctx, tx, a.sqlite); err != nil {
			return runtimeownership.Authority{}, err
		}
	}
	authority, err := a.session.Authority()
	if err != nil {
		return authority, err
	}
	current, err := ProcessAuthorityCurrent(ctx, tx, authority, a.sqlite, lock)
	if err != nil {
		return authority, err
	}
	if !current {
		return authority, errors.New("fan-out process authority is no longer current")
	}
	return authority, nil
}

func (a *fanOutAdmission) admitSource(ctx context.Context, tx *sql.Tx, authority runtimeownership.Authority, grant runtimeownership.GrantEvidence) error {
	return a.proveSource(ctx, tx, authority, grant, true)
}

func (a *fanOutAdmission) proveSource(ctx context.Context, tx *sql.Tx, authority runtimeownership.Authority, grant runtimeownership.GrantEvidence, lock bool) error {
	if err := grant.Validate(); err != nil {
		return err
	}
	if grant.State != runtimeownership.GrantAdmitted || grant.ProcessAuthorityID != authority.AuthorityID ||
		grant.ProcessOwnerID != authority.OwnerID || grant.ProcessBootID != authority.BootID {
		return errors.New("fan-out grant differs from the retained process authority")
	}
	if grant.SelectedFork != nil {
		return errors.New("fan-out serving does not admit selected-fork grants")
	}
	plan, exists, err := loadSourceSetModeTx(ctx, tx, a.sqlite, lock)
	if err != nil {
		return err
	}
	if !exists || plan.Revision != grant.SourceSetRevision || !sourceSetContains(plan, agenttopology.SourceCoordinate{BundleHash: grant.BundleHash}) {
		return errFanOutSourceNotCurrent
	}
	return nil
}

func (a *fanOutAdmission) ObserveFanOutGrantsTx(ctx context.Context, tx *sql.Tx, grants []runtimeownership.GrantEvidence) error {
	authority, err := a.proveProcess(ctx, tx, false)
	if err != nil {
		return err
	}
	expected := make(map[string]string, len(grants))
	pending := false
	for _, grant := range grants {
		if err := a.proveSource(ctx, tx, authority, grant, false); err != nil {
			if !errors.Is(err, errFanOutSourceNotCurrent) {
				return err
			}
			// A retained closing predecessor can outlive a source-head refresh.
			// It must still match durable admitted evidence below, but only a
			// missing current-head grant makes the registration census incomplete.
		}
		if _, duplicate := expected[grant.GrantID]; duplicate {
			return errors.New("fan-out admitted grant set contains a duplicate")
		}
		raw, err := canonicaljson.Bytes(grant)
		if err != nil {
			return err
		}
		expected[grant.GrantID] = string(raw)
	}
	allGrants, currentGrants, err := observeFanOutGrantFamilyTx(ctx, tx, authority, a.sqlite)
	if err != nil {
		return err
	}
	currentIDs := make(map[string]bool, len(currentGrants))
	for _, grant := range currentGrants {
		currentIDs[grant.GrantID] = true
	}
	for _, current := range allGrants {
		if _, registered := expected[current.GrantID]; !registered {
			pending = pending || currentIDs[current.GrantID]
			continue
		}
		canonical, err := canonicaljson.Bytes(current)
		if err != nil {
			return err
		}
		if expected[current.GrantID] != string(canonical) {
			return errors.New("fan-out selection requires the complete current admitted grant set")
		}
		delete(expected, current.GrantID)
	}
	if len(expected) != 0 {
		return errors.New("fan-out selection contains a stale or foreign grant")
	}
	if pending {
		return pipelinepersistence.ErrFanOutRegistrationPending
	}
	return nil
}

func loadAdmittedFanOutGrantsTx(ctx context.Context, tx *sql.Tx, authorityID string) ([]runtimeownership.GrantEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT g.grant_id,g.bundle_hash,g.selected_binding_id,g.state_version,g.snapshot FROM runtime_generation_grants g
		WHERE g.process_authority_id=$1 AND g.state='admitted'
		AND g.state_version=(SELECT MAX(head.state_version) FROM runtime_generation_grants head WHERE head.grant_id=g.grant_id)`, authorityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var grants []runtimeownership.GrantEvidence
	for rows.Next() {
		var raw []byte
		var grantID, bundleHash string
		var selectedBindingID sql.NullString
		var stateVersion uint64
		if err := rows.Scan(&grantID, &bundleHash, &selectedBindingID, &stateVersion, &raw); err != nil {
			return nil, err
		}
		var grant runtimeownership.GrantEvidence
		if err := canonicaljson.DecodeInto(raw, &grant); err != nil {
			return nil, err
		}
		if err := grant.Validate(); err != nil {
			return nil, err
		}
		if grant.GrantID != grantID || grant.BundleHash != bundleHash || grant.StateVersion != stateVersion ||
			grant.ProcessAuthorityID != authorityID || grant.State != runtimeownership.GrantAdmitted ||
			(grant.SelectedFork != nil) != selectedBindingID.Valid ||
			(grant.SelectedFork != nil && grant.SelectedFork.BindingID != selectedBindingID.String) {
			return nil, errors.New("fan-out grant snapshot differs from durable grant coordinates")
		}
		// Selected-fork execution is a separate, unsupported serving family.
		// Validate its durable shape, but do not demand a live registration.
		if grant.SelectedFork != nil {
			continue
		}
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}

// Source refresh may leave an admitted predecessor while its successor is
// prepared/registered. Only grants on the current source head compete for
// ordinary execution; off-head evidence is still decoded and validated.
func observeFanOutGrantFamilyTx(ctx context.Context, tx *sql.Tx, authority runtimeownership.Authority, sqlite bool) (all, current []runtimeownership.GrantEvidence, err error) {
	all, err = loadAdmittedFanOutGrantsTx(ctx, tx, authority.AuthorityID)
	if err != nil {
		return nil, nil, err
	}
	admission := fanOutAdmission{sqlite: sqlite}
	bundles := make(map[string]string)
	for _, grant := range all {
		if err := admission.proveSource(ctx, tx, authority, grant, false); err != nil {
			if errors.Is(err, errFanOutSourceNotCurrent) {
				continue
			}
			return nil, nil, err
		}
		if prior, exists := bundles[grant.BundleHash]; exists && prior != grant.GrantID {
			return nil, nil, errors.New("fan-out ordinary execution authority is ambiguous for the same bundle")
		}
		bundles[grant.BundleHash] = grant.GrantID
		current = append(current, grant)
	}
	return all, current, nil
}

// Transaction access stays in the private composition adapter, never on the
// semantic startup owner's effective method set.
type fanOutReadiness struct {
	sqlite bool
}

func (r *fanOutReadiness) CurrentFanOutGrantsTx(ctx context.Context, tx *sql.Tx) ([]runtimeownership.GrantEvidence, error) {
	return currentFanOutGrantsTx(ctx, tx, r.sqlite)
}

func currentFanOutGrantsTx(ctx context.Context, tx *sql.Tx, sqlite bool) ([]runtimeownership.GrantEvidence, error) {
	backend := "postgres_retained_session"
	if sqlite {
		backend = "sqlite_retained_owner"
	}
	authority, exists, err := loadAuthorityHeadModeTx(ctx, tx, backend, sqlite, false)
	if err != nil {
		return nil, err
	}
	if !exists || authority.State != runtimeownership.StateActive {
		return nil, nil
	}
	_, current, err := observeFanOutGrantFamilyTx(ctx, tx, authority, sqlite)
	return current, err
}

func (a *fanOutAdmission) ObserveFanOutRunTx(ctx context.Context, tx *sql.Tx, grant runtimeownership.GrantEvidence, runID string) (bool, error) {
	authority, err := a.proveProcess(ctx, tx, false)
	if err != nil {
		return false, err
	}
	if err := a.proveSource(ctx, tx, authority, grant, false); err != nil {
		return false, err
	}
	if _, _, err := observeFanOutGrantFamilyTx(ctx, tx, authority, a.sqlite); err != nil {
		return false, err
	}
	ownership, err := agentpersistence.ObserveOrdinaryRunExecutionOwnershipTx(ctx, tx, grant, runID, a.sqlite)
	if err != nil {
		return false, fmt.Errorf("observe fan-out run execution: %w", err)
	}
	return ownership == manager.RunExecutionOwned, nil
}

func (a *fanOutAdmission) AdmitFanOutRunTx(ctx context.Context, tx *sql.Tx, grant runtimeownership.GrantEvidence, runID string) (bool, error) {
	authority, err := a.admitProcess(ctx, tx)
	if err != nil {
		return false, err
	}
	if err := a.admitSource(ctx, tx, authority, grant); err != nil {
		return false, err
	}
	if _, _, err := observeFanOutGrantFamilyTx(ctx, tx, authority, a.sqlite); err != nil {
		return false, err
	}
	ownership, err := agentpersistence.InspectRunExecutionOwnershipTx(ctx, tx, grant, runID, a.sqlite)
	if err != nil {
		return false, fmt.Errorf("admit fan-out run execution: %w", err)
	}
	return ownership == manager.RunExecutionOwned && grant.SelectedFork == nil, nil
}
