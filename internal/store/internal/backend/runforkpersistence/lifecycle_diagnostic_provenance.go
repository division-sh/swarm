package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
)

func (s *RunForkPostgresOwner) ValidateLifecycleDiagnosticOriginTx(ctx context.Context, tx *sql.Tx, result runtimemanager.AgentLifecycleTransitionResult, historical bool) (string, error) {
	return validateLifecycleDiagnosticOriginTx(ctx, tx, result, historical, false)
}

func (s *RunForkSQLiteOwner) ValidateLifecycleDiagnosticOriginTx(ctx context.Context, tx *sql.Tx, result runtimemanager.AgentLifecycleTransitionResult, historical bool) (string, error) {
	return validateLifecycleDiagnosticOriginTx(ctx, tx, result, historical, true)
}

func validateLifecycleDiagnosticOriginTx(ctx context.Context, tx *sql.Tx, result runtimemanager.AgentLifecycleTransitionResult, historical, sqlite bool) (string, error) {
	origin := result.DiagnosticProvenance.Origin
	if err := origin.Validate(); err != nil {
		return "", err
	}
	var hasBinding bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM run_fork_selected_contract_bindings WHERE fork_run_id=$1)`, result.Identity.RunID).Scan(&hasBinding); err != nil {
		return "", err
	}
	if origin.Owner == runtimemanager.LifecycleDiagnosticNormal {
		if hasBinding {
			return "", validateOrdinaryLifecycleHandoffTx(ctx, tx, result, historical, sqlite)
		}
		return "", nil
	}
	if !hasBinding || origin.SelectedFork.ForkRunID != result.Identity.RunID || origin.SelectedFork.Generation != result.ProcessBinding.RuntimeGeneration {
		return "", errors.New("lifecycle diagnostic selected run/generation mismatch")
	}
	var bindingID, sourceRunID, pointKind, forkEventID, bundleHash string
	var admission, container, actors, config, state string
	var generation uint64
	var forkRevision int64
	var expires any
	err := tx.QueryRowContext(ctx, `
		SELECT x.binding_id,x.source_run_id,x.fork_point_kind,x.fork_revision,COALESCE(CAST(x.fork_event_id AS TEXT),''),x.generation,
		       x.admission_fingerprint,x.container_plan_fingerprint,x.actor_census_fingerprint,x.effective_config_fingerprint,
		       x.state,x.lease_expires_at,COALESCE(NULLIF(b.bundle_hash,''),r.bundle_hash)
		FROM run_fork_selected_contract_runtime_executions x
		JOIN run_fork_selected_contract_bindings b ON b.binding_id=x.binding_id AND b.fork_run_id=x.fork_run_id AND b.source_run_id=x.source_run_id
			AND b.fork_point_kind=x.fork_point_kind AND b.fork_revision=x.fork_revision
			AND (b.fork_event_id=x.fork_event_id OR (b.fork_event_id IS NULL AND x.fork_event_id IS NULL))
		JOIN runs r ON r.run_id=x.fork_run_id
		WHERE x.execution_id=$1 AND x.fork_run_id=$2`, origin.SelectedFork.ExecutionID, result.Identity.RunID).Scan(&bindingID, &sourceRunID, &pointKind, &forkRevision, &forkEventID, &generation, &admission, &container, &actors, &config, &state, &expires, &bundleHash)
	if err != nil {
		return "", fmt.Errorf("load lifecycle diagnostic selected execution: %w", err)
	}
	s := origin.SelectedFork
	if sourceRunID != origin.SourceRunID || pointKind != string(origin.ForkPoint.Kind) || forkRevision != origin.ForkPoint.Revision ||
		forkEventID != origin.ForkPoint.EventID || generation != s.Generation || admission != s.AdmissionFingerprint ||
		container != s.ContainerPlanFingerprint || actors != s.ActorCensusFingerprint || config != s.EffectiveConfigFingerprint ||
		bundleHash != result.ProcessBinding.BundleHash {
		return "", errors.New("lifecycle diagnostic selected execution facts conflict")
	}
	if historical {
		if result.DiagnosticProvenance.BindingID != bindingID {
			return "", errors.New("lifecycle diagnostic historical binding conflict")
		}
	} else {
		expiresAt, present, err := sqliteTimeValue(expires)
		if err != nil {
			return "", err
		}
		if !present || state == "closed" || !time.Now().Before(expiresAt) {
			return "", errors.New("lifecycle diagnostic selected execution is no longer admitted")
		}
	}
	if origin.Causality == runtimemanager.LifecycleDiagnosticAcceptedEvent {
		var belongs bool
		if err := tx.QueryRowContext(ctx, `WITH RECURSIVE ancestors AS (
			SELECT event_id,source_event_id FROM events WHERE run_id=$1 AND event_id=$2
			UNION SELECT p.event_id,p.source_event_id FROM events p JOIN ancestors c ON c.source_event_id=p.event_id WHERE p.run_id=$1
		) SELECT EXISTS(SELECT 1 FROM ancestors a JOIN run_fork_selected_contract_executions x ON x.fork_event_id=a.event_id AND x.fork_run_id=$1 AND x.source_run_id=$3)`, result.Identity.RunID, origin.ParentEventID, origin.SourceRunID).Scan(&belongs); err != nil {
			return "", err
		}
		if !belongs {
			return "", errors.New("lifecycle diagnostic parent lacks selected execution lineage")
		}
	}
	return bindingID, nil
}

func validateOrdinaryLifecycleHandoffTx(ctx context.Context, tx *sql.Tx, result runtimemanager.AgentLifecycleTransitionResult, historical, sqlite bool) error {
	observe := sourceadmission.ObserveRetainedLiveHandoff
	if historical {
		observe = sourceadmission.ObserveHistoricalLiveHandoff
	}
	handedOff, err := observe(ctx, tx, result.Identity.RunID, result.ProcessBinding.BundleHash)
	if err != nil || !handedOff {
		return errors.Join(err, errors.New("selected-fork lifecycle diagnostic requires its execution origin or exact committed handoff"))
	}
	if historical {
		return nil
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT snapshot FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, result.ProcessBinding.GenerationGrantID).Scan(&raw); err != nil {
		return err
	}
	var grant startupownership.GrantEvidence
	if err := canonicaljson.DecodeInto(raw, &grant); err != nil {
		return err
	}
	expected := runtimemanager.ProcessExecutionBinding{
		ProcessAuthorityID: grant.ProcessAuthorityID, ProcessOwnerID: grant.ProcessOwnerID, ProcessBootID: grant.ProcessBootID,
		GenerationGrantID: grant.GrantID, BundleHash: grant.BundleHash,
		RuntimeInstanceID: grant.RuntimeInstanceID, RuntimeGeneration: grant.RuntimeGeneration,
	}
	if !expected.Equal(result.ProcessBinding) {
		return errors.New("live lifecycle diagnostic differs from its ordinary grant")
	}
	return generationauthority.RequireAdmittedOrdinaryGrantTx(ctx, tx, grant, result.ProcessBinding.BundleHash, sqlite)
}
