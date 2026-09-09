package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

func (s *RunForkPostgresOwner) ValidateLifecycleDiagnosticOriginTx(ctx context.Context, tx *sql.Tx, result runtimemanager.AgentLifecycleTransitionResult, historical bool) (string, error) {
	return validateLifecycleDiagnosticOriginTx(ctx, tx, result, historical)
}

func (s *RunForkSQLiteOwner) ValidateLifecycleDiagnosticOriginTx(ctx context.Context, tx *sql.Tx, result runtimemanager.AgentLifecycleTransitionResult, historical bool) (string, error) {
	return validateLifecycleDiagnosticOriginTx(ctx, tx, result, historical)
}

func validateLifecycleDiagnosticOriginTx(ctx context.Context, tx *sql.Tx, result runtimemanager.AgentLifecycleTransitionResult, historical bool) (string, error) {
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
			return "", errors.New("selected-fork lifecycle diagnostic requires its execution origin")
		}
		return "", nil
	}
	if !hasBinding || origin.SelectedFork.ForkRunID != result.Identity.RunID || origin.SelectedFork.Generation != result.ProcessBinding.RuntimeGeneration {
		return "", errors.New("lifecycle diagnostic selected run/generation mismatch")
	}
	var bindingID, sourceRunID, forkEventID, bundleHash string
	var admission, container, actors, config, state string
	var generation uint64
	var expires any
	err := tx.QueryRowContext(ctx, `
		SELECT x.binding_id,x.source_run_id,x.fork_event_id,x.generation,
		       x.admission_fingerprint,x.container_plan_fingerprint,x.actor_census_fingerprint,x.effective_config_fingerprint,
		       x.state,x.lease_expires_at,COALESCE(NULLIF(b.bundle_hash,''),r.bundle_hash)
		FROM run_fork_selected_contract_runtime_executions x
		JOIN run_fork_selected_contract_bindings b ON b.binding_id=x.binding_id AND b.fork_run_id=x.fork_run_id AND b.source_run_id=x.source_run_id AND b.fork_event_id=x.fork_event_id
		JOIN runs r ON r.run_id=x.fork_run_id
		WHERE x.execution_id=$1 AND x.fork_run_id=$2`, origin.SelectedFork.ExecutionID, result.Identity.RunID).Scan(&bindingID, &sourceRunID, &forkEventID, &generation, &admission, &container, &actors, &config, &state, &expires, &bundleHash)
	if err != nil {
		return "", fmt.Errorf("load lifecycle diagnostic selected execution: %w", err)
	}
	s := origin.SelectedFork
	if sourceRunID != origin.SourceRunID || forkEventID != origin.ForkEventID || generation != s.Generation || admission != s.AdmissionFingerprint || container != s.ContainerPlanFingerprint || actors != s.ActorCensusFingerprint || config != s.EffectiveConfigFingerprint || bundleHash != result.ProcessBinding.BundleHash {
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
