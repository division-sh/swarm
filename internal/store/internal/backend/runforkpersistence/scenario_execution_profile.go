package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"
)

func admitRunForkScenarioProfile(
	ctx context.Context,
	tx *sql.Tx,
	sourceRunID string,
	target scenarioexecution.EffectiveSourceIdentity,
	targetBundle runtimecorrelation.SourceArtifactFact,
) (scenarioexecution.Profile, bool, error) {
	return admitRunForkScenarioProfileWithLoader(ctx, tx, sourceRunID, target, targetBundle, scenarioexecutionpersistence.LoadPostgresTx)
}

func admitSQLiteRunForkScenarioProfile(
	ctx context.Context,
	tx *sql.Tx,
	sourceRunID string,
	target scenarioexecution.EffectiveSourceIdentity,
	targetBundle runtimecorrelation.SourceArtifactFact,
) (scenarioexecution.Profile, bool, error) {
	return admitRunForkScenarioProfileWithLoader(ctx, tx, sourceRunID, target, targetBundle, scenarioexecutionpersistence.LoadSQLiteTx)
}

type runForkScenarioProfileLoader func(context.Context, *sql.Tx, string) (scenarioexecution.Profile, bool, error)

func admitRunForkScenarioProfileWithLoader(
	ctx context.Context,
	tx *sql.Tx,
	sourceRunID string,
	target scenarioexecution.EffectiveSourceIdentity,
	targetBundle runtimecorrelation.SourceArtifactFact,
	load runForkScenarioProfileLoader,
) (scenarioexecution.Profile, bool, error) {
	if load == nil {
		return scenarioexecution.Profile{}, false, fmt.Errorf("run fork scenario profile loader is required")
	}
	profile, found, err := load(ctx, tx, sourceRunID)
	if err != nil {
		return scenarioexecution.Profile{}, false, fmt.Errorf("load source run scenario execution profile: %w", err)
	}
	if !found {
		return scenarioexecution.Profile{}, false, nil
	}
	if err := target.Validate(); err != nil {
		return scenarioexecution.Profile{}, false, fmt.Errorf("profiled run fork requires exact target effective source identity: %w", err)
	}
	if !target.SourceArtifactFact().Matches(targetBundle) {
		return scenarioexecution.Profile{}, false, fmt.Errorf("profiled run fork target bundle source does not match fork materialization source")
	}
	if !profile.EffectiveSourceIdentity().Equal(target) {
		return scenarioexecution.Profile{}, false, fmt.Errorf(
			"profiled run fork effective source mismatch: source=%s target=%s",
			profile.EffectiveSourceIdentity().Digest(), target.Digest(),
		)
	}
	return profile, true, nil
}

func requireExactRunForkScenarioProfile(ctx context.Context, tx *sql.Tx, forkRunID string, source scenarioexecution.Profile, sourceProfiled bool) error {
	return requireExactRunForkScenarioProfileWithLoader(ctx, tx, forkRunID, source, sourceProfiled, scenarioexecutionpersistence.LoadPostgresTx, scenarioexecutionpersistence.RequirePostgresExact)
}

func requireExactSQLiteRunForkScenarioProfile(ctx context.Context, tx *sql.Tx, forkRunID string, source scenarioexecution.Profile, sourceProfiled bool) error {
	return requireExactRunForkScenarioProfileWithLoader(ctx, tx, forkRunID, source, sourceProfiled, scenarioexecutionpersistence.LoadSQLiteTx, scenarioexecutionpersistence.RequireSQLiteExact)
}

type runForkScenarioProfileExact func(context.Context, *sql.Tx, string, scenarioexecution.Profile) error

func requireExactRunForkScenarioProfileWithLoader(ctx context.Context, tx *sql.Tx, forkRunID string, source scenarioexecution.Profile, sourceProfiled bool, load runForkScenarioProfileLoader, requireExact runForkScenarioProfileExact) error {
	if load == nil || requireExact == nil {
		return fmt.Errorf("run fork scenario profile verification owner is required")
	}
	forkProfile, found, err := load(ctx, tx, forkRunID)
	if err != nil {
		return fmt.Errorf("load fork scenario execution profile: %w", err)
	}
	if sourceProfiled {
		if !found {
			return fmt.Errorf("fork materialization %s is missing inherited scenario execution profile", forkRunID)
		}
		if forkProfile.Digest() != source.Digest() || !forkProfile.EffectiveSourceIdentity().Equal(source.EffectiveSourceIdentity()) {
			return fmt.Errorf("fork materialization %s scenario execution profile conflicts with source", forkRunID)
		}
		return requireExact(ctx, tx, forkRunID, source)
	}
	if found {
		return fmt.Errorf("fork materialization %s has an unexpected scenario execution profile", forkRunID)
	}
	return nil
}
