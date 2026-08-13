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
	targetBundle runtimecorrelation.BundleSourceFact,
) (scenarioexecution.Profile, bool, error) {
	profile, found, err := scenarioexecutionpersistence.LoadPostgresTx(ctx, tx, sourceRunID)
	if err != nil {
		return scenarioexecution.Profile{}, false, fmt.Errorf("load source run scenario execution profile: %w", err)
	}
	if !found {
		return scenarioexecution.Profile{}, false, nil
	}
	if err := target.Validate(); err != nil {
		return scenarioexecution.Profile{}, false, fmt.Errorf("profiled run fork requires exact target effective source identity: %w", err)
	}
	if !target.BundleSourceFact().Matches(targetBundle) {
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
	forkProfile, found, err := scenarioexecutionpersistence.LoadPostgresTx(ctx, tx, forkRunID)
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
		return scenarioexecutionpersistence.RequirePostgresExact(ctx, tx, forkRunID, source)
	}
	if found {
		return fmt.Errorf("fork materialization %s has an unexpected scenario execution profile", forkRunID)
	}
	return nil
}
