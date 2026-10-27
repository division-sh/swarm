package startupownership

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
)

func (s *postgresSession) ObserveStartupRunSource(ctx context.Context, evidence runtimeownership.GrantEvidence, runID string) (bool, error) {
	var applicable bool
	err := s.lease.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		applicable, err = observeStartupRunSourceTx(ctx, tx, evidence, runID, false)
		return err
	})
	return applicable && err == nil, err
}

func (s *sqliteSession) ObserveStartupRunSource(ctx context.Context, evidence runtimeownership.GrantEvidence, runID string) (bool, error) {
	var applicable bool
	err := s.owner.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		applicable, err = observeStartupRunSourceTx(ctx, tx, evidence, runID, true)
		return err
	})
	return applicable && err == nil, err
}

func observeStartupRunSourceTx(ctx context.Context, tx *sql.Tx, evidence runtimeownership.GrantEvidence, runID string, sqlite bool) (bool, error) {
	if err := evidence.Validate(); err != nil {
		return false, err
	}
	if evidence.SelectedFork != nil || (evidence.State != runtimeownership.GrantPrepared && evidence.State != runtimeownership.GrantProbeSettled) {
		return false, errors.New("startup source observation requires a pre-execution ordinary grant")
	}
	if err := generationauthority.ObserveCurrentGrantTx(ctx, tx, evidence); err != nil {
		return false, err
	}
	current, err := generationauthority.ProcessCoordinatesCurrent(ctx, tx, evidence.ProcessCoordinates(), sqlite)
	if err != nil || !current {
		return false, errors.Join(err, errors.New("startup source observation process is not current"))
	}
	plan, present, err := loadSourceSetModeTx(ctx, tx, sqlite, false)
	if err != nil {
		return false, err
	}
	if !present || plan.Revision != evidence.SourceSetRevision || !sourceSetContains(plan, agenttopology.SourceCoordinate{BundleHash: evidence.BundleHash}) {
		return false, errors.New("startup source observation source set is not current")
	}
	source, err := correlation.DecodeSourceArtifactFact(evidence.BundleHash)
	if err != nil {
		return false, err
	}
	return agentpersistence.ObserveOrdinaryRunSourceTx(ctx, tx, source, runID, sqlite)
}
