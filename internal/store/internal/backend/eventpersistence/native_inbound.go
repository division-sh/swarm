package eventpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	inbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/store/internal/backend/channelonboarding"
)

func admitInboundLandingRunTx(ctx context.Context, tx *sql.Tx, postgres bool, command inbound.CommitCommand) error {
	request := command.Request.Normalized()
	query := `SELECT run_id,bundle_hash,status FROM runs WHERE flow_path=$1 AND landing_run=TRUE`
	if postgres {
		query += ` FOR UPDATE`
	}
	var runID, bundleHash, state string
	if err := tx.QueryRowContext(ctx, query, request.FlowPath).Scan(&runID, &bundleHash, &state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("flow %s has no landing run", request.FlowPath)
		}
		return fmt.Errorf("lock inbound landing run: %w", err)
	}
	source, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found || source.BundleHash() != bundleHash || runID != request.ResolvedRunID || (state != "live" && state != "paused") {
		return fmt.Errorf("stale, foreign or unavailable inbound landing run")
	}
	return nil
}

func admitNativeInboundPublicationTx(ctx context.Context, tx *sql.Tx, postgres bool, command inbound.CommitCommand) error {
	input, native := command.Admission.NativeInput()
	if !native {
		return nil
	}
	request := command.Request
	target, err := packs.ParseChannelRegistrationTarget(input.TargetSelector())
	source, found := correlation.SourceArtifactFactFromContext(ctx)
	if err != nil || !found || source.BundleHash() != input.Coordinate().BundleHash || target.FlowPath != request.FlowPath ||
		target.Provider != request.Provider || input.PublicationRunID() != request.ResolvedRunID ||
		input.Coordinate().TargetGeneration != uint64(request.ExpectedGeneration) {
		return fmt.Errorf("native publication changed its original source, target or run")
	}
	if err := channelonboarding.RequireSessionBusinessInputTx(ctx, tx, postgres, input); err != nil {
		return err
	}
	if err := command.RequireNativePublicationRequest(); err != nil {
		return err
	}
	for index, event := range command.Finalization.Events {
		if err := command.Admission.ValidateCommitOutput(ctx, source.BundleHash(), request.Provider, index, len(command.Finalization.Events), event.Event, event.Kind, event.Authorization); err != nil {
			return err
		}
	}
	if len(command.Finalization.Events) == 0 {
		return command.RequireOperatorAdmission(ctx)
	}
	if !input.LifetimeCurrent(ctx) {
		return fmt.Errorf("native business publication requires its sealed outputs and lifetime")
	}
	return nil
}
