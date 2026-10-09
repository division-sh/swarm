package eventpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	inbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/store/internal/backend/channelonboarding"
)

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
	for index, event := range command.Finalization.Events {
		if err := command.Admission.ValidateCommitOutput(ctx, source.BundleHash(), request.Provider, index, len(command.Finalization.Events), event.Event, event.Kind, event.Authorization); err != nil {
			return err
		}
	}
	if len(command.Finalization.Events) == 0 || !input.LifetimeCurrent(ctx) {
		return fmt.Errorf("native business publication requires its sealed outputs and lifetime")
	}
	return nil
}
