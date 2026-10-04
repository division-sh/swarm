package runforkexecution

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type SelectedForkRecoveryInspectionReader interface {
	SourceArtifactSelectedContractSourceStore
	ListSelectedForkRecoveryEntries(context.Context) ([]runfork.SelectedForkRecoveryEntry, error)
	InspectSelectedForkRecovery(context.Context, runfork.SelectedForkRecoveryEntry) (runfork.SelectedForkRecoveryInspection, error)
}

type InspectedSelectedForkRecovery struct {
	Entry          runfork.SelectedForkRecoveryEntry
	Evidence       runfork.SelectedForkRecoveryInspection
	SelectedSource LoadedSelectedContractSource
	OriginalSource *LoadedSelectedContractSource
}

// InspectSelectedForkRecoveries consumes boot's source, recovery-record and
// handoff admission owners. It neither prepares a runtime nor settles a fork.
func InspectSelectedForkRecoveries(ctx context.Context, reader SelectedForkRecoveryInspectionReader, loader SourceArtifactSelectedContractSourceLoader) ([]InspectedSelectedForkRecovery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, fmt.Errorf("selected fork recovery inspection reader is required")
	}
	loader.Store = reader
	entries, err := reader.ListSelectedForkRecoveryEntries(ctx)
	if err != nil {
		return nil, err
	}
	inspections := make([]InspectedSelectedForkRecovery, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fact, err := admitSelectedRecoverySource(ctx, reader, entry)
		if err != nil {
			return nil, err
		}
		evidence, err := reader.InspectSelectedForkRecovery(ctx, entry)
		if err != nil {
			return nil, fmt.Errorf("inspect selected fork %s: %w", entry.Binding.ForkRunID, err)
		}
		action, err := selectedRecoveryActionFor(evidence.Plan, entry)
		if err != nil {
			return nil, err
		}
		selected, err := loader.InspectRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
			SourceRunID: entry.Binding.ForkRunID, BundleHash: entry.BundleHash,
			SourceArtifactFact: fact, Selection: entry.Binding.ContractSelection,
		})
		if err != nil {
			return nil, err
		}
		inspection := InspectedSelectedForkRecovery{Entry: entry, Evidence: evidence, SelectedSource: selected}
		if action == selectedRecoveryResumeFiniteFeed || action == selectedRecoveryActivateFiniteFeed {
			original, err := loader.InspectRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
				SourceRunID: entry.Binding.SourceRunID,
				Selection:   runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
			})
			if err != nil {
				return nil, err
			}
			carriage, err := semanticview.CompileOriginalLoopCarriage(original.Source)
			if err != nil {
				return nil, err
			}
			if err := carriage.RequireSource(original.SourceArtifactFact.BundleHash()); err != nil {
				return nil, err
			}
			inspection.OriginalSource = &original
		}
		inspections = append(inspections, inspection)
	}
	return inspections, ctx.Err()
}
