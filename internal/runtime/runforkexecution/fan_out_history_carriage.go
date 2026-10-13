package runforkexecution

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func retainOriginalFanOutHistory(ctx context.Context, loader SelectedContractSourceLoader, original semanticview.OriginalLoopCarriage, originalHash string, plan runfork.RunForkPlan) (semanticview.OriginalLoopCarriage, error) {
	if err := ctx.Err(); err != nil {
		return semanticview.OriginalLoopCarriage{}, err
	}
	requirements, err := runfork.FanOutContinuationRequirements(plan)
	if err != nil {
		return semanticview.OriginalLoopCarriage{}, err
	}
	seen := map[string]struct{}{originalHash: {}}
	for _, obligation := range plan.FanOutObligations {
		if err := ctx.Err(); err != nil {
			return semanticview.OriginalLoopCarriage{}, err
		}
		if obligation.Intent.Request.Deployment != nil {
			continue
		}
		if requirements[obligation.Intent.Request.Key] {
			continue
		}
		hash := obligation.Intent.Request.PlanRef.BundleHash
		if _, retained := seen[hash]; retained {
			continue
		}
		var loaded LoadedSelectedContractSource
		_, err := loadRunForkSelectedContractSource(ctx, loader, SelectedContractSourceLoadRequest{
			BundleHash: hash, Selection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: hash},
		}, &loaded)
		if err == nil {
			original, err = original.WithFanOutHistory(loaded.Source)
		}
		if err = errors.Join(err, cleanupLoadedSelectedContractSource(loaded)); err != nil {
			return semanticview.OriginalLoopCarriage{}, fmt.Errorf("retain original fan-out artifact %s: %w", hash, err)
		}
		seen[hash] = struct{}{}
	}
	return original, nil
}
