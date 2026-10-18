package runforkexecution

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func retainOriginalActivityHistory(ctx context.Context, loader SelectedContractSourceLoader, original semanticview.OriginalLoopCarriage, plan runfork.RunForkPlan) (semanticview.OriginalLoopCarriage, error) {
	seen := make(map[string]bool)
	for _, origin := range plan.ActivityRequestOrigins() {
		if err := ctx.Err(); err != nil {
			return semanticview.OriginalLoopCarriage{}, err
		}
		if err := origin.Validate(); err != nil {
			return semanticview.OriginalLoopCarriage{}, err
		}
		fact := origin.Source()
		key := origin.Request.RunID() + "/" + fact.BundleHash()
		if !seen[key] {
			var loaded LoadedSelectedContractSource
			_, err := loadRunForkSelectedContractSource(ctx, loader, SelectedContractSourceLoadRequest{
				SourceRunID: origin.Request.RunID(), SourceArtifactFact: fact, BundleHash: fact.BundleHash(),
				Selection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: fact.BundleHash()},
			}, &loaded)
			if err == nil && !loaded.SourceArtifactFact.Matches(fact) {
				err = fmt.Errorf("original activity loader returned a different source artifact")
			}
			if err == nil {
				original, err = original.WithActivityHistory(loaded.Source)
			}
			if err = errors.Join(err, cleanupLoadedSelectedContractSource(loaded)); err != nil {
				return semanticview.OriginalLoopCarriage{}, fmt.Errorf("retain original activity artifact for run %s: %w", origin.Request.RunID(), err)
			}
			seen[key] = true
		}
		var err error
		original, err = original.WithActivityOrigin(origin)
		if err != nil {
			return semanticview.OriginalLoopCarriage{}, err
		}
	}
	return original, ctx.Err()
}
