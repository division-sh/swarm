package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
)

func resolveRunForkPublicationPoint(ctx context.Context, tx *sql.Tx, data *storedurabledata.Owner, runID, publicationID string, resolve runForkRevisionPointResolver) (runfork.RunForkPoint, error) {
	record, found, err := storedurabledata.ReadFeedPublicationTx(data, ctx, tx, publicationID)
	if err != nil {
		return runfork.RunForkPoint{}, err
	}
	if !found {
		cursor, err := resolve(ctx, tx, runID, publicationID)
		return runfork.RunForkPoint{Kind: cursor.Kind, Revision: cursor.Revision, Input: publicationID, EventID: cursor.EventID}, err
	}
	if record.Summary.RunID != runID || record.Summary.Outcome != "created" || len(record.DeploymentFeedIDs()) == 0 {
		return runfork.RunForkPoint{}, fmt.Errorf("publication is not an accepted feed of the selected source run")
	}
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointStateRevision}
	for _, feedID := range record.DeploymentFeedIDs() {
		revision, err := runforkrevision.FirstDeploymentFeedRevisionTx(ctx, tx, runID, feedID)
		if err != nil {
			return runfork.RunForkPoint{}, err
		}
		if point.Revision != 0 && point.Revision != revision {
			return runfork.RunForkPoint{}, fmt.Errorf("publication feeds do not share one committed revision")
		}
		point.Revision = revision
	}
	return point, point.Validate()
}

type runForkDeploymentProjection struct {
	feed   runtimedata.DeploymentFeed
	target runtimedata.PinnedSource
	source *runfork.RunForkFanOutObligation
}

func projectRunForkDeploymentFeeds(ctx context.Context, tx *sql.Tx, data *storedurabledata.Owner, forkRunID, targetBundle string, plan runfork.RunForkPlan, pins []runtimedata.Pin, replay bool) ([]runForkDeploymentProjection, error) {
	sources := make(map[string]runtimedata.DeploymentFeed)
	overrides := make([]runtimedata.ExplicitPin, 0)
	for _, pin := range pins {
		if pin.Selection == "fork_override" {
			overrides = append(overrides, runtimedata.ExplicitPin{Declaration: pin.Declaration, VersionID: pin.VersionID})
		}
	}
	for _, obligation := range plan.FanOutObligations {
		intent := obligation.Intent
		if intent.Request.Deployment == nil {
			continue
		}
		if err := intent.Validate(); err != nil {
			return nil, err
		}
		sources[intent.Request.Key.DeploymentFeedID] = runtimedata.DeploymentFeed{
			RunID: plan.SourceRunID, BundleHash: intent.Request.Deployment.BundleHash, Declaration: intent.Source.Declaration,
			VersionID: intent.Source.VersionID, SchemaDigest: intent.Request.Deployment.SchemaDigest, RowCount: uint64(intent.Request.Cardinality),
		}
	}
	publications, err := storedurabledata.MaterializeForkFeedPublicationsTx(data, ctx, tx, plan.SourceRunID, forkRunID, targetBundle, sources, overrides, replay)
	if err != nil {
		return nil, err
	}
	legacy := make(map[runtimedata.DeclarationRef]*runfork.RunForkFanOutObligation)
	publicationDeclarations := make(map[runtimedata.DeclarationRef]bool)
	projected := make([]runForkDeploymentProjection, 0, len(pins)+len(publications))
	for index := range plan.FanOutObligations {
		source := &plan.FanOutObligations[index]
		if source.Intent.Request.Deployment == nil {
			continue
		}
		feed, present := publications[source.Intent.Request.Key.DeploymentFeedID]
		if !present {
			ref := source.Intent.Source.Declaration
			if legacy[ref] != nil {
				return nil, fmt.Errorf("fork source repeats an unqualified deployment feed")
			}
			legacy[ref] = source
			continue
		}
		target, err := storedurabledata.RequireFeedSourceTx(ctx, data, tx, forkRunID, targetBundle, feed.PublicationID(), feed.Declaration)
		if err != nil {
			return nil, err
		}
		projected = append(projected, runForkDeploymentProjection{feed, target, source})
		publicationDeclarations[feed.Declaration] = true
	}
	for _, pin := range pins {
		if pin.RunID != forkRunID {
			return nil, fmt.Errorf("fork deployment pin belongs to another run")
		}
		if publicationDeclarations[pin.Declaration] && legacy[pin.Declaration] == nil {
			continue
		}
		target, err := storedurabledata.RequirePinnedSourceTx(ctx, data, tx, forkRunID, targetBundle, pin.Declaration)
		if err != nil || target.VersionID != pin.VersionID || target.SchemaDigest != pin.SchemaDigest {
			return nil, fmt.Errorf("fork feed contradicts its exact child pin: %w", err)
		}
		feed := runtimedata.DeploymentFeed{RunID: forkRunID, BundleHash: targetBundle, Declaration: pin.Declaration,
			VersionID: target.VersionID, SchemaDigest: target.SchemaDigest, RowCount: uint64(target.RowCount)}
		projected = append(projected, runForkDeploymentProjection{feed, target, legacy[pin.Declaration]})
		delete(legacy, pin.Declaration)
	}
	if len(legacy) != 0 {
		return nil, fmt.Errorf("fork source deployment feed has no exact child pin")
	}
	return projected, nil
}
