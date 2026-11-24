package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
)

func resolveRunForkPublicationPoint(ctx context.Context, tx *sql.Tx, data *storedurabledata.Owner, runID, publicationID string, resolve runForkRevisionPointResolver) (runfork.RunForkPoint, bool, error) {
	record, found, err := storedurabledata.ReadFeedPublicationTx(data, ctx, tx, publicationID)
	if err != nil {
		return runfork.RunForkPoint{}, false, err
	}
	if !found {
		cursor, err := resolve(ctx, tx, runID, publicationID)
		var missing missingRunForkEvent
		if !errors.As(err, &missing) {
			return runfork.RunForkPoint{Kind: cursor.Kind, Revision: cursor.Revision, Input: publicationID, EventID: cursor.EventID}, false, err
		}
		revision, found, readErr := runforkrevision.FirstDeploymentFeedRevisionTx(ctx, tx, runID, publicationID)
		if readErr != nil {
			return runfork.RunForkPoint{}, false, readErr
		}
		if !found {
			return runfork.RunForkPoint{}, false, err
		}
		return runfork.RunForkPoint{Kind: runfork.RunForkPointStateRevision, Revision: revision}, true, nil
	}
	if record.Summary.RunID != runID || record.Summary.Outcome != "created" || len(record.DeploymentFeedIDs()) == 0 {
		return runfork.RunForkPoint{}, false, fmt.Errorf("publication is not an accepted feed of the selected source run")
	}
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointStateRevision}
	for _, feedID := range record.DeploymentFeedIDs() {
		revision, found, err := runforkrevision.FirstDeploymentFeedRevisionTx(ctx, tx, runID, feedID)
		if err != nil {
			return runfork.RunForkPoint{}, false, err
		}
		if !found {
			return runfork.RunForkPoint{}, false, fmt.Errorf("publication feed has no committed insertion revision")
		}
		if point.Revision != 0 && point.Revision != revision {
			return runfork.RunForkPoint{}, false, fmt.Errorf("publication feeds do not share one committed revision")
		}
		point.Revision = revision
	}
	return point, false, point.Validate()
}

func selectRunForkDeploymentFeed(ctx context.Context, tx *sql.Tx, data *storedurabledata.Owner, plan *runfork.RunForkPlan, named string) error {
	owed, err := runfork.FanOutContinuationRequirements(*plan)
	if err != nil {
		return err
	}
	selection := &runfork.RunForkDeploymentFeedSelection{Named: named}
	if plan.DeploymentFeedSelection != nil {
		selection.Inherited = plan.DeploymentFeedSelection.Inherited
	}
	retained := make([]runfork.RunForkFanOutObligation, 0, len(plan.FanOutObligations))
	parkedDeliveries := make(map[string]bool)
	found := false
	for _, obligation := range plan.FanOutObligations {
		req := obligation.Intent.Request
		switch {
		case req.Deployment == nil:
		case req.Key.DeploymentFeedID == named:
			source, err := storedurabledata.RequireFeedSourceTx(ctx, data, tx, plan.SourceRunID, req.Deployment.BundleHash, named, req.Deployment.Declaration)
			if err != nil || source.VersionID != req.Deployment.VersionID || source.SchemaDigest != req.Deployment.SchemaDigest || source.RowCount != req.Cardinality {
				return fmt.Errorf("fork deployment feed contradicts its exact source: %w", err)
			}
			found = true
		case owed[req.Key]:
			selection.Parked = append(selection.Parked, obligation)
			for _, ordinal := range obligation.PendingDeployment {
				for _, delivery := range ordinal.SourceDeliveryIDs {
					parkedDeliveries[delivery] = true
				}
			}
			continue
		}
		retained = append(retained, obligation)
	}
	if !found {
		return fmt.Errorf("named fork feed is absent from its committed cut")
	}
	pending := make([]runfork.RunForkPendingWork, 0, len(plan.PendingWork))
	for _, item := range plan.PendingWork {
		if !parkedDeliveries[item.DeliveryID] {
			pending = append(pending, item)
		}
	}
	plan.DeploymentFeedSelection = selection
	plan.FanOutObligations, plan.FanOutObligationCount = retained, len(retained)
	plan.PendingWork, plan.PendingWorkCount = pending, len(pending)
	return nil
}

type runForkDeploymentProjection struct {
	feed   runtimedata.DeploymentFeed
	target runtimedata.PinnedSource
	source *runfork.RunForkFanOutObligation
}

func forkDeploymentSourceMembers(plan runfork.RunForkPlan) (map[string]runtimedata.DeploymentFeed, map[string]bool, error) {
	sources := make(map[string]runtimedata.DeploymentFeed)
	all := plan.FanOutObligations
	var parked map[string]bool
	if selection := plan.DeploymentFeedSelection; selection != nil && len(selection.Parked) > 0 {
		all = append(append([]runfork.RunForkFanOutObligation(nil), all...), selection.Parked...)
		parked = make(map[string]bool, len(selection.Parked))
		for _, obligation := range selection.Parked {
			parked[obligation.Intent.Request.Key.DeploymentFeedID] = true
		}
	}
	for _, obligation := range all {
		intent := obligation.Intent
		if intent.Request.Deployment == nil {
			continue
		}
		if err := intent.Validate(); err != nil {
			return nil, nil, err
		}
		sources[intent.Request.Key.DeploymentFeedID] = runtimedata.DeploymentFeed{
			RunID: plan.SourceRunID, BundleHash: intent.Request.Deployment.BundleHash, Declaration: intent.Source.Declaration,
			VersionID: intent.Source.VersionID, SchemaDigest: intent.Request.Deployment.SchemaDigest, RowCount: uint64(intent.Request.Cardinality),
		}
	}
	return sources, parked, nil
}

func projectRunForkDeploymentFeeds(ctx context.Context, tx *sql.Tx, data *storedurabledata.Owner, forkRunID, targetBundle string, plan runfork.RunForkPlan, pins []runtimedata.Pin, replay bool) ([]runForkDeploymentProjection, error) {
	sources, parked, err := forkDeploymentSourceMembers(plan)
	if err != nil {
		return nil, err
	}
	overrides := make([]runtimedata.ExplicitPin, 0)
	for _, pin := range pins {
		if pin.Selection == "fork_override" {
			overrides = append(overrides, runtimedata.ExplicitPin{Declaration: pin.Declaration, VersionID: pin.VersionID})
		}
	}
	publications, err := storedurabledata.MaterializeForkFeedPublicationsTx(data, ctx, tx, plan.SourceRunID, forkRunID, targetBundle, sources, parked, overrides, replay)
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
		if legacy[pin.Declaration] == nil && plan.IsParkedDeploymentPin(pin) {
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
