package durabledata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
)

// MaterializeForkFeedPublicationsTx retains each fixed-cut feed's own version.
// Several publications of one declaration never collapse into a default pin.
func MaterializeForkFeedPublicationsTx(o *Owner, ctx context.Context, tx *sql.Tx, sourceRunID, forkRunID, targetBundle string, sources map[string]runtimedata.DeploymentFeed, overrides []runtimedata.ExplicitPin, replay bool) (map[string]runtimedata.DeploymentFeed, error) {
	if len(sources) != 0 && (o == nil || tx == nil) {
		return nil, fmt.Errorf("fork publications require their native data transaction")
	}
	groups := make(map[string]map[string]runtimedata.DeploymentFeed)
	for feedID, source := range sources {
		var group string
		err := tx.QueryRowContext(ctx, o.query(`SELECT p.publication_id FROM resource_feed_publication_members m
			JOIN resource_feed_publications p ON p.publication_id=m.publication_id
			WHERE p.run_id=%s AND m.deployment_feed_id=%s`, 2), sourceRunID, feedID).Scan(&group)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("fork publication lost its exact source member: %w", err)
		}
		source.Publication = group
		pinned, err := RequireFeedSourceTx(ctx, o, tx, sourceRunID, source.BundleHash, feedID, source.Declaration)
		if err != nil || source.PublicationID() != feedID || pinned.VersionID != source.VersionID || pinned.SchemaDigest != source.SchemaDigest || uint64(pinned.RowCount) != source.RowCount {
			return nil, fmt.Errorf("fork publication contradicts its fixed source feed: %w", err)
		}
		if groups[group] == nil {
			groups[group] = make(map[string]runtimedata.DeploymentFeed)
		}
		groups[group][feedID] = source
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	projected := make(map[string]runtimedata.DeploymentFeed, len(sources))
	for _, key := range keys {
		group, err := o.materializeForkFeedPublicationGroupTx(ctx, tx, sourceRunID, forkRunID, targetBundle, key, groups[key], overrides, replay)
		if err != nil {
			return nil, err
		}
		for sourceID, feed := range group {
			projected[sourceID] = feed
		}
	}
	return projected, nil
}

func (o *Owner) materializeForkFeedPublicationGroupTx(ctx context.Context, tx *sql.Tx, sourceRunID, forkRunID, targetBundle, key string, sources map[string]runtimedata.DeploymentFeed, overrides []runtimedata.ExplicitPin, replay bool) (map[string]runtimedata.DeploymentFeed, error) {
	original, found, err := ReadFeedPublicationTx(o, ctx, tx, key)
	if err != nil || !found || original.Summary.RunID != sourceRunID || len(original.DeploymentFeedIDs()) != len(sources) {
		return nil, fmt.Errorf("fork publication does not contain its complete source member set: %w", err)
	}
	group := runtimedata.ForkRowsPublicationID(forkRunID, key)
	command := runtimedata.RunCreationCommand{RunID: forkRunID, BundleHash: targetBundle, Actor: "run.fork", PublicationID: group}
	seen := make(map[runtimedata.DeclarationRef]bool)
	for _, source := range sources {
		if seen[source.Declaration] {
			return nil, fmt.Errorf("fork publication repeats a declaration")
		}
		seen[source.Declaration] = true
		pin := runtimedata.ExplicitPin{Declaration: source.Declaration, VersionID: source.VersionID}
		for _, override := range overrides {
			if override.Declaration == pin.Declaration {
				pin.VersionID = override.VersionID
			}
		}
		command.Data.Pins = append(command.Data.Pins, pin)
	}
	plan, err := PrepareFeedPublicationTx(o, ctx, tx, command, group)
	if err != nil || plan.Failed() || plan.Replay() != replay {
		return nil, fmt.Errorf("fork publication conflicts with its native target binding: %w", err)
	}
	if !replay {
		if err := CommitRunCreationImportsTx(o, ctx, tx, &plan); err != nil {
			return nil, err
		}
		if _, err := plan.ConsumeDeploymentFeeds(); err != nil {
			return nil, err
		}
		if _, err := CompleteRunCreationTx(o, ctx, tx, &plan, "", "paused"); err != nil {
			return nil, err
		}
	}
	projected := make(map[string]runtimedata.DeploymentFeed, len(sources))
	for _, source := range sources {
		feed := runtimedata.DeploymentFeed{RunID: forkRunID, BundleHash: targetBundle, Declaration: source.Declaration, Publication: group}
		target, err := RequireFeedSourceTx(ctx, o, tx, forkRunID, targetBundle, feed.PublicationID(), feed.Declaration)
		if err != nil {
			return nil, err
		}
		feed.VersionID, feed.SchemaDigest, feed.RowCount = target.VersionID, target.SchemaDigest, uint64(target.RowCount)
		projected[source.PublicationID()] = feed
	}
	return projected, nil
}
