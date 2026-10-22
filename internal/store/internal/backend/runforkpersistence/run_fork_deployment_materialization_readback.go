package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
)

func requireExactMaterializedRunForkDeploymentFeeds(ctx context.Context, tx *sql.Tx, postgres bool, forkRunID, targetBundleHash string, data *storedurabledata.Owner, plan runfork.RunForkPlan, pins []runtimedata.Pin) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, forkRunID).Scan(&count); err != nil {
		return fmt.Errorf("count fork deployment feeds: %w", err)
	}
	if count != len(pins) {
		return fmt.Errorf("fork has %d deployment feeds, want %d exact child pins", count, len(pins))
	}
	if len(pins) != 0 && data == nil {
		return fmt.Errorf("fork deployment readback requires selected durable-data owner")
	}
	sourceByDeclaration := make(map[runtimedata.DeclarationRef]runfork.RunForkFanOutObligation)
	for _, obligation := range plan.FanOutObligations {
		if obligation.Intent.Request.Deployment == nil {
			continue
		}
		declaration := obligation.Intent.Source.Declaration
		if _, duplicate := sourceByDeclaration[declaration]; duplicate {
			return fmt.Errorf("fork source has duplicate deployment feed for %s", declaration.Key())
		}
		sourceByDeclaration[declaration] = obligation
	}
	for _, pin := range pins {
		if pin.RunID != forkRunID {
			return fmt.Errorf("fork deployment pin belongs to another run")
		}
		target, err := storedurabledata.RequirePinnedSourceTx(ctx, data, tx, forkRunID, targetBundleHash, pin.Declaration)
		if err != nil {
			return err
		}
		if target.VersionID != pin.VersionID || target.SchemaDigest != pin.SchemaDigest {
			return fmt.Errorf("fork deployment pin metadata changed for %s", pin.Declaration.Key())
		}
		key, err := loadRunForkChildDeploymentFeedKeyTx(ctx, tx, forkRunID, pin.Declaration)
		if err != nil {
			return err
		}
		var carriage forkDeploymentCarriage
		if source, present := sourceByDeclaration[pin.Declaration]; present {
			carriage, err = projectForkDeploymentCarriage(source, target)
		} else {
			carriage, err = freshForkDeploymentCarriage(target)
		}
		if err != nil {
			return err
		}
		cursor, err := requireRunForkDeploymentFeedRowTx(ctx, tx, key, targetBundleHash, target, carriage)
		if err != nil {
			return err
		}
		if err := requireRunForkDeploymentOutcomesTx(ctx, tx, postgres, key, target, cursor, carriage); err != nil {
			return err
		}
	}
	return nil
}

func requireRunForkDeploymentFeedRowTx(ctx context.Context, tx *sql.Tx, key fanoutobligation.IntentKey, bundleHash string, target runtimedata.PinnedSource, carriage forkDeploymentCarriage) (int, error) {
	var gotBundle, kind, flowPath, eventName, versionID, schemaDigest, status string
	var cardinality, cursor, disjoint int
	var blocked sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT bundle_hash,source_kind,source_resource_flow_path,
		source_resource_event_name,source_resource_version_id,deployment_schema_digest,
		cardinality,cursor,status,blocked_reason,
		CASE WHEN triggering_delivery_id IS NULL AND flow_path IS NULL AND declaration_family IS NULL
			AND semantic_path IS NULL AND semantic_digest IS NULL AND capsule IS NULL THEN 1 ELSE 0 END
		FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment' AND deployment_feed_id=$2`,
		key.RunID, key.DeploymentFeedID).Scan(&gotBundle, &kind, &flowPath, &eventName, &versionID, &schemaDigest,
		&cardinality, &cursor, &status, &blocked, &disjoint)
	if err != nil {
		return 0, fmt.Errorf("read exact fork deployment feed: %w", err)
	}
	if gotBundle != bundleHash || kind != string(fanoutobligation.SourceResourceVersion) || flowPath != target.Declaration.FlowPath ||
		eventName != target.Declaration.EventName || versionID != string(target.VersionID) || schemaDigest != string(target.SchemaDigest) ||
		cardinality != target.RowCount || cursor < carriage.cursor || cursor > cardinality || disjoint != 1 {
		return 0, fmt.Errorf("fork deployment feed contradicts child pin or inherited prefix")
	}
	if carriage.inherit && cursor == carriage.cursor && (status != string(carriage.status) || strings.TrimSpace(blocked.String) != carriage.blockedReason) {
		return 0, fmt.Errorf("fork deployment feed changed its inherited status before progress")
	}
	if !carriage.inherit && cursor == 0 && (status != string(carriage.status) || blocked.Valid) {
		return 0, fmt.Errorf("fork deployment feed changed its fresh initial status before progress")
	}
	if cursor > carriage.cursor && status != string(fanoutobligation.StatusOpen) && status != string(fanoutobligation.StatusClosed) {
		return 0, fmt.Errorf("fork deployment feed progressed into invalid status %q", status)
	}
	return cursor, nil
}

func requireRunForkDeploymentOutcomesTx(ctx context.Context, tx *sql.Tx, postgres bool, key fanoutobligation.IntentKey, target runtimedata.PinnedSource, cursor int, carriage forkDeploymentCarriage) error {
	query := `SELECT ordinal,outcome_kind,event_id,source_event_id,inherited_disposition,failure,created_at
		FROM fan_out_outcomes WHERE run_id=$1 AND deployment_feed_id=$2 ORDER BY ordinal`
	if postgres {
		query = strings.Replace(query, "event_id,source_event_id", "event_id::text,source_event_id::text", 1)
	}
	rows, err := tx.QueryContext(ctx, query, key.RunID, key.DeploymentFeedID)
	if err != nil {
		return fmt.Errorf("read fork deployment outcomes: %w", err)
	}
	defer rows.Close()
	wantInherited := make(map[int]fanoutobligation.Outcome, len(carriage.outcomes))
	for _, outcome := range carriage.outcomes {
		wantInherited[outcome.Ordinal] = outcome
	}
	wantPending := make(map[int]fanoutobligation.Outcome, len(carriage.pendingDeployment))
	for _, pending := range carriage.pendingDeployment {
		if _, duplicate := wantPending[pending.Ordinal]; duplicate {
			return fmt.Errorf("fork deployment repeats pending ordinal %d", pending.Ordinal)
		}
		wantPending[pending.Ordinal] = fanoutobligation.Outcome{
			Ordinal: pending.Ordinal, Kind: fanoutobligation.OutcomeContinued,
			EventID:       activityidentity.ForkLineageEventID(key.RunID, pending.SourceEventID),
			SourceEventID: pending.SourceEventID, InheritedDisposition: pending.InheritedDisposition,
		}
	}
	for ordinal := range wantPending {
		if ordinal < 0 || ordinal >= carriage.cursor {
			return fmt.Errorf("fork deployment pending ordinal %d is outside inherited prefix", ordinal)
		}
		if _, terminal := wantInherited[ordinal]; terminal {
			return fmt.Errorf("fork deployment ordinal %d repeats terminal and pending evidence", ordinal)
		}
	}
	seen := make(map[int]bool)
	bound := make(map[int]runForkContinuedOutcomeRow, len(wantPending))
	for rows.Next() {
		var ordinal int
		var kind string
		var eventID, sourceEventID, disposition sql.NullString
		var failure []byte
		var createdRaw any
		if err := rows.Scan(&ordinal, &kind, &eventID, &sourceEventID, &disposition, &failure, &createdRaw); err != nil {
			return err
		}
		if ordinal < 0 || ordinal >= cursor || ordinal >= target.RowCount || seen[ordinal] {
			return fmt.Errorf("fork deployment has duplicate or out-of-range ordinal %d", ordinal)
		}
		seen[ordinal] = true
		createdAt, present, err := sqliteTimeValue(createdRaw)
		if err != nil || !present {
			return fmt.Errorf("fork deployment ordinal %d has invalid creation time: %v", ordinal, err)
		}
		outcome := fanoutobligation.Outcome{
			Ordinal: ordinal, Kind: fanoutobligation.OutcomeKind(kind), EventID: eventID.String,
			SourceEventID: sourceEventID.String, InheritedDisposition: fanoutobligation.InheritedTerminalDisposition(disposition.String),
			Failure: failure, CreatedAt: createdAt,
		}
		if err := outcome.Validate(); err != nil {
			return fmt.Errorf("fork deployment ordinal %d: %w", ordinal, err)
		}
		if wanted, present := wantInherited[ordinal]; present {
			if kind != string(wanted.Kind) || eventID.Valid || sourceEventID.String != wanted.SourceEventID ||
				disposition.String != string(wanted.InheritedDisposition) || !equalOptionalJSON(failure, wanted.Failure) {
				return fmt.Errorf("fork deployment inherited ordinal %d changed", ordinal)
			}
		} else if wantedReplay, present := wantPending[ordinal]; present {
			if !matchesRunForkFanOutContinuedOutcome(wantedReplay, kind, eventID, sourceEventID, disposition, failure != nil) {
				return fmt.Errorf("fork deployment pending replay ordinal %d changed", ordinal)
			}
			bound[ordinal] = runForkContinuedOutcomeRow{kind: kind, eventID: eventID, sourceEventID: sourceEventID,
				disposition: disposition, failurePresent: failure != nil, createdAt: createdAt}
		} else if ordinal < carriage.cursor || sourceEventID.Valid || disposition.Valid {
			return fmt.Errorf("fork deployment ordinal %d borrowed or lost inherited evidence", ordinal)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for ordinal := range wantInherited {
		if !seen[ordinal] {
			return fmt.Errorf("fork deployment inherited ordinal %d is missing", ordinal)
		}
	}
	for ordinal := carriage.cursor; ordinal < cursor; ordinal++ {
		if !seen[ordinal] {
			return fmt.Errorf("fork deployment child-owned ordinal %d is missing", ordinal)
		}
	}
	for _, pending := range carriage.pendingDeployment {
		row, present := bound[pending.Ordinal]
		if err := requireRunForkContinuedPublicationReadback(ctx, tx, postgres, key.RunID,
			runfork.RunForkPlan{SourceRunID: carriage.sourceRunID}, runfork.RunForkFanOutPendingReplay{
				Ordinal: pending.Ordinal, SourceEventID: pending.SourceEventID, SourceDeliveryIDs: pending.SourceDeliveryIDs,
				InheritedDisposition: pending.InheritedDisposition,
			}, row, present); err != nil {
			return err
		}
	}
	return nil
}
