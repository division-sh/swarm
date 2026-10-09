package eventpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storeapiidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storescenarioexecution "github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
)

type deploymentRunCreationWriter interface {
	CreateRunTx(context.Context, *mutationprotocol.Attempt, runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error)
	CommitFlowInstanceActivationsTx(context.Context, *mutationprotocol.Attempt, []runtimepipeline.FlowInstanceActivationPlan) ([]runtimepipeline.CommittedFlowInstanceActivation, error)
	ReplaceFlowInstanceRouteTopologyTx(context.Context, *sql.Tx, []runtimebus.FlowInstanceRouteRecordSet) ([]runtimebus.FlowInstanceRouteRecordSet, error)
	deploymentFeedWriterTx
	mutationprotocol.CandidateWriter
}

type deploymentFeedWriterTx interface {
	CreateDeploymentFeedTx(context.Context, *sql.Tx, runtimedata.DeploymentFeed) error
}

func commitRunCreationFeedsTx(ctx context.Context, tx *sql.Tx, plan *storedurabledata.RunCreationPlan, writer deploymentFeedWriterTx) error {
	if tx == nil || plan == nil {
		return fmt.Errorf("deployment feed commit requires transaction and run-creation plan")
	}
	if len(plan.DeploymentFeeds()) != 0 && writer == nil {
		return fmt.Errorf("deployment feed owner is required for pinned run creation")
	}
	feeds, err := plan.ConsumeDeploymentFeeds()
	if err != nil {
		return err
	}
	for _, feed := range feeds {
		if err := writer.CreateDeploymentFeedTx(ctx, tx, feed); err != nil {
			return err
		}
	}
	return nil
}

func validateDeploymentRunCreation(ctx context.Context, command runtimedata.RunCreationCommand, request apiidempotency.Request) (runtimedata.RunCreationCommand, runtimecorrelation.SourceArtifactFact, error) {
	_, _, canonical, err := command.RequestHash()
	if err != nil {
		return runtimedata.RunCreationCommand{}, runtimecorrelation.SourceArtifactFact{}, err
	}
	initiation, err := canonical.Initiation()
	if err != nil || initiation != runtimedata.RunCreationFeedOnly || request.Method != "run.start" ||
		request.Actor != apiidempotency.BearerActor(canonical.Actor) ||
		(request.ResourceID != "" && request.ResourceID != canonical.RunID) {
		return runtimedata.RunCreationCommand{}, runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("deployment run creation requires feed-only run.start authority")
	}
	fact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !ok || fact.BundleHash() != canonical.BundleHash {
		return runtimedata.RunCreationCommand{}, runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("deployment run creation requires exact selected bundle source")
	}
	return canonical, fact, nil
}

func commitDeploymentRunCreationTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	owner *storedurabledata.Owner,
	writer deploymentRunCreationWriter,
	command runtimedata.RunCreationCommand,
	root runtimebus.FlowInstanceActivationCommand,
	fact runtimecorrelation.SourceArtifactFact,
	requireReplay bool,
	ensureScenario func(context.Context, *sql.Tx, string, time.Time) error,
) (runtimebus.CommittedDeploymentRunCreation, error) {
	var result runtimebus.CommittedDeploymentRunCreation
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		plan, err := storedurabledata.PrepareRunCreationTx(owner, ctx, tx, command)
		if err != nil {
			return err
		}
		if requireReplay && !plan.Replay() {
			return fmt.Errorf("cached deployment run completion has no permanent run-creation receipt")
		}
		if plan.Replay() || plan.Failed() {
			result.Record, result.Replay = plan.Record(), plan.Replay()
			// The permanent receipt reuses construction. Pending attachment is
			// recovered by its durable readiness attempt, never a fresh plan.
			return runtimedata.ValidateRunCreationReceiptForCommand(result.Record, command)
		}
		if err := storedurabledata.CommitRunCreationImportsTx(owner, ctx, tx, &plan); err != nil {
			return err
		}
		startedAt := runtimerunlifecycle.CanonicalTimestamp(root.Plan.OccurredAt)
		disposition, err := writer.CreateRunTx(ctx, attempt, runtimerunlifecycle.CreateRequest{
			RunID: command.RunID, Origin: runtimerunlifecycle.DeploymentRunOrigin(), Source: fact, StartedAt: startedAt,
		})
		if err != nil {
			return err
		}
		if disposition != runtimerunlifecycle.MutationApplied {
			return fmt.Errorf("deployment run creation found an existing run without its permanent receipt")
		}
		if err := ensureScenario(ctx, tx, command.RunID, startedAt); err != nil {
			return err
		}
		result.Activations, err = writer.CommitFlowInstanceActivationsTx(ctx, attempt, []runtimepipeline.FlowInstanceActivationPlan{root.Plan})
		if err != nil {
			return fmt.Errorf("construct deployment root tree: %w", err)
		}
		if _, err := writer.ReplaceFlowInstanceRouteTopologyTx(ctx, tx, root.RouteTopology); err != nil {
			return fmt.Errorf("stage deployment root routes: %w", err)
		}
		result.Record, err = storedurabledata.CompleteRunCreationTx(owner, ctx, tx, &plan, "", "running")
		if err != nil {
			return err
		}
		if err := commitRunCreationFeedsTx(ctx, tx, &plan, writer); err != nil {
			return err
		}
		if err := recordDeploymentRunFeedFactsTx(ctx, tx, attempt, plan.DeploymentFeeds()); err != nil {
			return err
		}
		allEmpty := true
		for _, feed := range plan.DeploymentFeeds() {
			allEmpty = allEmpty && feed.RowCount == 0
		}
		if allEmpty {
			if _, err := attempt.RequestCompletion(ctx, writer, command.RunID, nil); err != nil {
				return err
			}
		}
		return nil
	})
	return result, err
}

type deploymentFeedFactRecorder interface {
	AddRunStartFacts(context.Context, string, ...runforkrevision.FactRef) error
}

func recordDeploymentRunFeedFactsTx(ctx context.Context, tx *sql.Tx, recorder deploymentFeedFactRecorder, feeds []runtimedata.DeploymentFeed) error {
	if tx == nil || recorder == nil {
		return fmt.Errorf("deployment feed revision requires transaction and mutation attempt")
	}
	if len(feeds) == 0 {
		return nil
	}
	runID := feeds[0].RunID
	refs := make([]runforkrevision.FactRef, 0, len(feeds))
	for _, feed := range feeds {
		if err := feed.Validate(); err != nil {
			return err
		}
		if feed.RunID != runID {
			return fmt.Errorf("deployment feed revision spans multiple runs")
		}
		rows, err := tx.QueryContext(ctx, `SELECT CAST(deployment_feed_id AS TEXT),bundle_hash,
			source_resource_version_id,deployment_schema_digest,cardinality
			FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'
			AND source_resource_flow_path=$2 AND source_resource_event_name=$3`,
			feed.RunID, feed.Declaration.FlowPath, feed.Declaration.EventName)
		if err != nil {
			return fmt.Errorf("read created deployment feed: %w", err)
		}
		var feedID, bundleHash, versionID, schemaDigest string
		var cardinality int64
		if !rows.Next() {
			err := rows.Err()
			_ = rows.Close()
			if err != nil {
				return err
			}
			return fmt.Errorf("created deployment feed for %s is missing", feed.Declaration.Key())
		}
		if err := rows.Scan(&feedID, &bundleHash, &versionID, &schemaDigest, &cardinality); err != nil {
			_ = rows.Close()
			return err
		}
		if rows.Next() {
			_ = rows.Close()
			return fmt.Errorf("created deployment feed for %s is ambiguous", feed.Declaration.Key())
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if bundleHash != feed.BundleHash || versionID != string(feed.VersionID) || schemaDigest != string(feed.SchemaDigest) || cardinality != int64(feed.RowCount) {
			return fmt.Errorf("created deployment feed for %s contradicts its pinned source", feed.Declaration.Key())
		}
		ref, err := runforkrevision.FanOutIntentFact(fanoutobligation.IntentKey{RunID: runID, DeploymentFeedID: feedID})
		if err != nil {
			return err
		}
		refs = append(refs, ref)
	}
	return recorder.AddRunStartFacts(ctx, runID, refs...)
}

func deploymentRunCompletion(record runtimedata.RunCreationOperationRecord) (apiidempotency.Completion, error) {
	if err := record.Validate(); err != nil {
		return apiidempotency.Completion{}, err
	}
	if record.Summary.Outcome != "created" {
		return apiidempotency.Completion{}, fmt.Errorf("rejected run creation has no success completion")
	}
	response, err := json.Marshal(struct {
		RunID       string                  `json:"run_id"`
		Status      string                  `json:"status"`
		DataBinding runtimedata.DataBinding `json:"data_binding"`
	}{RunID: record.Summary.RunID, Status: record.Summary.Status, DataBinding: record.Binding})
	if err != nil {
		return apiidempotency.Completion{}, err
	}
	return apiidempotency.Completion{ResourceID: record.Summary.RunID, Response: response}, nil
}

func validateDeploymentRoot(input runtimebus.FlowInstanceActivationCommand, command runtimedata.RunCreationCommand) error {
	if err := input.Validate(); err != nil {
		return err
	}
	root := input.Plan
	if root.Readiness.RunID != command.RunID || root.Readiness.BundleHash != command.BundleHash ||
		root.Identity.InstanceID != command.RunID || root.Identity.InstancePath != command.RunID || root.Identity.EntityID != command.RunID ||
		root.Identity.ParentEntityID != "" || root.Identity.ParentRoute.FlowID != "" ||
		root.CreatingInput != (runtimepipeline.FlowConstructionInput{}) || root.StandingGenerationReplacement {
		return fmt.Errorf("deployment root construction requires its exact no-argument run owner")
	}
	return nil
}

func acknowledgeDeploymentRunCreation(result runtimebus.CommittedDeploymentRunCreation) runtimebus.CommittedDeploymentRunCreation {
	result.Acknowledged = true
	for index, activation := range result.Activations {
		result.Activations[index] = activation.WithCommitAcknowledgment()
	}
	return result
}

func (s *EventPostgresOwner) CommitDeploymentRunCreation(ctx context.Context, input runtimebus.DeploymentRunCreationCommand) (result runtimebus.CommittedDeploymentRunCreation, err error) {
	request := input.Idempotency
	command, fact, err := validateDeploymentRunCreation(ctx, input.RunCreation, request)
	if err != nil {
		return result, err
	}
	if err := validateDeploymentRoot(input.Root, command); err != nil {
		return result, err
	}
	var lease *storeapiidempotency.PostgresRequestLease
	if strings.TrimSpace(request.IdempotencyKey) != "" {
		lease, err = storeapiidempotency.AcquirePostgresRequest(ctx, s.apiIdempotency, request)
		if err != nil {
			return result, err
		}
		defer func() {
			if releaseErr := lease.Release(ctx); releaseErr != nil {
				err = errors.Join(err, fmt.Errorf("release deployment run idempotency authority: %w", releaseErr))
			}
		}()
	}
	_, cachedReplay := lease.Replay()
	outcome := runPostgresEventMutationResult(ctx, s, true, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimebus.CommittedDeploymentRunCreation, error) {
		result, err := commitDeploymentRunCreationTx(ctx, attempt, s.durableData, s, command, input.Root, fact, cachedReplay, storescenarioexecution.EnsurePostgresFromContext)
		if err != nil || lease == nil || cachedReplay || result.Record.Summary.Outcome != "created" {
			return result, err
		}
		completion, err := deploymentRunCompletion(result.Record)
		if err != nil {
			return result, err
		}
		err = attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return storeapiidempotency.StorePostgresCompletionTx(ctx, lease, tx, completion)
		})
		return result, err
	})
	var acknowledged bool
	result, acknowledged = outcome.Value()
	if !acknowledged {
		return result, outcome.Err()
	}
	result = acknowledgeDeploymentRunCreation(result)
	return result, errors.Join(outcome.Err(), runtimedata.ValidateRunCreationReceiptForCommand(result.Record, command))
}

func (s *EventSQLiteOwner) CommitDeploymentRunCreation(ctx context.Context, input runtimebus.DeploymentRunCreationCommand) (result runtimebus.CommittedDeploymentRunCreation, err error) {
	request := input.Idempotency
	command, fact, err := validateDeploymentRunCreation(ctx, input.RunCreation, request)
	if err != nil {
		return result, err
	}
	if err := validateDeploymentRoot(input.Root, command); err != nil {
		return result, err
	}
	var lease *storeapiidempotency.SQLiteRequestLease
	if strings.TrimSpace(request.IdempotencyKey) != "" {
		lease, err = storeapiidempotency.AcquireSQLiteRequest(ctx, s.apiIdempotency, request)
		if err != nil {
			return result, err
		}
		defer lease.Release()
	}
	_, cachedReplay := lease.Replay()
	outcome := runSQLiteEventMutationResult(ctx, s, "sqlite deployment run creation", true, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimebus.CommittedDeploymentRunCreation, error) {
		result, err := commitDeploymentRunCreationTx(ctx, attempt, s.durableData, s, command, input.Root, fact, cachedReplay, storescenarioexecution.EnsureSQLiteFromContext)
		if err != nil || lease == nil || cachedReplay || result.Record.Summary.Outcome != "created" {
			return result, err
		}
		completion, err := deploymentRunCompletion(result.Record)
		if err != nil {
			return result, err
		}
		err = attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return storeapiidempotency.StoreSQLiteCompletionTx(ctx, lease, tx, completion)
		})
		return result, err
	})
	var acknowledged bool
	result, acknowledged = outcome.Value()
	if !acknowledged {
		return result, outcome.Err()
	}
	result = acknowledgeDeploymentRunCreation(result)
	return result, errors.Join(outcome.Err(), runtimedata.ValidateRunCreationReceiptForCommand(result.Record, command))
}
