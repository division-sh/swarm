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
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storeapiidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	storescenarioexecution "github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
)

type deploymentRunCreationWriter interface {
	CreateRunTx(context.Context, *mutationprotocol.Attempt, runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error)
	storedurabledata.DeploymentFeedWriterTx
	mutationprotocol.CandidateWriter
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
	fact runtimecorrelation.SourceArtifactFact,
	requireReplay bool,
	ensureScenario func(context.Context, *sql.Tx, string, time.Time) error,
) (runtimedata.RunCreationOperationRecord, error) {
	var record runtimedata.RunCreationOperationRecord
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		plan, err := storedurabledata.PrepareRunCreationTx(owner, ctx, tx, command)
		if err != nil {
			return err
		}
		if requireReplay && !plan.Replay() {
			return fmt.Errorf("cached deployment run completion has no permanent run-creation receipt")
		}
		if plan.Replay() || plan.Failed() {
			record = plan.Record()
			return runtimedata.ValidateRunCreationReceiptForCommand(record, command)
		}
		if err := storedurabledata.CommitRunCreationImportsTx(owner, ctx, tx, &plan); err != nil {
			return err
		}
		startedAt := runtimerunlifecycle.CanonicalTimestamp(time.Now())
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
		record, err = storedurabledata.CompleteRunCreationTx(owner, ctx, tx, &plan, "", "running")
		if err != nil {
			return err
		}
		if err := storedurabledata.CommitRunCreationFeedsTx(ctx, tx, &plan, writer); err != nil {
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
	return record, err
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

func (s *EventPostgresOwner) CommitDeploymentRunCreation(ctx context.Context, command runtimedata.RunCreationCommand, request apiidempotency.Request) (record runtimedata.RunCreationOperationRecord, err error) {
	command, fact, err := validateDeploymentRunCreation(ctx, command, request)
	if err != nil {
		return record, err
	}
	var lease *storeapiidempotency.PostgresRequestLease
	if strings.TrimSpace(request.IdempotencyKey) != "" {
		lease, err = storeapiidempotency.AcquirePostgresRequest(ctx, s.apiIdempotency, request)
		if err != nil {
			return record, err
		}
		defer func() {
			if releaseErr := lease.Release(ctx); releaseErr != nil {
				err = errors.Join(err, fmt.Errorf("release deployment run idempotency authority: %w", releaseErr))
			}
		}()
	}
	_, cachedReplay := lease.Replay()
	outcome := runPostgresEventMutationResult(ctx, s, true, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimedata.RunCreationOperationRecord, error) {
		record, err := commitDeploymentRunCreationTx(ctx, attempt, s.durableData, s, command, fact, cachedReplay, storescenarioexecution.EnsurePostgresFromContext)
		if err != nil || lease == nil || cachedReplay || record.Summary.Outcome != "created" {
			return record, err
		}
		completion, err := deploymentRunCompletion(record)
		if err != nil {
			return record, err
		}
		err = attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return storeapiidempotency.StorePostgresCompletionTx(ctx, lease, tx, completion)
		})
		return record, err
	})
	var acknowledged bool
	record, acknowledged = outcome.Value()
	if !acknowledged {
		return record, outcome.Err()
	}
	return record, errors.Join(outcome.Err(), runtimedata.ValidateRunCreationReceiptForCommand(record, command))
}

func (s *EventSQLiteOwner) CommitDeploymentRunCreation(ctx context.Context, command runtimedata.RunCreationCommand, request apiidempotency.Request) (record runtimedata.RunCreationOperationRecord, err error) {
	command, fact, err := validateDeploymentRunCreation(ctx, command, request)
	if err != nil {
		return record, err
	}
	var lease *storeapiidempotency.SQLiteRequestLease
	if strings.TrimSpace(request.IdempotencyKey) != "" {
		lease, err = storeapiidempotency.AcquireSQLiteRequest(ctx, s.apiIdempotency, request)
		if err != nil {
			return record, err
		}
		defer lease.Release()
	}
	_, cachedReplay := lease.Replay()
	outcome := runSQLiteEventMutationResult(ctx, s, "sqlite deployment run creation", true, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimedata.RunCreationOperationRecord, error) {
		record, err := commitDeploymentRunCreationTx(ctx, attempt, s.durableData, s, command, fact, cachedReplay, storescenarioexecution.EnsureSQLiteFromContext)
		if err != nil || lease == nil || cachedReplay || record.Summary.Outcome != "created" {
			return record, err
		}
		completion, err := deploymentRunCompletion(record)
		if err != nil {
			return record, err
		}
		err = attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return storeapiidempotency.StoreSQLiteCompletionTx(ctx, lease, tx, completion)
		})
		return record, err
	})
	var acknowledged bool
	record, acknowledged = outcome.Value()
	if !acknowledged {
		return record, outcome.Err()
	}
	return record, errors.Join(outcome.Err(), runtimedata.ValidateRunCreationReceiptForCommand(record, command))
}
