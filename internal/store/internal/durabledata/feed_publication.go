package durabledata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/division-sh/swarm/internal/apiidempotency"
	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/google/uuid"
)

func StoreFeedPublicationCompletionTx(o *Owner, ctx context.Context, tx *sql.Tx, publicationID string, request apiidempotency.Request, completion apiidempotency.Completion) error {
	if o == nil || tx == nil || request.Method != "run.start" {
		return fmt.Errorf("rows completion requires its native data transaction and method authority")
	}
	if request.IdempotencyKey != "" && publicationID != runtimedata.RowsPublicationID(request.Actor.ID, request.IdempotencyKey) {
		return fmt.Errorf("rows completion differs from its exact actor and idempotency identity")
	}
	var raw any
	if len(completion.Response) != 0 {
		data, err := json.Marshal(completion)
		if err != nil {
			return err
		}
		raw = data
	}
	result, err := tx.ExecContext(ctx, o.query(`UPDATE resource_feed_publications SET api_request_hash=%s,completion_json=%s
		WHERE publication_id=%s AND api_request_hash=''`, 3), request.RequestHash, raw, publicationID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return errors.Join(err, fmt.Errorf("rows completion lost its exact fresh publication"))
	}
	return nil
}

func (o *Owner) LookupFeedPublicationCompletion(ctx context.Context, request apiidempotency.Request) (apiidempotency.Completion, bool, error) {
	if request.Method != "run.start" || request.IdempotencyKey == "" {
		return apiidempotency.Completion{}, false, nil
	}
	if err := request.Actor.ValidateMethod(request.Method); err != nil {
		return apiidempotency.Completion{}, false, err
	}
	var result apiidempotency.Completion
	found := false
	err := o.runReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var hash string
		var raw, requestJSON []byte
		publicationID := runtimedata.RowsPublicationID(request.Actor.ID, request.IdempotencyKey)
		err := tx.QueryRowContext(ctx, o.query(`SELECT api_request_hash,completion_json,request_json
			FROM resource_feed_publications WHERE publication_id=%s`, 1), publicationID).Scan(&hash, &raw, &requestJSON)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if hash != request.RequestHash {
			return &apiidempotency.ConflictError{Method: request.Method, OriginalRequestHash: hash, ConflictingRequestHash: request.RequestHash}
		}
		found = true
		var command runtimedata.RunCreationCommand
		if err := json.Unmarshal(requestJSON, &command); err != nil {
			return err
		}
		plan, retained, err := FindFeedPublicationTx(o, ctx, tx, command, publicationID)
		if err != nil || !retained || command.Actor != request.Actor.ID {
			return errors.Join(err, fmt.Errorf("retained rows completion contradicts its native publication"))
		}
		record := plan.Record()
		if record.Summary.Outcome != "created" {
			code := runtimedata.CodeRunDataRejected
			if record.Summary.Outcome == "head_conflict" {
				code = runtimedata.CodeRunHeadConflict
			}
			return runtimedata.NewDomainErrorWithDetails(code, map[string]any{"operation": record.Summary}, "rows publication %s", record.Summary.Outcome)
		}
		if err := json.Unmarshal(raw, &result); err != nil || result.ResourceID == "" || len(result.Response) == 0 {
			return errors.Join(err, fmt.Errorf("retained rows completion is missing or contradictory"))
		}
		var response struct {
			PublicationID     string                  `json:"publication_id"`
			RunID             string                  `json:"run_id"`
			EventID           string                  `json:"event_id"`
			DataBinding       runtimedata.DataBinding `json:"data_binding"`
			DeploymentFeedIDs []string                `json:"deployment_feed_ids"`
		}
		if err := json.Unmarshal(result.Response, &response); err != nil {
			return err
		}
		resourceID := command.RunID
		if command.EventID != "" {
			resourceID = command.EventID
		}
		if result.ResourceID != resourceID || response.PublicationID != publicationID || response.RunID != command.RunID ||
			response.EventID != command.EventID || !reflect.DeepEqual(response.DataBinding, record.Binding) ||
			!reflect.DeepEqual(response.DeploymentFeedIDs, record.DeploymentFeedIDs()) {
			return fmt.Errorf("retained rows completion contradicts its committed result")
		}
		return nil
	})
	return result, found, err
}

// Retention is run-owned even when several publications retain the same bytes.
// A prune receipt has one dependency per run/declaration, not one per retry.
const retainedResourcePins = `SELECT run_id,flow_path,event_name,schema_digest,version_id,MIN(selection) AS selection
	FROM (SELECT run_id,flow_path,event_name,schema_digest,version_id,selection FROM resource_version_pins
	UNION ALL SELECT p.run_id,m.flow_path,m.event_name,m.schema_digest,m.version_id,m.selection
	FROM resource_feed_publication_members m JOIN resource_feed_publications p ON p.publication_id=m.publication_id
	WHERE m.version_id IS NOT NULL) retained
	GROUP BY run_id,flow_path,event_name,schema_digest,version_id`

func (o *Owner) LoadFeedPublication(ctx context.Context, publicationID string) (runtimedata.RunCreationOperationRecord, error) {
	if err := o.requireCurrent(); err != nil {
		return runtimedata.RunCreationOperationRecord{}, err
	}
	var record runtimedata.RunCreationOperationRecord
	err := o.runReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var raw []byte
		err := tx.QueryRowContext(ctx, o.query(`SELECT request_json FROM resource_feed_publications WHERE publication_id=%s`, 1), publicationID).Scan(&raw)
		if errors.Is(err, sql.ErrNoRows) {
			return runtimedata.NewDomainError(runtimedata.CodeOperationMissing, "rows publication %s does not exist", publicationID)
		}
		if err != nil {
			return err
		}
		var command runtimedata.RunCreationCommand
		if err := json.Unmarshal(raw, &command); err != nil {
			return err
		}
		plan, found, err := FindFeedPublicationTx(o, ctx, tx, command, publicationID)
		if err != nil || !found {
			return errors.Join(err, fmt.Errorf("retained rows publication disappeared"))
		}
		record = plan.Record()
		binding, err := runtimedata.BindRunCreationRequest(command, record)
		if err != nil {
			return err
		}
		record.RequestBinding = &binding
		return nil
	})
	return record, err
}

func ReadFeedPublicationTx(o *Owner, ctx context.Context, tx *sql.Tx, publicationID string) (runtimedata.RunCreationOperationRecord, bool, error) {
	if o == nil || tx == nil {
		return runtimedata.RunCreationOperationRecord{}, false, fmt.Errorf("publication read requires its native data transaction")
	}
	var raw []byte
	err := tx.QueryRowContext(ctx, o.query(`SELECT request_json FROM resource_feed_publications WHERE publication_id=%s`, 1), publicationID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimedata.RunCreationOperationRecord{}, false, nil
	}
	if err != nil {
		return runtimedata.RunCreationOperationRecord{}, false, err
	}
	var command runtimedata.RunCreationCommand
	if err := json.Unmarshal(raw, &command); err != nil {
		return runtimedata.RunCreationOperationRecord{}, false, err
	}
	plan, found, err := FindFeedPublicationTx(o, ctx, tx, command, publicationID)
	return plan.Record(), found, err
}

func RequireFeedSourceTx(ctx context.Context, o *Owner, tx *sql.Tx, runID, bundleHash, feedID string, ref runtimedata.DeclarationRef) (runtimedata.PinnedSource, error) {
	if o == nil || tx == nil {
		return runtimedata.PinnedSource{}, fmt.Errorf("feed source requires its native data transaction")
	}
	var ownerRun string
	err := tx.QueryRowContext(ctx, o.query(`SELECT p.run_id FROM resource_feed_publication_members m
		JOIN resource_feed_publications p ON p.publication_id=m.publication_id WHERE m.deployment_feed_id=%s`, 1), feedID).Scan(&ownerRun)
	if err == nil {
		if ownerRun != runID {
			return runtimedata.PinnedSource{}, runtimedata.NewDomainError(runtimedata.CodeAccessDenied, "feed belongs to another run")
		}
		return o.publicationSourceTx(ctx, tx, runID, bundleHash, ref, feedID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return runtimedata.PinnedSource{}, err
	}
	var publications int
	if err := tx.QueryRowContext(ctx, o.query(`SELECT COUNT(*) FROM resource_feed_publications WHERE run_id=%s`, 1), runID).Scan(&publications); err != nil {
		return runtimedata.PinnedSource{}, err
	}
	if publications != 0 {
		var request, summary, binding, evidence []byte
		var hash string
		err := tx.QueryRowContext(ctx, o.query(`SELECT request_hash,request_json,summary_json,binding_json,evidence_json
			FROM resource_run_creation_operations WHERE run_id=%s`, 1), runID).Scan(&hash, &request, &summary, &binding, &evidence)
		if err != nil {
			return runtimedata.PinnedSource{}, errors.Join(err, runtimedata.NewDomainError(runtimedata.CodeAccessDenied, "feed publication binding is missing"))
		}
		record, _, err := decodeRunCreationReceipt(runID, hash, request, summary, binding, evidence)
		if err != nil {
			return runtimedata.PinnedSource{}, err
		}
		var found bool
		for _, retained := range record.DeploymentFeedIDs() {
			found = found || retained == feedID
		}
		if !found {
			return runtimedata.PinnedSource{}, runtimedata.NewDomainError(runtimedata.CodeAccessDenied, "feed publication binding is missing")
		}
	}
	return o.pinnedSourceTx(ctx, tx, runID, bundleHash, ref)
}

func (o *Owner) LoadFeedSource(ctx context.Context, runID, bundleHash, feedID string, ref runtimedata.DeclarationRef) (runtimedata.PinnedSource, error) {
	if err := o.requireCurrent(); err != nil {
		return runtimedata.PinnedSource{}, err
	}
	var result runtimedata.PinnedSource
	err := o.runReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = RequireFeedSourceTx(ctx, o, tx, runID, bundleHash, feedID, ref)
		return err
	})
	return result, err
}

// PrepareFeedPublicationTx reuses data admission without acquiring run-creation
// authority. The caller must fence the selected live run in this transaction.
func PrepareFeedPublicationTx(o *Owner, ctx context.Context, tx *sql.Tx, command runtimedata.RunCreationCommand, publicationID string) (RunCreationPlan, error) {
	if o == nil || tx == nil {
		return RunCreationPlan{}, fmt.Errorf("feed publication requires its native data transaction")
	}
	parsed, err := uuid.Parse(publicationID)
	if err != nil || parsed == uuid.Nil || parsed.String() != publicationID {
		return RunCreationPlan{}, fmt.Errorf("feed publication requires a canonical publication identity")
	}
	hash, request, canonical, err := command.RequestHash()
	if err != nil {
		return RunCreationPlan{}, err
	}
	if record, found, err := o.loadFeedPublicationTx(ctx, tx, canonical, publicationID, hash); err != nil {
		return RunCreationPlan{}, err
	} else if found {
		return RunCreationPlan{command: canonical, requestHash: hash, requestJSON: request, publicationID: publicationID,
			record: record, replay: true, failed: record.Summary.Outcome != "created"}, nil
	}
	return prepareRunDataTx(o, ctx, tx, canonical, hash, request, publicationID)
}

func FindFeedPublicationTx(o *Owner, ctx context.Context, tx *sql.Tx, command runtimedata.RunCreationCommand, publicationID string) (RunCreationPlan, bool, error) {
	if o == nil || tx == nil {
		return RunCreationPlan{}, false, fmt.Errorf("feed publication requires its native data transaction")
	}
	hash, request, canonical, err := command.RequestHash()
	if err != nil {
		return RunCreationPlan{}, false, err
	}
	record, found, err := o.loadFeedPublicationTx(ctx, tx, canonical, publicationID, hash)
	return RunCreationPlan{command: canonical, requestHash: hash, requestJSON: request, publicationID: publicationID,
		record: record, replay: found, failed: found && record.Summary.Outcome != "created"}, found, err
}

func (o *Owner) insertFeedPublicationTx(ctx context.Context, tx *sql.Tx, plan RunCreationPlan, record runtimedata.RunCreationOperationRecord) error {
	if err := runtimedata.ValidateRunCreationReceiptForCommand(record, plan.command); err != nil {
		return err
	}
	if err := validatePlannedSourceEvaluations(plan, record); err != nil {
		return err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, o.query(`INSERT INTO resource_feed_publications
		(publication_id,run_id,request_hash,request_json,record_json) VALUES (%s,%s,%s,%s,%s)`, 5),
		plan.publicationID, plan.command.RunID, plan.requestHash, plan.requestJSON, raw); err != nil {
		return err
	}
	for _, item := range plan.command.Data.Imports {
		var evaluation *sourceEvaluation
		for index := range plan.sources {
			if plan.sources[index].command.SourceInvocationID == item.SourceInvocationID {
				evaluation = &plan.sources[index]
				break
			}
		}
		if evaluation == nil {
			return fmt.Errorf("publication import has no exact evaluation")
		}
		var version any
		if record.Summary.Outcome == "created" {
			version = evaluation.compiled.VersionID
		}
		input, err := json.Marshal(evaluation.context)
		if err != nil {
			return err
		}
		feed := runtimedata.DeploymentFeed{RunID: plan.command.RunID, Declaration: item.Declaration, Publication: plan.publicationID}
		if _, err := tx.ExecContext(ctx, o.query(`INSERT INTO resource_feed_publication_members
			(publication_id,flow_path,event_name,deployment_feed_id,source_invocation_id,schema_digest,version_id,selection,evaluation_json)
			VALUES (%s,%s,%s,%s,%s,%s,%s,'fused_import',%s)`, 8), plan.publicationID,
			item.Declaration.FlowPath, item.Declaration.EventName, feed.PublicationID(), item.SourceInvocationID,
			evaluation.declaration.SchemaDigest, version, input); err != nil {
			return err
		}
	}
	if record.Summary.Outcome == "created" {
		for _, pin := range plan.pins {
			if pin.Selection != "explicit" {
				continue
			}
			feed := runtimedata.DeploymentFeed{RunID: plan.command.RunID, Declaration: pin.Declaration, Publication: plan.publicationID}
			if _, err := tx.ExecContext(ctx, o.query(`INSERT INTO resource_feed_publication_members
				(publication_id,flow_path,event_name,deployment_feed_id,schema_digest,version_id,selection)
				VALUES (%s,%s,%s,%s,%s,%s,'explicit')`, 6), plan.publicationID, pin.Declaration.FlowPath,
				pin.Declaration.EventName, feed.PublicationID(), pin.SchemaDigest, pin.VersionID); err != nil {
				return err
			}
		}
	}
	_, found, err := o.loadFeedPublicationTx(ctx, tx, plan.command, plan.publicationID, plan.requestHash)
	if err == nil && !found {
		return fmt.Errorf("committed feed publication is missing")
	}
	return err
}

func (o *Owner) loadFeedPublicationTx(ctx context.Context, tx *sql.Tx, command runtimedata.RunCreationCommand, publicationID, hash string) (runtimedata.RunCreationOperationRecord, bool, error) {
	var runID, storedHash string
	var request, raw []byte
	err := tx.QueryRowContext(ctx, o.query(`SELECT run_id,request_hash,request_json,record_json
		FROM resource_feed_publications WHERE publication_id=%s`, 1), publicationID).Scan(&runID, &storedHash, &request, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimedata.RunCreationOperationRecord{}, false, nil
	}
	if err != nil {
		return runtimedata.RunCreationOperationRecord{}, false, err
	}
	var record runtimedata.RunCreationOperationRecord
	var stored runtimedata.RunCreationCommand
	if err := json.Unmarshal(request, &stored); err != nil {
		return record, false, err
	}
	actualHash, _, _, err := stored.RequestHash()
	if err != nil || actualHash != storedHash || stored.RunID != runID || stored.PublicationID != publicationID {
		return record, false, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "publication request contradicts its retained identity")
	}
	if hash != storedHash || command.RunID != runID {
		return record, false, runtimedata.NewDomainError(runtimedata.CodeInvocationConflict, "publication %s already belongs to a different request", publicationID)
	}
	if err := json.Unmarshal(raw, &record); err != nil {
		return record, false, err
	}
	if err := record.ValidateForCommand(stored); err != nil {
		return record, false, err
	}
	var members int
	if err := tx.QueryRowContext(ctx, o.query(`SELECT COUNT(*) FROM resource_feed_publication_members WHERE publication_id=%s`, 1), publicationID).Scan(&members); err != nil {
		return record, false, err
	}
	wantMembers := record.Summary.PinCount
	if record.Summary.Outcome != "created" {
		wantMembers = len(stored.Data.Imports)
	}
	if members != wantMembers {
		return record, false, fmt.Errorf("publication member set contradicts its retained result")
	}
	for _, item := range record.Evidence.RunBinding {
		if item.Pin == nil {
			continue
		}
		pin := item.Pin
		var digest, version, selection, feedID string
		err := tx.QueryRowContext(ctx, o.query(`SELECT schema_digest,version_id,selection,deployment_feed_id FROM resource_feed_publication_members
			WHERE publication_id=%s AND flow_path=%s AND event_name=%s`, 3), publicationID, pin.Declaration.FlowPath, pin.Declaration.EventName).Scan(&digest, &version, &selection, &feedID)
		feed := runtimedata.DeploymentFeed{RunID: runID, Declaration: pin.Declaration, Publication: publicationID}
		if err != nil || digest != string(pin.SchemaDigest) || version != string(pin.VersionID) || selection != pin.Selection || feedID != feed.PublicationID() {
			return record, false, errors.Join(err, fmt.Errorf("publication pin contradicts its retained result"))
		}
	}
	for _, item := range stored.Data.Imports {
		var contextJSON []byte
		var sourceID, digest, selection, feedID string
		var version sql.NullString
		err := tx.QueryRowContext(ctx, o.query(`SELECT source_invocation_id,evaluation_json,schema_digest,version_id,selection,deployment_feed_id
			FROM resource_feed_publication_members WHERE publication_id=%s AND flow_path=%s AND event_name=%s`, 3),
			publicationID, item.Declaration.FlowPath, item.Declaration.EventName).Scan(&sourceID, &contextJSON, &digest, &version, &selection, &feedID)
		feed := runtimedata.DeploymentFeed{RunID: runID, Declaration: item.Declaration, Publication: publicationID}
		if err != nil || sourceID != item.SourceInvocationID || selection != "fused_import" || feedID != feed.PublicationID() ||
			version.Valid != (record.Summary.Outcome == "created") {
			return record, false, errors.Join(err, fmt.Errorf("publication source custody contradicts its request"))
		}
		var evaluation runtimedata.SourceEvaluationContext
		if err := json.Unmarshal(contextJSON, &evaluation); err != nil {
			return record, false, err
		}
		if evaluation.Declaration.Ref != item.Declaration || string(evaluation.Declaration.SchemaDigest) != digest {
			return record, false, fmt.Errorf("publication source schema contradicts its admitted evaluation")
		}
		child := fusedSourceCommand(stored, item)
		if record.Summary.Outcome == "created" {
			receipt, found, err := o.readStoredSourceReceipt(ctx, tx, sourceID)
			if err != nil || !found {
				return record, false, errors.Join(err, fmt.Errorf("publication source receipt is missing"))
			}
			source, original, err := o.validateStoredSourceReceipt(ctx, tx, sourceID, receipt)
			if err != nil || !reflect.DeepEqual(child, original) || !reflect.DeepEqual(source.Evaluation, evaluation) {
				return record, false, errors.Join(err, fmt.Errorf("publication source receipt contradicts its admitted evaluation"))
			}
		} else {
			projection, defects, err := evaluation.FusedProjection(child)
			if err != nil {
				return record, false, err
			}
			var matched bool
			for _, expected := range record.Evidence.ChildEvaluations {
				matched = matched || reflect.DeepEqual(expected, projection)
			}
			var retained []runtimedata.ValidationDefect
			for _, defect := range record.Evidence.ChildDefects {
				if defect.SourceInvocationID == sourceID {
					retained = append(retained, defect.Defect)
				}
			}
			if !matched || len(defects) != len(retained) || !reflect.DeepEqual(defects, retained) && len(defects) != 0 {
				return record, false, fmt.Errorf("rejected publication lost its original child evaluation")
			}
			if err := o.requireNoSourceCommitFacts(ctx, tx, sourceID); err != nil {
				return record, false, err
			}
		}
	}
	return record, true, nil
}
