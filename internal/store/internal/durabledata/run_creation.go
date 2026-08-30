package durabledata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
)

// RunCreationPlan is the typed handoff between durable data admission and the
// existing selected-store event transaction. Its internals are deliberately
// opaque outside this owner package.
type RunCreationPlan struct {
	command     runtimedata.RunCreationCommand
	requestHash string
	requestJSON []byte
	record      runtimedata.RunCreationOperationRecord
	replay      bool
	failed      bool
	sources     []sourceEvaluation
	pins        []runtimedata.Pin
	committed   bool
}

func (p RunCreationPlan) Replay() bool { return p.replay }
func (p RunCreationPlan) Failed() bool { return p.failed }
func (p RunCreationPlan) Record() runtimedata.RunCreationOperationRecord {
	return p.record
}

func PrepareRunCreationTx(o *Owner, ctx context.Context, tx *sql.Tx, command runtimedata.RunCreationCommand) (RunCreationPlan, error) {
	if o == nil {
		return RunCreationPlan{}, fmt.Errorf("durable data owner is required")
	}
	if tx == nil {
		return RunCreationPlan{}, fmt.Errorf("run creation transaction is required")
	}
	hash, requestJSON, canonical, err := command.RequestHash()
	if err != nil {
		return RunCreationPlan{}, err
	}
	if replay, found, err := o.loadRunCreationReceiptTx(ctx, tx, canonical.RunID, hash); err != nil {
		return RunCreationPlan{}, err
	} else if found {
		return RunCreationPlan{
			command: canonical, requestHash: hash, requestJSON: requestJSON,
			record: replay, replay: true, failed: replay.Summary.Outcome != "created",
		}, nil
	}
	if exists, err := o.runExistsTx(ctx, tx, canonical.RunID); err != nil {
		return RunCreationPlan{}, err
	} else if exists {
		return RunCreationPlan{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "run %s exists without its permanent run-creation receipt", canonical.RunID)
	}

	var declarations []runtimedata.Declaration
	if len(canonical.Data.Imports) > 0 || len(canonical.Data.Pins) > 0 {
		declarations, err = o.loadBundleDeclarationsTx(ctx, tx, canonical.BundleHash)
		if err != nil {
			return RunCreationPlan{}, err
		}
	}
	plan := RunCreationPlan{command: canonical, requestHash: hash, requestJSON: requestJSON}
	imports := make(map[string]runtimedata.FusedImport, len(canonical.Data.Imports))
	for _, item := range canonical.Data.Imports {
		imports[item.Declaration.Key()] = item
	}
	explicit := make(map[string]runtimedata.ExplicitPin, len(canonical.Data.Pins))
	for _, item := range canonical.Data.Pins {
		explicit[item.Declaration.Key()] = item
	}
	known := make(map[string]runtimedata.Declaration, len(declarations))
	for _, declaration := range declarations {
		known[declaration.Ref.Key()] = declaration
	}
	for key := range imports {
		if _, ok := known[key]; !ok {
			return RunCreationPlan{}, runtimedata.NewDomainError(runtimedata.CodePinConflict, "fused import selects declaration %s outside exact bundle", key)
		}
	}
	for key := range explicit {
		if _, ok := known[key]; !ok {
			return RunCreationPlan{}, runtimedata.NewDomainError(runtimedata.CodePinConflict, "explicit pin selects declaration %s outside exact bundle", key)
		}
	}

	validationRejected := false
	headConflict := false
	for _, item := range canonical.Data.Imports {
		if err := o.requireUnusedFusedChildTx(ctx, tx, item.SourceInvocationID); err != nil {
			return RunCreationPlan{}, err
		}
	}
	for _, item := range canonical.Data.Imports {
		declaration := known[item.Declaration.Key()]
		command := runtimedata.SourceCommand{
			Operation: "import", SourceInvocationID: item.SourceInvocationID, ParentRunID: canonical.RunID,
			Actor: canonical.Actor, BundleHash: canonical.BundleHash, Declaration: item.Declaration,
			ExpectedHead: item.ExpectedHead, InputFormat: item.InputFormat, Input: item.Input,
		}
		evaluation, err := o.evaluateSourceTx(ctx, tx, command, declaration)
		if err != nil {
			return RunCreationPlan{}, err
		}
		plan.sources = append(plan.sources, evaluation)
		switch evaluation.result.Outcome {
		case "validation_rejected":
			validationRejected = true
		case "head_conflict":
			headConflict = true
		case "accepted":
		default:
			return RunCreationPlan{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "fused child %s has invalid outcome %q", command.SourceInvocationID, evaluation.result.Outcome)
		}
	}
	if validationRejected {
		return o.rejectRunCreationTx(ctx, tx, plan, "data_rejected", runtimedata.RunCreationRejection{
			State: "rejected", Code: runtimedata.RunCreationRejectionFusedValidation,
		})
	}
	if headConflict {
		return o.rejectRunCreationTx(ctx, tx, plan, "head_conflict", runtimedata.RunCreationRejection{
			State: "rejected", Code: runtimedata.RunCreationRejectionFusedHead,
		})
	}
	for _, pin := range canonical.Data.Pins {
		key := pin.Declaration.Key()
		declaration := known[key]
		declarationRef := declaration.Ref
		version, found, err := o.loadStoredVersion(ctx, tx, declaration.Ref, pin.VersionID)
		if err != nil {
			return RunCreationPlan{}, err
		}
		if !found {
			return o.rejectRunCreationTx(ctx, tx, plan, "data_rejected", runtimedata.RunCreationRejection{
				State: "rejected", Code: runtimedata.RunCreationRejectionVersionMissing,
				Declaration: &declarationRef, VersionID: pin.VersionID,
			})
		}
		if version.PrunedAt != nil {
			return o.rejectRunCreationTx(ctx, tx, plan, "data_rejected", runtimedata.RunCreationRejection{
				State: "rejected", Code: runtimedata.RunCreationRejectionVersionPruned,
				Declaration: &declarationRef, VersionID: version.VersionID,
			})
		}
		if version.Manifest.SchemaDigest != declaration.SchemaDigest {
			return o.rejectRunCreationTx(ctx, tx, plan, "data_rejected", runtimedata.RunCreationRejection{
				State: "rejected", Code: runtimedata.RunCreationRejectionSchemaMismatch,
				Declaration: &declarationRef, VersionID: version.VersionID,
				ExpectedSchemaDigest: declaration.SchemaDigest, SelectedSchemaDigest: version.Manifest.SchemaDigest,
			})
		}
		plan.pins = append(plan.pins, runtimedata.Pin{
			RunID: canonical.RunID, RunState: "running", Declaration: declaration.Ref,
			SchemaDigest: declaration.SchemaDigest, VersionID: version.VersionID, Selection: "explicit",
		})
	}
	for _, evaluation := range plan.sources {
		plan.pins = append(plan.pins, runtimedata.Pin{
			RunID: canonical.RunID, RunState: "running", Declaration: evaluation.command.Declaration,
			SchemaDigest: evaluation.declaration.SchemaDigest, VersionID: evaluation.compiled.VersionID, Selection: "fused_import",
		})
	}
	runtimedata.SortPins(plan.pins)
	return plan, nil
}

func (o *Owner) rejectRunCreationTx(ctx context.Context, tx *sql.Tx, plan RunCreationPlan, outcome string, rejection runtimedata.RunCreationRejection) (RunCreationPlan, error) {
	record := runtimedata.RunCreationOperationRecord{
		Summary: runtimedata.RunCreationOperationSummary{
			Kind: "run_creation", Outcome: outcome, RunID: plan.command.RunID, BundleHash: plan.command.BundleHash,
			PinCount: 0, ImportCount: len(plan.command.Data.Imports), Rejection: rejection, CompletedAt: o.currentTime(),
		},
		Binding: runtimedata.DataBinding{State: "none"},
	}
	for _, source := range plan.sources {
		evaluation := fusedChildEvaluation(source)
		record.Evidence.ChildEvaluations = append(record.Evidence.ChildEvaluations, evaluation)
		for _, defect := range source.evidence.Defects {
			record.Evidence.ChildDefects = append(record.Evidence.ChildDefects, runtimedata.FusedChildDefect{
				SourceInvocationID: source.command.SourceInvocationID, Defect: defect,
			})
		}
	}
	if err := o.insertRunCreationReceiptTx(ctx, tx, plan, record); err != nil {
		return RunCreationPlan{}, err
	}
	for _, source := range plan.sources {
		hash, _, err := source.command.RequestHash()
		if err != nil {
			return RunCreationPlan{}, err
		}
		if _, err := tx.ExecContext(ctx, o.query(`
			INSERT INTO resource_run_creation_child_reservations
			(source_invocation_id, parent_run_id, request_hash, reserved_at) VALUES (%s, %s, %s, %s)
		`, 4), source.command.SourceInvocationID, plan.command.RunID, hash, record.Summary.CompletedAt); err != nil {
			return RunCreationPlan{}, fmt.Errorf("reserve failed fused child: %w", err)
		}
		contextJSON, _ := json.Marshal(source.context)
		evaluationJSON, _ := json.Marshal(fusedChildEvaluation(source))
		defectsJSON, _ := json.Marshal(source.evidence.Defects)
		if _, err := tx.ExecContext(ctx, o.query(`
			INSERT INTO resource_run_creation_child_evaluations
			(parent_run_id, source_invocation_id, observed_head_revision, context_json, evaluation_json, defects_json)
			VALUES (%s, %s, %s, %s, %s, %s)
		`, 6), plan.command.RunID, source.command.SourceInvocationID, source.context.Base.Head.Revision, contextJSON, evaluationJSON, defectsJSON); err != nil {
			return RunCreationPlan{}, fmt.Errorf("store failed fused child evaluation: %w", err)
		}
	}
	stored, found, err := o.readStoredRunCreationReceipt(ctx, tx, plan.command.RunID)
	if err != nil {
		return RunCreationPlan{}, fmt.Errorf("read inserted failed run-creation receipt: %w", err)
	}
	if !found {
		return RunCreationPlan{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "inserted run creation operation %s is missing", plan.command.RunID)
	}
	if _, _, err := o.validateStoredRunCreationReceipt(ctx, tx, plan.command.RunID, stored, true); err != nil {
		return RunCreationPlan{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "inserted run creation operation %s is contradictory: %v", plan.command.RunID, err)
	}
	plan.record = record
	plan.failed = true
	return plan, nil
}

func fusedChildEvaluation(source sourceEvaluation) runtimedata.FusedChildEvaluation {
	outcome := source.result.Outcome
	if outcome == "accepted" {
		outcome = "ready"
	}
	return runtimedata.FusedChildEvaluation{
		SourceInvocationID: source.command.SourceInvocationID, BundleHash: source.command.BundleHash,
		SchemaDigest: source.declaration.SchemaDigest, Declaration: source.command.Declaration,
		ExpectedHead: source.command.ExpectedHead, ObservedHead: source.result.ObservedHead,
		Candidate: source.result.Candidate, Delta: source.result.Delta,
		DefectCount: len(source.evidence.Defects), Outcome: outcome,
	}
}

func CommitRunCreationImportsTx(o *Owner, ctx context.Context, tx *sql.Tx, plan *RunCreationPlan) error {
	if o == nil {
		return fmt.Errorf("durable data owner is required")
	}
	if plan == nil || plan.replay || plan.failed || plan.committed {
		return fmt.Errorf("run creation import commit requires one fresh ready plan")
	}
	for index := range plan.sources {
		if err := o.commitSourceEvaluationTx(ctx, tx, &plan.sources[index]); err != nil {
			return err
		}
		hash, requestJSON, err := plan.sources[index].command.RequestHash()
		if err != nil {
			return err
		}
		if err := o.insertSourceReceipt(ctx, tx, plan.sources[index].command, hash, requestJSON, plan.sources[index]); err != nil {
			return err
		}
	}
	plan.committed = true
	return nil
}

func CompleteRunCreationTx(o *Owner, ctx context.Context, tx *sql.Tx, plan *RunCreationPlan, eventID, status string) (runtimedata.RunCreationOperationRecord, error) {
	if o == nil {
		return runtimedata.RunCreationOperationRecord{}, fmt.Errorf("durable data owner is required")
	}
	if plan == nil || plan.replay || plan.failed || !plan.committed || eventID != plan.command.EventID || strings.TrimSpace(status) == "" {
		return runtimedata.RunCreationOperationRecord{}, fmt.Errorf("run creation completion requires one matching committed plan and event")
	}
	now := o.currentTime()
	for _, pin := range plan.pins {
		if _, err := tx.ExecContext(ctx, o.query(`
			INSERT INTO resource_version_pins
			(run_id, flow_path, event_name, schema_digest, version_id, selection, pinned_at)
			VALUES (%s, %s, %s, %s, %s, %s, %s)
		`, 7), pin.RunID, pin.Declaration.FlowPath, pin.Declaration.EventName, pin.SchemaDigest, pin.VersionID, pin.Selection, now); err != nil {
			return runtimedata.RunCreationOperationRecord{}, fmt.Errorf("insert resource version pin: %w", err)
		}
	}
	items := make([]runtimedata.RunCreationDataItem, 0, len(plan.pins)+len(plan.sources))
	imports := make(map[string]runtimedata.SourceOperationSummary, len(plan.sources))
	for _, source := range plan.sources {
		imports[source.command.Declaration.Key()] = runtimedata.SummarizeSource(runtimedata.SourceOperationRecord{Evaluation: source.context, Result: source.result, Evidence: source.evidence})
	}
	for index := range plan.pins {
		pin := plan.pins[index]
		items = append(items, runtimedata.RunCreationDataItem{Kind: "pin", Pin: &pin})
		if summary, ok := imports[pin.Declaration.Key()]; ok {
			copy := summary
			items = append(items, runtimedata.RunCreationDataItem{Kind: "import", Import: &copy})
		}
	}
	binding := runtimedata.DataBinding{State: "none"}
	if len(plan.pins) > 0 {
		page := pageRunCreationItems(items)
		binding = runtimedata.DataBinding{
			State: "bound", RunID: plan.command.RunID, PinCount: len(plan.pins), ImportCount: len(plan.sources), Evidence: &page,
		}
	}
	record := runtimedata.RunCreationOperationRecord{
		Summary: runtimedata.RunCreationOperationSummary{
			Kind: "run_creation", Outcome: "created", RunID: plan.command.RunID, BundleHash: plan.command.BundleHash,
			EventID: eventID, Status: status, PinCount: len(plan.pins), ImportCount: len(plan.sources),
			Rejection: runtimedata.NoRunCreationRejection(), CompletedAt: now,
		},
		Binding:  binding,
		Evidence: runtimedata.RunCreationEvidence{RunBinding: items},
	}
	if err := o.insertRunCreationReceiptTx(ctx, tx, *plan, record); err != nil {
		return runtimedata.RunCreationOperationRecord{}, err
	}
	stored, found, err := o.readStoredRunCreationReceipt(ctx, tx, plan.command.RunID)
	if err != nil {
		return runtimedata.RunCreationOperationRecord{}, fmt.Errorf("read inserted run-creation receipt: %w", err)
	}
	if !found {
		return runtimedata.RunCreationOperationRecord{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "inserted run creation operation %s is missing", plan.command.RunID)
	}
	if _, _, err := o.validateStoredRunCreationReceipt(ctx, tx, plan.command.RunID, stored, true); err != nil {
		return runtimedata.RunCreationOperationRecord{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "inserted run creation operation %s is contradictory: %v", plan.command.RunID, err)
	}
	plan.record = record
	return record, nil
}

func pageRunCreationItems(all []runtimedata.RunCreationDataItem) runtimedata.PageResult[runtimedata.RunCreationDataItem] {
	return runtimedata.FirstEvidencePage(all)
}

func (o *Owner) insertRunCreationReceiptTx(ctx context.Context, tx *sql.Tx, plan RunCreationPlan, record runtimedata.RunCreationOperationRecord) error {
	if err := record.ValidateForCommand(plan.command); err != nil {
		return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "run creation operation %s is contradictory: %v", plan.command.RunID, err)
	}
	if err := validatePlannedSourceEvaluations(plan, record); err != nil {
		return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "run creation operation %s source evaluation is contradictory: %v", plan.command.RunID, err)
	}
	summaryJSON, err := json.Marshal(record.Summary)
	if err != nil {
		return err
	}
	bindingJSON, err := json.Marshal(record.Binding)
	if err != nil {
		return err
	}
	evidenceJSON, err := json.Marshal(record.Evidence)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, o.query(`
		INSERT INTO resource_run_creation_operations
		(run_id, request_hash, actor, bundle_hash, request_json, summary_json, binding_json, evidence_json, completed_at)
		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
	`, 9), plan.command.RunID, plan.requestHash, plan.command.Actor, plan.command.BundleHash, plan.requestJSON,
		summaryJSON, bindingJSON, evidenceJSON, record.Summary.CompletedAt)
	if err != nil {
		return err
	}
	stored, found, err := o.readStoredRunCreationReceipt(ctx, tx, plan.command.RunID)
	if err != nil {
		return fmt.Errorf("read inserted run-creation receipt: %w", err)
	}
	if !found {
		return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "inserted run creation operation %s is missing", plan.command.RunID)
	}
	if _, _, err := o.validateStoredRunCreationReceipt(ctx, tx, plan.command.RunID, stored, false); err != nil {
		return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "inserted run creation operation %s is contradictory: %v", plan.command.RunID, err)
	}
	return nil
}

func (o *Owner) loadRunCreationReceiptTx(ctx context.Context, tx *sql.Tx, runID, requestHash string) (runtimedata.RunCreationOperationRecord, bool, error) {
	stored, found, err := o.readStoredRunCreationReceipt(ctx, tx, runID)
	if err != nil || !found {
		if err != nil {
			return runtimedata.RunCreationOperationRecord{}, false, err
		}
		return runtimedata.RunCreationOperationRecord{}, false, nil
	}
	if stored.requestHash != requestHash {
		return runtimedata.RunCreationOperationRecord{}, false, runtimedata.NewDomainError(runtimedata.CodeInvocationConflict, "run_id %s was already used for a different run-creation request", runID)
	}
	record, _, err := o.validateStoredRunCreationReceipt(ctx, tx, runID, stored, true)
	if err != nil {
		return runtimedata.RunCreationOperationRecord{}, false, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "run creation operation %s evidence is contradictory: %v", runID, err)
	}
	return record, true, nil
}

func (o *Owner) LoadRunCreationOperation(ctx context.Context, runID string) (runtimedata.RunCreationOperationRecord, error) {
	if err := o.requireCurrent(); err != nil {
		return runtimedata.RunCreationOperationRecord{}, err
	}
	var record runtimedata.RunCreationOperationRecord
	err := o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		stored, found, err := o.readStoredRunCreationReceipt(txctx, tx, runID)
		if err != nil {
			return err
		}
		if !found {
			return runtimedata.NewDomainError(runtimedata.CodeOperationMissing, "run creation operation %s does not exist", runID)
		}
		record, _, err = o.validateStoredRunCreationReceipt(txctx, tx, runID, stored, true)
		if err != nil {
			return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "run creation operation %s evidence is contradictory: %v", runID, err)
		}
		return nil
	})
	return record, err
}

func decodeRunCreationReceipt(runID, storedHash string, requestJSON, summaryJSON, bindingJSON, evidenceJSON []byte) (runtimedata.RunCreationOperationRecord, runtimedata.RunCreationCommand, error) {
	var command runtimedata.RunCreationCommand
	var record runtimedata.RunCreationOperationRecord
	if err := json.Unmarshal(requestJSON, &command); err != nil {
		return runtimedata.RunCreationOperationRecord{}, runtimedata.RunCreationCommand{}, fmt.Errorf("decode run-creation request: %w", err)
	}
	computedHash, canonicalRequest, canonical, err := command.RequestHash()
	if err != nil || computedHash != storedHash || !jsonEqual(canonicalRequest, requestJSON) || canonical.RunID != runID {
		return runtimedata.RunCreationOperationRecord{}, runtimedata.RunCreationCommand{}, fmt.Errorf("run-creation request identity or hash is contradictory")
	}
	if err := json.Unmarshal(summaryJSON, &record.Summary); err != nil {
		return runtimedata.RunCreationOperationRecord{}, runtimedata.RunCreationCommand{}, err
	}
	if err := json.Unmarshal(bindingJSON, &record.Binding); err != nil {
		return runtimedata.RunCreationOperationRecord{}, runtimedata.RunCreationCommand{}, err
	}
	if err := json.Unmarshal(evidenceJSON, &record.Evidence); err != nil {
		return runtimedata.RunCreationOperationRecord{}, runtimedata.RunCreationCommand{}, err
	}
	if err := record.ValidateForCommand(canonical); err != nil {
		return runtimedata.RunCreationOperationRecord{}, runtimedata.RunCreationCommand{}, err
	}
	return record, canonical, nil
}

func validatePlannedSourceEvaluations(plan RunCreationPlan, record runtimedata.RunCreationOperationRecord) error {
	for _, source := range plan.sources {
		if record.Summary.Outcome == "created" {
			sourceRecord := runtimedata.SourceOperationRecord{Evaluation: source.context, Result: source.result, Evidence: source.evidence}
			if err := sourceRecord.ValidateForCommand(source.command); err != nil {
				return err
			}
			continue
		}
		projection, defects, err := source.context.FusedProjection(source.command)
		if err != nil || !reflect.DeepEqual(projection, fusedChildEvaluation(source)) || !reflect.DeepEqual(defects, source.evidence.Defects) {
			return fmt.Errorf("fused child %s projection contradicts canonical evaluation", source.command.SourceInvocationID)
		}
	}
	return nil
}

func (o *Owner) validateStoredRunCreationSourcesTx(ctx context.Context, tx *sql.Tx, command runtimedata.RunCreationCommand, record runtimedata.RunCreationOperationRecord) error {
	imports := make(map[string]runtimedata.FusedImport, len(command.Data.Imports))
	for _, item := range command.Data.Imports {
		imports[item.SourceInvocationID] = item
	}
	if record.Summary.Outcome == "created" {
		summaries := make(map[string]runtimedata.SourceOperationSummary, len(command.Data.Imports))
		for _, item := range record.Evidence.RunBinding {
			if item.Kind == "import" && item.Import != nil {
				summaries[item.Import.SourceInvocationID] = *item.Import
			}
		}
		for _, item := range command.Data.Imports {
			sourceCommand := fusedSourceCommand(command, item)
			sourceRecord, found, err := o.loadSourceRecordTx(ctx, tx, sourceCommand)
			if err != nil || !found {
				if err != nil {
					return err
				}
				return fmt.Errorf("committed fused child %s has no source receipt", item.SourceInvocationID)
			}
			if !reflect.DeepEqual(summaries[item.SourceInvocationID], runtimedata.SummarizeSource(sourceRecord)) {
				return fmt.Errorf("committed fused child %s summary contradicts source receipt", item.SourceInvocationID)
			}
		}
		return nil
	}
	rows, err := tx.QueryContext(ctx, o.query(`
		SELECT source_invocation_id, observed_head_revision, context_json, evaluation_json, defects_json
		FROM resource_run_creation_child_evaluations WHERE parent_run_id = %s ORDER BY source_invocation_id
	`, 1), command.RunID)
	if err != nil {
		return err
	}
	type storedChildEvaluation struct {
		id                                       string
		observedHeadRevision                     uint64
		contextJSON, evaluationJSON, defectsJSON []byte
	}
	var storedChildren []storedChildEvaluation
	for rows.Next() {
		var child storedChildEvaluation
		if err := rows.Scan(&child.id, &child.observedHeadRevision, &child.contextJSON, &child.evaluationJSON, &child.defectsJSON); err != nil {
			_ = rows.Close()
			return err
		}
		storedChildren = append(storedChildren, child)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(imports))
	parentEvaluations := make(map[string]runtimedata.FusedChildEvaluation, len(record.Evidence.ChildEvaluations))
	parentDefects := make(map[string][]runtimedata.ValidationDefect, len(record.Evidence.ChildEvaluations))
	for _, item := range record.Evidence.ChildEvaluations {
		parentEvaluations[item.SourceInvocationID] = item
	}
	for _, item := range record.Evidence.ChildDefects {
		parentDefects[item.SourceInvocationID] = append(parentDefects[item.SourceInvocationID], item.Defect)
	}
	for _, child := range storedChildren {
		id := child.id
		item, ok := imports[id]
		if !ok {
			return fmt.Errorf("stored fused child %s is absent from parent request", id)
		}
		var evaluationContext runtimedata.SourceEvaluationContext
		var projection runtimedata.FusedChildEvaluation
		var defects []runtimedata.ValidationDefect
		if json.Unmarshal(child.contextJSON, &evaluationContext) != nil || json.Unmarshal(child.evaluationJSON, &projection) != nil || json.Unmarshal(child.defectsJSON, &defects) != nil {
			return fmt.Errorf("stored fused child %s evidence is not decodable", id)
		}
		expected, expectedDefects, err := evaluationContext.FusedProjection(fusedSourceCommand(command, item))
		sourceCommand := fusedSourceCommand(command, item)
		if err := o.validateSourceEvaluationContext(ctx, tx, sourceCommand, evaluationContext, child.observedHeadRevision); err != nil {
			return fmt.Errorf("stored fused child %s context is not anchored: %w", id, err)
		}
		if err := o.requireNoSourceCommitFacts(ctx, tx, id); err != nil {
			return fmt.Errorf("stored failed fused child %s has commit facts: %w", id, err)
		}
		if _, found, err := o.readStoredSourceReceipt(ctx, tx, id); err != nil || found {
			if err != nil {
				return err
			}
			return fmt.Errorf("stored failed fused child %s has a source receipt", id)
		}
		if err != nil || !reflect.DeepEqual(expected, projection) || !reflect.DeepEqual(expectedDefects, defects) {
			return fmt.Errorf("stored fused child %s contradicts canonical evaluation", id)
		}
		if !reflect.DeepEqual(parentEvaluations[id], projection) || !reflect.DeepEqual(parentDefects[id], defects) {
			return fmt.Errorf("failed parent projection for fused child %s contradicts stored evaluation", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != len(imports) || len(parentEvaluations) != len(imports) {
		return fmt.Errorf("failed run child evaluation set contradicts stored canonical evaluations")
	}
	return nil
}

func fusedSourceCommand(parent runtimedata.RunCreationCommand, item runtimedata.FusedImport) runtimedata.SourceCommand {
	return runtimedata.SourceCommand{
		Operation: "import", SourceInvocationID: item.SourceInvocationID, ParentRunID: parent.RunID,
		Actor: parent.Actor, BundleHash: parent.BundleHash, Declaration: item.Declaration,
		ExpectedHead: item.ExpectedHead, InputFormat: item.InputFormat, Input: item.Input,
	}
}

func (o *Owner) requireUnusedFusedChildTx(ctx context.Context, tx *sql.Tx, id string) error {
	var marker int
	err := tx.QueryRowContext(ctx, o.query(`SELECT 1 FROM resource_source_invocations WHERE source_invocation_id = %s`, 1), id).Scan(&marker)
	if err == nil {
		return runtimedata.NewDomainError(runtimedata.CodeInvocationConflict, "source_invocation_id %s already has a successful source receipt", id)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if reserved, err := o.sourceInvocationReserved(ctx, tx, id); err != nil {
		return err
	} else if reserved {
		return runtimedata.NewDomainError(runtimedata.CodeInvocationConflict, "source_invocation_id %s is reserved by another failed run creation", id)
	}
	return nil
}

func (o *Owner) loadBundleDeclarationsTx(ctx context.Context, tx *sql.Tx, bundleHash string) ([]runtimedata.Declaration, error) {
	var marker int
	if err := tx.QueryRowContext(ctx, o.query(`SELECT 1 FROM source_artifacts WHERE bundle_hash = %s`, 1), bundleHash).Scan(&marker); errors.Is(err, sql.ErrNoRows) {
		return nil, runtimedata.NewDomainError(runtimedata.CodeContractNotFound, "bundle %s is not in the exact selected-store catalog", bundleHash)
	} else if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, o.query(`
		SELECT flow_path, event_name, display_name, owner_flow_id, COALESCE(business_key_field, ''), schema_digest, canonical_schema_bytes
		FROM resource_bundle_declarations WHERE bundle_hash = %s ORDER BY flow_path, event_name
	`, 1), bundleHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var declarations []runtimedata.Declaration
	for rows.Next() {
		var declaration runtimedata.Declaration
		if err := rows.Scan(&declaration.Ref.FlowPath, &declaration.Ref.EventName, &declaration.Name, &declaration.OwnerFlowID,
			&declaration.BusinessKey, &declaration.SchemaDigest, &declaration.CanonicalSchema); err != nil {
			return nil, err
		}
		if declaration.Ref.Validate() != nil || runtimedata.SchemaDigestFor(declaration.CanonicalSchema) != declaration.SchemaDigest {
			return nil, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "bundle %s data dependency aggregate is contradictory", bundleHash)
		}
		declarations = append(declarations, declaration)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(declarations, func(i, j int) bool {
		return runtimedata.CompareDeclarationRef(declarations[i].Ref, declarations[j].Ref) < 0
	})
	return declarations, nil
}

func (o *Owner) runExistsTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var marker int
	err := tx.QueryRowContext(ctx, o.query(`SELECT 1 FROM runs WHERE run_id = %s`, 1), runID).Scan(&marker)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}
