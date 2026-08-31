package durabledata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
)

type storedSourceReceipt struct {
	requestHash          string
	operation            string
	parentRunID          sql.NullString
	actor                string
	bundleHash           string
	packageKey           string
	eventName            string
	requestJSON          []byte
	evaluationJSON       []byte
	resultJSON           []byte
	evidenceJSON         []byte
	observedHeadRevision uint64
	completedAt          any
}

func (o *Owner) readStoredSourceReceipt(ctx context.Context, tx *sql.Tx, id string) (storedSourceReceipt, bool, error) {
	var stored storedSourceReceipt
	err := tx.QueryRowContext(ctx, o.query(`
		SELECT request_hash, operation, parent_run_id, actor, bundle_hash, flow_path, event_name,
		       request_json, evaluation_json, result_json, evidence_json, observed_head_revision, completed_at
		FROM resource_source_invocations WHERE source_invocation_id = %s
	`, 1), id).Scan(&stored.requestHash, &stored.operation, &stored.parentRunID, &stored.actor, &stored.bundleHash,
		&stored.packageKey, &stored.eventName, &stored.requestJSON, &stored.evaluationJSON, &stored.resultJSON,
		&stored.evidenceJSON, &stored.observedHeadRevision, &stored.completedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return storedSourceReceipt{}, false, nil
	}
	return stored, err == nil, err
}

func (o *Owner) validateStoredSourceReceipt(ctx context.Context, tx *sql.Tx, id string, stored storedSourceReceipt) (runtimedata.SourceOperationRecord, runtimedata.SourceCommand, error) {
	record, err := decodeSourceReceipt(id, stored.requestHash, stored.requestJSON, stored.evaluationJSON, stored.resultJSON, stored.evidenceJSON)
	if err != nil {
		return runtimedata.SourceOperationRecord{}, runtimedata.SourceCommand{}, err
	}
	var command runtimedata.SourceCommand
	if err := json.Unmarshal(stored.requestJSON, &command); err != nil {
		return runtimedata.SourceOperationRecord{}, runtimedata.SourceCommand{}, err
	}
	parentRunID := ""
	if stored.parentRunID.Valid {
		parentRunID = stored.parentRunID.String
	}
	completedAt, present, err := persistedTime(stored.completedAt)
	if err != nil || !present || stored.operation != command.Operation || parentRunID != command.ParentRunID ||
		stored.actor != command.Actor || stored.bundleHash != command.BundleHash || stored.packageKey != command.Declaration.FlowPath ||
		stored.eventName != command.Declaration.EventName || stored.observedHeadRevision != record.Evaluation.Base.Head.Revision ||
		!completedAt.Equal(record.Result.CompletedAt) {
		return runtimedata.SourceOperationRecord{}, runtimedata.SourceCommand{}, fmt.Errorf("typed source receipt contradicts canonical operation")
	}
	if err := o.validateSourceReceiptAggregate(ctx, tx, command, record); err != nil {
		return runtimedata.SourceOperationRecord{}, runtimedata.SourceCommand{}, err
	}
	return record, command, nil
}

func (o *Owner) validateSourceReceiptAggregate(ctx context.Context, tx *sql.Tx, command runtimedata.SourceCommand, record runtimedata.SourceOperationRecord) error {
	declaration, err := o.loadCatalogDeclaration(ctx, tx, command.BundleHash, command.Declaration)
	if err != nil {
		return fmt.Errorf("load admitted source declaration: %w", err)
	}
	if !declarationsEqual(declaration, record.Evaluation.Declaration) {
		return fmt.Errorf("source evaluation declaration contradicts immutable admitted declaration")
	}
	if err := o.validateSourceEvaluationBase(ctx, tx, record.Evaluation.Base, command.Declaration); err != nil {
		return err
	}
	if command.Operation == "import" && record.Result.Outcome == "accepted" {
		if err := o.validateAcceptedSourceCommit(ctx, tx, command, record.Result, record.Evaluation.Base.Head.Revision); err != nil {
			return err
		}
		return nil
	}
	return o.requireNoSourceCommitFacts(ctx, tx, command.SourceInvocationID)
}

func (o *Owner) validateSourceEvaluationContext(ctx context.Context, tx *sql.Tx, command runtimedata.SourceCommand, evaluation runtimedata.SourceEvaluationContext, observedHeadRevision uint64) error {
	declaration, err := o.loadCatalogDeclaration(ctx, tx, command.BundleHash, command.Declaration)
	if err != nil {
		return fmt.Errorf("load admitted source declaration: %w", err)
	}
	if !declarationsEqual(declaration, evaluation.Declaration) {
		return fmt.Errorf("source evaluation declaration contradicts immutable admitted declaration")
	}
	if evaluation.Base.Head.Revision != observedHeadRevision {
		return fmt.Errorf("source evaluation base contradicts typed observed head revision")
	}
	return o.validateSourceEvaluationBase(ctx, tx, evaluation.Base, command.Declaration)
}

type immutableVersionFact struct {
	sequenceAlias uint64
	manifest      runtimedata.Manifest
	businessKey   string
	schema        []byte
}

func (o *Owner) loadImmutableVersionFact(ctx context.Context, tx *sql.Tx, ref runtimedata.DeclarationRef, versionID runtimedata.VersionID) (immutableVersionFact, bool, error) {
	var fact immutableVersionFact
	var schemaDigest runtimedata.SchemaDigest
	var contentDigest runtimedata.ContentDigest
	var rowCodec string
	var rowCount uint64
	var manifestJSON []byte
	err := tx.QueryRowContext(ctx, o.query(`
		SELECT sequence_alias, schema_digest, canonical_schema_bytes, COALESCE(business_key_field, ''),
		       content_digest, row_codec, row_count, manifest_json
		FROM resource_versions WHERE version_id = %s AND flow_path = %s AND event_name = %s
	`, 3), versionID, ref.FlowPath, ref.EventName).Scan(&fact.sequenceAlias, &schemaDigest, &fact.schema,
		&fact.businessKey, &contentDigest, &rowCodec, &rowCount, &manifestJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return immutableVersionFact{}, false, nil
	}
	if err != nil {
		return immutableVersionFact{}, false, err
	}
	if err := json.Unmarshal(manifestJSON, &fact.manifest); err != nil {
		return immutableVersionFact{}, false, fmt.Errorf("resource version %s manifest is invalid", versionID)
	}
	derived, deriveErr := fact.manifest.VersionID()
	if deriveErr != nil || derived != versionID || fact.manifest.Declaration != ref || fact.manifest.SchemaDigest != schemaDigest ||
		fact.manifest.ContentDigest != contentDigest || fact.manifest.RowCodec != rowCodec || fact.manifest.RowCount != rowCount ||
		runtimedata.SchemaDigestFor(fact.schema) != schemaDigest {
		return immutableVersionFact{}, false, fmt.Errorf("resource version %s immutable columns contradict manifest", versionID)
	}
	if _, err := schemaBusinessKey(fact.schema, fact.businessKey); err != nil {
		return immutableVersionFact{}, false, fmt.Errorf("resource version %s immutable schema facts are contradictory", versionID)
	}
	canonicalManifest, _ := json.Marshal(fact.manifest)
	if !jsonEqual(manifestJSON, canonicalManifest) {
		return immutableVersionFact{}, false, fmt.Errorf("resource version %s manifest projection is contradictory", versionID)
	}
	return fact, true, nil
}

func (o *Owner) validateSourceEvaluationBase(ctx context.Context, tx *sql.Tx, base runtimedata.SourceEvaluationBase, ref runtimedata.DeclarationRef) error {
	if _, err := base.Validate(ref); err != nil {
		return fmt.Errorf("source evaluation base is contradictory: %w", err)
	}
	if base.State == "absent" {
		return nil
	}
	versionID := base.Head.Before.VersionID
	fact, found, err := o.loadImmutableVersionFact(ctx, tx, ref, versionID)
	if err != nil {
		return err
	}
	if !found || base.Manifest == nil || !reflect.DeepEqual(fact.manifest, *base.Manifest) ||
		fact.businessKey != base.BusinessKey || !bytes.Equal(fact.schema, base.CanonicalSchema) {
		return fmt.Errorf("source evaluation base contradicts immutable version %s", versionID)
	}
	history, found, err := o.loadHeadHistoryFact(ctx, tx, ref, base.Head.Revision)
	if err != nil {
		return err
	}
	if !found || !history.After.Equal(base.Head.Before) {
		return fmt.Errorf("source evaluation base contradicts immutable head revision %d", base.Head.Revision)
	}
	return nil
}
func (o *Owner) validateAcceptedSourceCommit(ctx context.Context, tx *sql.Tx, command runtimedata.SourceCommand, result runtimedata.SourceOperationResult, observedHeadRevision uint64) error {
	fact, found, err := o.loadImmutableVersionFact(ctx, tx, command.Declaration, result.Candidate.VersionID)
	if err != nil {
		return err
	}
	alias, aliasErr := runtimedata.ParseVersionAlias(result.Candidate.Alias)
	if !found || aliasErr != nil || fact.sequenceAlias != alias || result.Candidate.Manifest == nil ||
		!reflect.DeepEqual(fact.manifest, *result.Candidate.Manifest) || fact.manifest.SchemaDigest != result.SchemaDigest {
		return fmt.Errorf("accepted source operation contradicts immutable committed version")
	}
	provenance, found, err := o.loadExactImportProvenance(ctx, tx, result.Candidate.VersionID, command.SourceInvocationID)
	if err != nil {
		return err
	}
	if !found || provenance.Actor != command.Actor || !provenance.CommittedAt.Equal(result.CompletedAt) {
		return fmt.Errorf("accepted source operation has no exact import provenance")
	}
	provenanceCount, err := o.countSourceProvenance(ctx, tx, command.SourceInvocationID)
	if err != nil {
		return err
	}
	if provenanceCount != 1 {
		return fmt.Errorf("accepted source operation has contradictory import provenance cardinality")
	}
	historyCount, err := o.countSourceHeadHistory(ctx, tx, command.SourceInvocationID)
	if err != nil {
		return err
	}
	if !result.Head.Changed {
		if historyCount != 0 {
			return fmt.Errorf("unchanged accepted source operation carries head-history mutation")
		}
		return nil
	}
	history, found, err := o.loadHeadHistoryFact(ctx, tx, command.Declaration, result.Head.Revision)
	if err != nil {
		return err
	}
	operation := "source_import"
	if command.ParentRunID != "" {
		operation = "fused_import"
	}
	if !found || historyCount != 1 || history.Revision != observedHeadRevision+1 || history.Operation != operation || history.OperationID != command.SourceInvocationID ||
		!history.Before.Equal(result.Head.Before) || !history.After.Equal(result.Head.After) || !history.CommittedAt.Equal(result.CompletedAt) {
		return fmt.Errorf("accepted source operation contradicts immutable head-history mutation")
	}
	return nil
}

func (o *Owner) loadExactImportProvenance(ctx context.Context, tx *sql.Tx, versionID runtimedata.VersionID, sourceInvocationID string) (runtimedata.Provenance, bool, error) {
	row := tx.QueryRowContext(ctx, o.query(`
		SELECT provenance_sequence, producer_kind, producer_id, actor, provenance_json, committed_at
		FROM resource_version_provenance
		WHERE version_id = %s AND producer_kind = 'import' AND producer_id = %s
	`, 2), versionID, sourceInvocationID)
	provenance, err := scanVersionProvenance(row, versionID)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimedata.Provenance{}, false, nil
	}
	return provenance, err == nil, err
}

func (o *Owner) countSourceProvenance(ctx context.Context, tx *sql.Tx, sourceInvocationID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, o.query(`
		SELECT COUNT(*) FROM resource_version_provenance WHERE producer_kind = 'import' AND producer_id = %s
	`, 1), sourceInvocationID).Scan(&count)
	return count, err
}

func (o *Owner) countSourceHeadHistory(ctx context.Context, tx *sql.Tx, sourceInvocationID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, o.query(`SELECT COUNT(*) FROM resource_head_history WHERE operation_id = %s`, 1), sourceInvocationID).Scan(&count)
	return count, err
}

func (o *Owner) requireNoSourceCommitFacts(ctx context.Context, tx *sql.Tx, sourceInvocationID string) error {
	provenanceCount, err := o.countSourceProvenance(ctx, tx, sourceInvocationID)
	if err != nil {
		return err
	}
	historyCount, err := o.countSourceHeadHistory(ctx, tx, sourceInvocationID)
	if err != nil {
		return err
	}
	if provenanceCount != 0 || historyCount != 0 {
		return fmt.Errorf("non-mutating source operation carries immutable commit facts")
	}
	return nil
}

func (o *Owner) loadHeadHistoryFact(ctx context.Context, tx *sql.Tx, ref runtimedata.DeclarationRef, revision uint64) (runtimedata.HeadHistory, bool, error) {
	var history runtimedata.HeadHistory
	var before sql.NullString
	var after string
	var committedAt any
	history.Declaration = ref
	err := tx.QueryRowContext(ctx, o.query(`
		SELECT before_version_id, after_version_id, operation_kind, operation_id, committed_at
		FROM resource_head_history WHERE flow_path = %s AND event_name = %s AND revision = %s
	`, 3), ref.FlowPath, ref.EventName, revision).Scan(&before, &after, &history.Operation, &history.OperationID, &committedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimedata.HeadHistory{}, false, nil
	}
	if err != nil {
		return runtimedata.HeadHistory{}, false, err
	}
	history.Revision = revision
	history.Before = runtimedata.AbsentHead()
	if before.Valid {
		history.Before = runtimedata.VersionHead(runtimedata.VersionID(before.String))
	}
	history.After = runtimedata.VersionHead(runtimedata.VersionID(after))
	parsed, present, err := persistedTime(committedAt)
	if err != nil || !present || revision == 0 || history.Before.Validate() != nil || history.After.Validate() != nil {
		return runtimedata.HeadHistory{}, false, fmt.Errorf("resource head-history revision %d is contradictory", revision)
	}
	history.CommittedAt = parsed
	return history, true, nil
}

type storedRunCreationReceipt struct {
	requestHash  string
	actor        string
	bundleHash   string
	requestJSON  []byte
	summaryJSON  []byte
	bindingJSON  []byte
	evidenceJSON []byte
	completedAt  any
}

func (o *Owner) readStoredRunCreationReceipt(ctx context.Context, tx *sql.Tx, runID string) (storedRunCreationReceipt, bool, error) {
	var stored storedRunCreationReceipt
	err := tx.QueryRowContext(ctx, o.query(`
		SELECT request_hash, actor, bundle_hash, request_json, summary_json, binding_json, evidence_json, completed_at
		FROM resource_run_creation_operations WHERE run_id = %s
	`, 1), runID).Scan(&stored.requestHash, &stored.actor, &stored.bundleHash, &stored.requestJSON, &stored.summaryJSON,
		&stored.bindingJSON, &stored.evidenceJSON, &stored.completedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return storedRunCreationReceipt{}, false, nil
	}
	return stored, err == nil, err
}

func (o *Owner) validateStoredRunCreationReceipt(ctx context.Context, tx *sql.Tx, runID string, stored storedRunCreationReceipt, validateSources bool) (runtimedata.RunCreationOperationRecord, runtimedata.RunCreationCommand, error) {
	record, command, err := decodeRunCreationReceipt(runID, stored.requestHash, stored.requestJSON, stored.summaryJSON, stored.bindingJSON, stored.evidenceJSON)
	if err != nil {
		return runtimedata.RunCreationOperationRecord{}, runtimedata.RunCreationCommand{}, err
	}
	completedAt, present, err := persistedTime(stored.completedAt)
	if err != nil || !present || stored.actor != command.Actor || stored.bundleHash != command.BundleHash || !completedAt.Equal(record.Summary.CompletedAt) {
		return runtimedata.RunCreationOperationRecord{}, runtimedata.RunCreationCommand{}, fmt.Errorf("typed run-creation receipt contradicts canonical operation")
	}
	if validateSources {
		if err := o.validateStoredRunCreationSourcesTx(ctx, tx, command, record); err != nil {
			return runtimedata.RunCreationOperationRecord{}, runtimedata.RunCreationCommand{}, err
		}
	}
	return record, command, nil
}

type storedPruneReceipt struct {
	requestHash string
	actor       string
	packageKey  string
	eventName   string
	versionID   string
	requestJSON []byte
	resultJSON  []byte
	completedAt any
}

func (o *Owner) readStoredPruneReceipt(ctx context.Context, tx *sql.Tx, id string) (storedPruneReceipt, bool, error) {
	var stored storedPruneReceipt
	err := tx.QueryRowContext(ctx, o.query(`
		SELECT request_hash, actor, flow_path, event_name, version_id, request_json, result_json, completed_at
		FROM resource_prune_invocations WHERE prune_invocation_id = %s
	`, 1), id).Scan(&stored.requestHash, &stored.actor, &stored.packageKey, &stored.eventName, &stored.versionID,
		&stored.requestJSON, &stored.resultJSON, &stored.completedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return storedPruneReceipt{}, false, nil
	}
	return stored, err == nil, err
}

func (o *Owner) validateStoredPruneReceipt(ctx context.Context, tx *sql.Tx, id string, stored storedPruneReceipt) (runtimedata.PruneOperationResult, runtimedata.PruneCommand, []runtimedata.Pin, error) {
	pins, err := o.loadPrunePinEvidence(ctx, tx, id)
	if err != nil {
		return runtimedata.PruneOperationResult{}, runtimedata.PruneCommand{}, nil, err
	}
	result, err := decodePruneReceipt(id, stored.requestHash, stored.requestJSON, stored.resultJSON, pins)
	if err != nil {
		return runtimedata.PruneOperationResult{}, runtimedata.PruneCommand{}, nil, err
	}
	var command runtimedata.PruneCommand
	if err := json.Unmarshal(stored.requestJSON, &command); err != nil {
		return runtimedata.PruneOperationResult{}, runtimedata.PruneCommand{}, nil, err
	}
	completedAt, present, err := persistedTime(stored.completedAt)
	if err != nil || !present || stored.actor != command.Actor || stored.packageKey != command.Declaration.FlowPath ||
		stored.eventName != command.Declaration.EventName || stored.versionID != string(command.VersionID) || !completedAt.Equal(result.CompletedAt) {
		return runtimedata.PruneOperationResult{}, runtimedata.PruneCommand{}, nil, fmt.Errorf("typed prune receipt contradicts canonical operation")
	}
	return result, command, pins, nil
}
