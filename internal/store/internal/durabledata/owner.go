package durabledata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type dialect string

const (
	dialectPostgres dialect = "postgres"
	dialectSQLite   dialect = "sqlite"
)

type transactionRunner func(context.Context, func(context.Context, *sql.Tx) error) error

// Owner is the sole durable declaration/version/head/receipt/pin interpreter.
// Backend differences stop at transaction and placeholder syntax.
type Owner struct {
	dialect        dialect
	runTransaction transactionRunner
	requireCurrent func() error
	now            func() time.Time
}

func NewPostgres(backend *postgresbackend.Backend, requireCurrent func() error) (*Owner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil {
		return nil, fmt.Errorf("durable data postgres owner dependencies are required")
	}
	return &Owner{
		dialect: dialectPostgres, requireCurrent: requireCurrent, now: time.Now,
		runTransaction: backend.RunTransaction,
	}, nil
}

func NewSQLite(backend *sqlitebackend.Backend, requireCurrent func() error, now func() time.Time) (*Owner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil {
		return nil, fmt.Errorf("durable data sqlite owner dependencies are required")
	}
	if now == nil {
		now = time.Now
	}
	return &Owner{
		dialect: dialectSQLite, requireCurrent: requireCurrent, now: now,
		runTransaction: func(ctx context.Context, operation func(context.Context, *sql.Tx) error) error {
			return backend.RunTransaction(ctx, "durable data mutation", operation)
		},
	}, nil
}

func (o *Owner) currentTime() time.Time { return o.now().UTC().Truncate(time.Microsecond) }

func RegisterCatalogTx(o *Owner, ctx context.Context, tx *sql.Tx, catalog runtimedata.Catalog, now time.Time) error {
	if o == nil {
		return fmt.Errorf("durable data owner is required")
	}
	if tx == nil {
		return fmt.Errorf("durable data catalog transaction is required")
	}
	if err := validateCatalog(catalog); err != nil {
		return err
	}
	for _, declaration := range catalog.Declarations {
		if err := o.registerDeclaration(ctx, tx, catalog.BundleHash, declaration, now.UTC().Truncate(time.Microsecond)); err != nil {
			return err
		}
	}
	for _, item := range catalog.StaticData {
		if err := o.registerStaticData(ctx, tx, item, now.UTC().Truncate(time.Microsecond)); err != nil {
			return err
		}
	}
	return nil
}

func validateCatalog(catalog runtimedata.Catalog) error {
	if err := runtimebundleidentity.ValidateCanonicalHash(catalog.BundleHash); err != nil {
		return err
	}
	if len(catalog.Declarations) > runtimedata.MaxDataDeclarationsPerBundle {
		return fmt.Errorf("bundle declares more than %d resources", runtimedata.MaxDataDeclarationsPerBundle)
	}
	seenRefs := map[string]struct{}{}
	seenNames := map[string]struct{}{}
	for _, declaration := range catalog.Declarations {
		if err := declaration.Ref.Validate(); err != nil {
			return err
		}
		if declaration.Name == "" || declaration.Name != declaration.Ref.EventName {
			return fmt.Errorf("resource declaration name must equal its canonical authored event name")
		}
		if err := declaration.SchemaDigest.Validate(); err != nil {
			return err
		}
		if got := runtimedata.SchemaDigestFor(declaration.CanonicalSchema); got != declaration.SchemaDigest {
			return fmt.Errorf("resource %s schema digest contradiction", declaration.Name)
		}
		var schema map[string]any
		if err := json.Unmarshal(declaration.CanonicalSchema, &schema); err != nil || strings.TrimSpace(fmt.Sprint(schema["type"])) != "object" {
			return fmt.Errorf("resource %s canonical schema must be one JSON object schema", declaration.Name)
		}
		if _, duplicate := seenRefs[declaration.Ref.Key()]; duplicate {
			return fmt.Errorf("bundle repeats resource declaration %s", declaration.Ref.Key())
		}
		seenRefs[declaration.Ref.Key()] = struct{}{}
		nameKey := declaration.Ref.PackageKey + "\x00" + declaration.Name
		if _, duplicate := seenNames[nameKey]; duplicate {
			return fmt.Errorf("bundle repeats resource name %s in package %s", declaration.Name, declaration.Ref.PackageKey)
		}
		seenNames[nameKey] = struct{}{}
	}
	seenStatic := map[runtimedata.StaticDataID]struct{}{}
	for _, item := range catalog.StaticData {
		if item.Ref.BundleHash != catalog.BundleHash || !utf8.Valid(item.Content) {
			return fmt.Errorf("static data %s requires exact bundle identity and valid UTF-8 bytes", item.Ref.CanonicalInputLabel)
		}
		id, err := runtimedata.NewStaticDataID(item.Ref)
		if err != nil || id != item.StaticID {
			return fmt.Errorf("static data %s identity contradiction", item.Ref.CanonicalInputLabel)
		}
		if runtimedata.StaticContentDigest(item.Content) != item.ContentDigest {
			return fmt.Errorf("static data %s content digest contradiction", item.Ref.CanonicalInputLabel)
		}
		if _, duplicate := seenStatic[item.StaticID]; duplicate {
			return fmt.Errorf("bundle repeats static data %s", item.StaticID)
		}
		seenStatic[item.StaticID] = struct{}{}
	}
	return nil
}

func (o *Owner) registerDeclaration(ctx context.Context, tx *sql.Tx, bundleHash string, declaration runtimedata.Declaration, now time.Time) error {
	result, err := tx.ExecContext(ctx, o.query(`
		INSERT INTO resource_declarations (package_key, event_name, admitted_at)
		VALUES (%s, %s, %s) ON CONFLICT (package_key, event_name) DO NOTHING
	`, 3), declaration.Ref.PackageKey, declaration.Ref.EventName, now)
	if err != nil {
		return fmt.Errorf("insert resource declaration identity: %w", err)
	}
	inserted, _ := result.RowsAffected()
	if inserted > 0 {
		if _, err := tx.ExecContext(ctx, o.query(`
			INSERT INTO resource_heads (package_key, event_name, version_id, revision, updated_at)
			VALUES (%s, %s, NULL, 0, %s)
		`, 3), declaration.Ref.PackageKey, declaration.Ref.EventName, now); err != nil {
			return fmt.Errorf("insert resource head: %w", err)
		}
	}

	var existing runtimedata.Declaration
	var schema []byte
	err = tx.QueryRowContext(ctx, o.query(`
		SELECT display_name, owner_flow_id, COALESCE(business_key_field, ''), schema_digest, canonical_schema_bytes
		FROM resource_bundle_declarations
		WHERE bundle_hash = %s AND package_key = %s AND event_name = %s
	`, 3), bundleHash, declaration.Ref.PackageKey, declaration.Ref.EventName).Scan(
		&existing.Name, &existing.OwnerFlowID, &existing.BusinessKey, &existing.SchemaDigest, &schema,
	)
	if err == nil {
		existing.Ref = declaration.Ref
		existing.CanonicalSchema = schema
		if !declarationsEqual(existing, declaration) {
			return fmt.Errorf("bundle %s resource declaration %s conflicts with immutable catalog facts", bundleHash, declaration.Ref.Key())
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read resource bundle declaration: %w", err)
	}
	_, err = tx.ExecContext(ctx, o.query(`
		INSERT INTO resource_bundle_declarations
		(bundle_hash, package_key, event_name, display_name, owner_flow_id, business_key_field, schema_digest, canonical_schema_bytes, admitted_at)
		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
	`, 9), bundleHash, declaration.Ref.PackageKey, declaration.Ref.EventName, declaration.Name, declaration.OwnerFlowID,
		nullableText(declaration.BusinessKey), declaration.SchemaDigest, declaration.CanonicalSchema, now)
	if err != nil {
		return fmt.Errorf("insert resource bundle declaration: %w", err)
	}
	return nil
}

func declarationsEqual(a, b runtimedata.Declaration) bool {
	return a.Name == b.Name && a.Ref == b.Ref && a.OwnerFlowID == b.OwnerFlowID &&
		a.BusinessKey == b.BusinessKey && a.SchemaDigest == b.SchemaDigest && bytes.Equal(a.CanonicalSchema, b.CanonicalSchema)
}

func (o *Owner) registerStaticData(ctx context.Context, tx *sql.Tx, item runtimedata.StaticData, now time.Time) error {
	var bundleHash, label, packageKey, flowID, relativePath, digest, contentType string
	var content []byte
	err := tx.QueryRowContext(ctx, o.query(`
		SELECT bundle_hash, canonical_input_label, package_key, owner_flow_id, relative_path, content_digest, content_type, content_bytes
		FROM bundle_static_data WHERE static_id = %s
	`, 1), item.StaticID).Scan(&bundleHash, &label, &packageKey, &flowID, &relativePath, &digest, &contentType, &content)
	if err == nil {
		if bundleHash != item.Ref.BundleHash || label != item.Ref.CanonicalInputLabel || packageKey != item.PackageKey || flowID != item.OwnerFlowID ||
			relativePath != item.RelativePath || digest != item.ContentDigest || contentType != item.ContentType || !bytes.Equal(content, item.Content) {
			return fmt.Errorf("static data %s conflicts with immutable admitted bytes", item.StaticID)
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read static data: %w", err)
	}
	_, err = tx.ExecContext(ctx, o.query(`
		INSERT INTO bundle_static_data
		(static_id, bundle_hash, canonical_input_label, package_key, owner_flow_id, relative_path, content_digest, content_type, content_bytes, admitted_at)
		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
	`, 10), item.StaticID, item.Ref.BundleHash, item.Ref.CanonicalInputLabel, item.PackageKey, item.OwnerFlowID,
		item.RelativePath, item.ContentDigest, item.ContentType, item.Content, now)
	return err
}

func (o *Owner) ExecuteSourceOperation(ctx context.Context, command runtimedata.SourceCommand) (runtimedata.SourceOperationResult, error) {
	if err := o.requireCurrent(); err != nil {
		return runtimedata.SourceOperationResult{}, err
	}
	requestHash, requestJSON, err := command.RequestHash()
	if err != nil {
		return runtimedata.SourceOperationResult{}, err
	}
	var result runtimedata.SourceOperationResult
	err = o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		replayed, found, replayErr := o.loadSourceReceipt(txctx, tx, command.SourceInvocationID, requestHash)
		if replayErr != nil {
			return replayErr
		}
		if found {
			result = replayed
			return nil
		}
		if reserved, reserveErr := o.sourceInvocationReserved(txctx, tx, command.SourceInvocationID); reserveErr != nil {
			return reserveErr
		} else if reserved {
			return runtimedata.NewDomainError(runtimedata.CodeInvocationConflict, "source_invocation_id %s is owned by a failed fused run creation", command.SourceInvocationID)
		}
		declaration, catalogErr := o.loadCatalogDeclaration(txctx, tx, command.BundleHash, command.Declaration)
		if catalogErr != nil {
			return catalogErr
		}
		evaluation, executionErr := o.executeSourceTx(txctx, tx, command, declaration)
		err = executionErr
		if err != nil {
			return err
		}
		result = evaluation.result
		return o.insertSourceReceipt(txctx, tx, command, requestHash, requestJSON, evaluation)
	})
	return result, err
}

type sourceEvaluation struct {
	command     runtimedata.SourceCommand
	declaration runtimedata.Declaration
	context     runtimedata.SourceEvaluationContext
	compiled    runtimedata.CompiledVersion
	result      runtimedata.SourceOperationResult
	evidence    runtimedata.SourceEvidence
}

func (o *Owner) executeSourceTx(ctx context.Context, tx *sql.Tx, command runtimedata.SourceCommand, declaration runtimedata.Declaration) (sourceEvaluation, error) {
	evaluation, err := o.evaluateSourceTx(ctx, tx, command, declaration)
	if err != nil {
		return sourceEvaluation{}, err
	}
	if command.Operation == "import" && evaluation.result.Outcome == "accepted" {
		if err := o.commitSourceEvaluationTx(ctx, tx, &evaluation); err != nil {
			return sourceEvaluation{}, err
		}
	}
	return evaluation, nil
}

func (o *Owner) evaluateSourceTx(ctx context.Context, tx *sql.Tx, command runtimedata.SourceCommand, declaration runtimedata.Declaration) (sourceEvaluation, error) {
	head, err := o.loadHead(ctx, tx, command.Declaration, true)
	if err != nil {
		return sourceEvaluation{}, err
	}
	base, err := o.loadSourceEvaluationBase(ctx, tx, head, declaration.Ref)
	if err != nil {
		return sourceEvaluation{}, err
	}
	evaluationContext := runtimedata.SourceEvaluationContext{
		Format: runtimedata.SourceEvaluationFormat, Declaration: declaration, Base: base, CompletedAt: o.currentTime(),
	}
	compiled, result, evidence, err := evaluationContext.Evaluate(command)
	if err != nil {
		return sourceEvaluation{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "source evaluation context is contradictory: %v", err)
	}
	return sourceEvaluation{command: command, declaration: declaration, context: evaluationContext, compiled: compiled, result: result, evidence: evidence}, nil
}

func (o *Owner) commitSourceEvaluationTx(ctx context.Context, tx *sql.Tx, evaluation *sourceEvaluation) error {
	if evaluation == nil || evaluation.result.Outcome != "accepted" || evaluation.command.Operation != "import" {
		return fmt.Errorf("source commit requires one accepted import evaluation")
	}
	result := evaluation.result
	baseHead := result.ObservedHead
	alias, err := o.persistVersion(ctx, tx, evaluation.compiled, evaluation.declaration, evaluation.command, result.CompletedAt)
	if err != nil {
		return err
	}
	result.Candidate.Alias = fmt.Sprintf("v%d", alias)
	result.Candidate.State = "version"
	result.Head.After = runtimedata.VersionHead(evaluation.compiled.VersionID)
	result.Head.Changed = !baseHead.Equal(result.Head.After)
	if result.Head.Changed {
		result.Head.Revision++
		_, err = tx.ExecContext(ctx, o.query(`
			UPDATE resource_heads SET version_id = %s, revision = %s, updated_at = %s
			WHERE package_key = %s AND event_name = %s
		`, 5), evaluation.compiled.VersionID, result.Head.Revision, result.CompletedAt, evaluation.command.Declaration.PackageKey, evaluation.command.Declaration.EventName)
		if err != nil {
			return fmt.Errorf("advance resource head: %w", err)
		}
		var before any
		if baseHead.State == "version" {
			before = baseHead.VersionID
		}
		operationKind := "source_import"
		if evaluation.command.ParentRunID != "" {
			operationKind = "fused_import"
		}
		if _, err := tx.ExecContext(ctx, o.query(`
				INSERT INTO resource_head_history
				(package_key, event_name, revision, before_version_id, after_version_id, operation_kind, operation_id, committed_at)
				VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
			`, 8), evaluation.command.Declaration.PackageKey, evaluation.command.Declaration.EventName, result.Head.Revision, before,
			evaluation.compiled.VersionID, operationKind, evaluation.command.SourceInvocationID, result.CompletedAt); err != nil {
			return fmt.Errorf("record resource head history: %w", err)
		}
	}
	evaluation.result = result
	return nil
}

func (o *Owner) persistVersion(ctx context.Context, tx *sql.Tx, compiled runtimedata.CompiledVersion, declaration runtimedata.Declaration, command runtimedata.SourceCommand, now time.Time) (uint64, error) {
	var alias uint64
	var manifestJSON, schemaJSON, payload []byte
	var businessKey string
	var prunedAt any
	err := tx.QueryRowContext(ctx, o.query(`
		SELECT sequence_alias, manifest_json, canonical_schema_bytes, COALESCE(business_key_field, ''), canonical_jsonl, pruned_at
		FROM resource_versions WHERE version_id = %s
	`, 1), compiled.VersionID).Scan(&alias, &manifestJSON, &schemaJSON, &businessKey, &payload, &prunedAt)
	if err == nil {
		expectedManifest, _ := json.Marshal(compiled.Manifest)
		if !jsonEqual(manifestJSON, expectedManifest) || !jsonEqual(schemaJSON, declaration.CanonicalSchema) || businessKey != declaration.BusinessKey {
			return 0, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s immutable evidence is corrupt", compiled.VersionID)
		}
		if prunedAt == nil && !bytes.Equal(payload, compiled.CanonicalJSONL) {
			return 0, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s materialized payload is corrupt", compiled.VersionID)
		}
		if prunedAt != nil {
			if payload != nil {
				return 0, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s tombstone retains payload", compiled.VersionID)
			}
			if _, err := tx.ExecContext(ctx, o.query(`
				UPDATE resource_versions SET canonical_jsonl = %s, pruned_at = NULL WHERE version_id = %s
			`, 2), compiled.CanonicalJSONL, compiled.VersionID); err != nil {
				return 0, fmt.Errorf("rematerialize resource version: %w", err)
			}
		}
		return alias, o.insertImportProvenance(ctx, tx, compiled.VersionID, command, now)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("read resource version: %w", err)
	}
	if err := tx.QueryRowContext(ctx, o.query(`
		SELECT COALESCE(MAX(sequence_alias), 0) + 1 FROM resource_versions
		WHERE package_key = %s AND event_name = %s
	`, 2), command.Declaration.PackageKey, command.Declaration.EventName).Scan(&alias); err != nil {
		return 0, fmt.Errorf("allocate resource version alias: %w", err)
	}
	manifestJSON, _ = json.Marshal(compiled.Manifest)
	_, err = tx.ExecContext(ctx, o.query(`
		INSERT INTO resource_versions
		(version_id, package_key, event_name, sequence_alias, schema_digest, canonical_schema_bytes, business_key_field, content_digest, row_codec, row_count,
		 manifest_json, canonical_jsonl, created_at, pruned_at)
		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)
	`, 13), compiled.VersionID, command.Declaration.PackageKey, command.Declaration.EventName, alias,
		compiled.Manifest.SchemaDigest, declaration.CanonicalSchema, nullableText(declaration.BusinessKey),
		compiled.Manifest.ContentDigest, compiled.Manifest.RowCodec, compiled.Manifest.RowCount,
		manifestJSON, compiled.CanonicalJSONL, now)
	if err != nil {
		return 0, fmt.Errorf("insert resource version: %w", err)
	}
	return alias, o.insertImportProvenance(ctx, tx, compiled.VersionID, command, now)
}

func (o *Owner) insertImportProvenance(ctx context.Context, tx *sql.Tx, versionID runtimedata.VersionID, command runtimedata.SourceCommand, now time.Time) error {
	sequence, err := o.allocateProvenanceSequence(ctx, tx, versionID)
	if err != nil {
		return err
	}
	provenance := runtimedata.Provenance{
		Sequence:  sequence,
		VersionID: versionID, ProducerRef: runtimedata.ProvenanceRef{Kind: "import", SourceInvocationID: command.SourceInvocationID},
		Actor: command.Actor, CommittedAt: now,
	}
	if err := provenance.Validate(); err != nil {
		return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource provenance is contradictory: %v", err)
	}
	raw, err := json.Marshal(provenance)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, o.query(`
		INSERT INTO resource_version_provenance
		(version_id, provenance_sequence, producer_kind, producer_id, actor, provenance_json, committed_at)
		VALUES (%s, %s, 'import', %s, %s, %s, %s)
	`, 6), versionID, sequence, command.SourceInvocationID, command.Actor, raw, now)
	if err != nil {
		return fmt.Errorf("insert resource provenance: %w", err)
	}
	return nil
}

func (o *Owner) allocateProvenanceSequence(ctx context.Context, tx *sql.Tx, versionID runtimedata.VersionID) (uint64, error) {
	query := o.query(`SELECT version_id FROM resource_versions WHERE version_id = %s`, 1)
	if o.dialect == dialectPostgres {
		query += " FOR UPDATE"
	}
	var locked runtimedata.VersionID
	if err := tx.QueryRowContext(ctx, query, versionID).Scan(&locked); err != nil || locked != versionID {
		return 0, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s cannot own provenance sequence", versionID)
	}
	var sequence uint64
	if err := tx.QueryRowContext(ctx, o.query(`
		SELECT COALESCE(MAX(provenance_sequence), 0) + 1 FROM resource_version_provenance WHERE version_id = %s
	`, 1), versionID).Scan(&sequence); err != nil {
		return 0, fmt.Errorf("allocate resource provenance sequence: %w", err)
	}
	if sequence == 0 {
		return 0, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s provenance sequence overflowed", versionID)
	}
	return sequence, nil
}

func (o *Owner) loadSourceReceipt(ctx context.Context, tx *sql.Tx, id, requestHash string) (runtimedata.SourceOperationResult, bool, error) {
	stored, found, err := o.readStoredSourceReceipt(ctx, tx, id)
	if err != nil || !found {
		if err != nil {
			return runtimedata.SourceOperationResult{}, false, err
		}
		return runtimedata.SourceOperationResult{}, false, nil
	}
	if stored.requestHash != requestHash {
		return runtimedata.SourceOperationResult{}, false, runtimedata.NewDomainError(runtimedata.CodeInvocationConflict, "source_invocation_id %s was already used for a different request", id)
	}
	record, _, err := o.validateStoredSourceReceipt(ctx, tx, id, stored)
	if err != nil {
		return runtimedata.SourceOperationResult{}, false, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "source operation %s result is contradictory: %v", id, err)
	}
	return record.Result, true, nil
}

func (o *Owner) loadSourceRecordTx(ctx context.Context, tx *sql.Tx, command runtimedata.SourceCommand) (runtimedata.SourceOperationRecord, bool, error) {
	requestHash, _, err := command.RequestHash()
	if err != nil {
		return runtimedata.SourceOperationRecord{}, false, err
	}
	stored, found, err := o.readStoredSourceReceipt(ctx, tx, command.SourceInvocationID)
	if err != nil || !found {
		if err != nil {
			return runtimedata.SourceOperationRecord{}, false, err
		}
		return runtimedata.SourceOperationRecord{}, false, nil
	}
	if stored.requestHash != requestHash {
		return runtimedata.SourceOperationRecord{}, false, runtimedata.NewDomainError(runtimedata.CodeInvocationConflict, "source_invocation_id %s was already used for a different request", command.SourceInvocationID)
	}
	record, _, err := o.validateStoredSourceReceipt(ctx, tx, command.SourceInvocationID, stored)
	if err != nil {
		return runtimedata.SourceOperationRecord{}, false, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "source operation %s result is contradictory: %v", command.SourceInvocationID, err)
	}
	return record, true, nil
}

func (o *Owner) insertSourceReceipt(ctx context.Context, tx *sql.Tx, command runtimedata.SourceCommand, hash string, requestJSON []byte, evaluation sourceEvaluation) error {
	record := runtimedata.SourceOperationRecord{Evaluation: evaluation.context, Result: evaluation.result, Evidence: evaluation.evidence}
	if err := record.ValidateForCommand(command); err != nil {
		return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "source operation %s result is contradictory: %v", command.SourceInvocationID, err)
	}
	evaluationJSON, err := json.Marshal(evaluation.context)
	if err != nil {
		return err
	}
	resultJSON, err := json.Marshal(evaluation.result)
	if err != nil {
		return err
	}
	evidenceJSON, err := json.Marshal(evaluation.evidence)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, o.query(`
		INSERT INTO resource_source_invocations
		(source_invocation_id, request_hash, operation, parent_run_id, actor, bundle_hash, package_key, event_name, request_json, evaluation_json, result_json, evidence_json, observed_head_revision, completed_at)
		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
	`, 14), command.SourceInvocationID, hash, command.Operation, nullableUUID(command.ParentRunID), command.Actor, command.BundleHash,
		command.Declaration.PackageKey, command.Declaration.EventName, requestJSON, evaluationJSON, resultJSON, evidenceJSON,
		evaluation.context.Base.Head.Revision, evaluation.result.CompletedAt)
	if err != nil {
		return err
	}
	stored, found, err := o.readStoredSourceReceipt(ctx, tx, command.SourceInvocationID)
	if err != nil {
		return fmt.Errorf("read inserted source receipt: %w", err)
	}
	if !found {
		return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "inserted source operation %s is missing", command.SourceInvocationID)
	}
	if _, _, err := o.validateStoredSourceReceipt(ctx, tx, command.SourceInvocationID, stored); err != nil {
		return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "inserted source operation %s is contradictory: %v", command.SourceInvocationID, err)
	}
	return nil
}

func decodeSourceReceipt(id, storedHash string, requestJSON, evaluationJSON, resultJSON, evidenceJSON []byte) (runtimedata.SourceOperationRecord, error) {
	var command runtimedata.SourceCommand
	var record runtimedata.SourceOperationRecord
	if err := json.Unmarshal(requestJSON, &command); err != nil {
		return runtimedata.SourceOperationRecord{}, fmt.Errorf("decode source request: %w", err)
	}
	computedHash, canonicalRequest, err := command.RequestHash()
	if err != nil || computedHash != storedHash || !jsonEqual(canonicalRequest, requestJSON) || command.SourceInvocationID != id {
		return runtimedata.SourceOperationRecord{}, fmt.Errorf("source request identity or hash is contradictory")
	}
	if err := json.Unmarshal(resultJSON, &record.Result); err != nil {
		return runtimedata.SourceOperationRecord{}, fmt.Errorf("decode source result: %w", err)
	}
	if err := json.Unmarshal(evidenceJSON, &record.Evidence); err != nil {
		return runtimedata.SourceOperationRecord{}, fmt.Errorf("decode source evidence: %w", err)
	}
	if err := json.Unmarshal(evaluationJSON, &record.Evaluation); err != nil {
		return runtimedata.SourceOperationRecord{}, fmt.Errorf("decode source evaluation: %w", err)
	}
	if err := record.ValidateForCommand(command); err != nil {
		return runtimedata.SourceOperationRecord{}, err
	}
	return record, nil
}

func (o *Owner) sourceInvocationReserved(ctx context.Context, tx *sql.Tx, id string) (bool, error) {
	var marker int
	err := tx.QueryRowContext(ctx, o.query(`SELECT 1 FROM resource_run_creation_child_reservations WHERE source_invocation_id = %s`, 1), id).Scan(&marker)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func nullableUUID(raw string) any {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return raw
}

func nullableText(raw string) any {
	if raw == "" {
		return nil
	}
	return raw
}

func (o *Owner) loadCatalogDeclaration(ctx context.Context, tx *sql.Tx, bundleHash string, ref runtimedata.DeclarationRef) (runtimedata.Declaration, error) {
	var marker int
	if err := tx.QueryRowContext(ctx, o.query(`SELECT 1 FROM bundles WHERE bundle_hash = %s`, 1), bundleHash).Scan(&marker); errors.Is(err, sql.ErrNoRows) {
		return runtimedata.Declaration{}, runtimedata.NewDomainError(runtimedata.CodeContractNotFound, "bundle %s is not in the exact selected-store catalog", bundleHash)
	} else if err != nil {
		return runtimedata.Declaration{}, err
	}
	var declaration runtimedata.Declaration
	declaration.Ref = ref
	err := tx.QueryRowContext(ctx, o.query(`
		SELECT display_name, owner_flow_id, COALESCE(business_key_field, ''), schema_digest, canonical_schema_bytes
		FROM resource_bundle_declarations
		WHERE bundle_hash = %s AND package_key = %s AND event_name = %s
	`, 3), bundleHash, ref.PackageKey, ref.EventName).Scan(&declaration.Name, &declaration.OwnerFlowID,
		&declaration.BusinessKey, &declaration.SchemaDigest, &declaration.CanonicalSchema)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimedata.Declaration{}, runtimedata.NewDomainError(runtimedata.CodeContractNotFound, "resource declaration %s is not admitted by bundle %s", ref.Key(), bundleHash)
	}
	if err != nil {
		return runtimedata.Declaration{}, err
	}
	if runtimedata.SchemaDigestFor(declaration.CanonicalSchema) != declaration.SchemaDigest {
		return runtimedata.Declaration{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource declaration %s schema integrity failure", ref.Key())
	}
	return declaration, nil
}

func (o *Owner) loadHead(ctx context.Context, tx *sql.Tx, ref runtimedata.DeclarationRef, lock bool) (runtimedata.HeadResult, error) {
	query := o.query(`SELECT version_id, revision FROM resource_heads WHERE package_key = %s AND event_name = %s`, 2)
	if lock && o.dialect == dialectPostgres {
		query += " FOR UPDATE"
	}
	var version sql.NullString
	var revision uint64
	if err := tx.QueryRowContext(ctx, query, ref.PackageKey, ref.EventName).Scan(&version, &revision); err != nil {
		return runtimedata.HeadResult{}, err
	}
	head := runtimedata.AbsentHead()
	if version.Valid {
		head = runtimedata.VersionHead(runtimedata.VersionID(version.String))
	}
	return runtimedata.HeadResult{Before: head, After: head, Revision: revision}, nil
}

func (o *Owner) loadSourceEvaluationBase(ctx context.Context, tx *sql.Tx, head runtimedata.HeadResult, declaration runtimedata.DeclarationRef) (runtimedata.SourceEvaluationBase, error) {
	if head.Before.State == "absent" {
		return runtimedata.SourceEvaluationBase{State: "absent", Head: head}, nil
	}
	var payload, schemaJSON, manifestJSON []byte
	var schemaDigest, businessKey string
	var prunedAt any
	err := tx.QueryRowContext(ctx, o.query(`
		SELECT canonical_jsonl, schema_digest, canonical_schema_bytes, COALESCE(business_key_field, ''), manifest_json, pruned_at
		FROM resource_versions WHERE version_id = %s
	`, 1), head.Before.VersionID).Scan(&payload, &schemaDigest, &schemaJSON, &businessKey, &manifestJSON, &prunedAt)
	if err != nil || payload == nil || prunedAt != nil {
		return runtimedata.SourceEvaluationBase{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource head %s has missing or pruned version payload", head.Before.VersionID)
	}
	var manifest runtimedata.Manifest
	if err := json.Unmarshal(manifestJSON, &manifest); err != nil || manifest.SchemaDigest != runtimedata.SchemaDigest(schemaDigest) ||
		runtimedata.SchemaDigestFor(schemaJSON) != runtimedata.SchemaDigest(schemaDigest) || manifest.Declaration != declaration {
		return runtimedata.SourceEvaluationBase{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource head %s manifest is contradictory", head.Before.VersionID)
	}
	businessKey, err = schemaBusinessKey(schemaJSON, businessKey)
	if err != nil {
		return runtimedata.SourceEvaluationBase{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource head %s business key schema is contradictory", head.Before.VersionID)
	}
	base := runtimedata.SourceEvaluationBase{
		State: "version", Head: head, Manifest: &manifest, BusinessKey: businessKey,
		CanonicalSchema: append([]byte(nil), schemaJSON...), CanonicalJSONL: append([]byte(nil), payload...),
	}
	if _, err := base.Validate(declaration); err != nil {
		return runtimedata.SourceEvaluationBase{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource head %s payload failed immutable verification: %v", head.Before.VersionID, err)
	}
	return base, nil
}

func (o *Owner) Show(ctx context.Context, bundleHash string, ref runtimedata.DeclarationRef) (runtimedata.ResourceSnapshot, error) {
	if err := o.requireCurrent(); err != nil {
		return runtimedata.ResourceSnapshot{}, err
	}
	if err := ref.Validate(); err != nil {
		return runtimedata.ResourceSnapshot{}, err
	}
	var snapshot runtimedata.ResourceSnapshot
	err := o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		declaration := runtimedata.Declaration{Ref: ref}
		if strings.TrimSpace(bundleHash) != "" {
			var err error
			declaration, err = o.loadCatalogDeclaration(txctx, tx, bundleHash, ref)
			if err != nil {
				return err
			}
		} else {
			var marker int
			if err := tx.QueryRowContext(txctx, o.query(`SELECT 1 FROM resource_declarations WHERE package_key = %s AND event_name = %s`, 2), ref.PackageKey, ref.EventName).Scan(&marker); errors.Is(err, sql.ErrNoRows) {
				return runtimedata.NewDomainError(runtimedata.CodeDeclarationMissing, "data declaration %s does not exist", ref.Key())
			} else if err != nil {
				return err
			}
		}
		head, err := o.loadHead(txctx, tx, ref, false)
		if err != nil {
			return err
		}
		versions, err := o.loadVersions(txctx, tx, declaration)
		if err != nil {
			return err
		}
		snapshot = runtimedata.ResourceSnapshot{Declaration: declaration, Head: head, Versions: versions}
		return nil
	})
	return snapshot, err
}

func (o *Owner) ListDeclarationSummaries(ctx context.Context, bundleHash string) ([]runtimedata.DeclarationSummary, error) {
	if err := o.requireCurrent(); err != nil {
		return nil, err
	}
	if err := runtimebundleidentity.ValidateCanonicalHash(bundleHash); err != nil {
		return nil, err
	}
	var summaries []runtimedata.DeclarationSummary
	err := o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(txctx, o.query(`
			SELECT d.package_key, d.event_name, d.display_name, d.schema_digest, d.canonical_schema_bytes,
			       h.version_id, h.revision,
			       (SELECT COUNT(*) FROM resource_versions v
			        WHERE v.package_key = d.package_key AND v.event_name = d.event_name),
			       (SELECT COUNT(*) FROM resource_versions v
			        WHERE v.package_key = d.package_key AND v.event_name = d.event_name AND v.pruned_at IS NULL),
			       (SELECT COALESCE(SUM(LENGTH(v.canonical_jsonl)), 0) FROM resource_versions v
			        WHERE v.package_key = d.package_key AND v.event_name = d.event_name AND v.pruned_at IS NULL),
			       (SELECT COUNT(*) FROM resource_versions v
			        WHERE v.package_key = d.package_key AND v.event_name = d.event_name AND v.version_id = h.version_id)
			FROM resource_bundle_declarations d
			LEFT JOIN resource_heads h ON h.package_key = d.package_key AND h.event_name = d.event_name
			WHERE d.bundle_hash = %s ORDER BY d.package_key, d.event_name
		`, 1), bundleHash)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var summary runtimedata.DeclarationSummary
			var canonicalSchema []byte
			var headVersion sql.NullString
			var revision sql.NullInt64
			var versionCount, materializedCount, materializedBytes, headMatchCount int64
			if err := rows.Scan(&summary.Declaration.PackageKey, &summary.Declaration.EventName, &summary.LocalName,
				&summary.SchemaDigest, &canonicalSchema, &headVersion, &revision, &versionCount,
				&materializedCount, &materializedBytes, &headMatchCount); err != nil {
				return err
			}
			if runtimedata.SchemaDigestFor(canonicalSchema) != summary.SchemaDigest || !revision.Valid || revision.Int64 < 0 {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "bundle %s data declaration aggregate is contradictory", bundleHash)
			}
			if versionCount < 0 || materializedCount < 0 || materializedBytes < 0 || headMatchCount < 0 ||
				versionCount > int64(^uint(0)>>1) || materializedCount > int64(^uint(0)>>1) || materializedBytes > int64(^uint(0)>>1) {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "bundle %s data declaration summary exceeds supported bounds", bundleHash)
			}
			summary.Head = runtimedata.AbsentHead()
			if headVersion.Valid {
				if revision.Int64 == 0 {
					return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "bundle %s version data declaration head has zero revision", bundleHash)
				}
				summary.Head = runtimedata.VersionHead(runtimedata.VersionID(headVersion.String))
				if headMatchCount != 1 {
					return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "bundle %s data declaration head is outside its version inventory", bundleHash)
				}
			} else if headMatchCount != 0 || revision.Int64 != 0 {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "bundle %s absent data declaration head has mutation evidence", bundleHash)
			}
			summary.VersionCount = int(versionCount)
			summary.MaterializedVersionCount = int(materializedCount)
			summary.MaterializedBytes = int(materializedBytes)
			if err := summary.Validate(); err != nil {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "bundle %s data declaration summary is contradictory: %v", bundleHash, err)
			}
			summaries = append(summaries, summary)
		}
		return rows.Err()
	})
	return summaries, err
}

func validateReadPageLimit(limit int) error {
	if limit < 1 || limit > runtimedata.MaxPublicPageItems+1 {
		return fmt.Errorf("data read limit must be between 1 and %d", runtimedata.MaxPublicPageItems+1)
	}
	return nil
}

func (o *Owner) requireDeclarationExistsTx(ctx context.Context, tx *sql.Tx, ref runtimedata.DeclarationRef) error {
	var marker int
	err := tx.QueryRowContext(ctx, o.query(`SELECT 1 FROM resource_declarations WHERE package_key = %s AND event_name = %s`, 2), ref.PackageKey, ref.EventName).Scan(&marker)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimedata.NewDomainError(runtimedata.CodeDeclarationMissing, "data declaration %s does not exist", ref.Key())
	}
	return err
}

func (o *Owner) scanVersionSummary(row interface{ Scan(...any) error }, ref runtimedata.DeclarationRef) (runtimedata.VersionSummary, uint64, error) {
	var summary runtimedata.VersionSummary
	var sequence uint64
	var manifestJSON, canonicalSchema []byte
	var payloadBytes sql.NullInt64
	var prunedRaw any
	var provenanceCount int64
	if err := row.Scan(&summary.VersionID, &sequence, &manifestJSON, &summary.BusinessKey, &canonicalSchema,
		&payloadBytes, &prunedRaw, &provenanceCount); err != nil {
		return runtimedata.VersionSummary{}, 0, err
	}
	if err := json.Unmarshal(manifestJSON, &summary.Manifest); err != nil {
		return runtimedata.VersionSummary{}, 0, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s manifest is invalid", summary.VersionID)
	}
	summary.Declaration = ref
	summary.Alias = fmt.Sprintf("v%d", sequence)
	if _, err := schemaBusinessKey(canonicalSchema, summary.BusinessKey); err != nil ||
		runtimedata.SchemaDigestFor(canonicalSchema) != summary.Manifest.SchemaDigest || provenanceCount < 1 {
		return runtimedata.VersionSummary{}, 0, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s metadata is contradictory", summary.VersionID)
	}
	_, pruned, err := persistedTime(prunedRaw)
	if err != nil {
		return runtimedata.VersionSummary{}, 0, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s prune evidence is contradictory", summary.VersionID)
	}
	if pruned {
		if payloadBytes.Valid {
			return runtimedata.VersionSummary{}, 0, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s tombstone retains payload", summary.VersionID)
		}
		summary.PayloadState = "pruned"
	} else {
		if !payloadBytes.Valid || payloadBytes.Int64 < 0 || payloadBytes.Int64 > int64(^uint(0)>>1) {
			return runtimedata.VersionSummary{}, 0, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s payload metadata is contradictory", summary.VersionID)
		}
		summary.PayloadState = "materialized"
		summary.MaterializedBytes = int(payloadBytes.Int64)
	}
	if err := summary.Validate(); err != nil {
		return runtimedata.VersionSummary{}, 0, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s summary is contradictory: %v", summary.VersionID, err)
	}
	return summary, sequence, nil
}

func (o *Owner) ListVersionSummaries(ctx context.Context, ref runtimedata.DeclarationRef, afterSequence uint64, limit int) ([]runtimedata.VersionSummary, error) {
	if err := o.requireCurrent(); err != nil {
		return nil, err
	}
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	if err := validateReadPageLimit(limit); err != nil {
		return nil, err
	}
	var summaries []runtimedata.VersionSummary
	err := o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := o.requireDeclarationExistsTx(txctx, tx, ref); err != nil {
			return err
		}
		if afterSequence != 0 {
			var marker int
			if err := tx.QueryRowContext(txctx, o.query(`SELECT 1 FROM resource_versions WHERE package_key = %s AND event_name = %s AND sequence_alias = %s`, 3), ref.PackageKey, ref.EventName, afterSequence).Scan(&marker); err != nil {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version cursor anchor is absent for declaration %s", ref.Key())
			}
		}
		rows, err := tx.QueryContext(txctx, o.query(`
			SELECT v.version_id, v.sequence_alias, v.manifest_json, COALESCE(v.business_key_field, ''),
			       v.canonical_schema_bytes, LENGTH(v.canonical_jsonl), v.pruned_at,
			       (SELECT COUNT(*) FROM resource_version_provenance p WHERE p.version_id = v.version_id)
			FROM resource_versions v
			WHERE v.package_key = %s AND v.event_name = %s AND v.sequence_alias > %s
			ORDER BY v.sequence_alias LIMIT %s
		`, 4), ref.PackageKey, ref.EventName, afterSequence, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		last := afterSequence
		for rows.Next() {
			summary, sequence, err := o.scanVersionSummary(rows, ref)
			if err != nil {
				return err
			}
			if sequence <= last {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version inventory order is contradictory")
			}
			last = sequence
			summaries = append(summaries, summary)
		}
		return rows.Err()
	})
	return summaries, err
}

func (o *Owner) ResolveVersionSummary(ctx context.Context, ref runtimedata.DeclarationRef, selector runtimedata.VersionSelector) (runtimedata.VersionSummary, error) {
	if err := o.requireCurrent(); err != nil {
		return runtimedata.VersionSummary{}, err
	}
	if err := ref.Validate(); err != nil {
		return runtimedata.VersionSummary{}, err
	}
	if err := selector.Validate(); err != nil {
		return runtimedata.VersionSummary{}, err
	}
	var summary runtimedata.VersionSummary
	err := o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		summary, err = o.resolveVersionSummaryTx(txctx, tx, ref, selector, false)
		return err
	})
	return summary, err
}

func (o *Owner) resolveVersionSummaryTx(ctx context.Context, tx *sql.Tx, ref runtimedata.DeclarationRef, selector runtimedata.VersionSelector, lock bool) (runtimedata.VersionSummary, error) {
	if err := o.requireDeclarationExistsTx(ctx, tx, ref); err != nil {
		return runtimedata.VersionSummary{}, err
	}
	var where string
	var args []any
	switch selector.Kind {
	case "head":
		where = `v.package_key = %s AND v.event_name = %s AND v.version_id = (SELECT h.version_id FROM resource_heads h WHERE h.package_key = %s AND h.event_name = %s)`
		args = []any{ref.PackageKey, ref.EventName, ref.PackageKey, ref.EventName}
	case "version":
		where = `v.package_key = %s AND v.event_name = %s AND v.version_id = %s`
		args = []any{ref.PackageKey, ref.EventName, selector.VersionID}
	case "alias":
		where = `v.package_key = %s AND v.event_name = %s AND v.sequence_alias = %s`
		args = []any{ref.PackageKey, ref.EventName, selector.SequenceAlias}
	}
	query := `SELECT v.version_id, v.sequence_alias, v.manifest_json, COALESCE(v.business_key_field, ''),
	                 v.canonical_schema_bytes, LENGTH(v.canonical_jsonl), v.pruned_at,
	                 (SELECT COUNT(*) FROM resource_version_provenance p WHERE p.version_id = v.version_id)
	          FROM resource_versions v WHERE ` + where
	if lock && o.dialect == dialectPostgres {
		query += ` FOR UPDATE`
	}
	summary, _, err := o.scanVersionSummary(tx.QueryRowContext(ctx, o.query(query, len(args)), args...), ref)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimedata.VersionSummary{}, runtimedata.NewDomainError(runtimedata.CodeVersionMissing, "selected resource version does not exist for declaration %s", ref.Key())
	}
	return summary, err
}

func (o *Owner) ResolveVersionPayload(ctx context.Context, ref runtimedata.DeclarationRef, selector runtimedata.VersionSelector) (runtimedata.VersionSummary, runtimedata.Version, error) {
	if err := o.requireCurrent(); err != nil {
		return runtimedata.VersionSummary{}, runtimedata.Version{}, err
	}
	if err := ref.Validate(); err != nil {
		return runtimedata.VersionSummary{}, runtimedata.Version{}, err
	}
	if err := selector.Validate(); err != nil {
		return runtimedata.VersionSummary{}, runtimedata.Version{}, err
	}
	var summary runtimedata.VersionSummary
	var version runtimedata.Version
	err := o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		summary, err = o.resolveVersionSummaryTx(txctx, tx, ref, selector, true)
		if err != nil {
			return err
		}
		var found bool
		version, found, err = o.loadStoredVersionPayload(txctx, tx, ref, summary.VersionID)
		if err != nil {
			return err
		}
		if !found {
			return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "selected resource version %s disappeared during payload resolution", summary.VersionID)
		}
		if err := summary.ValidateVersion(version); err != nil {
			return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "selected resource version %s metadata contradicts payload: %v", summary.VersionID, err)
		}
		return nil
	})
	return summary, version, err
}

func (o *Owner) ListVersionProvenance(ctx context.Context, versionID runtimedata.VersionID, afterSequence uint64, limit int) ([]runtimedata.Provenance, error) {
	if err := o.requireCurrent(); err != nil {
		return nil, err
	}
	if err := versionID.Validate(); err != nil {
		return nil, err
	}
	if err := validateReadPageLimit(limit); err != nil {
		return nil, err
	}
	var out []runtimedata.Provenance
	err := o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var marker int
		if err := tx.QueryRowContext(txctx, o.query(`SELECT 1 FROM resource_versions WHERE version_id = %s`, 1), versionID).Scan(&marker); errors.Is(err, sql.ErrNoRows) {
			return runtimedata.NewDomainError(runtimedata.CodeVersionMissing, "resource version %s does not exist", versionID)
		} else if err != nil {
			return err
		}
		if afterSequence != 0 {
			if err := tx.QueryRowContext(txctx, o.query(`SELECT 1 FROM resource_version_provenance WHERE version_id = %s AND provenance_sequence = %s`, 2), versionID, afterSequence).Scan(&marker); err != nil {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s provenance cursor anchor is absent", versionID)
			}
		}
		rows, err := tx.QueryContext(txctx, o.query(`
			SELECT provenance_sequence, producer_kind, producer_id, actor, provenance_json, committed_at
			FROM resource_version_provenance
			WHERE version_id = %s AND provenance_sequence > %s
			ORDER BY provenance_sequence LIMIT %s
		`, 3), versionID, afterSequence, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		last := afterSequence
		for rows.Next() {
			provenance, err := scanVersionProvenance(rows, versionID)
			if err != nil {
				return err
			}
			if provenance.Sequence <= last {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s provenance order is contradictory", versionID)
			}
			last = provenance.Sequence
			out = append(out, provenance)
		}
		return rows.Err()
	})
	return out, err
}

func (o *Owner) ListPins(ctx context.Context, versionID runtimedata.VersionID, afterRunID string, limit int) ([]runtimedata.Pin, error) {
	if err := o.requireCurrent(); err != nil {
		return nil, err
	}
	if err := versionID.Validate(); err != nil {
		return nil, err
	}
	if err := validateReadPageLimit(limit); err != nil {
		return nil, err
	}
	var pins []runtimedata.Pin
	err := o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var packageKey, eventName string
		var schemaDigest runtimedata.SchemaDigest
		if err := tx.QueryRowContext(txctx, o.query(`SELECT package_key, event_name, schema_digest FROM resource_versions WHERE version_id = %s`, 1), versionID).Scan(&packageKey, &eventName, &schemaDigest); errors.Is(err, sql.ErrNoRows) {
			return runtimedata.NewDomainError(runtimedata.CodeVersionMissing, "resource version %s does not exist", versionID)
		} else if err != nil {
			return err
		}
		ref := runtimedata.DeclarationRef{PackageKey: packageKey, EventName: eventName}
		query := `
			SELECT p.run_id, r.status, p.package_key, p.event_name, p.schema_digest, p.version_id, p.selection
			FROM resource_version_pins p JOIN runs r ON r.run_id = p.run_id
			WHERE p.version_id = %s`
		args := []any{versionID}
		if afterRunID != "" {
			query += ` AND p.run_id > %s`
			args = append(args, afterRunID)
		}
		query += ` ORDER BY p.run_id LIMIT %s`
		args = append(args, limit)
		rows, err := tx.QueryContext(txctx, o.query(query, len(args)), args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		last := afterRunID
		for rows.Next() {
			var pin runtimedata.Pin
			if err := rows.Scan(&pin.RunID, &pin.RunState, &pin.Declaration.PackageKey, &pin.Declaration.EventName, &pin.SchemaDigest, &pin.VersionID, &pin.Selection); err != nil {
				return err
			}
			if err := pin.Validate(); err != nil || pin.Declaration != ref || pin.SchemaDigest != schemaDigest || pin.VersionID != versionID || pin.RunID <= last {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s pin aggregate is contradictory", versionID)
			}
			last = pin.RunID
			pins = append(pins, pin)
		}
		return rows.Err()
	})
	return pins, err
}

func (o *Owner) ListHeadHistory(ctx context.Context, ref runtimedata.DeclarationRef, afterRevision uint64, limit int) ([]runtimedata.HeadHistory, error) {
	if err := o.requireCurrent(); err != nil {
		return nil, err
	}
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	if err := validateReadPageLimit(limit); err != nil {
		return nil, err
	}
	var history []runtimedata.HeadHistory
	err := o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := o.requireDeclarationExistsTx(txctx, tx, ref); err != nil {
			return err
		}
		if afterRevision != 0 {
			var marker int
			if err := tx.QueryRowContext(txctx, o.query(`SELECT 1 FROM resource_head_history WHERE package_key = %s AND event_name = %s AND revision = %s`, 3), ref.PackageKey, ref.EventName, afterRevision).Scan(&marker); err != nil {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource head-history cursor anchor is absent for declaration %s", ref.Key())
			}
		}
		rows, err := tx.QueryContext(txctx, o.query(`
			SELECT revision, before_version_id, after_version_id, operation_kind, operation_id, committed_at
			FROM resource_head_history
			WHERE package_key = %s AND event_name = %s AND revision > %s
			ORDER BY revision LIMIT %s
		`, 4), ref.PackageKey, ref.EventName, afterRevision, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		last := afterRevision
		for rows.Next() {
			var item runtimedata.HeadHistory
			var before sql.NullString
			var after string
			var committedRaw any
			item.Declaration = ref
			if err := rows.Scan(&item.Revision, &before, &after, &item.Operation, &item.OperationID, &committedRaw); err != nil {
				return err
			}
			committedAt, present, err := persistedTime(committedRaw)
			if err != nil || !present {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource head history revision %d timestamp is contradictory", item.Revision)
			}
			item.CommittedAt = committedAt
			item.Before = runtimedata.AbsentHead()
			if before.Valid {
				item.Before = runtimedata.VersionHead(runtimedata.VersionID(before.String))
			}
			item.After = runtimedata.VersionHead(runtimedata.VersionID(after))
			if item.Revision <= last || item.Before.Validate() != nil || item.After.Validate() != nil {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource head history revision %d is contradictory", item.Revision)
			}
			last = item.Revision
			history = append(history, item)
		}
		return rows.Err()
	})
	return history, err
}

func (o *Owner) LoadSourceOperation(ctx context.Context, id string) (runtimedata.SourceOperationRecord, error) {
	if err := o.requireCurrent(); err != nil {
		return runtimedata.SourceOperationRecord{}, err
	}
	var record runtimedata.SourceOperationRecord
	err := o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		stored, found, err := o.readStoredSourceReceipt(txctx, tx, id)
		if err != nil {
			return err
		}
		if !found {
			return runtimedata.NewDomainError(runtimedata.CodeOperationMissing, "source operation %s does not exist", id)
		}
		record, _, err = o.validateStoredSourceReceipt(txctx, tx, id, stored)
		if err != nil {
			return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "source operation %s evidence is contradictory: %v", id, err)
		}
		return nil
	})
	return record, err
}

func (o *Owner) LoadPruneOperation(ctx context.Context, id string) (runtimedata.PruneOperationResult, error) {
	if err := o.requireCurrent(); err != nil {
		return runtimedata.PruneOperationResult{}, err
	}
	var result runtimedata.PruneOperationResult
	err := o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		stored, found, err := o.readStoredPruneReceipt(txctx, tx, id)
		if err != nil {
			return err
		}
		if !found {
			return runtimedata.NewDomainError(runtimedata.CodeOperationMissing, "prune operation %s does not exist", id)
		}
		result, _, _, err = o.validateStoredPruneReceipt(txctx, tx, id, stored)
		if err != nil {
			return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "prune operation %s evidence is contradictory: %v", id, err)
		}
		return nil
	})
	return result, err
}

func (o *Owner) LoadPruneOperationPins(ctx context.Context, id string) ([]runtimedata.Pin, error) {
	if err := o.requireCurrent(); err != nil {
		return nil, err
	}
	var pins []runtimedata.Pin
	err := o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		stored, found, err := o.readStoredPruneReceipt(txctx, tx, id)
		if err != nil {
			return err
		}
		if !found {
			return runtimedata.NewDomainError(runtimedata.CodeOperationMissing, "prune operation %s does not exist", id)
		}
		_, _, pins, err = o.validateStoredPruneReceipt(txctx, tx, id, stored)
		if err != nil {
			return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "prune operation %s evidence is contradictory: %v", id, err)
		}
		return nil
	})
	return pins, err
}

func (o *Owner) LoadPins(ctx context.Context, versionID runtimedata.VersionID) ([]runtimedata.Pin, error) {
	if err := o.requireCurrent(); err != nil {
		return nil, err
	}
	if err := versionID.Validate(); err != nil {
		return nil, err
	}
	var pins []runtimedata.Pin
	err := o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		pins, err = o.loadPins(txctx, tx, versionID)
		return err
	})
	return pins, err
}

func (o *Owner) LoadHeadHistory(ctx context.Context, ref runtimedata.DeclarationRef) ([]runtimedata.HeadHistory, error) {
	if err := o.requireCurrent(); err != nil {
		return nil, err
	}
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	var history []runtimedata.HeadHistory
	err := o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(txctx, o.query(`
			SELECT revision, before_version_id, after_version_id, operation_kind, operation_id, committed_at
			FROM resource_head_history WHERE package_key = %s AND event_name = %s ORDER BY revision
		`, 2), ref.PackageKey, ref.EventName)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var item runtimedata.HeadHistory
			var before sql.NullString
			var after string
			item.Declaration = ref
			if err := rows.Scan(&item.Revision, &before, &after, &item.Operation, &item.OperationID, &item.CommittedAt); err != nil {
				return err
			}
			item.Before = runtimedata.AbsentHead()
			if before.Valid {
				item.Before = runtimedata.VersionHead(runtimedata.VersionID(before.String))
			}
			item.After = runtimedata.VersionHead(runtimedata.VersionID(after))
			if item.Before.Validate() != nil || item.After.Validate() != nil {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource head history revision %d is contradictory", item.Revision)
			}
			history = append(history, item)
		}
		return rows.Err()
	})
	return history, err
}

func (o *Owner) LoadRunResourceAccess(ctx context.Context, runID string, declarations []runtimedata.DeclarationRef) ([]runtimedata.ResourceAccessItem, error) {
	if err := o.requireCurrent(); err != nil {
		return nil, err
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, runtimedata.NewDomainError(runtimedata.CodeAccessDenied, "run identity is required for durable data access")
	}
	refs := append([]runtimedata.DeclarationRef(nil), declarations...)
	sort.Slice(refs, func(i, j int) bool { return runtimedata.CompareDeclarationRef(refs[i], refs[j]) < 0 })
	for index := range refs {
		if err := refs[index].Validate(); err != nil {
			return nil, err
		}
		if index > 0 && refs[index] == refs[index-1] {
			return nil, runtimedata.NewDomainError(runtimedata.CodeAccessDenied, "resource access repeats declaration %s", refs[index].Key())
		}
	}

	var out []runtimedata.ResourceAccessItem
	err := o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if exists, err := o.runExistsTx(txctx, tx, runID); err != nil {
			return err
		} else if !exists {
			return runtimedata.NewDomainError(runtimedata.CodeAccessDenied, "run %s does not exist", runID)
		}
		for _, ref := range refs {
			var schemaDigest runtimedata.SchemaDigest
			var versionID runtimedata.VersionID
			err := tx.QueryRowContext(txctx, o.query(`
				SELECT schema_digest, version_id FROM resource_version_pins
				WHERE run_id = %s AND package_key = %s AND event_name = %s
			`, 3), runID, ref.PackageKey, ref.EventName).Scan(&schemaDigest, &versionID)
			if errors.Is(err, sql.ErrNoRows) {
				return runtimedata.NewDomainError(runtimedata.CodeAccessDenied, "run %s has no exact pin for declaration %s", runID, ref.Key())
			}
			if err != nil {
				return err
			}
			version, found, err := o.loadStoredVersion(txctx, tx, ref, versionID)
			if err != nil {
				return err
			}
			if !found {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "run %s pin references missing version %s", runID, versionID)
			}
			if version.PrunedAt != nil {
				return runtimedata.NewDomainError(runtimedata.CodePayloadPruned, "run %s pin references pruned version %s", runID, versionID)
			}
			if schemaDigest != version.Manifest.SchemaDigest {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "run %s pin schema contradicts version %s", runID, versionID)
			}
			mountPath, err := runtimedata.ResourceMountPath(ref)
			if err != nil {
				return err
			}
			out = append(out, runtimedata.ResourceAccessItem{
				Kind: "resource", Declaration: ref, VersionID: versionID, SchemaDigest: schemaDigest,
				RowCount: version.Manifest.RowCount, MountPath: mountPath, BusinessKey: version.BusinessKey,
				Schema: append([]byte(nil), version.CanonicalSchema...), Content: append([]byte(nil), version.CanonicalJSONL...),
			})
		}
		return nil
	})
	return out, err
}

func (o *Owner) loadVersions(ctx context.Context, tx *sql.Tx, declaration runtimedata.Declaration) ([]runtimedata.Version, error) {
	rows, err := tx.QueryContext(ctx, o.query(`
		SELECT version_id, sequence_alias, manifest_json, COALESCE(business_key_field, ''), canonical_schema_bytes, canonical_jsonl, pruned_at
		FROM resource_versions WHERE package_key = %s AND event_name = %s ORDER BY sequence_alias
	`, 2), declaration.Ref.PackageKey, declaration.Ref.EventName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var versions []runtimedata.Version
	for rows.Next() {
		var version runtimedata.Version
		var manifestJSON []byte
		var pruned any
		if err := rows.Scan(&version.VersionID, &version.SequenceAlias, &manifestJSON, &version.BusinessKey, &version.CanonicalSchema, &version.CanonicalJSONL, &pruned); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(manifestJSON, &version.Manifest); err != nil {
			return nil, err
		}
		value, present, err := persistedTime(pruned)
		if err != nil {
			return nil, err
		}
		if present {
			version.PrunedAt = &value
		}
		derivedID, err := version.Manifest.VersionID()
		if err != nil || derivedID != version.VersionID || version.Manifest.Declaration != declaration.Ref ||
			runtimedata.SchemaDigestFor(version.CanonicalSchema) != version.Manifest.SchemaDigest {
			return nil, fmt.Errorf("resource version %s immutable evidence is contradictory", version.VersionID)
		}
		if version.PrunedAt == nil {
			businessKey, err := schemaBusinessKey(version.CanonicalSchema, version.BusinessKey)
			if err != nil {
				return nil, err
			}
			compiled, defects := runtimedata.CompileJSONL(declaration.Ref, mustDecodeSchema(version.CanonicalSchema), businessKey, version.CanonicalJSONL)
			if len(defects) > 0 || compiled.VersionID != version.VersionID {
				return nil, fmt.Errorf("resource version %s payload failed immutable verification", version.VersionID)
			}
		} else if version.CanonicalJSONL != nil {
			return nil, fmt.Errorf("resource version %s retains payload after prune", version.VersionID)
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for index := range versions {
		versions[index].Provenance, err = o.loadVersionProvenance(ctx, tx, versions[index].VersionID)
		if err != nil {
			return nil, err
		}
		if len(versions[index].Provenance) == 0 {
			return nil, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s has no provenance", versions[index].VersionID)
		}
	}
	return versions, nil
}

func (o *Owner) loadVersionProvenance(ctx context.Context, tx *sql.Tx, versionID runtimedata.VersionID) ([]runtimedata.Provenance, error) {
	rows, err := tx.QueryContext(ctx, o.query(`
		SELECT provenance_sequence, producer_kind, producer_id, actor, provenance_json, committed_at
		FROM resource_version_provenance WHERE version_id = %s ORDER BY provenance_sequence
	`, 1), versionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []runtimedata.Provenance
	for rows.Next() {
		provenance, err := scanVersionProvenance(rows, versionID)
		if err != nil {
			return nil, err
		}
		if len(out) > 0 && out[len(out)-1].Sequence >= provenance.Sequence {
			return nil, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s provenance order is contradictory", versionID)
		}
		out = append(out, provenance)
	}
	return out, rows.Err()
}

func scanVersionProvenance(row interface{ Scan(...any) error }, versionID runtimedata.VersionID) (runtimedata.Provenance, error) {
	var sequence uint64
	var kind, producerID, actor string
	var raw []byte
	var committedRaw any
	if err := row.Scan(&sequence, &kind, &producerID, &actor, &raw, &committedRaw); err != nil {
		return runtimedata.Provenance{}, err
	}
	committedAt, present, err := persistedTime(committedRaw)
	if err != nil || !present {
		return runtimedata.Provenance{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s provenance timestamp is contradictory", versionID)
	}
	ref, err := runtimedata.NewProvenanceRef(kind, producerID)
	if err != nil {
		return runtimedata.Provenance{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s provenance producer is contradictory", versionID)
	}
	var provenance runtimedata.Provenance
	if err := json.Unmarshal(raw, &provenance); err != nil {
		return runtimedata.Provenance{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s provenance is contradictory", versionID)
	}
	provenance.Sequence = sequence
	canonicalProjection, _ := json.Marshal(provenance)
	if err := provenance.Validate(); err != nil || provenance.VersionID != versionID || provenance.ProducerRef != ref ||
		provenance.Actor != actor || provenance.CommittedAt != committedAt || !jsonEqual(raw, canonicalProjection) {
		return runtimedata.Provenance{}, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s provenance is contradictory", versionID)
	}
	return provenance, nil
}

func (o *Owner) loadStoredVersion(ctx context.Context, tx *sql.Tx, ref runtimedata.DeclarationRef, versionID runtimedata.VersionID) (runtimedata.Version, bool, error) {
	version, found, err := o.loadStoredVersionPayload(ctx, tx, ref, versionID)
	if err != nil || !found {
		return version, found, err
	}
	version.Provenance, err = o.loadVersionProvenance(ctx, tx, versionID)
	if err != nil {
		return runtimedata.Version{}, false, err
	}
	if len(version.Provenance) == 0 {
		return runtimedata.Version{}, false, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s has no provenance", versionID)
	}
	return version, true, nil
}

func (o *Owner) loadStoredVersionPayload(ctx context.Context, tx *sql.Tx, ref runtimedata.DeclarationRef, versionID runtimedata.VersionID) (runtimedata.Version, bool, error) {
	var version runtimedata.Version
	var manifestJSON []byte
	var pruned any
	version.VersionID = versionID
	query := o.query(`
		SELECT sequence_alias, manifest_json, COALESCE(business_key_field, ''), canonical_schema_bytes, canonical_jsonl, pruned_at
		FROM resource_versions WHERE version_id = %s AND package_key = %s AND event_name = %s
	`, 3)
	if o.dialect == dialectPostgres {
		query += " FOR UPDATE"
	}
	err := tx.QueryRowContext(ctx, query, versionID, ref.PackageKey, ref.EventName).Scan(
		&version.SequenceAlias, &manifestJSON, &version.BusinessKey, &version.CanonicalSchema, &version.CanonicalJSONL, &pruned,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimedata.Version{}, false, nil
	}
	if err != nil {
		return runtimedata.Version{}, false, err
	}
	if err := json.Unmarshal(manifestJSON, &version.Manifest); err != nil {
		return runtimedata.Version{}, false, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s manifest is invalid", versionID)
	}
	value, present, err := persistedTime(pruned)
	if err != nil {
		return runtimedata.Version{}, false, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s prune evidence is invalid", versionID)
	}
	if present {
		version.PrunedAt = &value
	}
	derived, deriveErr := version.Manifest.VersionID()
	if deriveErr != nil || derived != versionID || version.Manifest.Declaration != ref ||
		runtimedata.SchemaDigestFor(version.CanonicalSchema) != version.Manifest.SchemaDigest {
		return runtimedata.Version{}, false, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s immutable evidence is contradictory", versionID)
	}
	businessKey, err := schemaBusinessKey(version.CanonicalSchema, version.BusinessKey)
	if err != nil {
		return runtimedata.Version{}, false, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s row key is contradictory", versionID)
	}
	if version.PrunedAt == nil {
		if version.CanonicalJSONL == nil {
			return runtimedata.Version{}, false, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s has no payload or tombstone", versionID)
		}
		compiled, defects := runtimedata.CompileJSONL(ref, mustDecodeSchema(version.CanonicalSchema), businessKey, version.CanonicalJSONL)
		if len(defects) != 0 || compiled.VersionID != versionID {
			return runtimedata.Version{}, false, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s payload is contradictory", versionID)
		}
	} else if version.CanonicalJSONL != nil {
		return runtimedata.Version{}, false, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s tombstone retains payload", versionID)
	}
	return version, true, nil
}

func schemaBusinessKey(schemaJSON []byte, fallback string) (string, error) {
	var schema map[string]any
	if err := json.Unmarshal(schemaJSON, &schema); err != nil {
		return "", err
	}
	declared, _ := schema["x-swarm-dataset-key"].(string)
	if declared != fallback {
		return "", fmt.Errorf("persisted business key %q contradicts schema owner %q", fallback, declared)
	}
	if fallback == "" {
		return "", nil
	}
	properties, ok := schema["properties"].(map[string]any)
	if !ok || properties[fallback] == nil {
		return "", fmt.Errorf("business key %q is absent from schema", fallback)
	}
	return fallback, nil
}

func (o *Owner) Prune(ctx context.Context, command runtimedata.PruneCommand) (runtimedata.PruneOperationResult, error) {
	if err := o.requireCurrent(); err != nil {
		return runtimedata.PruneOperationResult{}, err
	}
	hash, requestJSON, err := command.RequestHash()
	if err != nil {
		return runtimedata.PruneOperationResult{}, err
	}
	var result runtimedata.PruneOperationResult
	err = o.runTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		replayed, found, err := o.loadPruneReceipt(txctx, tx, command.PruneInvocationID, hash)
		if err != nil {
			return err
		}
		if found {
			result = replayed
			return nil
		}
		result = runtimedata.PruneOperationResult{
			PruneInvocationID: command.PruneInvocationID, Declaration: command.Declaration, VersionID: command.VersionID,
			ExpectedHead: command.ExpectedHead, ObservedHead: runtimedata.AbsentHead(), CompletedAt: o.currentTime(),
		}
		var marker int
		err = tx.QueryRowContext(txctx, o.query(`
			SELECT 1 FROM resource_declarations WHERE package_key = %s AND event_name = %s
		`, 2), command.Declaration.PackageKey, command.Declaration.EventName).Scan(&marker)
		if errors.Is(err, sql.ErrNoRows) {
			result.Outcome = "rejected"
			page := pagePruneDefects([]runtimedata.PruneDefect{{Code: "declaration_not_found", Message: "data declaration does not exist"}})
			result.Defects = &page
			return o.insertPruneReceipt(txctx, tx, command, hash, requestJSON, result, nil)
		}
		if err != nil {
			return err
		}
		head, err := o.loadHead(txctx, tx, command.Declaration, true)
		if err != nil {
			return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource declaration %s has no head aggregate", command.Declaration.Key())
		}
		result.ObservedHead = head.Before
		version, found, err := o.loadStoredVersion(txctx, tx, command.Declaration, command.VersionID)
		if err != nil {
			return err
		}
		if !found {
			result.Outcome = "rejected"
			page := pagePruneDefects([]runtimedata.PruneDefect{{Code: "version_not_found", Message: "data version does not exist for the declaration"}})
			result.Defects = &page
			return o.insertPruneReceipt(txctx, tx, command, hash, requestJSON, result, nil)
		}
		if head.Before.State == "version" && head.Before.VersionID != command.VersionID {
			current, currentFound, currentErr := o.loadStoredVersion(txctx, tx, command.Declaration, head.Before.VersionID)
			if currentErr != nil {
				return currentErr
			}
			if !currentFound || current.PrunedAt != nil {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource declaration %s current head %s has no materialized version aggregate", command.Declaration.Key(), head.Before.VersionID)
			}
		}
		pins, err := o.loadPins(txctx, tx, command.VersionID)
		if err != nil {
			return err
		}
		if err := validateStoredPins(command.Declaration, version, pins); err != nil {
			return err
		}
		switch {
		case !command.ExpectedHead.Equal(head.Before):
			result.Outcome = "head_conflict"
		case head.Before.State == "version" && head.Before.VersionID == command.VersionID:
			result.Outcome = "refused_current"
			result.CurrentVersionID = command.VersionID
		default:
			if len(pins) > 0 {
				result.Outcome = "refused_pinned"
				result.PinCount = len(pins)
				page := pagePins(pins)
				result.Pins = &page
				break
			}
			if version.PrunedAt != nil {
				result.Outcome = "already_pruned"
				result.PayloadBefore = "pruned"
				result.PayloadAfter = "pruned"
				break
			}
			result.PayloadBefore = "materialized"
			result.PayloadAfter = "pruned"
			result.Outcome = "pruned"
			mutation, err := tx.ExecContext(txctx, o.query(`
				UPDATE resource_versions SET canonical_jsonl = NULL, pruned_at = %s
				WHERE version_id = %s AND canonical_jsonl IS NOT NULL AND pruned_at IS NULL
			`, 2), result.CompletedAt, command.VersionID)
			if err != nil {
				return err
			}
			changed, _ := mutation.RowsAffected()
			if changed != 1 {
				return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s changed during prune", command.VersionID)
			}
		}
		var pinEvidence []runtimedata.Pin
		if result.Outcome == "refused_pinned" {
			pinEvidence = pins
		}
		return o.insertPruneReceipt(txctx, tx, command, hash, requestJSON, result, pinEvidence)
	})
	return result, err
}

func (o *Owner) loadPins(ctx context.Context, tx *sql.Tx, versionID runtimedata.VersionID) ([]runtimedata.Pin, error) {
	rows, err := tx.QueryContext(ctx, o.query(`
		SELECT p.run_id, r.status, p.package_key, p.event_name, p.schema_digest, p.version_id, p.selection
		FROM resource_version_pins p JOIN runs r ON r.run_id = p.run_id
		WHERE p.version_id = %s ORDER BY p.run_id
	`, 1), versionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pins []runtimedata.Pin
	for rows.Next() {
		var pin runtimedata.Pin
		if err := rows.Scan(&pin.RunID, &pin.RunState, &pin.Declaration.PackageKey, &pin.Declaration.EventName, &pin.SchemaDigest, &pin.VersionID, &pin.Selection); err != nil {
			return nil, err
		}
		pins = append(pins, pin)
	}
	return pins, rows.Err()
}

func validateStoredPins(ref runtimedata.DeclarationRef, version runtimedata.Version, pins []runtimedata.Pin) error {
	for _, pin := range pins {
		if strings.TrimSpace(pin.RunID) == "" || pin.Declaration != ref || pin.VersionID != version.VersionID ||
			pin.SchemaDigest != version.Manifest.SchemaDigest || pin.Declaration.Validate() != nil ||
			pin.SchemaDigest.Validate() != nil || pin.VersionID.Validate() != nil {
			return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s pin aggregate is contradictory", version.VersionID)
		}
		switch pin.Selection {
		case "explicit", "fused_import", "fork_inherited", "fork_override":
		default:
			return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "resource version %s pin selection is contradictory", version.VersionID)
		}
	}
	return nil
}

func (o *Owner) loadPruneReceipt(ctx context.Context, tx *sql.Tx, id, requestHash string) (runtimedata.PruneOperationResult, bool, error) {
	stored, found, err := o.readStoredPruneReceipt(ctx, tx, id)
	if err != nil || !found {
		if err != nil {
			return runtimedata.PruneOperationResult{}, false, err
		}
		return runtimedata.PruneOperationResult{}, false, nil
	}
	if stored.requestHash != requestHash {
		return runtimedata.PruneOperationResult{}, false, runtimedata.NewDomainError(runtimedata.CodeInvocationConflict, "prune_invocation_id %s was already used for a different request", id)
	}
	result, _, _, err := o.validateStoredPruneReceipt(ctx, tx, id, stored)
	if err != nil {
		return runtimedata.PruneOperationResult{}, false, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "prune operation %s evidence is contradictory: %v", id, err)
	}
	return result, true, nil
}

func (o *Owner) insertPruneReceipt(ctx context.Context, tx *sql.Tx, command runtimedata.PruneCommand, hash string, requestJSON []byte, result runtimedata.PruneOperationResult, pins []runtimedata.Pin) error {
	pins = append([]runtimedata.Pin(nil), pins...)
	runtimedata.SortPins(pins)
	if err := result.ValidateForCommand(command, pins); err != nil {
		return err
	}
	resultJSON, _ := json.Marshal(result)
	if _, err := tx.ExecContext(ctx, o.query(`
		INSERT INTO resource_prune_invocations
		(prune_invocation_id, request_hash, actor, package_key, event_name, version_id, request_json, result_json, completed_at)
		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
	`, 9), command.PruneInvocationID, hash, command.Actor, command.Declaration.PackageKey, command.Declaration.EventName,
		command.VersionID, requestJSON, resultJSON, result.CompletedAt); err != nil {
		return err
	}
	for index, pin := range pins {
		if _, err := tx.ExecContext(ctx, o.query(`
			INSERT INTO resource_prune_pin_evidence
			(prune_invocation_id, ordinal, run_id, run_state, package_key, event_name, schema_digest, version_id, selection)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
		`, 9), command.PruneInvocationID, index+1, pin.RunID, pin.RunState, pin.Declaration.PackageKey,
			pin.Declaration.EventName, pin.SchemaDigest, pin.VersionID, pin.Selection); err != nil {
			return fmt.Errorf("store prune pin evidence: %w", err)
		}
	}
	stored, found, err := o.readStoredPruneReceipt(ctx, tx, command.PruneInvocationID)
	if err != nil {
		return fmt.Errorf("read inserted prune receipt: %w", err)
	}
	if !found {
		return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "inserted prune operation %s is missing", command.PruneInvocationID)
	}
	if _, _, _, err := o.validateStoredPruneReceipt(ctx, tx, command.PruneInvocationID, stored); err != nil {
		return runtimedata.NewDomainError(runtimedata.CodeIntegrity, "inserted prune operation %s is contradictory: %v", command.PruneInvocationID, err)
	}
	return nil
}

func (o *Owner) loadPrunePinEvidence(ctx context.Context, tx *sql.Tx, id string) ([]runtimedata.Pin, error) {
	rows, err := tx.QueryContext(ctx, o.query(`
		SELECT ordinal, run_id, run_state, package_key, event_name, schema_digest, version_id, selection
		FROM resource_prune_pin_evidence WHERE prune_invocation_id = %s ORDER BY ordinal
	`, 1), id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pins []runtimedata.Pin
	for rows.Next() {
		var ordinal int
		var pin runtimedata.Pin
		if err := rows.Scan(&ordinal, &pin.RunID, &pin.RunState, &pin.Declaration.PackageKey, &pin.Declaration.EventName,
			&pin.SchemaDigest, &pin.VersionID, &pin.Selection); err != nil {
			return nil, err
		}
		if ordinal != len(pins)+1 {
			return nil, runtimedata.NewDomainError(runtimedata.CodeIntegrity, "prune operation %s pin evidence order is contradictory", id)
		}
		pins = append(pins, pin)
	}
	return pins, rows.Err()
}

func decodePruneReceipt(id, storedHash string, requestJSON, resultJSON []byte, pins []runtimedata.Pin) (runtimedata.PruneOperationResult, error) {
	var command runtimedata.PruneCommand
	var result runtimedata.PruneOperationResult
	if err := json.Unmarshal(requestJSON, &command); err != nil {
		return runtimedata.PruneOperationResult{}, fmt.Errorf("decode prune request: %w", err)
	}
	computedHash, canonicalRequest, err := command.RequestHash()
	if err != nil || computedHash != storedHash || !jsonEqual(canonicalRequest, requestJSON) || command.PruneInvocationID != id {
		return runtimedata.PruneOperationResult{}, fmt.Errorf("prune request identity or hash is contradictory")
	}
	if err := json.Unmarshal(resultJSON, &result); err != nil {
		return runtimedata.PruneOperationResult{}, fmt.Errorf("decode prune result: %w", err)
	}
	if err := result.ValidateForCommand(command, pins); err != nil {
		return runtimedata.PruneOperationResult{}, err
	}
	return result, nil
}

func (o *Owner) query(template string, count int) string {
	for index := 1; index <= count; index++ {
		placeholder := "?"
		if o.dialect == dialectPostgres {
			placeholder = fmt.Sprintf("$%d", index)
		}
		template = strings.Replace(template, "%s", placeholder, 1)
	}
	return template
}

func mustDecodeSchema(raw []byte) map[string]any {
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		return map[string]any{"type": "invalid"}
	}
	return schema
}

func jsonEqual(left, right []byte) bool {
	a, errA := canonicaljson.Decode(left)
	b, errB := canonicaljson.Decode(right)
	if errA != nil || errB != nil {
		return false
	}
	canonicalA, errA := canonicaljson.Encode(a)
	canonicalB, errB := canonicaljson.Encode(b)
	return errA == nil && errB == nil && bytes.Equal(canonicalA, canonicalB)
}

func persistedTime(raw any) (time.Time, bool, error) {
	if raw == nil {
		return time.Time{}, false, nil
	}
	if value, ok := raw.(time.Time); ok {
		return value.UTC(), true, nil
	}
	text := strings.TrimSpace(fmt.Sprint(raw))
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999"} {
		if value, err := time.Parse(layout, text); err == nil {
			return value.UTC(), true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("invalid persisted time %q", text)
}

func pagePins(pins []runtimedata.Pin) runtimedata.PageResult[runtimedata.Pin] {
	runtimedata.SortPins(pins)
	return runtimedata.FirstEvidencePage(pins)
}

func pagePruneDefects(defects []runtimedata.PruneDefect) runtimedata.PageResult[runtimedata.PruneDefect] {
	return runtimedata.FirstEvidencePage(defects)
}
