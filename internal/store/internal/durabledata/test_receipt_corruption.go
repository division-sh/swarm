package durabledata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ReceiptFaultCase is a finite integrity-negative operation, not a column or
// patch selector. Only the literal cases below have write authority.
type ReceiptFaultCase string

type ReceiptCorruption struct {
	id, child string
	change    receiptCorruptionChange
	before    []byte
	after     []byte
	original  any
	corrupt   any
	columns   []string
	values    []any
}

type receiptCorruptionChange struct {
	owner  receiptCorruptionOwner
	column string
	json   func(map[string]any)
	scalar func() any
	remove bool
}

type receiptCorruptionOwner uint8

const (
	sourceReceiptCorruption receiptCorruptionOwner = iota + 1
	pruneReceiptCorruption
	runCreationReceiptCorruption
	childReceiptCorruption
	versionReceiptCorruption
	importProvenanceCorruption
	headHistoryReceiptCorruption
	headReceiptCorruption
	firstProvenanceCorruption
	prunePinEvidenceCorruption
)

func CaptureReceiptCorruptionTx(ctx context.Context, tx *sql.Tx, id, child string, kind ReceiptFaultCase) (*ReceiptCorruption, error) {
	change, ok := receiptCorruptionCases[kind]
	if !ok {
		return nil, fmt.Errorf("unrecognized resource receipt integrity case %q", kind)
	}
	if uuid.Validate(id) != nil || (change.owner == childReceiptCorruption && uuid.Validate(child) != nil) || (change.owner != childReceiptCorruption && child != "") {
		return nil, errors.New("resource receipt fault requires its exact captured identity")
	}
	return captureReceiptCorruptionTx(ctx, tx, id, child, change)
}

func captureReceiptCorruptionTx(ctx context.Context, tx *sql.Tx, id, child string, change receiptCorruptionChange) (*ReceiptCorruption, error) {
	f := &ReceiptCorruption{id: id, child: child, change: change}
	var err error
	f.before, f.original, err = f.readTx(ctx, tx, false)
	if err != nil {
		return nil, err
	}
	if change.remove {
		return f, nil
	}
	if change.scalar != nil {
		f.corrupt = change.scalar()
		return f, nil
	}
	var raw []byte
	switch value := f.original.(type) {
	case []byte:
		raw = value
	case string:
		raw = []byte(value)
	default:
		return nil, fmt.Errorf("resource receipt JSON has storage type %T", value)
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	f.corrupt, err = corruptReceiptJSON(change, value)
	return f, err
}

func corruptReceiptJSON(change receiptCorruptionChange, value map[string]any) (raw []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("resource integrity case requires its original native receipt shape: %v", recovered)
		}
	}()
	change.json(value)
	return json.Marshal(value)
}

func (f *ReceiptCorruption) relation() (string, string, []any) {
	switch f.change.owner {
	case sourceReceiptCorruption:
		return "resource_source_invocations", "source_invocation_id=$1", []any{f.id}
	case pruneReceiptCorruption:
		return "resource_prune_invocations", "prune_invocation_id=$1", []any{f.id}
	case runCreationReceiptCorruption:
		return "resource_run_creation_operations", "run_id=$1", []any{f.id}
	case childReceiptCorruption:
		return "resource_run_creation_child_evaluations", "parent_run_id=$1 AND source_invocation_id=$2", []any{f.id, f.child}
	case versionReceiptCorruption:
		return "resource_versions", "version_id=$1", []any{f.id}
	case importProvenanceCorruption:
		return "resource_version_provenance", "producer_kind='import' AND producer_id=$1", []any{f.id}
	case headHistoryReceiptCorruption:
		return "resource_head_history", "operation_id=$1", []any{f.id}
	case headReceiptCorruption:
		return "resource_heads", "flow_path=$1 AND event_name=$2", []any{f.id, f.child}
	case firstProvenanceCorruption:
		return "resource_version_provenance", "version_id=$1 AND provenance_sequence=1", []any{f.id}
	case prunePinEvidenceCorruption:
		return "resource_prune_pin_evidence", "prune_invocation_id=$1 AND ordinal=$2", []any{f.id, f.child}
	default:
		panic("invalid captured receipt owner")
	}
}

func (f *ReceiptCorruption) readTx(ctx context.Context, tx *sql.Tx, lock bool) ([]byte, any, error) {
	if f == nil || tx == nil {
		return nil, nil, errors.New("resource receipt fault requires its selected transaction")
	}
	table, where, args := f.relation()
	query := "SELECT * FROM " + table + " WHERE " + where
	if lock {
		query += " FOR UPDATE"
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, nil, err
		}
		return nil, nil, sql.ErrNoRows
	}
	values, pointers, types := make([]any, len(columns)), make([]any, len(columns)), make([]string, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}
	if err := rows.Scan(pointers...); err != nil {
		return nil, nil, err
	}
	if f.change.remove && len(f.before) == 0 {
		f.columns, f.values = columns, values
	}
	var field any
	for i, column := range columns {
		types[i] = fmt.Sprintf("%T", values[i])
		if column == f.change.column {
			field = values[i]
		}
	}
	if rows.Next() {
		return nil, nil, errors.New("resource receipt identity matched multiple rows")
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	image, err := json.Marshal(struct {
		Columns []string
		Types   []string
		Values  []any
	}{columns, types, values})
	return image, field, err
}

func (f *ReceiptCorruption) installTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	current, _, err := f.readTx(ctx, tx, postgres)
	if err != nil {
		return err
	}
	if !bytes.Equal(current, f.before) {
		return errors.New("resource receipt changed before exact fault installation")
	}
	if f.change.remove {
		table, where, args := f.relation()
		result, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE "+where, args...)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err == nil && n != 1 {
			err = fmt.Errorf("resource evidence removal affected %d rows, want exactly one", n)
		}
		return err
	}
	if err := f.replaceTx(ctx, tx, f.corrupt); err != nil {
		return err
	}
	f.after, _, err = f.readTx(ctx, tx, false)
	return err
}

func (f *ReceiptCorruption) restoreTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	current, _, err := f.readTx(ctx, tx, postgres)
	if f.change.remove && errors.Is(err, sql.ErrNoRows) {
		return f.restoreRemovedTx(ctx, tx)
	}
	if err != nil {
		return err
	}
	if bytes.Equal(current, f.before) {
		return nil
	}
	if len(f.after) == 0 || !bytes.Equal(current, f.after) {
		return errors.New("resource receipt replacement changed before exact cleanup")
	}
	if err := f.replaceTx(ctx, tx, f.original); err != nil {
		return err
	}
	current, _, err = f.readTx(ctx, tx, false)
	if err == nil && !bytes.Equal(current, f.before) {
		err = errors.New("resource receipt cleanup did not restore its complete original")
	}
	return err
}

func (f *ReceiptCorruption) restoreRemovedTx(ctx context.Context, tx *sql.Tx) error {
	table, _, _ := f.relation()
	var columns, parameters string
	for i, column := range f.columns {
		if i > 0 {
			columns += ","
			parameters += ","
		}
		columns += column
		parameters += fmt.Sprintf("$%d", i+1)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+table+" ("+columns+") VALUES ("+parameters+")", f.values...); err != nil {
		return err
	}
	current, _, err := f.readTx(ctx, tx, false)
	if err == nil && !bytes.Equal(current, f.before) {
		err = errors.New("resource evidence removal cleanup lost its complete original")
	}
	return err
}

func (f *ReceiptCorruption) replaceTx(ctx context.Context, tx *sql.Tx, value any) error {
	table, where, args := f.relation()
	args = append(args, value)
	result, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET %s=$%d WHERE %s", table, f.change.column, len(args), where), args...)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		err = fmt.Errorf("resource receipt fault affected %d rows, want exactly one", n)
	}
	return err
}

var receiptCorruptionCases = map[ReceiptFaultCase]receiptCorruptionChange{
	"source/candidate state": {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) {
		c := v["candidate"].(map[string]any)
		c["state"] = "candidate"
		delete(c, "alias")
	}},
	"source/empty defects page": {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) {
		v["defects"] = map[string]any{"items": nil, "item_count": 0, "encoded_items_bytes": 0, "continuation": map[string]any{"state": ""}}
	}},
	"source/evaluation business key": {owner: sourceReceiptCorruption, column: "evaluation_json", json: func(v map[string]any) { v["declaration"].(map[string]any)["business_key"] = "hostile" }},
	"child/context business key":     {owner: childReceiptCorruption, column: "context_json", json: func(v map[string]any) { v["declaration"].(map[string]any)["business_key"] = "hostile" }},
	"child/observed revision":        {owner: childReceiptCorruption, column: "observed_head_revision", scalar: func() any { return 1 }},
	"source/positional result":       {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) { v["delta"].(map[string]any)["row_identity"] = "position" }},
	"source/empty delta evidence":    {owner: sourceReceiptCorruption, column: "evidence_json", json: func(v map[string]any) { v["delta_added"] = nil; v["delta_removed"] = nil; v["delta_changed"] = nil }},
	"source/delta summary classes": {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) {
		v["delta"].(map[string]any)["summary"] = map[string]any{"added": 0, "removed": 1, "changed": 0}
	}},
	"source/delta evidence classes": {owner: sourceReceiptCorruption, column: "evidence_json", json: func(v map[string]any) {
		v["delta_added"] = nil
		v["delta_removed"] = []any{map[string]any{"key": "alpha"}}
		v["delta_changed"] = nil
	}},
	"source/false validation result": {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) {
		items := []any{receiptCorruptionFalseDefect()}
		raw, _ := json.Marshal(items)
		v["defects"] = map[string]any{"items": items, "item_count": 1, "encoded_items_bytes": len(raw), "continuation": map[string]any{"state": "end"}}
	}},
	"source/false validation evidence": {owner: sourceReceiptCorruption, column: "evidence_json", json: func(v map[string]any) { v["defects"] = []any{receiptCorruptionFalseDefect()} }},
	"source/result invocation":         {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) { v["source_invocation_id"] = uuid.NewString() }},
	"source/result operation":          {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) { v["operation"] = "import" }},
	"source/result outcome":            {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) { v["outcome"] = "validation_rejected" }},
	"source/result bundle": {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) {
		v["bundle_hash"] = "bundle-v2:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	}},
	"source/result schema": {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) {
		v["schema_digest"] = "resource-schema-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	}},
	"source/result declaration": {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) {
		v["declaration"] = map[string]any{"flow_path": ".", "event_name": "hostile.changed"}
	}},
	"source/result expected head": {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) {
		v["expected_head"] = map[string]any{"state": "version", "version_id": v["candidate"].(map[string]any)["version_id"]}
	}},
	"source/result observed head": {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) {
		v["observed_head"] = map[string]any{"state": "version", "version_id": v["candidate"].(map[string]any)["version_id"]}
	}},
	"source/result candidate union": {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) { v["candidate"].(map[string]any)["state"] = "none" }},
	"source/result head transition": {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) { v["head"].(map[string]any)["changed"] = true }},
	"source/result delta": {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) {
		v["delta"] = map[string]any{"state": "not_computed", "reason": "head_conflict"}
	}},
	"source/result delta row identity": {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) { delete(v["delta"].(map[string]any), "row_identity") }},
	"source/result defect page": {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) {
		v["defects"] = map[string]any{"items": []any{map[string]any{"code": "hostile", "message": "hostile"}}, "item_count": 1, "encoded_items_bytes": 42, "continuation": map[string]any{"state": "end"}}
	}},
	"source/result completion": {owner: sourceReceiptCorruption, column: "result_json", json: func(v map[string]any) { v["completed_at"] = "0001-01-01T00:00:00Z" }},
	"source/evidence defects":  {owner: sourceReceiptCorruption, column: "evidence_json", json: func(v map[string]any) { v["defects"] = []any{map[string]any{"code": "hostile", "message": "hostile"}} }},
	"source/keyed delta evidence deleted": {owner: sourceReceiptCorruption, column: "evidence_json", json: func(v map[string]any) {
		delete(v, "delta_added")
		delete(v, "delta_removed")
		delete(v, "delta_changed")
	}},
	"source/request hash agreement": {owner: sourceReceiptCorruption, column: "request_json", json: func(v map[string]any) { v["actor"] = "hostile" }},
	"prune/result invocation":       {owner: pruneReceiptCorruption, column: "result_json", json: func(v map[string]any) { v["prune_invocation_id"] = uuid.NewString() }},
	"prune/result outcome":          {owner: pruneReceiptCorruption, column: "result_json", json: func(v map[string]any) { v["outcome"] = "already_pruned" }},
	"prune/result declaration": {owner: pruneReceiptCorruption, column: "result_json", json: func(v map[string]any) {
		v["declaration"] = map[string]any{"flow_path": ".", "event_name": "hostile.changed"}
	}},
	"prune/result version": {owner: pruneReceiptCorruption, column: "result_json", json: func(v map[string]any) {
		v["version_id"] = "resource-version-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	}},
	"prune/result expected head": {owner: pruneReceiptCorruption, column: "result_json", json: func(v map[string]any) { v["expected_head"] = map[string]any{"state": "absent"} }},
	"prune/result observed head": {owner: pruneReceiptCorruption, column: "result_json", json: func(v map[string]any) {
		v["observed_head"] = map[string]any{"state": "version", "version_id": v["version_id"]}
	}},
	"prune/result current version": {owner: pruneReceiptCorruption, column: "result_json", json: func(v map[string]any) { v["current_version_id"] = v["version_id"] }},
	"prune/result payload before":  {owner: pruneReceiptCorruption, column: "result_json", json: func(v map[string]any) { v["payload_before"] = "pruned" }},
	"prune/result payload after":   {owner: pruneReceiptCorruption, column: "result_json", json: func(v map[string]any) { v["payload_after"] = "materialized" }},
	"prune/result pin count":       {owner: pruneReceiptCorruption, column: "result_json", json: func(v map[string]any) { v["pin_count"] = 1 }},
	"prune/result defects": {owner: pruneReceiptCorruption, column: "result_json", json: func(v map[string]any) {
		v["defects"] = map[string]any{"items": []any{map[string]any{"code": "version_not_found", "message": "hostile"}}, "item_count": 1, "encoded_items_bytes": 61, "continuation": map[string]any{"state": "end"}}
	}},
	"prune/result completion":      {owner: pruneReceiptCorruption, column: "result_json", json: func(v map[string]any) { v["completed_at"] = "0001-01-01T00:00:00Z" }},
	"prune/request hash agreement": {owner: pruneReceiptCorruption, column: "request_json", json: func(v map[string]any) { v["actor"] = "hostile" }},
	"run/summary kind":             {owner: runCreationReceiptCorruption, column: "summary_json", json: func(v map[string]any) { v["kind"] = "event_publish" }},
	"run/summary counts":           {owner: runCreationReceiptCorruption, column: "summary_json", json: func(v map[string]any) { v["pin_count"] = float64(0) }},
	"run/summary outcome union":    {owner: runCreationReceiptCorruption, column: "summary_json", json: func(v map[string]any) { v["outcome"] = "data_rejected" }},
	"run/summary event identity":   {owner: runCreationReceiptCorruption, column: "summary_json", json: func(v map[string]any) { v["event_id"] = uuid.NewString() }},
	"run/summary completion":       {owner: runCreationReceiptCorruption, column: "summary_json", json: func(v map[string]any) { v["completed_at"] = "0001-01-01T00:00:00Z" }},
	"run/binding tagged union":     {owner: runCreationReceiptCorruption, column: "binding_json", json: func(v map[string]any) { v["run_id"] = uuid.NewString() }},
	"run/binding evidence":         {owner: runCreationReceiptCorruption, column: "evidence_json", json: func(v map[string]any) { v["run_binding"] = []any{map[string]any{"kind": "pin"}} }},
	"run/request hash agreement":   {owner: runCreationReceiptCorruption, column: "request_json", json: func(v map[string]any) { v["event_id"] = uuid.NewString() }},
	"run/pinless status":           {owner: runCreationReceiptCorruption, column: "summary_json", json: func(v map[string]any) { v["status"] = "bogus" }},
	"run/missing rejection":        {owner: runCreationReceiptCorruption, column: "summary_json", json: func(v map[string]any) { delete(v, "rejection") }},
	"run/request hash conflict": {owner: runCreationReceiptCorruption, column: "request_hash", scalar: func() any {
		return "resource-run-creation-request-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	}},
	"source/typed operation":              {owner: sourceReceiptCorruption, column: "operation", scalar: func() any { return "check" }},
	"source/typed parent run":             {owner: sourceReceiptCorruption, column: "parent_run_id", scalar: func() any { return uuid.NewString() }},
	"source/typed actor":                  {owner: sourceReceiptCorruption, column: "actor", scalar: func() any { return "hostile-actor" }},
	"source/typed bundle":                 {owner: sourceReceiptCorruption, column: "bundle_hash", scalar: receiptCorruptionForeignBundle},
	"source/typed package":                {owner: sourceReceiptCorruption, column: "flow_path", scalar: func() any { return "hostile" }},
	"source/typed event":                  {owner: sourceReceiptCorruption, column: "event_name", scalar: func() any { return "hostile.event" }},
	"source/typed observed head revision": {owner: sourceReceiptCorruption, column: "observed_head_revision", scalar: func() any { return 999 }},
	"source/typed completed at":           {owner: sourceReceiptCorruption, column: "completed_at", scalar: receiptCorruptionForeignTime},
	"prune/typed actor":                   {owner: pruneReceiptCorruption, column: "actor", scalar: func() any { return "hostile-actor" }},
	"prune/typed package":                 {owner: pruneReceiptCorruption, column: "flow_path", scalar: func() any { return "hostile" }},
	"prune/typed event":                   {owner: pruneReceiptCorruption, column: "event_name", scalar: func() any { return "hostile.event" }},
	"prune/typed version": {owner: pruneReceiptCorruption, column: "version_id", scalar: func() any {
		return "resource-version-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	}},
	"prune/typed completed at": {owner: pruneReceiptCorruption, column: "completed_at", scalar: receiptCorruptionForeignTime},
	"run/typed actor":          {owner: runCreationReceiptCorruption, column: "actor", scalar: func() any { return "hostile-actor" }},
	"run/typed bundle":         {owner: runCreationReceiptCorruption, column: "bundle_hash", scalar: receiptCorruptionForeignBundle},
	"run/typed completed at":   {owner: runCreationReceiptCorruption, column: "completed_at", scalar: receiptCorruptionForeignTime},
}

func receiptCorruptionFalseDefect() map[string]any {
	return map[string]any{"row": 1, "path": "$.slug", "code": "schema_rejected", "message": "coordinated false defect"}
}

func receiptCorruptionForeignBundle() any {
	return "bundle-v2:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
}

func receiptCorruptionForeignTime() any {
	return time.Date(2035, 1, 2, 3, 4, 5, 0, time.UTC)
}
