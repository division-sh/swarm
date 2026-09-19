package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

// Authored items_from has no resource producer. Like the existing resource-pin
// owner fixture, install only the persisted source binding; shared serving must
// still load, evaluate, commit and publish every ordinal through real owners.
func (f *semanticProofFixture) installPinnedResourceSource(t *testing.T, trigger string, rows []map[string]any) fanoutobligation.SourceRef {
	t.Helper()
	owner, ok := f.selected.(interface {
		EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
		ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
	})
	if !ok {
		t.Fatal("selected fixture lacks its real durable-data owner")
	}
	bundle, ok := semanticview.Bundle(f.source)
	if !ok {
		t.Fatal("resource fixture requires the exact admitted bundle")
	}
	ref, err := durabledata.ParseDeclarationRef("portfolio", "proof.accounts")
	if err != nil {
		t.Fatal(err)
	}
	schema := map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"account_id", "eng_roles", "gem_score", "external_id"},
		"properties": map[string]any{
			"account_id":  map[string]any{"type": "string"},
			"eng_roles":   map[string]any{"type": "integer"},
			"gem_score":   map[string]any{"type": "number"},
			"external_id": map[string]any{"type": "string"},
		},
	}
	encode := func(values []map[string]any) []byte {
		t.Helper()
		var raw strings.Builder
		// Import order is deliberately the reverse of canonical business-key order.
		for i := len(values) - 1; i >= 0; i-- {
			value, err := json.Marshal(values[i])
			if err != nil {
				t.Fatal(err)
			}
			raw.Write(value)
			raw.WriteByte('\n')
		}
		return []byte(raw.String())
	}
	input := encode(rows)
	compiled, defects := durabledata.CompileJSONL(ref, schema, "account_id", input)
	if len(defects) != 0 {
		t.Fatalf("compile exact resource fixture: %+v", defects)
	}
	catalog := durabledata.Catalog{BundleHash: bundle.SourceArtifact.BundleHash(), Declarations: []durabledata.Declaration{{
		Name: "proof.accounts", Ref: ref, BusinessKey: "account_id", SchemaDigest: compiled.Manifest.SchemaDigest, CanonicalSchema: compiled.CanonicalSchema,
	}}}
	if _, err := owner.EnsureSourceArtifactWithData(f.ctx, bundle.SourceArtifact, catalog); err != nil {
		t.Fatal(err)
	}
	command := durabledata.SourceCommand{Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: catalog.BundleHash, Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: input}
	imported, err := owner.ExecuteDataSourceOperation(f.ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := f.db.ExecContext(f.ctx, `INSERT INTO resource_version_pins (run_id,flow_path,event_name,schema_digest,version_id,selection,pinned_at) VALUES ($1,$2,$3,$4,$5,'explicit',$6)`, f.runID, ref.FlowPath, ref.EventName, imported.SchemaDigest, imported.Candidate.VersionID, now); err != nil {
		t.Fatal(err)
	}
	result, err := f.db.ExecContext(f.ctx, `UPDATE fan_out_intents SET source_kind='resource_version',source_event_id=NULL,source_field=NULL,source_resource_flow_path=$3,source_resource_event_name=$4,source_resource_version_id=$5 WHERE run_id=$1 AND triggering_delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id=$1 AND event_id=$2) AND cursor=0 AND claim_owner IS NULL AND cardinality=$6`, f.runID, trigger, ref.FlowPath, ref.EventName, imported.Candidate.VersionID, len(rows))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		t.Fatalf("bind exactly one untouched resource fixture: rows=%d err=%v", changed, err)
	}
	// A new resource head must not replace the run's exact pinned version.
	next := semanticProofRows(len(rows))
	for i := range next {
		next[i]["account_id"] = fmt.Sprintf("new-head-%02d", i)
		next[i]["gem_score"] = float64(99)
	}
	command.SourceInvocationID = uuid.NewString()
	command.ExpectedHead = durabledata.VersionHead(imported.Candidate.VersionID)
	command.Input = encode(next)
	replaced, err := owner.ExecuteDataSourceOperation(f.ctx, command)
	if err != nil || replaced.Candidate.VersionID == imported.Candidate.VersionID {
		t.Fatalf("resource head did not advance independently of pin: %+v err=%v", replaced, err)
	}
	return fanoutobligation.SourceRef{Kind: fanoutobligation.SourceResourceVersion, Declaration: ref, VersionID: imported.Candidate.VersionID}
}
