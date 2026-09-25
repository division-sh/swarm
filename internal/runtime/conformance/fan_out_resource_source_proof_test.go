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
	"github.com/google/uuid"
)

func (f *semanticProofFixture) installPinnedResourceSource(t *testing.T, rows []map[string]any) fanoutobligation.SourceRef {
	t.Helper()
	owner, ok := f.selected.(interface {
		ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
	})
	if !ok {
		t.Fatal("selected fixture lacks its real durable-data owner")
	}
	bundle, ok := semanticview.Bundle(f.source)
	if !ok {
		t.Fatal("resource fixture requires the exact admitted bundle")
	}
	ref, err := durabledata.ParseDeclarationRef("portfolio", "portfolio/account.registered")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := bundle.DurableDataDeclarationByRef(ref); !ok {
		t.Fatal("compiled source event declaration is missing")
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
	command := durabledata.SourceCommand{Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: bundle.SourceArtifact.BundleHash(), Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: input}
	imported, err := owner.ExecuteDataSourceOperation(f.ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := f.db.ExecContext(f.ctx, `INSERT INTO resource_version_pins (run_id,flow_path,event_name,schema_digest,version_id,selection,pinned_at) VALUES ($1,$2,$3,$4,$5,'explicit',$6)`, f.runID, ref.FlowPath, ref.EventName, imported.SchemaDigest, imported.Candidate.VersionID, now); err != nil {
		t.Fatal(err)
	}
	reader, ok := f.selected.(interface {
		LoadPinnedSource(context.Context, string, string, durabledata.DeclarationRef) (durabledata.PinnedSource, error)
	})
	if !ok {
		t.Fatal("selected store lacks pinned source admission")
	}
	if pinned, err := reader.LoadPinnedSource(f.ctx, f.runID, command.BundleHash, ref); err != nil || pinned.VersionID != imported.Candidate.VersionID || pinned.RowCount != len(rows) {
		t.Fatalf("admit exact imported source: %+v err=%v", pinned, err)
	}
	// A new resource head must not replace the run's exact pinned version.
	next := make([]map[string]any, len(rows))
	for i := range rows {
		next[i] = make(map[string]any, len(rows[i]))
		for key, value := range rows[i] {
			next[i][key] = value
		}
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
