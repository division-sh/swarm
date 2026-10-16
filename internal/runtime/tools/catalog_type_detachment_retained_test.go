package tools_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestCatalogTypeDetachmentRetainedArtifactBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var selected eventAdmissionRetainedStore
			var reopen func()
			if backend == "sqlite" {
				location := filepath.Join(t.TempDir(), "runtime.db")
				reopen = func() { selected, _ = storetest.StartSQLiteRuntimeStoreWithReopen(t, ctx, location) }
			} else {
				location := testutil.StartPostgresDSN(t)
				reopen = func() { selected, _ = storetest.StartPostgresRuntimeStoreWithReopen(t, location) }
			}
			reopen()
			root := t.TempDir()
			fields := "  label: {type: text, pattern: '^[a-z]+$', length: {min: 1, max: 10}, equal_to: peer}\n  peer: text\n  score: {type: numeric, range: {min: 1.5, max: 9.5}}\n  note: text?\n"
			for name, body := range map[string]string{
				"schema.yaml": "name: detached-retained\nstages: {done: {final: true}}\n",
				"events.yaml": "audit.record:\n" + fields,
				"state.yaml":  "person:\n" + fields,
			} {
				if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			repo := canonicalrouting.RepoRoot(t)
			initial, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := selected.EnsureSourceArtifact(ctx, initial.SourceArtifact); err != nil {
				t.Fatal(err)
			}
			if err := selected.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(root); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(root); !os.IsNotExist(err) {
				t.Fatalf("original source remains: %v", err)
			}
			reopen()
			persisted, err := selected.GetSourceArtifact(ctx, initial.SourceArtifact.BundleHash())
			if err != nil {
				t.Fatal(err)
			}
			artifact, err := persisted.Decode()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(initial.SourceArtifact.LogicalBlob(), artifact.LogicalBlob()) {
				t.Fatal("retained source bytes changed")
			}
			for _, bound := range []string{"length_min", "length_max", "range_min", "range_max"} {
				t.Run(bound, func(t *testing.T) {
					rebuilt, err := contracts.LoadWorkflowContractBundleFromArtifact(repo, artifact, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{})
					if err != nil {
						t.Fatal(err)
					}
					compiled, ok, err := rebuilt.ResolveCompiledFlowEventSchema(".", "audit.record")
					if err != nil || !ok {
						t.Fatalf("retained compiled event: %t %v", ok, err)
					}
					read := func() []byte {
						primary, err := rebuilt.ResolveRootPrimaryEntity()
						if err != nil {
							t.Fatal(err)
						}
						shape, err := primary.StructuralType()
						if err != nil {
							t.Fatal(err)
						}
						event, _ := compiled.StructuralType()
						data, err := json.Marshal([]any{shape, event, rebuilt.RootEntityContracts(), compiled.CanonicalAcceptanceSchema(), compiled.AcceptanceSchemaDigest(), compiled.Source(), rebuilt.EffectiveProvenance().Entries()})
						if err != nil {
							t.Fatal(err)
						}
						return data
					}
					before := read()
					name := "label"
					if bound == "range_min" || bound == "range_max" {
						name = "score"
					}
					field, _ := compiled.StructuralField(name)
					switch bound {
					case "length_min":
						*field.Refinements.Length.Min = 2
					case "length_max":
						*field.Refinements.Length.Max = 9
					case "range_min":
						*field.Refinements.Range.Min = 2.5
					case "range_max":
						*field.Refinements.Range.Max = 8.5
					}
					if after := read(); !bytes.Equal(before, after) {
						t.Fatalf("native retained %s changed: before=%s after=%s", bound, before, after)
					}
					if rebuilt.SourceArtifact.BundleHash() != artifact.BundleHash() {
						t.Fatal("retained bundle identity changed")
					}
				})
			}
			t.Log("proof_surface=selected-store reopen and source-absent artifact reconstruction; not public restart")
		})
	}
}
