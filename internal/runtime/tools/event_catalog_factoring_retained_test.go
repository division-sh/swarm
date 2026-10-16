package tools_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

type eventAdmissionRetainedStore interface {
	EnsureSourceArtifact(context.Context, *sourceartifact.AdmittedSourceArtifact) (sourceartifact.EnsureResult, error)
	GetSourceArtifact(context.Context, string) (sourceartifact.Persisted, error)
	Close() error
}

func TestEventCatalogFactoringRetainedArtifactBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var selected eventAdmissionRetainedStore
			var location string
			if backend == "sqlite" {
				location = filepath.Join(t.TempDir(), "runtime.db")
				selected, _ = storetest.StartSQLiteRuntimeStoreWithReopen(t, ctx, location)
			} else {
				location = testutil.StartPostgresDSN(t)
				selected, _ = storetest.StartPostgresRuntimeStoreWithReopen(t, location)
			}
			root := t.TempDir()
			for name, body := range map[string]string{
				"schema.yaml": "name: event-retained\nstages: {done: {final: true}}\n",
				"events.yaml": "# exact authored bytes\nsignal.ping:  # bare\ntask.requested:\n  _key: id\n  id: uuid\n  note: {type: 'text?', length: {min: 0, max: 8}}\n",
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
			if err := os.Rename(root, root+"-relocated"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(root + "-relocated") })
			if _, err := os.Stat(root); !os.IsNotExist(err) {
				t.Fatalf("original source path remains available: %v", err)
			}
			if backend == "sqlite" {
				selected, _ = storetest.StartSQLiteRuntimeStoreWithReopen(t, ctx, location)
			} else {
				selected, _ = storetest.StartPostgresRuntimeStoreWithReopen(t, location)
			}
			persisted, err := selected.GetSourceArtifact(ctx, initial.SourceArtifact.BundleHash())
			if err != nil {
				t.Fatal(err)
			}
			artifact, err := persisted.Decode()
			if err != nil {
				t.Fatal(err)
			}
			rebuilt, err := contracts.LoadWorkflowContractBundleFromArtifact(repo, artifact, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(initial.SourceArtifact.LogicalBlob(), artifact.LogicalBlob()) || artifact.BundleHash() != initial.SourceArtifact.BundleHash() || !reflect.DeepEqual(initial.ScopedEventEntries(), rebuilt.ScopedEventEntries()) || !reflect.DeepEqual(initial.EffectiveProvenance().Entries(), rebuilt.EffectiveProvenance().Entries()) {
				t.Fatal("native retained artifact changed event values, provenance or exact bytes/hash")
			}
			for _, name := range []string{"signal.ping", "task.requested"} {
				before, ok, err := initial.ResolveCompiledFlowEventSchema(".", name)
				if err != nil || !ok {
					t.Fatalf("original compiled event %s: %t %v", name, ok, err)
				}
				after, ok, err := rebuilt.ResolveCompiledFlowEventSchema(".", name)
				if err != nil || !ok || !bytes.Equal(before.CanonicalAcceptanceSchema(), after.CanonicalAcceptanceSchema()) || before.AcceptanceSchemaDigest() != after.AcceptanceSchemaDigest() || !reflect.DeepEqual(before.RequiredFieldNames(), after.RequiredFieldNames()) {
					t.Fatalf("retained compiled event %s changed: %t %v", name, ok, err)
				}
			}
			t.Log("proof_surface=native persisted source reopen and artifact reconstruction without original path; not public serve restart")
		})
	}
}
