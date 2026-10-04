package cliapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/google/uuid"
)

type verifyPackGenerationArtifactReader struct {
	availability runbundle.Availability
	record       sourceartifact.Persisted
}

func (r verifyPackGenerationArtifactReader) LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error) {
	return r.availability, nil
}

func (r verifyPackGenerationArtifactReader) GetSourceArtifact(context.Context, string) (sourceartifact.Persisted, error) {
	return r.record, nil
}

func TestVerifyRetainedSourceConsumerSharesFrozenConfiguredPackGeneration(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	embedded := packfixture.EmbeddedBase(t)
	entry, ok := embedded.Lookup("provider.telegram")
	if !ok {
		t.Fatal("missing Telegram pack")
	}
	configured, dirs := packfixture.DevelopmentBase(t, map[string][]byte{
		"provider.telegram": []byte(strings.Replace(string(entry.ManifestBody()), "telegram update object is required", "frozen configured telegram object is required", 1)),
	})
	root := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
	configPath := filepath.Join(t.TempDir(), "runtime.yaml")
	writeRuntimeConfigText(t, configPath, "platform:\n  packs:\n    platform_dirs: ["+strings.Join(dirs, ", ")+"]\n")
	cfg, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: configPath})
	if err != nil {
		t.Fatal(err)
	}
	base, err := LoadConfiguredPlatformPackBase(root, cfg)
	if err != nil || base.Digest() != configured.Digest() || base.Digest() == embedded.Digest() {
		t.Fatalf("configured generation was not selected: %v", err)
	}
	bases, err := packartifact.NewPlatformPackBaseGenerationOwner(base)
	if err != nil {
		t.Fatal(err)
	}
	_, bundle, err := NewSwarmWorkflowModuleWithPackBase(root, root, filepath.Join(RepoRoot(), "platform-spec.yaml"), base)
	if err != nil {
		t.Fatal(err)
	}
	record, err := sourceartifact.PersistedFromArtifact(bundle.SourceArtifact, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(configPath); err != nil {
		t.Fatal(err)
	}
	paths := CLISourcePlatformSpecPaths{PlatformSpecPath: filepath.Join(RepoRoot(), "platform-spec.yaml")}
	verify, err := verifyRetainedSourceLoader(root, paths, bases)
	if err != nil {
		t.Fatal(err)
	}
	sourceID := uuid.NewString()
	reader := verifyPackGenerationArtifactReader{
		record: record, availability: runbundle.Availability{RunID: sourceID, Status: "running", BundleHash: record.BundleHash, SourceArtifactPresent: true},
	}
	verify.Store = reader
	boot := runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: root, PlatformSpecPath: paths.PlatformSpecPath, PlatformPackBases: bases, Store: reader}
	for _, request := range []runforkexecution.SelectedContractSourceLoadRequest{
		{SourceRunID: sourceID, BundleHash: record.BundleHash, Selection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: record.BundleHash}},
		{SourceRunID: sourceID, Selection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts}},
	} {
		for _, loader := range []runforkexecution.SourceArtifactSelectedContractSourceLoader{verify, boot} {
			loaded, err := loader.InspectRunForkSelectedContractSourceForRequest(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			actual, ok := semanticview.Bundle(loaded.Source)
			if !ok || actual.PackInventory.BaseDigest() != configured.Digest() || loaded.RuntimeProjection != nil || loaded.Cleanup != nil {
				t.Fatalf("selected/original retained consumer lost its frozen generation or prepared a runtime: %+v", loaded)
			}
		}
	}
	if _, err := verifyRetainedSourceLoader(root, paths, nil); err == nil {
		t.Fatal("missing generation fell back to embedded inventory")
	}
}
