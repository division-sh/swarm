package runforkexecution

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/google/uuid"
)

func configuredVerifyForkBase(t *testing.T) (*packartifact.PlatformPackInventory, []string) {
	t.Helper()
	embedded := packfixture.EmbeddedBase(t)
	entry, ok := embedded.Lookup("provider.telegram")
	if !ok {
		t.Fatal("missing Telegram pack")
	}
	body := []byte(strings.Replace(string(entry.ManifestBody()), "telegram update object is required", "configured fork telegram object is required", 1))
	if bytes.Equal(body, entry.ManifestBody()) {
		t.Fatal("development pack mutation missed its field")
	}
	return packfixture.DevelopmentBase(t, map[string][]byte{"provider.telegram": body})
}

func TestRetainedSelectedAndOriginalSourceUseConfiguredPackGeneration(t *testing.T) {
	bundle := loadRunForkExecutionFixtureBundle(t, filepath.Join("tests", "tier12-runtime-fork", "test-selected-contract-fork-execution"))
	record := persistedSourceArtifactForTest(t, bundle)
	base, _ := configuredVerifyForkBase(t)
	bases, err := packartifact.NewPlatformPackBaseGenerationOwner(base)
	if err != nil {
		t.Fatal(err)
	}
	sourceID := uuid.NewString()
	reader := &fakeSourceArtifactSelectedContractSourceStore{
		record:       record,
		availability: runbundle.Availability{RunID: sourceID, Status: "running", BundleHash: record.BundleHash, SourceArtifactPresent: true},
	}
	loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: runForkExecutionRepoRoot(t), Store: reader, PlatformPackBases: bases}
	for _, request := range []SelectedContractSourceLoadRequest{
		{SourceRunID: sourceID, BundleHash: record.BundleHash, Selection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: record.BundleHash}},
		{SourceRunID: sourceID, Selection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts}},
	} {
		loaded, err := loader.InspectRunForkSelectedContractSourceForRequest(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		actual, ok := semanticview.Bundle(loaded.Source)
		if !ok || actual.PackInventory.BaseDigest() != base.Digest() || loaded.RuntimeProjection != nil || loaded.Cleanup != nil {
			t.Fatalf("retained inspection changed pack generation or materialized a runtime: %+v", loaded)
		}
	}
}

func TestPublicVerifyRetainedForkUsesConfiguredPackGeneration(t *testing.T) {
	ctx := runForkTestContext(t)
	repo := runForkExecutionRepoRoot(t)
	project := t.TempDir()
	if err := os.CopyFS(project, os.DirFS(filepath.Join(repo, "tests/tier1-primitives/test-emits-multiple"))); err != nil {
		t.Fatal(err)
	}
	base, dirs := configuredVerifyForkBase(t)
	loader := admittedFixtureSelectedContractSourceLoader{
		RepoRoot: repo, SourceRoot: project, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo), PlatformPackBase: base,
	}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts})
	if err != nil {
		t.Fatal(err)
	}
	s := storetest.StartSQLiteRuntimeStore(t)
	db := storetest.Database(s)
	sourceID, eventID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	seedSelectedOperationSource(t, ctx, "sqlite", db, s, loaded, sourceID, eventID, entityID)
	capability := selectedContractTestProcessCapability(t, ctx, s)
	owner := selectedContractSQLiteExecutionOwnerForTest(t, s)
	fork, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID: sourceID, At: eventID, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
		AgentRuntime:      SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.RetireSelectedContexts(ctx); err != nil {
		t.Fatal(err)
	}
	if err := capability.Release(ctx); err != nil {
		t.Fatal(err)
	}
	var sequence int
	var name, storePath string
	if err := db.QueryRowContext(ctx, "PRAGMA database_list").Scan(&sequence, &name, &storePath); err != nil {
		t.Fatal(err)
	}
	directoryJSON, err := json.Marshal(dirs)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "runtime.yaml")
	config := fmt.Sprintf("platform: {packs: {platform_dirs: %s}}\nllm: {backend: anthropic}\nstore: {backend: sqlite, sqlite: {path: %q}}\nserve: {api_listen_addr: '127.0.0.1:0', mcp_listen_addr: '127.0.0.1:0'}\n", directoryJSON, storePath)
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, "go", "run", "./cmd/swarm", "verify", project, "--config", configPath, "--json")
	command.Dir = repo
	// The public binary is not a test harness; do not forward test-only inputs.
	for _, pair := range os.Environ() {
		if !strings.HasPrefix(pair, "SWARM_TEST_") {
			command.Env = append(command.Env, pair)
		}
	}
	var out, diagnostic bytes.Buffer
	command.Stdout, command.Stderr = &out, &diagnostic
	err = command.Run()
	var result struct {
		OK           bool `json:"ok"`
		Observations []struct {
			CheckID string `json:"check_id"`
			Status  string `json:"status"`
			Subject string `json:"subject"`
		} `json:"observations"`
		ExecutionObligations []struct{ ID, Subject string } `json:"execution_obligations"`
	}
	if decodeErr := json.Unmarshal(out.Bytes(), &result); decodeErr != nil {
		t.Fatalf("public verify: run=%v decode=%v stdout=%s stderr=%s", err, decodeErr, &out, &diagnostic)
	}
	passed := false
	for _, observation := range result.Observations {
		if observation.CheckID == "selected_fork_recovery_admission" {
			passed = observation.Status == "passed"
		}
	}
	settlement := false
	for _, obligation := range result.ExecutionObligations {
		settlement = settlement || obligation.ID == "selected_fork_recovery_settlement" && obligation.Subject == "fork:"+fork.Materialization.ForkRunID
	}
	if !passed || !settlement {
		t.Fatalf("public retained fork lost configured generation: run=%v result=%+v stderr=%s", err, result, &diagnostic)
	}
}
