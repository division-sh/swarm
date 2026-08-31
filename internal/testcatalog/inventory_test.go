package testcatalog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func TestCatalogRequiredInventory(t *testing.T) {
	inventory, err := Load(catalogRepoRoot(t))
	if err != nil {
		t.Fatalf("load catalog inventory: %v", err)
	}
	counts := map[Disposition]int{}
	verifyCounts := map[VerifyResult]int{}
	for _, fixture := range inventory.Fixtures {
		counts[fixture.Metadata.Disposition]++
		if fixture.Metadata.Disposition == DispositionVerifyOnly {
			verifyCounts[fixture.Metadata.Verify]++
		}
	}
	if got := len(inventory.Fixtures); got != 157 {
		t.Fatalf("fixture count = %d, want 157", got)
	}
	if counts[DispositionRuntime] != 99 || counts[DispositionVerifyOnly] != 36 || counts[DispositionRetired] != 22 {
		t.Fatalf("disposition counts = %#v, want runtime=99 verify-only=36 retired=22", counts)
	}
	if verifyCounts[VerifyPass] != 2 || verifyCounts[VerifyWarning] != 8 || verifyCounts[VerifyReject] != 26 {
		t.Fatalf("verify-only counts = %#v, want pass=2 warning=8 reject=26", verifyCounts)
	}
	if got := len(inventory.PublicCompanions()); got != 87 {
		t.Fatalf("public companion count = %d, want 87", got)
	}
	if got := len(inventory.Claims); got != 24 {
		t.Fatalf("canonical claim count = %d, want 24", got)
	}
	if got := len(inventory.ExternalProofs); got != 4 {
		t.Fatalf("external proof count = %d, want 4", got)
	}
	wantProofs := map[string]struct {
		executor string
		claims   []string
	}{
		"examples/integrations/telegram-agent": {executor: "github.com/division-sh/swarm/internal/serveapp", claims: telegramAgentClaims()},
		"internal/runtime/llm":                 {executor: "github.com/division-sh/swarm/internal/runtime/llm", claims: []string{"catalog.runtime.managed_hitl_api_transport", "catalog.runtime.managed_hitl_inprocess_transport"}},
		"internal/releasee2e":                  {executor: "github.com/division-sh/swarm/internal/releasee2e", claims: []string{"catalog.runtime.managed_hitl_cli_mcp_transport"}},
		"internal/releasee2e/testdata/golden_agent_workload": {
			executor: "github.com/division-sh/swarm/internal/releasee2e",
			claims: []string{
				"catalog.runtime.agent_instance_materialization",
				"catalog.runtime.agent_turn_completion",
				"catalog.runtime.agent_emission_delivery",
				"catalog.runtime.agent_terminal_teardown",
			},
		},
	}
	for _, proof := range inventory.ExternalProofs {
		want, ok := wantProofs[proof.Source]
		if !ok || proof.Executor != want.executor || !equalCatalogStrings(proof.Proves, want.claims) {
			t.Fatalf("external proof = %#v", proof)
		}
		delete(wantProofs, proof.Source)
	}
	if len(wantProofs) != 0 {
		t.Fatalf("missing external proofs = %#v", wantProofs)
	}
}

func TestCatalogInventoryFailsClosed(t *testing.T) {
	tests := []struct {
		name      string
		spec      string
		expected  string
		companion bool
		want      string
	}{
		{name: "missing metadata", expected: "expected: {}\n", want: "missing top-level conformance metadata"},
		{name: "unknown disposition", expected: fixtureMetadata("other", "pass", "claim.runtime"), want: "invalid disposition"},
		{name: "unknown field", expected: fixtureMetadata("runtime", "pass", "claim.runtime") + "  unknown: true\n", want: "field unknown"},
		{name: "unknown claim", expected: fixtureMetadata("runtime", "pass", "claim.unknown"), want: "references unknown claim"},
		{name: "duplicate claim", expected: "conformance:\n  disposition: runtime\n  verify: pass\n  proves: [claim.runtime, claim.runtime]\n", want: "repeats claim"},
		{name: "unproved active claim", spec: claimSpec("claim.runtime", "runtime") + claimSpec("claim.other", "runtime"), expected: fixtureMetadata("runtime", "pass", "claim.runtime"), want: "has no non-retired proof fixture"},
		{name: "companion without metadata", expected: fixtureMetadata("runtime", "pass", "claim.runtime"), companion: true, want: "public companion metadata disagrees"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := writeInventoryFixture(t, test.spec, test.expected, test.companion)
			_, err := Load(root)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCatalogExternalProofsFailClosed(t *testing.T) {
	valid := externalProofSpec("examples/external", "github.com/division-sh/swarm/internal/executor", []string{"claim.external"})
	tests := []struct {
		name     string
		proof    string
		expected string
		want     string
	}{
		{name: "missing record", want: "claim.external has no non-retired proof fixture"},
		{name: "duplicate record", proof: valid + valid, want: "duplicate top-level external_proofs"},
		{name: "empty source", proof: externalProofSpec("", "github.com/division-sh/swarm/internal/executor", []string{"claim.external"}), want: "invalid repository-relative source"},
		{name: "parent source", proof: externalProofSpec("..", "github.com/division-sh/swarm/internal/executor", []string{"claim.external"}), want: "path escapes repository"},
		{name: "missing source", proof: externalProofSpec("examples/missing", "github.com/division-sh/swarm/internal/executor", []string{"claim.external"}), want: "external proof source"},
		{name: "missing executor", proof: externalProofSpec("examples/external", "", []string{"claim.external"}), want: "invalid executor"},
		{name: "parent executor", proof: externalProofSpec("examples/external", "github.com/division-sh/swarm/../outside-executor", []string{"claim.external"}), want: "path escapes repository"},
		{name: "unknown claim", proof: externalProofSpec("examples/external", "github.com/division-sh/swarm/internal/executor", []string{"claim.unknown"}), want: "references unknown claim"},
		{name: "duplicate claim", proof: externalProofSpec("examples/external", "github.com/division-sh/swarm/internal/executor", []string{"claim.external", "claim.external"}), want: "multiple runtime-credit owners"},
		{name: "multiple credit", proof: valid, expected: fixtureMetadata("runtime", "pass", "claim.external"), want: "multiple runtime-credit owners"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := writeExternalProofInventory(t, test.proof, test.expected)
			_, err := Load(root)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load error = %v, want %q", err, test.want)
			}
		})
	}

	t.Run("source symlink escape", func(t *testing.T) {
		root := writeExternalProofInventory(t, valid, "")
		source := filepath.Join(root, "examples", "external")
		if err := os.RemoveAll(source); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(t.TempDir(), source); err != nil {
			t.Fatal(err)
		}
		_, err := Load(root)
		if err == nil || !strings.Contains(err.Error(), "resolved path escapes repository") {
			t.Fatalf("Load error = %v, want source symlink containment rejection", err)
		}
	})

	t.Run("executor symlink escape", func(t *testing.T) {
		root := writeExternalProofInventory(t, valid, "")
		executor := filepath.Join(root, "internal", "executor")
		if err := os.RemoveAll(executor); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(t.TempDir(), executor); err != nil {
			t.Fatal(err)
		}
		_, err := Load(root)
		if err == nil || !strings.Contains(err.Error(), "resolved path escapes repository") {
			t.Fatalf("Load error = %v, want executor symlink containment rejection", err)
		}
	})

	t.Run("contained symlinks", func(t *testing.T) {
		const source = "examples/external-link"
		const executor = "github.com/division-sh/swarm/internal/executor-link"
		root := writeExternalProofInventory(t, externalProofSpec(source, executor, []string{"claim.external"}), "")
		if err := os.Symlink("external", filepath.Join(root, "examples", "external-link")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("executor", filepath.Join(root, "internal", "executor-link")); err != nil {
			t.Fatal(err)
		}
		writeCatalogTestFile(t, filepath.Join(root, ".github", "test-proof-plan.yaml"), externalProofPolicy(executor))
		inventory, err := Load(root)
		if err != nil {
			t.Fatalf("load contained symlink proof: %v", err)
		}
		if len(inventory.ExternalProofs) != 1 || inventory.ExternalProofs[0].Source != source {
			t.Fatalf("contained symlink proof = %#v", inventory.ExternalProofs)
		}
	})

	t.Run("missing executor CI owner", func(t *testing.T) {
		root := writeExternalProofInventory(t, valid, "")
		writeCatalogTestFile(t, filepath.Join(root, ".github", "test-proof-plan.yaml"), externalProofPolicy("github.com/division-sh/swarm/internal/other"))
		_, err := Load(root)
		if err == nil || !strings.Contains(err.Error(), "is not a special CI package") {
			t.Fatalf("Load error = %v, want missing executor proof", err)
		}
	})

	t.Run("filtered executor CI owner", func(t *testing.T) {
		root := writeExternalProofInventory(t, valid, "")
		writeCatalogTestFile(t, filepath.Join(root, ".github", "test-proof-plan.yaml"), externalProofPolicyWithRun("github.com/division-sh/swarm/internal/executor", "^TestUnrelated$"))
		_, err := Load(root)
		if err == nil || !strings.Contains(err.Error(), "filtered CI owner") {
			t.Fatalf("Load error = %v, want filtered executor proof rejection", err)
		}
	})

	root := writeExternalProofInventory(t, valid, "")
	inventory, err := Load(root)
	if err != nil {
		t.Fatalf("load valid external proof: %v", err)
	}
	if len(inventory.ExternalProofs) != 1 || inventory.ExternalProofs[0].Source != "examples/external" {
		t.Fatalf("valid external proofs = %#v", inventory.ExternalProofs)
	}
}

func TestCatalogOwnershipHasNoLegacyClassifierOrSimulator(t *testing.T) {
	root := catalogRepoRoot(t)
	forbidden := []string{
		"executeCatalog" + "HandlerStep",
		"catalogCase" + "ExecutableNow",
		"catalogCollect" + "BootIssues",
		"loadCatalogExpectedField" + "GateManifestations",
		"TestSwarmTestTier1" + "PositiveEmission",
		"TestSwarmTestTier3" + "ListProcessing",
	}
	legacySelector := regexp.MustCompile(`(?m)\bvar\s+tier[0-9]+[A-Za-z0-9_]*(Fixtures|ExcludedFixtures|RetiredFixtures)\b`)
	for _, relativeRoot := range []string{"internal/cliapp", "internal/runtime/cataloge2e", "internal/runtime/swarmflowtest"} {
		scanRoot := filepath.Join(root, relativeRoot)
		if _, err := os.Stat(scanRoot); os.IsNotExist(err) {
			continue
		} else if err != nil {
			t.Fatalf("stat %s: %v", relativeRoot, err)
		}
		err := filepath.WalkDir(scanRoot, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			text := string(raw)
			for _, symbol := range forbidden {
				if strings.Contains(text, symbol) {
					t.Errorf("%s restores non-authoritative catalog owner %s", path, symbol)
				}
			}
			if legacySelector.MatchString(text) {
				t.Errorf("%s restores a per-tier fixture selector", path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", relativeRoot, err)
		}
	}
}

func TestCatalogRequiredCIProofSelection(t *testing.T) {
	policyFile, err := os.Open(filepath.Join(catalogRepoRoot(t), ".github", "test-proof-plan.yaml"))
	if err != nil {
		t.Fatalf("open proof policy: %v", err)
	}
	defer policyFile.Close()
	policy, err := testplanning.LoadPolicy(policyFile)
	if err != nil {
		t.Fatalf("load proof policy: %v", err)
	}
	const cliappPackage = "github.com/division-sh/swarm/internal/cliapp"
	const releasePackage = "github.com/division-sh/swarm/internal/releasee2e"
	cliappUnit, ok := policy.Units["catalog-required-verify"]
	if !ok {
		t.Fatal("proof policy omits catalog-required-verify")
	}
	if len(cliappUnit.Packages) != 1 || cliappUnit.Packages[0] != cliappPackage {
		t.Fatalf("catalog-required-verify packages = %v, want [%s]", cliappUnit.Packages, cliappPackage)
	}
	if strings.TrimSpace(cliappUnit.Run) != "" {
		t.Fatalf("catalog-required-verify run filter = %q, want unfiltered full package", cliappUnit.Run)
	}
	requiredUnits := []string{"catalog-required-inventory", "catalog-required-verify"}
	releaseUnit, ok := policy.Units["hitl-releasee2e-full"]
	if !ok || len(releaseUnit.Packages) != 1 || releaseUnit.Packages[0] != releasePackage || strings.TrimSpace(releaseUnit.Run) != "" || releaseUnit.BudgetClass != "full" {
		t.Fatalf("hitl-releasee2e-full = %#v, want one unfiltered releasee2e owner with the full timing budget", releaseUnit)
	}
	planPackages := append([]string{"github.com/division-sh/swarm/internal/events"}, policy.SpecialPackages...)
	model := testplanning.WeightModel{Version: 1, SourceRunID: "issue-2143-ci-owner-guard", Packages: map[string]float64{}}
	for _, profileName := range []string{
		testplanning.ProfilePRCommon,
		testplanning.ProfilePREscalated,
		testplanning.ProfileFull,
		testplanning.ProfileNightly,
	} {
		profile := policy.Profiles[profileName]
		for _, unit := range requiredUnits {
			if !containsString(profile.Units, unit) {
				t.Errorf("profile %s omits %s", profileName, unit)
			}
		}
		if !containsString(profile.Units, "hitl-releasee2e-full") {
			t.Errorf("profile %s omits hitl-releasee2e-full", profileName)
		}
		catalogUnit := "catalog-full"
		if profileName == testplanning.ProfilePRCommon {
			catalogUnit = "catalog-required-smoke"
		}
		if !containsString(profile.Units, catalogUnit) {
			t.Errorf("profile %s omits %s", profileName, catalogUnit)
		}
		plan, err := testplanning.BuildPlan(policy, model, planPackages, profileName, "catalog CI owner guard", "issue-2143")
		if err != nil {
			t.Fatalf("build %s plan: %v", profileName, err)
		}
		cliappOwners := 0
		releaseOwners := 0
		for _, unit := range plan.Units {
			for _, pkg := range unit.Packages {
				switch pkg {
				case cliappPackage:
					cliappOwners++
					if unit.ID != "catalog-required-verify" || strings.TrimSpace(unit.Run) != "" {
						t.Errorf("profile %s cliapp owner = %s run=%q, want catalog-required-verify unfiltered", profileName, unit.ID, unit.Run)
					}
				case releasePackage:
					releaseOwners++
					if unit.ID != "hitl-releasee2e-full" || strings.TrimSpace(unit.Run) != "" {
						t.Errorf("profile %s releasee2e owner = %s run=%q, want hitl-releasee2e-full unfiltered", profileName, unit.ID, unit.Run)
					}
				}
			}
		}
		if cliappOwners != 1 {
			t.Errorf("profile %s cliapp owner count = %d, want 1", profileName, cliappOwners)
		}
		if releaseOwners != 1 {
			t.Errorf("profile %s releasee2e owner count = %d, want 1", profileName, releaseOwners)
		}
	}
	for _, changedPath := range []string{
		"internal/runtime/engine.go",
		"internal/releasee2e/golden_agent_workload_test.go",
		"internal/releasee2e/testdata/golden_agent_workload/package.yaml",
		"internal/testcatalog/inventory.go",
		"tests/tier1-primitives/test-advances-to/expected.yaml",
		"platform-spec.yaml",
	} {
		profile, _, err := policy.ResolveProfile("pull_request", []string{changedPath}, "")
		if err != nil {
			t.Fatalf("resolve profile for %s: %v", changedPath, err)
		}
		if profile != testplanning.ProfilePREscalated {
			t.Errorf("changed path %s resolved profile %s, want %s", changedPath, profile, testplanning.ProfilePREscalated)
		}
	}
	workflow, err := os.ReadFile(filepath.Join(catalogRepoRoot(t), ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatalf("read CI workflow: %v", err)
	}
	if !strings.Contains(string(workflow), "SWARM_TEST_PROOF_PROFILE: ${{ needs.ci-plan.outputs.profile }}") {
		t.Fatal("CI proof units do not receive the canonical planned profile")
	}
}

func TestCatalogRequiredCensusArtifacts(t *testing.T) {
	type census struct {
		Schema             string `json:"schema"`
		Phase              string `json:"phase"`
		FixtureCount       int    `json:"fixture_count"`
		PassCount          int    `json:"pass_count"`
		RejectCount        int    `json:"reject_count"`
		RuntimeRejectCount int    `json:"runtime_reject_count"`
		Fixtures           []struct {
			Fixture string `json:"fixture"`
		} `json:"fixtures"`
	}
	for _, test := range []struct {
		phase              string
		passCount          int
		rejectCount        int
		runtimeRejectCount int
	}{
		{phase: "before", passCount: 67, rejectCount: 88, runtimeRejectCount: 34},
		{phase: "after", passCount: 101, rejectCount: 54, runtimeRejectCount: 0},
	} {
		t.Run(test.phase, func(t *testing.T) {
			path := filepath.Join(catalogRepoRoot(t), "internal", "testcatalog", "testdata", "issue-2143-"+test.phase+"-census.json")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read census artifact: %v", err)
			}
			var got census
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("decode census artifact: %v", err)
			}
			if got.Schema != "issue-2143-supported-verify-census/v1" || got.Phase != test.phase || got.FixtureCount != 155 || len(got.Fixtures) != 155 {
				t.Fatalf("census identity = %#v", got)
			}
			if got.PassCount != test.passCount || got.RejectCount != test.rejectCount || got.RuntimeRejectCount != test.runtimeRejectCount {
				t.Fatalf("census counts = pass:%d reject:%d runtime_reject:%d, want %d/%d/%d", got.PassCount, got.RejectCount, got.RuntimeRejectCount, test.passCount, test.rejectCount, test.runtimeRejectCount)
			}
			seen := map[string]bool{}
			for _, row := range got.Fixtures {
				if row.Fixture == "" || seen[row.Fixture] {
					t.Fatalf("census has empty or duplicate fixture %q", row.Fixture)
				}
				seen[row.Fixture] = true
			}
		})
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func equalCatalogStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

func telegramAgentClaims() []string {
	return []string{
		"catalog.authoring.webhook_responder_first_user_journey",
		"catalog.runtime.provider_normalized_template_selection",
		"catalog.runtime.standing_ingress_identity_recovery",
		"catalog.runtime.agent_memory_instance_continuity",
		"catalog.runtime.provider_connector_reply",
	}
}

func catalogRepoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve catalog inventory source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func writeInventoryFixture(t *testing.T, claims, expected string, companion bool) string {
	t.Helper()
	root := t.TempDir()
	if claims == "" {
		claims = claimSpec("claim.runtime", "runtime")
	}
	spec := "test_specification:\n  internal_catalog_conformance:\n    claims:\n" + claims
	writeCatalogTestFile(t, filepath.Join(root, "platform-spec.yaml"), spec)
	fixtureRoot := filepath.Join(root, "tests", "tier1", "test-case")
	writeCatalogTestFile(t, filepath.Join(fixtureRoot, "expected.yaml"), expected)
	if companion {
		writeCatalogTestFile(t, filepath.Join(fixtureRoot, "tests", "visible-smoke.yaml"), "name: smoke\n")
	}
	return root
}

func writeExternalProofInventory(t *testing.T, proof, expected string) string {
	t.Helper()
	root := t.TempDir()
	claims := claimSpec("claim.runtime", "runtime") + claimSpec("claim.external", "runtime")
	spec := "test_specification:\n  internal_catalog_conformance:\n    claims:\n" + claims + proof
	writeCatalogTestFile(t, filepath.Join(root, "platform-spec.yaml"), spec)
	if expected == "" {
		expected = fixtureMetadata("runtime", "pass", "claim.runtime")
	}
	writeCatalogTestFile(t, filepath.Join(root, "tests", "tier1", "test-case", "expected.yaml"), expected)
	if err := os.MkdirAll(filepath.Join(root, "examples", "external"), 0o755); err != nil {
		t.Fatalf("create external source: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "internal", "executor"), 0o755); err != nil {
		t.Fatalf("create executor package: %v", err)
	}
	writeCatalogTestFile(t, filepath.Join(root, ".github", "test-proof-plan.yaml"), externalProofPolicy("github.com/division-sh/swarm/internal/executor"))
	return root
}

func externalProofSpec(source, executor string, proves []string) string {
	var out strings.Builder
	out.WriteString("    external_proofs:\n")
	out.WriteString("    - source: " + source + "\n")
	out.WriteString("      executor: " + executor + "\n")
	out.WriteString("      proves:\n")
	for _, claimID := range proves {
		out.WriteString("      - " + claimID + "\n")
	}
	return out.String()
}

func externalProofPolicy(executor string) string {
	return externalProofPolicyWithRun(executor, "")
}

func externalProofPolicyWithRun(executor, run string) string {
	runLine := ""
	if run != "" {
		runLine = "    run: " + run + "\n"
	}
	return `version: 1
module: github.com/division-sh/swarm
planning:
  target_seconds: 1
  max_shards: 1
  unknown_package_seconds: 1
escalation_paths: []
special_packages: [` + executor + `]
profiles:
  pr-common: {count_mode: cache-default, environment_id: test, units: [external-proof]}
  pr-escalated: {count_mode: count-1, environment_id: test, units: [external-proof]}
  full: {count_mode: count-1, environment_id: test, units: [external-proof]}
  nightly: {count_mode: count-1, environment_id: test, units: [external-proof]}
units:
  external-proof:
    packages: [` + executor + `]
` + runLine + `    count_mode: count-1
    environment_id: test
    budget_class: broad
projections: {}
`
}

func claimSpec(id, disposition string) string {
	return "      " + id + ":\n        status: active\n        required_disposition: " + disposition + "\n        scope: test\n"
}

func fixtureMetadata(disposition, verify, claim string) string {
	return "conformance:\n  disposition: " + disposition + "\n  verify: " + verify + "\n  proves: [" + claim + "]\n"
}

func writeCatalogTestFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create test directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
}
