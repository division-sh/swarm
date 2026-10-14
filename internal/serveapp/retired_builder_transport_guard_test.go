package serveapp

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var retiredBuilderCandidatePattern = regexp.MustCompile(`(?i)builder`)

type retiredBuilderCandidate struct {
	line int
	text string
}

func TestRetiredBuilderSemanticReferencesStayExplicit(t *testing.T) {
	repoRoot := repoRootForTest()
	cmd := exec.Command("git", "-C", repoRoot, "ls-files", "-z")
	tracked, err := cmd.Output()
	if err != nil {
		t.Fatalf("list tracked files: %v", err)
	}

	expectedCounts := map[string]int{
		// Immutable historical test names; timing loader pins every artifact byte.
		".github/test-time-reference.json":                                             1,
		".github/audit-artifacts/issue-2550-preimplementation.md":                      5,
		".github/audit-artifacts/issue-2447-postimplementation.md":                     2,
		".github/audit-artifacts/issue-2007-failure-class.yaml":                        14,
		".github/audit-artifacts/issue-2378-failure-class.yaml":                        82,
		"internal/apiv1/handler_test.go":                                               2,
		"internal/apiv1/testdata/openrpc_compliance_matrix.yaml":                       1,
		"internal/apiv1/testdata/public_surface_backend_matrix.yaml":                   1,
		"internal/cliapp/api_consumption_boundary_test.go":                             3,
		"internal/cliapp/current_env_authority_test.go":                                1,
		"internal/cliapp/main_test.go":                                                 1,
		"internal/runtime/destructivereset/contracts.go":                               1,
		"internal/runtime/destructivereset/coordinator_test.go":                        2,
		"internal/runtime/manager/runtime_reset_test.go":                               3,
		"internal/runtime/runforkexecution/readiness_classifier.go":                    1,
		"internal/runtime/runforkexecution/runtime_container.go":                       1,
		"internal/store/internal/runtimepersistence/postgres_store_additional_test.go": 6,
		// One forbidden constructor-name literal in the debt guard's retired map.
		"internal/store/persistence_authority_debt_test.go": 1,
		// Exact retired-test selector/reference lines introduced by core promotion.
		".github/test-proof-plan.yaml":                          1,
		"internal/testplanning/static_authority_guards_test.go": 1,
		"internal/testcatalog/inventory_test.go":                1,
		"openrpc.json":                                          2,
		"platform-spec.yaml":                                    50,
	}
	expectedUnrelatedTextCounts := map[string]int{
		// Exact historical and current factory/producer-label snapshots, pinned by
		// the finite codemod guards; these do not restore the retired product.
		"tools/fixture-codemod/pipeline-observations/recipes.json":                                18,
		"tools/fixture-codemod/pipeline-observations/family144-transitions.json":                  4,
		"tools/fixture-codemod/pipeline-observations/family145-transitions.json":                  12,
		"tools/fixture-codemod/pipeline-observations/family145-channel-anchor-preimages.json":     2,
		"tools/fixture-codemod/pipeline-observations/family145-channel-settlement-preimages.json": 2,
		"internal/runtime/bootverify/workflow_transition_relation_test.go":                        1,
		"internal/runtime/semanticview/lifecycle_emitter_ownership_guard_test.go":                 1,
		"internal/store/internal/runtimepersistence/dynamic_flow_creation_revision_test.go":       1,
		"internal/events/construction_test.go":                                                    3,
		"internal/runtime/conformance/repo_validation_snapshot_test.go":                           1,
		"internal/runtime/contracts/mock_performance_ownership_guard_test.go":                     1,
		"internal/runtime/llm/runtime_resolver.go":                                                1,
		"internal/runtime/pythonmodule/artifact_manifest_generated.go":                            2,
		"internal/store/internal/runtimepersistence/run_debug_read_surface_test.go":               5,
		"internal/store/selected/boundary_test.go":                                                1,
		"internal/store/testdata/persistence_authority_findings.tsv":                              1,
		"internal/userfacing/human_code_projection_cli_test.go":                                   5,
		"scripts/rewrite-value-slots-2556/intent.json":                                            1,
	}
	classificationDocuments := map[string]bool{
		".github/audit-artifacts/issue-2550-preimplementation.md": true,
		".github/audit-artifacts/issue-2007-failure-class.yaml":   true,
		".github/audit-artifacts/issue-2378-failure-class.yaml":   true,
		"platform-spec.yaml": true,
	}
	rawRetirementFixtures := map[string]bool{
		"internal/apiv1/handler_test.go":                                               true,
		"internal/cliapp/api_consumption_boundary_test.go":                             true,
		"internal/cliapp/current_env_authority_test.go":                                true,
		"internal/runtime/destructivereset/coordinator_test.go":                        true,
		"internal/runtime/manager/runtime_reset_test.go":                               true,
		"internal/store/internal/runtimepersistence/postgres_store_additional_test.go": true,
		"internal/store/persistence_authority_debt_test.go":                            true,
	}
	classificationMarkers := []string{
		"retir", "legacy", "removed", "historical", "fail-closed", "must not",
		"no direct", "not accepted", "invalid", "rejected", "consumer only", "consumers only",
	}

	actualCounts := map[string]int{}
	actualUnrelatedTextCounts := map[string]int{}
	var violations []string
	for _, rawPath := range bytes.Split(tracked, []byte{0}) {
		if len(rawPath) == 0 {
			continue
		}
		relative := filepath.ToSlash(string(rawPath))
		if relative == "internal/serveapp/retired_builder_transport_guard_test.go" {
			continue
		}
		if strings.HasPrefix(strings.ToLower(relative), "internal/builder/") {
			violations = append(violations, fmt.Sprintf("%s: tracked path retains a Builder product candidate", relative))
		}
		body, readErr := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(relative)))
		if readErr != nil {
			t.Fatalf("read tracked file %s: %v", relative, readErr)
		}
		if bytes.IndexByte(body, 0) >= 0 {
			continue
		}
		candidates, candidateErr := retiredBuilderCandidates(relative, body)
		if candidateErr != nil {
			t.Fatalf("classify tracked file %s: %v", relative, candidateErr)
		}
		for _, candidate := range candidates {
			line := candidate.text
			if isExplicitUnrelatedBuilderText(relative, line) {
				actualUnrelatedTextCounts[relative]++
				continue
			}
			actualCounts[relative]++
			if classificationDocuments[relative] || rawRetirementFixtures[relative] {
				continue
			}
			lower := strings.ToLower(line)
			classified := false
			for _, marker := range classificationMarkers {
				if strings.Contains(lower, marker) {
					classified = true
					break
				}
			}
			if !classified {
				violations = append(violations, fmt.Sprintf("%s:%d: %s", relative, candidate.line, strings.TrimSpace(line)))
			}
		}
	}
	paths := make([]string, 0, len(actualCounts)+len(expectedCounts))
	seen := map[string]bool{}
	for path := range actualCounts {
		paths = append(paths, path)
		seen[path] = true
	}
	for path := range expectedCounts {
		if !seen[path] {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	for _, path := range paths {
		if actualCounts[path] != expectedCounts[path] {
			violations = append(violations, fmt.Sprintf("%s: semantic reference count=%d, want %d", path, actualCounts[path], expectedCounts[path]))
		}
	}
	for path, expected := range expectedUnrelatedTextCounts {
		if actualUnrelatedTextCounts[path] != expected {
			violations = append(violations, fmt.Sprintf("%s: unrelated Builder text count=%d, want %d", path, actualUnrelatedTextCounts[path], expected))
		}
	}
	for path, count := range actualUnrelatedTextCounts {
		if _, ok := expectedUnrelatedTextCounts[path]; !ok {
			violations = append(violations, fmt.Sprintf("%s: unexpected unrelated Builder text count=%d", path, count))
		}
	}
	if len(violations) > 0 {
		t.Fatalf("retired Builder semantic references escaped the exact tracked-file classification:\n%s", strings.Join(violations, "\n"))
	}
	requireChannelSettlementReferenceCorpora(t, repoRoot)
}

func retiredBuilderCandidates(relative string, body []byte) ([]retiredBuilderCandidate, error) {
	if filepath.Ext(relative) != ".go" {
		return retiredBuilderCandidateLines(body, nil), nil
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, relative, body, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	type byteRange struct{ start, end int }
	var semanticRanges []byteRange
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		semanticRanges = append(semanticRanges, byteRange{
			start: fset.Position(literal.Pos()).Offset,
			end:   fset.Position(literal.End()).Offset,
		})
		return true
	})
	for _, group := range file.Comments {
		for _, comment := range group.List {
			semanticRanges = append(semanticRanges, byteRange{
				start: fset.Position(comment.Pos()).Offset,
				end:   fset.Position(comment.End()).Offset,
			})
		}
	}
	return retiredBuilderCandidateLines(body, func(start, end int) bool {
		for _, semanticRange := range semanticRanges {
			if start < semanticRange.end && end > semanticRange.start {
				return true
			}
		}
		return false
	}), nil
}

func TestRetiredBuilderNegativeControlReferenceCountRemainsExact(t *testing.T) {
	const recipePath = "tools/fixture-codemod/pipeline-observations/recipes.json"
	recipes, recipeErr := os.ReadFile(filepath.Join(repoRootForTest(), recipePath))
	recipeCandidates, scanErr := retiredBuilderCandidates(recipePath, recipes)
	if recipeErr != nil || scanErr != nil || len(recipeCandidates) != 18 {
		t.Fatalf("exact runtime factory snapshot inventory changed: %+v, %v, %v", recipeCandidates, recipeErr, scanErr)
	}
	for _, candidate := range recipeCandidates {
		if !isExplicitUnrelatedBuilderText(recipePath, candidate.text) {
			t.Fatal("runtime factory snapshot lost its exact disposition")
		}
	}
	extraRecipe := append(append([]byte(nil), recipes...), []byte("\n\"After\": \"func foreignFactory() { builder() }\"\n")...)
	extraCandidates, extraErr := retiredBuilderCandidates(recipePath, extraRecipe)
	if extraErr != nil || len(extraCandidates) != 19 || isExplicitUnrelatedBuilderText(recipePath, extraCandidates[18].text) {
		t.Fatalf("foreign snapshot text escaped exact inventory: %+v, %v", extraCandidates, extraErr)
	}
	const transitionPath = "tools/fixture-codemod/pipeline-observations/family144-transitions.json"
	transitionBytes, err := os.ReadFile(filepath.Join(repoRootForTest(), transitionPath))
	transitionCandidates, scanErr := retiredBuilderCandidates(transitionPath, transitionBytes)
	if err != nil || scanErr != nil || len(transitionCandidates) != 4 {
		t.Fatalf("exact run-fixture transition inventory changed: %+v, %v, %v", transitionCandidates, err, scanErr)
	}
	for _, candidate := range transitionCandidates {
		if !isExplicitUnrelatedBuilderText(transitionPath, candidate.text) {
			t.Fatal("run-fixture transition lost its exact disposition")
		}
	}
	for _, hostile := range []string{
		`"After": "func foreignFactory() { builder() }"`,
		`"After": "func TestRunDebugReadSurface_ResolveLatestRunDebugRunID_UsesLatestPersistedRun(t *testing.T) { builder() }"`,
		`"Successor": "func TestRunDebugReadSurface_ResolveLatestRunDebugRunID_UsesLatestPersistedRun(t *testing.T) {\n builder() }"`,
	} {
		candidate := append(append([]byte(nil), transitionBytes...), []byte("\n"+hostile+"\n")...)
		got, err := retiredBuilderCandidates(transitionPath, candidate)
		if err != nil || len(got) != 5 {
			t.Fatalf("additional transition escaped exact four-reference inventory: %+v, %v", got, err)
		}
	}
	const family145Path = "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	family145Bytes, err := os.ReadFile(filepath.Join(repoRootForTest(), family145Path))
	if err != nil {
		t.Fatal(err)
	}
	family145Candidates, err := retiredBuilderCandidates(family145Path, family145Bytes)
	if err != nil || len(family145Candidates) != 12 {
		t.Fatalf("exact family145 text-reference inventory changed: count%d/%v", len(family145Candidates), err)
	}
	for _, candidate := range family145Candidates {
		if !isExplicitUnrelatedBuilderText(family145Path, candidate.text) {
			t.Fatal("family145 snapshot lost exact source/field disposition")
		}
	}
	for _, path := range []string{recipePath, family145Path} {
		for _, candidate := range family145Candidates {
			for index, mutate := range []func(string) string{
				func(line string) string { return strings.Replace(line, `"Before":`, `"Unreviewed":`, 1) },
				func(line string) string { return strings.Replace(line, "func Test", "func ForeignTest", 1) },
				func(line string) string { return strings.Replace(line, `\n`, `\n _ = \"builder\"\n`, 1) },
			} {
				changed := mutate(candidate.text)
				if changed != candidate.text && isExplicitUnrelatedBuilderText(path, changed) {
					t.Fatalf("changed snapshot field/function/source or extra reference inherited disposition: %s/mutant%d/prefix%.120s", path, index, changed)
				}
			}
		}
	}
	const path = "internal/store/persistence_authority_debt_test.go"
	body, err := os.ReadFile(filepath.Join(repoRootForTest(), path))
	if err != nil {
		t.Fatal(err)
	}
	got, err := retiredBuilderCandidates(path, body)
	if err != nil || len(got) != 1 || !strings.Contains(got[0].text, `"APIOptionalCapabilityBuilder": true`) {
		t.Fatalf("expected one forbidden constructor-name reference, got %+v: %v", got, err)
	}
	for _, extra := range []string{
		"\n// Builder compatibility routes are available.\n",
		"\nvar extraRetirementReference = \"APIOptionalCapabilityBuilder\"\n",
	} {
		candidate := append(append([]byte(nil), body...), extra...)
		got, err := retiredBuilderCandidates(path, candidate)
		if err != nil || len(got) != 2 {
			t.Fatalf("additional reference escaped the one-reference inventory: %+v: %v", got, err)
		}
	}
	for _, selectorPath := range []string{
		".github/test-proof-plan.yaml", "internal/testplanning/static_authority_guards_test.go", "internal/testcatalog/inventory_test.go",
	} {
		t.Run(selectorPath, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join(repoRootForTest(), selectorPath))
			if err != nil {
				t.Fatal(err)
			}
			got, err := retiredBuilderCandidates(selectorPath, body)
			if err != nil || len(got) != 1 {
				t.Fatalf("exact selector inventory changed: %+v, %v", got, err)
			}
			extra := "\nvar extraRetirementSelector = \"TestRetiredBuilderSemanticReferencesStayExplicit\"\n"
			if strings.HasSuffix(selectorPath, ".yaml") {
				extra = "\n# retired Builder additional selector reference\n"
			}
			candidate := append(append([]byte(nil), body...), extra...)
			got, err = retiredBuilderCandidates(selectorPath, candidate)
			if err != nil || len(got) != 2 {
				t.Fatalf("extra selector reference escaped the exact inventory: %+v, %v", got, err)
			}
		})
	}
}

func retiredBuilderCandidateLines(body []byte, include func(start, end int) bool) []retiredBuilderCandidate {
	var candidates []retiredBuilderCandidate
	offset := 0
	for index, rawLine := range bytes.SplitAfter(body, []byte("\n")) {
		line := string(bytes.TrimSuffix(rawLine, []byte("\n")))
		matched := false
		for _, match := range retiredBuilderCandidatePattern.FindAllStringIndex(line, -1) {
			if include == nil || include(offset+match[0], offset+match[1]) {
				matched = true
				break
			}
		}
		if matched {
			candidates = append(candidates, retiredBuilderCandidate{line: index + 1, text: line})
		}
		offset += len(rawLine)
	}
	return candidates
}

func isExplicitUnrelatedBuilderText(relative, line string) bool {
	lower := strings.ToLower(line)
	switch {
	case isExactFamily145ReferenceSnapshot(relative, line):
		return true
	case (relative == "tools/fixture-codemod/pipeline-observations/recipes.json" ||
		relative == "tools/fixture-codemod/pipeline-observations/family144-transitions.json") &&
		isExplicitRunFixtureSnapshot(line) &&
		(relative != "tools/fixture-codemod/pipeline-observations/recipes.json" || !strings.Contains(line, "func TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority(")):
		return true
	case relative == "tools/fixture-codemod/pipeline-observations/recipes.json" &&
		strings.HasPrefix(strings.TrimSpace(line), `"Successor": `) &&
		isExplicitRunFixtureSnapshot(strings.Replace(line, `"Successor":`, `"After":`, 1)) &&
		!strings.Contains(line, "func TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority("):
		return true
	case relative == "tools/fixture-codemod/pipeline-observations/recipes.json" &&
		(strings.HasPrefix(strings.TrimSpace(line), `"Before": "func TestSelectedContractAgentRuntimeBuildsCanonicalMockAdapter(`) ||
			strings.HasPrefix(strings.TrimSpace(line), `"After": "func TestSelectedContractAgentRuntimeBuildsCanonicalMockAdapter(`)):
		return true
	case relative == "internal/runtime/bootverify/workflow_transition_relation_test.go" && strings.Contains(lower, "graph builders"):
		return true
	case relative == "internal/runtime/semanticview/lifecycle_emitter_ownership_guard_test.go" && strings.Contains(lower, "(*endpointcensusbuilder).addlifecycleendpoints"):
		return true
	case relative == "internal/store/internal/runtimepersistence/dynamic_flow_creation_revision_test.go" && strings.Contains(lower, "declaration-to-plan builder"):
		return true
	case relative == "internal/events/construction_test.go" && strings.Contains(lower, "fixture builder"):
		return true
	case relative == "internal/runtime/conformance/repo_validation_snapshot_test.go" && strings.Contains(lower, "snapshot builder returned"):
		return true
	case relative == "internal/runtime/contracts/mock_performance_ownership_guard_test.go" && strings.Contains(lower, "bundlehashentrybuilder"):
		return true
	case relative == "internal/runtime/llm/runtime_resolver.go" && strings.Contains(lower, "runtime builder is not configured"):
		return true
	case relative == "internal/runtime/pythonmodule/artifact_manifest_generated.go" &&
		(strings.Contains(lower, "expatbuilder.py") || strings.Contains(lower, "xmlbuilder.py")):
		return true
	case relative == "internal/store/internal/runtimepersistence/run_debug_read_surface_test.go" && strings.Contains(lower, `"builder"`):
		return true
	case relative == "internal/store/selected/boundary_test.go" && strings.Contains(lower, "apioptionalcapabilitybuilder"):
		return true
	case relative == "internal/store/testdata/persistence_authority_findings.tsv" && strings.Contains(lower, "runtimefactorybuilder"):
		return true
	case relative == "internal/userfacing/human_code_projection_cli_test.go" && strings.Contains(lower, "strings.builder"):
		return true
	case relative == "scripts/rewrite-value-slots-2556/intent.json" && strings.Contains(lower, "var pins, connects strings.builder"):
		return true
	default:
		return false
	}
}

func isExactFamily145ReferenceSnapshot(path, line string) bool {
	corpus := strings.TrimPrefix(path, "tools/fixture-codemod/pipeline-observations/")
	if corpus != "recipes.json" && corpus != "family145-transitions.json" && corpus != "family145-channel-anchor-preimages.json" && corpus != "family145-channel-settlement-preimages.json" {
		return false
	}
	var field map[string]string
	if err := json.Unmarshal([]byte("{"+strings.TrimSuffix(strings.TrimSpace(line), ",")+"}"), &field); err != nil || len(field) != 1 {
		return false
	}
	for name, source := range field {
		for _, row := range family145ReferenceSnapshots {
			if row.corpus == corpus && row.field == name && strings.HasPrefix(source, "func "+row.function+"(") && fmt.Sprintf("%x", sha256.Sum256([]byte(source))) == row.digest {
				return true
			}
		}
	}
	return false
}

func TestChannelAnchorReferenceSnapshotsCannotAdmitChangedSourceOrProductReferences(t *testing.T) {
	const path = "tools/fixture-codemod/pipeline-observations/family145-channel-anchor-preimages.json"
	body, err := os.ReadFile(filepath.Join(repoRootForTest(), path))
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := retiredBuilderCandidates(path, body)
	if err != nil || len(candidates) != 2 {
		t.Fatalf("native channel-anchor text inventory changed: %d/%v", len(candidates), err)
	}
	for _, candidate := range candidates {
		if !isExplicitUnrelatedBuilderText(path, candidate.text) {
			t.Fatal("exact ordinary Go string accumulator is not classified")
		}
		for _, mutant := range []string{
			strings.Replace(candidate.text, `"Before":`, `"Foreign":`, 1),
			strings.Replace(candidate.text, "runChannelDeliveryInboundDispositionE2E", "foreignFixture", 1),
			strings.Replace(candidate.text, "strings.Builder", "builder()", 1),
			strings.Replace(candidate.text, `\n`, `\n retiredBuilderEscape()\n`, 1),
		} {
			if mutant != candidate.text && isExplicitUnrelatedBuilderText(path, mutant) {
				t.Fatal("modified field/function/source or retired product gained text disposition")
			}
		}
	}
}

var channelSettlementReferenceCounts = map[string]int{
	"tools/fixture-codemod/pipeline-observations/recipes.json":                                18,
	"tools/fixture-codemod/pipeline-observations/family145-transitions.json":                  12,
	"tools/fixture-codemod/pipeline-observations/family145-channel-settlement-preimages.json": 2,
}

func requireChannelSettlementReferenceCorpora(t *testing.T, root string) {
	t.Helper()
	for corpus := range channelSettlementReferenceCounts {
		body, err := os.ReadFile(filepath.Join(root, corpus))
		if err != nil || !exactChannelSettlementReferenceMultiplicity(corpus, body) {
			t.Fatalf("ordinary accumulator snapshot multiplicity changed: %s: %v", corpus, err)
		}
	}
}

func exactChannelSettlementReferenceMultiplicity(path string, body []byte) bool {
	want, known := channelSettlementReferenceCounts[path]
	rows, err := retiredBuilderCandidates(path, body)
	if !known || err != nil || len(rows) != want {
		return false
	}
	for _, row := range rows {
		if !isExplicitUnrelatedBuilderText(path, row.text) {
			return false
		}
	}
	return true
}

func TestChannelSettlementReferenceSnapshotsRejectChangedEvidenceAndDuplicateRecords(t *testing.T) {
	for corpus, want := range channelSettlementReferenceCounts {
		body, err := os.ReadFile(filepath.Join(repoRootForTest(), corpus))
		if err != nil || !exactChannelSettlementReferenceMultiplicity(corpus, body) {
			t.Fatal("exact approved snapshot inventory refused:", corpus, err)
		}
		candidates, err := retiredBuilderCandidates(corpus, body)
		if err != nil || len(candidates) != want {
			t.Fatal("snapshot count changed:", corpus, err)
		}
		for _, row := range candidates {
			if !strings.Contains(row.text, "func proveObjectChannelPages(") {
				continue
			}
			requireChannelSettlementReferenceHostility(t, corpus, row.text)
			duplicate := append(append([]byte{}, body...), []byte("\n"+row.text+"\n")...)
			if exactChannelSettlementReferenceMultiplicity(corpus, duplicate) {
				t.Fatal("extra identical approved field paid for another occurrence")
			}
		}
		requireChannelSettlementDuplicateRecordRefusal(t, corpus, body)
	}
}

func requireChannelSettlementReferenceHostility(t *testing.T, path, line string) {
	t.Helper()
	for _, other := range []string{"foreign.json", "tools/fixture-codemod/pipeline-observations/family145-channel-anchor-preimages.json"} {
		if isExplicitUnrelatedBuilderText(other, line) {
			t.Fatal("wrong corpus inherited exact disposition")
		}
	}
	for _, mutant := range []string{
		strings.Replace(line, `"Before":`, `"Successor":`, 1),
		strings.Replace(line, `"After":`, `"Successor":`, 1),
		strings.Replace(line, "func proveObjectChannelPages(", "func foreignObjectChannelPages(", 1),
		strings.Replace(line, "strings.Builder", "retiredBuilderEscape()", 1),
		strings.Replace(line, `\n`, `\n retiredBuilderEscape()\n`, 1),
	} {
		if mutant != line && isExplicitUnrelatedBuilderText(path, mutant) {
			t.Fatal("changed field/function/body/product reference inherited exact disposition")
		}
	}
}

func requireChannelSettlementDuplicateRecordRefusal(t *testing.T, corpus string, body []byte) {
	t.Helper()
	var rows []map[string]any
	if err := json.Unmarshal(body, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row["Function"] != "proveObjectChannelPages" {
			continue
		}
		out, err := json.MarshalIndent(append(rows, row), "", "  ")
		if err != nil || exactChannelSettlementReferenceMultiplicity(corpus, out) {
			t.Fatal("duplicated approved complete record escaped corpus multiplicity:", err)
		}
		return
	}
	t.Fatal("approved complete record missing")
}

// Exact approved source/field evidence: buildErr and strings.Builder, not the
// retired product. Historical bodies are neither edited nor broadly exempted.
var family145ReferenceSnapshots = []struct{ corpus, function, field, digest string }{
	// Ruling6168648501: exact ordinary text-accumulator source, multiplicity1 each.
	{"recipes.json", "proveObjectChannelPages", "Before", "c52c553017283fd7937ce1395f80199241072bc9f97275773da7505603f5a6af"},
	{"recipes.json", "proveObjectChannelPages", "After", "10705b404bc6700ea0b4269c798f886ab317d71d6d3c5121ecb79819c866df35"},
	{"family145-transitions.json", "proveObjectChannelPages", "Before", "c52c553017283fd7937ce1395f80199241072bc9f97275773da7505603f5a6af"},
	{"family145-transitions.json", "proveObjectChannelPages", "After", "10705b404bc6700ea0b4269c798f886ab317d71d6d3c5121ecb79819c866df35"},
	{"family145-channel-settlement-preimages.json", "proveObjectChannelPages", "Before", "c52c553017283fd7937ce1395f80199241072bc9f97275773da7505603f5a6af"},
	{"family145-channel-settlement-preimages.json", "proveObjectChannelPages", "After", "10705b404bc6700ea0b4269c798f886ab317d71d6d3c5121ecb79819c866df35"},
	{"recipes.json", "runChannelDeliveryInboundDispositionE2E", "Before", "5be2795a666eeb21728756b05239ec4a4cdb28045b06528da78a26e2b34664ca"},
	{"recipes.json", "runChannelDeliveryInboundDispositionE2E", "After", "d88540e3887eb4d53eb7586716efe669a82442a9417021f259504262133094d5"},
	{"family145-transitions.json", "runChannelDeliveryInboundDispositionE2E", "Before", "5be2795a666eeb21728756b05239ec4a4cdb28045b06528da78a26e2b34664ca"},
	{"family145-transitions.json", "runChannelDeliveryInboundDispositionE2E", "After", "d88540e3887eb4d53eb7586716efe669a82442a9417021f259504262133094d5"},
	{"family145-channel-anchor-preimages.json", "runChannelDeliveryInboundDispositionE2E", "Before", "5e5ec62277208488ec537166449eb550422b26dd63b16a8bff66f3e01781f382"},
	{"family145-channel-anchor-preimages.json", "runChannelDeliveryInboundDispositionE2E", "After", "d88540e3887eb4d53eb7586716efe669a82442a9417021f259504262133094d5"},
	{"recipes.json", "runtimeTestEventDiagnostics", "Before", "222818632cfa7253d1988a057dd751dfb29245ac1ff53049a2473976bef16455"},
	{"recipes.json", "runtimeTestEventDiagnostics", "After", "ef3a68977a3aec247d33de356d4fed2cc02fc072ac6533cd7996bd163dae1814"},
	{"family145-transitions.json", "runtimeTestEventDiagnostics", "Before", "222818632cfa7253d1988a057dd751dfb29245ac1ff53049a2473976bef16455"},
	{"family145-transitions.json", "runtimeTestEventDiagnostics", "After", "ef3a68977a3aec247d33de356d4fed2cc02fc072ac6533cd7996bd163dae1814"},
	{"recipes.json", "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority", "Before", "0095c88157da39be35aba627ff3991757f724e15a0d0c95b3843670d92b05c1f"},
	{"recipes.json", "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority", "After", "8bd9808550c6259b25b91fa5ac5fa8f102ea2706ed1dc538a7c73d580258368f"},
	{"recipes.json", "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority", "Successor", "91095150b46600d260565161d127d1d47bf017e10379e79e960fc6428ab01662"},
	{"recipes.json", "TestDataShowCLIReadFamilyAcrossSelectedStores", "Before", "a26913c407b7fe2b278d6ee991f092d6881d947256eca14510f3ccf67e8ffaeb"},
	{"recipes.json", "TestDataShowCLIReadFamilyAcrossSelectedStores", "After", "a6af0a0d3cdceef57df96324ec9a187db0e963888f7172946806f3e34d7b64f5"},
	{"recipes.json", "TestRuntimeLogFilterSupportedConsumersBothStores", "Before", "e155647b462d71fda709645ec1b7de4af90e5700b9a18c24ccf20aeaa532b237"},
	{"recipes.json", "TestRuntimeLogFilterSupportedConsumersBothStores", "After", "c1120aaa65b9cf697306792d94af7194e7f61b7143be41d74dfe78f1a357fe2a"},
	{"family145-transitions.json", "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority", "Before", "247707110fb19b1ee87f200e2c16fa574479f71dbb46d0f82105cc71b4c731fb"},
	{"family145-transitions.json", "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority", "After", "91095150b46600d260565161d127d1d47bf017e10379e79e960fc6428ab01662"},
	{"family145-transitions.json", "TestDataShowCLIReadFamilyAcrossSelectedStores", "Before", "a26913c407b7fe2b278d6ee991f092d6881d947256eca14510f3ccf67e8ffaeb"},
	{"family145-transitions.json", "TestDataShowCLIReadFamilyAcrossSelectedStores", "After", "a6af0a0d3cdceef57df96324ec9a187db0e963888f7172946806f3e34d7b64f5"},
	{"family145-transitions.json", "TestRuntimeLogFilterSupportedConsumersBothStores", "Before", "e155647b462d71fda709645ec1b7de4af90e5700b9a18c24ccf20aeaa532b237"},
	{"family145-transitions.json", "TestRuntimeLogFilterSupportedConsumersBothStores", "After", "c1120aaa65b9cf697306792d94af7194e7f61b7143be41d74dfe78f1a357fe2a"},
}

func isExplicitRunFixtureSnapshot(line string) bool {
	line = strings.TrimSpace(line)
	for _, declaration := range []string{
		"TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority",
		"TestRunDebugReadSurface_ResolveLatestRunDebugRunID_UsesLatestPersistedRun",
	} {
		for _, field := range []string{"Before", "After"} {
			if strings.HasPrefix(line, `"`+field+`": "func `+declaration+`(t *testing.T) {\n`) {
				return true
			}
		}
	}
	return false
}
