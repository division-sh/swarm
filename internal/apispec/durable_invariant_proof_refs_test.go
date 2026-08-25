package apispec

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/effects"
	"gopkg.in/yaml.v3"
)

const platformSpecRefPrefix = "platform-spec.yaml#"

var (
	invariantIDPattern = regexp.MustCompile(`^(?:[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+|PPO-[A-Z]{3}-[0-9]{3})$`)
	goTestNamePattern  = regexp.MustCompile(`^Test[A-Z0-9_][A-Za-z0-9_]*$`)
	goTestTokenPattern = regexp.MustCompile(`Test[A-Z0-9_][A-Za-z0-9_]*`)
)

type invariantProofReference struct {
	Path string
	Test string
}

type durableInvariantRecord struct {
	ID               string
	YAMLPath         string
	OwnerRef         string
	Rule             string
	ExecutableProofs []invariantProofReference
	entryNode        *yaml.Node
}

type managedEffectProofReference struct {
	Adapter    string
	LaunchSite string
	Proof      string
}

func TestDurableInvariantProofReferencesConform(t *testing.T) {
	rootDir := repoRoot(t)
	root := loadPlatformSpecYAMLNode(t)

	records, problems := validateDurableInvariantRecords(rootDir, root)
	problems = append(problems, findLegacyInvariantProofShapes(root)...)
	problems = append(problems, findUnclassifiedGoTestScalars(root)...)
	if len(records) != 42 {
		problems = append(problems, fmt.Sprintf("canonical invariant inventory has %d entries, want 42", len(records)))
	}

	registrations := effects.Registrations()
	if len(registrations) != 17 {
		problems = append(problems, fmt.Sprintf("managed-effect registration inventory has %d entries, want 17", len(registrations)))
	}
	seenAdapters := make(map[string]struct{}, len(registrations))
	for _, registration := range registrations {
		if _, exists := seenAdapters[registration.Adapter]; exists {
			problems = append(problems, fmt.Sprintf("managed-effect adapter %q is registered more than once", registration.Adapter))
		}
		seenAdapters[registration.Adapter] = struct{}{}
		problems = append(problems, validateManagedEffectProof(rootDir, managedEffectProofReference{
			Adapter:    registration.Adapter,
			LaunchSite: registration.LaunchSite,
			Proof:      registration.Proof,
		})...)
	}

	if len(problems) > 0 {
		sort.Strings(problems)
		t.Fatalf("durable invariant proof-reference conformance failed:\n%s", strings.Join(problems, "\n"))
	}

	ids := make([]string, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.ID)
	}
	sort.Strings(ids)
	t.Logf("validated %d domain-local invariant IDs and %d managed-effect proof registrations", len(ids), len(registrations))
}

func validateDurableInvariantRecords(rootDir string, root *yaml.Node) ([]durableInvariantRecord, []string) {
	var records []durableInvariantRecord
	var problems []string
	seenIDs := make(map[string]string)

	var walk func(*yaml.Node, []string)
	walk = func(node *yaml.Node, path []string) {
		if node == nil {
			return
		}
		switch node.Kind {
		case yaml.DocumentNode, yaml.SequenceNode:
			for i, child := range node.Content {
				walk(child, appendPath(path, strconv.Itoa(i)))
			}
		case yaml.MappingNode:
			problems = append(problems, duplicateMappingKeyProblems(node, path)...)
			for i := 0; i+1 < len(node.Content); i += 2 {
				key := node.Content[i].Value
				value := node.Content[i+1]
				keyPath := appendPath(path, key)
				if key != "invariant_ids" {
					walk(value, keyPath)
					continue
				}
				if value.Kind != yaml.MappingNode {
					problems = append(problems, fmt.Sprintf("%s must be a mapping", joinYAMLPath(keyPath)))
					continue
				}
				problems = append(problems, duplicateMappingKeyProblems(value, keyPath)...)
				for j := 0; j+1 < len(value.Content); j += 2 {
					id := value.Content[j].Value
					entry := value.Content[j+1]
					entryPath := joinYAMLPath(appendPath(keyPath, id))
					if previous, exists := seenIDs[id]; exists {
						problems = append(problems, fmt.Sprintf("duplicate invariant ID %q at %s and %s", id, previous, entryPath))
					} else {
						seenIDs[id] = entryPath
					}
					if !invariantIDPattern.MatchString(id) {
						problems = append(problems, fmt.Sprintf("%s has invalid invariant ID %q", entryPath, id))
					}
					record, entryProblems := parseDurableInvariantEntry(id, entryPath, entry)
					problems = append(problems, entryProblems...)
					if len(entryProblems) == 0 {
						records = append(records, record)
					}
				}
			}
		}
	}
	walk(root, nil)

	for _, record := range records {
		problems = append(problems, validateInvariantOwnerReference(root, record)...)
		for _, proof := range record.ExecutableProofs {
			proofPath, pathProblems := validateRepositoryPath(rootDir, proof.Path, true)
			for _, problem := range pathProblems {
				problems = append(problems, fmt.Sprintf("%s proof %s: %s", record.YAMLPath, proof.Test, problem))
			}
			if len(pathProblems) > 0 {
				continue
			}
			if problem := validateExactRunnableTest(proofPath, proof.Test); problem != "" {
				problems = append(problems, fmt.Sprintf("%s proof %s: %s", record.YAMLPath, proof.Test, problem))
			}
		}
	}
	return records, problems
}

func parseDurableInvariantEntry(id, yamlPath string, node *yaml.Node) (durableInvariantRecord, []string) {
	record := durableInvariantRecord{ID: id, YAMLPath: yamlPath, entryNode: node}
	if node.Kind != yaml.MappingNode {
		return record, []string{fmt.Sprintf("%s must be a mapping", yamlPath)}
	}
	problems := duplicateMappingKeyProblems(node, strings.Split(yamlPath, "."))
	allowed := map[string]bool{"owner_ref": true, "rule": true, "executable_proofs": true}
	values := make(map[string]*yaml.Node)
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := node.Content[i].Value
		if !allowed[key] {
			problems = append(problems, fmt.Sprintf("%s has unknown field %q", yamlPath, key))
		}
		values[key] = node.Content[i+1]
	}
	for _, key := range []string{"owner_ref", "rule", "executable_proofs"} {
		if values[key] == nil {
			problems = append(problems, fmt.Sprintf("%s is missing %s", yamlPath, key))
		}
	}
	if owner := values["owner_ref"]; owner != nil {
		if owner.Kind != yaml.ScalarNode || strings.TrimSpace(owner.Value) == "" {
			problems = append(problems, fmt.Sprintf("%s.owner_ref must be a non-empty scalar", yamlPath))
		} else {
			record.OwnerRef = owner.Value
		}
	}
	if rule := values["rule"]; rule != nil {
		if rule.Kind != yaml.ScalarNode || strings.TrimSpace(rule.Value) == "" {
			problems = append(problems, fmt.Sprintf("%s.rule must be a non-empty scalar", yamlPath))
		} else {
			record.Rule = rule.Value
		}
	}
	if proofs := values["executable_proofs"]; proofs != nil {
		if proofs.Kind != yaml.SequenceNode || len(proofs.Content) == 0 {
			problems = append(problems, fmt.Sprintf("%s.executable_proofs must be a non-empty sequence", yamlPath))
		} else {
			for i, proof := range proofs.Content {
				proofPath := fmt.Sprintf("%s.executable_proofs[%d]", yamlPath, i)
				if proof.Kind != yaml.MappingNode {
					problems = append(problems, fmt.Sprintf("%s must be a mapping", proofPath))
					continue
				}
				problems = append(problems, duplicateMappingKeyProblems(proof, []string{proofPath})...)
				if len(proof.Content) != 4 {
					problems = append(problems, fmt.Sprintf("%s must contain exactly path and test", proofPath))
					continue
				}
				fields := make(map[string]*yaml.Node)
				for j := 0; j+1 < len(proof.Content); j += 2 {
					fields[proof.Content[j].Value] = proof.Content[j+1]
				}
				pathNode, testNode := fields["path"], fields["test"]
				if pathNode == nil || testNode == nil || pathNode.Kind != yaml.ScalarNode || testNode.Kind != yaml.ScalarNode || strings.TrimSpace(pathNode.Value) == "" || strings.TrimSpace(testNode.Value) == "" {
					problems = append(problems, fmt.Sprintf("%s must contain non-empty scalar path and test", proofPath))
					continue
				}
				record.ExecutableProofs = append(record.ExecutableProofs, invariantProofReference{Path: pathNode.Value, Test: testNode.Value})
			}
		}
	}
	return record, problems
}

func validateInvariantOwnerReference(root *yaml.Node, record durableInvariantRecord) []string {
	if !strings.HasPrefix(record.OwnerRef, platformSpecRefPrefix) {
		return []string{fmt.Sprintf("%s.owner_ref %q must start with %s", record.YAMLPath, record.OwnerRef, platformSpecRefPrefix)}
	}
	path := strings.TrimPrefix(record.OwnerRef, platformSpecRefPrefix)
	segments, ok := parseYAMLPathSegments(path)
	if !ok {
		return []string{fmt.Sprintf("%s.owner_ref %q has an invalid YAML path", record.YAMLPath, record.OwnerRef)}
	}
	target := root
	for _, segment := range segments {
		target = mappingValue(target, segment)
		if target == nil {
			return []string{fmt.Sprintf("%s.owner_ref %q does not resolve", record.YAMLPath, record.OwnerRef)}
		}
	}
	if target.Kind != yaml.MappingNode {
		return []string{fmt.Sprintf("%s.owner_ref %q must resolve to a semantic-owner mapping", record.YAMLPath, record.OwnerRef)}
	}
	if yamlNodeContains(record.entryNode, target) {
		return []string{fmt.Sprintf("%s.owner_ref %q resolves inside its own invariant record", record.YAMLPath, record.OwnerRef)}
	}
	return nil
}

func validateRepositoryPath(rootDir, relative string, requireTestFile bool) (string, []string) {
	var problems []string
	if relative == "" || filepath.IsAbs(relative) || filepath.ToSlash(filepath.Clean(relative)) != relative || relative == "." || strings.HasPrefix(relative, "../") {
		problems = append(problems, fmt.Sprintf("path %q must be a normalized repository-relative path", relative))
		return "", problems
	}
	if requireTestFile && !strings.HasSuffix(relative, "_test.go") {
		problems = append(problems, fmt.Sprintf("path %q must name a _test.go file", relative))
	}
	if !requireTestFile && !strings.HasSuffix(relative, ".go") {
		problems = append(problems, fmt.Sprintf("path %q must name a Go source file", relative))
	}
	abs := filepath.Join(rootDir, filepath.FromSlash(relative))
	rel, err := filepath.Rel(rootDir, abs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		problems = append(problems, fmt.Sprintf("path %q escapes the repository", relative))
		return "", problems
	}
	info, err := os.Stat(abs)
	if err != nil || !info.Mode().IsRegular() {
		problems = append(problems, fmt.Sprintf("path %q does not name an existing regular file", relative))
	}
	return abs, problems
}

func validateExactRunnableTest(filename, testName string) string {
	if !goTestNamePattern.MatchString(testName) {
		return fmt.Sprintf("test %q is not one top-level Go test name", testName)
	}
	file, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
	if err != nil {
		return fmt.Sprintf("cannot parse %s: %v", filename, err)
	}
	var matches []*ast.FuncDecl
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == testName {
			matches = append(matches, function)
		}
	}
	if len(matches) == 0 {
		return fmt.Sprintf("exact file does not declare %s", testName)
	}
	if len(matches) != 1 {
		return fmt.Sprintf("exact file declares %s %d times", testName, len(matches))
	}
	if !isRunnableTestingFunction(file, matches[0]) {
		return fmt.Sprintf("%s is not receiver-free func %s(t *testing.T) with no type parameters or results and a standard-library testing import", testName, testName)
	}
	return ""
}

func isRunnableTestingFunction(file *ast.File, function *ast.FuncDecl) bool {
	if function.Recv != nil || function.Type.TypeParams != nil || function.Type.Results != nil || function.Type.Params == nil || len(function.Type.Params.List) != 1 {
		return false
	}
	parameter := function.Type.Params.List[0]
	if len(parameter.Names) > 1 {
		return false
	}
	pointer, ok := parameter.Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := pointer.X.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "T" {
		return false
	}
	alias, ok := selector.X.(*ast.Ident)
	if !ok {
		return false
	}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil || path != "testing" {
			continue
		}
		importName := "testing"
		if spec.Name != nil {
			importName = spec.Name.Name
		}
		return importName != "." && importName != "_" && importName == alias.Name
	}
	return false
}

func validateManagedEffectProof(rootDir string, reference managedEffectProofReference) []string {
	var problems []string
	if !goTestNamePattern.MatchString(reference.Proof) || strings.Contains(reference.Proof, "/") {
		return []string{fmt.Sprintf("managed-effect %q proof %q must be one top-level Go test name", reference.Adapter, reference.Proof)}
	}
	launchFile, pathProblems := validateRepositoryPath(rootDir, reference.LaunchSite, false)
	for _, problem := range pathProblems {
		problems = append(problems, fmt.Sprintf("managed-effect %q launch site: %s", reference.Adapter, problem))
	}
	if len(pathProblems) > 0 {
		return problems
	}
	launchAST, err := parser.ParseFile(token.NewFileSet(), launchFile, nil, parser.PackageClauseOnly)
	if err != nil {
		return append(problems, fmt.Sprintf("managed-effect %q launch site cannot be parsed: %v", reference.Adapter, err))
	}
	launchPackage := launchAST.Name.Name
	directory := filepath.Dir(launchFile)
	entries, err := os.ReadDir(directory)
	if err != nil {
		return append(problems, fmt.Sprintf("managed-effect %q proof package cannot be read: %v", reference.Adapter, err))
	}
	var validMatches []string
	var invalidMatches []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		filename := filepath.Join(directory, entry.Name())
		file, parseErr := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
		if parseErr != nil || (file.Name.Name != launchPackage && file.Name.Name != launchPackage+"_test") {
			continue
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Name.Name != reference.Proof {
				continue
			}
			if isRunnableTestingFunction(file, function) {
				validMatches = append(validMatches, filename)
			} else {
				invalidMatches = append(invalidMatches, filename)
			}
		}
	}
	if len(validMatches) != 1 || len(invalidMatches) > 0 {
		return append(problems, fmt.Sprintf("managed-effect %q proof %s must resolve to exactly one runnable package-local declaration (valid=%d invalid=%d)", reference.Adapter, reference.Proof, len(validMatches), len(invalidMatches)))
	}
	return problems
}

func findLegacyInvariantProofShapes(root *yaml.Node) []string {
	var problems []string
	var walk func(*yaml.Node, []string)
	walk = func(node *yaml.Node, path []string) {
		if node == nil {
			return
		}
		if node.Kind == yaml.MappingNode {
			for i := 0; i+1 < len(node.Content); i += 2 {
				key := node.Content[i].Value
				keyPath := appendPath(path, key)
				if key == "negative_proof" {
					problems = append(problems, fmt.Sprintf("legacy invariant proof field survives at %s", joinYAMLPath(keyPath)))
				}
				walk(node.Content[i+1], keyPath)
			}
		} else {
			for i, child := range node.Content {
				walk(child, appendPath(path, strconv.Itoa(i)))
			}
		}
	}
	walk(root, nil)
	ppo := mappingValue(root, "durable_pipeline_processing_obligation_authority")
	if ppo != nil && mappingValue(ppo, "invariants") != nil {
		problems = append(problems, "durable_pipeline_processing_obligation_authority.invariants uses the retired scalar invariant shape")
	}
	return problems
}

func findUnclassifiedGoTestScalars(root *yaml.Node) []string {
	var problems []string
	var walk func(*yaml.Node, []string)
	walk = func(node *yaml.Node, path []string) {
		if node == nil {
			return
		}
		if node.Kind != yaml.MappingNode {
			for i, child := range node.Content {
				walk(child, appendPath(path, strconv.Itoa(i)))
			}
			return
		}
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := node.Content[i].Value
			value := node.Content[i+1]
			keyPath := appendPath(path, key)
			if key == "invariant_ids" {
				continue
			}
			if value.Kind == yaml.ScalarNode && closedGoTestList(value.Value) && !isGuardedProviderGuaranteeProof(keyPath) {
				problems = append(problems, fmt.Sprintf("unclassified closed Go test reference at %s", joinYAMLPath(keyPath)))
			}
			walk(value, keyPath)
		}
	}
	walk(root, nil)
	return problems
}

func closedGoTestList(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || !strings.HasPrefix(value, "Test") || len(goTestTokenPattern.FindAllString(value, -1)) == 0 {
		return false
	}
	remainder := goTestTokenPattern.ReplaceAllString(value, "")
	remainder = regexp.MustCompile(`\band\b`).ReplaceAllString(remainder, "")
	remainder = strings.Trim(remainder, " \t\r\n,;")
	return remainder == ""
}

func isGuardedProviderGuaranteeProof(path []string) bool {
	return len(path) == 6 &&
		path[0] == "tool_model" &&
		path[1] == "provider_capability_surface" &&
		path[2] == "guarantee_enforcement_registry" &&
		path[3] == "claims" &&
		path[5] == "execution_proof"
}

func duplicateMappingKeyProblems(node *yaml.Node, path []string) []string {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	seen := make(map[string]struct{}, len(node.Content)/2)
	var problems []string
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := node.Content[i].Value
		if _, exists := seen[key]; exists {
			problems = append(problems, fmt.Sprintf("duplicate mapping key %q at %s", key, joinYAMLPath(path)))
		}
		seen[key] = struct{}{}
	}
	return problems
}

func yamlNodeContains(root, candidate *yaml.Node) bool {
	if root == candidate {
		return true
	}
	for _, child := range root.Content {
		if yamlNodeContains(child, candidate) {
			return true
		}
	}
	return false
}

func appendPath(path []string, segment string) []string {
	result := make([]string, len(path), len(path)+1)
	copy(result, path)
	return append(result, segment)
}

func joinYAMLPath(path []string) string {
	if len(path) == 0 {
		return "<root>"
	}
	return strings.Join(path, ".")
}

func TestDurableInvariantProofReferenceMutationsFailClosed(t *testing.T) {
	rootDir := t.TempDir()
	writeFixtureFile(t, rootDir, "pkg/live_test.go", "package pkg\nimport \"testing\"\nfunc TestLive(t *testing.T) {}\n")
	writeFixtureFile(t, rootDir, "pkg/other_test.go", "package pkg\nimport \"testing\"\nfunc TestOther(t *testing.T) {}\n")

	validEntry := `        owner_ref: platform-spec.yaml#owner
        rule: exact durable rule
        executable_proofs:
          - {path: pkg/live_test.go, test: TestLive}`
	validSource := "owner: {rule: authority}\ndomain:\n  nested:\n    invariant_ids:\n      runtime.test.fixture:\n" + validEntry + "\n"
	records, problems := validateDurableInvariantRecords(rootDir, parseYAMLFixture(t, validSource))
	if len(problems) != 0 || len(records) != 1 {
		t.Fatalf("valid recursive fixture = %d records, problems %v", len(records), problems)
	}

	mutations := []struct {
		name   string
		source string
		want   string
	}{
		{name: "duplicate nested ID", source: validSource + "other:\n  invariant_ids:\n    runtime.test.fixture:\n" + strings.ReplaceAll(validEntry, "        ", "      ") + "\n", want: "duplicate invariant ID"},
		{name: "empty ID", source: strings.Replace(validSource, "runtime.test.fixture", `""`, 1), want: "invalid invariant ID"},
		{name: "whitespace ID", source: strings.Replace(validSource, "runtime.test.fixture", `"   "`, 1), want: "invalid invariant ID"},
		{name: "slash ID", source: strings.Replace(validSource, "runtime.test.fixture", "runtime/test", 1), want: "invalid invariant ID"},
		{name: "punctuated ID", source: strings.Replace(validSource, "runtime.test.fixture", "runtime@test", 1), want: "invalid invariant ID"},
		{name: "unknown field", source: strings.Replace(validSource, "        rule:", "        legacy: rejected\n        rule:", 1), want: "unknown field"},
		{name: "missing field", source: strings.Replace(validSource, "        rule: exact durable rule\n", "", 1), want: "missing rule"},
		{name: "unresolved owner", source: strings.Replace(validSource, "platform-spec.yaml#owner", "platform-spec.yaml#missing", 1), want: "does not resolve"},
		{name: "scalar owner", source: strings.Replace(validSource, "owner: {rule: authority}", "owner: scalar", 1), want: "semantic-owner mapping"},
		{name: "self owner", source: strings.Replace(validSource, "platform-spec.yaml#owner", `platform-spec.yaml#domain.nested.invariant_ids["runtime.test.fixture"]`, 1), want: "inside its own invariant record"},
		{name: "missing file", source: strings.Replace(validSource, "pkg/live_test.go", "pkg/missing_test.go", 1), want: "existing regular file"},
		{name: "absolute file", source: strings.Replace(validSource, "pkg/live_test.go", "/tmp/live_test.go", 1), want: "normalized repository-relative"},
		{name: "traversal file", source: strings.Replace(validSource, "pkg/live_test.go", "../live_test.go", 1), want: "normalized repository-relative"},
		{name: "non-test file", source: strings.Replace(validSource, "pkg/live_test.go", "pkg/live.go", 1), want: "_test.go"},
		{name: "foreign exact file", source: strings.Replace(validSource, "pkg/live_test.go", "pkg/other_test.go", 1), want: "does not declare"},
		{name: "missing test", source: strings.Replace(validSource, "TestLive", "TestMissing", 1), want: "does not declare"},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			_, got := validateDurableInvariantRecords(rootDir, parseYAMLFixture(t, mutation.source))
			assertProblemsContain(t, got, mutation.want)
		})
	}
}

func TestDurableInvariantRunnableTestMutationsFailClosed(t *testing.T) {
	rootDir := t.TempDir()
	cases := []struct {
		name   string
		source string
		want   string
	}{
		{name: "helper", source: "package pkg\nvar TestLive = 1\n", want: "does not declare"},
		{name: "method", source: "package pkg\nimport \"testing\"\ntype S struct{}\nfunc (S) TestLive(t *testing.T) {}\n", want: "not receiver-free"},
		{name: "wrong parameter", source: "package pkg\nfunc TestLive(t int) {}\n", want: "not receiver-free"},
		{name: "result", source: "package pkg\nimport \"testing\"\nfunc TestLive(t *testing.T) error { return nil }\n", want: "not receiver-free"},
		{name: "type parameter", source: "package pkg\nimport \"testing\"\nfunc TestLive[T any](t *testing.T) {}\n", want: "not receiver-free"},
		{name: "fake testing", source: "package pkg\ntype testing struct{}\nfunc TestLive(t *testing.T) {}\n", want: "not receiver-free"},
		{name: "ambiguous", source: "package pkg\nimport \"testing\"\nfunc TestLive(t *testing.T) {}\nfunc TestLive(t *testing.T) {}\n", want: "2 times"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			filename := filepath.Join(rootDir, testCase.name+"_test.go")
			if err := os.WriteFile(filename, []byte(testCase.source), 0o600); err != nil {
				t.Fatal(err)
			}
			assertTextContains(t, validateExactRunnableTest(filename, "TestLive"), testCase.want)
		})
	}
}

func TestDurableInvariantScalarSentinel(t *testing.T) {
	unclassified := parseYAMLFixture(t, "owner:\n  proof: TestUnclassified and TestStillUnclassified\n")
	assertProblemsContain(t, findUnclassifiedGoTestScalars(unclassified), "owner.proof")

	guarded := parseYAMLFixture(t, "tool_model:\n  provider_capability_surface:\n    guarantee_enforcement_registry:\n      claims:\n        exact_claim:\n          execution_proof: TestGuarded\n")
	if problems := findUnclassifiedGoTestScalars(guarded); len(problems) != 0 {
		t.Fatalf("guarded provider guarantee was classified as unguarded: %v", problems)
	}

	prose := parseYAMLFixture(t, "api:\n  description: Test setup returns TestSetupEntityResult values.\n  ref: '#/components/schemas/RunTestQuiescence'\n")
	if problems := findUnclassifiedGoTestScalars(prose); len(problems) != 0 {
		t.Fatalf("API prose/schema names were classified as proof lists: %v", problems)
	}
}

func TestManagedEffectProofReferenceMutationsFailClosed(t *testing.T) {
	rootDir := t.TempDir()
	writeFixtureFile(t, rootDir, "pkg/launch.go", "package pkg\n")
	writeFixtureFile(t, rootDir, "pkg/proof_test.go", "package pkg\nimport \"testing\"\nfunc TestLive(t *testing.T) {}\n")
	writeFixtureFile(t, rootDir, "foreign/launch.go", "package foreign\n")
	writeFixtureFile(t, rootDir, "foreign/proof_test.go", "package foreign\nimport \"testing\"\nfunc TestForeign(t *testing.T) {}\n")

	valid := managedEffectProofReference{Adapter: "fixture", LaunchSite: "pkg/launch.go", Proof: "TestLive"}
	if problems := validateManagedEffectProof(rootDir, valid); len(problems) != 0 {
		t.Fatalf("valid managed-effect fixture failed: %v", problems)
	}

	mutations := []struct {
		name string
		ref  managedEffectProofReference
		want string
	}{
		{name: "slash", ref: managedEffectProofReference{Adapter: "fixture", LaunchSite: "pkg/launch.go", Proof: "TestLive/subtest"}, want: "top-level"},
		{name: "prefix only", ref: managedEffectProofReference{Adapter: "fixture", LaunchSite: "pkg/launch.go", Proof: "TestLiv"}, want: "valid=0"},
		{name: "global foreign name", ref: managedEffectProofReference{Adapter: "fixture", LaunchSite: "pkg/launch.go", Proof: "TestForeign"}, want: "valid=0"},
		{name: "moved launch", ref: managedEffectProofReference{Adapter: "fixture", LaunchSite: "foreign/launch.go", Proof: "TestLive"}, want: "valid=0"},
		{name: "missing launch", ref: managedEffectProofReference{Adapter: "fixture", LaunchSite: "missing/launch.go", Proof: "TestLive"}, want: "existing regular file"},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			assertProblemsContain(t, validateManagedEffectProof(rootDir, mutation.ref), mutation.want)
		})
	}

	writeFixtureFile(t, rootDir, "bad/launch.go", "package bad\n")
	writeFixtureFile(t, rootDir, "bad/proof_test.go", "package bad\nfunc TestBad(t int) {}\n")
	assertProblemsContain(t, validateManagedEffectProof(rootDir, managedEffectProofReference{Adapter: "bad", LaunchSite: "bad/launch.go", Proof: "TestBad"}), "invalid=1")

	writeFixtureFile(t, rootDir, "ambiguous/launch.go", "package ambiguous\n")
	writeFixtureFile(t, rootDir, "ambiguous/one_test.go", "package ambiguous\nimport \"testing\"\nfunc TestDuplicate(t *testing.T) {}\n")
	writeFixtureFile(t, rootDir, "ambiguous/two_test.go", "package ambiguous\nimport \"testing\"\nfunc TestDuplicate(t *testing.T) {}\n")
	assertProblemsContain(t, validateManagedEffectProof(rootDir, managedEffectProofReference{Adapter: "ambiguous", LaunchSite: "ambiguous/launch.go", Proof: "TestDuplicate"}), "valid=2")
}

func parseYAMLFixture(t *testing.T, source string) *yaml.Node {
	t.Helper()
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(source), &document); err != nil {
		t.Fatalf("parse YAML fixture: %v\n%s", err, source)
	}
	if len(document.Content) != 1 {
		t.Fatalf("fixture document has %d roots", len(document.Content))
	}
	return document.Content[0]
}

func writeFixtureFile(t *testing.T, rootDir, relative, source string) {
	t.Helper()
	filename := filepath.Join(rootDir, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(filename), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertProblemsContain(t *testing.T, problems []string, want string) {
	t.Helper()
	assertTextContains(t, strings.Join(problems, "\n"), want)
}

func assertTextContains(t *testing.T, got, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Fatalf("got %q, want substring %q", got, want)
	}
}
