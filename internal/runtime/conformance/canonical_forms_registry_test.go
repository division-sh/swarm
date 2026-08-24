package conformance

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"gopkg.in/yaml.v3"
)

const canonicalFormsRegistryPath = "internal/runtime/conformance/testdata/canonical_forms_registry.yaml"

var canonicalGoConnectClassifications = map[string]struct{}{
	"mutation_base":      {},
	"non_authoring_text": {},
	"positive_producer":  {},
	"positive_producer_and_explicit_retired_negative": {},
	"positive_producer_and_mutation_base":             {},
}

var canonicalGoCorpusExcludedDirectories = map[string]string{
	".git":   "version-control metadata",
	"vendor": "vendored source is not repository-owned production code",
}

type canonicalFormsRegistry struct {
	Kind            string                         `yaml:"kind"`
	RegistryVersion int                            `yaml:"registry_version"`
	Inventory       canonicalFormsInventory        `yaml:"inventory"`
	Rows            []canonicalFormsRow            `yaml:"rows"`
	DecoderCoverage map[string]map[string][]string `yaml:"decoder_coverage"`
	Wave1           canonicalFormsWave1            `yaml:"wave_1"`
}

type canonicalFormsInventory struct {
	CustomUnmarshalTotal int `yaml:"custom_unmarshal_total"`
}

type canonicalFormsRow struct {
	ID       string                 `yaml:"id"`
	Ruled    string                 `yaml:"ruled"`
	Spelling canonicalFormsSpelling `yaml:"spelling"`
}

type canonicalFormsSpelling struct {
	Canonical          any      `yaml:"canonical"`
	CanonicalCandidate any      `yaml:"canonical_candidate"`
	RetiredDuplicates  []string `yaml:"retired_duplicates"`
}

type canonicalFormsWave1 struct {
	Issue                  int                           `yaml:"issue"`
	Rows                   []string                      `yaml:"rows"`
	CheckedInPackageCorpus canonicalFormsPackageCorpus   `yaml:"checked_in_package_corpus"`
	GoConnectCorpus        canonicalFormsGoConnectCorpus `yaml:"go_connect_corpus"`
}

type canonicalFormsPackageCorpus struct {
	Examples         canonicalFormsCorpusCount `yaml:"examples"`
	TestsAndTestdata canonicalFormsCorpusCount `yaml:"tests_and_testdata"`
	Total            canonicalFormsCorpusCount `yaml:"total"`
}

type canonicalFormsCorpusCount struct {
	Files int `yaml:"files"`
	Rows  int `yaml:"rows"`
}

type canonicalFormsGoConnectCorpus struct {
	TotalFiles                  int               `yaml:"total_files"`
	CanonicalRoutingOccurrences int               `yaml:"canonicalrouting_occurrences"`
	Files                       map[string]string `yaml:"files"`
}

func TestCanonicalFormsRegistryOwnsCompleteDecoderInventory(t *testing.T) {
	root := conformanceRepoRoot(t)
	record := loadCanonicalFormsRegistry(t, root)
	if record.Kind != "canonical_forms_registry" || record.RegistryVersion != 1 {
		t.Fatalf("registry identity = %q/v%d", record.Kind, record.RegistryVersion)
	}
	if got, want := len(record.Rows), 50; got != want {
		t.Fatalf("registry rows = %d, want %d", got, want)
	}

	rows := make(map[string]canonicalFormsRow, len(record.Rows))
	for _, row := range record.Rows {
		if row.ID == "" || row.Ruled == "" {
			t.Fatalf("unruled registry row: %#v", row)
		}
		if _, exists := rows[row.ID]; exists {
			t.Fatalf("duplicate registry row %q", row.ID)
		}
		rows[row.ID] = row
	}
	for _, id := range record.Wave1.Rows {
		if _, ok := rows[id]; !ok {
			t.Fatalf("wave 1 row %q is not in the registry", id)
		}
	}
	if record.Wave1.Issue != 2309 || !reflect.DeepEqual(record.Wave1.Rows, []string{"package.child_collection", "package.child_reference", "package.connect"}) {
		t.Fatalf("wave 1 registry = %#v", record.Wave1)
	}

	expected := make(map[string]string)
	for file, rowMappings := range record.DecoderCoverage {
		for rowID, receiverTypes := range rowMappings {
			if _, ok := rows[rowID]; !ok {
				t.Fatalf("decoder coverage %s references unknown row %q", file, rowID)
			}
			for _, receiverType := range receiverTypes {
				identity := canonicalDecoderIdentity(file, receiverType)
				if previous, exists := expected[identity]; exists {
					t.Fatalf("decoder %s is mapped by both %s and %s", identity, previous, rowID)
				}
				expected[identity] = rowID
			}
		}
	}
	actual, err := collectCustomYAMLDecoders(root)
	if err != nil {
		t.Fatalf("collect custom YAML decoders: %v", err)
	}
	if record.Inventory.CustomUnmarshalTotal != 102 || len(expected) != 102 || len(actual) != 102 {
		t.Fatalf("decoder inventory registry/coverage/source = %d/%d/%d, want 102/102/102", record.Inventory.CustomUnmarshalTotal, len(expected), len(actual))
	}
	if err := validateCustomYAMLDecoderInventory(expected, actual); err != nil {
		t.Fatal(err)
	}
}

func TestCanonicalFormsRegistryRejectsNestedRuntimeDecoderUntilRegistered(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "internal/runtime/gate2313probe/probe.go")
	writeRegistryMutationFile(t, path, `package gate2313probe

import "gopkg.in/yaml.v3"

type Probe struct{}

func (*Probe) UnmarshalYAML(*yaml.Node) error { return nil }
`)
	actual, err := collectCustomYAMLDecoders(root)
	if err != nil {
		t.Fatalf("collect custom YAML decoders: %v", err)
	}
	if err := validateCustomYAMLDecoderInventory(map[string]string{}, actual); err == nil || !strings.Contains(err.Error(), "gate2313probe/probe.go:Probe") {
		t.Fatalf("unregistered decoder validation error = %v, want nested decoder identity", err)
	}
	registered := map[string]string{"gate2313probe/probe.go:Probe": "mutation.probe"}
	if err := validateCustomYAMLDecoderInventory(registered, actual); err != nil {
		t.Fatalf("registered decoder validation: %v", err)
	}
}

func TestCanonicalFormsRegistryPinsWave1RetirementsAndEffectiveStructs(t *testing.T) {
	record := loadCanonicalFormsRegistry(t, conformanceRepoRoot(t))
	rows := make(map[string]canonicalFormsRow, len(record.Rows))
	for _, row := range record.Rows {
		rows[row.ID] = row
	}
	for id, want := range map[string][]string{
		"package.child_collection": {"children", "subpackages"},
		"package.child_reference":  {"package", "dir"},
	} {
		got := append([]string(nil), rows[id].Spelling.RetiredDuplicates...)
		sort.Strings(got)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s retired spellings = %v, want %v", id, got, want)
		}
	}
	connectRetirements := strings.Join(rows["package.connect"].Spelling.RetiredDuplicates, " ")
	if !strings.Contains(connectRetirements, "flow.pin") {
		t.Fatalf("package.connect retirements = %q, want endpoint-centric form", connectRetirements)
	}

	assertNoYAMLFields(t, reflect.TypeOf(runtimecontracts.ProjectPackageDocument{}), "children", "subpackages")
	assertNoYAMLFields(t, reflect.TypeOf(runtimecontracts.ProjectPackageRef{}), "package", "dir")
	assertExactYAMLFields(t, reflect.TypeOf(runtimecontracts.FlowPackageConnect{}), []string{"adapter", "event", "from", "rename", "to"})
}

func TestCanonicalFormsRegistryPinsWave1CorpusLedger(t *testing.T) {
	root := conformanceRepoRoot(t)
	record := loadCanonicalFormsRegistry(t, root)
	counts := canonicalFormsPackageCorpus{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "package.yaml" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var packageDocument struct {
			Flows []struct {
				ID string `yaml:"id"`
			} `yaml:"flows"`
			Connect []struct {
				Event string `yaml:"event"`
				From  string `yaml:"from"`
				To    string `yaml:"to"`
			} `yaml:"connect"`
		}
		if err := yaml.Unmarshal(raw, &packageDocument); err != nil {
			return fmt.Errorf("decode %s: %w", path, err)
		}
		if len(packageDocument.Connect) == 0 {
			return nil
		}
		visibleFlows := map[string]struct{}{".": {}}
		for _, flow := range packageDocument.Flows {
			if id := strings.TrimSpace(flow.ID); id != "" {
				visibleFlows[id] = struct{}{}
			}
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		for _, connect := range packageDocument.Connect {
			if strings.TrimSpace(connect.Event) == "" || strings.TrimSpace(connect.From) == "" || strings.TrimSpace(connect.To) == "" {
				return fmt.Errorf("%s retains a non-canonical connect row: %#v", relative, connect)
			}
			if _, ok := visibleFlows[connect.From]; !ok {
				return fmt.Errorf("%s connect source is not an exact package-visible flow: %#v", relative, connect)
			}
			if _, ok := visibleFlows[connect.To]; !ok {
				return fmt.Errorf("%s connect receiver is not an exact package-visible flow: %#v", relative, connect)
			}
		}
		count := &counts.TestsAndTestdata
		if strings.HasPrefix(filepath.ToSlash(relative), "examples/") {
			count = &counts.Examples
		}
		count.Files++
		count.Rows += len(packageDocument.Connect)
		counts.Total.Files++
		counts.Total.Rows += len(packageDocument.Connect)
		return nil
	})
	if err != nil {
		t.Fatalf("scan checked-in package corpus: %v", err)
	}
	if !reflect.DeepEqual(counts, record.Wave1.CheckedInPackageCorpus) {
		t.Fatalf("checked-in package corpus = %#v, registry = %#v", counts, record.Wave1.CheckedInPackageCorpus)
	}

	goFiles, canonicalRoutingOccurrences, err := collectGoConnectCorpus(root, record.Wave1.GoConnectCorpus.Files)
	if err != nil {
		t.Fatalf("scan connect-shaped Go corpus: %v", err)
	}
	if len(goFiles) != record.Wave1.GoConnectCorpus.TotalFiles || !reflect.DeepEqual(goFiles, record.Wave1.GoConnectCorpus.Files) {
		t.Fatalf("classified connect-shaped Go corpus = %d files, registry = %d; actual=%v", len(goFiles), record.Wave1.GoConnectCorpus.TotalFiles, mapKeyDifference(goFiles, record.Wave1.GoConnectCorpus.Files))
	}
	if canonicalRoutingOccurrences != record.Wave1.GoConnectCorpus.CanonicalRoutingOccurrences {
		t.Fatalf("canonicalrouting connect occurrences = %d, want %d", canonicalRoutingOccurrences, record.Wave1.GoConnectCorpus.CanonicalRoutingOccurrences)
	}
}

func TestCanonicalFormsRegistryRejectsOutsideInternalConnectProducerUntilRegistered(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "cmd/gate2313probe/main.go")
	writeRegistryMutationFile(t, path, "package main\n\nvar packageDocument = `"+"connect"+": []`\n")
	if _, _, err := collectGoConnectCorpus(root, map[string]string{}); err == nil || !strings.Contains(err.Error(), "cmd/gate2313probe/main.go") {
		t.Fatalf("unregistered producer validation error = %v, want outside-internal producer path", err)
	}
	registered := map[string]string{"cmd/gate2313probe/main.go": "positive_producer"}
	files, _, err := collectGoConnectCorpus(root, registered)
	if err != nil {
		t.Fatalf("registered producer validation: %v", err)
	}
	if !reflect.DeepEqual(files, registered) {
		t.Fatalf("registered producer files = %#v, want %#v", files, registered)
	}
	registered["cmd/gate2313probe/main.go"] = "open_ended_classification"
	if _, _, err := collectGoConnectCorpus(root, registered); err == nil || !strings.Contains(err.Error(), "unsupported classification") {
		t.Fatalf("open classification validation error = %v, want closed-set rejection", err)
	}
}

func loadCanonicalFormsRegistry(t testing.TB, root string) canonicalFormsRegistry {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, canonicalFormsRegistryPath))
	if err != nil {
		t.Fatalf("read canonical forms registry: %v", err)
	}
	var record canonicalFormsRegistry
	if err := yaml.Unmarshal(raw, &record); err != nil {
		t.Fatalf("decode canonical forms registry: %v", err)
	}
	return record
}

func collectCustomYAMLDecoders(root string) (map[string]string, error) {
	out := make(map[string]string)
	runtimeRoot := filepath.Join(root, "internal/runtime")
	err := filepath.WalkDir(runtimeRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Contains(raw, []byte("UnmarshalYAML")) {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		relative, err := filepath.Rel(runtimeRoot, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(relative)
		key = strings.TrimPrefix(key, "contracts/")
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != "UnmarshalYAML" || fn.Recv == nil || len(fn.Recv.List) != 1 {
				continue
			}
			receiverType := yamlReceiverType(fn.Recv.List[0].Type)
			if receiverType == "" {
				return fmt.Errorf("unsupported UnmarshalYAML receiver in %s", path)
			}
			identity := canonicalDecoderIdentity(key, receiverType)
			out[identity] = path
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func validateCustomYAMLDecoderInventory(expected, actual map[string]string) error {
	missing, extra := mapKeyDifference(expected, actual), mapKeyDifference(actual, expected)
	if len(missing) > 0 || len(extra) > 0 {
		return fmt.Errorf("decoder inventory drift\nmissing from source: %v\nunmapped source decoders: %v", missing, extra)
	}
	return nil
}

func collectGoConnectCorpus(root string, classifications map[string]string) (map[string]string, int, error) {
	for path, classification := range classifications {
		if _, ok := canonicalGoConnectClassifications[classification]; !ok {
			return nil, 0, fmt.Errorf("connect-shaped Go file %s has unsupported classification %q", path, classification)
		}
	}
	files := make(map[string]string)
	canonicalRoutingOccurrences := 0
	connectMarker := []byte("connect" + ":")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root {
				if _, excluded := canonicalGoCorpusExcludedDirectories[entry.Name()]; excluded {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Contains(raw, connectMarker) {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		classification, ok := classifications[relative]
		if !ok {
			return fmt.Errorf("connect-shaped Go file %s is not classified", relative)
		}
		files[relative] = classification
		if strings.Contains(relative, "/testfixtures/canonicalrouting/") {
			canonicalRoutingOccurrences += bytes.Count(raw, connectMarker)
		}
		return nil
	})
	return files, canonicalRoutingOccurrences, err
}

func writeRegistryMutationFile(t testing.TB, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create mutation fixture directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write mutation fixture: %v", err)
	}
}

func yamlReceiverType(expr ast.Expr) string {
	if pointer, ok := expr.(*ast.StarExpr); ok {
		expr = pointer.X
	}
	if identifier, ok := expr.(*ast.Ident); ok {
		return identifier.Name
	}
	return ""
}

func canonicalDecoderIdentity(file, receiverType string) string {
	return strings.TrimSpace(file) + ":" + strings.TrimSpace(receiverType)
}

func mapKeyDifference(left, right map[string]string) []string {
	var out []string
	for key := range left {
		if _, ok := right[key]; !ok {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func assertNoYAMLFields(t testing.TB, typ reflect.Type, forbidden ...string) {
	t.Helper()
	for _, field := range reflect.VisibleFields(typ) {
		tag := strings.Split(field.Tag.Get("yaml"), ",")[0]
		for _, name := range forbidden {
			if tag == name {
				t.Fatalf("%s retains retired yaml field %q", typ.Name(), name)
			}
		}
	}
}

func assertExactYAMLFields(t testing.TB, typ reflect.Type, want []string) {
	t.Helper()
	var got []string
	for _, field := range reflect.VisibleFields(typ) {
		tag := strings.Split(field.Tag.Get("yaml"), ",")[0]
		if tag != "" && tag != "-" {
			got = append(got, tag)
		}
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s yaml fields = %v, want %v", typ.Name(), got, want)
	}
}
