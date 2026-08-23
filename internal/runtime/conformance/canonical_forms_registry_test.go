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
	actual := collectCustomYAMLDecoders(t, root)
	if record.Inventory.CustomUnmarshalTotal != 101 || len(expected) != 101 || len(actual) != 101 {
		t.Fatalf("decoder inventory registry/coverage/source = %d/%d/%d, want 101/101/101", record.Inventory.CustomUnmarshalTotal, len(expected), len(actual))
	}
	if missing, extra := mapKeyDifference(expected, actual), mapKeyDifference(actual, expected); len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("decoder inventory drift\nmissing from source: %v\nunmapped source decoders: %v", missing, extra)
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

	goFiles := make(map[string]string)
	canonicalRoutingOccurrences := 0
	err = filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		connectMarker := []byte("connect" + ":")
		if !bytes.Contains(raw, connectMarker) {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		classification, ok := record.Wave1.GoConnectCorpus.Files[relative]
		if !ok {
			return fmt.Errorf("connect-shaped Go file %s is not classified", relative)
		}
		goFiles[relative] = classification
		if strings.Contains(relative, "/testfixtures/canonicalrouting/") {
			canonicalRoutingOccurrences += bytes.Count(raw, connectMarker)
		}
		return nil
	})
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

func collectCustomYAMLDecoders(t testing.TB, root string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	for _, dir := range []string{"contracts", "flowmodel", "agentintent"} {
		base := filepath.Join(root, "internal/runtime", dir)
		entries, err := os.ReadDir(base)
		if err != nil {
			t.Fatalf("read decoder directory %s: %v", dir, err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(base, entry.Name())
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			for _, decl := range parsed.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Name.Name != "UnmarshalYAML" || fn.Recv == nil || len(fn.Recv.List) != 1 {
					continue
				}
				receiverType := yamlReceiverType(fn.Recv.List[0].Type)
				if receiverType == "" {
					t.Fatalf("unsupported UnmarshalYAML receiver in %s", path)
				}
				key := entry.Name()
				if dir != "contracts" {
					key = dir + "/" + key
				}
				identity := canonicalDecoderIdentity(key, receiverType)
				out[identity] = path
			}
		}
	}
	return out
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
