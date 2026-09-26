package conformance

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
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

var canonicalDecoderExclusionClassifications = map[string]struct{}{
	"deployment.swarm_yaml": {},
}

var canonicalDecodeBypassFamilies = map[string]struct{}{
	"canonical_owner":                {},
	"wave_4_node_handler":            {},
	"wave_5_agent_tool_policy":       {},
	"mixed_wave_4_wave_5":            {},
	"mixed_existing_typed_admission": {},
	"closure_pack_platform":          {},
	"other_production_projection":    {},
	"excluded_deployment_config":     {},
	"excluded_scenario_tooling":      {},
	"excluded_test_fixture":          {},
	"excluded_test_tooling":          {},
}

var unquotedConnectYAMLKey = regexp.MustCompile(`(?m)^[\t ]*(?:-[\t ]+)?connect[\t ]*:`)

type canonicalFormsRegistry struct {
	Kind              string                         `yaml:"kind"`
	RegistryVersion   int                            `yaml:"registry_version"`
	Inventory         canonicalFormsInventory        `yaml:"inventory"`
	Rows              []canonicalFormsRow            `yaml:"rows"`
	DecoderCoverage   map[string]map[string][]string `yaml:"decoder_coverage"`
	DecoderRetired    map[string]map[string][]string `yaml:"decoder_retired"`
	DecoderExclusions map[string]map[string][]string `yaml:"decoder_exclusions"`
	DecodeBypasses    canonicalDecodeBypassInventory `yaml:"decode_bypasses"`
	Wave1             canonicalFormsWave1            `yaml:"wave_1"`
	Wave2             canonicalFormsWave2            `yaml:"wave_2"`
	Wave3             canonicalFormsWave3            `yaml:"wave_3"`
}

type canonicalFormsInventory struct {
	CustomUnmarshalTotal     int `yaml:"custom_unmarshal_total"`
	CustomUnmarshalReachable int `yaml:"custom_unmarshal_reachable"`
	CustomUnmarshalRetired   int `yaml:"custom_unmarshal_retired"`
	CustomUnmarshalExcluded  int `yaml:"custom_unmarshal_excluded"`
}

type canonicalDecodeBypassInventory struct {
	NodeSiteCeiling     int                                  `yaml:"node_site_ceiling"`
	DirectDecodeCeiling int                                  `yaml:"off_owner_direct_decode_ceiling"`
	Files               map[string]canonicalDecodeBypassFile `yaml:"files"`
}

type canonicalDecodeBypassFile struct {
	Family            string `yaml:"family"`
	NodeSites         int    `yaml:"node_sites"`
	DirectDecodeRoots int    `yaml:"direct_decode_roots"`
}

const (
	initialCustomUnmarshalCeiling  = 91
	initialReachableDecoderCeiling = 86
	initialOffOwnerNodeSiteCeiling = 290
	initialDirectDecodeCeiling     = 30
)

type canonicalFormsRow struct {
	ID                 string                 `yaml:"id"`
	Ruled              string                 `yaml:"ruled"`
	Spelling           canonicalFormsSpelling `yaml:"spelling"`
	IdentityRetirement string                 `yaml:"identity_retirement"`
}

func TestCanonicalFormsRegistryPinsNodeIDRetirement(t *testing.T) {
	registry := loadCanonicalFormsRegistry(t, conformanceRepoRoot(t))
	for _, row := range registry.Rows {
		if row.ID != "node.identity_defaults_projections" {
			continue
		}
		if !strings.Contains(row.IdentityRetirement, "#2308") || !strings.Contains(row.IdentityRetirement, "Other projections in this row remain open") {
			t.Fatalf("identity-only closure not recorded: %#v", row)
		}
		if !reflect.DeepEqual(row.Spelling.RetiredDuplicates, []string{"node.id", "execution_type_system_node", "subscribes_to_equal_to_handler_keys", "produces_equal_to_emit_sites"}) {
			t.Fatalf("node retirement classification: %#v", row.Spelling)
		}
		if _, ok := reflect.TypeOf(runtimecontracts.SystemNodeContract{}).FieldByName("ID"); ok {
			t.Fatal("retired node ID carrier restored")
		}
		var nodes map[string]runtimecontracts.SystemNodeContract
		if err := yaml.Unmarshal([]byte("worker:\n  id: worker\n  event_handlers: {}\n"), &nodes); err == nil || !strings.Contains(err.Error(), "node.id is retired; the map key is the identity.") {
			t.Fatalf("node ID admitted: %v", err)
		}
		return
	}
	t.Fatal("node identity registry row missing")
}

type canonicalFormsSpelling struct {
	Canonical          any      `yaml:"canonical"`
	CanonicalCandidate any      `yaml:"canonical_candidate"`
	RetiredDuplicates  []string `yaml:"retired_duplicates"`
}

type canonicalFormsWave1 struct {
	Issue           int                           `yaml:"issue"`
	Rows            []string                      `yaml:"rows"`
	GoConnectCorpus canonicalFormsGoConnectCorpus `yaml:"go_connect_corpus"`
}

type canonicalFormsWave3 struct {
	Issue              int      `yaml:"issue"`
	Status             string   `yaml:"status"`
	Rows               []string `yaml:"rows"`
	MigratedDecoders   []string `yaml:"migrated_decoders"`
	RemainingReachable int      `yaml:"remaining_reachable_decoders"`
}

type canonicalFormsWave2 struct {
	Issue           int      `yaml:"issue"`
	Status          string   `yaml:"status"`
	Rows            []string `yaml:"rows"`
	RetiredSurfaces []string `yaml:"retired_surfaces"`
	CanonicalOwners []string `yaml:"canonical_owners"`
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
	if got, want := len(record.Rows), 45; got != want {
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
	if record.Wave1.Issue != 2320 || !reflect.DeepEqual(record.Wave1.Rows, []string{"schema.connect", "schema.imports", "schema.ingress"}) {
		t.Fatalf("wave 1 registry = %#v", record.Wave1)
	}

	expectedReachable := make(map[string]string)
	for file, rowMappings := range record.DecoderCoverage {
		for rowID, receiverTypes := range rowMappings {
			if _, ok := rows[rowID]; !ok {
				t.Fatalf("decoder coverage %s references unknown row %q", file, rowID)
			}
			for _, receiverType := range receiverTypes {
				identity := canonicalDecoderIdentity(file, receiverType)
				if previous, exists := expectedReachable[identity]; exists {
					t.Fatalf("decoder %s is mapped by both %s and %s", identity, previous, rowID)
				}
				expectedReachable[identity] = rowID
			}
		}
	}
	expectedExcluded := make(map[string]string)
	expectedRetired := make(map[string]string)
	for file, rowMappings := range record.DecoderRetired {
		for rowID, receiverTypes := range rowMappings {
			if _, ok := rows[rowID]; !ok {
				t.Fatalf("retired decoder %s references unknown row %q", file, rowID)
			}
			for _, receiverType := range receiverTypes {
				identity := canonicalDecoderIdentity(file, receiverType)
				if owner, exists := expectedReachable[identity]; exists {
					t.Fatalf("decoder %s is both reachable through %s and retired", identity, owner)
				}
				if previous, exists := expectedRetired[identity]; exists {
					t.Fatalf("decoder %s is retired twice under %s and %s", identity, previous, rowID)
				}
				expectedRetired[identity] = rowID
			}
		}
	}
	for file, surfaceMappings := range record.DecoderExclusions {
		for surface, receiverTypes := range surfaceMappings {
			if _, ok := canonicalDecoderExclusionClassifications[surface]; !ok {
				t.Fatalf("decoder exclusion %s has unsupported surface %q", file, surface)
			}
			for _, receiverType := range receiverTypes {
				identity := canonicalDecoderIdentity(file, receiverType)
				if owner, exists := expectedReachable[identity]; exists {
					t.Fatalf("decoder %s is both reachable through %s and excluded as %s", identity, owner, surface)
				}
				if owner, exists := expectedRetired[identity]; exists {
					t.Fatalf("decoder %s is both retired through %s and excluded as %s", identity, owner, surface)
				}
				if previous, exists := expectedExcluded[identity]; exists {
					t.Fatalf("decoder %s is excluded by both %s and %s", identity, previous, surface)
				}
				expectedExcluded[identity] = surface
			}
		}
	}
	actual, err := collectCustomYAMLDecoders(root)
	if err != nil {
		t.Fatalf("collect custom YAML decoders: %v", err)
	}
	classified := make(map[string]string, len(expectedReachable)+len(expectedRetired))
	for identity, rowID := range expectedReachable {
		classified[identity] = rowID
	}
	for identity, rowID := range expectedRetired {
		classified[identity] = rowID
	}
	if err := validateCustomYAMLDecoderInventory(classified, expectedExcluded, actual); err != nil {
		t.Fatal(err)
	}
	if record.Inventory.CustomUnmarshalTotal != len(actual) || record.Inventory.CustomUnmarshalReachable != len(expectedReachable) || record.Inventory.CustomUnmarshalRetired != len(expectedRetired) || record.Inventory.CustomUnmarshalExcluded != len(expectedExcluded) {
		t.Fatalf("decoder inventory total/reachable/retired/excluded = %d/%d/%d/%d, source/classified = %d/%d/%d/%d", record.Inventory.CustomUnmarshalTotal, record.Inventory.CustomUnmarshalReachable, record.Inventory.CustomUnmarshalRetired, record.Inventory.CustomUnmarshalExcluded, len(actual), len(expectedReachable), len(expectedRetired), len(expectedExcluded))
	}
	if len(actual) > initialCustomUnmarshalCeiling || len(expectedReachable) > initialReachableDecoderCeiling {
		t.Fatalf("decoder count exceeded initial census: total=%d reachable=%d", len(actual), len(expectedReachable))
	}
}

func TestCanonicalFormsRegistryRatchetsOffOwnerDecodeBypasses(t *testing.T) {
	root := conformanceRepoRoot(t)
	record := loadCanonicalFormsRegistry(t, root)
	actual, err := collectYAMLDecodeSites(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateYAMLDecodeSites(record.DecodeBypasses, actual); err != nil {
		t.Fatal(err)
	}
	if baseSHA := os.Getenv("SWARM_CANONICAL_FORMS_BASE_SHA"); baseSHA != "" {
		base := loadCanonicalFormsRegistryAtRevision(t, root, baseSHA)
		if err := validateDecodeCensusMonotone(base, record); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCanonicalFormsRegistryRejectsUnregisteredDecodeBypasses(t *testing.T) {
	root := t.TempDir()
	writeRegistryMutationFile(t, filepath.Join(root, "internal/runtime/contracts/new.go"), `package contracts
import source "gopkg.in/yaml.v3"
var raw *source.Node
`)
	writeRegistryMutationFile(t, filepath.Join(root, "internal/packs/new.go"), `package packs
import "gopkg.in/yaml.v3"
func decode(raw []byte) error { return yaml.Unmarshal(raw, new(any)) }
`)
	actual, err := collectYAMLDecodeSites(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateYAMLDecodeSites(canonicalDecodeBypassInventory{Files: map[string]canonicalDecodeBypassFile{}}, actual); err == nil || !strings.Contains(err.Error(), "internal/runtime/contracts/new.go") || !strings.Contains(err.Error(), "internal/packs/new.go") {
		t.Fatalf("unregistered bypass error = %v", err)
	}
	path := "internal/runtime/contracts/new.go"
	packPath := "internal/packs/new.go"
	baseline := canonicalDecodeBypassInventory{
		NodeSiteCeiling:     1,
		DirectDecodeCeiling: 1,
		Files: map[string]canonicalDecodeBypassFile{
			path:     {Family: "wave_4_node_handler", NodeSites: 1},
			packPath: {Family: "closure_pack_platform", DirectDecodeRoots: 1},
		},
	}
	if err := validateYAMLDecodeSites(baseline, actual); err != nil {
		t.Fatalf("classified bypass validation: %v", err)
	}
	writeRegistryMutationFile(t, filepath.Join(root, path), `package contracts
import source "gopkg.in/yaml.v3"
var first *source.Node
var second *source.Node
`)
	actual, err = collectYAMLDecodeSites(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateYAMLDecodeSites(baseline, actual); err == nil || !strings.Contains(err.Error(), "recorded node/root=1/0, actual=2/0") {
		t.Fatalf("increased existing-file bypass error = %v", err)
	}
	writeRegistryMutationFile(t, filepath.Join(root, "internal/yamlsource/source.go"), `package yamlsource
import "gopkg.in/yaml.v3"
var owner *yaml.Node
func decode(raw []byte) error { return yaml.Unmarshal(raw, new(any)) }
`)
	writeRegistryMutationFile(t, filepath.Join(root, "internal/runtime/contracts/new_test.go"), `package contracts
import "gopkg.in/yaml.v3"
var fixture *yaml.Node
`)
	actual, err = collectYAMLDecodeSites(root)
	if err != nil {
		t.Fatal(err)
	}
	if owner := actual["internal/yamlsource/source.go"]; owner.NodeSites != 0 || owner.DirectDecodeRoots != 1 {
		t.Fatalf("canonical owner classification = %#v, want one direct root and zero off-owner node sites", owner)
	}
	if _, exists := actual["internal/runtime/contracts/new_test.go"]; exists {
		t.Fatal("test-only fixture was included in production bypass inventory")
	}
	for _, path := range []string{
		"internal/cliapp/unified_config.go",
		"internal/providertriggers/providertriggers.go",
		"internal/providerconnectors/catalog_types.go",
		"internal/newingress/probe.go",
	} {
		t.Run(path, func(t *testing.T) {
			mutationRoot := t.TempDir()
			writeRegistryMutationFile(t, filepath.Join(mutationRoot, path), `package probe
import source "gopkg.in/yaml.v3"
func decode(raw []byte) error { return source.Unmarshal(raw, new(any)) }
`)
			got, err := collectYAMLDecodeSites(mutationRoot)
			if err != nil {
				t.Fatal(err)
			}
			if got[path].DirectDecodeRoots != 1 {
				t.Fatalf("unclassified direct root in %s was not discovered: %#v", path, got[path])
			}
			if err := validateYAMLDecodeSites(canonicalDecodeBypassInventory{Files: map[string]canonicalDecodeBypassFile{}}, got); err == nil || !strings.Contains(err.Error(), path) {
				t.Fatalf("unclassified direct root %s was not rejected: %v", path, err)
			}
		})
	}
	baselineCensus := canonicalDecodeBypassInventory{
		NodeSiteCeiling: 2, DirectDecodeCeiling: 1,
		Files: map[string]canonicalDecodeBypassFile{
			"internal/runtime/contracts/a.go": {Family: "wave_4_node_handler", NodeSites: 2, DirectDecodeRoots: 1},
		},
	}
	reducedCensus := canonicalDecodeBypassInventory{
		NodeSiteCeiling: 1, DirectDecodeCeiling: 0,
		Files: map[string]canonicalDecodeBypassFile{
			"internal/runtime/contracts/a.go": {Family: "wave_4_node_handler", NodeSites: 1},
		},
	}
	if err := validateDecodeBypassMonotone(baselineCensus, reducedCensus); err != nil {
		t.Fatalf("accepted reduction rejected: %v", err)
	}
	if err := validateDecodeBypassMonotone(reducedCensus, baselineCensus); err == nil || !strings.Contains(err.Error(), "budget rose") {
		t.Fatalf("restored lower budget accepted: %v", err)
	}
	sameTotalOtherFamily := canonicalDecodeBypassInventory{
		NodeSiteCeiling: 2, DirectDecodeCeiling: 1,
		Files: map[string]canonicalDecodeBypassFile{
			"internal/runtime/contracts/b.go": {Family: "wave_5_agent_tool_policy", NodeSites: 2, DirectDecodeRoots: 1},
		},
	}
	if err := validateDecodeBypassMonotone(baselineCensus, sameTotalOtherFamily); err == nil || !strings.Contains(err.Error(), "contracts/b.go") {
		t.Fatalf("same-total cross-family reintroduction accepted: %v", err)
	}
	baselineDecoders := canonicalFormsRegistry{
		Inventory:       canonicalFormsInventory{CustomUnmarshalTotal: 2, CustomUnmarshalReachable: 2},
		DecoderCoverage: map[string]map[string][]string{"a.go": {"handler.container": {"A", "B"}}},
	}
	reducedDecoders := canonicalFormsRegistry{
		Inventory:       canonicalFormsInventory{CustomUnmarshalTotal: 1, CustomUnmarshalReachable: 1},
		DecoderCoverage: map[string]map[string][]string{"a.go": {"handler.container": {"A"}}},
	}
	if err := validateDecodeCensusMonotone(baselineDecoders, reducedDecoders); err != nil {
		t.Fatalf("accepted decoder reduction rejected: %v", err)
	}
	if err := validateDecodeCensusMonotone(reducedDecoders, baselineDecoders); err == nil || !strings.Contains(err.Error(), "budget rose") {
		t.Fatalf("restored decoder budget accepted: %v", err)
	}
}

func TestCanonicalFormsRegistryPinsWave3EventAdmission(t *testing.T) {
	root := conformanceRepoRoot(t)
	record := loadCanonicalFormsRegistry(t, root)
	wantDecoders := []string{
		"workflow_contract_yaml_handlers.go:EventCatalogEntry",
		"workflow_contract_yaml_schema.go:EventFieldSpec",
	}
	if record.Wave3.Issue != 2300 || record.Wave3.Status != "closed" || !reflect.DeepEqual(record.Wave3.Rows, []string{"event.schema_ownership"}) || !reflect.DeepEqual(record.Wave3.MigratedDecoders, wantDecoders) || record.Wave3.RemainingReachable != 95 {
		t.Fatalf("wave 3 registry = %#v", record.Wave3)
	}

	migratedFiles := []string{
		"internal/runtime/contracts/event_catalog_admission.go",
		"internal/runtime/contracts/event_schema_ownership.go",
		"internal/runtime/contracts/effective_provenance.go",
	}
	for _, relative := range migratedFiles {
		path := filepath.Join(root, filepath.FromSlash(relative))
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if selector.Sel.Name == "Decode" || selector.Sel.Name == "Kind" || selector.Sel.Name == "Tag" || selector.Sel.Name == "Unmarshal" {
				t.Errorf("migrated W3 file %s contains raw YAML bypass selector %s", relative, selector.Sel.Name)
			}
			return true
		})
	}

	actual, err := collectCustomYAMLDecoders(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, retired := range wantDecoders {
		if _, exists := actual[retired]; exists {
			t.Errorf("retired W3 decoder %s reappeared", retired)
		}
	}

	treeSource, err := os.ReadFile(filepath.Join(root, "internal/runtime/contracts/workflow_contract_tree.go"))
	if err != nil {
		t.Fatal(err)
	}
	if got := bytes.Count(treeSource, []byte("loadOptionalEventCatalogFromSource(")); got != 1 {
		t.Fatalf("event catalog artifact loader call count = %d, want one flow-source admission call", got)
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
	if err := validateCustomYAMLDecoderInventory(map[string]string{}, map[string]string{}, actual); err == nil || !strings.Contains(err.Error(), "gate2313probe/probe.go:Probe") {
		t.Fatalf("unregistered decoder validation error = %v, want nested decoder identity", err)
	}
	registered := map[string]string{"gate2313probe/probe.go:Probe": "mutation.probe"}
	if err := validateCustomYAMLDecoderInventory(registered, map[string]string{}, actual); err != nil {
		t.Fatalf("registered decoder validation: %v", err)
	}
}

func TestCanonicalFormsRegistryRejectsNonRuntimeDecoderUntilClassified(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "internal/packs/nested/gate2313probe/probe.go")
	writeRegistryMutationFile(t, path, `package gate2313probe

import "gopkg.in/yaml.v3"

type Probe struct{}

func (*Probe) UnmarshalYAML(*yaml.Node) error { return nil }
`)
	actual, err := collectCustomYAMLDecoders(root)
	if err != nil {
		t.Fatalf("collect custom YAML decoders: %v", err)
	}
	identity := "internal/packs/nested/gate2313probe/probe.go:Probe"
	if err := validateCustomYAMLDecoderInventory(map[string]string{}, map[string]string{}, actual); err == nil || !strings.Contains(err.Error(), identity) {
		t.Fatalf("unregistered decoder validation error = %v, want non-runtime decoder identity", err)
	}
	registered := map[string]string{identity: "mutation.probe"}
	if err := validateCustomYAMLDecoderInventory(registered, map[string]string{}, actual); err != nil {
		t.Fatalf("registered decoder validation: %v", err)
	}
}

func TestCanonicalFormsRegistryPinsCurrentConnectFailureCodes(t *testing.T) {
	root := conformanceRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	var spec struct {
		FlowModel struct {
			CompositionRouting struct {
				RoutePlanLowering struct {
					ImplementationSlice1545 struct {
						FailureReasons []string `yaml:"failure_reasons"`
					} `yaml:"implementation_slice_1545"`
				} `yaml:"route_plan_lowering"`
			} `yaml:"composition_routing"`
		} `yaml:"flow_model"`
	}
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("decode platform spec: %v", err)
	}
	got := append([]string(nil), spec.FlowModel.CompositionRouting.RoutePlanLowering.ImplementationSlice1545.FailureReasons...)
	want := make([]string, 0, int(runtimepinrouting.ConnectFailureLifecycleUnavailable))
	for failure := runtimepinrouting.ConnectFailureSourceMissing; failure <= runtimepinrouting.ConnectFailureLifecycleUnavailable; failure++ {
		want = append(want, failure.Code())
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("platform connect failure reasons = %v, runtime owner = %v", got, want)
	}
	if _, err := runtimepinrouting.ParseTargetFailure("connect_pin_ref_invalid"); err == nil {
		t.Fatal("retired connect_pin_ref_invalid remains API-admissible")
	}
}

func TestCanonicalFormsRegistryPinsFilesystemTopologyRetirementAndEffectiveConnect(t *testing.T) {
	record := loadCanonicalFormsRegistry(t, conformanceRepoRoot(t))
	rows := make(map[string]canonicalFormsRow, len(record.Rows))
	for _, row := range record.Rows {
		rows[row.ID] = row
	}
	connectRetirements := strings.Join(rows["schema.connect"].Spelling.RetiredDuplicates, " ")
	if !strings.Contains(connectRetirements, "flow.pin") {
		t.Fatalf("schema.connect retirements = %q, want endpoint-centric form", connectRetirements)
	}

	assertExactYAMLFields(t, reflect.TypeOf(runtimecontracts.FlowConnect{}), []string{"event", "from", "rename", "to"})
}

func TestTypedFieldDecoderFamilyUsesYAMLSourceProjectionOnly(t *testing.T) {
	root := conformanceRepoRoot(t)
	path := filepath.Join(root, "internal/runtime/contracts/workflow_contract_yaml_wave1.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("decodeWave1FieldNode")) {
		t.Fatal("retired decodeWave1FieldNode owner reappeared")
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
	if err != nil {
		t.Fatalf("parse paired decoder family: %v", err)
	}
	want := map[string]bool{
		"projectTypeCatalogDocument":     false,
		"projectNamedTypeDeclarations":   false,
		"projectNamedTypeDeclaration":    false,
		"projectTypeFieldSpec":           false,
		"projectEntityContractsDocument": false,
		"projectEntityContract":          false,
		"projectEntityFieldDecl":         false,
		"decodeWave1FieldValue":          false,
	}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		if _, tracked := want[function.Name.Name]; !tracked {
			continue
		}
		want[function.Name.Name] = true
		ast.Inspect(function.Body, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch selector.Sel.Name {
			case "Decode", "Kind", "Tag", "Content":
				t.Errorf("%s uses forbidden raw yaml.Node selector %s", function.Name.Name, selector.Sel.Name)
			}
			return true
		})
	}
	for name, found := range want {
		if !found {
			t.Errorf("paired decoder owner %s missing", name)
		}
	}
}

func TestCanonicalFormsRegistryPinsWave2RetirementsAndOwners(t *testing.T) {
	record := loadCanonicalFormsRegistry(t, conformanceRepoRoot(t))
	wantRows := []string{"flow.pin_event_entry", "flow.input_pin_resolution", "flow.output_pin_route_projection", "schema.connect"}
	wantRetired := []string{"pin.name", "input.carries", "carry.type", "carry.optional", "carry.convert", "output.key", "output.carries", "qualified_or_wildcard_flow_pin_event", "connect.adapter"}
	wantOwners := []string{"CompiledFlowInputPin", "CompiledFlowOutputPin", "CompiledFlowEntityPermissions", "CompiledEventSchema", "ConnectRoutePlan"}
	if record.Wave2.Issue != 2352 || record.Wave2.Status != "closed" || !reflect.DeepEqual(record.Wave2.Rows, wantRows) || !reflect.DeepEqual(record.Wave2.RetiredSurfaces, wantRetired) || !reflect.DeepEqual(record.Wave2.CanonicalOwners, wantOwners) {
		t.Fatalf("wave 2 registry = %#v", record.Wave2)
	}
	assertExactYAMLFields(t, reflect.TypeOf(runtimecontracts.FlowInputEventPin{}), []string{"event", "initialize", "resolution", "source"})
	assertExactYAMLFields(t, reflect.TypeOf(runtimecontracts.FlowOutputEventPin{}), []string{"event", "sink"})
	for _, owner := range []reflect.Type{
		reflect.TypeOf(runtimecontracts.CompiledFlowInputPin{}),
		reflect.TypeOf(runtimecontracts.CompiledFlowOutputPin{}),
		reflect.TypeOf(runtimecontracts.CompiledFlowEntityPermissions{}),
		reflect.TypeOf(runtimepinrouting.ConnectRoutePlan{}),
	} {
		for index := 0; index < owner.NumField(); index++ {
			if owner.Field(index).IsExported() {
				t.Fatalf("W2 owner %s exposes mutable field %s", owner, owner.Field(index).Name)
			}
		}
	}
}

func TestCanonicalFormsRegistryWave2ProductionConsumersUseCompiledPins(t *testing.T) {
	root := conformanceRepoRoot(t)
	allowedAdmissionOwners := map[string]struct{}{
		"internal/runtime/contracts/event_schema_ownership.go":      {},
		"internal/runtime/contracts/workflow_contract_connect.go":   {},
		"internal/runtime/contracts/workflow_contract_semantics.go": {},
	}
	var bypasses []string
	err := filepath.WalkDir(filepath.Join(root, "internal", "runtime"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if _, allowed := allowedAdmissionOwners[relative]; allowed {
			return nil
		}
		files := token.NewFileSet()
		parsed, err := parser.ParseFile(files, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			path := canonicalSelectorPath(selector)
			joined := strings.Join(path, ".")
			if strings.HasSuffix(joined, ".Pins.Inputs") || strings.HasSuffix(joined, ".Pins.Outputs") || strings.HasSuffix(joined, ".EventPins") {
				position := files.Position(selector.Pos())
				bypasses = append(bypasses, fmt.Sprintf("%s:%d:%s", relative, position.Line, joined))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("scan W2 production consumers: %v", err)
	}
	if len(bypasses) != 0 {
		sort.Strings(bypasses)
		t.Fatalf("production consumers reconstruct flow-pin semantics outside admission owners: %s", strings.Join(bypasses, ", "))
	}

	planPath := filepath.Join(root, "internal", "runtime", "core", "pinrouting", "connect_route_plan.go")
	files := token.NewFileSet()
	parsed, err := parser.ParseFile(files, planPath, nil, 0)
	if err != nil {
		t.Fatalf("parse compiled edge owner: %v", err)
	}
	constructors := 0
	ast.Inspect(parsed, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok || len(literal.Elts) == 0 {
			return true
		}
		name, ok := literal.Type.(*ast.Ident)
		if ok && name.Name == "ConnectRoutePlan" {
			constructors++
		}
		return true
	})
	if constructors != 1 {
		t.Fatalf("ConnectRoutePlan populated constructors = %d, want one closed constructor", constructors)
	}
}

func canonicalSelectorPath(expr ast.Expr) []string {
	switch value := expr.(type) {
	case *ast.Ident:
		return []string{value.Name}
	case *ast.SelectorExpr:
		return append(canonicalSelectorPath(value.X), value.Sel.Name)
	case *ast.IndexExpr:
		return canonicalSelectorPath(value.X)
	default:
		return nil
	}
}

func TestCanonicalFormsRegistryRejectsOutsideInternalConnectProducerUntilRegistered(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "cmd/gate2313probe/main.go")
	writeRegistryMutationFile(t, path, "package main\n\nvar packageDocument = `"+"con"+"nect : []`\n")
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

func loadCanonicalFormsRegistryAtRevision(t testing.TB, root, revision string) canonicalFormsRegistry {
	t.Helper()
	if matched, _ := regexp.MatchString(`^[0-9a-f]{40}$`, revision); !matched {
		t.Fatalf("invalid accepted-base SHA %q", revision)
	}
	command := exec.Command("git", "show", revision+":"+canonicalFormsRegistryPath)
	command.Dir = root
	raw, err := command.Output()
	if err != nil {
		t.Fatalf("read accepted-base canonical forms registry: %v", err)
	}
	var record canonicalFormsRegistry
	if err := yaml.Unmarshal(raw, &record); err != nil {
		t.Fatalf("decode accepted-base canonical forms registry: %v", err)
	}
	return record
}

func validateDecodeCensusMonotone(base, current canonicalFormsRegistry) error {
	if current.Inventory.CustomUnmarshalTotal > base.Inventory.CustomUnmarshalTotal || current.Inventory.CustomUnmarshalReachable > base.Inventory.CustomUnmarshalReachable {
		return fmt.Errorf("custom decoder budget rose from total/reachable %d/%d to %d/%d", base.Inventory.CustomUnmarshalTotal, base.Inventory.CustomUnmarshalReachable, current.Inventory.CustomUnmarshalTotal, current.Inventory.CustomUnmarshalReachable)
	}
	baseAll := canonicalDecoderIDs(base.DecoderCoverage, base.DecoderRetired, base.DecoderExclusions)
	for identity := range canonicalDecoderIDs(current.DecoderCoverage, current.DecoderRetired, current.DecoderExclusions) {
		if _, ok := baseAll[identity]; !ok {
			return fmt.Errorf("new custom decoder %s is outside the accepted-base census", identity)
		}
	}
	baseReachable := canonicalDecoderIDs(base.DecoderCoverage)
	for identity := range canonicalDecoderIDs(current.DecoderCoverage) {
		if _, ok := baseReachable[identity]; !ok {
			return fmt.Errorf("custom decoder %s became reachable after the accepted base", identity)
		}
	}
	baseExcluded := canonicalDecoderIDs(base.DecoderExclusions)
	for identity := range canonicalDecoderIDs(current.DecoderExclusions) {
		if _, ok := baseExcluded[identity]; !ok {
			return fmt.Errorf("custom decoder %s became excluded after the accepted base", identity)
		}
	}
	// The setup PR introduces this ledger; every later PR compares to its accepted base.
	if base.DecodeBypasses.Files == nil {
		return nil
	}
	return validateDecodeBypassMonotone(base.DecodeBypasses, current.DecodeBypasses)
}

func canonicalDecoderIDs(groups ...map[string]map[string][]string) map[string]struct{} {
	identities := make(map[string]struct{})
	for _, group := range groups {
		for file, mappings := range group {
			for _, receiverTypes := range mappings {
				for _, receiverType := range receiverTypes {
					identities[canonicalDecoderIdentity(file, receiverType)] = struct{}{}
				}
			}
		}
	}
	return identities
}

func validateDecodeBypassMonotone(base, current canonicalDecodeBypassInventory) error {
	if current.NodeSiteCeiling > base.NodeSiteCeiling || current.DirectDecodeCeiling > base.DirectDecodeCeiling {
		return fmt.Errorf("decode budget rose from node/root %d/%d to %d/%d", base.NodeSiteCeiling, base.DirectDecodeCeiling, current.NodeSiteCeiling, current.DirectDecodeCeiling)
	}
	baseFamilies := make(map[string]canonicalDecodeBypassFile)
	currentFamilies := make(map[string]canonicalDecodeBypassFile)
	for _, entry := range base.Files {
		if entry.Family == "canonical_owner" {
			continue
		}
		total := baseFamilies[entry.Family]
		total.NodeSites += entry.NodeSites
		total.DirectDecodeRoots += entry.DirectDecodeRoots
		baseFamilies[entry.Family] = total
	}
	paths := make([]string, 0, len(current.Files))
	for path := range current.Files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		entry := current.Files[path]
		if entry.Family == "canonical_owner" {
			continue
		}
		prior := base.Files[path]
		if entry.NodeSites > prior.NodeSites || entry.DirectDecodeRoots > prior.DirectDecodeRoots {
			return fmt.Errorf("decode site %s rose from node/root %d/%d to %d/%d", path, prior.NodeSites, prior.DirectDecodeRoots, entry.NodeSites, entry.DirectDecodeRoots)
		}
		if prior.Family != entry.Family && (entry.NodeSites != 0 || entry.DirectDecodeRoots != 0) {
			return fmt.Errorf("decode site %s moved from family %q to %q", path, prior.Family, entry.Family)
		}
		total := currentFamilies[entry.Family]
		total.NodeSites += entry.NodeSites
		total.DirectDecodeRoots += entry.DirectDecodeRoots
		currentFamilies[entry.Family] = total
	}
	for family, total := range currentFamilies {
		prior := baseFamilies[family]
		if total.NodeSites > prior.NodeSites || total.DirectDecodeRoots > prior.DirectDecodeRoots {
			return fmt.Errorf("decode family %s rose from node/root %d/%d to %d/%d", family, prior.NodeSites, prior.DirectDecodeRoots, total.NodeSites, total.DirectDecodeRoots)
		}
	}
	return nil
}

func collectCustomYAMLDecoders(root string) (map[string]string, error) {
	out := make(map[string]string)
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
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		key := canonicalDecoderFileKey(filepath.ToSlash(relative))
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

func collectYAMLDecodeSites(root string) (map[string]canonicalDecodeBypassFile, error) {
	out := make(map[string]canonicalDecodeBypassFile)
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
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		owner := strings.HasPrefix(relative, "internal/yamlsource/")
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Contains(raw, []byte("gopkg.in/yaml.v3")) {
			return nil
		}
		files := token.NewFileSet()
		parsed, err := parser.ParseFile(files, path, raw, 0)
		if err != nil {
			return fmt.Errorf("parse %s: %w", relative, err)
		}
		aliases := make(map[string]struct{})
		for _, imported := range parsed.Imports {
			name, err := strconv.Unquote(imported.Path.Value)
			if err != nil || name != "gopkg.in/yaml.v3" {
				continue
			}
			alias := "yaml"
			if imported.Name != nil {
				alias = imported.Name.Name
			}
			if alias == "." {
				return fmt.Errorf("%s imports yaml.v3 with a dot alias; bypass census cannot classify it", relative)
			}
			aliases[alias] = struct{}{}
		}
		if len(aliases) == 0 {
			return nil
		}
		lines := make(map[int]struct{})
		directDecodeRoots := 0
		ast.Inspect(parsed, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.SelectorExpr:
				identifier, ok := value.X.(*ast.Ident)
				if !ok || value.Sel.Name != "Node" {
					break
				}
				if _, ok := aliases[identifier.Name]; ok && !owner {
					lines[files.Position(value.Pos()).Line] = struct{}{}
				}
			case *ast.CallExpr:
				selector, ok := value.Fun.(*ast.SelectorExpr)
				if !ok || (selector.Sel.Name != "Unmarshal" && selector.Sel.Name != "NewDecoder") {
					break
				}
				identifier, ok := selector.X.(*ast.Ident)
				if !ok {
					break
				}
				if _, ok := aliases[identifier.Name]; ok {
					directDecodeRoots++
				}
			}
			return true
		})
		if len(lines) != 0 || directDecodeRoots != 0 {
			out[relative] = canonicalDecodeBypassFile{NodeSites: len(lines), DirectDecodeRoots: directDecodeRoots}
		}
		return nil
	})
	return out, err
}

func validateYAMLDecodeSites(record canonicalDecodeBypassInventory, actual map[string]canonicalDecodeBypassFile) error {
	if record.Files == nil {
		return fmt.Errorf("YAML decode-site inventory is missing")
	}
	if record.NodeSiteCeiling < 0 || record.DirectDecodeCeiling < 0 || record.NodeSiteCeiling > initialOffOwnerNodeSiteCeiling || record.DirectDecodeCeiling > initialDirectDecodeCeiling {
		return fmt.Errorf("off-owner decode ceilings exceed initial census: node=%d direct=%d", record.NodeSiteCeiling, record.DirectDecodeCeiling)
	}
	var missing, unclassified, mismatched []string
	var nodeSites, directDecodeRoots int
	for path, expected := range record.Files {
		if _, known := canonicalDecodeBypassFamilies[expected.Family]; !known {
			return fmt.Errorf("off-owner decode bypass %s has unknown migration family %q", path, expected.Family)
		}
		owner := strings.HasPrefix(path, "internal/yamlsource/")
		if (expected.Family == "canonical_owner") != owner || owner && expected.NodeSites != 0 {
			return fmt.Errorf("YAML decode-site %s has invalid canonical-owner classification", path)
		}
		if expected.NodeSites < 0 || expected.DirectDecodeRoots < 0 {
			return fmt.Errorf("off-owner decode bypass %s has negative count", path)
		}
		nodeSites += expected.NodeSites
		if !owner {
			directDecodeRoots += expected.DirectDecodeRoots
		}
		got, exists := actual[path]
		if !exists {
			missing = append(missing, path)
		} else if expected.NodeSites != got.NodeSites || expected.DirectDecodeRoots != got.DirectDecodeRoots {
			mismatched = append(mismatched, fmt.Sprintf("%s: recorded node/root=%d/%d, actual=%d/%d", path, expected.NodeSites, expected.DirectDecodeRoots, got.NodeSites, got.DirectDecodeRoots))
		}
	}
	for path := range actual {
		if _, exists := record.Files[path]; !exists {
			unclassified = append(unclassified, path)
		}
	}
	sort.Strings(missing)
	sort.Strings(unclassified)
	sort.Strings(mismatched)
	if nodeSites != record.NodeSiteCeiling || directDecodeRoots != record.DirectDecodeCeiling || len(missing) != 0 || len(unclassified) != 0 || len(mismatched) != 0 {
		return fmt.Errorf("off-owner decode bypass drift: recorded node/root=%d/%d, ceiling=%d/%d; missing=%v; unclassified=%v; mismatched=%v", nodeSites, directDecodeRoots, record.NodeSiteCeiling, record.DirectDecodeCeiling, missing, unclassified, mismatched)
	}
	return nil
}

func validateCustomYAMLDecoderInventory(reachable, excluded, actual map[string]string) error {
	expected := make(map[string]string, len(reachable)+len(excluded))
	for identity, classification := range reachable {
		expected[identity] = classification
	}
	for identity, classification := range excluded {
		if previous, exists := expected[identity]; exists {
			return fmt.Errorf("decoder %s is classified as both %s and %s", identity, previous, classification)
		}
		expected[identity] = classification
	}
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
		occurrences, err := countGoStringLiteralConnectKeys(path, raw)
		if err != nil {
			return err
		}
		if occurrences == 0 {
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
			canonicalRoutingOccurrences += occurrences
		}
		return nil
	})
	return files, canonicalRoutingOccurrences, err
}

func countGoStringLiteralConnectKeys(path string, raw []byte) (int, error) {
	parsed, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", path, err)
	}
	count := 0
	ast.Inspect(parsed, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		content, err := strconv.Unquote(literal.Value)
		if err != nil {
			return true
		}
		count += len(unquotedConnectYAMLKey.FindAllStringIndex(content, -1))
		return true
	})
	return count, nil
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

func canonicalDecoderFileKey(relative string) string {
	if key, ok := strings.CutPrefix(relative, "internal/runtime/contracts/"); ok {
		return key
	}
	if key, ok := strings.CutPrefix(relative, "internal/runtime/"); ok {
		return key
	}
	return relative
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
