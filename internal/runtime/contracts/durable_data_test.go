package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
)

func TestAuthoredEventOwnsDatasetSchemaIdentityAndAgentAccess(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := writePromptTestBundle(t, repo)
	appendFixtureFile(t, filepath.Join(root, "flows", "intake", "events.yaml"), `
score.available:
  slug: text
  score: integer
`)
	appendFixtureFile(t, filepath.Join(root, "agents.yaml"), `
  data_access:
    - data: intake/score.available
`)

	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	ref, err := durableDataDeclarationRef(".", "intake/score.available")
	if err != nil {
		t.Fatal(err)
	}
	declaration, ok := bundle.DurableDataDeclarationByRef(ref)
	if !ok {
		t.Fatalf("event-owned declaration %s missing from %#v", ref.Key(), bundle.DurableDataDeclarations())
	}
	if declaration.Name != "intake/score.available" || declaration.BusinessKey != "" ||
		declaration.SchemaDigest == "" || len(declaration.CanonicalSchema) == 0 {
		t.Fatalf("durable declaration = %#v", declaration)
	}
	var consumer AgentDeclarationRecord
	for _, record := range bundle.AgentDeclarationRecords() {
		if len(record.Entry.DataAccess) != 0 {
			consumer = record
			break
		}
	}
	if consumer.LogicalID == "" {
		t.Fatal("agent carrying data_access was not compiled")
	}
	access := bundle.DurableDataForAgent(consumer.Source.PackageKey, consumer.OwnerFlowID, consumer.LogicalID)
	if len(access) != 1 || access[0] != declaration.Ref {
		t.Fatalf("compiled resource access = %#v, want %#v", access, declaration.Ref)
	}
	if !bundle.DataProjectionRequired() {
		t.Fatal("compiled data_access did not require a workspace projection")
	}

	before := declaration.SchemaDigest
	eventsPath := filepath.Join(root, "flows", "intake", "events.yaml")
	writeFixtureFile(t, eventsPath, strings.ReplaceAll(readFixtureFile(t, eventsPath), "  score: integer\n", "  score: integer?\n"))
	drifted, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	after, ok := drifted.DurableDataDeclarationByRef(ref)
	if !ok || before == after.SchemaDigest {
		t.Fatalf("compiled acceptance-schema mutation did not change resource schema identity: before=%s after=%#v", before, after)
	}
}

func TestDurableDataProjectionConsumesCompiledBusinessKey(t *testing.T) {
	entry := EventCatalogEntry{
		Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{
			"slug":  {Type: "text"},
			"score": {Type: "integer"},
		}, Required: []string{"slug", "score"}},
	}
	keyed, err := newCompiledEventSchema(".", "score.available", entry, TypeCatalogDocument{}, "slug", CompiledEventSchemaSource{FlowID: "intake", File: "events.yaml"})
	if err != nil {
		t.Fatalf("compile keyed event: %v", err)
	}
	keyless, err := newCompiledEventSchema(".", "score.available", entry, TypeCatalogDocument{}, "", CompiledEventSchemaSource{FlowID: "intake", File: "events.yaml"})
	if err != nil {
		t.Fatalf("compile keyless event: %v", err)
	}

	project := func(event CompiledEventSchema) DurableDataDeclaration {
		t.Helper()
		bundle := &WorkflowContractBundle{dataDeclarations: map[string]DurableDataDeclaration{}}
		if err := appendDurableDataEvent(bundle, event); err != nil {
			t.Fatalf("append compiled event: %v", err)
		}
		declarations := bundle.DurableDataDeclarations()
		if len(declarations) != 1 {
			t.Fatalf("declarations = %#v", declarations)
		}
		return declarations[0]
	}
	keyedDeclaration := project(keyed)
	keylessDeclaration := project(keyless)
	if keyedDeclaration.BusinessKey != "slug" {
		t.Fatalf("business key = %q, want slug", keyedDeclaration.BusinessKey)
	}
	if keyedDeclaration.SchemaDigest == keylessDeclaration.SchemaDigest {
		t.Fatalf("typed keyed and keyless interpretations aliased at %s", keyedDeclaration.SchemaDigest)
	}
}

func TestEventPatternsAreNotDatasetDeclarationIdentities(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := writePromptTestBundle(t, repo)
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), `
'*.completed': {}
task.completed:
  task_id: text
`)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load wildcard event fixture: %v", err)
	}
	for _, declaration := range bundle.DurableDataDeclarations() {
		if strings.Contains(declaration.Name, "*") {
			t.Fatalf("event pattern became dataset declaration: %#v", declaration)
		}
	}
	if bundle.DataProjectionRequired() {
		t.Fatal("authored events without agent access required a workspace projection")
	}
}

func TestDurableDataRejectsRetiredDataYAML(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := writePromptTestBundle(t, repo)
	writeFixtureFile(t, filepath.Join(root, "data.yaml"), "data: {}\n")
	if _, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo)); err == nil || !strings.Contains(err.Error(), "data.yaml is retired") {
		t.Fatalf("retired data.yaml error = %v", err)
	}
}

func durableDataDeclarationRef(packageKey, eventName string) (durabledata.DeclarationRef, error) {
	return durabledata.ParseDeclarationRef(packageKey, eventName)
}

func appendFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer file.Close()
	if _, err := file.WriteString(strings.TrimLeft(contents, "\n")); err != nil {
		t.Fatalf("append %s: %v", path, err)
	}
}

func readFixtureFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
