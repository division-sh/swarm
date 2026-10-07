package runtime

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/authoringview"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"gopkg.in/yaml.v3"
)

func TestA9AliasSystematicConsumption(t *testing.T) {
	for _, override := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "override"}[override], func(t *testing.T) {
			proveA9AliasSystematicConsumption(t, override)
		})
	}
}

func proveA9AliasSystematicConsumption(t *testing.T, override bool) {
	t.Helper()
	source, catalog := a9AliasSource(t, override, false)
	bundle, _ := semanticview.Bundle(source)
	composed, err := SourceWithProviderTriggerEvents(source, catalog)
	if err != nil {
		t.Fatal(err)
	}
	// Readers must consume the admitted fact even if a raw DTO is later changed.
	bundle.RootSchema.Name = "changed-root"
	bundle.RootSchema.Ingress.Alias = "changed-root-alias"
	bundle.FlowSchemas["beta"].Ingress.Alias = "changed-child-alias"
	declarations, err := ResolveStandingTargetDeclarations(composed, catalog)
	if err != nil {
		t.Fatal(err)
	}
	view, err := authoringview.Build(t.Context(), composed, authoringview.BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	credentials := a9AliasCredentials(t, true, true)
	rt := &Runtime{Options: RuntimeOptions{WorkflowModule: semanticOnlyWorkflowRuntime{source: composed}, ProviderTriggerCatalog: catalog,
		ProviderCredentials: credentials, SourceArtifactFact: testSourceArtifactFact(t, runtimeContextTestHashA)}}
	targets, err := rt.PlanStandingTargets()
	if err != nil || len(targets) != 2 {
		t.Fatalf("targets=%+v err=%v", targets, err)
	}
	declared, planned, projected, described, children := map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}
	for _, declaration := range declarations {
		alias, present := bundle.FlowIngressAlias(declaration.FlowPath)
		if !present || declaration.Alias != alias {
			t.Fatalf("runtime bypassed the compiled alias: %+v", declaration)
		}
		declared[declaration.FlowPath] = alias
		if declaration.FlowPath != "." {
			children[declaration.FlowPath] = alias
		}
	}
	for _, target := range targets {
		planned[target.FlowPath] = target.Alias
	}
	for _, item := range view.RoutingTopology.RootInputSources {
		projected[item.Target.FlowPath] = item.Alias
	}
	for _, item := range view.Flows {
		if item.Ingress != nil {
			described[item.ID] = item.Ingress.Alias
		}
	}
	if !reflect.DeepEqual(children, described) {
		t.Fatalf("child describe changed compiled aliases: %v != %v", described, children)
	}
	for name, aliases := range map[string]map[string]string{"targets": planned, "topology": projected} {
		if !reflect.DeepEqual(declared, aliases) {
			t.Fatalf("%s changed compiled aliases: %v != %v", name, aliases, declared)
		}
	}
}

func TestA9DormantAliasesRemainDistinct(t *testing.T) {
	source, catalog := a9AliasSource(t, true, true)
	rt := &Runtime{Options: RuntimeOptions{WorkflowModule: semanticOnlyWorkflowRuntime{source: source}, ProviderTriggerCatalog: catalog,
		ProviderCredentials: a9AliasCredentials(t, false, false), SourceArtifactFact: testSourceArtifactFact(t, runtimeContextTestHashA)}}
	targets, err := rt.PlanStandingTargets()
	if err != nil || len(targets) != 0 {
		t.Fatalf("dormant aliases granted execution: targets=%+v err=%v", targets, err)
	}
	declarations, err := ResolveStandingTargetDeclarations(source, catalog)
	if err != nil || len(declarations) != 2 || declarations[0].Alias != declarations[1].Alias {
		t.Fatalf("duplicate declared aliases disappeared: %+v err=%v", declarations, err)
	}
	subjects, err := baseStandingIngressCapabilitySubjects(source, catalog)
	if err != nil || len(subjects) != 2 || subjects[0].ID == subjects[1].ID || subjects[0].TriggerAdmission.FlowPath == subjects[1].TriggerAdmission.FlowPath {
		t.Fatalf("capability identity collapsed distinct declarations: %+v err=%v", subjects, err)
	}
	for _, tc := range []struct{ root, child bool }{{true, false}, {false, true}, {true, true}} {
		selected := &Runtime{Options: RuntimeOptions{WorkflowModule: semanticOnlyWorkflowRuntime{source: source}, ProviderTriggerCatalog: catalog,
			ProviderCredentials: a9AliasCredentials(t, tc.root, tc.child), SourceArtifactFact: testSourceArtifactFact(t, runtimeContextTestHashA)}}
		targets, err := selected.PlanStandingTargets()
		if tc.root && tc.child {
			if err == nil || len(targets) != 0 || selected.standingCredentialAdmission != nil {
				t.Fatalf("collision published admission: targets=%+v err=%v", targets, err)
			}
		} else if err != nil || len(targets) != 1 {
			t.Fatalf("dormant sibling blocked the enabled owner: targets=%+v err=%v", targets, err)
		}
	}
}

func TestA9IngressLookupUsesExactAlias(t *testing.T) {
	catalog := runtimeAdmissionTestCatalog(t, "a")
	contextDef := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "primary", catalog)
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatal(err)
	}
	if !manager.LookupIngress("primary", "acme").Loaded() {
		t.Fatal("exact admitted ingress is not discoverable")
	}
	for _, alias := range []string{" primary", "primary ", "/primary", "primary/"} {
		if strings.HasPrefix(alias, " ") || strings.HasSuffix(alias, " ") {
			if parsed, _, _ := parseWebhookPath("/webhooks/" + alias + "/acme"); parsed != alias {
				t.Fatalf("lower gateway parser normalized unadmitted alias %q into %q", alias, parsed)
			}
		}
		if lookup := manager.LookupIngress(alias, "acme"); lookup.Found || lookup.AliasFound {
			t.Fatalf("unadmitted alias %q selected an incumbent: %+v", alias, lookup)
		}
		use, lookup, err := manager.AcquireIngress(t.Context(), alias, "acme")
		if use != nil {
			_ = use.Done()
			t.Fatalf("unadmitted alias %q acquired execution", alias)
		}
		if err != nil || lookup.Found || lookup.AliasFound {
			t.Fatalf("unadmitted lookup returned authority: %+v err=%v", lookup, err)
		}
	}
}

func a9AliasSource(t *testing.T, override, collide bool) (semanticview.Source, *providertriggers.CatalogSnapshot) {
	t.Helper()
	root := canonicalrouting.CopyStandingRootTreePublic(t)
	for _, flow := range []string{".", "beta"} {
		path := filepath.Join(root, flow, "schema.yaml")
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var schema map[string]any
		if err := yaml.Unmarshal(body, &schema); err != nil {
			t.Fatal(err)
		}
		if flow == "." {
			schema["name"] = "shop"
		}
		ingress := schema["ingress"].(map[string]any)
		delete(ingress, "alias")
		if override {
			ingress["alias"] = "shared"
			if flow == "beta" && !collide {
				ingress["alias"] = "other"
			}
		}
		updated, err := yaml.Marshal(schema)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, updated, 0600); err != nil {
			t.Fatal(err)
		}
	}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := packadmission.FromBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle), projection.ProviderTriggers
}

func a9AliasCredentials(t *testing.T, root, child bool) *runtimecredentials.FileStore {
	t.Helper()
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	for key, present := range map[string]bool{"webhook_signing.alpha": root, "webhook_signing.beta": child} {
		if present {
			if err := store.Set(context.Background(), key, "signed-"+key); err != nil {
				t.Fatal(err)
			}
		}
	}
	return store
}
