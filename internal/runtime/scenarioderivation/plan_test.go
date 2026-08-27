package scenarioderivation

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCompileRequiresExactInputWhenAmbiguousAndBindsEmptyProfile(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	root := filepath.Join(repoRoot, "internal", "runtime", "testdata", "generic-swarm-bundle")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatal(err)
	}
	hash, _ := runtimecontracts.BundleHash(bundle)
	fact, _ := runtimecorrelation.NewEphemeralBundleSourceFact(hash)
	identity, _ := scenarioexecution.NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("a", 64))
	source := semanticview.Wrap(bundle)
	inputs := semanticview.BuildAuthoredEventEndpointCensus(source).InputPins()
	if len(inputs) == 0 {
		t.Fatal("fixture has no public inputs")
	}
	flowID := inputs[0].FlowID
	plans, err := Compile(source, identity, Request{FlowID: flowID, Input: inputs[0].PinName})
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 1 || len(plans[0].Payload) == 0 || len(plans[0].Profile.ConnectorResponses()) != 0 {
		t.Fatalf("derived plans = %#v", plans)
	}
	selector, err := scenarioexecution.NewSelector(plans[0].Profile)
	if err != nil {
		t.Fatal(err)
	}
	if selector.EffectiveSourceDigest != identity.Digest() {
		t.Fatalf("selector effective digest = %q", selector.EffectiveSourceDigest)
	}
}

func TestCompileInputSelectionFailsClosedAndAllInputsIsDeterministic(t *testing.T) {
	source, identity := derivationHostileTestSource(t)
	if _, err := Compile(source, identity, Request{FlowID: "work"}); err == nil || !strings.Contains(err.Error(), "multiple public inputs") || !strings.Contains(err.Error(), "--input") {
		t.Fatalf("ambiguous input error = %v", err)
	}
	if _, err := Compile(source, identity, Request{FlowID: "missing"}); err == nil || !strings.Contains(err.Error(), "no public input") {
		t.Fatalf("missing flow error = %v", err)
	}
	if _, err := Compile(source, identity, Request{FlowID: "work", Input: "missing"}); err == nil || !strings.Contains(err.Error(), "no public input") {
		t.Fatalf("missing input error = %v", err)
	}
	plans, err := Compile(source, identity, Request{FlowID: "work", AllInputs: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 2 || plans[0].PinName != "work.requested" || plans[1].PinName != "work.alternate" {
		t.Fatalf("all-input plans = %#v", plans)
	}
}

func TestCompileGeneratedBaseOverlayUsesCanonicalValidation(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.ArtifactID("internal/runtime/scenarioderivation/testdata/hostile"))
	source, identity := derivationHostileTestSource(t)
	plans, err := Compile(source, identity, Request{
		FlowID: "work", Input: "work.requested",
		Set: map[string]any{
			"mode": "fast", "details": map[string]any{"count": 7}, "tags": []any{"alpha", "beta"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(plans[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["mode"] != "fast" || !reflect.DeepEqual(payload["details"], map[string]any{"count": float64(7)}) || !reflect.DeepEqual(payload["tags"], []any{"alpha", "beta"}) {
		t.Fatalf("overlaid payload = %#v", payload)
	}

	for _, tc := range []struct {
		name string
		set  map[string]any
		want string
	}{
		{name: "unknown field", set: map[string]any{"unknown": true}, want: "unknown"},
		{name: "nested type mismatch", set: map[string]any{"details": map[string]any{"count": "seven"}}, want: "count"},
		{name: "out of enum", set: map[string]any{"mode": "turbo"}, want: "enum"},
		{name: "required field corruption", set: map[string]any{"mode": nil}, want: "mode"},
		{name: "array item mismatch", set: map[string]any{"tags": []any{1}}, want: "tags"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile(source, identity, Request{FlowID: "work", Input: "work.requested", Set: tc.set})
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.want)) {
				t.Fatalf("overlay error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestCompileCatalogRejectsDuplicateExactFlowInputCoordinates(t *testing.T) {
	source, identity := derivationHostileTestSource(t)
	declarations := []Declaration{
		{Name: "first", FlowID: "work", Input: "work.requested"},
		{Name: "second", FlowID: "work", Input: "work.requested"},
	}
	if _, err := CompileCatalog(source, identity, declarations...); err == nil || !strings.Contains(err.Error(), "same exact flow/input coordinate") {
		t.Fatalf("duplicate catalog coordinate error = %v", err)
	}
}

func derivationHostileTestSource(t *testing.T) (semanticview.Source, scenarioexecution.EffectiveSourceIdentity) {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	root := filepath.Join(repoRoot, "internal", "runtime", "scenarioderivation", "testdata", "hostile")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatal(err)
	}
	hash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatal(err)
	}
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(hash)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := scenarioexecution.NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("e", 64))
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle), identity
}
