package pinrouting

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templatefanin"
)

func TestConnectSourceEndpointMatchesEventUsesImmutableSourceAcrossTargetProjection(t *testing.T) {
	endpoint := ConnectRoutePlanEndpoint{
		FlowID:        "producer",
		FlowPath:      "producer",
		Event:         "deploy.done",
		ResolvedEvent: "producer/deploy.done",
	}
	source := events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1", EntityID: "producer-entity"}
	for _, tc := range []struct {
		name   string
		target events.RouteIdentity
	}{
		{name: "root receiver", target: events.RouteIdentity{EntityID: "root-entity"}},
		{name: "different template target", target: events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/inst-9", EntityID: "consumer-entity"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evt := eventtest.RootIngress("", "producer/inst-1/deploy.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{
				FlowInstance: tc.target.FlowInstance,
				Source:       source,
				Target:       tc.target,
			}, time.Unix(1, 0).UTC())
			if !ConnectSourceEndpointMatchesEvent(endpoint, evt) {
				t.Fatalf("source endpoint did not match immutable producer route; envelope = %#v", evt.NormalizedEnvelope())
			}
		})
	}
}

func TestConnectSourceEndpointMatchesEventRejectsTargetIdentityAsSource(t *testing.T) {
	endpoint := ConnectRoutePlanEndpoint{
		FlowID:        "consumer",
		FlowPath:      "consumer",
		Event:         "deploy.done",
		ResolvedEvent: "consumer/deploy.done",
	}
	target := events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/inst-9", EntityID: "consumer-entity"}
	evt := eventtest.RootIngress("", "deploy.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{
		FlowInstance: target.FlowInstance,
		Source:       events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1", EntityID: "producer-entity"},
		Target:       target,
	}, time.Unix(1, 0).UTC())
	if ConnectSourceEndpointMatchesEvent(endpoint, evt) {
		t.Fatalf("consumer target matched as producer source; envelope = %#v", evt.NormalizedEnvelope())
	}
}

func TestConnectSourceEndpointMatchesEventRejectsConcreteInstanceWithoutSourceRoute(t *testing.T) {
	endpoint := ConnectRoutePlanEndpoint{
		FlowID:        "producer",
		FlowPath:      "producer",
		Event:         "deploy.done",
		ResolvedEvent: "producer/deploy.done",
	}
	evt := eventtest.RootIngress("", "producer/inst-1/deploy.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	if ConnectSourceEndpointMatchesEvent(endpoint, evt) {
		t.Fatalf("concrete instance event matched without authoritative source route; envelope = %#v", evt.NormalizedEnvelope())
	}
}

func TestLowerTargetFreeInputRoutePlans_RejectsHarnessSource(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load harness injection artifact: %v", err)
	}
	plans, issues := LowerTargetFreeInputRoutePlans(semanticview.Wrap(bundle), []runtimeprovideroutput.Authorization{{
		Provider: "test", Event: "worker/work.requested", PackID: "provider.test", PackVersion: "1.0.0",
		ManifestHash: "sha256:test", GenerationID: "generation-test",
	}})
	if len(plans) != 0 || len(issues) != 0 {
		t.Fatalf("plans = %#v issues = %#v, want harness excluded without lowering issues", plans, issues)
	}
}

func TestLowerCompositionConnectRoutePlansFromLoadedPackageFixture(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeConnectRoutePlanPackageFixture(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := LowerCompositionConnectRoutePlans(semanticview.Wrap(bundle))
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if !strings.HasPrefix(plan.AuthoredLocation, filepath.Join(root, "package.yaml")+":") {
		t.Fatalf("AuthoredLocation = %q, want exact root package.yaml:line", plan.AuthoredLocation)
	}
	if got, want := plan.Source.ResolvedEvent, "producer/work.ready"; got != want {
		t.Fatalf("Source.ResolvedEvent = %q, want %q", got, want)
	}
	if got, want := plan.Receiver.ResolvedEvent, "consumer/work.ready"; got != want {
		t.Fatalf("Receiver.ResolvedEvent = %q, want %q", got, want)
	}
	if plan.Address == nil || plan.Address.By != "work_id" {
		t.Fatalf("Address = %#v, want loaded work_id address", plan.Address)
	}
	if got, want := plan.ResolutionKind, ConnectResolutionStatic; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
	}
	if plan.Target.FlowInstance != "consumer" {
		t.Fatalf("Target = %#v, want concrete static consumer route", plan.Target)
	}
}

func TestLowerCompositionConnectRoutePlanRejectsOtherwiseValidConnectWithoutSourceLocation(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeConnectRoutePlanPackageFixture(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := semanticview.Wrap(bundle)
	connects := source.CompositionConnects()
	if len(connects) != 1 {
		t.Fatalf("connects = %#v, want one", connects)
	}
	connects[0].SourceFile = ""
	connects[0].SourceLine = 0

	plan, issue := LowerCompositionConnectRoutePlan(source, connects[0])
	if issue.Failure != ConnectFailureSourceLocationMissing || issue.AuthoredLocation != "" || plan.AuthoredLocation != "" {
		t.Fatalf("plan = %#v issue = %#v, want source-location issue and no plan", plan, issue)
	}
}

func TestLowerCompositionConnectRoutePlansUsesFanInStreamSingularTarget(t *testing.T) {
	source := templatefanin.LoadSource(t, templatefanin.Options{})

	plans, issues := LowerCompositionConnectRoutePlans(source)

	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	plan := requireFanInRoutePlan(t, plans)
	if plan.FanIn == nil {
		t.Fatalf("fan-in metadata = nil in %#v", plan)
	}
	if plan.FanIn.Aggregation != "stream" || plan.FanIn.Window != "payload.period_id" || len(plan.FanIn.DedupBy) != 1 || plan.FanIn.DedupBy[0] != "payload.operating_id" {
		t.Fatalf("fan-in metadata = %#v, want stream/window/dedup", plan.FanIn)
	}
	if plan.TargetKind != ConnectTargetKindTarget || plan.ResolutionKind != ConnectResolutionStatic {
		t.Fatalf("fan-in routing shape = target_kind:%s resolution:%s, want target/static", plan.TargetKind, plan.ResolutionKind)
	}
	if plan.Target.FlowID != templatefanin.ReceiverFlowID || plan.Target.FlowInstance != templatefanin.ReceiverFlowInstance || plan.Target.EntityID != flowidentity.EntityID(templatefanin.ReceiverFlowInstance) {
		t.Fatalf("fan-in target = %#v, want receiver singleton %s with entity %s", plan.Target, templatefanin.ReceiverFlowInstance, flowidentity.EntityID(templatefanin.ReceiverFlowInstance))
	}
}

func TestLowerCompositionConnectRoutePlansAllowsFanInStreamEventIDDedup(t *testing.T) {
	source := templatefanin.LoadSource(t, templatefanin.Options{EventIDDedup: true})

	plans, issues := LowerCompositionConnectRoutePlans(source)

	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	plan := requireFanInRoutePlan(t, plans)
	if plan.FanIn == nil || len(plan.FanIn.DedupBy) != 1 || plan.FanIn.DedupBy[0] != "event.id" {
		t.Fatalf("fan-in metadata = %#v, want event.id dedup", plan.FanIn)
	}
}

func TestLowerCompositionConnectRoutePlansFailsClosedForInvalidFanInStream(t *testing.T) {
	tests := []struct {
		name    string
		opts    templatefanin.Options
		failure ConnectRoutePlanFailure
		detail  string
	}{
		{name: "missing dedup", opts: templatefanin.Options{MissingDedup: true}, failure: ConnectFailureInstanceResolutionInvalid, detail: "requires dedup_by"},
		{name: "dedup tuple", opts: templatefanin.Options{DedupTuple: true}, failure: ConnectFailureInstanceResolutionInvalid, detail: "exactly one dedup_by"},
		{name: "missing window", opts: templatefanin.Options{MissingWindow: true}, failure: ConnectFailureInstanceResolutionInvalid, detail: "requires window"},
		{name: "wrong singleton", opts: templatefanin.Options{WrongSingleton: true}, failure: ConnectFailureInstanceResolutionInvalid, detail: "must be the receiver singleton route or a child"},
		{name: "non-singleton receiver", opts: templatefanin.Options{NonSingletonReceiver: true}, failure: ConnectFailureInstanceResolutionInvalid, detail: "is not mode: singleton"},
		{name: "legacy map", opts: templatefanin.Options{LegacyConnectMap: true}, failure: ConnectFailureInstanceResolutionInvalid, detail: "connect.map is incompatible"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := templatefanin.LoadSource(t, tc.opts)

			_, issues := LowerCompositionConnectRoutePlans(source)

			if len(issues) != 1 {
				t.Fatalf("issues = %#v, want one", issues)
			}
			if issues[0].Failure != tc.failure || !strings.Contains(issues[0].Detail, tc.detail) {
				t.Fatalf("issue = %#v, want failure %s containing %q", issues[0], tc.failure, tc.detail)
			}
			if issues[0].AuthoredLocation == "" || !strings.Contains(issues[0].AuthoredLocation, "package.yaml:") {
				t.Fatalf("issue location = %q, want exact package.yaml:line", issues[0].AuthoredLocation)
			}
		})
	}
}

func TestLowerCompositionConnectRoutePlansUsesFanInBarrierSingularTarget(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, canonicalrouting.ExampleRoot(t, canonicalrouting.FanInBarrier), runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load canonical barrier: %v", err)
	}
	source := semanticview.Wrap(bundle)
	plans, issues := LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("LowerCompositionConnectRoutePlans issues = %#v, want none", issues)
	}
	plan := requireFanInRoutePlan(t, plans)
	if plan.FanIn.Aggregation != "barrier" || plan.FanIn.Window != "payload.period_id" || len(plan.FanIn.DedupBy) != 1 || plan.FanIn.DedupBy[0] != "payload.operating_id" {
		t.Fatalf("fan-in metadata = %#v, want barrier/window/member identity", plan.FanIn)
	}
	if plan.TargetKind != ConnectTargetKindTarget || plan.ResolutionKind != ConnectResolutionStatic {
		t.Fatalf("barrier routing shape = %#v, want singular static target", plan)
	}
}

func requireFanInRoutePlan(t *testing.T, plans []ConnectRoutePlan) ConnectRoutePlan {
	t.Helper()
	var matches []ConnectRoutePlan
	for _, plan := range plans {
		if plan.FanIn != nil {
			matches = append(matches, plan)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("fan-in route plans = %#v in all plans %#v, want exactly one", matches, plans)
	}
	return matches[0]
}

func TestLowerCompositionConnectRoutePlansUsesTemplateInstanceKey(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeInstanceKeyConnectRoutePlanPackageFixture(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := LowerCompositionConnectRoutePlans(semanticview.Wrap(bundle))
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if plan.Address != nil {
		t.Fatalf("Address = %#v, want nil for canonical instance-key plan", plan.Address)
	}
	if plan.InstanceKey == nil {
		t.Fatal("InstanceKey = nil, want canonical receiver instance key evidence")
	}
	if got, want := plan.ResolutionKind, ConnectResolutionInstanceKey; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
	}
	if got, want := plan.Source.Key, "vertical_id"; got != want {
		t.Fatalf("Source.Key = %q, want %q", got, want)
	}
	if len(plan.Source.Carries) != 1 || plan.Source.Carries[0] != "vertical_id" {
		t.Fatalf("Source.Carries = %#v, want [vertical_id]", plan.Source.Carries)
	}
	if len(plan.InstanceKey.Fields) != 1 || plan.InstanceKey.Fields[0] != "vertical_id" {
		t.Fatalf("InstanceKey.Fields = %#v, want [vertical_id]", plan.InstanceKey.Fields)
	}
	if got, want := plan.InstanceKey.OnMissing, "reject"; got != want {
		t.Fatalf("InstanceKey.OnMissing = %q, want %q", got, want)
	}
	if !plan.RequiresRuntimeResolution {
		t.Fatal("template instance-key receiver should require runtime resolution")
	}

	materialized := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{"vertical_id": "v-1"},
		Descriptors: []Descriptor{{
			EntityID:      "ent-1",
			FlowInstance:  "consumer/one",
			AddressFields: map[string]string{"entity.vertical_id": "v-1"},
		}},
	})
	if materialized.Failure != "" {
		t.Fatalf("Failure = %q, want empty", materialized.Failure)
	}
	if got, want := materialized.Target.FlowInstance, "consumer/one"; got != want {
		t.Fatalf("Target.FlowInstance = %q, want %q", got, want)
	}

	missing := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{},
	})
	if missing.Failure != ConnectFailureAddressValueMissing {
		t.Fatalf("missing Failure = %q, want %q", missing.Failure, ConnectFailureAddressValueMissing)
	}

	ambiguous := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{"payload.vertical_id": "v-1"},
		Descriptors: []Descriptor{
			{EntityID: "ent-1", FlowInstance: "consumer/one", AddressFields: map[string]string{"entity.vertical_id": "v-1"}},
			{EntityID: "ent-2", FlowInstance: "consumer/two", AddressFields: map[string]string{"entity.vertical_id": "v-1"}},
		},
	})
	if ambiguous.Failure != ConnectFailureTargetAmbiguous {
		t.Fatalf("ambiguous Failure = %q, want %q", ambiguous.Failure, ConnectFailureTargetAmbiguous)
	}
}

func TestLowerCompositionConnectRoutePlansUsesCreateInputResolution(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeCreateResolutionConnectRoutePlanPackageFixture(t, runtimecontracts.FlowInputResolutionMintUUID)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := LowerCompositionConnectRoutePlans(semanticview.Wrap(bundle))
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	var createPlans []ConnectRoutePlan
	for _, candidate := range plans {
		if candidate.InstanceKey != nil && candidate.InstanceKey.Mode == runtimecontracts.FlowInputResolutionModeCreate {
			createPlans = append(createPlans, candidate)
		}
	}
	if len(createPlans) != 1 {
		t.Fatalf("create plans = %#v in all plans %#v, want exactly one", createPlans, plans)
	}
	plan := createPlans[0]
	if plan.InstanceKey == nil {
		t.Fatal("InstanceKey = nil, want create resolution instance-key evidence")
	}
	if got, want := plan.ResolutionKind, ConnectResolutionInstanceKey; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
	}
	if plan.Source.Key != "" || len(plan.Source.Carries) != 0 {
		t.Fatalf("Source key/carries = %q/%#v, want create resolution independent of producer output key", plan.Source.Key, plan.Source.Carries)
	}
	if got, want := plan.InstanceKey.Mode, runtimecontracts.FlowInputResolutionModeCreate; got != want {
		t.Fatalf("InstanceKey.Mode = %q, want %q", got, want)
	}
	if got, want := plan.InstanceKey.Mint, runtimecontracts.FlowInputResolutionMintUUID; got != want {
		t.Fatalf("InstanceKey.Mint = %q, want %q", got, want)
	}
	if got, want := plan.InstanceKey.As, "validation_case_id"; got != want {
		t.Fatalf("InstanceKey.As = %q, want %q", got, want)
	}
	if got, want := plan.InstanceKey.OnMissing, "create"; got != want {
		t.Fatalf("InstanceKey.OnMissing = %q, want %q", got, want)
	}
	if got, want := plan.InstanceKey.OnConflict, "reuse"; got != want {
		t.Fatalf("InstanceKey.OnConflict = %q, want %q", got, want)
	}
	eventID := "11111111-1111-4111-8111-111111111111"
	material, failure := MintedInstanceKeyMaterialForConnectRoutePlan(plan, eventID)
	if failure != "" {
		t.Fatalf("MintedInstanceKeyMaterialForConnectRoutePlan failure = %q", failure)
	}
	if len(material.Keys) != 1 || material.Keys[0].Field != "validation_case_id" || material.Keys[0].Value == "" || material.Keys[0].Value == eventID {
		t.Fatalf("minted material = %#v, want deterministic uuid material distinct from event id", material)
	}
}

func TestLowerCompositionConnectRoutePlansUsesSelectInputResolution(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeSelectResolutionConnectRoutePlanPackageFixture(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := LowerCompositionConnectRoutePlans(semanticview.Wrap(bundle))
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if plan.InstanceKey == nil {
		t.Fatal("InstanceKey = nil, want select resolution instance-key evidence")
	}
	if got, want := plan.ResolutionKind, ConnectResolutionInstanceKey; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
	}
	if !plan.RequiresRuntimeResolution {
		t.Fatal("select resolution should require runtime descriptor resolution")
	}
	if got, want := plan.InstanceKey.Mode, runtimecontracts.FlowInputResolutionModeSelect; got != want {
		t.Fatalf("InstanceKey.Mode = %q, want %q", got, want)
	}
	if got, want := plan.InstanceKey.OnMissing, "reject"; got != want {
		t.Fatalf("InstanceKey.OnMissing = %q, want %q", got, want)
	}
	if got, want := plan.InstanceKey.OnConflict, "reject"; got != want {
		t.Fatalf("InstanceKey.OnConflict = %q, want %q", got, want)
	}
	if len(plan.InstanceKey.Fields) != 1 || plan.InstanceKey.Fields[0] != "account_id" {
		t.Fatalf("InstanceKey.Fields = %#v, want [account_id]", plan.InstanceKey.Fields)
	}
	if len(plan.InstanceKey.Mappings) != 1 || plan.InstanceKey.Mappings[0].Source != "account_id" || plan.InstanceKey.Mappings[0].Target != "account_id" || !plan.InstanceKey.Mappings[0].Explicit {
		t.Fatalf("InstanceKey.Mappings = %#v, want explicit account_id -> account_id", plan.InstanceKey.Mappings)
	}

	materialized := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{"payload.account_id": "acct-1"},
		Descriptors: []Descriptor{{
			EntityID:      "ent-1",
			FlowInstance:  "account/one",
			AddressFields: map[string]string{"entity.account_id": "acct-1"},
		}},
	})
	if materialized.Failure != "" {
		t.Fatalf("Failure = %q, want empty", materialized.Failure)
	}
	if got, want := materialized.Target.FlowInstance, "account/one"; got != want {
		t.Fatalf("Target.FlowInstance = %q, want %q", got, want)
	}

	missing := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{"payload.account_id": "acct-1"},
	})
	if missing.Failure != ConnectFailureTargetUnresolved {
		t.Fatalf("missing Failure = %q, want %q", missing.Failure, ConnectFailureTargetUnresolved)
	}

	ambiguous := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{"payload.account_id": "acct-1"},
		Descriptors: []Descriptor{
			{EntityID: "ent-1", FlowInstance: "account/one", AddressFields: map[string]string{"entity.account_id": "acct-1"}},
			{EntityID: "ent-2", FlowInstance: "account/two", AddressFields: map[string]string{"entity.account_id": "acct-1"}},
		},
	})
	if ambiguous.Failure != ConnectFailureTargetAmbiguous {
		t.Fatalf("ambiguous Failure = %q, want %q", ambiguous.Failure, ConnectFailureTargetAmbiguous)
	}
}

func TestLowerCompositionConnectRoutePlansUsesSelectOrCreateInputResolution(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeSelectResolutionConnectRoutePlanPackageFixtureWithOptions(t, selectResolutionConnectRoutePlanFixtureOptions{
		mode: runtimecontracts.FlowInputResolutionModeSelectOrCreate,
	})
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := LowerCompositionConnectRoutePlans(semanticview.Wrap(bundle))
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if plan.InstanceKey == nil {
		t.Fatal("InstanceKey = nil, want select-or-create resolution instance-key evidence")
	}
	if got, want := plan.ResolutionKind, ConnectResolutionInstanceKey; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
	}
	if !plan.RequiresRuntimeResolution {
		t.Fatal("select-or-create resolution should require runtime descriptor resolution")
	}
	if got, want := plan.InstanceKey.Mode, runtimecontracts.FlowInputResolutionModeSelectOrCreate; got != want {
		t.Fatalf("InstanceKey.Mode = %q, want %q", got, want)
	}
	if got, want := plan.InstanceKey.OnMissing, "create"; got != want {
		t.Fatalf("InstanceKey.OnMissing = %q, want %q", got, want)
	}
	if got, want := plan.InstanceKey.OnConflict, "reuse"; got != want {
		t.Fatalf("InstanceKey.OnConflict = %q, want %q", got, want)
	}
	if len(plan.InstanceKey.Fields) != 1 || plan.InstanceKey.Fields[0] != "account_id" {
		t.Fatalf("InstanceKey.Fields = %#v, want [account_id]", plan.InstanceKey.Fields)
	}
	if len(plan.InstanceKey.Mappings) != 1 || plan.InstanceKey.Mappings[0].Source != "account_id" || plan.InstanceKey.Mappings[0].Target != "account_id" || !plan.InstanceKey.Mappings[0].Explicit {
		t.Fatalf("InstanceKey.Mappings = %#v, want explicit account_id -> account_id", plan.InstanceKey.Mappings)
	}

	materialized := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{"payload.account_id": "acct-1"},
		Descriptors: []Descriptor{{
			EntityID:      "ent-1",
			FlowInstance:  "account/one",
			AddressFields: map[string]string{"entity.account_id": "acct-1"},
		}},
	})
	if materialized.Failure != "" {
		t.Fatalf("Failure = %q, want empty", materialized.Failure)
	}
	if got, want := materialized.Target.FlowInstance, "account/one"; got != want {
		t.Fatalf("Target.FlowInstance = %q, want %q", got, want)
	}
}

func TestLowerCompositionConnectRoutePlansRejectsExtraSelectResolutionFields(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeSelectResolutionConnectRoutePlanPackageFixtureWithExtraResolution(t, "          aggregation: stream\n")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := LowerCompositionConnectRoutePlans(semanticview.Wrap(bundle))
	if len(plans) != 0 {
		t.Fatalf("plans = %#v, want none for invalid select resolution", plans)
	}
	if len(issues) != 1 {
		t.Fatalf("issues = %#v, want one fail-closed issue", issues)
	}
	if issues[0].Failure != ConnectFailureInstanceResolutionInvalid || !strings.Contains(issues[0].Detail, "may only declare instance_key and carries") {
		t.Fatalf("issue = %#v, want instance resolution invalid for extra select field", issues[0])
	}
}

func TestLowerCompositionConnectRoutePlansRejectsSelectCarryTypeMismatch(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeSelectResolutionConnectRoutePlanPackageFixtureWithOptions(t, selectResolutionConnectRoutePlanFixtureOptions{
		accountIDEntityType: "integer",
		accountIDCarryType:  "string",
	})
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := LowerCompositionConnectRoutePlans(semanticview.Wrap(bundle))
	if len(plans) != 0 {
		t.Fatalf("plans = %#v, want none for invalid select resolution", plans)
	}
	if len(issues) != 1 {
		t.Fatalf("issues = %#v, want one fail-closed issue", issues)
	}
	if issues[0].Failure != ConnectFailureInstanceResolutionInvalid || !strings.Contains(issues[0].Detail, "key_types_incompatible") {
		t.Fatalf("issue = %#v, want instance resolution invalid for select carry type mismatch", issues[0])
	}
}

func TestLowerCompositionConnectRoutePlansRejectsSelectOrCreateCarryTypeMismatch(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeSelectResolutionConnectRoutePlanPackageFixtureWithOptions(t, selectResolutionConnectRoutePlanFixtureOptions{
		mode:                runtimecontracts.FlowInputResolutionModeSelectOrCreate,
		accountIDEntityType: "integer",
		accountIDCarryType:  "string",
	})
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := LowerCompositionConnectRoutePlans(semanticview.Wrap(bundle))
	if len(plans) != 0 {
		t.Fatalf("plans = %#v, want none for invalid select-or-create resolution", plans)
	}
	if len(issues) != 1 {
		t.Fatalf("issues = %#v, want one fail-closed issue", issues)
	}
	if issues[0].Failure != ConnectFailureInstanceResolutionInvalid || !strings.Contains(issues[0].Detail, "key_types_incompatible") {
		t.Fatalf("issue = %#v, want instance resolution invalid for select-or-create carry type mismatch", issues[0])
	}
}

func TestLowerCompositionConnectRoutePlansUsesRenamedInstanceKeyAdapter(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeInstanceKeyAdapterConnectRoutePlanPackageFixture(t, `
    using:
      instance:
        source: source_vertical_id
        target: vertical_id
`, "source_vertical_id", "[source_vertical_id]", "vertical_id")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := LowerCompositionConnectRoutePlans(semanticview.Wrap(bundle))
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if got, want := plan.ResolutionKind, ConnectResolutionInstanceKey; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
	}
	if plan.InstanceKey == nil {
		t.Fatal("InstanceKey = nil, want adapter-backed instance key")
	}
	if len(plan.InstanceKey.Fields) != 1 || plan.InstanceKey.Fields[0] != "vertical_id" {
		t.Fatalf("InstanceKey.Fields = %#v, want receiver target field vertical_id", plan.InstanceKey.Fields)
	}
	if len(plan.InstanceKey.Mappings) != 1 || plan.InstanceKey.Mappings[0].Source != "source_vertical_id" || plan.InstanceKey.Mappings[0].Target != "vertical_id" || !plan.InstanceKey.Mappings[0].Explicit {
		t.Fatalf("InstanceKey.Mappings = %#v, want source_vertical_id -> vertical_id", plan.InstanceKey.Mappings)
	}

	materialized := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{"payload.source_vertical_id": "v-1", "source_vertical_id": "wrong-alias"},
		Descriptors: []Descriptor{{
			EntityID:      "ent-1",
			FlowInstance:  "consumer/one",
			AddressFields: map[string]string{"entity.vertical_id": "v-1"},
		}},
	})
	if materialized.Failure != "" {
		t.Fatalf("Failure = %q, want empty", materialized.Failure)
	}
	if got, want := materialized.Target.FlowInstance, "consumer/one"; got != want {
		t.Fatalf("Target.FlowInstance = %q, want %q", got, want)
	}

	unqualifiedAlias := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{"source_vertical_id": "v-1"},
		Descriptors: []Descriptor{{
			EntityID:      "ent-1",
			FlowInstance:  "consumer/one",
			AddressFields: map[string]string{"entity.vertical_id": "v-1"},
		}},
	})
	if unqualifiedAlias.Failure != ConnectFailureAddressValueMissing {
		t.Fatalf("unqualified alias Failure = %q, want %q", unqualifiedAlias.Failure, ConnectFailureAddressValueMissing)
	}

	wrongCanonicalField := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{"vertical_id": "v-1"},
		Descriptors: []Descriptor{{
			EntityID:      "ent-1",
			FlowInstance:  "consumer/one",
			AddressFields: map[string]string{"entity.vertical_id": "v-1"},
		}},
	})
	if wrongCanonicalField.Failure != ConnectFailureAddressValueMissing {
		t.Fatalf("wrong canonical field Failure = %q, want %q", wrongCanonicalField.Failure, ConnectFailureAddressValueMissing)
	}
}

func TestLowerCompositionConnectRoutePlansFailsClosedForInvalidRenamedInstanceKeyAdapter(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeInstanceKeyAdapterConnectRoutePlanPackageFixture(t, `
    using:
      instance:
        source: source_vertical_id
        target: missing_vertical_id
`, "source_vertical_id", "[source_vertical_id]", "vertical_id")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := LowerCompositionConnectRoutePlans(semanticview.Wrap(bundle))
	if len(plans) != 0 {
		t.Fatalf("plans = %#v, want none for invalid adapter", plans)
	}
	if len(issues) != 1 || issues[0].Failure != ConnectFailureInstanceKeyAdapterInvalid {
		t.Fatalf("issues = %#v, want %q", issues, ConnectFailureInstanceKeyAdapterInvalid)
	}
}

func TestLowerCompositionConnectRoutePlansPreservesAddressedTemplateRoute(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeAddressedTemplateConnectRoutePlanPackageFixture(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := LowerCompositionConnectRoutePlans(semanticview.Wrap(bundle))
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if plan.Address == nil {
		t.Fatal("Address = nil, want addressed-input route evidence")
	}
	if plan.InstanceKey != nil {
		t.Fatalf("InstanceKey = %#v, want nil when receiver input declares address", plan.InstanceKey)
	}
	if got, want := plan.ResolutionKind, ConnectResolutionAddress; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
	}

	materialized := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{
			"customer_id": "cust-1",
			"vertical_id": "v-wrong",
		},
		Descriptors: []Descriptor{
			{
				EntityID:     "ent-address",
				FlowInstance: "account/addressed",
				AddressFields: map[string]string{
					"entity.customer_id": "cust-1",
					"entity.vertical_id": "v-other",
				},
			},
			{
				EntityID:     "ent-instance",
				FlowInstance: "account/instance-key",
				AddressFields: map[string]string{
					"entity.customer_id": "cust-other",
					"entity.vertical_id": "v-wrong",
				},
			},
		},
		SupportedAddressTargets: []string{"entity.customer_id"},
	})
	if materialized.Failure != "" {
		t.Fatalf("Failure = %q, want empty", materialized.Failure)
	}
	if got, want := materialized.Target.FlowInstance, "account/addressed"; got != want {
		t.Fatalf("Target.FlowInstance = %q, want %q", got, want)
	}
}

func TestLowerCompositionConnectRoutePlansOneToOneStatic(t *testing.T) {
	source := testConnectRoutePlanSource([]connectRoutePlanFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "deploy_done",
				Event: "deploy.done",
			}},
		},
		{
			id:   "consumer",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "deploy_completed",
				Event: "deploy.completed",
				Address: &runtimecontracts.FlowInputPinAddress{
					By:          "vertical_id",
					Source:      "payload.vertical_id",
					Target:      "entity.vertical_id",
					Cardinality: "one",
				},
			}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From:    "producer.deploy_done",
		To:      "consumer.deploy_completed",
		Adapter: "deploy_done_to_completed",
		Map: map[string]runtimecontracts.FlowPackageConnectMap{
			"vertical_id": {Source: "payload.vertical_id", Target: "entity.vertical_id"},
		},
	}})

	plans, issues := LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if got, want := plan.Source.FlowID, "producer"; got != want {
		t.Fatalf("Source.FlowID = %q, want %q", got, want)
	}
	if got, want := plan.Source.Pin, "deploy_done"; got != want {
		t.Fatalf("Source.Pin = %q, want %q", got, want)
	}
	if got, want := plan.Source.Event, "deploy.done"; got != want {
		t.Fatalf("Source.Event = %q, want %q", got, want)
	}
	if got, want := plan.Source.ResolvedEvent, "producer/deploy.done"; got != want {
		t.Fatalf("Source.ResolvedEvent = %q, want %q", got, want)
	}
	if got, want := plan.Receiver.Pin, "deploy_completed"; got != want {
		t.Fatalf("Receiver.Pin = %q, want %q", got, want)
	}
	if got, want := plan.Receiver.Event, "deploy.completed"; got != want {
		t.Fatalf("Receiver.Event = %q, want %q", got, want)
	}
	if got, want := plan.TargetKind, ConnectTargetKindTarget; got != want {
		t.Fatalf("TargetKind = %q, want %q", got, want)
	}
	if got, want := plan.ResolutionKind, ConnectResolutionStatic; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
	}
	if plan.Address == nil || plan.Address.By != "vertical_id" || plan.Address.Source != "payload.vertical_id" || plan.Address.Target != "entity.vertical_id" {
		t.Fatalf("Address = %#v, want vertical_id payload/entity mapping", plan.Address)
	}
	if len(plan.Map) != 1 || plan.Map[0].Key != "vertical_id" {
		t.Fatalf("Map = %#v, want vertical_id entry", plan.Map)
	}
	if plan.Target.FlowInstance != "consumer" {
		t.Fatalf("Target.FlowInstance = %q, want consumer", plan.Target.FlowInstance)
	}
	if plan.Target.EntityID != flowidentity.EntityID("consumer") {
		t.Fatalf("Target.EntityID = %q, want static route entity id", plan.Target.EntityID)
	}
	if plan.RequiresRuntimeResolution {
		t.Fatal("static connect should not require runtime descriptor resolution")
	}
}

func TestLowerCompositionConnectRoutePlansRootProducerToStaticReceiver(t *testing.T) {
	source := testRootConnectRoutePlanSource([]runtimecontracts.FlowOutputEventPin{{
		Name:  "root_ready",
		Event: "root.ready",
	}}, []connectRoutePlanFlow{
		{
			id:   "consumer",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "ready",
				Event: "root.ready",
			}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From: ".root_ready",
		To:   "consumer.ready",
	}})

	plans, issues := LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if !plan.Source.Root {
		t.Fatalf("Source.Root = false, want true: %#v", plan.Source)
	}
	if got, want := plan.Source.FlowID, ""; got != want {
		t.Fatalf("Source.FlowID = %q, want root empty flow id", got)
	}
	if got, want := plan.Source.Pin, "root_ready"; got != want {
		t.Fatalf("Source.Pin = %q, want %q", got, want)
	}
	if got, want := plan.Source.ResolvedEvent, "root.ready"; got != want {
		t.Fatalf("Source.ResolvedEvent = %q, want %q", got, want)
	}
	if got, want := plan.Receiver.FlowID, "consumer"; got != want {
		t.Fatalf("Receiver.FlowID = %q, want %q", got, want)
	}
	if plan.Target.FlowInstance != "consumer" {
		t.Fatalf("Target.FlowInstance = %q, want consumer", plan.Target.FlowInstance)
	}
}

func TestLowerCompositionConnectRoutePlansSupportsRootReceiverEndpoint(t *testing.T) {
	source := testRootReceiverConnectRoutePlanSource([]runtimecontracts.FlowInputEventPin{{
		Name:  "root_ready",
		Event: "root.ready",
	}}, []connectRoutePlanFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "ready",
				Event: "root.ready",
			}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From: "producer.ready",
		To:   ".root_ready",
	}})

	plans, issues := LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if !plan.Receiver.Root || plan.Receiver.FlowID != "" || plan.Receiver.Pin != "root_ready" {
		t.Fatalf("Receiver = %#v, want root input root_ready", plan.Receiver)
	}
	if plan.Target.FlowInstance != "" || plan.Target.EntityID != "" || plan.RequiresRuntimeResolution {
		t.Fatalf("root target = %#v (runtime=%t), want root-static target", plan.Target, plan.RequiresRuntimeResolution)
	}
}

func TestMaterializeConnectRoutePlanFanoutForTemplateDescriptors(t *testing.T) {
	source := testConnectRoutePlanSource([]connectRoutePlanFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "ticket_ready",
				Event: "ticket.ready",
			}},
		},
		{
			id:   "worker",
			mode: "template",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "ticket_ready",
				Event: "ticket.ready",
				Address: &runtimecontracts.FlowInputPinAddress{
					By:          "team_entity",
					Source:      "payload.team_entity",
					Target:      "_entity.id",
					Cardinality: "many",
				},
			}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From: "producer.ticket_ready",
		To:   "worker.ticket_ready",
	}})

	plans, issues := LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if !plan.RequiresRuntimeResolution {
		t.Fatal("template receiver should require runtime descriptor resolution")
	}
	if got, want := plan.TargetKind, ConnectTargetKindTargetSet; got != want {
		t.Fatalf("TargetKind = %q, want %q", got, want)
	}

	materialized := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{"team_entity": "team-a"},
		Descriptors: []Descriptor{
			{EntityID: "team-a", FlowInstance: "worker/alpha"},
			{EntityID: "team-a", FlowInstance: "worker/beta"},
			{EntityID: "team-a", FlowInstance: "other/alpha"},
			{EntityID: "team-b", FlowInstance: "worker/gamma"},
		},
	})
	if materialized.Failure != "" {
		t.Fatalf("Failure = %q, want empty", materialized.Failure)
	}
	if len(materialized.TargetSet) != 2 {
		t.Fatalf("TargetSet = %#v, want two team-a routes", materialized.TargetSet)
	}
	if materialized.TargetSet[0].FlowInstance != "worker/alpha" || materialized.TargetSet[1].FlowInstance != "worker/beta" {
		t.Fatalf("TargetSet = %#v, want deterministic worker alpha/beta routes", materialized.TargetSet)
	}
}

func TestLowerCompositionConnectRoutePlanDoesNotDependOnRawPinNamesOrProducerTargets(t *testing.T) {
	source := testConnectRoutePlanSource([]connectRoutePlanFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "public_done",
				Event: "internal.done",
			}},
		},
		{
			id:   "consumer",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "accept_completed",
				Event: "external.completed",
			}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From:    "producer.public_done",
		To:      "consumer.accept_completed",
		Adapter: "public_done_to_accept_completed",
	}})

	plan, issue := LowerCompositionConnectRoutePlan(source, source.CompositionConnects()[0])
	if issue.Failure != "" {
		t.Fatalf("issue = %#v, want none", issue)
	}
	if got, want := plan.Source.Pin, "public_done"; got != want {
		t.Fatalf("Source.Pin = %q, want %q", got, want)
	}
	if got, want := plan.Source.Event, "internal.done"; got != want {
		t.Fatalf("Source.Event = %q, want %q", got, want)
	}
	if got, want := plan.Receiver.Pin, "accept_completed"; got != want {
		t.Fatalf("Receiver.Pin = %q, want %q", got, want)
	}
	if got, want := plan.Receiver.Event, "external.completed"; got != want {
		t.Fatalf("Receiver.Event = %q, want %q", got, want)
	}
	if got, want := plan.Adapter, "public_done_to_accept_completed"; got != want {
		t.Fatalf("Adapter = %q, want %q", got, want)
	}
}

func TestLowerCompositionConnectRoutePlanFailsClosedForInvalidInputs(t *testing.T) {
	tests := []struct {
		name    string
		connect runtimecontracts.FlowPackageConnect
		want    ConnectRoutePlanFailure
	}{
		{
			name:    "missing output pin",
			connect: runtimecontracts.FlowPackageConnect{From: "producer.missing", To: "consumer.deploy_completed"},
			want:    ConnectFailureProducerOutputPinMissing,
		},
	}
	source := testConnectRoutePlanSource([]connectRoutePlanFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "deploy_done",
				Event: "deploy.done",
			}},
		},
		{
			id:   "consumer",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "deploy_completed",
				Event: "deploy.completed",
			}},
		},
	}, nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, issue := LowerCompositionConnectRoutePlan(source, tc.connect)
			if issue.Failure != tc.want {
				t.Fatalf("Failure = %q, want %q (issue %#v)", issue.Failure, tc.want, issue)
			}
		})
	}
}

func TestMaterializeConnectRoutePlanFailsClosedForUnsupportedAddressTarget(t *testing.T) {
	source := testConnectRoutePlanSource([]connectRoutePlanFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "deploy_done",
				Event: "deploy.done",
			}},
		},
		{
			id:   "consumer",
			mode: "template",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "deploy_completed",
				Event: "deploy.completed",
				Address: &runtimecontracts.FlowInputPinAddress{
					By:          "vertical_id",
					Source:      "payload.vertical_id",
					Target:      "entity.vertical_id",
					Cardinality: "one",
				},
			}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From: "producer.deploy_done",
		To:   "consumer.deploy_completed",
	}})

	plans, issues := LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	materialized := MaterializeConnectRoutePlan(plans[0], ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{"vertical_id": "v1"},
		Descriptors: []Descriptor{{
			EntityID:     "entity-1",
			FlowInstance: "consumer/inst-1",
		}},
	})
	if materialized.Failure != ConnectFailureTargetUnsupported {
		t.Fatalf("Failure = %q, want %q", materialized.Failure, ConnectFailureTargetUnsupported)
	}
}

type connectRoutePlanFlow struct {
	id      string
	mode    string
	inputs  []runtimecontracts.FlowInputEventPin
	outputs []runtimecontracts.FlowOutputEventPin
}

func testConnectRoutePlanSource(flows []connectRoutePlanFlow, connects []runtimecontracts.FlowPackageConnect) semanticview.Source {
	return testRootConnectRoutePlanSource(nil, flows, connects)
}

func testRootConnectRoutePlanSource(rootOutputs []runtimecontracts.FlowOutputEventPin, flows []connectRoutePlanFlow, connects []runtimecontracts.FlowPackageConnect) semanticview.Source {
	return testRootInputOutputConnectRoutePlanSource(nil, rootOutputs, flows, connects)
}

func testRootReceiverConnectRoutePlanSource(rootInputs []runtimecontracts.FlowInputEventPin, flows []connectRoutePlanFlow, connects []runtimecontracts.FlowPackageConnect) semanticview.Source {
	return testRootInputOutputConnectRoutePlanSource(rootInputs, nil, flows, connects)
}

func testRootInputOutputConnectRoutePlanSource(rootInputs []runtimecontracts.FlowInputEventPin, rootOutputs []runtimecontracts.FlowOutputEventPin, flows []connectRoutePlanFlow, connects []runtimecontracts.FlowPackageConnect) semanticview.Source {
	connects = append([]runtimecontracts.FlowPackageConnect(nil), connects...)
	for i := range connects {
		connects[i].SourceFile = "package.yaml"
		connects[i].SourceLine = i + 1
	}
	children := make([]runtimecontracts.FlowContractView, 0, len(flows))
	byID := make(map[string]*runtimecontracts.FlowContractView, len(flows))
	inputPins := make(map[string][]runtimecontracts.FlowInputEventPin, len(flows))
	outputPins := make(map[string][]runtimecontracts.FlowOutputEventPin, len(flows))
	flowInputs := make(map[string][]string, len(flows))
	flowOutputs := make(map[string][]string, len(flows))
	flowSchemas := make(map[string]runtimecontracts.FlowSchemaDocument, len(flows))
	for _, flow := range flows {
		view := runtimecontracts.FlowContractView{
			Paths: runtimecontracts.FlowContractPaths{
				ID:   flow.id,
				Flow: flow.id,
			},
			Schema: runtimecontracts.FlowSchemaDocument{
				Mode: flow.mode,
				Pins: runtimecontracts.FlowPins{
					Inputs: runtimecontracts.FlowInputPins{
						Events:    inputEventNames(flow.inputs),
						EventPins: flow.inputs,
					},
					Outputs: runtimecontracts.FlowOutputPins{
						Events:    outputEventNames(flow.outputs),
						EventPins: flow.outputs,
					},
				},
			},
			Path: flow.id,
		}
		children = append(children, view)
		viewCopy := view
		byID[flow.id] = &viewCopy
		inputPins[flow.id] = append([]runtimecontracts.FlowInputEventPin{}, flow.inputs...)
		outputPins[flow.id] = append([]runtimecontracts.FlowOutputEventPin{}, flow.outputs...)
		flowInputs[flow.id] = inputEventNames(flow.inputs)
		flowOutputs[flow.id] = outputEventNames(flow.outputs)
		flowSchemas[flow.id] = view.Schema
	}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{
					Events:    inputEventNames(rootInputs),
					EventPins: rootInputs,
				},
				Outputs: runtimecontracts.FlowOutputPins{
					Events:    outputEventNames(rootOutputs),
					EventPins: rootOutputs,
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"root.ready": {},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			FlowInputs:          flowInputs,
			FlowOutputs:         flowOutputs,
			FlowInputEventPins:  inputPins,
			FlowOutputEventPins: outputPins,
			CompositionConnects: connects,
		},
		FlowSchemas: flowSchemas,
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &runtimecontracts.FlowContractView{
				Children: children,
			},
			ByID: byID,
		},
	})
}

func inputEventNames(pins []runtimecontracts.FlowInputEventPin) []string {
	out := make([]string, 0, len(pins))
	for _, pin := range pins {
		out = append(out, pin.EventType())
	}
	return out
}

func outputEventNames(pins []runtimecontracts.FlowOutputEventPin) []string {
	out := make([]string, 0, len(pins))
	for _, pin := range pins {
		out = append(out, pin.EventType())
	}
	return out
}

func writeConnectRoutePlanPackageFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyParentConnectAddressVariant(t, canonicalrouting.ParentConnectAddressLowering)
}

func writeInstanceKeyConnectRoutePlanPackageFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyLegacyInstanceRoute(t, canonicalrouting.LegacyInstanceRouteOptions{
		Missing:  canonicalrouting.LegacyInstancePolicyReject,
		Conflict: canonicalrouting.LegacyInstancePolicyReject,
	})
}

func writeCreateResolutionConnectRoutePlanPackageFixture(t *testing.T, mint string) string {
	t.Helper()
	mode := canonicalrouting.CreateMintUUID
	if strings.TrimSpace(mint) == runtimecontracts.FlowInputResolutionMintEventID {
		mode = canonicalrouting.CreateMintEventID
	}
	return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: mode})
}

func writeSelectResolutionConnectRoutePlanPackageFixture(t *testing.T) string {
	return writeSelectResolutionConnectRoutePlanPackageFixtureWithOptions(t, selectResolutionConnectRoutePlanFixtureOptions{})
}

func writeSelectResolutionConnectRoutePlanPackageFixtureWithExtraResolution(t *testing.T, extraResolution string) string {
	return writeSelectResolutionConnectRoutePlanPackageFixtureWithOptions(t, selectResolutionConnectRoutePlanFixtureOptions{extraResolution: extraResolution})
}

type selectResolutionConnectRoutePlanFixtureOptions struct {
	mode                string
	extraResolution     string
	accountIDEntityType string
	accountIDCarryType  string
}

func writeSelectResolutionConnectRoutePlanPackageFixtureWithOptions(t *testing.T, options selectResolutionConnectRoutePlanFixtureOptions) string {
	t.Helper()
	mode := canonicalrouting.SelectResolutionSelect
	if strings.TrimSpace(options.mode) == runtimecontracts.FlowInputResolutionModeSelectOrCreate {
		mode = canonicalrouting.SelectResolutionSelectOrCreate
	}
	invalidity := canonicalrouting.SelectResolutionValid
	switch {
	case strings.TrimSpace(options.extraResolution) != "":
		invalidity = canonicalrouting.SelectResolutionExtraAggregation
	case strings.TrimSpace(options.accountIDEntityType) == "integer":
		invalidity = canonicalrouting.SelectResolutionEntityTypeMismatch
	case strings.TrimSpace(options.accountIDCarryType) == "integer":
		invalidity = canonicalrouting.SelectResolutionCarryTypeMismatch
	}
	return canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{Mode: mode, Invalidity: invalidity})
}

func writeInstanceKeyAdapterConnectRoutePlanPackageFixture(t *testing.T, usingBlock, outputKey, outputCarries, instanceBy string) string {
	t.Helper()
	if outputKey != "source_vertical_id" || outputCarries != "[source_vertical_id]" || instanceBy != "vertical_id" {
		t.Fatalf("unsupported legacy instance adapter shape")
	}
	adapter := canonicalrouting.LegacyInstanceAdapterRenamed
	if strings.Contains(usingBlock, "target: missing_vertical_id") {
		adapter = canonicalrouting.LegacyInstanceAdapterInvalidTarget
	} else if !strings.Contains(usingBlock, "target: vertical_id") {
		t.Fatalf("unsupported legacy instance adapter %q", usingBlock)
	}
	return canonicalrouting.CopyLegacyInstanceRoute(t, canonicalrouting.LegacyInstanceRouteOptions{
		Missing:  canonicalrouting.LegacyInstancePolicyReject,
		Conflict: canonicalrouting.LegacyInstancePolicyReject,
		Adapter:  adapter,
	})
}

func writeAddressedTemplateConnectRoutePlanPackageFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyLegacyAddressedTemplateSelect(t)
}

func writeConnectRoutePlanFlowFixture(t *testing.T, root, flowID, schemaTail, events, entities string) {
	t.Helper()
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), `
name: `+flowID+`
mode: static
`+schemaTail)
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", flowID, "agents.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", flowID, "nodes.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), events)
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", flowID, "entities.yaml"), entities)
}

func writeConnectRoutePlanFixtureFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}
