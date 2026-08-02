package pinrouting

import (
	"os"
	"path/filepath"
	"reflect"
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
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

func TestConnectSourceEndpointMatchesEventUsesImmutableSourceAcrossTargetProjection(t *testing.T) {
	endpoint := newConnectRoutePlanEndpoint(false, "producer", "producer", runtimecontracts.FlowModeTemplate, "", "deploy.done", "producer/deploy.done", "", nil)
	source := events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1", EntityID: "producer-entity"}
	for _, tc := range []struct {
		name   string
		target events.RouteIdentity
	}{
		{name: "root receiver", target: events.RouteIdentity{EntityID: "root-entity"}},
		{name: "different template target", target: events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/inst-9", EntityID: "consumer-entity"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, source)
			evt := eventtest.RunCreatingRootIngress("", "producer/inst-1/deploy.done", "", "", []byte(`{}`), 0, "", "", envelope, time.Unix(1, 0).UTC())
			evt = eventtest.TargetRouted(evt, tc.target)
			if !connectSourceEndpointMatchesEvent(endpoint, evt) {
				t.Fatalf("source endpoint did not match immutable producer route; envelope = %#v", evt.NormalizedEnvelope())
			}
		})
	}
}

func TestConnectSourceEndpointMatchesEventRejectsTargetIdentityAsSource(t *testing.T) {
	endpoint := newConnectRoutePlanEndpoint(false, "consumer", "consumer", runtimecontracts.FlowModeStatic, "", "deploy.done", "consumer/deploy.done", "", nil)
	target := events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/inst-9", EntityID: "consumer-entity"}
	envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1", EntityID: "producer-entity"})
	evt := eventtest.RunCreatingRootIngress("", "deploy.done", "", "", []byte(`{}`), 0, "", "", envelope, time.Unix(1, 0).UTC())
	evt = eventtest.TargetRouted(evt, target)
	if connectSourceEndpointMatchesEvent(endpoint, evt) {
		t.Fatalf("consumer target matched as producer source; envelope = %#v", evt.NormalizedEnvelope())
	}
}

func TestConnectSourceEndpointMatchesEventRejectsConcreteInstanceWithoutSourceRoute(t *testing.T) {
	endpoint := newConnectRoutePlanEndpoint(false, "producer", "producer", runtimecontracts.FlowModeTemplate, "", "deploy.done", "producer/deploy.done", "", nil)
	evt := eventtest.RunCreatingRootIngress("", "producer/inst-1/deploy.done", "", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Unix(1, 0).UTC())
	if connectSourceEndpointMatchesEvent(endpoint, evt) {
		t.Fatalf("concrete instance event matched without authoritative source route; envelope = %#v", evt.NormalizedEnvelope())
	}
}

func TestConnectSourceEndpointMatchesEventEnforcesRootSourceContextMatrix(t *testing.T) {
	rootEndpoint := newConnectRoutePlanEndpoint(true, "", "", "root", "", "root.ready", "root.ready", "", nil)
	staticEndpoint := newConnectRoutePlanEndpoint(false, "producer", "producer", runtimecontracts.FlowModeStatic, "", "deploy.done", "producer/deploy.done", "", nil)
	templateEndpoint := newConnectRoutePlanEndpoint(false, "producer", "producer", runtimecontracts.FlowModeTemplate, "", "deploy.done", "producer/deploy.done", "", nil)

	tests := []struct {
		name      string
		endpoint  ConnectRoutePlanEndpoint
		eventType events.EventType
		source    events.RoutingSource
		want      bool
	}{
		{name: "typed root", endpoint: rootEndpoint, eventType: "root.ready", source: mustRootRoutingSource(t), want: true},
		{name: "absent source", endpoint: rootEndpoint, eventType: "root.ready", source: events.NoRoutingSource()},
		{name: "platform control", endpoint: rootEndpoint, eventType: "root.ready", source: events.NewPlatformControlRoutingSource()},
		{name: "child source cannot become root", endpoint: rootEndpoint, eventType: "root.ready", source: mustConcreteRoutingSource(t, "child", "child/inst-1")},
		{name: "static source control", endpoint: staticEndpoint, eventType: "producer/deploy.done", source: mustStaticRoutingSource(t, "producer"), want: true},
		{name: "template source control", endpoint: templateEndpoint, eventType: "producer/inst-1/deploy.done", source: mustConcreteRoutingSource(t, "producer", "producer/inst-1"), want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := connectSourceEndpointMatches(tc.endpoint, tc.eventType, tc.source); got != tc.want {
				t.Fatalf("connectSourceEndpointMatches = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestConnectSourceEndpointMatchesEnforcesProducerModeMatrix(t *testing.T) {
	flowEndpoint := newConnectRoutePlanEndpoint(false, "producer", "producer", runtimecontracts.FlowModeStatic, "", "deploy.done", "producer/deploy.done", "", nil)
	rootEndpoint := newConnectRoutePlanEndpoint(true, "", "", "root", "", "deploy.done", "deploy.done", "", nil)
	tests := []struct {
		name      string
		endpoint  ConnectRoutePlanEndpoint
		eventType events.EventType
		source    events.RoutingSource
		want      bool
	}{
		{name: "root exact", endpoint: rootEndpoint, eventType: "deploy.done", source: mustRootRoutingSource(t), want: true},
		{name: "root rejects child evidence", endpoint: rootEndpoint, eventType: "deploy.done", source: mustConcreteRoutingSource(t, "producer", "producer/inst-1")},
		{name: "static requires source", endpoint: flowEndpoint, eventType: "producer/deploy.done", source: events.NoRoutingSource()},
		{name: "static exact instance", endpoint: flowEndpoint, eventType: "producer/deploy.done", source: mustStaticRoutingSource(t, "producer"), want: true},
		{name: "static rejects descendant instance", endpoint: flowEndpoint, eventType: "producer/inst-1/deploy.done", source: mustConcreteRoutingSource(t, "producer", "producer/inst-1")},
		{name: "singleton exact instance", endpoint: withConnectSourceMode(flowEndpoint, "singleton"), eventType: "producer/deploy.done", source: mustStaticRoutingSource(t, "producer"), want: true},
		{name: "singleton rejects descendant instance", endpoint: withConnectSourceMode(flowEndpoint, "singleton"), eventType: "producer/inst-1/deploy.done", source: mustConcreteRoutingSource(t, "producer", "producer/inst-1")},
		{name: "template concrete instance", endpoint: withConnectSourceMode(flowEndpoint, "template"), eventType: "producer/inst-1/deploy.done", source: mustConcreteRoutingSource(t, "producer", "producer/inst-1"), want: true},
		{name: "template rejects base without route", endpoint: withConnectSourceMode(flowEndpoint, "template"), eventType: "producer/deploy.done", source: events.NoRoutingSource()},
		{name: "template rejects static source", endpoint: withConnectSourceMode(flowEndpoint, "template"), eventType: "producer/deploy.done", source: mustStaticRoutingSource(t, "producer")},
		{name: "template rejects base name with concrete route", endpoint: withConnectSourceMode(flowEndpoint, "template"), eventType: "producer/deploy.done", source: mustConcreteRoutingSource(t, "producer", "producer/inst-1")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := connectSourceEndpointMatches(tc.endpoint, tc.eventType, tc.source); got != tc.want {
				t.Fatalf("connectSourceEndpointMatches(%#v, %q) = %v, want %v", tc.endpoint, tc.eventType, got, tc.want)
			}
		})
	}
}

func withConnectSourceMode(endpoint ConnectRoutePlanEndpoint, mode string) ConnectRoutePlanEndpoint {
	if strings.TrimSpace(mode) == runtimecontracts.FlowModeTemplate {
		endpoint.kind = connectEndpointTemplateFlow
	} else {
		endpoint.kind = connectEndpointStaticFlow
	}
	return endpoint
}

func TestConnectSourceEndpointMatchesRejectsStaticEventWhenSourceRouteContradicts(t *testing.T) {
	endpoint := newConnectRoutePlanEndpoint(false, "producer", "producer", runtimecontracts.FlowModeStatic, "", "deploy.done", "producer/deploy.done", "", nil)
	if connectSourceEndpointMatches(endpoint, "producer/deploy.done", mustConcreteRoutingSource(t, "unrelated", "unrelated/inst-1")) {
		t.Fatal("static producer event matched contradictory source route")
	}
}

func TestConnectSourceEndpointMatchesRejectsStaticEventWithoutSourceRoute(t *testing.T) {
	endpoint := newConnectRoutePlanEndpoint(false, "producer", "producer", runtimecontracts.FlowModeStatic, "", "deploy.done", "producer/deploy.done", "", nil)
	if connectSourceEndpointMatches(endpoint, "producer/deploy.done", events.NoRoutingSource()) {
		t.Fatal("fully scoped static producer event matched without typed source authority")
	}
}

func TestConnectSourceEndpointMatchesRejectsRootEventWithChildFlowEvidence(t *testing.T) {
	endpoint := newConnectRoutePlanEndpoint(true, "", "", "root", "", "deploy.done", "deploy.done", "", nil)
	if connectSourceEndpointMatches(endpoint, "deploy.done", mustStaticRoutingSource(t, "child")) {
		t.Fatal("root endpoint matched child/static FlowID evidence")
	}
}

func mustRootRoutingSource(t *testing.T) events.RoutingSource {
	t.Helper()
	source, err := events.NewRootRoutingSource("root-entity")
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func mustStaticRoutingSource(t *testing.T, flowID string) events.RoutingSource {
	t.Helper()
	source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: flowID, FlowInstance: flowID, EntityID: flowID + "-entity"})
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func mustConcreteRoutingSource(t *testing.T, flowID, flowInstance string) events.RoutingSource {
	t.Helper()
	source, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{FlowID: flowID, FlowInstance: flowInstance, EntityID: flowID + "-entity"})
	if err != nil {
		t.Fatal(err)
	}
	return source
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
	authorization := runtimeprovideroutput.MustAuthorization(
		"test", "worker/work.requested", "provider.test", "1.0.0",
		"sha256:"+strings.Repeat("a", 64),
		triggergeneration.FromCanonicalBytes([]byte("generation-test")),
	)
	plans, issues := lowerTargetFreeInputRoutePlans(semanticview.Wrap(bundle), []runtimeprovideroutput.Authorization{authorization})
	if len(plans) != 0 || len(issues) != 0 {
		t.Fatalf("plans = %#v issues = %#v, want harness excluded without lowering issues", plans, issues)
	}
}

func TestLowerTargetFreeInputRoutePlansUsesCanonicalRenamedIdentitySource(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyProviderRollbackRenamedSource(t, true)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load provider rollback artifact: %v", err)
	}
	authorization := runtimeprovideroutput.MustAuthorization(
		"telegram", "inbound.telegram.text_message", "provider.telegram", "1.0.0",
		"sha256:"+strings.Repeat("a", 64),
		triggergeneration.FromCanonicalBytes([]byte("target-free-renamed-source")),
	)

	plans, issues := lowerTargetFreeInputRoutePlans(semanticview.Wrap(bundle), []runtimeprovideroutput.Authorization{authorization})
	if len(issues) != 0 || len(plans) != 1 || plans[0].InstanceKey == nil {
		t.Fatalf("plans/issues = %#v/%#v, want one target-free instance plan", plans, issues)
	}
	plan := plans[0]
	if got, want := plan.InstanceKey.Source.Path, "payload.external_chat_id"; got != want {
		t.Fatalf("typed source = %q, want %q", got, want)
	}
	materialized := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{
			"payload.external_chat_id": "chat-authoritative",
			"payload.chat_id":          "chat-conflicting",
		},
		Descriptors: []Descriptor{
			{EntityID: "ent-authoritative", FlowInstance: "consumer/authoritative", AddressFields: map[string]string{"entity.chat_id": "chat-authoritative"}},
			{EntityID: "ent-conflicting", FlowInstance: "consumer/conflicting", AddressFields: map[string]string{"entity.chat_id": "chat-conflicting"}},
		},
	})
	if materialized.Failure != "" || materialized.Target.FlowInstance != "consumer/authoritative" {
		t.Fatalf("materialized target/failure = %#v/%q, want renamed-source consumer/authoritative", materialized.Target, materialized.Failure)
	}
}

func TestCompileConnectPlansFromLoadedPackageFixture(t *testing.T) {
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

	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
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
	if got, want := plan.Source.resolvedEvent, "producer/work.ready"; got != want {
		t.Fatalf("Source.ResolvedEvent = %q, want %q", got, want)
	}
	if got, want := plan.Receiver.resolvedEvent, "consumer/work.ready"; got != want {
		t.Fatalf("Receiver.ResolvedEvent = %q, want %q", got, want)
	}
	if got, want := plan.ResolutionKind, ConnectResolutionStatic; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
	}
	if plan.Target.FlowInstance != "consumer" {
		t.Fatalf("Target = %#v, want concrete static consumer route", plan.Target)
	}
}

func TestLowerCompositionConnectRoutePlanWithLocationRejectsOtherwiseValidConnectWithoutSourceLocation(t *testing.T) {
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
	connects := bundle.CompositionConnects()
	if len(connects) != 1 {
		t.Fatalf("connects = %#v, want one", connects)
	}
	connects[0].SourceFile = ""
	connects[0].SourceLine = 0

	plan, issue := lowerCompositionConnectRoutePlanWithLocation(source, connects[0])
	if issue.Failure != ConnectFailureSourceLocationMissing || issue.AuthoredLocation != "" || plan.AuthoredLocation != "" {
		t.Fatalf("plan = %#v issue = %#v, want source-location issue and no plan", plan, issue)
	}
}

func TestCompileConnectPlansUsesFanInStreamSingularTarget(t *testing.T) {
	source := templatefanin.LoadSource(t, templatefanin.Options{})

	plans, issues := compileConnectPlans(source)

	if len(issues) != 0 {
		t.Fatalf("compileConnectPlans issues = %#v, want none", issues)
	}
	plan := requireFanInRoutePlan(t, plans)
	if plan.FanIn == nil {
		t.Fatalf("fan-in metadata = nil in %#v", plan)
	}
	if plan.FanIn.Aggregation != ConnectFanInStream || plan.FanIn.Window != "payload.period_id" || len(plan.FanIn.DedupBy) != 1 || plan.FanIn.DedupBy[0] != "payload.operating_id" {
		t.Fatalf("fan-in metadata = %#v, want stream/window/dedup", plan.FanIn)
	}
	if plan.TargetKind != ConnectTargetKindTarget || plan.ResolutionKind != ConnectResolutionStatic {
		t.Fatalf("fan-in routing shape = target_kind:%s resolution:%s, want target/static", plan.TargetKind.Code(), plan.ResolutionKind.Code())
	}
	if plan.Target.FlowID != templatefanin.ReceiverFlowID || plan.Target.FlowInstance != templatefanin.ReceiverFlowInstance || plan.Target.EntityID != flowidentity.EntityID(templatefanin.ReceiverFlowInstance) {
		t.Fatalf("fan-in target = %#v, want receiver singleton %s with entity %s", plan.Target, templatefanin.ReceiverFlowInstance, flowidentity.EntityID(templatefanin.ReceiverFlowInstance))
	}
}

func TestCompileConnectPlansAllowsFanInStreamEventIDDedup(t *testing.T) {
	source := templatefanin.LoadSource(t, templatefanin.Options{EventIDDedup: true})

	plans, issues := compileConnectPlans(source)

	if len(issues) != 0 {
		t.Fatalf("compileConnectPlans issues = %#v, want none", issues)
	}
	plan := requireFanInRoutePlan(t, plans)
	if plan.FanIn == nil || len(plan.FanIn.DedupBy) != 1 || plan.FanIn.DedupBy[0] != "event.id" {
		t.Fatalf("fan-in metadata = %#v, want event.id dedup", plan.FanIn)
	}
}

func TestCompileConnectPlansFailsClosedForInvalidFanInStream(t *testing.T) {
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
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := templatefanin.LoadSource(t, tc.opts)

			_, issues := compileConnectPlans(source)

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

func TestCompileConnectPlansUsesFanInBarrierSingularTarget(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, canonicalrouting.ExampleRoot(t, canonicalrouting.FanInBarrier), runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load canonical barrier: %v", err)
	}
	source := semanticview.Wrap(bundle)
	plans, issues := compileConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("compileConnectPlans issues = %#v, want none", issues)
	}
	plan := requireFanInRoutePlan(t, plans)
	if plan.FanIn.Aggregation != ConnectFanInBarrier || plan.FanIn.Window != "payload.period_id" || len(plan.FanIn.DedupBy) != 1 || plan.FanIn.DedupBy[0] != "payload.operating_id" {
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

func TestCompileConnectPlansRejectsAddresslessImplicitInstanceKey(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{})
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	pins := bundle.Semantics.FlowInputEventPins["account"]
	for i := range pins {
		pins[i].Resolution = runtimecontracts.FlowInputPinResolution{}
	}
	bundle.Semantics.FlowInputEventPins["account"] = pins

	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
	if len(plans) != 0 {
		t.Fatalf("plans = %#v, want none without receiver resolution", plans)
	}
	if len(issues) != 1 || issues[0].Failure != ConnectFailureReceiverResolutionMissing {
		t.Fatalf("issues = %#v, want %q", issues, ConnectFailureReceiverResolutionMissing)
	}
}

func TestCompileConnectPlansUsesCreateInputResolution(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeCreateResolutionConnectRoutePlanPackageFixture(t, runtimecontracts.FlowInputCarrySourceGeneratedUUID)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
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
	if plan.Source.key != "" || len(plan.Source.carries) != 0 {
		t.Fatalf("Source key/carries = %q/%#v, want create resolution independent of producer output key", plan.Source.key, plan.Source.carries)
	}
	if got, want := plan.InstanceKey.Mode, runtimecontracts.FlowInputResolutionModeCreate; got != want {
		t.Fatalf("InstanceKey.Mode = %q, want %q", got, want)
	}
	if got, want := plan.InstanceKey.Source.Kind, runtimecontracts.FlowInputInstanceSourceGeneratedUUID; got != want {
		t.Fatalf("InstanceKey.Source.Kind = %q, want %q", got, want)
	}
	if got, want := plan.InstanceKey.Source.Path, runtimecontracts.FlowInputCarrySourceGeneratedUUID; got != want {
		t.Fatalf("InstanceKey.Source.Path = %q, want %q", got, want)
	}
	if got, want := plan.InstanceKey.Field.Path(), "validation_case_id"; got != want {
		t.Fatalf("InstanceKey.Field = %q, want %q", got, want)
	}
	eventID := "11111111-1111-4111-8111-111111111111"
	material, failure := EventSourcedInstanceKeyMaterialForConnectRoutePlan(plan, eventID)
	if failure != "" {
		t.Fatalf("MintedInstanceKeyMaterialForConnectRoutePlan failure = %q", failure)
	}
	if len(material.Keys) != 1 || material.Keys[0].Field.Path() != "validation_case_id" || material.Keys[0].Value == "" || material.Keys[0].Value == eventID {
		t.Fatalf("minted material = %#v, want deterministic uuid material distinct from event id", material)
	}
}

func TestGeneratedUUIDMaterialIsReplayStableAndFreshForForkEventIdentity(t *testing.T) {
	plan := ConnectRoutePlan{
		Receiver: newConnectRoutePlanEndpoint(false, "validator", "validator", runtimecontracts.FlowModeTemplate, "validation_requested", "", "", "", nil),
		InstanceKey: &ConnectRoutePlanInstanceKey{
			Mode:  runtimecontracts.FlowInputResolutionModeCreate,
			Field: mustTemplateInstanceField(t, "validation_case_id"),
			Source: runtimecontracts.FlowInputInstanceSource{
				Kind: runtimecontracts.FlowInputInstanceSourceGeneratedUUID,
				Path: runtimecontracts.FlowInputCarrySourceGeneratedUUID,
			},
		},
	}
	source, failure := EventSourcedInstanceKeyMaterialForConnectRoutePlan(plan, "source-event-id")
	if failure != "" {
		t.Fatalf("source material failure = %q", failure)
	}
	replay, failure := EventSourcedInstanceKeyMaterialForConnectRoutePlan(plan, "source-event-id")
	if failure != "" || !reflect.DeepEqual(replay, source) {
		t.Fatalf("replay material/failure = %#v/%q, want exact %#v", replay, failure, source)
	}
	fork, failure := EventSourcedInstanceKeyMaterialForConnectRoutePlan(plan, "fork-event-id")
	if failure != "" {
		t.Fatalf("fork material failure = %q", failure)
	}
	if len(source.Keys) != 1 || len(fork.Keys) != 1 || source.Keys[0].Value == fork.Keys[0].Value {
		t.Fatalf("source/fork material = %#v/%#v, want fresh deterministic fork value", source, fork)
	}
}

func TestInputPinResolutionMultiPinSatisfactionDerivesOneFlowIdentity(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyExample(t, canonicalrouting.TemplateSelectExisting)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
	if len(issues) != 0 || len(plans) != 2 {
		t.Fatalf("plans/issues = %#v/%#v, want two valid input-pin plans", plans, issues)
	}
	modes := map[runtimecontracts.FlowInputResolutionMode]bool{}
	for _, plan := range plans {
		if plan.InstanceKey == nil || plan.InstanceKey.Field.Path() != "account_id" {
			t.Fatalf("plan instance identity = %#v, want scalar flow instance account_id", plan.InstanceKey)
		}
		if plan.InstanceKey.Source.Kind != runtimecontracts.FlowInputInstanceSourcePayload || plan.InstanceKey.Source.Path != "payload.account_id" {
			t.Fatalf("plan source = %#v, want per-pin payload.account_id", plan.InstanceKey.Source)
		}
		modes[plan.InstanceKey.Mode] = true
	}
	if !modes[runtimecontracts.FlowInputResolutionModeSelect] || !modes[runtimecontracts.FlowInputResolutionModeSelectOrCreate] {
		t.Fatalf("derived modes = %#v, want independent select and select-or-create pins", modes)
	}
}

func TestLowerCompositionConnectRoutePlanWithLocationDerivesRenamedPayloadSourceFromCarry(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyTemplateSelectResolutionRenamedSource(t, canonicalrouting.TemplateSelectResolutionOptions{})
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	pins := bundle.Semantics.FlowInputEventPins["account"]
	if len(pins) != 2 || pins[1].Carries["account_id"].From != "payload.external_account_id" {
		t.Fatalf("renamed source fixture pins = %#v, want account_ready from payload.external_account_id", pins)
	}
	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
	if len(issues) != 0 || len(plans) != 1 || plans[0].InstanceKey == nil {
		t.Fatalf("plans/issues = %#v/%#v, want one derived plan", plans, issues)
	}
	if plans[0].InstanceKey.Source.Kind != runtimecontracts.FlowInputInstanceSourcePayload || plans[0].InstanceKey.Source.Path != "payload.external_account_id" || plans[0].InstanceKey.Field.Path() != "account_id" {
		t.Fatalf("derived identity/source = %#v, want account_id from renamed payload source", plans[0].InstanceKey)
	}
	for _, tc := range []struct {
		name        string
		matchValues map[string]string
	}{
		{name: "renamed field only", matchValues: map[string]string{"payload.external_account_id": "acct-authoritative"}},
		{name: "conflicting same-named field", matchValues: map[string]string{
			"payload.external_account_id": "acct-authoritative",
			"payload.account_id":          "acct-conflicting",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			materialized := MaterializeConnectRoutePlan(plans[0], ConnectRoutePlanMaterializationInput{
				MatchValues: tc.matchValues,
				Descriptors: []Descriptor{
					{EntityID: "ent-authoritative", FlowInstance: "account/authoritative", AddressFields: map[string]string{"entity.account_id": "acct-authoritative"}},
					{EntityID: "ent-conflicting", FlowInstance: "account/conflicting", AddressFields: map[string]string{"entity.account_id": "acct-conflicting"}},
				},
			})
			if materialized.Failure != "" || materialized.Target.FlowInstance != "account/authoritative" {
				t.Fatalf("materialized target/failure = %#v/%q, want renamed-source account/authoritative", materialized.Target, materialized.Failure)
			}
		})
	}
}

func TestCompileConnectPlansValidatesAuthoritativeInstanceSourceTypeMatrix(t *testing.T) {
	tests := []struct {
		name      string
		root      func(*testing.T) string
		wantError bool
	}{
		{
			name: "select accepts compatible aliases",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{})
			},
		},
		{
			name: "select rejects omitted annotation mismatch",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{Invalidity: canonicalrouting.SelectResolutionSourceTypeMismatchWithoutCarryType})
			},
			wantError: true,
		},
		{
			name: "select-or-create rejects dishonest annotation",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{Mode: canonicalrouting.SelectResolutionSelectOrCreate, Invalidity: canonicalrouting.SelectResolutionDishonestCarryType})
			},
			wantError: true,
		},
		{
			name: "select rejects number source for integer receiver",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{Invalidity: canonicalrouting.SelectResolutionNumberSourceToIntegerReceiver})
			},
			wantError: true,
		},
		{
			name: "select-or-create rejects number source for integer receiver",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{Mode: canonicalrouting.SelectResolutionSelectOrCreate, Invalidity: canonicalrouting.SelectResolutionNumberSourceToIntegerReceiver})
			},
			wantError: true,
		},
		{
			name: "create accepts payload alias",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintPayload})
			},
		},
		{
			name: "create accepts intrinsic generated uuid",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintUUID})
			},
		},
		{
			name: "create accepts intrinsic event id",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintEventID})
			},
		},
		{
			name: "create payload rejects omitted annotation mismatch",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintPayload, Invalidity: canonicalrouting.CreateResolutionSourceTypeMismatchWithoutCarryType})
			},
			wantError: true,
		},
		{
			name: "create rejects number source for integer receiver",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintPayload, Invalidity: canonicalrouting.CreateResolutionNumberSourceToIntegerReceiver})
			},
			wantError: true,
		},
		{
			name: "create generated uuid rejects incompatible receiver",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintUUID, Invalidity: canonicalrouting.CreateResolutionSourceTypeMismatchWithoutCarryType})
			},
			wantError: true,
		},
		{
			name: "create event id rejects dishonest annotation",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintEventID, Invalidity: canonicalrouting.CreateResolutionDishonestCarryType})
			},
			wantError: true,
		},
	}
	repoRoot := canonicalrouting.RepoRoot(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, tc.root(t), runtimecontracts.DefaultPlatformSpecFile(repoRoot))
			if err != nil {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
			}
			_, issues := compileConnectPlans(semanticview.Wrap(bundle))
			gotError := false
			for _, issue := range issues {
				if strings.Contains(issue.Detail, "key_types_incompatible") {
					gotError = true
				}
			}
			if gotError != tc.wantError {
				t.Fatalf("key type issue = %v, want %v; issues = %#v", gotError, tc.wantError, issues)
			}
		})
	}
}

func TestLowerTargetFreeInputRoutePlansRejectsAuthoritativeSourceTypeMismatch(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyProviderRollbackInvalidSourceType(t, canonicalrouting.ProviderRollbackSourceTypeOmittedMismatch)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load provider rollback artifact: %v", err)
	}
	authorization := runtimeprovideroutput.MustAuthorization(
		"telegram", "inbound.telegram.text_message", "provider.telegram", "1.0.0",
		"sha256:"+strings.Repeat("a", 64),
		triggergeneration.FromCanonicalBytes([]byte("target-free-source-type")),
	)

	plans, issues := lowerTargetFreeInputRoutePlans(semanticview.Wrap(bundle), []runtimeprovideroutput.Authorization{authorization})
	if len(plans) != 0 || len(issues) != 1 || !strings.Contains(issues[0].Detail, "key_types_incompatible") {
		t.Fatalf("plans/issues = %#v/%#v, want target-free source type blocker", plans, issues)
	}
}

func TestConnectRoutePlanProductionAPIHasNoRetiredIdentityOrPolicyFacts(t *testing.T) {
	for _, tc := range []struct {
		name      string
		typeValue reflect.Type
		forbidden []string
	}{
		{name: "plan", typeValue: reflect.TypeOf(ConnectRoutePlan{}), forbidden: []string{"Address", "Map"}},
		{name: "instance key", typeValue: reflect.TypeOf(ConnectRoutePlanInstanceKey{}), forbidden: []string{"Fields", "Mappings", "OnMissing", "OnConflict"}},
		{name: "materialization input", typeValue: reflect.TypeOf(ConnectRoutePlanMaterializationInput{}), forbidden: []string{"SupportedAddressTargets"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, field := range tc.forbidden {
				if _, ok := tc.typeValue.FieldByName(field); ok {
					t.Fatalf("%s still exposes retired field %s", tc.typeValue, field)
				}
			}
		})
	}
}

func TestCompileConnectPlansUsesSelectInputResolution(t *testing.T) {
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

	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
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
	if got, want := plan.InstanceKey.Field.Path(), "account_id"; got != want {
		t.Fatalf("InstanceKey.Field = %q, want %q", got, want)
	}
	if plan.InstanceKey.Source.Kind != runtimecontracts.FlowInputInstanceSourcePayload || plan.InstanceKey.Source.Path != "payload.account_id" {
		t.Fatalf("InstanceKey.Source = %#v, want payload.account_id", plan.InstanceKey.Source)
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

func TestCompileConnectPlansUsesSelectOrCreateInputResolution(t *testing.T) {
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

	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
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
	if got, want := plan.InstanceKey.Field.Path(), "account_id"; got != want {
		t.Fatalf("InstanceKey.Field = %q, want %q", got, want)
	}
	if plan.InstanceKey.Source.Kind != runtimecontracts.FlowInputInstanceSourcePayload || plan.InstanceKey.Source.Path != "payload.account_id" {
		t.Fatalf("InstanceKey.Source = %#v, want payload.account_id", plan.InstanceKey.Source)
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

func TestCompileConnectPlansRejectsExtraSelectResolutionFields(t *testing.T) {
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

	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
	if len(plans) != 0 {
		t.Fatalf("plans = %#v, want none for invalid select resolution", plans)
	}
	if len(issues) != 1 {
		t.Fatalf("issues = %#v, want one fail-closed issue", issues)
	}
	if issues[0].Failure != ConnectFailureInstanceResolutionInvalid || !strings.Contains(issues[0].Detail, "may only declare mode and carries") {
		t.Fatalf("issue = %#v, want instance resolution invalid for extra select field", issues[0])
	}
}

func TestCompileConnectPlansRejectsSelectCarryTypeMismatch(t *testing.T) {
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

	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
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

func TestCompileConnectPlansRejectsSelectOrCreateCarryTypeMismatch(t *testing.T) {
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

	plans, issues := compileConnectPlans(semanticview.Wrap(bundle))
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

func TestCompileConnectPlansOneToOneStatic(t *testing.T) {
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
	}, []runtimecontracts.FlowPackageConnect{{
		From:    "producer.deploy_done",
		To:      "consumer.deploy_completed",
		Adapter: "deploy_done_to_completed",
	}})

	plans, issues := compileConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if got, want := plan.Source.flowID, "producer"; got != want {
		t.Fatalf("Source.FlowID = %q, want %q", got, want)
	}
	if got, want := plan.Source.pin, "deploy_done"; got != want {
		t.Fatalf("Source.Pin = %q, want %q", got, want)
	}
	if got, want := plan.Source.event, "deploy.done"; got != want {
		t.Fatalf("Source.Event = %q, want %q", got, want)
	}
	if got, want := plan.Source.resolvedEvent, "producer/deploy.done"; got != want {
		t.Fatalf("Source.ResolvedEvent = %q, want %q", got, want)
	}
	if got, want := plan.Receiver.pin, "deploy_completed"; got != want {
		t.Fatalf("Receiver.Pin = %q, want %q", got, want)
	}
	if got, want := plan.Receiver.event, "deploy.completed"; got != want {
		t.Fatalf("Receiver.Event = %q, want %q", got, want)
	}
	if got, want := plan.TargetKind, ConnectTargetKindTarget; got != want {
		t.Fatalf("TargetKind = %q, want %q", got, want)
	}
	if got, want := plan.ResolutionKind, ConnectResolutionStatic; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
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

func TestCompileConnectPlansRootProducerToStaticReceiver(t *testing.T) {
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

	plans, issues := compileConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if !plan.Source.IsRoot() {
		t.Fatalf("Source.Root = false, want true: %#v", plan.Source)
	}
	if got, want := plan.Source.flowID, ""; got != want {
		t.Fatalf("Source.FlowID = %q, want root empty flow id", got)
	}
	if got, want := plan.Source.pin, "root_ready"; got != want {
		t.Fatalf("Source.Pin = %q, want %q", got, want)
	}
	if got, want := plan.Source.resolvedEvent, "root.ready"; got != want {
		t.Fatalf("Source.ResolvedEvent = %q, want %q", got, want)
	}
	if got, want := plan.Receiver.flowID, "consumer"; got != want {
		t.Fatalf("Receiver.FlowID = %q, want %q", got, want)
	}
	if plan.Target.FlowInstance != "consumer" {
		t.Fatalf("Target.FlowInstance = %q, want consumer", plan.Target.FlowInstance)
	}
}

func TestCompileConnectPlansSupportsRootReceiverEndpoint(t *testing.T) {
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

	plans, issues := compileConnectPlans(source)
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
	if !plan.Receiver.IsRoot() || plan.Receiver.flowID != "" || plan.Receiver.pin != "root_ready" {
		t.Fatalf("Receiver = %#v, want root input root_ready", plan.Receiver)
	}
	if plan.Target.FlowInstance != "" || plan.Target.EntityID != "" || plan.RequiresRuntimeResolution {
		t.Fatalf("root target = %#v (runtime=%t), want root-static target", plan.Target, plan.RequiresRuntimeResolution)
	}
}

func TestCompiledConnectProducerEvidenceOwnsConnectedRootInput(t *testing.T) {
	source := testRootReceiverConnectRoutePlanSource([]runtimecontracts.FlowInputEventPin{{
		Name: "work_requested", Event: "work.requested",
	}}, []connectRoutePlanFlow{{
		id: "producer", mode: "static",
		outputs: []runtimecontracts.FlowOutputEventPin{{Name: "work_completed", Event: "work.requested"}},
	}}, []runtimecontracts.FlowPackageConnect{{From: "producer.work_completed", To: ".work_requested"}})

	resolution := ResolveFlowInputProducer(source, "", "work.requested")
	if !resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryParentConnect) {
		t.Fatalf("evidence = %#v, want compiled parent-connect evidence", resolution.Evidence)
	}
	if resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryExternalIngress) {
		t.Fatalf("evidence = %#v, connected root input must not retain external-ingress authority", resolution.Evidence)
	}
}

func TestCompiledConnectProducerEvidenceOwnsConnectedChildInput(t *testing.T) {
	source := testConnectRoutePlanSource([]connectRoutePlanFlow{
		{id: "producer", mode: "static", outputs: []runtimecontracts.FlowOutputEventPin{{Name: "work_ready", Event: "work.requested"}}},
		{id: "worker", mode: "static", inputs: []runtimecontracts.FlowInputEventPin{{Name: "work_requested", Event: "work.requested"}}},
	}, []runtimecontracts.FlowPackageConnect{{From: "producer.work_ready", To: "worker.work_requested"}})

	resolution := ResolveFlowInputProducer(source, "worker", "work.requested")
	if !resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryParentConnect) {
		t.Fatalf("evidence = %#v, want compiled parent-connect evidence", resolution.Evidence)
	}
	if len(resolution.ProducerPatterns()) != 0 {
		t.Fatalf("patterns = %#v, compiled connect evidence must not create auto-wire authority", resolution.ProducerPatterns())
	}
}

func TestLowerCompositionConnectRoutePlanWithLocationDoesNotDependOnRawPinNamesOrProducerTargets(t *testing.T) {
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

	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil || len(bundle.CompositionConnects()) != 1 {
		t.Fatal("compiled connect input is unavailable")
	}
	plan, issue := lowerCompositionConnectRoutePlanWithLocation(source, bundle.CompositionConnects()[0])
	if issue.Failure != "" {
		t.Fatalf("issue = %#v, want none", issue)
	}
	if got, want := plan.Source.pin, "public_done"; got != want {
		t.Fatalf("Source.Pin = %q, want %q", got, want)
	}
	if got, want := plan.Source.event, "internal.done"; got != want {
		t.Fatalf("Source.Event = %q, want %q", got, want)
	}
	if got, want := plan.Receiver.pin, "accept_completed"; got != want {
		t.Fatalf("Receiver.Pin = %q, want %q", got, want)
	}
	if got, want := plan.Receiver.event, "external.completed"; got != want {
		t.Fatalf("Receiver.Event = %q, want %q", got, want)
	}
	if got, want := plan.Adapter, "public_done_to_accept_completed"; got != want {
		t.Fatalf("Adapter = %q, want %q", got, want)
	}
}

func TestLowerCompositionConnectRoutePlanWithLocationFailsClosedForInvalidInputs(t *testing.T) {
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
			_, issue := lowerCompositionConnectRoutePlanWithLocation(source, tc.connect)
			if issue.Failure != tc.want {
				t.Fatalf("Failure = %q, want %q (issue %#v)", issue.Failure, tc.want, issue)
			}
		})
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
	return canonicalrouting.CopyExample(t, canonicalrouting.ParentConnect)
}

func writeCreateResolutionConnectRoutePlanPackageFixture(t *testing.T, source string) string {
	t.Helper()
	mode := canonicalrouting.CreateMintUUID
	if strings.TrimSpace(source) == runtimecontracts.FlowInputCarrySourceEventID {
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
	mode                runtimecontracts.FlowInputResolutionMode
	extraResolution     string
	accountIDEntityType string
	accountIDCarryType  string
}

func writeSelectResolutionConnectRoutePlanPackageFixtureWithOptions(t *testing.T, options selectResolutionConnectRoutePlanFixtureOptions) string {
	t.Helper()
	mode := canonicalrouting.SelectResolutionSelect
	if options.mode == runtimecontracts.FlowInputResolutionModeSelectOrCreate {
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

func mustTemplateInstanceField(t testing.TB, raw string) runtimecontracts.TemplateInstanceField {
	t.Helper()
	field, err := runtimecontracts.ParseTemplateInstanceField(raw)
	if err != nil {
		t.Fatalf("ParseTemplateInstanceField(%q): %v", raw, err)
	}
	return field
}
