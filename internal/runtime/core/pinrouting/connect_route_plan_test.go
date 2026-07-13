package pinrouting

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templatefanin"
)

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
	if len(plans) != 1 {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want one fan-in route plan", plans)
	}
	plan := plans[0]
	if plan.FanIn == nil {
		t.Fatalf("fan-in metadata = nil in %#v", plan)
	}
	if plan.FanIn.Aggregation != "stream" || plan.FanIn.Window != "payload.period_id" || len(plan.FanIn.DedupBy) != 1 || plan.FanIn.DedupBy[0] != "payload.report_id" {
		t.Fatalf("fan-in metadata = %#v, want stream/window/dedup", plan.FanIn)
	}
	if plan.TargetKind != ConnectTargetKindTarget || plan.ResolutionKind != ConnectResolutionStatic || plan.Delivery != ConnectDeliveryOne {
		t.Fatalf("fan-in routing shape = delivery:%s target_kind:%s resolution:%s, want one/target/static", plan.Delivery, plan.TargetKind, plan.ResolutionKind)
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
	if len(plans) != 1 {
		t.Fatalf("LowerCompositionConnectRoutePlans = %#v, want one fan-in route plan", plans)
	}
	plan := plans[0]
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
		{name: "barrier", opts: templatefanin.Options{BarrierAggregation: true}, failure: ConnectFailureInstanceResolutionInvalid, detail: "aggregation: stream"},
		{name: "wrong singleton", opts: templatefanin.Options{WrongSingleton: true}, failure: ConnectFailureInstanceResolutionInvalid, detail: "must be the receiver singleton route or a child"},
		{name: "non-singleton receiver", opts: templatefanin.Options{NonSingletonReceiver: true}, failure: ConnectFailureInstanceResolutionInvalid, detail: "is not mode: singleton"},
		{name: "delivery many", opts: templatefanin.Options{DeliveryMany: true}, failure: ConnectFailureDeliveryTopologyInvalid, detail: "requires delivery one"},
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
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	plan := plans[0]
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

func TestLowerCompositionConnectRoutePlansBroadcastBeatsInstanceKey(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	repoRoot = filepath.Clean(filepath.Join(repoRoot, "..", "..", "..", ".."))
	root := writeInstanceKeyConnectRoutePlanPackageFixtureWithDelivery(t, "broadcast")
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
	if got, want := plan.ResolutionKind, ConnectResolutionBroadcast; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
	}
	if plan.InstanceKey != nil {
		t.Fatalf("InstanceKey = %#v, want nil for explicit parent broadcast", plan.InstanceKey)
	}
	if got, want := plan.Source.Key, "vertical_id"; got != want {
		t.Fatalf("Source.Key = %q, want retained producer key evidence", got)
	}

	materialized := MaterializeConnectRoutePlan(plan, ConnectRoutePlanMaterializationInput{
		MatchValues: map[string]string{"vertical_id": "v-1"},
		Descriptors: []Descriptor{
			{EntityID: "ent-1", FlowInstance: "consumer/one", AddressFields: map[string]string{"entity.vertical_id": "v-1"}},
			{EntityID: "ent-2", FlowInstance: "consumer/two", AddressFields: map[string]string{"entity.vertical_id": "v-2"}},
			{EntityID: "ent-3", FlowInstance: "other/three", AddressFields: map[string]string{"entity.vertical_id": "v-1"}},
		},
	})
	if materialized.Failure != "" {
		t.Fatalf("Failure = %q, want empty", materialized.Failure)
	}
	if len(materialized.TargetSet) != 2 {
		t.Fatalf("TargetSet = %#v, want both receiver-scoped descriptors", materialized.TargetSet)
	}
	if materialized.TargetSet[0].FlowInstance != "consumer/one" || materialized.TargetSet[1].FlowInstance != "consumer/two" {
		t.Fatalf("TargetSet = %#v, want consumer/one and consumer/two without payload-key filtering", materialized.TargetSet)
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
		From:     "producer.deploy_done",
		To:       "consumer.deploy_completed",
		Adapter:  "deploy_done_to_completed",
		Delivery: "one",
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
	if got, want := plan.Delivery, ConnectDeliveryOne; got != want {
		t.Fatalf("Delivery = %q, want %q", got, want)
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
		From:     ".root_ready",
		To:       "consumer.ready",
		Delivery: "one",
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

func TestLowerCompositionConnectRoutePlansRejectsRootReceiverEndpoint(t *testing.T) {
	source := testRootConnectRoutePlanSource([]runtimecontracts.FlowOutputEventPin{{
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
	if len(plans) != 0 {
		t.Fatalf("plans = %#v, want none", plans)
	}
	if len(issues) != 1 || issues[0].Failure != ConnectFailureReceiverRootUnsupported {
		t.Fatalf("issues = %#v, want receiver_root_unsupported", issues)
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
		From:     "producer.ticket_ready",
		To:       "worker.ticket_ready",
		Delivery: "many",
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

func TestMaterializeConnectRoutePlanBroadcastsAddresslessTemplateDescriptors(t *testing.T) {
	source := testConnectRoutePlanSource([]connectRoutePlanFlow{
		{
			id:   "producer",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "notice_ready",
				Event: "notice.ready",
			}},
		},
		{
			id:   "worker",
			mode: "template",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "notice_ready",
				Event: "notice.ready",
			}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From:     "producer.notice_ready",
		To:       "worker.notice_ready",
		Delivery: "broadcast",
	}})

	plans, issues := LowerCompositionConnectRoutePlans(source)
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	materialized := MaterializeConnectRoutePlan(plans[0], ConnectRoutePlanMaterializationInput{
		Descriptors: []Descriptor{
			{FlowInstance: "worker/alpha"},
			{FlowInstance: "other/alpha"},
			{FlowInstance: "worker/beta"},
		},
	})
	if materialized.Failure != "" {
		t.Fatalf("Failure = %q, want empty", materialized.Failure)
	}
	if len(materialized.TargetSet) != 2 {
		t.Fatalf("TargetSet = %#v, want two worker routes", materialized.TargetSet)
	}
	if materialized.TargetSet[0].FlowInstance != "worker/alpha" || materialized.TargetSet[1].FlowInstance != "worker/beta" {
		t.Fatalf("TargetSet = %#v, want receiver-scoped worker routes only", materialized.TargetSet)
	}
}

func TestLowerCompositionConnectRoutePlanPreservesReplyLineage(t *testing.T) {
	source := testConnectRoutePlanSource([]connectRoutePlanFlow{
		{
			id:   "requester",
			mode: "static",
			outputs: []runtimecontracts.FlowOutputEventPin{{
				Name:  "approval_requested",
				Event: "approval.requested",
			}},
		},
		{
			id:   "approver",
			mode: "static",
			inputs: []runtimecontracts.FlowInputEventPin{{
				Name:  "approval_requested",
				Event: "approval.requested",
			}},
		},
	}, []runtimecontracts.FlowPackageConnect{{
		From:     "requester.approval_requested",
		To:       "approver.approval_requested",
		Delivery: "reply",
		Reply: map[string]string{
			"source_event_id": "event.source_event_id",
			"target":          "event.source",
		},
	}})

	plan, issue := LowerCompositionConnectRoutePlan(source, source.CompositionConnects()[0])
	if issue.Failure != "" {
		t.Fatalf("issue = %#v, want none", issue)
	}
	if got, want := plan.Delivery, ConnectDeliveryReply; got != want {
		t.Fatalf("Delivery = %q, want %q", got, want)
	}
	if got, want := plan.TargetKind, ConnectTargetKindReply; got != want {
		t.Fatalf("TargetKind = %q, want %q", got, want)
	}
	if plan.Reply["source_event_id"] != "event.source_event_id" || plan.Reply["target"] != "event.source" {
		t.Fatalf("Reply = %#v, want lineage preserved", plan.Reply)
	}
	if plan.Target.FlowInstance != "approver" {
		t.Fatalf("Target = %#v, want static approver route", plan.Target)
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
		From:     "producer.public_done",
		To:       "consumer.accept_completed",
		Adapter:  "public_done_to_accept_completed",
		Delivery: "one",
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
	// routing-example-census: parser-only issue=none owner=pinrouting.typed_route_plan_lowering proof=TestLowerCompositionConnectRoutePlanFailsClosedForInvalidInputs
	tests := []struct {
		name    string
		connect runtimecontracts.FlowPackageConnect
		want    ConnectRoutePlanFailure
	}{
		{
			name:    "missing output pin",
			connect: runtimecontracts.FlowPackageConnect{From: "producer.missing", To: "consumer.deploy_completed", Delivery: "one"},
			want:    ConnectFailureProducerOutputPinMissing,
		},
		{
			name:    "invalid delivery",
			connect: runtimecontracts.FlowPackageConnect{From: "producer.deploy_done", To: "consumer.deploy_completed", Delivery: "maybe"},
			want:    ConnectFailureDeliveryTopologyInvalid,
		},
		{
			name:    "reply without lineage",
			connect: runtimecontracts.FlowPackageConnect{From: "producer.deploy_done", To: "consumer.deploy_completed", Delivery: "reply"},
			want:    ConnectFailureReplyLineageMissing,
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
		From:     "producer.deploy_done",
		To:       "consumer.deploy_completed",
		Delivery: "one",
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
	root := canonicalrouting.CopyExample(t, canonicalrouting.ParentConnect)
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "package.yaml"), "    to: consumer.work_ready\n", `    to: consumer.work_ready
    adapter: work_ready_projection
    map:
      work_id:
        source: payload.work_id
        target: entity.work_id
`)
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), "        event: work.ready\n", `        event: work.ready
        address:
          by: work_id
          source: payload.work_id
          target: entity.work_id
          cardinality: one
`)
	canonicalrouting.WriteFile(t, root, "flows/consumer/entities.yaml", `
work:
  work_id:
    type: text
`)
	return root
}

func writeInstanceKeyConnectRoutePlanPackageFixture(t *testing.T) string {
	return writeInstanceKeyConnectRoutePlanPackageFixtureWithDelivery(t, "one")
}

func writeCreateResolutionConnectRoutePlanPackageFixture(t *testing.T, mint string) string {
	t.Helper()
	root := canonicalrouting.CopyExample(t, canonicalrouting.TemplateCreateMintedKey)
	if strings.TrimSpace(mint) != "uuid" {
		canonicalrouting.ReplaceFile(t, filepath.Join(root, "flows", "validator", "schema.yaml"), "            mint: uuid\n", "            mint: "+strings.TrimSpace(mint)+"\n")
	}
	return root
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
	// routing-example-census: different-concept issue=none owner=pinrouting.select_resolution_lowering_matrix proof=TestLowerCompositionConnectRoutePlansRejectsExtraSelectResolutionFields
	t.Helper()
	accountIDEntityType := strings.TrimSpace(options.accountIDEntityType)
	if accountIDEntityType == "" {
		accountIDEntityType = "string"
	}
	accountIDCarryType := strings.TrimSpace(options.accountIDCarryType)
	if accountIDCarryType == "" {
		accountIDCarryType = "string"
	}
	mode := strings.TrimSpace(options.mode)
	if mode == "" {
		mode = runtimecontracts.FlowInputResolutionModeSelect
	}
	root := canonicalrouting.CopyExample(t, canonicalrouting.TemplateSelectExisting)
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "package.yaml"), `  - from: producer.account_setup
    to: account.account_setup
`, "")
	accountSchema := filepath.Join(root, "flows", "account", "schema.yaml")
	canonicalrouting.ReplaceFile(t, accountSchema, `      - name: account_ready
        event: account.ready
        resolution:
          mode: select
          instance_key: account_id
`, `      - name: account_ready
        event: account.ready
        resolution:
          mode: `+mode+`
          instance_key: account_id
`+options.extraResolution)
	canonicalrouting.ReplaceFile(t, accountSchema, `      - name: account_ready
        event: account.ready
        resolution:
          mode: `+mode+`
          instance_key: account_id
`+options.extraResolution+`        carries:
          account_id:
            from: payload.account_id
            type: text
`, `      - name: account_ready
        event: account.ready
        resolution:
          mode: `+mode+`
          instance_key: account_id
`+options.extraResolution+`        carries:
          account_id:
            from: payload.account_id
            type: `+accountIDCarryType+`
`)
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "flows", "account", "entities.yaml"), "    type: text\n", "    type: "+accountIDEntityType+"\n")
	return root
}

func writeInstanceKeyConnectRoutePlanPackageFixtureWithDelivery(t *testing.T, delivery string) string {
	// routing-example-census: different-concept issue=1738 owner=legacy_template_instance_routing proof=TestLowerCompositionConnectRoutePlansUsesTemplateInstanceKey
	t.Helper()
	root := t.TempDir()
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: instance-key-connect-route-plan-package
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: template
connect:
  - from: producer.deploy_done
    to: consumer.deploy_completed
    delivery: `+delivery+`
`)
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: instance-key-connect-route-plan-package\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeConnectRoutePlanFlowFixture(t, root, "producer", `
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
        key: vertical_id
        carries: [vertical_id]
`, "deploy.done:\n  vertical_id: string\n", "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
mode: template
instance:
  by: vertical_id
  on_missing: reject
  on_conflict: reject
pins:
  inputs:
    events:
      - name: deploy_completed
        event: deploy.done
`)
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), "deploy.done:\n  vertical_id: string\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), `
deployment:
  vertical_id:
    type: string
`)
	return root
}

func writeInstanceKeyAdapterConnectRoutePlanPackageFixture(t *testing.T, usingBlock, outputKey, outputCarries, instanceBy string) string {
	// routing-example-census: different-concept issue=1738 owner=legacy_template_instance_routing proof=TestLowerCompositionConnectRoutePlansUsesRenamedInstanceKeyAdapter
	t.Helper()
	root := t.TempDir()
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: instance-key-adapter-connect-route-plan-package
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: template
connect:
  - from: producer.deploy_done
    to: consumer.deploy_completed
    delivery: one`+usingBlock+`
`)
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: instance-key-adapter-connect-route-plan-package\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeConnectRoutePlanFlowFixture(t, root, "producer", `
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
        key: `+outputKey+`
        carries: `+outputCarries+`
`, "deploy.done:\n  source_vertical_id: string\n", "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
mode: template
instance:
  by: `+instanceBy+`
  on_missing: reject
  on_conflict: reject
pins:
  inputs:
    events:
      - name: deploy_completed
        event: deploy.done
`)
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), "deploy.done:\n  source_vertical_id: string\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), `
deployment:
  vertical_id:
    type: string
`)
	return root
}

func writeAddressedTemplateConnectRoutePlanPackageFixture(t *testing.T) string {
	// routing-example-census: different-concept issue=1738 owner=legacy_addressed_template_routing proof=TestLowerCompositionConnectRoutePlansPreservesAddressedTemplateRoute
	t.Helper()
	root := canonicalrouting.CopyExample(t, canonicalrouting.TemplateSelectExisting)
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "package.yaml"), `  - from: producer.account_setup
    to: account.account_setup
`, "")
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `      - name: account_ready
        event: account.ready
`, `      - name: account_ready
        event: account.ready
        key: account_id
        carries: [account_id, customer_id]
`)
	for _, eventsFile := range []string{"flows/producer/events.yaml", "flows/account/events.yaml"} {
		canonicalrouting.ReplaceFile(t, filepath.Join(root, eventsFile), `account.ready:
  account_id: text
`, `account.ready:
  account_id: text
  customer_id: text
`)
	}
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "flows", "account", "schema.yaml"), `      - name: account_ready
        event: account.ready
        resolution:
          mode: select
          instance_key: account_id
        carries:
          account_id:
            from: payload.account_id
            type: text
`, `      - name: account_ready
        event: account.ready
        address:
          by: customer_id
          source: payload.customer_id
          target: entity.customer_id
          cardinality: one
`)
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "flows", "account", "entities.yaml"), `    _unused_reason: receiver instance identity
`, `    _unused_reason: receiver instance identity
  customer_id:
    type: text
    indexed: true
`)
	return root
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
