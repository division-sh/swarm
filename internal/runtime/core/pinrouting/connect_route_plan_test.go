package pinrouting

import (
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
	if got, want := plan.Source.ResolvedEvent, "producer/deploy.done"; got != want {
		t.Fatalf("Source.ResolvedEvent = %q, want %q", got, want)
	}
	if got, want := plan.Receiver.ResolvedEvent, "consumer/deploy.completed"; got != want {
		t.Fatalf("Receiver.ResolvedEvent = %q, want %q", got, want)
	}
	if plan.Address == nil || plan.Address.By != "vertical_id" {
		t.Fatalf("Address = %#v, want loaded vertical_id address", plan.Address)
	}
	if got, want := plan.ResolutionKind, ConnectResolutionStatic; got != want {
		t.Fatalf("ResolutionKind = %q, want %q", got, want)
	}
	if plan.Target.FlowInstance != "consumer" {
		t.Fatalf("Target = %#v, want concrete static consumer route", plan.Target)
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
				FlowInstance: "consumer/addressed",
				AddressFields: map[string]string{
					"entity.customer_id": "cust-1",
					"entity.vertical_id": "v-other",
				},
			},
			{
				EntityID:     "ent-instance",
				FlowInstance: "consumer/instance-key",
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
	if got, want := materialized.Target.FlowInstance, "consumer/addressed"; got != want {
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
	root := t.TempDir()
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: connect-route-plan-package
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: static
connect:
  - from: producer.deploy_done
    to: consumer.deploy_completed
    adapter: deploy_done_to_completed
    delivery: one
    map:
      vertical_id:
        source: payload.vertical_id
        target: entity.vertical_id
`)
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: connect-route-plan-package\n")
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
`, "deploy.done: {}\n", "{}\n")
	writeConnectRoutePlanFlowFixture(t, root, "consumer", `
pins:
  inputs:
    events:
      - name: deploy_completed
        event: deploy.completed
        address:
          by: vertical_id
          source: payload.vertical_id
          target: entity.vertical_id
          cardinality: one
`, "deploy.completed: {}\n", `
deployment:
  vertical_id:
    type: string
`)
	return root
}

func writeInstanceKeyConnectRoutePlanPackageFixture(t *testing.T) string {
	return writeInstanceKeyConnectRoutePlanPackageFixtureWithDelivery(t, "one")
}

func writeCreateResolutionConnectRoutePlanPackageFixture(t *testing.T, mint string) string {
	t.Helper()
	root := t.TempDir()
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: create-resolution-connect-route-plan-package
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: validator
    flow: validator
    mode: template
connect:
  - from: producer.validation_requested
    to: validator.validation_requested
    delivery: one
`)
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: create-resolution-connect-route-plan-package\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeConnectRoutePlanFlowFixture(t, root, "producer", `
pins:
  outputs:
    events:
      - name: validation_requested
        event: validation.requested
`, "validation.requested:\n  candidate: string\n", "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "validator", "schema.yaml"), `
name: validator
mode: template
instance:
  by: validation_case_id
  on_missing: reject
  on_conflict: reject
pins:
  inputs:
    events:
      - name: validation_requested
        event: validation.requested
        resolution:
          mode: create
          instance_key:
            mint: `+mint+`
            as: validation_case_id
        carries:
          validation_case_id:
            from: instance.key.validation_case_id
            type: uuid
`)
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "validator", "policy.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "validator", "agents.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "validator", "nodes.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "validator", "events.yaml"), "validation.requested:\n  candidate: string\n  validation_case_id: uuid\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "validator", "entities.yaml"), `
validation_case:
  validation_case_id:
    type: uuid
`)
	return root
}

func writeSelectResolutionConnectRoutePlanPackageFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: select-resolution-connect-route-plan-package
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: account
    flow: account
    mode: template
connect:
  - from: producer.account_ready
    to: account.account_ready
    delivery: one
`)
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: select-resolution-connect-route-plan-package\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeConnectRoutePlanFlowFixture(t, root, "producer", `
pins:
  outputs:
    events:
      - name: account_ready
        event: account.ready
`, "account.ready:\n  account_id: string\n", "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "account", "schema.yaml"), `
name: account
mode: template
instance:
  by: account_id
  on_missing: create
  on_conflict: reuse
pins:
  inputs:
    events:
      - name: account_ready
        event: account.ready
        resolution:
          mode: select
          instance_key: account_id
        carries:
          account_id:
            from: payload.account_id
            type: string
`)
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "account", "policy.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "account", "agents.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "account", "nodes.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "account", "events.yaml"), "account.ready:\n  account_id: string\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "account", "entities.yaml"), `
account_state:
  account_id:
    type: string
`)
	return root
}

func writeInstanceKeyConnectRoutePlanPackageFixtureWithDelivery(t *testing.T, delivery string) string {
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
	t.Helper()
	root := t.TempDir()
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: addressed-template-connect-route-plan-package
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
    delivery: one
`)
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: addressed-template-connect-route-plan-package\n")
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
        carries: [vertical_id, customer_id]
`, "deploy.done:\n  vertical_id: string\n  customer_id: string\n", "{}\n")
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
        address:
          by: customer_id
          source: payload.customer_id
          target: entity.customer_id
          cardinality: one
`)
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), "{}\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), "deploy.done:\n  vertical_id: string\n  customer_id: string\n")
	writeConnectRoutePlanFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), `
deployment:
  vertical_id:
    type: string
  customer_id:
    type: string
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
