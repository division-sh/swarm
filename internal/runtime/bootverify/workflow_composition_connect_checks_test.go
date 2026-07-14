package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/templatefanin"
)

func TestRun_AllowsParentCompositionConnectAsVerifyRouteProof(t *testing.T) {
	root := canonicalrouting.ExampleRoot(t, canonicalrouting.ParentConnect)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "") {
		t.Fatalf("unexpected composition_connect_validation error: %#v", report.Errors())
	}
	if reportContains(report.Errors(), "pin_target_resolution", "work.ready") {
		t.Fatalf("parent connect should satisfy output pin target proof, got %#v", report.Errors())
	}
	if reportContains(report.Errors(), "input_pin_wiring", "work.ready") {
		t.Fatalf("parent connect should satisfy input pin wiring proof, got %#v", report.Errors())
	}
}

func TestRun_AllowsRootProducerCompositionConnectAsRouteProof(t *testing.T) {
	root := writeRootCompositionConnectBootverifyFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "") {
		t.Fatalf("unexpected composition_connect_validation error: %#v", report.Errors())
	}
	if reportContains(report.Errors(), "pin_target_resolution", "root.ready") {
		t.Fatalf("root connect should satisfy root output pin target proof, got %#v", report.Errors())
	}
}

func TestRun_AllowsTemplateInstanceKeyCompositionConnectWithoutAddress(t *testing.T) {
	root := writeCompositionConnectBootverifyFixture(t, canonicalrouting.CompositionConnectTemplateInstance)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "") {
		t.Fatalf("unexpected composition_connect_validation error: %#v", report.Errors())
	}
	if reportContains(report.Errors(), "template_instance_validation", "") {
		t.Fatalf("unexpected template_instance_validation error: %#v", report.Errors())
	}
}

func TestRun_AllowsCreateInputResolutionCompositionConnect(t *testing.T) {
	root := writeCreateResolutionCompositionConnectFixture(t, createResolutionCompositionFixtureOptions{
		mode:         runtimecontracts.FlowInputResolutionModeCreate,
		mint:         runtimecontracts.FlowInputResolutionMintUUID,
		includeCarry: true,
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "") {
		t.Fatalf("unexpected composition_connect_validation error: %#v", report.Errors())
	}
	if reportContains(report.Errors(), "template_instance_validation", "") {
		t.Fatalf("unexpected template_instance_validation error: %#v", report.Errors())
	}
	if reportContains(report.Errors(), "input_pin_wiring", "validation.requested") {
		t.Fatalf("parent connect should satisfy create-resolution input pin wiring proof, got %#v", report.Errors())
	}
}

func TestCreateSyntheticCarryRejectsStaticallyAuthoredProducerCollision(t *testing.T) {
	root := canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{
		Mint:       canonicalrouting.CreateMintUUID,
		Invalidity: canonicalrouting.CreateResolutionProducerCollision,
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "composition_connect_validation", "producer producer-node emit field validation_case_id conflicts with receiver-owned carry instance.key.validation_case_id") {
		t.Fatalf("expected producer/synthetic carry collision blocker, got %#v", report.Errors())
	}
}

func TestRun_AllowsSelectInputResolutionCompositionConnect(t *testing.T) {
	root := writeSelectResolutionCompositionConnectFixture(t, selectResolutionCompositionFixtureOptions{})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "") {
		t.Fatalf("unexpected composition_connect_validation error: %#v", report.Errors())
	}
	if reportContains(report.Errors(), "template_instance_validation", "") {
		t.Fatalf("unexpected template_instance_validation error: %#v", report.Errors())
	}
	if reportContains(report.Errors(), "input_pin_wiring", "account.ready") {
		t.Fatalf("parent connect should satisfy select-resolution input pin wiring proof, got %#v", report.Errors())
	}
}

func TestRun_AllowsSelectOrCreateInputResolutionCompositionConnect(t *testing.T) {
	root := writeSelectResolutionCompositionConnectFixture(t, selectResolutionCompositionFixtureOptions{
		mode: runtimecontracts.FlowInputResolutionModeSelectOrCreate,
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "") {
		t.Fatalf("unexpected composition_connect_validation error: %#v", report.Errors())
	}
	if reportContains(report.Errors(), "template_instance_validation", "") {
		t.Fatalf("unexpected template_instance_validation error: %#v", report.Errors())
	}
	if reportContains(report.Errors(), "input_pin_wiring", "account.ready") {
		t.Fatalf("parent connect should satisfy select-or-create-resolution input pin wiring proof, got %#v", report.Errors())
	}
}

func TestRunRejectsProviderOnlyProjectionOptionsOnFlowInputCarries(t *testing.T) {
	root := writeSelectResolutionCompositionConnectFixture(t, selectResolutionCompositionFixtureOptions{})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
	pins := bundle.Semantics.FlowInputEventPins["account"]
	if len(pins) == 0 {
		t.Fatal("account input pin fixture is unavailable")
	}
	pin := &pins[0]
	for name, carry := range pin.Carries {
		carry.Optional = true
		carry.Convert = runtimecontracts.FieldProjectionConvertNumberToText
		pin.Carries[name] = carry
		break
	}
	bundle.Semantics.FlowInputEventPins["account"] = pins

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "composition_connect_validation", "reserved for provider normalized-event projections") {
		t.Fatalf("expected flow carry projection blocker, got %#v", report.Errors())
	}
}

func TestRun_FailsClosedForInvalidSelectInputResolution(t *testing.T) {
	tests := []struct {
		name string
		opts selectResolutionCompositionFixtureOptions
		want string
	}{
		{
			name: "undeclared carried key",
			opts: selectResolutionCompositionFixtureOptions{instanceKey: "missing_account_id"},
			want: "instance_key missing_account_id must name a declared carries.missing_account_id field",
		},
		{
			name: "composite receiver key",
			opts: selectResolutionCompositionFixtureOptions{compositeKey: true},
			want: "requires exactly one receiver instance.by field",
		},
		{
			name: "type mismatch",
			opts: selectResolutionCompositionFixtureOptions{carryType: "integer"},
			want: "key_types_incompatible",
		},
		{
			name: "legacy address is incompatible",
			opts: selectResolutionCompositionFixtureOptions{legacyAddress: true},
			want: "input pin resolution is incompatible with legacy address",
		},
		{
			name: "legacy connect using instance is incompatible",
			opts: selectResolutionCompositionFixtureOptions{usingInstance: true},
			want: "connect.using.instance is incompatible with input pin resolution",
		},
		{
			name: "legacy connect map is incompatible",
			opts: selectResolutionCompositionFixtureOptions{connectMap: true},
			want: "connect.map is incompatible with input pin resolution",
		},
		{
			name: "non-template receiver",
			opts: selectResolutionCompositionFixtureOptions{receiverMode: "static"},
			want: "INVALID-TEMPLATE-INSTANCE",
		},
		{
			name: "wrong delivery",
			opts: selectResolutionCompositionFixtureOptions{delivery: "many"},
			want: "requires delivery one",
		},
	}
	for _, mode := range []string{runtimecontracts.FlowInputResolutionModeSelect, runtimecontracts.FlowInputResolutionModeSelectOrCreate} {
		for _, tc := range tests {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				tc.opts.mode = mode
				root := writeSelectResolutionCompositionConnectFixture(t, tc.opts)
				bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

				report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

				if !reportContains(report.Errors(), "composition_connect_validation", tc.want) {
					t.Fatalf("expected composition_connect_validation %q, got %#v", tc.want, report.Errors())
				}
			})
		}
	}
}

func TestRun_FailsClosedForInvalidCreateInputResolution(t *testing.T) {
	tests := []struct {
		name string
		opts createResolutionCompositionFixtureOptions
		want string
	}{
		{
			name: "non-runnable modes are design-locked but not runnable",
			opts: createResolutionCompositionFixtureOptions{
				mode:         runtimecontracts.FlowInputResolutionModeFanOut,
				mint:         runtimecontracts.FlowInputResolutionMintUUID,
				includeCarry: true,
			},
			want: "instance_resolution_unimplemented",
		},
		{
			name: "invalid mint",
			opts: createResolutionCompositionFixtureOptions{
				mode:         runtimecontracts.FlowInputResolutionModeCreate,
				mint:         "random",
				includeCarry: true,
			},
			want: "mint \"random\" must be uuid or event_id",
		},
		{
			name: "missing carried instance key",
			opts: createResolutionCompositionFixtureOptions{
				mode: runtimecontracts.FlowInputResolutionModeCreate,
				mint: runtimecontracts.FlowInputResolutionMintUUID,
			},
			want: "must carry validation_case_id from instance.key.validation_case_id",
		},
		{
			name: "legacy connect using instance is incompatible",
			opts: createResolutionCompositionFixtureOptions{
				mode:          runtimecontracts.FlowInputResolutionModeCreate,
				mint:          runtimecontracts.FlowInputResolutionMintUUID,
				includeCarry:  true,
				usingInstance: true,
			},
			want: "connect.using.instance is incompatible with input pin resolution",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := writeCreateResolutionCompositionConnectFixture(t, tc.opts)
			bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if !reportContains(report.Errors(), "composition_connect_validation", tc.want) {
				t.Fatalf("expected composition_connect_validation %q, got %#v", tc.want, report.Errors())
			}
		})
	}
}

func TestRun_AllowsFanInStreamInputResolution(t *testing.T) {
	tests := []struct {
		name string
		opts templatefanin.Options
	}{
		{name: "payload field dedup", opts: templatefanin.Options{}},
		{name: "event id dedup", opts: templatefanin.Options{EventIDDedup: true}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := templatefanin.LoadSource(t, tc.opts)

			report := Run(context.Background(), source, Options{})

			if got := reportContains(report.Errors(), "composition_connect_validation", "fan-in"); got {
				t.Fatalf("fan-in stream fixture composition_connect_validation errors = %#v, want none", report.Errors())
			}
		})
	}
}

func TestRun_FailsClosedForInvalidFanInStreamInputResolution(t *testing.T) {
	tests := []struct {
		name string
		opts templatefanin.Options
		want string
	}{
		{name: "missing dedup", opts: templatefanin.Options{MissingDedup: true}, want: "requires dedup_by"},
		{name: "dedup tuple", opts: templatefanin.Options{DedupTuple: true}, want: "supports exactly one dedup_by field"},
		{name: "missing window", opts: templatefanin.Options{MissingWindow: true}, want: "requires window"},
		{name: "missing singleton", opts: templatefanin.Options{MissingSingleton: true}, want: "requires explicit singleton"},
		{name: "wrong singleton", opts: templatefanin.Options{WrongSingleton: true}, want: "must be the receiver singleton route or a child"},
		{name: "non-singleton receiver", opts: templatefanin.Options{NonSingletonReceiver: true}, want: "is not mode: singleton"},
		{name: "missing receiver handler", opts: templatefanin.Options{MissingReceiverHandler: true}, want: "has no handler for fan-in input event operating.reported"},
		{name: "missing accumulate", opts: templatefanin.Options{MissingAccumulate: true}, want: "for fan-in input must declare accumulate"},
		{name: "accumulator dedup redeclaration", opts: templatefanin.Options{AccumulateDedupMismatch: true}, want: "accumulate.dedup_by \"payload.period_id\" must not redeclare fan-in dedup_by"},
		{name: "accumulator window redeclaration", opts: templatefanin.Options{AccumulateWindowMismatch: true}, want: "accumulate.window \"payload.operating_id\" must not redeclare fan-in window"},
		{name: "wrong delivery", opts: templatefanin.Options{DeliveryMany: true}, want: "requires delivery one"},
		{name: "legacy map", opts: templatefanin.Options{LegacyConnectMap: true}, want: "connect.map is incompatible with input pin resolution"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := templatefanin.LoadSource(t, tc.opts)

			report := Run(context.Background(), source, Options{})

			if !reportContains(report.Errors(), "composition_connect_validation", tc.want) {
				t.Fatalf("expected fan-in composition_connect_validation %q, got %#v", tc.want, report.Errors())
			}
		})
	}
}

func TestFanInBarrierCanonicalBundlePassesStrictVerify(t *testing.T) {
	bundle := loadFanInBarrierBundle(t)
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if findings := report.HardInvalidities(); len(findings) != 0 {
		t.Fatalf("canonical fan-in barrier hard invalidities: %#v", findings)
	}
}

func TestFanInBarrierRejectsAuthoredDerivedJoinFields(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mutation canonicalrouting.FanInNegativeMutation
		want     string
	}{
		{name: "members by", mutation: canonicalrouting.FanInAuthoredMembersBy, want: "join.members.by derives from resolution.dedup_by (payload.operating_id); remove authored by"},
		{name: "window by", mutation: canonicalrouting.FanInAuthoredWindowBy, want: "join.window.by derives from resolution.window (payload.period_id); remove authored by"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := runFanInBarrierMutation(t, tc.mutation)
			if !reportContains(report.Errors(), "composition_connect_validation", tc.want) {
				t.Fatalf("expected teaching diagnostic %q, got %#v", tc.want, report.Errors())
			}
		})
	}
}

func TestFanInBarrierRequiresExactlyOneJoinRow(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mutation canonicalrouting.FanInNegativeMutation
		want     []string
	}{
		{
			name:     "zero matches",
			mutation: canonicalrouting.FanInMissingJoinRow,
			want:     []string{"requires exactly one handler.join row", "members.from, output, on_complete, and timeout"},
		},
		{
			name:     "multiple matches",
			mutation: canonicalrouting.FanInMultipleJoinRows,
			want:     []string{"matches multiple join rows", "portfolio-collector-duplicate.operating.reported", "use distinct events or distinct stages per join"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := runFanInBarrierMutation(t, tc.mutation)
			for _, want := range tc.want {
				if !reportContains(report.Errors(), "composition_connect_validation", want) {
					t.Fatalf("expected exact-association diagnostic %q, got %#v", want, report.Errors())
				}
			}
		})
	}
}

func TestFanInBarrierRequiresSinglePayloadMemberIdentity(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mutation canonicalrouting.FanInNegativeMutation
		want     string
	}{
		{name: "event identity", mutation: canonicalrouting.FanInEventIDDedup, want: "event.id cannot appear in expected members"},
		{name: "composite identity", mutation: canonicalrouting.FanInDedupTuple, want: "supports exactly one dedup_by field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := runFanInBarrierMutation(t, tc.mutation)
			if !reportContains(report.Errors(), "composition_connect_validation", tc.want) {
				t.Fatalf("expected member-identity diagnostic %q, got %#v", tc.want, report.Errors())
			}
		})
	}
}

func TestFanInBarrierWindowRequirementUsesStageReentrancy(t *testing.T) {
	t.Run("non reentrant stage may omit window", func(t *testing.T) {
		report := runFanInBarrierMutation(t, canonicalrouting.FanInBarrierNoWindow)
		if findings := report.HardInvalidities(); len(findings) != 0 {
			t.Fatalf("non-reentrant barrier without window hard invalidities: %#v", findings)
		}
	})
	t.Run("reentrant stage requires window", func(t *testing.T) {
		report := runFanInBarrierMutation(t, canonicalrouting.FanInBarrierReentrantNoWindow)
		for _, want := range []string{"add resolution.window", "make the stage provably non-reentrant"} {
			if !reportContains(report.Errors(), "join_validation", want) {
				t.Fatalf("expected reentrancy remediation %q, got %#v", want, report.Errors())
			}
		}
	})
}

func TestFanInAggregationSelectsExactlyOneRuntimeOwner(t *testing.T) {
	t.Run("stream requires accumulator", func(t *testing.T) {
		source := templatefanin.LoadSource(t, templatefanin.Options{MissingAccumulate: true})
		report := Run(context.Background(), source, Options{})
		if !reportContains(report.Errors(), "composition_connect_validation", "must declare accumulate") {
			t.Fatalf("stream fan-in accepted without accumulator: %#v", report.Errors())
		}
	})
	t.Run("barrier rejects accumulator", func(t *testing.T) {
		report := runFanInBarrierMutation(t, canonicalrouting.FanInBarrierWithAccumulate)
		if !reportContains(report.Errors(), "composition_connect_validation", "use handler.join as the sole finite-barrier owner") {
			t.Fatalf("barrier fan-in accepted accumulator: %#v", report.Errors())
		}
	})
}

func loadFanInBarrierBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := repoRootForBootverifyTest(t)
	return loadFixtureBundleAt(t, repoRoot, canonicalrouting.ExampleRoot(t, canonicalrouting.FanInBarrier), runtimecontracts.DefaultPlatformSpecFile(repoRoot))
}

func runFanInBarrierMutation(t *testing.T, mutation canonicalrouting.FanInNegativeMutation) Report {
	t.Helper()
	repoRoot := repoRootForBootverifyTest(t)
	root := canonicalrouting.CopyExample(t, canonicalrouting.FanInBarrier)
	if mutation == canonicalrouting.FanInMultipleJoinRows {
		bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
		applyFanInMultipleJoinRows(t, bundle)
		return Run(context.Background(), semanticview.Wrap(bundle), Options{})
	}
	canonicalrouting.ApplyFanInNegativeMutation(t, root, mutation)
	bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	return Run(context.Background(), semanticview.Wrap(bundle), Options{})
}

func applyFanInMultipleJoinRows(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) {
	t.Helper()
	if bundle == nil {
		t.Fatal("fan-in semantic mutation requires a bundle")
	}
	flow, ok := bundle.FlowViewByID("portfolio")
	if !ok || flow == nil {
		t.Fatal("canonical portfolio flow is unavailable")
	}
	const sourceNodeID = "portfolio-collector"
	const duplicateNodeID = "portfolio-collector-duplicate"
	node, ok := flow.Nodes[sourceNodeID]
	if !ok {
		t.Fatal("canonical portfolio collector is unavailable")
	}
	node.ID = duplicateNodeID
	flow.Nodes[duplicateNodeID] = node
	if bundle.Nodes == nil {
		bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{}
	}
	bundle.Nodes[duplicateNodeID] = node
	if bundle.Semantics.NodeHandlers == nil {
		bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	bundle.Semantics.NodeHandlers[duplicateNodeID] = node.EventHandlers
	if bundle.Semantics.EffectiveNodes == nil {
		bundle.Semantics.EffectiveNodes = map[string]runtimecontracts.SystemNodeEffectiveSemantics{}
	}
	bundle.Semantics.EffectiveNodes[duplicateNodeID] = runtimecontracts.SystemNodeEffectiveSemantics{
		ID:                   duplicateNodeID,
		RuntimeSubscriptions: runtimecontracts.EffectiveSystemNodeSubscriptions(node),
	}
	for _, plan := range bundle.Semantics.Joins {
		if plan.FlowID == "portfolio" && plan.NodeID == sourceNodeID && plan.HandlerEvent == "operating.reported" {
			plan.NodeID = duplicateNodeID
			bundle.Semantics.Joins = append(bundle.Semantics.Joins, plan)
			return
		}
	}
	t.Fatal("canonical portfolio join plan is unavailable")
}

func TestRun_AllowsCompositeTemplateInstanceKeyCompositionConnectWithoutAddress(t *testing.T) {
	root := writeCompositionConnectBootverifyFixture(t, canonicalrouting.CompositionConnectCompositeTemplateInstance)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "") {
		t.Fatalf("unexpected composition_connect_validation error: %#v", report.Errors())
	}
	if reportContains(report.Errors(), "output_pin_key_carries_validation", "") {
		t.Fatalf("unexpected output_pin_key_carries_validation error: %#v", report.Errors())
	}
	if reportContains(report.Errors(), "template_instance_validation", "") {
		t.Fatalf("unexpected template_instance_validation error: %#v", report.Errors())
	}
}

func TestRun_AllowsRenamedTemplateInstanceKeyCompositionConnectWithUsingInstance(t *testing.T) {
	tests := []struct {
		name    string
		variant canonicalrouting.CompositionConnectAdapterVariant
	}{
		{
			name:    "scalar renamed key",
			variant: canonicalrouting.CompositionAdapterScalar,
		},
		{
			name:    "complete composite renamed key",
			variant: canonicalrouting.CompositionAdapterComposite,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := writeCompositionConnectAdapterBootverifyFixture(t, tc.variant)
			bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if reportContains(report.Errors(), "composition_connect_validation", "") {
				t.Fatalf("unexpected composition_connect_validation error: %#v", report.Errors())
			}
			if reportContains(report.Errors(), "template_instance_validation", "") {
				t.Fatalf("unexpected template_instance_validation error: %#v", report.Errors())
			}
			if reportContains(report.Errors(), "output_pin_key_carries_validation", "") {
				t.Fatalf("unexpected output_pin_key_carries_validation error: %#v", report.Errors())
			}
		})
	}
}

func TestRun_FailsClosedForInvalidRenamedTemplateInstanceKeyAdapters(t *testing.T) {
	tests := []struct {
		name      string
		variant   canonicalrouting.CompositionConnectAdapterVariant
		want      string
		wantExtra string
	}{
		{name: "missing adapter for renamed key", variant: canonicalrouting.CompositionAdapterMissing, want: "instance_key_mismatch"},
		{name: "missing source", variant: canonicalrouting.CompositionAdapterMissingSource, want: "connect_key_adapter_missing_source"},
		{name: "missing target", variant: canonicalrouting.CompositionAdapterMissingTarget, want: "connect_key_adapter_missing_target"},
		{name: "source not carried", variant: canonicalrouting.CompositionAdapterSourceNotCarried, want: "connect_key_adapter_source_missing", wantExtra: "missing_region"},
		{name: "target not instance key", variant: canonicalrouting.CompositionAdapterTargetNotKey, want: "connect_key_adapter_target_missing", wantExtra: "missing_region"},
		{name: "partial composite mapping", variant: canonicalrouting.CompositionAdapterPartial, want: "connect_key_adapter_partial"},
		{name: "source target cardinality mismatch", variant: canonicalrouting.CompositionAdapterCardinality, want: "connect_key_adapter_cardinality"},
		{name: "duplicate source mapping", variant: canonicalrouting.CompositionAdapterDuplicateSource, want: "connect_key_adapter_duplicate_source"},
		{name: "duplicate target mapping", variant: canonicalrouting.CompositionAdapterDuplicateTarget, want: "connect_key_adapter_duplicate_target"},
		{name: "incompatible adapter types", variant: canonicalrouting.CompositionAdapterIncompatibleTypes, want: "key_types_incompatible"},
		{name: "old connect map remains invalid for addressless template receiver", variant: canonicalrouting.CompositionAdapterLegacyMap, want: "connect_key_adapter_unsupported"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := writeCompositionConnectAdapterBootverifyFixture(t, tc.variant)
			bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if !reportContains(report.Errors(), "composition_connect_validation", tc.want) {
				t.Fatalf("expected composition_connect_validation %q, got %#v", tc.want, report.Errors())
			}
			if tc.wantExtra != "" && !reportContains(report.Errors(), "composition_connect_validation", tc.wantExtra) {
				t.Fatalf("expected composition_connect_validation detail %q, got %#v", tc.wantExtra, report.Errors())
			}
		})
	}
}

func TestRun_FailsClosedForInvalidParentCompositionConnect(t *testing.T) {
	tests := []struct {
		name      string
		variant   canonicalrouting.CompositionConnectVariant
		want      string
		wantExtra string
	}{
		{name: "missing producer flow", variant: canonicalrouting.CompositionConnectMissingProducerFlow, want: "producer_flow_missing"},
		{name: "missing producer output pin", variant: canonicalrouting.CompositionConnectMissingProducerPin, want: "producer_output_pin_missing"},
		{name: "missing receiver flow", variant: canonicalrouting.CompositionConnectMissingReceiverFlow, want: "receiver_flow_missing"},
		{name: "missing receiver input pin", variant: canonicalrouting.CompositionConnectMissingReceiverPin, want: "receiver_input_pin_missing"},
		{name: "root receiver unsupported", variant: canonicalrouting.CompositionConnectRootReceiver, want: "receiver_root_unsupported"},
		{name: "event names differ without adapter", variant: canonicalrouting.CompositionConnectMissingAdapter, want: "event_alias_or_adapter_invalid"},
		{name: "missing address key", variant: canonicalrouting.CompositionConnectMissingAddressKey, want: "output_carries_address_key", wantExtra: "missing_vertical_id"},
		{name: "incompatible address key types", variant: canonicalrouting.CompositionConnectIncompatibleKeyType, want: "key_types_incompatible"},
		{name: "unindexed business-field target", variant: canonicalrouting.CompositionConnectUnindexedTarget, want: "receiver_address_rule_invalid", wantExtra: "indexed: true"},
		{name: "nested business-field target", variant: canonicalrouting.CompositionConnectNestedTarget, want: "receiver_address_rule_invalid", wantExtra: "top-level indexed entity fields"},
		{name: "invalid delivery topology", variant: canonicalrouting.CompositionConnectDeliveryMany, want: "delivery_topology_invalid"},
		{name: "broadcast delivery with singular address", variant: canonicalrouting.CompositionConnectDeliveryBroadcast, want: "delivery_topology_invalid", wantExtra: "broadcast"},
		{name: "missing reply lineage", variant: canonicalrouting.CompositionConnectDeliveryReply, want: "reply_lineage_missing"},
		{name: "template receiver missing address rule", variant: canonicalrouting.CompositionConnectTemplateMissingAddress, want: "receiver_instance_key_invalid"},
		{name: "template instance key route rejects renamed connect map", variant: canonicalrouting.CompositionConnectTemplateLegacyMap, want: "connect_key_adapter_unsupported"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := writeCompositionConnectBootverifyFixture(t, tc.variant)
			bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if !reportContains(report.Errors(), "composition_connect_validation", tc.want) {
				t.Fatalf("expected composition_connect_validation %q, got %#v", tc.want, report.Errors())
			}
			if tc.wantExtra != "" && !reportContains(report.Errors(), "composition_connect_validation", tc.wantExtra) {
				t.Fatalf("expected composition_connect_validation detail %q, got %#v", tc.wantExtra, report.Errors())
			}
		})
	}
}

func TestRun_FailsClosedForInvalidOutputPinKeyCarriesEvidence(t *testing.T) {
	tests := []struct {
		name    string
		variant canonicalrouting.CompositionConnectVariant
		want    string
	}{
		{name: "connected output missing key", variant: canonicalrouting.CompositionConnectMissingOutputKey, want: "missing_key"},
		{name: "connected output missing carries", variant: canonicalrouting.CompositionConnectMissingOutputCarries, want: "missing_carries"},
		{name: "key not listed in carries", variant: canonicalrouting.CompositionConnectKeyNotCarried, want: "key_not_carried"},
		{name: "duplicate carried field", variant: canonicalrouting.CompositionConnectDuplicateCarry, want: "duplicate_carry_field"},
		{name: "ambiguous output key", variant: canonicalrouting.CompositionConnectAmbiguousOutputKey, want: "ambiguous_output_key"},
		{name: "declared key missing from event payload schema", variant: canonicalrouting.CompositionConnectMissingPayloadKey, want: "does not declare payload field component_id"},
		{name: "declared key is not scalar", variant: canonicalrouting.CompositionConnectNonScalarKey, want: "not a scalar key type"},
		{name: "node emit does not prove carried field", variant: canonicalrouting.CompositionConnectEmitMissingKey, want: "emit_payload_missing_key"},
		{name: "agent emit_events cannot prove carried field", variant: canonicalrouting.CompositionConnectAgentEmitUnproven, want: "agent_emit_payload_unproven"},
		{name: "auto_emit_on_create cannot prove carried field", variant: canonicalrouting.CompositionConnectAutoEmitUnproven, want: "auto_emit_payload_unproven"},
		{name: "workflow timer cannot prove carried field", variant: canonicalrouting.CompositionConnectTimerUnproven, want: "timer_payload_unproven"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := writeCompositionConnectBootverifyFixture(t, tc.variant)
			bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if !reportContains(report.Errors(), "output_pin_key_carries_validation", tc.want) {
				t.Fatalf("expected output_pin_key_carries_validation %q, got %#v", tc.want, report.Errors())
			}
		})
	}
}

func TestRun_FailsClosedForRootAutoEmitOutputPinKeyCarriesEvidence(t *testing.T) {
	root := writeRootAutoEmitOutputPinKeyCarriesFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "output_pin_key_carries_validation", "auto_emit_payload_unproven") {
		t.Fatalf("expected root auto_emit output_pin_key_carries_validation error, got %#v", report.Errors())
	}
}

func TestOutputPinKeyCarriesPinsForEventIgnoresPublicPinName(t *testing.T) {
	root := writeCompositionConnectBootverifyFixture(t, canonicalrouting.CompositionConnectValid)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
	source := semanticview.Wrap(bundle)

	if got := outputPinKeyCarriesPinsForEvent(source, "producer", "deploy_done"); len(got) != 0 {
		t.Fatalf("output pins for public pin name deploy_done = %#v, want none", got)
	}
	if got := outputPinKeyCarriesPinsForEvent(source, "producer", "deploy.done"); len(got) != 1 || got[0].PinName() != "deploy_done" {
		t.Fatalf("output pins for emitted event deploy.done = %#v, want deploy_done pin", got)
	}
}

func TestRun_AllowsImportBoundaryAliasAsConnectEventAdapter(t *testing.T) {
	root := writeCompositionConnectBootverifyFixture(t, canonicalrouting.CompositionConnectInputAlias)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "event_alias_or_adapter_invalid") {
		t.Fatalf("import-boundary alias should satisfy connect event adaptation, got %#v", report.Errors())
	}
}

func TestRun_AllowsOutputBoundaryAliasAsConnectEventAdapter(t *testing.T) {
	root := writeCompositionConnectBootverifyFixture(t, canonicalrouting.CompositionConnectOutputAlias)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "event_alias_or_adapter_invalid") {
		t.Fatalf("producer import-boundary alias should satisfy connect event adaptation, got %#v", report.Errors())
	}
}

func TestRun_AllowsParentCompositionConnectAsCrossFlowAmbiguityProof(t *testing.T) {
	root := writeCompositionConnectAmbiguityFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "") {
		t.Fatalf("unexpected composition_connect_validation error: %#v", report.Errors())
	}
	if reportContains(report.Errors(), "cross_flow_pin_ambiguity_validation", "ticket.ready") {
		t.Fatalf("parent connect should disambiguate cross-flow input pin, got %#v", report.Errors())
	}
}

func TestRun_TreatsParentCompositionConnectAsEventTopologyProof(t *testing.T) {
	root := writeCompositionConnectTopologyFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "") {
		t.Fatalf("unexpected composition_connect_validation error: %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "event_producer_exists", "consumer/deploy.completed") {
		t.Fatalf("parent connect should prove receiver input has a producer, got %#v", report.Warnings())
	}
	if reportContains(report.Warnings(), "event_consumer_exists", "producer/deploy.done") {
		t.Fatalf("parent connect should prove producer output has a consumer, got %#v", report.Warnings())
	}
	for _, eventRef := range []string{"producer/deploy.done", "consumer/deploy.completed"} {
		if reportContains(report.Warnings(), "semantic_drift_dead_event_schema", eventRef) {
			t.Fatalf("parent connect should mark %s as active, got %#v", eventRef, report.Warnings())
		}
	}
}

func writeCompositionConnectBootverifyFixture(t *testing.T, variant canonicalrouting.CompositionConnectVariant) string {
	t.Helper()
	return canonicalrouting.CopyCompositionConnect(t, variant)
}

func writeCompositionConnectAdapterBootverifyFixture(t *testing.T, variant canonicalrouting.CompositionConnectAdapterVariant) string {
	t.Helper()
	return canonicalrouting.CopyCompositionConnectAdapter(t, variant)
}

func writeCompositionConnectTopologyFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyCompositionConnectTopology(t)
}

func writeCompositionConnectAmbiguityFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyCompositionConnectAmbiguity(t)
}

func writeRootCompositionConnectBootverifyFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyRootOutputConnect(t, canonicalrouting.RootConnectCanonicalEmit)
}

func writeRootAutoEmitOutputPinKeyCarriesFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyRootAutoEmitKeyCarries(t)
}

type createResolutionCompositionFixtureOptions struct {
	mode          string
	mint          string
	includeCarry  bool
	usingInstance bool
}

type selectResolutionCompositionFixtureOptions struct {
	mode          string
	instanceKey   string
	carryType     string
	receiverMode  string
	delivery      string
	compositeKey  bool
	legacyAddress bool
	usingInstance bool
	connectMap    bool
}

func writeSelectResolutionCompositionConnectFixture(t *testing.T, opts selectResolutionCompositionFixtureOptions) string {
	t.Helper()
	mode := canonicalrouting.SelectResolutionSelect
	if strings.TrimSpace(opts.mode) == runtimecontracts.FlowInputResolutionModeSelectOrCreate {
		mode = canonicalrouting.SelectResolutionSelectOrCreate
	}
	invalidity := canonicalrouting.SelectResolutionValid
	switch {
	case strings.TrimSpace(opts.instanceKey) == "missing_account_id":
		invalidity = canonicalrouting.SelectResolutionUndeclaredCarry
	case opts.compositeKey:
		invalidity = canonicalrouting.SelectResolutionCompositeKey
	case strings.TrimSpace(opts.carryType) == "integer":
		invalidity = canonicalrouting.SelectResolutionCarryTypeMismatch
	case opts.legacyAddress:
		invalidity = canonicalrouting.SelectResolutionLegacyAddress
	case opts.usingInstance:
		invalidity = canonicalrouting.SelectResolutionLegacyUsingInstance
	case opts.connectMap:
		invalidity = canonicalrouting.SelectResolutionLegacyConnectMap
	case strings.TrimSpace(opts.receiverMode) == "static":
		invalidity = canonicalrouting.SelectResolutionStaticReceiver
	case strings.TrimSpace(opts.delivery) == "many":
		invalidity = canonicalrouting.SelectResolutionManyDelivery
	}
	return canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{Mode: mode, Invalidity: invalidity})
}

func writeCreateResolutionCompositionConnectFixture(t *testing.T, opts createResolutionCompositionFixtureOptions) string {
	t.Helper()
	invalidity := canonicalrouting.CreateResolutionValid
	switch {
	case opts.mode == runtimecontracts.FlowInputResolutionModeFanOut:
		invalidity = canonicalrouting.CreateResolutionNonRunnableMode
	case opts.mint == "random":
		invalidity = canonicalrouting.CreateResolutionInvalidMint
	case !opts.includeCarry:
		invalidity = canonicalrouting.CreateResolutionMissingCarry
	case opts.usingInstance:
		invalidity = canonicalrouting.CreateResolutionLegacyUsingInstance
	}
	return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{
		Mint:       canonicalrouting.CreateMintUUID,
		Invalidity: invalidity,
	})
}
