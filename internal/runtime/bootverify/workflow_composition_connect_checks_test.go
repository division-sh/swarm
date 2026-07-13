package bootverify

import (
	"context"
	"path/filepath"
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
	root := writeCompositionConnectBootverifyFixture(t, compositionConnectFixtureOptions{
		consumerMode:             "template",
		consumerScalarInput:      true,
		consumerTemplateInstance: true,
		connectTo:                "consumer.deploy.completed",
		omitMap:                  true,
	})
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
		{name: "barrier remains unrunnable", opts: templatefanin.Options{BarrierAggregation: true}, want: "supports only aggregation: stream"},
		{name: "missing singleton", opts: templatefanin.Options{MissingSingleton: true}, want: "requires explicit singleton"},
		{name: "wrong singleton", opts: templatefanin.Options{WrongSingleton: true}, want: "must be the receiver singleton route or a child"},
		{name: "non-singleton receiver", opts: templatefanin.Options{NonSingletonReceiver: true}, want: "is not mode: singleton"},
		{name: "missing receiver handler", opts: templatefanin.Options{MissingReceiverHandler: true}, want: "has no handler for fan-in input event operating.reported"},
		{name: "missing accumulate", opts: templatefanin.Options{MissingAccumulate: true}, want: "for fan-in input must declare accumulate"},
		{name: "accumulator dedup redeclaration", opts: templatefanin.Options{AccumulateDedupMismatch: true}, want: "accumulate.dedup_by \"payload.operating_id\" must not redeclare fan-in dedup_by"},
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

func TestRun_AllowsCompositeTemplateInstanceKeyCompositionConnectWithoutAddress(t *testing.T) {
	root := writeCompositionConnectBootverifyFixture(t, compositionConnectFixtureOptions{
		consumerMode:                   "template",
		consumerScalarInput:            true,
		consumerTemplateInstance:       true,
		consumerTemplateInstanceBy:     "[vertical_id, region]",
		consumerTemplateInstanceRegion: true,
		producerOutputCarries:          "[vertical_id, region]",
		connectTo:                      "consumer.deploy.completed",
		omitMap:                        true,
	})
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
		name string
		opts compositionConnectAdapterFixtureOptions
	}{
		{
			name: "scalar renamed key",
			opts: compositionConnectAdapterFixtureOptions{
				sourceFields:       []compositionConnectAdapterField{{Name: "source_vertical_id", Type: "string"}},
				outputKey:          "source_vertical_id",
				outputCarries:      "[source_vertical_id]",
				receiverInstanceBy: "vertical_id",
				receiverFields:     []compositionConnectAdapterField{{Name: "vertical_id", Type: "string"}},
				usingSource:        "source_vertical_id",
				usingTarget:        "vertical_id",
			},
		},
		{
			name: "complete composite renamed key",
			opts: compositionConnectAdapterFixtureOptions{
				sourceFields: []compositionConnectAdapterField{
					{Name: "source_vertical_id", Type: "string"},
					{Name: "region_code", Type: "string"},
				},
				outputKey:          "source_vertical_id",
				outputCarries:      "[source_vertical_id, region_code]",
				receiverInstanceBy: "[vertical_id, region]",
				receiverFields: []compositionConnectAdapterField{
					{Name: "vertical_id", Type: "string"},
					{Name: "region", Type: "string"},
				},
				usingSource: "[source_vertical_id, region_code]",
				usingTarget: "[vertical_id, region]",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := writeCompositionConnectAdapterBootverifyFixture(t, tc.opts)
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
	base := compositionConnectAdapterFixtureOptions{
		sourceFields: []compositionConnectAdapterField{
			{Name: "source_vertical_id", Type: "string"},
			{Name: "region_code", Type: "string"},
		},
		outputKey:          "source_vertical_id",
		outputCarries:      "[source_vertical_id, region_code]",
		receiverInstanceBy: "[vertical_id, region]",
		receiverFields: []compositionConnectAdapterField{
			{Name: "vertical_id", Type: "string"},
			{Name: "region", Type: "string"},
		},
		usingSource: "[source_vertical_id, region_code]",
		usingTarget: "[vertical_id, region]",
	}
	tests := []struct {
		name      string
		mutate    func(*compositionConnectAdapterFixtureOptions)
		want      string
		wantExtra string
	}{
		{
			name: "missing adapter for renamed key",
			mutate: func(o *compositionConnectAdapterFixtureOptions) {
				o.omitUsing = true
			},
			want: "instance_key_mismatch",
		},
		{
			name: "missing source",
			mutate: func(o *compositionConnectAdapterFixtureOptions) {
				o.usingSource = ""
			},
			want: "connect_key_adapter_missing_source",
		},
		{
			name: "missing target",
			mutate: func(o *compositionConnectAdapterFixtureOptions) {
				o.usingTarget = ""
			},
			want: "connect_key_adapter_missing_target",
		},
		{
			name: "source not carried",
			mutate: func(o *compositionConnectAdapterFixtureOptions) {
				o.usingSource = "[source_vertical_id, missing_region]"
			},
			want:      "connect_key_adapter_source_missing",
			wantExtra: "missing_region",
		},
		{
			name: "target not instance key",
			mutate: func(o *compositionConnectAdapterFixtureOptions) {
				o.usingTarget = "[vertical_id, missing_region]"
			},
			want:      "connect_key_adapter_target_missing",
			wantExtra: "missing_region",
		},
		{
			name: "partial composite mapping",
			mutate: func(o *compositionConnectAdapterFixtureOptions) {
				o.usingSource = "source_vertical_id"
				o.usingTarget = "vertical_id"
			},
			want: "connect_key_adapter_partial",
		},
		{
			name: "source target cardinality mismatch",
			mutate: func(o *compositionConnectAdapterFixtureOptions) {
				o.usingSource = "[source_vertical_id, region_code]"
				o.usingTarget = "vertical_id"
			},
			want: "connect_key_adapter_cardinality",
		},
		{
			name: "duplicate source mapping",
			mutate: func(o *compositionConnectAdapterFixtureOptions) {
				o.usingSource = "[source_vertical_id, source_vertical_id]"
			},
			want: "connect_key_adapter_duplicate_source",
		},
		{
			name: "duplicate target mapping",
			mutate: func(o *compositionConnectAdapterFixtureOptions) {
				o.usingTarget = "[vertical_id, vertical_id]"
			},
			want: "connect_key_adapter_duplicate_target",
		},
		{
			name: "incompatible adapter types",
			mutate: func(o *compositionConnectAdapterFixtureOptions) {
				o.sourceFields[0].Type = "integer"
			},
			want: "key_types_incompatible",
		},
		{
			name: "old connect map remains invalid for addressless template receiver",
			mutate: func(o *compositionConnectAdapterFixtureOptions) {
				o.omitUsing = true
				o.includeLegacyMap = true
			},
			want: "connect_key_adapter_unsupported",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := base.clone()
			tc.mutate(&opts)
			root := writeCompositionConnectAdapterBootverifyFixture(t, opts)
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
		opts      compositionConnectFixtureOptions
		want      string
		wantExtra string
	}{
		{
			name: "missing producer flow",
			opts: compositionConnectFixtureOptions{
				connectFrom: "missing.deploy_done",
			},
			want: "producer_flow_missing",
		},
		{
			name: "missing producer output pin",
			opts: compositionConnectFixtureOptions{
				connectFrom: "producer.missing_pin",
			},
			want: "producer_output_pin_missing",
		},
		{
			name: "missing receiver flow",
			opts: compositionConnectFixtureOptions{
				connectTo: "missing.deploy_completed",
			},
			want: "receiver_flow_missing",
		},
		{
			name: "missing receiver input pin",
			opts: compositionConnectFixtureOptions{
				connectTo: "consumer.missing_pin",
			},
			want: "receiver_input_pin_missing",
		},
		{
			name: "root receiver unsupported",
			opts: compositionConnectFixtureOptions{
				connectTo: ".deploy_completed",
			},
			want: "receiver_root_unsupported",
		},
		{
			name: "event names differ without adapter",
			opts: compositionConnectFixtureOptions{
				noAdapter: true,
			},
			want: "event_alias_or_adapter_invalid",
		},
		{
			name: "missing address key",
			opts: compositionConnectFixtureOptions{
				mapSource: "payload.missing_vertical_id",
			},
			want:      "output_carries_address_key",
			wantExtra: "missing_vertical_id",
		},
		{
			name: "incompatible address key types",
			opts: compositionConnectFixtureOptions{
				consumerEntityType: "integer",
			},
			want: "key_types_incompatible",
		},
		{
			name: "unindexed business-field target",
			opts: compositionConnectFixtureOptions{
				consumerEntityUnindexed: true,
			},
			want:      "receiver_address_rule_invalid",
			wantExtra: "indexed: true",
		},
		{
			name: "nested business-field target",
			opts: compositionConnectFixtureOptions{
				mapTarget: "entity.profile.vertical_id",
			},
			want:      "receiver_address_rule_invalid",
			wantExtra: "top-level indexed entity fields",
		},
		{
			name: "invalid delivery topology",
			opts: compositionConnectFixtureOptions{
				delivery: "many",
			},
			want: "delivery_topology_invalid",
		},
		{
			name: "broadcast delivery with singular address",
			opts: compositionConnectFixtureOptions{
				delivery: "broadcast",
			},
			want:      "delivery_topology_invalid",
			wantExtra: "broadcast",
		},
		{
			name: "missing reply lineage",
			opts: compositionConnectFixtureOptions{
				delivery: "reply",
			},
			want: "reply_lineage_missing",
		},
		{
			name: "template receiver missing address rule",
			opts: compositionConnectFixtureOptions{
				consumerMode:        "template",
				consumerScalarInput: true,
				connectTo:           "consumer.deploy.completed",
				omitMap:             true,
			},
			want: "receiver_instance_key_invalid",
		},
		{
			name: "template instance key route rejects renamed connect map",
			opts: compositionConnectFixtureOptions{
				consumerMode:             "template",
				consumerScalarInput:      true,
				consumerTemplateInstance: true,
				connectTo:                "consumer.deploy.completed",
			},
			want: "connect_key_adapter_unsupported",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := writeCompositionConnectBootverifyFixture(t, tc.opts)
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
		name string
		opts compositionConnectFixtureOptions
		want string
	}{
		{
			name: "connected output missing key",
			opts: compositionConnectFixtureOptions{omitProducerOutputKey: true},
			want: "missing_key",
		},
		{
			name: "connected output missing carries",
			opts: compositionConnectFixtureOptions{omitProducerOutputCarries: true},
			want: "missing_carries",
		},
		{
			name: "key not listed in carries",
			opts: compositionConnectFixtureOptions{producerOutputCarries: "[component_id]"},
			want: "key_not_carried",
		},
		{
			name: "duplicate carried field",
			opts: compositionConnectFixtureOptions{producerOutputCarries: "[vertical_id, vertical_id]"},
			want: "duplicate_carry_field",
		},
		{
			name: "ambiguous output key",
			opts: compositionConnectFixtureOptions{duplicateProducerOutputKey: true},
			want: "ambiguous_output_key",
		},
		{
			name: "declared key missing from event payload schema",
			opts: compositionConnectFixtureOptions{
				producerOutputKey:     "component_id",
				producerOutputCarries: "[component_id]",
				mapSource:             "payload.component_id",
			},
			want: "does not declare payload field component_id",
		},
		{
			name: "declared key is not scalar",
			opts: compositionConnectFixtureOptions{producerVerticalIDType: "[string]"},
			want: "not a scalar key type",
		},
		{
			name: "node emit does not prove carried field",
			opts: compositionConnectFixtureOptions{omitProducerEmitField: true},
			want: "emit_payload_missing_key",
		},
		{
			name: "agent emit_events cannot prove carried field",
			opts: compositionConnectFixtureOptions{producerAgentEmit: true},
			want: "agent_emit_payload_unproven",
		},
		{
			name: "auto_emit_on_create cannot prove carried field",
			opts: compositionConnectFixtureOptions{producerAutoEmit: true},
			want: "auto_emit_payload_unproven",
		},
		{
			name: "workflow timer cannot prove carried field",
			opts: compositionConnectFixtureOptions{producerTimer: true},
			want: "timer_payload_unproven",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := writeCompositionConnectBootverifyFixture(t, tc.opts)
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
	root := writeCompositionConnectBootverifyFixture(t, compositionConnectFixtureOptions{})
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
	root := writeCompositionConnectBootverifyFixture(t, compositionConnectFixtureOptions{
		consumerRequiresInput: true,
		consumerInputBind:     "deploy.done",
		consumerScalarInput:   true,
		connectTo:             "consumer.deploy.completed",
		noAdapter:             true,
		omitMap:               true,
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "event_alias_or_adapter_invalid") {
		t.Fatalf("import-boundary alias should satisfy connect event adaptation, got %#v", report.Errors())
	}
}

func TestRun_AllowsOutputBoundaryAliasAsConnectEventAdapter(t *testing.T) {
	root := writeCompositionConnectBootverifyFixture(t, compositionConnectFixtureOptions{
		producerRequiresOutput: true,
		producerOutputBind:     "deploy.completed",
		consumerScalarInput:    true,
		noAdapter:              true,
		omitMap:                true,
	})
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

type compositionConnectFixtureOptions struct {
	connectFrom                    string
	connectTo                      string
	delivery                       string
	noAdapter                      bool
	mapSource                      string
	mapTarget                      string
	omitMap                        bool
	consumerMode                   string
	consumerScalarInput            bool
	consumerEntityType             string
	consumerEntityUnindexed        bool
	consumerRequiresInput          bool
	consumerInputBind              string
	producerRequiresOutput         bool
	producerOutputBind             string
	producerOutputKey              string
	producerOutputCarries          string
	omitProducerOutputKey          bool
	omitProducerOutputCarries      bool
	omitProducerEmitField          bool
	producerVerticalIDType         string
	producerAgentEmit              bool
	producerAutoEmit               bool
	producerTimer                  bool
	duplicateProducerOutputKey     bool
	consumerTemplateInstance       bool
	consumerTemplateInstanceBy     string
	consumerTemplateInstanceRegion bool
}

type compositionConnectAdapterField struct {
	Name string
	Type string
}

type compositionConnectAdapterFixtureOptions struct {
	sourceFields       []compositionConnectAdapterField
	outputKey          string
	outputCarries      string
	receiverInstanceBy string
	receiverFields     []compositionConnectAdapterField
	usingSource        string
	usingTarget        string
	omitUsing          bool
	includeLegacyMap   bool
}

func (o compositionConnectAdapterFixtureOptions) clone() compositionConnectAdapterFixtureOptions {
	out := o
	out.sourceFields = append([]compositionConnectAdapterField(nil), o.sourceFields...)
	out.receiverFields = append([]compositionConnectAdapterField(nil), o.receiverFields...)
	return out
}

func writeCompositionConnectBootverifyFixture(t *testing.T, opts compositionConnectFixtureOptions) string {
	// routing-example-census: different-concept issue=1738 owner=legacy_connect_adapter_and_instance_policy_validation proof=internal/runtime/bootverify/workflow_composition_connect_checks_test.go:TestRun_AllowsTemplateInstanceKeyCompositionConnectWithoutAddress
	t.Helper()
	root := t.TempDir()
	connectFrom := firstTestValue(opts.connectFrom, "producer.deploy_done")
	connectTo := firstTestValue(opts.connectTo, "consumer.deploy_completed")
	delivery := firstTestValue(opts.delivery, "one")
	adapter := "    adapter: deploy_done_to_completed\n"
	if opts.noAdapter {
		adapter = ""
	}
	mapSource := firstTestValue(opts.mapSource, "payload.vertical_id")
	mapTarget := firstTestValue(opts.mapTarget, "entity.vertical_id")
	mapBlock := ""
	if !opts.omitMap {
		mapBlock = `
    map:
      vertical_id:
        source: ` + mapSource + `
        target: ` + mapTarget
	}
	flowBind := ""
	if opts.consumerRequiresInput {
		bind := firstTestValue(opts.consumerInputBind, "deploy.done")
		flowBind = `
    bind:
      inputs:
        deploy.completed: ` + bind
	}
	producerBind := ""
	if opts.producerRequiresOutput {
		bind := firstTestValue(opts.producerOutputBind, "deploy.completed")
		producerBind = `
    bind:
      outputs:
        deploy.done: ` + bind
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: composition-connect-bootverify
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer
    flow: producer
    mode: static`+producerBind+`
  - id: consumer
    flow: consumer
    mode: `+firstTestValue(opts.consumerMode, "static")+flowBind+`
connect:
  - from: `+connectFrom+`
    to: `+connectTo+`
`+adapter+`    delivery: `+delivery+`
`+mapBlock+`
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: composition-connect-bootverify\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	rootEvents := "{}\n"
	if opts.producerRequiresOutput {
		rootEvents = "deploy.completed:\n  vertical_id: string\n"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), rootEvents)
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeCompositionConnectProducerFlow(t, root, opts)
	writeCompositionConnectConsumerFlow(t, root, opts)
	return root
}

func writeRootCompositionConnectBootverifyFixture(t *testing.T) string {
	// routing-example-census: different-concept issue=none owner=bootverify.root_output_connect_validation proof=internal/runtime/bootverify/workflow_composition_connect_checks_test.go:TestRun_AllowsRootProducerCompositionConnectAsRouteProof
	t.Helper()
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: root-composition-connect-bootverify
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: consumer
    flow: consumer
    mode: static
connect:
  - from: .root_ready
    to: consumer.ready
    delivery: one
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: root-composition-connect-bootverify
pins:
  inputs:
    events: [root.start]
  outputs:
    events:
      - name: root_ready
        event: root.ready
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), `
root.start:
  entity_id: text
root.ready:
  entity_id: text
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
root-node:
  id: root-node
  execution_type: system_node
  event_handlers:
    root.start:
      emit:
        event: root.ready
        fields:
          entity_id: payload.entity_id
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
mode: static
pins:
  inputs:
    events:
      - name: ready
        event: root.ready
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
root.ready:
  entity_id: text
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), "{}\n")
	return root
}

func writeRootAutoEmitOutputPinKeyCarriesFixture(t *testing.T) string {
	// routing-example-census: different-concept issue=none owner=bootverify.root_auto_emit_key_carries proof=internal/runtime/bootverify/workflow_composition_connect_checks_test.go:TestRun_FailsClosedForRootAutoEmitOutputPinKeyCarriesEvidence
	t.Helper()
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: root-auto-emit-key-carries
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: consumer
    flow: consumer
    mode: static
connect:
  - from: .root_ready
    to: consumer.ready
    delivery: one
    map:
      entity_id:
        source: payload.entity_id
        target: _entity.id
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), `
name: root-auto-emit-key-carries
auto_emit_on_create:
  event: root.ready
pins:
  outputs:
    events:
      - name: root_ready
        event: root.ready
        key: entity_id
        carries: [entity_id]
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), `
root.ready:
  entity_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
mode: static
pins:
  inputs:
    events:
      - name: ready
        event: root.ready
        address:
          by: entity_id
          source: payload.entity_id
          target: _entity.id
          cardinality: one
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
root.ready:
  entity_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), `
deployment:
  entity_id:
    type: string
    indexed: true
    _unused_reason: root output pin key/carries receiver address proof field
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), "{}\n")
	return root
}

func writeCompositionConnectTopologyFixture(t *testing.T) string {
	// routing-example-census: different-concept issue=none owner=bootverify.composition_topology_diagnostics proof=internal/runtime/bootverify/workflow_composition_connect_checks_test.go:TestRun_TreatsParentCompositionConnectAsEventTopologyProof
	t.Helper()
	root := t.TempDir()
	writeCompositionConnectRootPackage(t, root, "composition-connect-topology", `
  - from: producer.deploy_done
    to: consumer.deploy_completed
    adapter: deploy_done_to_completed
    delivery: one
    map:
      vertical_id:
        source: payload.vertical_id
        target: entity.vertical_id
`)
	writeCompositionConnectProducerSchemaOnlyFlow(t, root)
	writeCompositionConnectConsumerSchemaOnlyFlow(t, root)
	return root
}

func writeCompositionConnectAmbiguityFixture(t *testing.T) string {
	// routing-example-census: different-concept issue=none owner=bootverify.composition_ambiguity_diagnostics proof=internal/runtime/bootverify/workflow_composition_connect_checks_test.go:TestRun_AllowsParentCompositionConnectAsCrossFlowAmbiguityProof
	t.Helper()
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: composition-connect-ambiguity
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: producer_a
    flow: producer_a
    mode: static
  - id: producer_b
    flow: producer_b
    mode: static
  - id: consumer
    flow: consumer
    mode: static
connect:
  - from: producer_a.ticket.ready
    to: consumer.ticket.ready
    delivery: one
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: composition-connect-ambiguity\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	for _, flowID := range []string{"producer_a", "producer_b"} {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), `
name: `+flowID+`
mode: static
pins:
  outputs:
    events:
      - ticket.ready
`)
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "agents.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "entities.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), `
ticket.ready:
  entity_id: string
`)
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "nodes.yaml"), "{}\n")
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
mode: static
pins:
  inputs:
    events:
      - ticket.ready
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
ticket.ready:
  entity_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), "{}\n")
	return root
}

func writeCompositionConnectRootPackage(t *testing.T, root, name, connectEntries string) {
	// routing-example-census: different-concept issue=none owner=bootverify.composition_topology_diagnostics proof=internal/runtime/bootverify/workflow_composition_connect_checks_test.go:TestRun_TreatsParentCompositionConnectAsEventTopologyProof
	t.Helper()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: `+name+`
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
`+connectEntries)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: "+name+"\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
}

func writeCompositionConnectProducerSchemaOnlyFlow(t *testing.T, root string) {
	t.Helper()
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
        key: vertical_id
        carries: [vertical_id]
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "entities.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
deploy.done:
  vertical_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), "{}\n")
}

func writeCompositionConnectConsumerSchemaOnlyFlow(t *testing.T, root string) {
	t.Helper()
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
mode: static
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
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
deploy.completed:
  vertical_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), `
deployment:
  vertical_id:
    type: string
    indexed: true
    _unused_reason: composition connect topology proof field
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), "{}\n")
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
	// routing-example-census: different-concept issue=none owner=bootverify.select_resolution_validation_matrix proof=internal/runtime/bootverify/workflow_composition_connect_checks_test.go:TestRun_FailsClosedForInvalidSelectInputResolution
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
	// routing-example-census: different-concept issue=none owner=bootverify.create_resolution_validation_matrix proof=internal/runtime/bootverify/workflow_composition_connect_checks_test.go:TestRun_FailsClosedForInvalidCreateInputResolution
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

func writeCompositionConnectProducerFlow(t *testing.T, root string, opts compositionConnectFixtureOptions) {
	t.Helper()
	if opts.producerRequiresOutput {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "package.yaml"), `
name: producer
version: "1.0.0"
requires:
  inputs: []
  outputs: [deploy.done]
`)
	}
	outputKeyBlock := ""
	if !opts.omitProducerOutputKey {
		outputKeyBlock += "\n        key: " + firstTestValue(opts.producerOutputKey, "vertical_id")
	}
	if !opts.omitProducerOutputCarries {
		outputKeyBlock += "\n        carries: " + firstTestValue(opts.producerOutputCarries, "[vertical_id]")
	}
	duplicateOutputPinBlock := ""
	if opts.duplicateProducerOutputKey {
		duplicateOutputPinBlock = `
      - name: deploy_done_alias
        event: deploy.done
        key: vertical_id
        carries: [vertical_id]
`
	}
	emitField := "          vertical_id: payload.vertical_id\n"
	if opts.omitProducerEmitField {
		emitField = ""
	} else if opts.consumerTemplateInstanceRegion {
		emitField += "          region: payload.region\n"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
`+producerAutoEmitOnCreateBlock(opts)+`
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
`+outputKeyBlock+`
`+duplicateOutputPinBlock+`
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	if !opts.producerRequiresOutput {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "entities.yaml"), "{}\n")
	}
	verticalIDType := firstTestValue(opts.producerVerticalIDType, "string")
	verticalIDSchema := "  vertical_id: " + verticalIDType + "\n"
	if opts.consumerTemplateInstanceRegion {
		verticalIDSchema += "  region: string\n"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
deploy.requested:
  vertical_id: string
deploy.done:
`+verticalIDSchema+`
`)
	producerAgents := "{}\n"
	if opts.producerAgentEmit {
		producerAgents = `
producer-agent:
  id: producer-agent
  type: claude
  role: producer
  mode: task
  emit_events: [deploy.done]
`
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), producerAgents)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), `
producer-node:
  id: producer-node
  execution_type: system_node
  subscribes_to: [deploy.requested]
`+producerWorkflowTimerBlock(opts)+`
  event_handlers:
    deploy.requested:
      emit:
        event: deploy.done
        fields:
`+emitField+`
`)
}

func producerWorkflowTimerBlock(opts compositionConnectFixtureOptions) string {
	if !opts.producerTimer {
		return ""
	}
	return `  timers:
    - id: deploy_done_timer
      owner: producer-node
      event: deploy.done
      delay: 1m
      start_on: event:deploy.requested
`
}

func producerAutoEmitOnCreateBlock(opts compositionConnectFixtureOptions) string {
	if !opts.producerAutoEmit {
		return ""
	}
	return `auto_emit_on_create:
  event: deploy.done
`
}

func writeCompositionConnectConsumerFlow(t *testing.T, root string, opts compositionConnectFixtureOptions) {
	// routing-example-census: different-concept issue=1738 owner=legacy_connect_adapter_and_instance_policy_validation proof=internal/runtime/bootverify/workflow_composition_connect_checks_test.go:TestRun_AllowsTemplateInstanceKeyCompositionConnectWithoutAddress
	t.Helper()
	if opts.consumerRequiresInput {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "package.yaml"), `
name: consumer
version: "1.0.0"
requires:
  inputs: [deploy.completed]
  outputs: []
`)
	}
	inputEvents := `
      - name: deploy_completed
        event: deploy.completed
        address:
          by: vertical_id
          source: payload.vertical_id
          target: entity.vertical_id
          cardinality: one
`
	if opts.consumerScalarInput {
		inputEvents = `
      - deploy.completed
`
	}
	instanceBlock := ""
	if opts.consumerTemplateInstance {
		instanceBy := firstTestValue(opts.consumerTemplateInstanceBy, "vertical_id")
		instanceBlock = `instance:
  by: ` + instanceBy + `
  on_missing: reject
  on_conflict: reject
`
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
mode: `+firstTestValue(opts.consumerMode, "static")+`
`+instanceBlock+`
initial_state: idle
terminal_states: [done]
states: [idle, done]
pins:
  inputs:
    events:`+inputEvents)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
deploy.completed:
  vertical_id: string
`+compositionConnectConsumerRegionEventSchema(opts))
	if !opts.consumerScalarInput || opts.consumerTemplateInstance {
		entityType := firstTestValue(opts.consumerEntityType, "string")
		indexed := "\n    indexed: true"
		if opts.consumerEntityUnindexed {
			indexed = ""
		}
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), `
deployment:
  vertical_id:
    type: `+entityType+`
`+indexed+`
    _unused_reason: composition connect route-key proof field
`+compositionConnectConsumerRegionEntitySchema(opts)+`
`)
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), `
consumer-node:
  id: consumer-node
  execution_type: system_node
  subscribes_to: [deploy.completed]
  event_handlers:
    deploy.completed:
      create_entity: true
      advances_to: done
`)
}

func compositionConnectConsumerRegionEventSchema(opts compositionConnectFixtureOptions) string {
	if !opts.consumerTemplateInstanceRegion {
		return ""
	}
	return "  region: string\n"
}

func compositionConnectConsumerRegionEntitySchema(opts compositionConnectFixtureOptions) string {
	if !opts.consumerTemplateInstanceRegion {
		return ""
	}
	return "  region:\n" +
		"    type: string\n" +
		"    _unused_reason: composite template instance key proof field\n"
}

func writeCompositionConnectAdapterBootverifyFixture(t *testing.T, opts compositionConnectAdapterFixtureOptions) string {
	// routing-example-census: different-concept issue=1738 owner=legacy_connect_instance_adapter_validation proof=internal/runtime/bootverify/workflow_composition_connect_checks_test.go:TestRun_AllowsRenamedTemplateInstanceKeyCompositionConnectWithUsingInstance
	t.Helper()
	root := t.TempDir()
	outputKey := firstTestValue(opts.outputKey, "source_vertical_id")
	outputCarries := firstTestValue(opts.outputCarries, "["+outputKey+"]")
	receiverInstanceBy := firstTestValue(opts.receiverInstanceBy, "vertical_id")
	usingBlock := ""
	if !opts.omitUsing {
		usingBlock = "\n" +
			"    using:\n" +
			"      instance:\n" +
			"        source: " + opts.usingSource + "\n" +
			"        target: " + opts.usingTarget
	}
	legacyMapBlock := ""
	if opts.includeLegacyMap {
		legacyMapBlock = "\n" +
			"    map:\n" +
			"      vertical_id:\n" +
			"        source: payload." + outputKey + "\n" +
			"        target: entity.vertical_id"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: composition-connect-adapter-bootverify
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
    delivery: one`+usingBlock+legacyMapBlock+`
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: composition-connect-adapter-bootverify\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
mode: static
pins:
  outputs:
    events:
      - name: deploy_done
        event: deploy.done
        key: `+outputKey+`
        carries: `+outputCarries+`
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "entities.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
deploy.requested:
`+compositionConnectAdapterFieldsSchema(opts.sourceFields)+`deploy.done:
`+compositionConnectAdapterFieldsSchema(opts.sourceFields))
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), `
producer-node:
  id: producer-node
  execution_type: system_node
  subscribes_to: [deploy.requested]
  event_handlers:
    deploy.requested:
      emit:
        event: deploy.done
        fields:
`+compositionConnectAdapterEmitFields(opts.sourceFields))
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
mode: template
instance:
  by: `+receiverInstanceBy+`
  on_missing: reject
  on_conflict: reject
pins:
  inputs:
    events:
      - name: deploy_completed
        event: deploy.done
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
deploy.done:
`+compositionConnectAdapterFieldsSchema(opts.sourceFields))
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "entities.yaml"), `
deployment:
`+compositionConnectAdapterEntityFields(opts.receiverFields))
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), "{}\n")
	return root
}

func compositionConnectAdapterFieldsSchema(fields []compositionConnectAdapterField) string {
	if len(fields) == 0 {
		return "  source_vertical_id: string\n"
	}
	var out strings.Builder
	for _, field := range fields {
		name := firstTestValue(field.Name, "source_vertical_id")
		typ := firstTestValue(field.Type, "string")
		out.WriteString("  " + name + ": " + typ + "\n")
	}
	return out.String()
}

func compositionConnectAdapterEmitFields(fields []compositionConnectAdapterField) string {
	if len(fields) == 0 {
		return "          source_vertical_id: payload.source_vertical_id\n"
	}
	var out strings.Builder
	for _, field := range fields {
		name := firstTestValue(field.Name, "source_vertical_id")
		out.WriteString("          " + name + ": payload." + name + "\n")
	}
	return out.String()
}

func compositionConnectAdapterEntityFields(fields []compositionConnectAdapterField) string {
	if len(fields) == 0 {
		return "  vertical_id:\n    type: string\n    _unused_reason: connect adapter receiver instance-key proof field\n"
	}
	var out strings.Builder
	for _, field := range fields {
		name := firstTestValue(field.Name, "vertical_id")
		typ := firstTestValue(field.Type, "string")
		out.WriteString("  " + name + ":\n    type: " + typ + "\n    _unused_reason: connect adapter receiver instance-key proof field\n")
	}
	return out.String()
}

func firstTestValue(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
