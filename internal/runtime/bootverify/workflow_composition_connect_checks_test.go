package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
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

func TestRun_RejectsSameSubscriberThroughDistinctReceiverPins(t *testing.T) {
	root := canonicalrouting.CopyCompositionConnect(t, canonicalrouting.CompositionConnectValid)
	canonicalrouting.ApplyCompositionConnectReceiverPinCollisionMutation(t, root)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "composition_connect_validation", "multiple receiver pins") ||
		!reportContains(report.Errors(), "composition_connect_validation", "deploy.completed") ||
		!reportContains(report.Errors(), "composition_connect_validation", "deploy.audited") {
		t.Fatalf("receiver-pin collision findings = %#v", report.Errors())
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
		source:       runtimecontracts.FlowInputCarrySourceGeneratedUUID,
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
	if !reportContains(report.Errors(), "composition_connect_validation", "emit field validation_case_id conflicts with receiver-owned carry generated.uuid") {
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
			want: "must declare a carry named account_id",
		},
		{
			name: "type mismatch",
			opts: selectResolutionCompositionFixtureOptions{carryType: "integer"},
			want: "key_types_incompatible",
		},
		{
			name: "non-template receiver",
			opts: selectResolutionCompositionFixtureOptions{receiverMode: "static"},
			want: "INVALID-TEMPLATE-INSTANCE",
		},
	}
	for _, mode := range []runtimecontracts.FlowInputResolutionMode{runtimecontracts.FlowInputResolutionModeSelect, runtimecontracts.FlowInputResolutionModeSelectOrCreate} {
		for _, tc := range tests {
			t.Run(runtimecontracts.FlowInputResolutionModeCode(mode)+"/"+tc.name, func(t *testing.T) {
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
				source:       runtimecontracts.FlowInputCarrySourceGeneratedUUID,
				includeCarry: true,
			},
			want: "instance_resolution_unimplemented",
		},
		{
			name: "invalid generated source",
			opts: createResolutionCompositionFixtureOptions{
				mode:         runtimecontracts.FlowInputResolutionModeCreate,
				source:       "generated.random",
				includeCarry: true,
			},
			want: "only generated.uuid is supported",
		},
		{
			name: "missing carried instance key",
			opts: createResolutionCompositionFixtureOptions{
				mode:   runtimecontracts.FlowInputResolutionModeCreate,
				source: runtimecontracts.FlowInputCarrySourceGeneratedUUID,
			},
			want: "must declare a carry named validation_case_id",
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

func TestRun_ValidatesAuthoritativeInstanceSourceTypeMatrix(t *testing.T) {
	tests := []struct {
		name      string
		root      func(*testing.T) string
		wantError bool
	}{
		{
			name: "select accepts schema string receiver text alias",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{})
			},
		},
		{
			name: "select rejects omitted annotation with incompatible schema source",
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
			name: "create accepts payload text receiver uuid alias",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintPayload})
			},
		},
		{
			name: "create accepts intrinsic event id",
			root: func(t *testing.T) string {
				return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{Mint: canonicalrouting.CreateMintEventID})
			},
		},
		{
			name: "create payload rejects omitted annotation with incompatible schema source",
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
			name: "create generated uuid rejects incompatible receiver without annotation",
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
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := tc.root(t)
			repoRoot := repoRootForBootverifyTest(t)
			bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
			gotError := reportContains(report.Errors(), "composition_connect_validation", "key_types_incompatible")
			if gotError != tc.wantError {
				t.Fatalf("key type blocker = %v, want %v; errors = %#v", gotError, tc.wantError, report.Errors())
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
		applyFanInMultipleJoinPlan(t, bundle)
		return Run(context.Background(), semanticview.Wrap(bundle), Options{})
	}
	canonicalrouting.ApplyFanInNegativeMutation(t, root, mutation)
	bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	return Run(context.Background(), semanticview.Wrap(bundle), Options{})
}

func applyFanInMultipleJoinPlan(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) {
	t.Helper()
	for _, plan := range bundle.Semantics.Joins {
		if plan.Node.FlowID() == "portfolio" && plan.Node.NodeID() == "portfolio-collector" && plan.HandlerEvent == "operating.reported" {
			plan.Node = identitytest.FlowNode(t, "portfolio", "portfolio-collector-duplicate")
			bundle.Semantics.Joins = append(bundle.Semantics.Joins, plan)
			return
		}
	}
	t.Fatal("canonical portfolio join plan is unavailable")
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
		{name: "event names differ without adapter", variant: canonicalrouting.CompositionConnectMissingAdapter, want: "event_alias_or_adapter_invalid"},
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

func TestRun_AcceptsParentCompositionConnectToRootInput(t *testing.T) {
	root := writeCompositionConnectBootverifyFixture(t, canonicalrouting.CompositionConnectRootReceiver)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	for _, finding := range append(report.Errors(), report.Warnings()...) {
		if finding.CheckID == "composition_connect_validation" {
			t.Fatalf("unexpected composition connect finding: %#v", finding)
		}
	}
}

func TestRun_FailsClosedForInvalidOutputPinKeyCarriesEvidence(t *testing.T) {
	tests := []struct {
		name    string
		variant canonicalrouting.CompositionConnectVariant
		want    string
	}{
		{name: "connected output missing key", variant: canonicalrouting.CompositionConnectMissingOutputKey, want: "missing_key"},
		{name: "connected output missing carries", variant: canonicalrouting.CompositionConnectMissingOutputCarries, want: "key_not_carried"},
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
	mode         runtimecontracts.FlowInputResolutionMode
	source       string
	includeCarry bool
}

type selectResolutionCompositionFixtureOptions struct {
	mode         runtimecontracts.FlowInputResolutionMode
	instanceKey  string
	carryType    string
	receiverMode string
}

func writeSelectResolutionCompositionConnectFixture(t *testing.T, opts selectResolutionCompositionFixtureOptions) string {
	t.Helper()
	mode := canonicalrouting.SelectResolutionSelect
	if opts.mode == runtimecontracts.FlowInputResolutionModeSelectOrCreate {
		mode = canonicalrouting.SelectResolutionSelectOrCreate
	}
	invalidity := canonicalrouting.SelectResolutionValid
	switch {
	case strings.TrimSpace(opts.instanceKey) == "missing_account_id":
		invalidity = canonicalrouting.SelectResolutionUndeclaredCarry
	case strings.TrimSpace(opts.carryType) == "integer":
		invalidity = canonicalrouting.SelectResolutionCarryTypeMismatch
	case strings.TrimSpace(opts.receiverMode) == "static":
		invalidity = canonicalrouting.SelectResolutionStaticReceiver
	}
	return canonicalrouting.CopyTemplateSelectResolution(t, canonicalrouting.TemplateSelectResolutionOptions{Mode: mode, Invalidity: invalidity})
}

func writeCreateResolutionCompositionConnectFixture(t *testing.T, opts createResolutionCompositionFixtureOptions) string {
	t.Helper()
	invalidity := canonicalrouting.CreateResolutionValid
	switch {
	case opts.mode == runtimecontracts.FlowInputResolutionModeFanOut:
		invalidity = canonicalrouting.CreateResolutionNonRunnableMode
	case opts.source == "generated.random":
		invalidity = canonicalrouting.CreateResolutionInvalidMint
	case !opts.includeCarry:
		invalidity = canonicalrouting.CreateResolutionMissingCarry
	}
	return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{
		Mint:       canonicalrouting.CreateMintUUID,
		Invalidity: invalidity,
	})
}
