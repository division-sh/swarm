package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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
		source:       runtimecontracts.FlowInputInstanceSourceGeneratedUUIDPath,
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
	if !reportContains(report.Errors(), "composition_connect_validation", "producer event validation.requested field validation_case_id conflicts with connection projection generated.uuid") {
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

func TestRun_FailsClosedForInvalidSelectInputResolution(t *testing.T) {
	tests := []struct {
		name string
		opts selectResolutionCompositionFixtureOptions
		want string
	}{
		{
			name: "undeclared instance source",
			opts: selectResolutionCompositionFixtureOptions{instanceKey: "missing_account_id"},
			want: "has no declared type",
		},
		{
			name: "source type mismatch",
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
				source:       runtimecontracts.FlowInputInstanceSourceGeneratedUUIDPath,
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
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := writeCreateResolutionCompositionConnectFixture(t, tc.opts)
			if tc.opts.mode == runtimecontracts.FlowInputResolutionModeFanOut || tc.opts.source == "generated.random" {
				repoRoot := repoRootForBootverifyTest(t)
				_, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
				want := tc.want
				if tc.opts.mode == runtimecontracts.FlowInputResolutionModeFanOut {
					want = "connect.resolution must be"
				}
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("expected immutable pin compilation rejection, got %v", err)
				}
				return
			}
			bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if !reportContains(report.Errors(), "composition_connect_validation", tc.want) {
				t.Fatalf("expected composition_connect_validation %q, got %#v", tc.want, report.Errors())
			}
		})
	}
}

func TestCanonicalResolutionAdmissionBlocksOutOfModeFromBeforeBootVerification(t *testing.T) {
	repoRoot := repoRootForBootverifyTest(t)
	for _, tc := range []struct {
		name string
		root func(testing.TB) string
	}{
		{name: "reply", root: canonicalrouting.CopyTemplateReplyWithInertFrom},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, tc.root(t), runtimecontracts.DefaultPlatformSpecFile(repoRoot)); err == nil || !strings.Contains(err.Error(), "input pin resolution field \"from\" is not supported") {
				t.Fatalf("bundle load error = %v, want canonical rejection before boot verification", err)
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

func TestRun_KeyedPortfolioStreamUsesOrdinaryConnectAndAccumulator(t *testing.T) {
	repoRoot := repoRootForBootverifyTest(t)
	root := canonicalrouting.ExampleRoot(t, canonicalrouting.FanInStream)
	bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	source := semanticview.Wrap(bundle)
	if findings := Run(context.Background(), source, Options{}).HardInvalidities(); len(findings) != 0 {
		t.Fatalf("keyed portfolio stream hard invalidities: %#v", findings)
	}
	assertOrdinaryPortfolioInput(t, source, "portfolio", "operating.reported", "period_id", runtimecontracts.FlowInputResolutionModeSelectOrCreate)
	handler, ok := source.ExecutableNodeEventHandler(identitytest.FlowNode(t, "portfolio", "portfolio-collector"), "operating.reported")
	if !ok || handler.Accumulate == nil || handler.Accumulate.Into != "operating_reports" || handler.Accumulate.From != "payload" || handler.Accumulate.Key != "payload.operating_id" || handler.Join != nil {
		t.Fatalf("stream must retain its own keyed accumulator, not a pin aggregation variant: %#v", handler)
	}
	if plans := source.WorkflowJoins(); len(plans) != 0 {
		t.Fatalf("ordinary stream connection invented a finite join: %#v", plans)
	}
}

func TestRun_NestedPortfolioJoinUsesOrdinaryConnectAndAuthoredMembership(t *testing.T) {
	repoRoot := repoRootForBootverifyTest(t)
	root := canonicalrouting.ExampleRoot(t, canonicalrouting.FanInBarrier)
	bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	source := semanticview.Wrap(bundle)
	if findings := Run(context.Background(), source, Options{}).HardInvalidities(); len(findings) != 0 {
		t.Fatalf("nested portfolio join hard invalidities: %#v", findings)
	}
	assertOrdinaryPortfolioInput(t, source, "portfolio", "operating.reported", "portfolio_id", runtimecontracts.FlowInputResolutionModeSelect)
	assertOrdinaryPortfolioInput(t, source, "portfolio/period", "period.setup", "period_id", runtimecontracts.FlowInputResolutionModeCreate)
	assertOrdinaryPortfolioInput(t, source, "portfolio/period", "period.reported", "period_id", runtimecontracts.FlowInputResolutionModeSelect)
	plans := source.WorkflowJoins()
	if len(plans) != 1 {
		t.Fatalf("expected one intrinsic arrival join, got %#v", plans)
	}
	plan := plans[0]
	if plan.Node.FlowPath() != "portfolio/period" || plan.Node.NodeID() != "portfolio-collector" || plan.HandlerEvent != "period.reported" || plan.Mode != runtimecontracts.WorkflowJoinModeArrival {
		t.Fatalf("join ownership must stay on the nested period handler: %#v", plan)
	}
	if plan.Spec.Members.From != "state.expected_operating_ids" || plan.Spec.Members.By != "payload.operating_id" || plan.Spec.Members.Count != nil || plan.Spec.Output != "payload.revenue" {
		t.Fatalf("join membership and output must come from the handler: %#v", plan.Spec)
	}
	if plan.Spec.Stage != "awaiting" || plan.Spec.OnComplete.AdvancesTo != "complete" || plan.Spec.Deadline == nil || plan.Spec.Deadline.After != "5m" || plan.Spec.Deadline.From != runtimecontracts.JoinDeadlineFromStageEntry || plan.Spec.OnDeadline.AdvancesTo != "failed" {
		t.Fatalf("join must retain its lifecycle-owned closure and deadline: %#v", plan.Spec)
	}
	resultType, err := plan.ResultType.Resolve()
	if err != nil || resultType.Kind != runtimecontracts.CatalogTypeInteger {
		t.Fatalf("join must retain the event's integer result type: %#v, err=%v", resultType, err)
	}
}

func assertOrdinaryPortfolioInput(t *testing.T, source semanticview.Source, flowID, eventType, key string, mode runtimecontracts.FlowInputResolutionMode) {
	t.Helper()
	pin, ok := source.FlowInputEventPin(flowID, eventType)
	if !ok || !pin.Resolution().Empty() {
		t.Fatalf("input %s/%s must be an ordinary boundary without pin resolution: %#v", flowID, eventType, pin)
	}
	graph := runtimepinrouting.CompileConnectGraph(source)
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatalf("ordinary composition failed to compile: %#v", issues)
	}
	plans := graph.PlansToInputPin(flowID, pin)
	if len(plans) != 1 || plans[0].ResolutionKind() != runtimepinrouting.ConnectResolutionInstanceKey || plans[0].InstanceKey() == nil {
		t.Fatalf("input %s/%s must have exactly one keyed ordinary connect: %#v", flowID, eventType, plans)
	}
	instance := plans[0].InstanceKey().Readback()
	if plans[0].InstanceKey().Mode() != mode || instance.Field != key || instance.SourcePath != "payload."+key {
		t.Fatalf("input %s/%s instance policy = %#v, want %s by payload.%s", flowID, eventType, instance, runtimecontracts.FlowInputResolutionModeCode(mode), key)
	}
	receiver := plans[0].ReceiverEndpoint().Readback()
	if receiver.FlowPath != flowID || receiver.LocalEvent != eventType {
		t.Fatalf("ordinary connection lost its exact receiver scope: %#v", receiver)
	}
}

func TestCompositionSourceRejectsRetiredFanInGrammarBeforeBoot(t *testing.T) {
	t.Run("canonical retired specimen", func(t *testing.T) {
		repoRoot := repoRootForBootverifyTest(t)
		root := canonicalrouting.CopyRetiredFanInPin(t)
		_, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
		if err == nil || !strings.Contains(err.Error(), "portfolio/schema.yaml") || !strings.Contains(err.Error(), "resolution") {
			t.Fatalf("retired canonical pin reached boot verification: %v", err)
		}
	})
	for _, tc := range []struct {
		name, diagnostic string
		variant          canonicalrouting.RetiredFanInGrammar
	}{
		{"pin mode", "mode", canonicalrouting.RetiredFanInPinMode},
		{"legacy aggregate", "resolution", canonicalrouting.RetiredFanInAggregate},
		{"connect mode", "connect.resolution", canonicalrouting.RetiredFanInConnectMode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repoRoot := repoRootForBootverifyTest(t)
			root := canonicalrouting.CopyRetiredFanInGrammar(t, tc.variant)
			_, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
			if err == nil || !strings.Contains(err.Error(), "schema.yaml") || !strings.Contains(err.Error(), tc.diagnostic) {
				t.Fatalf("retired fan-in grammar reached boot verification: err=%v, want source-local %q rejection", err, tc.diagnostic)
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
		{name: "missing explicit rename", variant: canonicalrouting.CompositionConnectWithoutRename, want: "receiver_input_pin_missing"},
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

func TestRun_RejectsUnconnectedInputDespiteUnambiguousSiblingSchemas(t *testing.T) {
	root := writeCompositionConnectAmbiguityFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "composition_connect_validation", "") {
		t.Fatalf("unexpected composition_connect_validation error: %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "input_pin_wiring", "ticket.ready") {
		t.Fatalf("sibling schemas without a compiled edge supplied input authority: %#v", report.Errors())
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
		invalidity = canonicalrouting.SelectResolutionUndeclaredSource
	case strings.TrimSpace(opts.carryType) == "integer":
		invalidity = canonicalrouting.SelectResolutionSourceTypeMismatch
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
	}
	return canonicalrouting.CopyTemplateCreateResolution(t, canonicalrouting.TemplateCreateResolutionOptions{
		Mint:       canonicalrouting.CreateMintUUID,
		Invalidity: invalidity,
	})
}
