package semanticview

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"gopkg.in/yaml.v3"
)

func TestResolveFlowInputProducer_ClassifiesIntrinsicInputPinSource(t *testing.T) {
	source := flowInputProducerFixture(runtimecontracts.FlowInputEventPin{
		Name:   "work.requested",
		Event:  "work.requested",
		Source: "external",
	}, nil)

	resolution := ResolveFlowInputProducer(source, "worker", "work.requested")

	if !resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryIntrinsicIngress) {
		t.Fatalf("evidence = %#v, want intrinsic ingress", resolution.Evidence)
	}
	if len(resolution.ProducerPatterns()) != 0 {
		t.Fatalf("patterns = %#v, want none for intrinsic ingress", resolution.ProducerPatterns())
	}
}

func TestResolveFlowInputProducer_ClassifiesRootBoundaryExternalIngress(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{"work.requested"}},
			},
		},
	}

	resolution := ResolveFlowInputProducer(Wrap(bundle), "", "work.requested")

	if !resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryExternalIngress) {
		t.Fatalf("evidence = %#v, want boundary external ingress", resolution.Evidence)
	}
}

func TestResolveFlowInputProducer_ExplicitConnectOwnsRootInput(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{
					Events: []string{"work.requested"},
					EventPins: []runtimecontracts.FlowInputEventPin{{
						Name:  "work_requested",
						Event: "work.requested",
					}},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			CompositionConnects: []runtimecontracts.FlowPackageConnect{{
				From: "worker.work_completed",
				To:   ".work_requested",
			}},
		},
	}

	resolution := ResolveFlowInputProducer(Wrap(bundle), "", "work.requested")

	if !resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryParentConnect) {
		t.Fatalf("evidence = %#v, want parent connect", resolution.Evidence)
	}
	if resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryExternalIngress) {
		t.Fatalf("evidence = %#v, connected root pin must not retain external-ingress authority", resolution.Evidence)
	}
}

func TestResolveFlowInputProducer_NestedPackageRootConnectDoesNotSuppressRepositoryRootIngress(t *testing.T) {
	rootInput := runtimecontracts.FlowInputEventPin{Name: "work_requested", Event: "root.work.requested"}
	childInput := runtimecontracts.FlowInputEventPin{Name: "work_requested", Event: "child.work.requested"}
	child := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child", PackageKey: "flows/child"},
		Schema: runtimecontracts.FlowSchemaDocument{Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{
			Events:    []string{childInput.EventType()},
			EventPins: []runtimecontracts.FlowInputEventPin{childInput},
		}}},
		Path: "child",
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{child}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{
			Events:    []string{rootInput.EventType()},
			EventPins: []runtimecontracts.FlowInputEventPin{rootInput},
		}}},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{"child": &root.Children[0]},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"child": child.Schema},
		Semantics: runtimecontracts.WorkflowSemanticView{CompositionConnects: []runtimecontracts.FlowPackageConnect{{
			PackageKey: "flows/child",
			From:       "producer.work_completed",
			To:         ".work_requested",
		}}},
	}
	source := Wrap(bundle)

	rootResolution := ResolveFlowInputProducer(source, "", rootInput.EventType())
	if !rootResolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryExternalIngress) {
		t.Fatalf("root evidence = %#v, want repository-root external ingress", rootResolution.Evidence)
	}
	if rootResolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryParentConnect) {
		t.Fatalf("root evidence = %#v, nested package-root connect must not own the repository-root pin", rootResolution.Evidence)
	}

	childResolution := ResolveFlowInputProducer(source, "child", childInput.EventType())
	if !childResolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryParentConnect) {
		t.Fatalf("child evidence = %#v, want nested package-root connect ownership", childResolution.Evidence)
	}
}

func TestResolveFlowInputProducer_ClassifiesParentConnectWithoutRoutePattern(t *testing.T) {
	source := flowInputProducerFixture(runtimecontracts.FlowInputEventPin{
		Name:  "work.requested",
		Event: "work.requested",
	}, []runtimecontracts.FlowPackageConnect{{From: ".work.requested", To: "worker.work.requested"}})

	resolution := ResolveFlowInputProducer(source, "worker", "work.requested")

	if !resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryParentConnect) {
		t.Fatalf("evidence = %#v, want parent connect", resolution.Evidence)
	}
	if len(resolution.ProducerPatterns()) != 0 {
		t.Fatalf("patterns = %#v, want none for direct parent connect route proof", resolution.ProducerPatterns())
	}
}

func TestResolveFlowInputProducer_ClassifiesDeclaredHarnessSource(t *testing.T) {
	source := harnessOnlyFlowInputProducerFixture(runtimecontracts.FlowInputEventPin{
		Name:   "work.requested",
		Event:  "work.requested",
		Source: "harness",
	}, nil)

	resolution := ResolveFlowInputProducer(source, "worker", "work.requested")

	if !resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerBoundaryHarnessInjection) {
		t.Fatalf("evidence = %#v, want harness injection", resolution.Evidence)
	}
	if got := resolution.ProducerPatterns(); len(got) != 0 {
		t.Fatalf("ProducerPatterns = %#v, want no harness routing authority", got)
	}
	if got := resolution.ProducerFlows(); len(got) != 0 {
		t.Fatalf("ProducerFlows = %#v, want no harness routing authority", got)
	}
	if resolution.HasConflictingHarnessEvidence() {
		t.Fatalf("evidence = %#v, want harness as the sole producer proof", resolution.Evidence)
	}
}

func TestResolveFlowInputAutoWire_HarnessEvidenceHasNoPatternsOrProducerFlows(t *testing.T) {
	source := harnessOnlyFlowInputProducerFixture(runtimecontracts.FlowInputEventPin{
		Name: "work.requested", Event: "work.requested", Source: "harness",
	}, nil)

	resolution := ResolveFlowInputAutoWire(source, "worker", "work.requested")
	if len(resolution.Patterns) != 0 || len(resolution.ProducerFlows) != 0 {
		t.Fatalf("auto-wire = %#v, want no harness routing authority", resolution)
	}
	if patterns := FlowInputProducerPatterns(source, "worker", "work.requested"); len(patterns) != 0 {
		t.Fatalf("producer patterns = %#v, want none", patterns)
	}
}

func TestImportBoundaryHarnessInputCreatesNoAlias(t *testing.T) {
	source := harnessOnlyFlowInputProducerFixture(runtimecontracts.FlowInputEventPin{
		Name: "work.requested", Event: "work.requested", Source: "harness",
	}, nil)

	if ImportBoundaryInputAliasRequired(source, "worker", "work.requested") {
		t.Fatal("bare harness input unexpectedly requires an import-boundary alias")
	}
	if aliases := ImportBoundaryInputAliases(source, "worker", "work.requested"); len(aliases) != 0 {
		t.Fatalf("import aliases = %#v, want none", aliases)
	}
}

func TestResolveFlowInputProducer_ClassifiesPlatformSource(t *testing.T) {
	source := flowInputProducerFixture(runtimecontracts.FlowInputEventPin{
		Name:  "platform.runtime_log",
		Event: "platform.runtime_log",
	}, nil)

	resolution := ResolveFlowInputProducer(source, "worker", "platform.runtime_log")

	if !resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerPlatformSource) {
		t.Fatalf("evidence = %#v, want platform source", resolution.Evidence)
	}
}

func TestResolveFlowInputProducer_ClassifiesInternalTopologyWithoutAutoWirePattern(t *testing.T) {
	source := flowInputProducerFixture(runtimecontracts.FlowInputEventPin{
		Name:  "work.requested",
		Event: "work.requested",
	}, nil)

	resolution := ResolveFlowInputProducer(source, "worker", "work.requested")

	if !resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerInternalTopology) {
		t.Fatalf("evidence = %#v, want internal topology producer", resolution.Evidence)
	}
	if len(resolution.ProducerPatterns()) != 0 {
		t.Fatalf("patterns = %#v, want no sibling-output auto-wire pattern", resolution.ProducerPatterns())
	}
	if got := resolution.AutoWireResolution().ProducerFlows; len(got) != 0 {
		t.Fatalf("ProducerFlows = %#v, want none for internal topology proof", got)
	}
}

func TestResolveFlowInputProducer_ClassifiesAutoEmitOnCreateAsInternalTopology(t *testing.T) {
	source := flowInputProducerFixture(runtimecontracts.FlowInputEventPin{
		Name:  "work.created",
		Event: "work.created",
	}, nil)
	bundle, ok := Bundle(source)
	if !ok || bundle == nil {
		t.Fatal("expected bundle source")
	}
	worker := bundle.FlowSchemas["worker"]
	worker.AutoEmitOnCreate = runtimecontracts.AutoEmitOnCreateContract{Event: "work.created"}
	worker.Pins.Inputs.Events = []string{"work.created"}
	worker.Pins.Inputs.EventPins = []runtimecontracts.FlowInputEventPin{{
		Name:  "work.created",
		Event: "work.created",
	}}
	bundle.FlowSchemas["worker"] = worker
	bundle.FlowTree.ByID["worker"].Schema = worker

	resolution := ResolveFlowInputProducer(source, "worker", "work.created")

	if !resolution.HasEvidenceKind(runtimecontracts.FlowInputProducerInternalTopology) {
		t.Fatalf("evidence = %#v, want auto_emit_on_create internal topology proof", resolution.Evidence)
	}
}

func TestResolveFlowInputProducer_ReportsMissingAndInvalidContext(t *testing.T) {
	source := flowInputProducerFixture(runtimecontracts.FlowInputEventPin{
		Name:  "work.requested",
		Event: "work.requested",
	}, nil)

	missing := ResolveFlowInputProducer(source, "worker", "work.missing")
	if missing.HasEvidence() || !missing.HasEvidenceKind(runtimecontracts.FlowInputProducerInvalidContext) {
		t.Fatalf("missing input evidence = %#v, want invalid context outcome without proof", missing.Evidence)
	}

	noProducer := flowInputProducerFixture(runtimecontracts.FlowInputEventPin{
		Name:  "work.unproduced",
		Event: "work.unproduced",
	}, nil)
	resolution := ResolveFlowInputProducer(noProducer, "worker", "work.unproduced")
	if resolution.HasEvidence() {
		t.Fatalf("evidence = %#v, want no producer proof", resolution.Evidence)
	}
}

func flowInputProducerFixture(inputPin runtimecontracts.FlowInputEventPin, connects []runtimecontracts.FlowPackageConnect) Source {
	producer := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{Events: []string{"work.requested"}},
			},
		},
		Path: "producer",
	}
	worker := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "worker", Flow: "worker"},
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{
					Events:    []string{inputPin.EventType()},
					EventPins: []runtimecontracts.FlowInputEventPin{inputPin},
				},
			},
		},
		Path: "worker",
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, worker}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer": &root.Children[0],
				"worker":   &root.Children[1],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"producer": producer.Schema,
			"worker":   worker.Schema,
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			CompositionConnects: connects,
		},
	}
	bundle.Platform.PlatformEvents.Catalog = map[string]yaml.Node{
		"platform.runtime_log": {},
	}
	return Wrap(bundle)
}

func harnessOnlyFlowInputProducerFixture(inputPin runtimecontracts.FlowInputEventPin, connects []runtimecontracts.FlowPackageConnect) Source {
	source := flowInputProducerFixture(inputPin, connects)
	bundle, ok := Bundle(source)
	if !ok || bundle == nil {
		panic("flow input producer fixture did not expose its bundle")
	}
	producer := bundle.FlowTree.ByID["producer"]
	producer.Schema.Pins.Outputs = runtimecontracts.FlowOutputPins{}
	bundle.FlowSchemas["producer"] = producer.Schema
	if bundle.Semantics.FlowOutputs == nil {
		bundle.Semantics.FlowOutputs = map[string][]string{}
	}
	bundle.Semantics.FlowOutputs["producer"] = nil
	return source
}
