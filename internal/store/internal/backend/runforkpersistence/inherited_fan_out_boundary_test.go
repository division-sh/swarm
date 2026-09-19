package runforkpersistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"
)

// This allowlist guards semantic construction and the named write handoff, not
// just the file containing each owner. Runtime relation/rollback tests remain
// required: reference counts are not proof of transaction or lineage correctness.
func inheritedFanOutBoundaryAllowances() map[string]historicalBoundaryAllowance {
	allowed := map[string]historicalBoundaryAllowance{}
	edge := func(caller, callee, reason string) {
		allowed[caller+"/reference:"+callee] = historicalBoundaryAllowance{1, reason}
	}
	edge("runtime/fanoutobligation::ordinalEmission", "events::NewInheritedFanOutOrigin", "one exact ordinal origin projection")
	edge("runtime/fanoutobligation::OrdinalEmission.NewEvent", "events::NewInheritedFanOutEvent", "only the semantic ordinal projection creates this event variant")
	edge("events::BindManagerOutputIdentity", "events::NewInheritedFanOutEvent", "event owner preserves previously admitted origin during identity binding")
	edge("events::RestoreAdmittedEvent", "events::NewInheritedFanOutEvent", "canonical durable restoration requires the exact decoded origin")
	edge("store/internal/backend/fanoutorigin::ValidateCommitted", "runtime/fanoutobligation::ValidateCommittedOrdinalEvent", "live exact outcome readback consumes ordinal semantics")
	edge("store/internal/backend/runforkpersistence::admitRunForkInheritedFanOutHistory", "runtime/fanoutobligation::ValidateCommittedOrdinalEvent", "fixed-revision exact outcome readback consumes ordinal semantics")
	edge("store/internal/backend/eventrecord::Record.decodeInheritedFanOutOrigin", "events::NewInheritedFanOutOrigin", "strict private durable codec")
	edge("runtime/engine::Executor.PrepareFanOutEvaluation", "runtime/fanoutobligation::PrepareOrdinalEmission", "preparation admits the pinned immutable intent before retaining its chunk frame")
	edge("runtime/engine::FanOutEvaluation.EvaluateOrdinal", "runtime/fanoutobligation::PrepareOrdinalEmission", "each prepared ordinal independently consumes the canonical intent and execution relation")
	edge("store/internal/backend/pipelinepersistence::commitFanOutChunk", "runtime/fanoutobligation::PrepareOrdinalEmission", "locked intent and trigger select exact ordinal")
	edge("runtime/bus::EventBus.prepareEnginePublicationsWithMember", "runtime/bus::EventBus.admitEnginePublishEvent", "the common engine planning owner prepares inherited origin; prospective state cannot authorize its commit")
	edge("runtime/bus::EventBus.PrepareFanOutPublications", "runtime/bus::EventBus.admitEnginePublishEvent", "bounded group preparation admits each member through the same event owner before claiming")
	edge("store/internal/backend/pipelinepersistence::publicationGroup.ClaimBatch", "runtime/fanoutobligation::ValidateCommittedOrdinalEvent", "each exact grouped ordinal is validated before any member claim is created")
	edge("runtime/bus::EnginePublicationPlan.ValidateDurablePublicationPlan", "runtime/bus::PublicationCommand.ValidateFanOut", "pure prepared-plan shape, not write authority")
	edge("store/internal/backend/eventpersistence::commitFanOutPublicationTx", "runtime/bus::PublicationCommand.ValidateFanOut", "named transaction validates plan shape")
	edge("store/internal/backend/eventpersistence::commitPublicationTx", "store/internal/backend/eventpersistence::commitValidatedPublicationTx", "generic publication rejects inherited class before private mutation")
	edge("store/internal/backend/eventpersistence::commitFanOutPublicationTx", "store/internal/backend/eventpersistence::commitValidatedPublicationTx", "named publication validates locked ordinal projection")
	edge("store/internal/backend/pipelinepersistence::commitFanOutChunk", "store/internal/backend/pipelinepersistence::eventCommitTxStore.commitFanOutPublicationTx", "only named atomic chunk may submit this write")
	for _, backend := range []string{"Postgres", "SQLite"} {
		edge("store/internal/backend/pipelinepersistence::Pipeline"+backend+"Owner.commitFanOutPublicationTx", "store/internal/backend/pipelinepersistence::EventCommitOwner.CommitFanOutPublicationTx", "thin selected-store handoff")
		edge("store/internal/backend/eventpersistence::Event"+backend+"Owner.CommitFanOutPublicationTx", "store/internal/backend/eventpersistence::commitFanOutPublicationTx", "thin selected-store handoff")
	}
	edge("store/internal/backend/eventrecord::Record.ValidateInheritedFanOutOwner", "store/internal/backend/fanoutorigin::ValidateCommitted", "durable readback consumes exact committed origin relation")
	for _, backend := range []string{"postgres", "sqlite"} {
		for _, caller := range []string{"Load", "LoadMany", "LoadAdmitted", "LoadAdmittedMany"} {
			edge("store/internal/backend/eventrecord/"+backend+"::"+caller, "store/internal/backend/eventrecord::Record.ValidateInheritedFanOutOwner", "all canonical hydration validates origin ownership after closing rows")
		}
	}
	return allowed
}

func inheritedFanOutBoundaryReference(callee string) bool {
	for _, guarded := range []string{
		"events::NewInheritedFanOutOrigin",
		"events::NewInheritedFanOutEvent",
		"runtime/fanoutobligation::ValidateCommittedOrdinalEvent",
		"runtime/fanoutobligation::PrepareOrdinalEmission",
		"runtime/bus::EventBus.admitEnginePublishEvent",
		"runtime/bus::PublicationCommand.ValidateFanOut",
		"store/internal/backend/eventpersistence::commitValidatedPublicationTx",
		"store/internal/backend/eventpersistence::commitFanOutPublicationTx",
		"store/internal/backend/pipelinepersistence::eventCommitTxStore.commitFanOutPublicationTx",
		"store/internal/backend/pipelinepersistence::EventCommitOwner.CommitFanOutPublicationTx",
		"store/internal/backend/eventrecord::Record.ValidateInheritedFanOutOwner",
		"store/internal/backend/fanoutorigin::ValidateCommitted",
	} {
		if callee == guarded {
			return true
		}
	}
	return false
}

func inheritedFanOutBoundaryHostile(t *testing.T, imports historicalBoundaryImports) {
	t.Helper()
	inheritedFanOutReaderBoundaryHostile(t, imports)
	const source = `package engine
import (
    events "github.com/division-sh/swarm/internal/events"
    fanout "github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)
type Executor struct{}
type FanOutEvaluation struct{}
type unrelated struct{}
func (arbitrary *Executor) PrepareFanOutEvaluation(intent fanout.Intent, trigger events.Event) {
    _, _ = fanout.PrepareOrdinalEmission(intent, trigger, 0)
    // EXTRA_PREPARATION
}

func (arbitrary *FanOutEvaluation) EvaluateOrdinal(intent fanout.Intent, trigger events.Event) {
    prepare := fanout.PrepareOrdinalEmission
    _, _ = prepare(intent, trigger, 0)
    // EXTRA_ORDINAL
}
func (arbitrary *Executor) EvaluateFanOutOrdinal(intent fanout.Intent, trigger events.Event) {
    _, _ = fanout.PrepareOrdinalEmission(intent, trigger, 0)
}
func (arbitrary *Executor) otherMethod() {
    alias := fanout.PrepareOrdinalEmission
    _ = alias
}
func (arbitrary *unrelated) PrepareFanOutEvaluation(intent fanout.Intent, trigger events.Event) {
    _, _ = fanout.PrepareOrdinalEmission(intent, trigger, 0)
}
func (arbitrary *unrelated) EvaluateOrdinal() {
    alias := fanout.PrepareOrdinalEmission
    _ = alias
}
`
	for _, duplicate := range []bool{false, true} {
		fixture := source
		if duplicate {
			fixture = strings.ReplaceAll(fixture, "// EXTRA_PREPARATION", "_, _ = fanout.PrepareOrdinalEmission(intent, trigger, 0)")
			fixture = strings.ReplaceAll(fixture, "// EXTRA_ORDINAL", "_, _ = fanout.PrepareOrdinalEmission(intent, trigger, 0)")
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "fan_out_evaluator.go", fixture, 0)
		if err != nil {
			t.Fatal(err)
		}
		info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
		pkg, err := (&types.Config{Importer: imports}).Check(historicalBoundaryModule+"runtime/engine", fset, []*ast.File{file}, info)
		if err != nil {
			t.Fatalf("prepared-evaluator hostile fixture must type-check: %v", err)
		}
		findings := historicalBoundaryCollect(pkg, info, fset, file)
		problems := historicalBoundaryProblems(findings, historicalBoundaryAllowances(), false)
		want := []string{
			"runtime/engine::Executor.EvaluateFanOutOrdinal/reference:runtime/fanoutobligation::PrepareOrdinalEmission at ",
			"runtime/engine::Executor.otherMethod/reference:runtime/fanoutobligation::PrepareOrdinalEmission at ",
			"runtime/engine::unrelated.PrepareFanOutEvaluation/reference:runtime/fanoutobligation::PrepareOrdinalEmission at ",
			"runtime/engine::unrelated.EvaluateOrdinal/reference:runtime/fanoutobligation::PrepareOrdinalEmission at ",
		}
		if duplicate {
			want = append(want,
				"runtime/engine::Executor.PrepareFanOutEvaluation/reference:runtime/fanoutobligation::PrepareOrdinalEmission: observed 2, audited 1",
				"runtime/engine::FanOutEvaluation.EvaluateOrdinal/reference:runtime/fanoutobligation::PrepareOrdinalEmission: observed 2, audited 1",
			)
		}
		if len(problems) != len(want) {
			t.Fatalf("prepared-evaluator findings (duplicate=%t) = %v, want exactly %v", duplicate, problems, want)
		}
		for _, expected := range want {
			found := false
			for _, problem := range problems {
				found = found || strings.HasPrefix(problem, expected)
			}
			if !found {
				t.Errorf("missing prepared-evaluator finding %s in %v", expected, problems)
			}
		}
	}
}
