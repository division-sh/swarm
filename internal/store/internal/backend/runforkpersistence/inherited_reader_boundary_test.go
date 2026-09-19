package runforkpersistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"
)

func inheritedFanOutReaderBoundaryHostile(t *testing.T, imports historicalBoundaryImports) {
	t.Helper()
	groupedAdmissionBoundaryHostile(t, imports)
	const source = `package adapter
import record "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
type unrelated struct{}
func (unrelated) ValidateInheritedFanOutOwner() {}
func LoadAdmitted(arbitrary record.Record) {
    _ = arbitrary.ValidateInheritedFanOutOwner(nil, nil, false)
    // EXTRA
}
func LoadAdmittedMany(arbitrary record.Record) {
    validate := arbitrary.ValidateInheritedFanOutOwner
    _ = validate(nil, nil, false)
    // EXTRA
}
func otherReader(arbitrary record.Record) {
    _ = arbitrary.ValidateInheritedFanOutOwner(nil, nil, false)
}
func sameSpelling(arbitrary unrelated) { arbitrary.ValidateInheritedFanOutOwner() }
`
	for _, backend := range []string{"postgres", "sqlite"} {
		for _, duplicate := range []bool{false, true} {
			fixture := source
			if duplicate {
				fixture = strings.ReplaceAll(fixture, "// EXTRA", "_ = arbitrary.ValidateInheritedFanOutOwner(nil, nil, false)")
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "admitted_batch.go", fixture, 0)
			if err != nil {
				t.Fatal(err)
			}
			info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
			scope := "store/internal/backend/eventrecord/" + backend + "::"
			pkg, err := (&types.Config{Importer: imports}).Check(historicalBoundaryModule+"store/internal/backend/eventrecord/"+backend, fset, []*ast.File{file}, info)
			if err != nil {
				t.Fatal(err)
			}
			problems := historicalBoundaryProblems(historicalBoundaryCollect(pkg, info, fset, file), historicalBoundaryAllowances(), false)
			callee := "/reference:store/internal/backend/eventrecord::Record.ValidateInheritedFanOutOwner"
			want := []string{scope + "otherReader" + callee + " at "}
			if duplicate {
				want = append(want, scope+"LoadAdmitted"+callee+": observed 2, audited 1", scope+"LoadAdmittedMany"+callee+": observed 2, audited 1")
			}
			if len(problems) != len(want) {
				t.Fatalf("%s duplicate=%v reader findings=%v want=%v", backend, duplicate, problems, want)
			}
			for _, expected := range want {
				found := false
				for _, problem := range problems {
					found = found || strings.HasPrefix(problem, expected)
				}
				if !found {
					t.Errorf("missing reader boundary finding %s: %v", expected, problems)
				}
			}
		}
	}
}

func groupedAdmissionBoundaryHostile(t *testing.T, imports historicalBoundaryImports) {
	t.Helper()
	for _, tc := range []struct {
		pkg, source, callee string
		approved            []string
		refused             string
	}{
		{
			pkg: "events", callee: "events::restoreDeliveryMaterialization",
			approved: []string{"RestoreReceiverMaterializationRecord", "RestoreDeliveryMaterialization"}, refused: "otherDecoder",
			source: `package events
func restoreDeliveryMaterialization() {}
func RestoreReceiverMaterializationRecord() { restoreDeliveryMaterialization() }
func RestoreDeliveryMaterialization() { restore := restoreDeliveryMaterialization; restore() }
func otherDecoder() { restoreDeliveryMaterialization() }
func localSpelling() { restoreDeliveryMaterialization := func() {}; restoreDeliveryMaterialization() }
`,
		},
		{
			pkg: "runtime/bus", callee: "runtime/bus::EventBus.admitEnginePublishEvent",
			approved: []string{"EventBus.PrepareFanOutPublications", "EventBus.prepareEnginePublicationsWithMember"}, refused: "EventBus.prepareEnginePublications",
			source: `package bus
type EventBus struct{}
type unrelated struct{}
func (*EventBus) admitEnginePublishEvent() {}
func (*unrelated) admitEnginePublishEvent() {}
func (arbitrary *EventBus) PrepareFanOutPublications() { arbitrary.admitEnginePublishEvent() }
func (arbitrary *EventBus) prepareEnginePublicationsWithMember() { admit := arbitrary.admitEnginePublishEvent; admit() }
func (arbitrary *EventBus) prepareEnginePublications() { arbitrary.admitEnginePublishEvent() }
func (arbitrary *unrelated) PrepareFanOutPublications() { arbitrary.admitEnginePublishEvent() }
`,
		},
		{
			pkg: "store/internal/backend/pipelinepersistence", callee: "runtime/fanoutobligation::ValidateCommittedOrdinalEvent",
			approved: []string{"publicationGroup.ClaimBatch"}, refused: "otherGroup.ClaimBatch",
			source: `package pipelinepersistence
import fanout "github.com/division-sh/swarm/internal/runtime/fanoutobligation"
type publicationGroup struct{}
type otherGroup struct{}
func (*publicationGroup) ClaimBatch() { validate := fanout.ValidateCommittedOrdinalEvent; _ = validate }
func (*otherGroup) ClaimBatch() { _ = fanout.ValidateCommittedOrdinalEvent }
`,
		},
	} {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, "approved_owner.go", tc.source, 0)
		if err != nil {
			t.Fatal(err)
		}
		info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
		pkg, err := (&types.Config{Importer: imports}).Check(historicalBoundaryModule+tc.pkg, fset, []*ast.File{file}, info)
		if err != nil {
			t.Fatal(err)
		}
		findings := historicalBoundaryCollect(pkg, info, fset, file)
		problems := historicalBoundaryProblems(findings, historicalBoundaryAllowances(), false)
		if len(problems) != 1 || !strings.HasPrefix(problems[0], tc.pkg+"::"+tc.refused+"/reference:"+tc.callee+" at ") {
			t.Fatalf("%s competing admission findings: %v", tc.pkg, problems)
		}
		for _, caller := range tc.approved {
			extra := historicalBoundaryFinding{Scope: tc.pkg + "::" + caller, Kind: "reference:" + tc.callee, Site: "extra approved-owner reference"}
			duplicated := append(append([]historicalBoundaryFinding(nil), findings...), extra)
			if got := historicalBoundaryProblems(duplicated, historicalBoundaryAllowances(), false); len(got) != 2 {
				t.Fatalf("%s extra reference must be rejected: %v", caller, got)
			}
		}
	}
}
