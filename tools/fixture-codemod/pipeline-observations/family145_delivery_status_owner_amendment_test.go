package main

import (
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

const repairedDeliveryStatusBridge = `func ReadServedDeliveryStatusCountForTest(ctx context.Context, selected any, eventID, subscriberType, subscriberID string, statuses ...string) (int, error) {
if err:=validateChannelObservationOwner(selected); err!=nil { return 0,err }
_,postgres:=selected.(*PostgresStore)
var count int
err:=readServedDeliveryObservation(ctx,selected,func(ctx context.Context,tx *sql.Tx)error {
var err error; count,err=delivery.ReadServedDeliveryStatusCount(ctx,tx,postgres,eventID,subscriberType,subscriberID,statuses...); return err
})
if err!=nil { return 0,err }; return count,nil
}`

// Historical whole-file evidence composes only these named, separately approved
// amendments. Every byte outside the exact declarations still matches history.
func deliveryStatusEarlierOwnerSource(t *testing.T, path string, current, original []byte) []byte {
	t.Helper()
	var err error
	switch path {
	case "internal/store/internal/backend/delivery/read_projections.go":
		current, err = remapDeliveryStatusDeclaration(current, "StandingDeliveryDiagnostic", `type StandingDeliveryDiagnostic struct{ EventID, Subscriber, Status string }`, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, endpoint := range standingDiagnosticManifest.Endpoints {
			if endpoint.File == path && endpoint.Function == "ReadStandingDeliveryDiagnosticsTx" {
				current, err = remapDeliveryStatusDeclaration(current, endpoint.Function, endpoint.After, "")
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		current = deliveryStatusEarlierStopOwner(t, path, current, original)
	case "internal/store/internal/runtimepersistence/test_served_delivery_observation.go":
		prior, _, _, lookupErr := deliveryStatusSourceDeclaration(original, "ReadServedDeliveryStatusCountForTest")
		if lookupErr != nil {
			t.Fatal(lookupErr)
		}
		current, err = remapDeliveryStatusDeclaration(current, "ReadServedDeliveryStatusCountForTest", repairedDeliveryStatusBridge, prior)
		if err != nil {
			t.Fatal(err)
		}
	}
	return current
}

func deliveryStatusEarlierStopOwner(t *testing.T, path string, current, original []byte) []byte {
	t.Helper()
	for _, pin := range stopWitnessPins(t) {
		if pin.File != path || pin.Function != "ReadStopPendingDeliveryCountTx" {
			continue
		}
		prior, _, _, err := deliveryStatusSourceDeclaration(original, pin.Function)
		if err != nil {
			t.Fatal(err)
		}
		current, err = remapDeliveryStatusDeclaration(current, pin.Function, pin.After, prior)
		if err != nil {
			t.Fatal(err)
		}
	}
	return current
}

func remapDeliveryStatusDeclaration(raw []byte, name, expected, prior string) ([]byte, error) {
	current, start, end, err := deliveryStatusSourceDeclaration(raw, name)
	if err != nil {
		return nil, err
	}
	if canonicalDeliveryStatusDeclaration(current) != canonicalDeliveryStatusDeclaration(expected) {
		return nil, fmt.Errorf("unreviewed delivery status endpoint amendment: %s", name)
	}
	out := append(append(append([]byte{}, raw[:start]...), prior...), raw[end:]...)
	return format.Source(out)
}

func canonicalDeliveryStatusDeclaration(raw string) string {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", "package proof\n"+raw, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return "invalid declaration: " + raw
	}
	return requestkey.Node(file.Decls[0])
}

func deliveryStatusSourceDeclaration(raw []byte, name string) (string, int, int, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "source.go", raw, parser.AllErrors)
	if err != nil {
		return "", 0, 0, err
	}
	var matched ast.Decl
	for _, declaration := range file.Decls {
		if !deliveryStatusDeclarationNamed(declaration, name) {
			continue
		}
		if matched != nil {
			return "", 0, 0, fmt.Errorf("ambiguous delivery status amendment: %s", name)
		}
		matched = declaration
	}
	if matched == nil {
		return "", 0, 0, fmt.Errorf("missing delivery status amendment: %s", name)
	}
	start, end := fset.Position(matched.Pos()).Offset, fset.Position(matched.End()).Offset
	return string(raw[start:end]), start, end, nil
}

func deliveryStatusDeclarationNamed(declaration ast.Decl, name string) bool {
	if fn, ok := declaration.(*ast.FuncDecl); ok {
		return fn.Recv == nil && fn.Name.Name == name
	}
	if group, ok := declaration.(*ast.GenDecl); ok && group.Tok == token.TYPE && len(group.Specs) == 1 {
		typeSpec, ok := group.Specs[0].(*ast.TypeSpec)
		return ok && typeSpec.Name.Name == name
	}
	return false
}

func TestFamily145DeliveryStatusEndpointCompositionRejectsDrift(t *testing.T) {
	current := []byte("package proof\n" + repairedDeliveryStatusBridge)
	if _, err := remapDeliveryStatusDeclaration(current, "ReadServedDeliveryStatusCountForTest", repairedDeliveryStatusBridge, ""); err != nil {
		t.Fatal(err)
	}
	for _, cut := range []string{"validateChannelObservationOwner", "selected.(*PostgresStore)", "readServedDeliveryObservation", "statuses...", "subscriberType", "eventID", "return 0, err"} {
		normal, err := format.Source(current)
		if err != nil || !strings.Contains(string(normal), cut) {
			t.Fatal("missing delivery status authority/lookup/refusal counterexample:", cut, err)
		}
		mutant := []byte(strings.Replace(string(normal), cut, "unreviewedStatusEndpoint", 1))
		if _, err := remapDeliveryStatusDeclaration(mutant, "ReadServedDeliveryStatusCountForTest", repairedDeliveryStatusBridge, ""); err == nil {
			t.Fatal("changed delivery status endpoint accepted:", cut)
		}
	}
	for _, pin := range stopWitnessPins(t) {
		if pin.Function != "ReadStopPendingDeliveryCountTx" {
			continue
		}
		mutant := strings.Replace(pin.After, "'pending','in_progress','failed'", "'pending'", 1)
		if mutant == pin.After {
			t.Fatal("missing stop status narrowing counterexample")
		}
		if _, err := remapDeliveryStatusDeclaration([]byte("package proof\n"+mutant), pin.Function, pin.After, pin.Before); err == nil {
			t.Fatal("changed stop predicate accepted as historical correspondence")
		}
	}
}
