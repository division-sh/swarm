package main

import (
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"strings"
)

func deliveryClockStatements(raw []byte) (*token.FileSet, *ast.BlockStmt, int, *ast.FuncLit) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, deliveryConformanceFile, raw, parser.ParseComments)
	must(err)
	var block *ast.BlockStmt
	index := -1
	var callback *ast.FuncLit
	ast.Inspect(file, func(n ast.Node) bool {
		candidate, ok := n.(*ast.BlockStmt)
		if !ok {
			return true
		}
		for i, stmt := range candidate.List {
			ifStmt, ok := stmt.(*ast.IfStmt)
			if !ok || ifStmt.Init == nil || !strings.Contains(node(ifStmt.Init), "eventfixture.RunMutation(") {
				continue
			}
			if block != nil {
				panic("duplicate delivery clock proof")
			}
			block, index = candidate, i
			ast.Inspect(ifStmt.Init, func(n ast.Node) bool {
				if literal, ok := n.(*ast.FuncLit); ok {
					if callback != nil {
						panic("nested delivery clock callback")
					}
					callback = literal
					return false
				}
				return true
			})
		}
		return true
	})
	if block == nil || index < 2 || callback == nil {
		panic("missing complete delivery clock proof")
	}
	return fset, block, index, callback
}

func rewriteDeliveryRecoveryClock() {
	prior, err := exec.Command("git", "show", deliveryRecoveryPredecessor+":"+deliveryConformanceFile).Output()
	must(err)
	_, before, beforeIndex, callback := deliveryClockStatements(prior)
	raw, err := os.ReadFile(deliveryConformanceFile)
	must(err)
	fset, current, index, _ := deliveryClockStatements(raw)
	for offset := -2; offset <= 0; offset++ {
		if node(before.List[beforeIndex+offset]) != node(current.List[index+offset]) {
			panic("delivery clock proof no longer matches signed predecessor")
		}
	}
	replacement := `if err := storetest.ProveDeliveryConformanceClock(ctx,backend.selected,storetest.DeliveryConformanceClockInput{
		Event:longEvent,Snapshot:longClaimSnapshot,ClaimRoute:longClaimRoute,RetryClaim:longRetryClaim,LeaseClaim:longLeaseClaim,Failure:testFailure("long_transaction_retry"),
	});err!=nil{t.Fatalf("commit long PostgreSQL delivery mutation: %v",err)}`
	start, end := fset.Position(current.List[index-2].Pos()).Offset, fset.Position(current.List[index].End()).Offset
	candidate, err := format.Source(append(append(append([]byte{}, raw[:start]...), replacement...), raw[end:]...))
	must(err)
	const path = "internal/store/internal/backend/delivery/adapter.go"
	owner, err := os.ReadFile(path)
	must(err)
	if strings.Contains(string(owner), "func (s *DeliveryPostgresOwner) ProveDeliveryConformanceClockForTest") {
		panic("clock proof already moved")
	}
	generated := deliveryClockOwner(callback)
	owner, err = format.Source(append(owner, generated...))
	must(err)
	must(os.WriteFile(path, owner, 0644))
	must(os.WriteFile(deliveryConformanceFile, candidate, 0644))
}

func deliveryClockOwner(callback *ast.FuncLit) []byte {
	var work, checks []string
	checking := false
	for _, stmt := range callback.Body.List {
		text := node(stmt)
		if strings.HasPrefix(text, "if leaseObservation.Disposition") {
			checking = true
		}
		text = strings.ReplaceAll(text, "runtimedelivery.", "")
		text = strings.ReplaceAll(text, "adapter.ObserveContinuationInTransaction(", "adapter.ObserveContinuation(")
		text = strings.ReplaceAll(text, `testFailure("long_transaction_retry")`, "input.Failure")
		if checking {
			checks = append(checks, text)
		} else {
			work = append(work, text)
		}
	}
	if len(checks) != 10 || len(work) != 12 {
		panic(fmt.Sprintf("delivery clock cut changed: work=%d checks=%d", len(work), len(checks)))
	}
	return []byte(`
// This finite proof retains all observations and assertions in one original
// native transaction; it exports no SQL or callback aperture.
type DeliveryConformanceClockInput struct {
	Event events.Event
	Snapshot Snapshot
	ClaimRoute events.DeliveryRoute
	RetryClaim,LeaseClaim ClaimedObligation
	Failure *runtimefailures.Envelope
}
func (s *DeliveryPostgresOwner) ProveDeliveryConformanceClockForTest(ctx context.Context,input DeliveryConformanceClockInput)error{
	if err:=s.requireCurrentSchema();err!=nil{return err}
	if err:=input.Snapshot.Authority.Validate();err!=nil{return err}
	for _,claim:=range []Claim{input.RetryClaim.Claim,input.LeaseClaim.Claim}{
		if err:=claim.Validate();err!=nil{return err}
		if claim.RunID()!=input.Event.RunID(){return fmt.Errorf("delivery clock proof cannot borrow a foreign run")}
	}
	result:=mutationprotocol.RunPostgres(ctx,s.backend,mutationprotocol.Story,mutationprotocol.Ordinary,nil,nil,func(txctx context.Context,attempt *mutationprotocol.Attempt)(struct{},error){return struct{}{},s.deliveryConformanceClockAttempt(txctx,attempt,input)})
	return result.Err()
}
func (s *DeliveryPostgresOwner) deliveryConformanceClockAttempt(txctx context.Context,attempt *mutationprotocol.Attempt,input DeliveryConformanceClockInput)error{
	adapter:=s.receiverAdapter
	longEvent,longClaimSnapshot,longClaimRoute:=input.Event,input.Snapshot,input.ClaimRoute
	longRetryClaim,longLeaseClaim:=input.RetryClaim,input.LeaseClaim
` + strings.Join(work, "\n") + `
	return validateDeliveryConformanceClockProof(transactionStartedAt,longClaimed,longRetry,leaseObservation,longPage,longLeaseClaim)
}
func validateDeliveryConformanceClockProof(transactionStartedAt time.Time,longClaimed ClaimedObligation,longRetry Snapshot,leaseObservation ContinuationObservation,longPage ContinuationPage,longLeaseClaim ClaimedObligation)error{
` + strings.Join(checks, "\n") + "\n}\n")
}
