package contracts

import (
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

func TestFanOutClaimOwnerGuardRejectsArbitraryReceiversAndApprovedFileBypasses(t *testing.T) {
	root := handlerRuleIdentityGuardRepoRoot(t)
	path := filepath.Join(root, "internal/runtime/pipeline/fan_out_pump.go")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Put hostile code inside an otherwise approved file. Resolution must use
	// semantic receiver/function identity, not filenames or variable spellings.
	source = append(source, []byte(`
func hostileDirectClaim(arbitraryReceiver FanOutObligationOwner, ctx context.Context, req FanOutClaimRequest) {
    arbitraryReceiver.ClaimFanOutIntent(ctx, req)
}
func hostileExtractedClaim(anotherName FanOutObligationOwner) {
    _ = anotherName.ClaimFanOutIntent
}
func hostilePublicationGroup(arbitraryReceiver FanOutObligationOwner, ctx context.Context, claim fanoutobligation.Claim) {
    arbitraryReceiver.BeginFanOutPublicationGroup(ctx, claim)
}
type unrelatedFanOutNamedMethod struct{}
func (unrelatedFanOutNamedMethod) ClaimFanOutIntent() {}
func unrelatedMethodControl() { unrelatedFanOutNamedMethod{}.ClaimFanOutIntent() }
`)...)
	busPath := filepath.Join(root, "internal/runtime/bus/outbox.go")
	busSource, err := os.ReadFile(busPath)
	if err != nil {
		t.Fatal(err)
	}
	busSource = append(busSource, []byte(`
func hostileGroupClose(arbitraryReceiver runtimepipelineobligation.PublicationGroup, ctx context.Context) {
    arbitraryReceiver.Close(ctx)
}
func hostileGroupPreparation(anotherName runtimepipelineobligation.PublicationGroup) {
    _ = anotherName.RecordPrepared
}
func hostileOneAtATimeGroupClaims(anotherName runtimepipelineobligation.PublicationGroup) {
    _ = anotherName.Claim
}
func hostileGroupMembership(anotherName runtimepipelineobligation.PublicationGroup) {
    _ = anotherName.ValidateCommittedMembership
}
`)...)
	observationPath := filepath.Join(root, "internal/runtime/startupownership/fan_out_readback.go")
	observationSource, err := os.ReadFile(observationPath)
	if err != nil {
		t.Fatal(err)
	}
	observationSource = append(observationSource, []byte(`
func hostileRegistrationSelection(owner FanOutServingStore, ctx context.Context) {
    owner.NextFanOutCandidate(ctx, nil, nil)
}
func hostileRegistrationReadback(owner fanOutExecutionObserver) {
    _ = owner.ObserveFanOutExecutions
}
`)...)
	pkgs, err := packages.Load(&packages.Config{
		Dir: root, Overlay: map[string][]byte{path: source, busPath: busSource, observationPath: observationSource},
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
	}, "./internal/runtime/...", "./internal/store/...", "./internal/operatorread/...", "./internal/apiv1/...")
	if err != nil || packages.PrintErrors(pkgs) != 0 {
		t.Fatalf("load fan-out owner census: %v", err)
	}
	const pipelinePath = "github.com/division-sh/swarm/internal/runtime/pipeline"
	allowed := map[string]bool{
		pipelinePath + ".PipelineCoordinator.claimAndServeFanOutTurn":                                               true,
		"github.com/division-sh/swarm/internal/runtime/startupownership.fanOutObservedClaimOwner.ClaimFanOutIntent": true,
	}
	hostile := map[string]bool{
		pipelinePath + ".hostileDirectClaim":    false,
		pipelinePath + ".hostileExtractedClaim": false,
	}
	const busPackage = "github.com/division-sh/swarm/internal/runtime/bus"
	const obligationPackage = "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	const startupPackage = "github.com/division-sh/swarm/internal/runtime/startupownership"
	observationAllowed := map[string]map[string]bool{
		startupPackage + ".FanOutServingStore.NextFanOutCandidate": {
			startupPackage + ".fanOutServingService.refill":                    true,
			startupPackage + ".fanOutServingService.detectMissedOpportunities": true,
		},
		startupPackage + ".fanOutExecutionObserver.ObserveFanOutExecutions": {
			startupPackage + ".ObserveFanOutRuntimePage": true,
		},
	}
	observationHostile := map[string]bool{
		startupPackage + ".hostileRegistrationSelection": false,
		startupPackage + ".hostileRegistrationReadback":  false,
	}
	groupAllowed := map[string]string{
		obligationPackage + ".PublicationGroup.ClaimBatch":                  busPackage + ".EventBus.PrepareFanOutPublications",
		pipelinePath + ".FanOutObligationOwner.BeginFanOutPublicationGroup": pipelinePath + ".PipelineCoordinator.claimAndServeFanOutTurn",
		obligationPackage + ".PublicationGroup.Claim":                       "",
		obligationPackage + ".PublicationGroup.RecordPrepared":              busPackage + ".EventBus.prepareEnginePublicationsWithMember",
		obligationPackage + ".PublicationGroup.Seal":                        busPackage + ".EventBus.SealFanOutPublications",
		obligationPackage + ".PublicationGroup.ValidateCommitted":           busPackage + ".EventBus.validateCommittedFanOutPublications",
		obligationPackage + ".PublicationGroup.ValidateCommittedMembership": busPackage + ".EventBus.validateCommittedFanOutPublications",
		obligationPackage + ".PublicationGroup.Settle":                      busPackage + ".fanOutPublicationSettlement.flushLocked",
		obligationPackage + ".PublicationGroup.ReadPublicationSettlement":   busPackage + ".fanOutPublicationSettlement.flushLocked",
		obligationPackage + ".PublicationGroup.Close":                       pipelinePath + ".PipelineCoordinator.claimAndServeFanOutTurn",
	}
	groupHostile := map[string]bool{
		busPackage + ".hostileOneAtATimeGroupClaims": false,
		pipelinePath + ".hostilePublicationGroup":    false,
		busPackage + ".hostileGroupClose":            false,
		busPackage + ".hostileGroupPreparation":      false,
		busPackage + ".hostileGroupMembership":       false,
	}
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			for _, declaration := range file.Decls {
				fn, ok := declaration.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				function, ok := pkg.TypesInfo.Defs[fn.Name].(*types.Func)
				if !ok {
					t.Fatalf("missing resolved function for %s", fn.Name.Name)
				}
				owner := fanOutGuardFunctionIdentity(function)
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					selector, ok := node.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					method, ok := pkg.TypesInfo.Uses[selector.Sel].(*types.Func)
					if ok && method.Pkg() != nil {
						if consumers, guarded := observationAllowed[fanOutGuardFunctionIdentity(method)]; guarded && !consumers[owner] {
							if _, expected := observationHostile[owner]; expected {
								observationHostile[owner] = true
							} else {
								t.Errorf("unauthorized registration observation consumer %s at %s", owner, pkg.Fset.Position(selector.Pos()))
							}
						}
						if allowedOwner, guarded := groupAllowed[fanOutGuardFunctionIdentity(method)]; guarded && owner != allowedOwner {
							if _, expected := groupHostile[owner]; expected {
								groupHostile[owner] = true
							} else {
								t.Errorf("unauthorized publication group consumer %s at %s", owner, pkg.Fset.Position(selector.Pos()))
							}
						}
					}
					if !ok || method.Name() != "ClaimFanOutIntent" || method.Pkg() == nil || method.Pkg().Path() != pipelinePath {
						return true
					}
					signature := method.Type().(*types.Signature)
					if signature.Recv() == nil || fanOutGuardReceiverName(signature.Recv().Type()) != "FanOutObligationOwner" {
						return true
					}
					if _, expectedHostile := hostile[owner]; expectedHostile {
						hostile[owner] = true
					} else if !allowed[owner] {
						t.Errorf("unauthorized fan-out claim consumer %s at %s", owner, pkg.Fset.Position(selector.Pos()))
					}
					return true
				})
			}
		}
		if strings.HasSuffix(pkg.PkgPath, "/store/internal/runtimepersistence") {
			for _, typeName := range []string{"PostgresStore", "SQLiteRuntimeStore"} {
				object := pkg.Types.Scope().Lookup(typeName)
				if object == nil {
					t.Fatalf("missing selected-store facade %s", typeName)
				}
				for _, name := range []string{"ClaimFanOutIntent", "BeginFanOutPublicationGroup", "LoadFanOutEvaluation", "CommitFanOutChunk", "ReleaseFanOutClaim", "ReleaseFanOutRetryable", "BlockFanOutClaim"} {
					if method, _, _ := types.LookupFieldOrMethod(types.NewPointer(object.Type()), true, pkg.Types, name); method != nil {
						t.Errorf("ungranted serving method remains on public selected-store facade: %s.%s", typeName, name)
					}
				}
			}
		}
	}
	for owner, detected := range hostile {
		if !detected {
			t.Errorf("semantic owner guard missed %s", owner)
		}
	}
	for owner, detected := range groupHostile {
		if !detected {
			t.Errorf("semantic group guard missed %s", owner)
		}
	}
	for owner, detected := range observationHostile {
		if !detected {
			t.Errorf("semantic observation guard missed %s", owner)
		}
	}
}

func fanOutGuardReceiverName(value types.Type) string {
	if pointer, ok := value.(*types.Pointer); ok {
		value = pointer.Elem()
	}
	named, ok := types.Unalias(value).(*types.Named)
	if !ok {
		return ""
	}
	return named.Obj().Name()
}

func fanOutGuardFunctionIdentity(fn *types.Func) string {
	name := fn.Pkg().Path() + "."
	if receiver := fn.Type().(*types.Signature).Recv(); receiver != nil {
		name += fanOutGuardReceiverName(receiver.Type()) + "."
	}
	return name + fn.Name()
}
