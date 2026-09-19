package runtimepersistence

import (
	"errors"
	"fmt"
	"go/ast"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var eventBusGroupMutationMethods = []string{
	"PrepareFanOutPublications", "PrepareFanOutPublication", "SealFanOutPublications",
	"FinalizeFanOutPublications", "DispatchFanOutPublications", "BeginRunStop",
}

func validateEventBusSourceOperationCensus(sources map[string]string, ledger map[string]string) error {
	found := map[string]bool{}
	var problems []error
	for path, source := range sources {
		functions, err := revisionGuardFunctions(source)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		for symbol := range functions {
			method, ok := strings.CutPrefix(symbol, "EventBus.")
			if !ok || !ast.IsExported(method) {
				continue
			}
			if found[method] {
				problems = append(problems, fmt.Errorf("duplicate EventBus operation %s", method))
			}
			found[method] = true
			switch category, ok := ledger[method]; {
			case !ok:
				problems = append(problems, fmt.Errorf("%s is an unclassified exported EventBus operation", method))
			case category != operationMutation && category != operationRetained && category != operationAdmittedChild && category != operationPureRead:
				problems = append(problems, fmt.Errorf("%s has unknown EventBus operation category %q", method, category))
			}
		}
	}
	for method := range ledger {
		if !found[method] {
			problems = append(problems, fmt.Errorf("classified EventBus operation %s no longer exists", method))
		}
	}
	// These operations mutate the admitted group, runtime handoff, or parent
	// transition. A retained handle does not make their consumers pure reads.
	for _, method := range eventBusGroupMutationMethods {
		if ledger[method] != operationMutation {
			problems = append(problems, fmt.Errorf("%s must remain a classified mutation", method))
		}
	}
	return errors.Join(problems...)
}

type eventBusSourceConsumerContract struct {
	path, method string
	statements   []string
}

func eventBusGroupSourceConsumerContracts() []eventBusSourceConsumerContract {
	return []eventBusSourceConsumerContract{
		{"fan_out_publication_group.go", "PrepareFanOutPublications", []string{
			`if err := flushEnclosingPublicationSettlement(ctx); err != nil { return nil, err }`,
			`preparedCtx, admitted, err := eb.admitEnginePublishEvent(events.WithDeliveryContext(ctx, request.Intent.Context), request.Intent.Event)`,
			`if err != nil { results[i].Err = err; continue }`,
			`issued, err := group.ClaimBatch(ctx, claims)`,
			`if err != nil { return nil, err }`,
			`plans, err := eb.prepareEnginePublicationsWithMember(ctx, []runtimeengine.EmitIntent{requests[i].Intent}, runtimepipeline.PreparedWorkflowPublicationState{}, &members[i])`,
		}},
		{"fan_out_publication_group.go", "PrepareFanOutPublication", []string{
			`results, err := eb.PrepareFanOutPublications(ctx, group, []runtimepipeline.FanOutPublicationRequest{{Ordinal: ordinal, Intent: intent}})`,
			`if err != nil { return nil, err }`,
			`return results[0].Publication, results[0].Err`,
		}},
		{"fan_out_publication_group.go", "SealFanOutPublications", []string{
			`plan, ok := value.(EnginePublicationPlan)`,
			`if err := plan.ValidateDurablePublicationPlan(); err != nil { return err }`,
			`claims = append(claims, plan.prepared.publicationClaim.Claim())`,
			`return group.Seal(ctx, end, claims)`,
		}},
		{"fan_out_publication_group.go", "validateCommittedFanOutPublications", []string{
			`publication, ok := value.(CommittedEnginePublication)`,
			`if !ok || publication.plan.prepared.publicationClaim == nil || publication.plan.prepared.publicationClaim.bus != eb { return nil, errors.New("fan-out committed evidence requires exact bus-owned publication claims") }`,
			`if err := publication.ValidateCommittedDurablePublication(); err != nil { return nil, err }`,
			`claims = append(claims, publication.plan.prepared.publicationClaim.Claim())`,
			`if err := group.ValidateCommittedMembership(claims); err != nil { return nil, err }`,
			`return retire, group.ValidateCommitted(ctx, claims)`,
		}},
		{"fan_out_publication_group.go", "FinalizeFanOutPublications", []string{
			`retire, err := eb.validateCommittedFanOutPublications(ctx, group, values)`,
			`if err != nil { return err }`,
			`return eb.FinalizeEnginePublications(ctx, values)`,
		}},
		{"fan_out_publication_group.go", "DispatchFanOutPublications", []string{
			`retire, err := eb.validateCommittedFanOutPublications(ctx, group, values)`,
			`if err != nil { return err }`,
			`if err := flushEnclosingPublicationSettlement(ctx); err != nil { return err }`,
			`ctx, err = eb.admitSourceArtifactFact(ctx)`,
			`if err != nil { return err }`,
			`ctx, lease, err := eb.beginRuntimeWork(ctx)`,
			`if err != nil { return err }`,
			`if err := committed.ValidateCommittedDurablePublication(); err != nil { return err }`,
			`operation, found, takeErr := eb.takeFanOutOutboxOperation(committed)`,
			`if takeErr != nil { return takeErr }`,
			`if err := dispatcher.dispatchFanOutOperation(ctx, operation, settlement); err != nil { return err }`,
		}},
		{"run_stop.go", "BeginRunStop", []string{
			`owner, ok := eb.pipelineObligations.(pipelineobligation.ParentTransitionOwner)`,
			`if !ok { return nil, errors.New("run stop requires selected-store pipeline parent transition admission") }`,
			`return owner.BeginParentTransition(ctx, runID)`,
		}},
	}
}

func validateEventBusGroupSourceConsumers(sources map[string]string) error {
	for _, contract := range eventBusGroupSourceConsumerContracts() {
		functions, err := revisionGuardFunctions(sources[contract.path])
		if err != nil {
			return err
		}
		fn := functions["EventBus."+contract.method]
		if fn == nil {
			return fmt.Errorf("missing EventBus source consumer %s", contract.method)
		}
		var statements []ast.Stmt
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			// A deferred/uninvoked closure or sibling method is not admission
			// of the operation being guarded.
			if _, closure := node.(*ast.FuncLit); closure {
				return false
			}
			if statement, ok := node.(ast.Stmt); ok {
				statements = append(statements, statement)
			}
			return true
		})
		index := 0
		for _, required := range contract.statements {
			parsed, err := revisionGuardFunctions("package p; func f() { " + required + " }")
			if err != nil {
				return fmt.Errorf("invalid source consumer contract: %w", err)
			}
			want := revisionGuardNode(parsed["f"].Body.List[0])
			for index < len(statements) && revisionGuardNode(statements[index]) != want {
				index++
			}
			if index == len(statements) {
				return fmt.Errorf("%s lost ordered exact authority consumer %s", contract.method, required)
			}
			index++
		}
	}
	return nil
}

func TestRepositoryEventBusSourceOperationLedgerHostileControls(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	sources := map[string]string{}
	for _, path := range []string{"fan_out_publication_group.go", "run_stop.go"} {
		body, err := os.ReadFile(filepath.Join(root, "internal/runtime/bus", path))
		if err != nil {
			t.Fatal(err)
		}
		sources[path] = string(body)
	}
	if err := validateEventBusGroupSourceConsumers(sources); err != nil {
		t.Fatalf("real source positive control: %v", err)
	}
	ledger := map[string]string{}
	for _, method := range eventBusGroupMutationMethods {
		ledger[method] = operationMutation
	}
	if err := validateEventBusSourceOperationCensus(sources, ledger); err != nil {
		t.Fatalf("six-method census positive control: %v", err)
	}
	for _, method := range eventBusGroupMutationMethods {
		t.Run("missing_classification/"+method, func(t *testing.T) {
			delete(ledger, method)
			defer func() { ledger[method] = operationMutation }()
			if err := validateEventBusSourceOperationCensus(sources, ledger); err == nil {
				t.Fatal("missing method classification accepted")
			}
		})
		t.Run("false_pure_read/"+method, func(t *testing.T) {
			ledger[method] = operationPureRead
			defer func() { ledger[method] = operationMutation }()
			if err := validateEventBusSourceOperationCensus(sources, ledger); err == nil {
				t.Fatal("mutation reclassified as pure read")
			}
		})
	}
	for _, tc := range []struct{ name, path, before, after string }{
		{"new_unclassified_export", "run_stop.go", "func (eb *EventBus) BeginRunStop", "func (eb *EventBus) OtherRunStop"},
		{"wrong_event", "fan_out_publication_group.go", "request.Intent.Event)", "foreignEvent)"},
		{"wrong_batch", "fan_out_publication_group.go", "group.ClaimBatch(ctx, claims)", "group.ClaimBatch(ctx, nil)"},
		{"wrong_member", "fan_out_publication_group.go", "&members[i])", "nil)"},
		{"singleton_wrong_group", "fan_out_publication_group.go", "eb.PrepareFanOutPublications(ctx, group,", "eb.PrepareFanOutPublications(ctx, nil,"},
		{"seal_wrong_claims", "fan_out_publication_group.go", "group.Seal(ctx, end, claims)", "group.Seal(ctx, end, nil)"},
		{"seal_comment_only", "fan_out_publication_group.go", "return group.Seal(ctx, end, claims)", "return nil // group.Seal(ctx, end, claims)"},
		{"seal_closure_only", "fan_out_publication_group.go", "return group.Seal(ctx, end, claims)", "unused := func() { group.Seal(ctx, end, claims) }; _ = unused; return nil"},
		{"seal_ignored_validation", "fan_out_publication_group.go", "if err := plan.ValidateDurablePublicationPlan(); err != nil {\n\t\t\treturn err\n\t\t}", "_ = plan.ValidateDurablePublicationPlan()"},
		{"foreign_bus", "fan_out_publication_group.go", "publication.plan.prepared.publicationClaim.bus != eb", "publication.plan.prepared.publicationClaim.bus == nil"},
		{"membership_wrong_claims", "fan_out_publication_group.go", "group.ValidateCommittedMembership(claims)", "group.ValidateCommittedMembership(nil)"},
		{"commit_wrong_context", "fan_out_publication_group.go", "group.ValidateCommitted(ctx, claims)", "group.ValidateCommitted(context.Background(), claims)"},
		{"finalize_wrong_values", "fan_out_publication_group.go", "eb.FinalizeEnginePublications(ctx, values)", "eb.FinalizeEnginePublications(ctx, nil)"},
		{"dispatch_wrong_source", "fan_out_publication_group.go", "eb.admitSourceArtifactFact(ctx)", "other.admitSourceArtifactFact(ctx)"},
		{"dispatch_wrong_operation", "fan_out_publication_group.go", "dispatcher.dispatchFanOutOperation(ctx, operation, settlement)", "dispatcher.dispatchFanOutOperation(ctx, other, settlement)"},
		{"stop_wrong_run", "run_stop.go", "owner.BeginParentTransition(ctx, runID)", "owner.BeginParentTransition(ctx, otherRun)"},
		{"stop_wrong_owner", "run_stop.go", "eb.pipelineObligations.(pipelineobligation.ParentTransitionOwner)", "other.(pipelineobligation.ParentTransitionOwner)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := sources[tc.path]
			if !strings.Contains(original, tc.before) {
				t.Fatal("hostile control did not change source")
			}
			sources[tc.path] = strings.Replace(original, tc.before, tc.after, 1)
			defer func() { sources[tc.path] = original }()
			if err := validateEventBusSourceOperationCensus(sources, ledger); err == nil {
				err = validateEventBusGroupSourceConsumers(sources)
				if err == nil {
					t.Fatal("hostile source consumer accepted")
				}
			}
		})
	}
}
