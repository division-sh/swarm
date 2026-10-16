package main

import (
	"fmt"
	"strings"
	"testing"
)

func family145GateEmptyPathRollbackProofError(source string) error {
	for _, cut := range []string{
		"validateWorkflowProjectionFaultKey(run, entity, true)", "workflowProjectionNativePostgres(selected)",
		"runWorkflowProjectionFault(ctx, selected,", "mutationlog.ProbeEmptyGatePathForTest(ctx, tx, postgres, run, entity)",
		`accepted := errors.New("roll back accepted empty-gate-path schema probe")`,
		"return accepted", "if err == accepted {\n\t\treturn nil\n\t}",
	} {
		if !strings.Contains(source, cut) {
			return fmt.Errorf("gate constraint probe lost original writer, actual outcome or exact rollback acknowledgment: %s", cut)
		}
	}
	return nil
}

func TestFamily145GateEmptyPathKeepsExactPhysicalWriteAndRollbackAcknowledgment(t *testing.T) {
	owner := family145ConversationForkOwnerSource(t, "internal/backend/mutationlog/test_missing_domain_path.go", "ProbeEmptyGatePathForTest")
	for _, cut := range []string{
		"SELECT 1 FROM entity_state WHERE run_id=$1 AND entity_id=$2", "run, entity).Scan(&present)",
		"VALUES ($1::uuid,$2::uuid,'gate','','null'::jsonb,'true'::jsonb,'platform','test','seed')",
		"VALUES ($1,$2,$3,'gate','','null','true','platform','test','seed')",
		"tx.ExecContext(ctx, query, run, entity)", "tx.ExecContext(ctx, query, uuid.NewString(), run, entity)", "return err",
	} {
		if !strings.Contains(owner, cut) {
			t.Fatalf("gate constraint probe lost exact physical domain/path/values/scope: %s", cut)
		}
	}
	if strings.Contains(owner, "fmt.Errorf") {
		t.Fatal("gate schema probe manufactures refusal after an accepted write")
	}
	bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_mutation_domain_path_fault.go", "ProbeWorkflowGateEmptyPathForTest")
	if err := family145GateEmptyPathRollbackProofError(bridge); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{"if err == accepted", "if errors.Is(err, accepted)"}, {"return accepted", "return nil"}, {"return nil", "return accepted"}} {
		if family145GateEmptyPathRollbackProofError(strings.ReplaceAll(bridge, pair[0], pair[1])) == nil {
			t.Fatalf("lost accepted-write rollback or masked cleanup failure accepted: %v", pair)
		}
	}
	constructor := family145ConversationForkOwnerSource(t, "../runtime/conformance/gate_mutation_schema_probe_test.go", "constructConformanceGateMutationSubject")
	for _, cut := range []string{
		"loadConformanceWorkflowFixtureModule(t, root).source", "bundle.SourceArtifact", "conformanceSourceArtifactFact(t, source)",
		"newFanInBarrierRuntimeForSource(t, selected, source, fact, 1)", "storetest.RequireRun(t, ctx, selected,",
		"Artifact: bundle.SourceArtifact", "native.manager.PrepareFlowInstanceActivation(ctx,", "flowidentity.Stored(source, \".\", run, run, run, \"\")",
		"native.bus.CommitFlowInstanceActivation(ctx, plan)", "!committed.Acknowledged || !committed.Created", "native.manager.FinalizeCommittedFlowInstanceActivation(ctx, committed)",
	} {
		if !strings.Contains(constructor, cut) {
			t.Fatalf("gate constraint subject lost genuine source-aware construction: %s", cut)
		}
	}
	if err := family145DescriptorReadinessEscape([]byte("package proof\n" + constructor)); err != nil {
		t.Fatal(err)
	}
}
