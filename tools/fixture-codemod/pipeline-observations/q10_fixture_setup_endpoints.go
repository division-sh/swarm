package main

import (
	"fmt"
	"strings"
)

type publicationSuccessorEdit struct{ before, after string }

// Keep the actual codemod and its whole-source proofs on the same exact cuts.
func q10FixtureSetupCurrentSource(row recipe) (string, error) {
	current := row.Successor
	if current == "" {
		current = row.After
	}
	var edits []publicationSuccessorEdit
	switch row.File + "/" + row.Function {
	case "internal/runtime/final_flow_instance_authoring_runtime_test.go/TestFinalFlowInstanceAuthoringRuntime_PublishActivatesAndExecutesSelectedTemplateInstance":
		edits = []publicationSuccessorEdit{{
			"\t\tSELECT COUNT(*) FROM entity_state\n\t\tWHERE flow_instance LIKE 'account/%'\n\t\t  AND current_state = 'reviewed'\n\t\t  AND fields @> $1::jsonb\n",
			"\t\tSELECT COUNT(*) FROM entity_state es JOIN flow_instances fi\n\t\t  ON fi.run_id=es.run_id AND fi.instance_path=es.flow_instance AND fi.entity_id=es.entity_id\n\t\tWHERE es.flow_instance LIKE 'account/%'\n\t\t  AND fi.current_state = 'reviewed'\n\t\t  AND es.fields @> $1::jsonb\n",
		}}
	case "internal/runtime/runforkexecution/activity_fork_execution_test.go/seedSource":
		edits = []publicationSuccessorEdit{
			{"source events.RoutingSource) {", "source events.RoutingSource) events.Event {"},
			{"\t\tt.Fatalf(\"construct activity source tree: committed=%+v err=%v\", committed, err)\n\t}\n}", "\t\tt.Fatalf(\"construct activity source tree: committed=%+v err=%v\", committed, err)\n\t}\n\treturn event\n}"},
		}
	case "internal/runtime/runforkexecution/execution_test.go/seedSelectedExecutionSourceRunWithPrimaryRouteModeAndSource":
		edits = []publicationSuccessorEdit{
			{"\t\t\tcurrent_state, gates, fields, accumulator, revision,\n\t\t\tentered_state_at, created_at, updated_at\n", "\t\t\tfields, created_at, updated_at\n"},
			{"\t\t\t'pending', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1,\n\t\t\t$4, $4, $4\n", "\t\t\t'{}'::jsonb, $4, $4\n"},
		}
	case "internal/runtime/pipeline/selection_retry_cas_external_test.go/TestSelectionRetryAfterRealCASConflictBothStores":
		edits = []publicationSuccessorEdit{
			{"fault := &selectionCASPersistence{WorkflowPersistenceOwner: selected.events.(pipeline.WorkflowPersistenceOwner), db: selected.db}", "fault := &selectionCASPersistence{WorkflowPersistenceOwner: selected.events.(pipeline.WorkflowPersistenceOwner)}"},
			{"initialCoordinator := newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: module})", "initialCoordinator := newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: module, SourceArtifactFact: sourceFact})"},
		}
	case "internal/runtime/tools/executor_entity_test.go/TestEntityTools_SaveEntityFieldAfterForkActivationUsesForkRunID":
		edits = []publicationSuccessorEdit{{"\t\tSELECT fields->>'status', revision\n\t\tFROM entity_state\n\t\tWHERE run_id = $1::uuid AND entity_id = $2::uuid\n", "\t\tSELECT es.fields->>'status', fi.revision\n\t\tFROM entity_state es JOIN flow_instances fi\n\t\t  ON fi.run_id=es.run_id AND fi.entity_id=es.entity_id AND fi.instance_path=es.flow_instance\n\t\tWHERE es.run_id = $1::uuid AND es.entity_id = $2::uuid\n"}}
	case "internal/runtime/pipeline/prospective_publication_external_test.go/TestProspectivePublicationRealPlannerTerminalAndOrdinaryWriterFenceBothStores":
		edits = []publicationSuccessorEdit{
			{"\t\t\t\tvar initialFields string\n\t\t\t\tvar initialRevision int64\n\t\t\t\tif err := selected.db.QueryRow(`SELECT CAST(fields AS TEXT), revision FROM entity_state WHERE run_id=$1`, runID).Scan(&initialFields, &initialRevision); err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n",
				"\t\t\t\tinitial, err := storetest.ReadWorkflowDuplicateProjectionStorage(ctx, selected.events, runID, runID)\n\t\t\t\tif err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n\t\t\t\tinitialFields, initialRevision := string(initial.Fields), int64(initial.Revision)\n\t\t\t\tif initialRevision != 1 {\n\t\t\t\t\tt.Fatalf(\"construction header revision=%d, want 1\", initialRevision)\n\t\t\t\t}\n"},
			{"\t\t\t\t\tvar fields string\n\t\t\t\t\tvar fieldRevision, headerRevision int64\n\t\t\t\t\tif err := selected.db.QueryRow(`SELECT CAST(fields AS TEXT), revision FROM entity_state WHERE run_id=$1`, runID).Scan(&fields, &fieldRevision); err != nil {\n\t\t\t\t\t\tt.Fatal(err)\n\t\t\t\t\t}\n\t\t\t\t\tif err := selected.db.QueryRow(`SELECT revision FROM flow_instances WHERE run_id=$1`, runID).Scan(&headerRevision); err != nil {\n\t\t\t\t\t\tt.Fatal(err)\n\t\t\t\t\t}\n\t\t\t\t\tif fields != initialFields || fieldRevision != initialRevision || headerRevision != initialRevision {\n\t\t\t\t\t\tt.Fatalf(\"failed handler changed construction: fields=%s field revision=%d header revision=%d; want %s at %d\", fields, fieldRevision, headerRevision, initialFields, initialRevision)\n\t\t\t\t\t}\n",
				"\t\t\t\t\tcurrent, err := storetest.ReadWorkflowDuplicateProjectionStorage(ctx, selected.events, runID, runID)\n\t\t\t\t\tif err != nil {\n\t\t\t\t\t\tt.Fatal(err)\n\t\t\t\t\t}\n\t\t\t\t\tfields, headerRevision := string(current.Fields), int64(current.Revision)\n\t\t\t\t\tif fields != initialFields || headerRevision != initialRevision {\n\t\t\t\t\t\tt.Fatalf(\"failed handler changed construction: fields=%s header revision=%d; want %s at %d\", fields, headerRevision, initialFields, initialRevision)\n\t\t\t\t\t}\n"},
			{"int64(current.State.Revision) != command.State.ExpectedRevision+1", "current.Lifecycle.State.Revision != command.State.ExpectedRevision+1"},
		}
	}
	for _, edit := range edits {
		if strings.Count(current, edit.before) != 1 {
			return "", fmt.Errorf("missing exact Q10 fixture setup cut: %s/%q", row.Function, edit.before)
		}
		current = strings.Replace(current, edit.before, edit.after, 1)
	}
	return current, nil
}
