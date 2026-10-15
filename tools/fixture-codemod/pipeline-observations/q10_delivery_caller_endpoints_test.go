package main

import (
	"fmt"
	"strings"
)

func q10DeliveryCallerCurrentSource(row recipe) (string, error) {
	if row.Family != "native-served-delivery-quiescence-caller" {
		return "", fmt.Errorf("unlisted Q10 delivery caller family")
	}
	current := row.After
	if row.Successor != "" {
		current = row.Successor
	}
	var edits []publicationSuccessorEdit
	switch row.File + "/" + row.Function {
	case "internal/serveapp/a2_map_recipe_surface_test.go/TestA2MapRecipeSupportedSurfaceBothStores":
		edits = []publicationSuccessorEdit{{`worker.Entity.CurrentState != "complete"`, `worker.Entity.CurrentState == nil || *worker.Entity.CurrentState != "complete"`}}
	case "internal/serveapp/a2_nested_portfolio_isolation_test.go/TestA2NestedPortfolioSamePeriodAndMemberIsolationBothStores":
		edits = []publicationSuccessorEdit{{`periods[index].Entity.CurrentState != "complete"`, `periods[index].Entity.CurrentState == nil || *periods[index].Entity.CurrentState != "complete"`}}
	case "internal/serveapp/a2_portfolio_arrival_admission_test.go/TestA2PortfolioPublicIndependentArrivalAdmissionBothStores":
		edits = []publicationSuccessorEdit{{"period.Entity.CurrentState != after.Entity.CurrentState", "period.Entity.CurrentState == nil || after.Entity.CurrentState == nil || *period.Entity.CurrentState != *after.Entity.CurrentState"}}
	case "internal/serveapp/a2_portfolio_supported_surface_test.go/waitA2PortfolioComplete":
		edits = []publicationSuccessorEdit{{`period.Entity.CurrentState == "complete"`, `period.Entity.CurrentState != nil && *period.Entity.CurrentState == "complete"`}}
	case "internal/serveapp/fork_retained_join_test.go/testServedJoinWriterCompletedHistoryRefusal":
		return q10RetainedJoinCurrentSource(row)
	case "internal/serveapp/lifecycle_emitter_competing_exit_test.go/TestServedLifecycleEmitterCompetingExitPublication":
		edits = []publicationSuccessorEdit{
			{"won := entity.Entity.CurrentState == \"done\"", "if entity.Entity.CurrentState == nil {\n\t\t\t\t\t\t\t\tt.Fatal(\"competing exit lost its constructed lifecycle\")\n\t\t\t\t\t\t\t}\n\t\t\t\t\t\t\twon := *entity.Entity.CurrentState == \"done\""},
			{`t.Fatalf("ordered winner changed: %s -> %s", order, entity.Entity.CurrentState)`, `t.Fatalf("ordered winner changed: %s -> %s", order, *entity.Entity.CurrentState)`},
		}
	case "internal/serveapp/main_runtime_test.go/runServedConversationForkLifecycleProof":
		edits = []publicationSuccessorEdit{{`chat.Snapshot.EntitySnapshot[0].CurrentState != "draft"`, `chat.Snapshot.EntitySnapshot[0].CurrentState == nil || *chat.Snapshot.EntitySnapshot[0].CurrentState != "draft"`}}
	case "internal/serveapp/materializing_sender_test.go/TestProspectiveTerminalReceiverRefusesWithoutMutationOrPublicationBothStores":
		edits = []publicationSuccessorEdit{
			{"\t\t\tvar entityID, stage, fields string\n\t\t\tvar headerRevision, fieldRevision, mutations int\n\t\t\tif err := rt.DB.QueryRow(`SELECT fi.entity_id,fi.current_state,fi.revision,CAST(es.fields AS TEXT),es.revision\n\t\t\t\tFROM flow_instances fi JOIN entity_state es ON es.run_id=fi.run_id AND es.entity_id=fi.entity_id AND es.flow_instance=fi.instance_path\n\t\t\t\tWHERE fi.run_id=$1 AND fi.instance_path=$2 AND fi.flow_template='.'`, seed.RunID, seed.RunID).Scan(&entityID, &stage, &headerRevision, &fields, &fieldRevision); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n",
				"\t\t\trecord, instance := requireReceiverTargetState(t, rt.ReceiverStateReader, seed.RunID, \".\", seed.RunID, seed.RunID)\n\t\t\tentityID, stage, fields, headerRevision := instance.EntityID, instance.CurrentState, string(record.State.Fields), instance.Revision\n\t\t\tvar mutations int\n"},
			{` || fieldRevision != 1`, ""},
			{`t.Fatalf("rejected handler changed construction: entity=%s stage=%s revisions=%d/%d fields=%s err=%v", entityID, stage, headerRevision, fieldRevision, fields, err)`, `t.Fatalf("rejected handler changed construction: entity=%s stage=%s header revision=%d fields=%s err=%v", entityID, stage, headerRevision, fields, err)`},
		}
	case "internal/serveapp/receiver_entityless_onward_test.go/TestReceiverCompositionEntitylessOnwardBothStores":
		edits = []publicationSuccessorEdit{{"\t\t\t\tvar rawFields string\n\t\t\t\tif err := rt.DB.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND flow_instance='sink/tail' AND current_state='done'`, started.RunID).Scan(&rawFields); err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n",
			"\t\t\t\trecord, tail := requireReceiverTargetState(t, rt.ReceiverStateReader, started.RunID, \"sink/tail\", \"sink/tail\", flowidentity.EntityID(\"sink/tail\"))\n\t\t\t\tif tail.CurrentState != \"done\" {\n\t\t\t\t\tt.Fatalf(\"grandchild state=%s, want done\", tail.CurrentState)\n\t\t\t\t}\n\t\t\t\trawFields := string(record.State.Fields)\n"}}
	case "internal/serveapp/repeated_static_baseline_probe_test.go/TestBaselineServedRepeatedStaticRunProbe":
		edits = []publicationSuccessorEdit{{"`SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND flow_instance='producer' AND current_state='active'`", "`SELECT COUNT(*) FROM entity_state es JOIN flow_instances fi\nON fi.run_id=es.run_id AND fi.instance_path=es.flow_instance AND fi.entity_id=es.entity_id\nWHERE es.run_id=$1 AND es.flow_instance='producer' AND fi.current_state='active'`"}}
	case "internal/serveapp/selected_fork_pending_input_test.go/TestExplicitTargetRestrictsConnectedSiblingsBothStores":
		edits = []publicationSuccessorEdit{q10ExplicitTargetOriginRead}
	case "internal/serveapp/transition_receiver_probe_test.go/proveReceiverMaterializationBothStores":
		edits = []publicationSuccessorEdit{{"`SELECT count(*) FROM entity_state WHERE run_id = $1 AND current_state = 'done'`", "`SELECT count(*) FROM entity_state es JOIN flow_instances fi\nON fi.run_id=es.run_id AND fi.instance_path=es.flow_instance AND fi.entity_id=es.entity_id\nWHERE es.run_id=$1 AND fi.current_state='done'`"}}
	}
	for _, edit := range edits {
		if strings.Count(current, edit.before) != 1 {
			return "", fmt.Errorf("missing exact Q10 delivery caller cut: %s/%q", row.Function, edit.before)
		}
		current = strings.Replace(current, edit.before, edit.after, 1)
	}
	return current, nil
}
