package cataloge2e

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	forkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// Retain the direct opening edges from the master control, with a local
// consumer for the cut event instead of the unrelated cross-instance edge.
func keyedAccountHistoryFixture(t *testing.T) string {
	t.Helper()
	root := canonicalrouting.CopyLexicalAddressing(t, true)
	schema := filepath.Join(root, "schema.yaml")
	original, err := os.ReadFile(schema)
	if err != nil {
		t.Fatal(err)
	}
	body := string(original)
	for _, row := range []string{
		"  - {event: ticket.cross, from: account/ticket, to: account, resolution: select, key_from: payload.target_account_id}\n",
		"  - {event: cross.requested, from: ., to: account/ticket, resolution: select}\n",
	} {
		if strings.Count(body, row) != 1 {
			t.Fatalf("master control lost its exact fixture row: %q", row)
		}
		body = strings.Replace(body, row, "", 1)
	}
	for name, content := range map[string]string{
		"schema.yaml":                body,
		"account/schema.yaml":        "instance: account_id\ninputs: [opened, root.audit, cross.forwarded]\nconnect:\n  - {event: ticket.done, from: ticket, to: audit}\n",
		"account/events.yaml":        "account.cross:\n  target_account_id: text\n",
		"account/nodes.yaml":         "receiver:\n  opened: {}\n  root.audit: {}\n  cross.forwarded:\n    emit: {event: account.cross, fields: {target_account_id: payload.target_account_id}}\n  account.cross: {}\n",
		"account/ticket/schema.yaml": "instance: ticket_id\ninputs: [opened]\noutputs: [ticket.done]\n",
		"account/ticket/events.yaml": "ticket.done:\n",
		"account/ticket/nodes.yaml":  "reporter:\n  opened:\n    emit: {event: ticket.done}\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestKeyedAccountHistorySelectedForkBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := keyedAccountHistoryFixture(t)
			h := newLiveForkCatalogHarness(t, root, backend)
			ctx := worklifetime.WithOccurrence(catalogRunContext(h, catalogRuntimeRunID), h.rt.WorkOccurrence())
			var selected interface {
				forkexecution.SelectedContractForkLifecycle
				forkexecution.SourceArtifactSelectedContractSourceStore
				storetest.RootedDataCatalogStore
			} = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
			openings := make(map[string]string)
			for _, key := range []string{"7", "8"} {
				id := uuid.NewString()
				openings[id] = key
				h.publishAndWait(catalogTriggerStep{eventID: id, Event: "opened", Payload: map[string]any{"account_id": key, "ticket_id": "42"}}, catalogRuntimePublishTimeout)
			}
			h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
			publishCross := func() {
				h.publishAndWait(catalogTriggerStep{Event: "cross.requested", Payload: map[string]any{"source_account_id": "7", "ticket_id": "42", "target_account_id": "8"}}, catalogRuntimePublishTimeout)
				h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
			}
			publishCross()
			public, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
			if err != nil {
				t.Fatal(err)
			}
			cut := ""
			for _, event := range public {
				if strings.HasSuffix(event.EventName, "/account.cross") {
					if cut != "" || len(event.Deliveries) != 1 {
						t.Fatal("expected one cut publication with one local delivery")
					}
					cut = event.EventID
				}
			}
			if cut == "" {
				t.Fatal("missing native account.cross cut")
			}
			publishCross()
			loader, selection, loaded := selectedContractForkFixtureSelection(t, ctx, repoRootFromCatalogE2E(t), root, selected)
			installCatalogSelectedSourceTopology(t, ctx, h, loaded)
			plan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: catalogRuntimeRunID, At: cut})
			if err != nil {
				t.Fatal(err)
			}
			selector, found, err := plan.HistoricalRouteSelector(cut)
			admission, admitted := selector.PayloadAdmission()
			if err != nil || !found || !admitted || admission.Binding().SchemaClass() != events.PayloadSchemaAuthored || admission.Binding().BundleHash() != selection.BundleHash || plan.ForkPoint.EventID != cut || plan.ForkPoint.Revision <= 0 {
				t.Fatalf("not the exact typed cut and bundle: point=%+v binding=%+v err=%v", plan.ForkPoint, admission.Binding(), err)
			}
			completed := make(map[string]bool)
			for _, work := range plan.PendingWork {
				if openings[work.EventID] != "" && work.Classification == runfork.RunForkPendingClassificationDeliveredCompleted {
					completed[work.EventID] = true
				}
			}
			if len(completed) != len(openings) {
				t.Fatalf("cut lost settled openings: %v", completed)
			}
			frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: loaded.Source, ContractSelection: selection})
			if err != nil {
				t.Fatal(err)
			}
			history, historyErr := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{Plan: plan, Source: loaded.Source, ContractSelection: selection, FrontierAdmission: frontier})
			before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, catalogRuntimeRunID)
			if err != nil {
				t.Fatal(err)
			}
			result, executeErr := executeCatalogLiveFork(t, h, ctx, forkexecution.SelectedContractExecutionRequest{
				SourceRunID: catalogRuntimeRunID, At: cut, SourceLoader: loader, ContractSelection: selection,
				Owner: selectedContractExecutionOwnerForCatalogHarness(t, h), AgentRuntime: selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig()), ExpectedBundleHash: selection.BundleHash,
			})
			if historyErr != nil || executeErr != nil || !result.Activation.Activated {
				t.Fatalf("selected keyed history must activate: history=%v activated=%t execution=%v", historyErr, result.Activation.Activated, executeErr)
			}
			if result.Activation.ForkPoint.EventID != cut || result.Activation.ForkPoint.Revision != plan.ForkPoint.Revision ||
				result.Activation.SelectedContractBinding == nil || result.Activation.SelectedContractBinding.ContractSelection.BundleHash != selection.BundleHash {
				t.Fatalf("fork changed admitted cut or bundle: %+v", result.Activation)
			}
			assertKeyedOpeningHistory(t, h, result, plan, history, openings)
			after, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, catalogRuntimeRunID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("selected keyed-history fork changed source business facts: %v", err)
			}
		})
	}
}

func assertKeyedOpeningHistory(t *testing.T, h *runtimeHarness, result forkexecution.SelectedContractExecutionResult, plan runfork.RunForkPlan, history runfork.RunForkSelectedContractRouteAdmission, openings map[string]string) {
	t.Helper()
	child := result.Materialization.ForkRunID
	rows, err := h.workflow.ListWorkflowInstances(catalogRunContext(h, child), child)
	if err != nil {
		t.Fatal(err)
	}
	accounts := make(map[string]string)
	type ticketState struct {
		parent string
		fields map[string]any
	}
	tickets := make(map[string]ticketState)
	for _, instance := range rows {
		switch instance.WorkflowName {
		case "account":
			key, _ := instance.Fields["account_id"].(string)
			if key == "" || instance.InstanceKey != key || accounts[instance.StorageRef] != "" || instance.EntityID != flowidentity.EntityID(instance.StorageRef) {
				t.Fatalf("invalid child account: %+v", instance)
			}
			accounts[instance.StorageRef] = key
		case "account/ticket":
			if instance.InstanceKey != "42" || instance.Fields["ticket_id"] != "42" ||
				instance.ParentEntityID != flowidentity.EntityID(instance.ParentFlowInstance) || instance.EntityID != flowidentity.EntityID(instance.StorageRef) {
				t.Fatalf("invalid child ticket: %+v", instance)
			}
			tickets[instance.StorageRef] = ticketState{parent: instance.ParentFlowInstance, fields: instance.Fields}
		}
	}
	if len(accounts) != 2 || len(tickets) != 2 {
		t.Fatalf("child lost sibling identities: accounts=%v tickets=%v", accounts, tickets)
	}
	for _, entity := range plan.Entities {
		meta := entity.MaterializationMetadata
		if meta.FlowTemplate == "account/ticket" && !reflect.DeepEqual(tickets[meta.FlowInstance], ticketState{parent: meta.ParentInstance, fields: entity.Fields}) {
			t.Fatalf("child ticket differs from fixed-cut state or parent: %s", meta.FlowInstance)
		}
	}
	for _, entry := range history.SelectedRouteEvents {
		if key := openings[entry.SourceEventID]; key != "" {
			count := 0
			for _, recipient := range entry.DerivedRecipients {
				if ticket, ok := tickets[recipient.Path]; ok {
					if accounts[ticket.parent] != key {
						t.Fatalf("opening %s chose a sibling's ticket %s", entry.SourceEventID, recipient.Path)
					}
					count++
				}
			}
			if count != 1 {
				t.Fatalf("opening %s has %d derived ticket recipients, want one", entry.SourceEventID, count)
			}
			delete(openings, entry.SourceEventID)
		}
	}
	if len(openings) != 0 {
		t.Fatalf("history did not derive each opening's exact ticket: %v", openings)
	}
	for _, executed := range result.ForkEvents {
		if executed.EventName == "opened" {
			t.Fatalf("fork reran a settled opening: %+v", executed)
		}
	}
}
