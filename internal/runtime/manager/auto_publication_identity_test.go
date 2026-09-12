package manager

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/google/uuid"
)

func TestAutoPublicationUsesAdmittedIdentityBeforeID(t *testing.T) {
	source := notifyallchildren.LoadSource(t, notifyallchildren.Options{AutoEmitOnCreate: true})
	schema, ok := source.FlowSchemaByID(notifyallchildren.ChildFlowID)
	if !ok {
		t.Fatal("missing account schema")
	}
	lineage := events.EventLineage{RunID: uuid.NewString(), ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live}
	occurredAt := time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)
	config := map[string]any{"account_id": "acct-1"}
	entity := uuid.NewString()
	local, err := buildDynamicFlowRuntimeCreationEventPlan(source, schema, "account", "account/acct-1", entity, lineage, config, events.DeliveryContext{}, occurredAt)
	if err != nil {
		t.Fatal(err)
	}
	if local.EventType != "account/acct-1/account.created" {
		t.Fatalf("wrong exact publication: %+v", local)
	}
	wantID := uuid.NewSHA1(dynamicFlowCreationEventNamespace, []byte(lineage.RunID+"\x00"+lineage.ParentEventID+"\x00account/acct-1\x00account/acct-1/account.created")).String()
	if local.EventID != wantID {
		t.Fatalf("event ID did not use final publication identity: %s want %s", local.EventID, wantID)
	}
	schema.AutoEmitOnCreate.Event = "account/account.created"
	qualified, err := buildDynamicFlowRuntimeCreationEventPlan(source, schema, "account", "account/acct-1", entity, lineage, config, events.DeliveryContext{}, occurredAt)
	if err != nil || !reflect.DeepEqual(local, qualified) {
		t.Fatalf("same declaration produced distinct event facts: local=%+v qualified=%+v err=%v", local, qualified, err)
	}
}

func TestAutoPublicationRejectsForeignOrConcreteDeclarationBeforePlan(t *testing.T) {
	source := notifyallchildren.LoadSource(t, notifyallchildren.Options{AutoEmitOnCreate: true})
	schema, ok := source.FlowSchemaByID(notifyallchildren.ChildFlowID)
	if !ok {
		t.Fatal("missing account schema")
	}
	lineage := events.EventLineage{RunID: uuid.NewString(), ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live}
	for _, tc := range []struct{ name, event, path, entity string }{
		{"foreign_declaration", "other/account.created", "account/acct-1", uuid.NewString()},
		{"concrete_declaration", "account/acct-1/account.created", "account/acct-1", uuid.NewString()},
		{"foreign_instance", "account.created", "other/acct-1", uuid.NewString()},
		{"missing_instance", "account.created", "", uuid.NewString()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			declaration := schema
			declaration.AutoEmitOnCreate.Event = tc.event
			plan, err := buildDynamicFlowRuntimeCreationEventPlan(source, declaration, "account", tc.path, tc.entity, lineage, map[string]any{"account_id": "acct-1"}, events.DeliveryContext{}, time.Now())
			if err == nil || plan != nil {
				t.Fatalf("invalid source/declaration produced a publishable plan: plan=%+v err=%v", plan, err)
			}
		})
	}
	schema.AutoEmitOnCreate.Event = ""
	plan, err := buildDynamicFlowRuntimeCreationEventPlan(source, schema, "account", "", "", events.EventLineage{}, nil, events.DeliveryContext{}, time.Time{})
	if err != nil || plan != nil {
		t.Fatalf("absent auto emit invented an occurrence: %+v %v", plan, err)
	}
}
