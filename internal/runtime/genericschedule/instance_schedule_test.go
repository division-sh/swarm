package genericschedule

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

func instanceScheduleCommand(t *testing.T, flow string) AdmissionCommand {
	t.Helper()
	runID := uuid.NewString()
	instance, eventType := flow, flow+"/poll.tick"
	if flow == "." {
		instance, eventType = runID, "poll.tick"
	}
	source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: flow, FlowInstance: instance})
	if err != nil {
		t.Fatal(err)
	}
	return AdmissionCommand{ScheduleKey: "poll", RunID: runID, FlowInstance: instance,
		OwnerKind: OwnerInstance, OwnerID: flow, EventType: eventType,
		Payload: semanticvalue.EmptyObject(), RoutingSource: source, ExecutionMode: executionmode.Live, Due: EveryDue(time.Minute)}
}

func TestInstanceScheduleAdmissionRejectsControlAndIdentityDrift(t *testing.T) {
	for _, flow := range []string{".", "account/poller"} {
		if err := instanceScheduleCommand(t, flow).Validate(); err != nil {
			t.Fatalf("lawful %s instance carrier: %v", flow, err)
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*AdmissionCommand)
	}{
		{"missing run", func(c *AdmissionCommand) { c.RunID = "" }},
		{"foreign owner", func(c *AdmissionCommand) { c.OwnerID = "unrelated" }},
		{"foreign instance", func(c *AdmissionCommand) { c.FlowInstance = "account/other" }},
		{"foreign entity", func(c *AdmissionCommand) { c.EntityID = uuid.NewString() }},
		{"system authority", func(c *AdmissionCommand) { c.OwnerKind = OwnerSystem }},
		{"platform event", func(c *AdmissionCommand) { c.EventType = "account/poller/platform.tick" }},
		{"unqualified child event", func(c *AdmissionCommand) { c.EventType = "poll.tick" }},
		{"payload", func(c *AdmissionCommand) { c.Payload, _ = canonicaljson.FromGo(map[string]any{"wake_id": "forbidden"}) }},
		{"agent identity", func(c *AdmissionCommand) {
			c.AgentIdentity = agentidentitytest.RootDeclaredForRun(t, c.RunID, "agent", "test/agents.yaml")
		}},
		{"task", func(c *AdmissionCommand) { c.TaskID = "fabricated" }},
		{"reply", func(c *AdmissionCommand) { c.ReplyContext = uuid.NewString() }},
		{"one shot", func(c *AdmissionCommand) { c.Due = DelayDue(time.Minute) }},
		{"control source", func(c *AdmissionCommand) { c.RoutingSource = events.NewPlatformControlRoutingSource() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			command := instanceScheduleCommand(t, "account/poller")
			tc.mutate(&command)
			if err := command.Validate(); err == nil {
				t.Fatal("hostile instance schedule admitted")
			}
		})
	}
}

func TestInstanceScheduleOccurrenceUsesPersistedBusinessSource(t *testing.T) {
	for _, flow := range []string{".", "account/poller"} {
		t.Run(flow, func(t *testing.T) {
			command := instanceScheduleCommand(t, flow)
			activation := Activation{ID: uuid.NewString(), Command: command}
			due := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
			occurrence := Occurrence{EventID: OccurrenceEventID(activation.ID, due), DueAt: due}
			event, err := occurrenceEvent(activation, occurrence, []byte(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			if event.AdmissionClass() != events.EventAdmissionInstancePublication || event.Producer().Type() != events.EventProducerInstance || event.Producer().ID() != flow {
				t.Fatalf("clock acquired non-instance authority: %#v", event)
			}
			if event.RunID() != command.RunID || event.RoutingSource() != command.RoutingSource || event.ParentEventID() != "" || event.TaskID() != "" || string(event.Payload()) != `{}` || !event.CreatedAt().Equal(due) {
				t.Fatalf("clock changed admitted facts or leaked wake metadata: %#v", event)
			}
			admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil || admitted.RunDisposition() != events.AdmittedRunRequireActive {
				t.Fatalf("clock admission = %s, %v", admitted.RunDisposition(), err)
			}
		})
	}
}

func TestInstanceScheduleHashBindsSourceAndOwner(t *testing.T) {
	command := instanceScheduleCommand(t, "account/poller")
	hash, err := command.ImmutableHash()
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*AdmissionCommand){
		func(c *AdmissionCommand) { c.RunID = uuid.NewString() },
		func(c *AdmissionCommand) {
			c.OwnerID, c.FlowInstance, c.EventType = "other/poller", "other/poller", "other/poller/poll.tick"
			c.RoutingSource, _ = events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: c.OwnerID, FlowInstance: c.FlowInstance})
		},
		func(c *AdmissionCommand) { c.ExecutionMode = executionmode.Mock },
		func(c *AdmissionCommand) { c.Due = EveryDue(2 * time.Minute) },
	} {
		changed := command
		mutate(&changed)
		got, err := changed.ImmutableHash()
		if err != nil || got == hash {
			t.Fatalf("admitted identity mutation did not change hash: %q, %v", got, err)
		}
	}
}
