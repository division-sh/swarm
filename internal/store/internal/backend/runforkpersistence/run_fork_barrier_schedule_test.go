package runforkpersistence

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestFixedRevisionBarrierScheduleExactRelation(t *testing.T) {
	for _, status := range []fanoutbarrier.Status{fanoutbarrier.StatusClosedPending, fanoutbarrier.StatusFired, fanoutbarrier.StatusOutcomeDeadLettered, fanoutbarrier.StatusSuppressedGenerationSuperseded, fanoutbarrier.StatusSuppressedRunTerminal} {
		t.Run(string(status), func(t *testing.T) {
			snapshot, obligations := barrierScheduleProjectionFixture(t)
			barrier, timer := obligations[0].Barrier, &snapshot.Timers[0]
			barrier.Status = status
			if status != fanoutbarrier.StatusClosedPending {
				at := timer.FireAt.Add(time.Second)
				timer.Status, timer.FiredAt, timer.AcceptedAt = "fired", &at, &at
				timer.OccurrenceEventID = genericschedule.OccurrenceEventID(timer.TimerID, timer.FireAt)
				timer.OccurrenceAdmittedAt = &at
			}
			var pending []runfork.RunForkPendingWork
			if status == fanoutbarrier.StatusOutcomeDeadLettered {
				snapshot, obligations, pending = terminalBarrierHistoryFixture(t)
			}
			before, _ := json.Marshal(snapshot)
			owned, err := validateRunForkBarrierSchedules(snapshot, obligations)
			if err != nil || len(owned) != 1 {
				t.Fatalf("exact relation: owned=%v err=%v", owned, err)
			}
			evidence, err := loadRunForkAdmissionEvidenceFromRevision(snapshot, nil, pending, obligations)
			if err != nil || evidence.RelevantTimer {
				t.Fatalf("owned historical schedule blocked: %+v %v", evidence, err)
			}
			after, _ := json.Marshal(snapshot)
			if string(before) != string(after) {
				t.Fatal("admission changed historical evidence")
			}
		})
	}
}

func TestFixedRevisionBarrierScheduleRejectsSubstitution(t *testing.T) {
	cases := []string{"missing", "duplicate_timer", "duplicate_owner", "run", "entity", "flow", "source", "mode", "owner", "owner_node", "agent_identity", "name", "scope", "key", "handle", "payload", "hash", "due", "initial_due", "current_due", "recurrence", "task_type", "state", "occurrence", "plan_digest", "summary", "cardinality", "terminal_active"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			snapshot, obligations := barrierScheduleProjectionFixture(t)
			timer := &snapshot.Timers[0]
			switch name {
			case "missing":
				snapshot.Timers = nil
			case "duplicate_timer":
				snapshot.Timers = append(snapshot.Timers, *timer)
			case "duplicate_owner":
				obligations = append(obligations, obligations[0])
			case "run":
				timer.RunID = uuid.NewString()
			case "entity":
				timer.EntityID = uuid.NewString()
			case "flow":
				timer.FlowInstance = "foreign"
			case "source":
				timer.RoutingSource = json.RawMessage(`{}`)
			case "mode":
				timer.ExecutionMode = "invalid"
			case "owner":
				timer.OwnerAgent = "foreign"
			case "owner_node":
				timer.OwnerNode = "scatter"
			case "agent_identity":
				timer.AgentNameSource = "foreign"
			case "name":
				timer.TimerName = "foreign"
			case "scope":
				timer.ScheduleScope = "foreign"
			case "key":
				timer.ScheduleKey = "foreign"
			case "handle":
				timer.TaskID = "foreign"
			case "payload":
				timer.FirePayload = json.RawMessage(`{"join":{"total":1}}`)
			case "hash":
				timer.ImmutableHash = strings.Repeat("f", 64)
			case "due":
				at := timer.FireAt.Add(time.Second)
				timer.DueBasisAbsolute = &at
			case "initial_due":
				timer.InitialFireAt = nil
			case "current_due":
				timer.FireAt = timer.FireAt.Add(time.Second)
			case "recurrence":
				timer.Recurring = true
			case "task_type":
				timer.TaskType = "scheduled_task"
			case "state":
				timer.Status = "cancelled"
			case "occurrence":
				timer.OccurrenceEventID = uuid.NewString()
			case "plan_digest":
				obligations[0].Barrier.Registration.PlanRef.SemanticDigest = "sha256:" + strings.Repeat("3", 64)
			case "summary":
				obligations[0].Barrier.Summary = &fanoutbarrier.Summary{Total: 1, Succeeded: 1}
			case "cardinality":
				obligations[0].Intent.Request.Cardinality = 1
			case "terminal_active":
				obligations[0].Barrier.Status = fanoutbarrier.StatusFired
			}
			before, _ := json.Marshal(snapshot)
			if _, err := validateRunForkBarrierSchedules(snapshot, obligations); err == nil {
				t.Fatal("contradictory barrier relation admitted")
			}
			after, _ := json.Marshal(snapshot)
			if string(before) != string(after) {
				t.Fatal("rejection mutated source")
			}
		})
	}
}

func TestFixedRevisionBarrierExtraScheduleRemainsBlocker(t *testing.T) {
	snapshot, obligations := barrierScheduleProjectionFixture(t)
	extra := snapshot.Timers[0]
	extra.TimerID = uuid.NewString()
	snapshot.Timers = append(snapshot.Timers, extra)
	evidence, err := loadRunForkAdmissionEvidenceFromRevision(snapshot, nil, nil, obligations)
	if err != nil || !evidence.RelevantTimer {
		t.Fatalf("extra schedule escaped generic gate: %+v %v", evidence, err)
	}
	if reflect.DeepEqual(snapshot.Timers[0], snapshot.Timers[1]) {
		t.Fatal("extra schedule fixture must have distinct activation")
	}
}

// This is a transport/semantic owner fixture. The both-store six-state matrix
// independently starts from the actual fan-out writer and activation owner.
func barrierScheduleProjectionFixture(t *testing.T) (*runForkRevisionSnapshot, []runfork.RunForkFanOutObligation) {
	t.Helper()
	runID, deliveryID := uuid.NewString(), uuid.NewString()
	element := contracts.FanOutElementRef{FlowPath: ".", Family: "fan_out", SemanticPath: "scatter/handler/items.ready/fan_out"}
	declaration, err := element.DeclarationIdentity()
	if err != nil {
		t.Fatal(err)
	}
	node, err := identity.AdmitExecutableNodeDeclaration(".", "scatter")
	if err != nil {
		t.Fatal(err)
	}
	bundle, digest := "bundle-v2:sha256:"+strings.Repeat("1", 64), "sha256:"+strings.Repeat("2", 64)
	ref, err := timeridentity.NewFanOutDeliveryJoinRef(node, "items.ready", "joined", declaration, bundle, digest)
	if err != nil {
		t.Fatal(err)
	}
	ref, err = ref.BindFanOutIntent(deliveryID, attemptgeneration.Generation{})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := timeridentity.JoinCompleteHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)
	key := fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: deliveryID, ElementRef: element}
	plan := contracts.FanOutPlanRef{BundleHash: bundle, ElementRef: element, SemanticDigest: digest}
	registration := fanoutbarrier.Registration{IntentKey: key, PlanRef: plan, Handle: handle, Route: flowidentity.StoredRoute(".", runID, runID), EntityID: runID, RoutingSource: eventtest.RootRoutingSource(runID), ExecutionMode: executionmode.Live, CreatedAt: at}
	summary := fanoutbarrier.Summary{Total: 0}
	command, err := genericschedule.FanOutBarrierAdmission(registration, summary, at)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := command.ImmutableHash()
	if err != nil {
		t.Fatal(err)
	}
	scope, err := command.ScopeKey()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := canonicaljson.Encode(command.Payload)
	if err != nil {
		t.Fatal(err)
	}
	source, err := json.Marshal(command.RoutingSource)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	timer := runForkRevisionTimer{TimerID: id, TimerName: command.ScheduleKey, ScheduleKey: command.ScheduleKey, ScheduleScope: scope, ImmutableHash: hash, RunID: runID, EntityID: runID, FireEvent: command.EventType, FirePayload: payload, RoutingSource: source, ExecutionMode: string(command.ExecutionMode), FireAt: at, InitialFireAt: &at, OwnerAgent: command.OwnerID, OwnerKind: string(command.OwnerKind), TaskID: command.TaskID, DueBasisKind: string(genericschedule.DueAbsolute), DueBasisAbsolute: &at, TaskType: "timer", Status: "active", CreatedAt: at}
	barrier := &fanoutbarrier.Barrier{Registration: registration, Status: fanoutbarrier.StatusClosedPending, ScheduleKey: command.ScheduleKey, ScheduleActivationID: id, Summary: &summary, UpdatedAt: at}
	obligations := []runfork.RunForkFanOutObligation{{Intent: fanoutobligation.Intent{Request: fanoutobligation.IntentRequest{Key: key, PlanRef: plan}}, Barrier: barrier}}
	return &runForkRevisionSnapshot{RunID: runID, Timers: []runForkRevisionTimer{timer}}, obligations
}
