package pipeline_test

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

type selectedPublishedJoinTestSource struct {
	semanticview.Source
	joins []contracts.WorkflowJoinPlan
}

func (s selectedPublishedJoinTestSource) WorkflowJoins() []contracts.WorkflowJoinPlan {
	return s.joins
}

func TestPublishedBarrierSelectedDeclarationDoesNotRelaxLiveAdmission(t *testing.T) {
	repo, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	original, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyForkFanOutBarrierReceiver(t), filepath.Join(repo, "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	selected, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyForkFanOutBarrierCompletionFailure(t), filepath.Join(repo, "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(original)
	target := semanticview.Wrap(selected)
	if !target.WorkflowJoins()[0].Spec.OnComplete.DataAccumulation.Writes[0].Value.HasCELValue() {
		t.Fatal("selected failure fixture must compile CEL, not a quoted literal")
	}
	ref := source.FanOutPlans()[0].Ref
	join := source.WorkflowJoins()[0]
	declaration, err := ref.ElementRef.DeclarationIdentity()
	if err != nil {
		t.Fatal(err)
	}
	handleRef, err := timeridentity.NewFanOutDeliveryJoinRef(join.Node, join.HandlerEvent, join.Spec.EffectiveID(), declaration, ref.BundleHash, ref.SemanticDigest)
	if err != nil {
		t.Fatal(err)
	}
	run, delivery, entity := uuid.NewString(), uuid.NewString(), uuid.NewString()
	handleRef, err = handleRef.BindFanOutIntent(delivery, attemptgeneration.Generation{})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := timeridentity.JoinCompleteHandle(handleRef)
	if err != nil {
		t.Fatal(err)
	}
	routing, err := events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: join.Node.FlowPath(), FlowInstance: "producer", EntityID: entity})
	if err != nil {
		t.Fatal(err)
	}
	due := time.Now().UTC().Truncate(time.Millisecond)
	registration := fanoutbarrier.Registration{
		IntentKey: fanoutobligation.IntentKey{RunID: run, TriggeringDeliveryID: delivery, ElementRef: ref.ElementRef},
		PlanRef:   ref, Handle: handle, Route: flowidentity.StoredRoute("producer", "producer", "producer"), EntityID: entity,
		RoutingSource: routing, ExecutionMode: executionmode.Live, CreatedAt: due.Add(-time.Second),
	}
	command, err := genericschedule.FanOutBarrierAdmission(registration, fanoutbarrier.Summary{Total: 1, Succeeded: 1}, due)
	if err != nil {
		t.Fatal(err)
	}
	ancestor, sourceEvent := uuid.NewString(), uuid.NewString()
	occurrence := genericschedule.TransferredJoinOccurrence{Command: command, Publication: joinruntime.TransferredPublication{
		EventID: activityidentity.ForkLineageEventID(run, sourceEvent), SourceRunID: ancestor, SourceEventID: sourceEvent,
		AuthorityStamp: runfork.RunForkSelectedContractExecutionOwner, ExecutionMode: executionmode.Live,
		CommittedCause: joinruntime.CommittedPublicationCause{RunID: ancestor, EventID: sourceEvent},
	}}
	event, err := occurrence.Event()
	if err != nil {
		t.Fatal(err)
	}
	event, err = eventtest.AdmitPayload(event, ".", "platform.join_complete")
	if err != nil {
		t.Fatal(err)
	}
	publication, err := runfork.NewTransferredPublishedJoinOccurrence(occurrence, event)
	if err != nil {
		t.Fatal(err)
	}
	before := publication.Event()
	for _, semantic := range []semanticview.Source{source, target} {
		recipient, destination, handler, found, err := pipeline.ResolvePublishedJoinOccurrenceDeliveryTarget(semantic, publication)
		if err != nil || !found || recipient != events.MustNodeDeliveryRecipient(join.Node) || destination != (events.RouteIdentity{FlowID: join.Node.FlowPath(), FlowInstance: "producer", EntityID: entity}) ||
			handler.Node() != join.Node {
			t.Fatalf("selected exact declaration changed retained recipient ownership: recipient=%+v target=%+v handler=%+v found=%t err=%v", recipient, destination, handler, found, err)
		}
		if !reflect.DeepEqual(before, publication.Event()) {
			t.Fatal("read-only declaration selection rewrote the original publication")
		}
	}
	if _, _, _, found, err := pipeline.ResolveWorkflowJoinOccurrenceDeliveryTarget(source, event); err != nil || !found {
		t.Fatalf("original live declaration no longer accepts its own artifact: found=%t err=%v", found, err)
	}
	if _, _, _, found, err := pipeline.ResolveWorkflowJoinOccurrenceDeliveryTarget(target, event); err == nil || found {
		t.Fatal("ordinary live admission borrowed the old publication digest")
	}
	for _, absent := range []semanticview.Source{nil, semanticview.Wrap(&contracts.WorkflowContractBundle{})} {
		if _, _, _, found, err := pipeline.ResolvePublishedJoinOccurrenceDeliveryTarget(absent, publication); err == nil || found {
			t.Fatal("missing selected declaration borrowed source authority")
		}
	}
	if _, _, _, found, err := pipeline.ResolvePublishedJoinOccurrenceDeliveryTarget(target, runfork.PublishedJoinOccurrence{}); err == nil || found {
		t.Fatal("unsealed zero publication gained a recipient")
	}
	for _, fault := range []string{"node", "handler", "join", "mode", "declaration", "bundle", "digest"} {
		t.Run(fault, func(t *testing.T) {
			bad := target.WorkflowJoins()[0].Clone()
			switch fault {
			case "node":
				bad.Node, err = identity.AdmitExecutableNodeDeclaration("other", "producer")
			case "handler":
				bad.HandlerEvent = "other.requested"
			case "join":
				bad.Spec.ID = "other-join"
			case "mode":
				bad.Mode = contracts.WorkflowJoinModeArrival
			case "declaration":
				bad.FanOut.FanOut.ElementRef.SemanticPath += ".foreign"
			case "bundle":
				bad.FanOut.FanOut.BundleHash = "bundle-v2:sha256:" + strings.Repeat("a", 64)
			case "digest":
				bad.FanOut.FanOut.SemanticDigest = "sha256:" + strings.Repeat("b", 64)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, _, _, found, err := pipeline.ResolvePublishedJoinOccurrenceDeliveryTarget(selectedPublishedJoinTestSource{Source: target, joins: []contracts.WorkflowJoinPlan{bad}}, publication); err == nil || found {
				t.Fatal("contradictory selected declaration gained a retained recipient")
			}
		})
	}
}
