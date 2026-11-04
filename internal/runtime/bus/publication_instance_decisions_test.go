package bus

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type publicationDecisionTestReader struct {
	constructionIndexTestReader
	lookups []pipeline.FlowInstanceLookupRequest
	forced  *pipeline.FlowInstanceObservation
}

func (r *publicationDecisionTestReader) LookupFlowInstance(ctx context.Context, request pipeline.FlowInstanceLookupRequest) (pipeline.FlowInstanceObservation, bool, error) {
	r.lookups = append(r.lookups, request)
	if r.forced != nil {
		return *r.forced, true, nil
	}
	return r.constructionIndexTestReader.LookupFlowInstance(ctx, request)
}

func publicationDecisionFixture(t *testing.T) (publicationTargetOwnerEvidence, *publicationDecisionTestReader, events.RouteIdentity) {
	t.Helper()
	source := connectRoutePlanRootProducerSingletonSource(t)
	runID := eventtest.UUID("publication-instance-decision-run")
	reader := &publicationDecisionTestReader{}
	for _, flow := range []string{".", "consumer"} {
		identity := ConstructedFlowInstanceIdentityFixture(source, flow, "", runID)
		reader.observations = append(reader.observations, constructionIndexObservation(t, source, runID, identity, ""))
	}
	ctx := constructionIndexContext(t, source)
	fact, _ := correlation.SourceArtifactFactFromContext(ctx)
	scope, err := pipeline.NewFlowInstanceLookupScope(source, fact, runID, []string{".", "consumer"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx = withSelectedTargetOwnerLookupScope(ctx, selectedTargetOwnerLookupScope{lookup: scope, observations: append([]pipeline.FlowInstanceObservation(nil), reader.observations...)})
	projection := publicationTargetOwnerEvidence{context: ctx, instances: reader, source: source, required: true}
	for _, observed := range reader.observations {
		identity := observed.Identity()
		projection.candidates = append(projection.candidates, pipeline.DeliveryTargetOwnerCandidate{
			Route: events.RouteIdentity{FlowID: identity.TemplateID,
				FlowInstance: identity.InstancePath,
				EntityID:     identity.EntityID},
		})
	}
	identity := reader.observations[1].Identity()
	return projection, reader, events.RouteIdentity{FlowID: identity.TemplateID, FlowInstance: identity.InstancePath, EntityID: identity.EntityID}
}

func TestPublicationTargetDecisionReadsOnlyNarrowedCoordinate(t *testing.T) {
	projection, reader, target := publicationDecisionFixture(t)
	owner, err := projection.replyOriginOwner(target)
	if err != nil || owner.Route() != target || !owner.ExistingEntity() {
		t.Fatalf("exact target=%+v err=%v", owner, err)
	}
	if len(reader.lookups) != 1 || reader.lookups[0].FlowID() != target.FlowID || reader.lookups[0].ExactPath() != target.FlowInstance {
		t.Fatalf("mutable decision read unrelated inventory: %+v", reader.lookups)
	}
	identity := reader.observations[1].Identity()
	reader.observations[1] = TerminatedFlowInstanceObservationFixture(t, projection.source, reader.observations[1].Owner().RunID, identity)
	_, err = projection.replyOriginOwner(target)
	var terminated *pipeline.TerminatedReceiverError
	if !errors.As(err, &terminated) || len(reader.lookups) != 2 {
		t.Fatalf("stale list availability replaced exact receiver refusal: %v reads=%+v", err, reader.lookups)
	}
	t.Run("captured_source_life", func(t *testing.T) {
		projection, reader, target := publicationDecisionFixture(t)
		runID := reader.observations[1].Owner().RunID
		source, err := events.NewStaticFlowRoutingSource(target)
		if err != nil {
			t.Fatal(err)
		}
		event := func(source events.RoutingSource) events.Event {
			return eventtest.RunCreatingRootIngressWithRoutingSource(eventtest.UUID("captured-source-life-event"), "source.result", "fixture", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, source, time.Now().UTC())
		}
		if err := projection.requireCurrentSourceLife(event(source)); err != nil || len(reader.lookups) != 1 || reader.lookups[0].ExactPath() != target.FlowInstance {
			t.Fatalf("source lifetime escaped its exact read: %+v %v", reader.lookups, err)
		}
		foreign, err := source.WithLife(2)
		if err != nil {
			t.Fatal(err)
		}
		err = projection.requireCurrentSourceLife(event(foreign))
		failure, typed := failures.EnvelopeFromError(err)
		if !typed || failure.Class != failures.ClassStaleArrival || failure.Detail.Code != "instance_restarted" || len(reader.lookups) != 2 {
			t.Fatalf("publication adopted another life: reads=%+v error=%v", reader.lookups, err)
		}
		fault := errors.New("source-life observation failed")
		reader.err = fault
		if err := projection.requireCurrentSourceLife(event(source)); !errors.Is(err, fault) {
			t.Fatalf("failed source read borrowed captured inventory: %v", err)
		}
		reader.err = nil
		current, err := admitConstructionIndexTestObservation(projection.source, runID, reader.observations[1].Identity(), "", 2)
		if err != nil {
			t.Fatal(err)
		}
		reader.observations[1] = current
		err = projection.requireCurrentSourceLife(event(source))
		failure, typed = failures.EnvelopeFromError(err)
		if !typed || failure.Class != failures.ClassStaleArrival || failure.Detail.Code != "instance_restarted" {
			t.Fatalf("cached first-life inventory overrode current source life: %v", err)
		}
		if err := projection.requireCurrentSourceLife(event(foreign)); err != nil {
			t.Fatalf("exact current source life was refused: %v", err)
		}
		root, err := events.NewRootRoutingSource(runID)
		if err != nil {
			t.Fatal(err)
		}
		if err := projection.requireCurrentSourceLife(event(root)); err != nil || reader.lookups[len(reader.lookups)-1].ExactPath() != runID {
			t.Fatalf("root source life borrowed another coordinate: %+v %v", reader.lookups, err)
		}
	})
}

func TestPublicationTargetDecisionPreservesExactReadFailures(t *testing.T) {
	storeError := errors.New("independent decision read failed")
	for _, test := range []struct {
		name string
		err  error
	}{
		{"store", storeError}, {"cancel", context.Canceled}, {"joined", errors.Join(storeError, context.Canceled)},
	} {
		t.Run(test.name, func(t *testing.T) {
			projection, reader, target := publicationDecisionFixture(t)
			reader.err = test.err
			owner, err := projection.replyOriginOwner(target)
			if !owner.Empty() || !errors.Is(err, test.err) || len(reader.lookups) != 1 {
				t.Fatalf("failed exact read borrowed inventory: owner=%+v err=%v reads=%+v", owner, err, reader.lookups)
			}
			if test.name == "joined" && (!errors.Is(err, storeError) || !errors.Is(err, context.Canceled)) {
				t.Fatalf("joined read lost independent cause: %v", err)
			}
		})
	}
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "crossed", true: "absent"}[missing], func(t *testing.T) {
			projection, reader, _ := publicationDecisionFixture(t)
			listed := reader.observations[1]
			if missing {
				reader.observations = nil
			} else {
				reader.forced = &reader.observations[0]
			}
			observed, err := projection.decisionObservation(listed)
			var corruption *pipeline.FlowInstanceConstructionCorruption
			if observed.Valid() || !errors.As(err, &corruption) {
				t.Fatalf("lost inventory membership became a valid decision: %+v %v", observed, err)
			}
		})
	}
}

func TestPublicationAgentDecisionBindsNegativeEligibilityAfterIdentity(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "ineligible"}[unavailable], func(t *testing.T) {
			projection, reader, target := publicationDecisionFixture(t)
			base := reader.observations[1]
			actor := agentidentitytest.DeclaredForRun(t, base.Owner().RunID, "consumer-agent", "decision-agent", target.FlowID, base.Identity().InstanceID, target.FlowInstance)
			header, _ := base.WorkflowInstance()
			receipt, _, _ := base.NativeConstruction()
			readiness, _ := base.Readiness()
			readiness.Plan.Agents = []pipeline.DynamicFlowRuntimeAgentExpectation{{Identity: actor, ConfigRevision: strings.Repeat("a", 64)}}
			if unavailable {
				header.Status = "terminated"
				header.TerminatedAt = header.CreatedAt.Add(time.Second)
				readiness.InstanceStatus, readiness.InstanceTerminatedAt = header.Status, header.TerminatedAt
			}
			bundle, found := semanticview.Bundle(projection.source)
			if !found || bundle == nil {
				t.Fatal("publication decision fixture requires its captured source")
			}
			rooted, err := bundle.RootedSource()
			if err != nil {
				t.Fatal(err)
			}
			run := runlifecycle.Snapshot{RunID: base.Owner().RunID, State: base.RunState(), Origin: runlifecycle.DeploymentRunOrigin(), BundleHash: base.SourceFact().BundleHash(), RootReference: rooted.Admission().Reference(), StartedAt: header.CreatedAt}
			observed, err := pipeline.AdmitNativeFlowInstanceObservation(base.Selection(), header, run, base.RunRevision(), receipt, readiness)
			if err != nil {
				t.Fatal(err)
			}
			reader.observations[1] = observed
			scope := projection.context.Value(selectedTargetOwnerLookupScopeKey{}).(selectedTargetOwnerLookupScope)
			scope.observations = append([]pipeline.FlowInstanceObservation(nil), reader.observations...)
			projection.context = withSelectedTargetOwnerLookupScope(projection.context, scope)
			projection, err = projection.withObservedAgentConfigurations()
			if err != nil || len(reader.lookups) != 0 {
				t.Fatalf("inventory applied mutable agent eligibility: %v reads=%+v", err, reader.lookups)
			}
			descriptor, found, pending, err := projection.decisionAgentDescriptor(actor)
			if err != nil || found == unavailable || pending == unavailable || found && descriptor.Identity != actor {
				t.Fatalf("actual eligibility=%+v found=%t pending=%t err=%v", descriptor, found, pending, err)
			}
			if len(reader.lookups) != 1 || reader.lookups[0].ExactPath() != target.FlowInstance {
				t.Fatalf("negative or positive eligibility escaped exact read: %+v", reader.lookups)
			}
		})
	}
}
