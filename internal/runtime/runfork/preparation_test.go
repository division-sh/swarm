package runfork

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/google/uuid"
)

func TestSelectedPreparationFingerprintWithoutInheritedWitness(t *testing.T) {
	plan := RunForkPlan{SourceRunID: uuid.NewString(), ForkPoint: RunForkPoint{Kind: RunForkPointEvent, Revision: 1, EventID: uuid.NewString()}}
	history := []string{plan.ForkPoint.EventID}
	plan = plan.WithHistoricalEvents(1, history)
	frontier := RunForkContractFrontierAdmission{}
	planning := RunForkSelectedContractRecipientPlanning{}
	const declarations = "unchanged"
	old, err := canonicaljson.Bytes(map[string]any{
		"Plan": plan, "History": history, "Inputs": plan.HistoricalInputCoordinates(), "Arrivals": plan.HistoricalJoinCoordinates(),
		"FirstTurn": nil, "Replies": plan.ReplyContexts, "Timers": plan.WorkflowTimers, "JoinSchedules": []string{}, "Transferred": []string{},
		"Frontier": frontier, "Recipients": planning, "Declarations": declarations,
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(old)
	got, err := SelectedPreparationPlanFingerprint(plan, frontier, planning, declarations)
	if err != nil || got != hex.EncodeToString(digest[:]) {
		t.Fatalf("absent inherited witness changed the prior preparation encoding: %s %v", got, err)
	}
}

func TestPendingProducerPreparationWitnessBindsExactAdmittedPublications(t *testing.T) {
	child, original := uuid.NewString(), uuid.NewString()
	producer := eventtest.ConcreteTemplateRoutingSource("outer", "outer/left", uuid.NewString())
	marker := eventtest.ConcreteTemplateRoutingSource("ticket", "ticket/first", uuid.NewString())
	opening := inheritedWitnessSelector(t, original, marker)
	ancestor := inheritedWitnessPlan(t, original, opening)
	cut := inheritedWitnessSelector(t, child, marker)
	a := inheritedWitnessSelector(t, child, producer)
	b := inheritedWitnessSelector(t, child, producer)
	base := inheritedWitnessPlan(t, child, cut, a, b)
	witness := InheritedConstructionWitness{
		Ancestor: ancestor, CreatingEventID: opening.ID(), OriginalEntityID: marker.Route().EntityID,
		Lineage:           []InheritedConstructionHop{{ChildRunID: child, SourceRunID: original, SourceCut: ancestor.ForkPoint}},
		ProjectedEntityID: marker.Route().EntityID,
	}
	base, err := base.WithInheritedConstructionWitness(witness)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []events.Event{a, b, a} {
		base.PendingWork = append(base.PendingWork, RunForkPendingWork{EventID: event.ID(), EventName: string(event.Type()), RoutingSource: producer, Classification: RunForkPendingClassificationPending})
	}
	reads := 0
	witness.ProjectedEntityID = producer.Route().EntityID
	withReader := func(reader func() (InheritedConstructionWitness, bool, error)) RunForkPlan {
		return base.WithPendingProducerWitnessReaders(map[events.RouteIdentity]func() (InheritedConstructionWitness, bool, error){producer.Route(): reader})
	}
	with := withReader(func() (InheritedConstructionWitness, bool, error) {
		reads++
		return witness, true, nil
	})
	name := string(a.Type())
	eventsAtCut := []RunForkContractFrontierEvent{
		{SourceEventID: a.ID(), EventName: name, SourceFlowInstances: []string{producer.Route().FlowInstance}},
		{SourceEventID: b.ID(), EventName: name, SourceFlowInstances: []string{producer.Route().FlowInstance}},
	}
	frontier := RunForkContractFrontierAdmission{FrontierEvents: eventsAtCut}
	excluded := RunForkContractFrontierAdmission{LineageOnlyEvents: []RunForkContractFrontierLineageEvent{{SourceEventID: a.ID(), EventName: name}}}
	for _, plan := range []RunForkPlan{base, with} {
		got, err := SelectedPreparationPlanFingerprint(plan, excluded, RunForkSelectedContractRecipientPlanning{}, "")
		want, wantErr := SelectedPreparationPlanFingerprint(base, excluded, RunForkSelectedContractRecipientPlanning{}, "")
		if err != nil || wantErr != nil || got != want || reads != 0 {
			t.Fatalf("lineage-only publication changed marker preparation or opened evidence: reads=%d %v / %v", reads, err, wantErr)
		}
	}
	digest := func(plan RunForkPlan, frontier RunForkContractFrontierAdmission) string {
		t.Helper()
		got, err := plan.pendingProducerPreparationWitnesses(frontier)
		if err != nil || len(got) != 1 {
			t.Fatalf("publications must bind one producer witness: %v / %v", got, err)
		}
		return got[0]
	}
	both := digest(with, frontier)
	reordered := with
	reordered.PendingWork = slices.Clone(with.PendingWork)
	slices.Reverse(reordered.PendingWork)
	if digest(reordered, RunForkContractFrontierAdmission{FrontierEvents: []RunForkContractFrontierEvent{eventsAtCut[1], eventsAtCut[0]}}) != both ||
		digest(with, RunForkContractFrontierAdmission{FrontierEvents: eventsAtCut[:1]}) == both {
		t.Fatal("witness digest did not bind the exact unordered publication set")
	}
	broken := withReader(func() (InheritedConstructionWitness, bool, error) {
		return InheritedConstructionWitness{}, false, errors.New("unreadable ancestor")
	})
	if _, err := broken.pendingProducerPreparationWitnesses(excluded); err != nil {
		t.Fatalf("excluded evidence was opened: %v", err)
	}
	if _, err := broken.pendingProducerPreparationWitnesses(frontier); err == nil {
		t.Fatal("admitted unreadable evidence did not refuse")
	}
	crossed := withReader(func() (InheritedConstructionWitness, bool, error) {
		w := witness
		w.ProjectedEntityID = marker.Route().EntityID
		return w, true, nil
	})
	if _, err := crossed.pendingProducerPreparationWitnesses(frontier); err == nil {
		t.Fatal("a different producer's evidence was accepted")
	}
	crossed = withReader(func() (InheritedConstructionWitness, bool, error) {
		w := witness
		w.Lineage = slices.Clone(w.Lineage)
		w.Lineage[0].SourceCut.Revision++
		return w, true, nil
	})
	if _, err := crossed.pendingProducerPreparationWitnesses(frontier); err == nil {
		t.Fatal("an ancestor from a different origin cut was accepted")
	}
}

func inheritedWitnessPlan(t *testing.T, run string, cut events.Event, others ...events.Event) RunForkPlan {
	t.Helper()
	plan := RunForkPlan{SourceRunID: run, ForkPoint: RunForkPoint{Kind: RunForkPointEvent, Revision: 1, EventID: cut.ID(), RoutingSource: cut.RoutingSource()}}
	selectors := append([]events.Event{cut}, others...)
	ids := make([]string, 0, len(selectors))
	for _, event := range selectors {
		ids = append(ids, event.ID())
	}
	plan, err := plan.WithHistoricalEvents(1, ids).WithHistoricalRouteSelectors(1, selectors)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func inheritedWitnessSelector(t *testing.T, run string, source events.RoutingSource) events.Event {
	t.Helper()
	const suffix = "work.ready"
	route := source.Route()
	payload := []byte(`{}`)
	event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(route.FlowInstance+"/"+suffix), "fixture", "", payload, 0, run, events.EventEnvelope{}, source, time.Unix(1700000000, 0).UTC())
	binding, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{
		BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), FlowID: route.FlowID, EventKey: suffix,
		SchemaDigest: "sha256:" + strings.Repeat("b", 64), SchemaClass: events.PayloadSchemaAuthored,
	})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := events.NewPayloadAdmission(payload, binding)
	if err != nil {
		t.Fatal(err)
	}
	event, err = events.ApplyPayloadAdmission(event, admission)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func preparedBindingFixture(t *testing.T) (SelectedForkPreparationBinding, managedcapabilities.Surface) {
	t.Helper()
	name, err := agentidentity.DeclaredName("worker", "target")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agentidentity.NewPlan(name, agentidentity.RootRoute())
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := plan.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	binding := SelectedForkPreparationBinding{
		ForkRunID: uuid.NewString(),
		SelectedForkPreparation: SelectedForkPreparation{
			DeclarationPlanFingerprint: "sha256:" + strings.Repeat("e", 64),
			PreparationID:              uuid.NewString(), ProcessGeneration: 1,
			SourceRunID: uuid.NewString(), ForkEventID: uuid.NewString(),
			Coordinates: managedcapabilities.SelectedForkPreparationCoordinates{
				ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "process", ProcessBootID: uuid.NewString(),
				BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), SourceFingerprint: strings.Repeat("b", 64),
				AdmittedPlanFingerprint: strings.Repeat("c", 64), ConfigurationFingerprint: strings.Repeat("d", 64), CatalogFingerprint: strings.Repeat("e", 64),
			},
		},
	}
	binding.ForkPoint = RunForkPoint{Kind: RunForkPointEvent, EventID: binding.ForkEventID, Revision: 1}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorPlan: plan, RuntimeMode: "startup_probe", Provider: "claude_cli", Transport: "cli", ProviderContract: "test", CreatedAt: time.Now().UTC(),
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityStartupProbe, ID: uuid.NewString(),
			ExecutionKind: managedcapabilities.ExecutionSelectedForkPreparation, ExecutionAuthorityID: binding.PreparationID,
			Preparation: &managedcapabilities.PreparedSelectedForkProbeAuthority{SelectedForkPreparationCoordinates: binding.Coordinates, ActorPlanFingerprint: fingerprint},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	binding.Actors = []SelectedForkPreparedActor{{Plan: plan, ConfigurationRevision: strings.Repeat("f", 64), Backend: selection.BackendClaudeCLI, Mode: executionmode.Live, SurfaceID: surface.ID, SurfaceIntegrity: surface.IntegrityHash}}
	return binding, surface
}

func TestSelectedPreparationBindingClosedCensus(t *testing.T) {
	base, surface := preparedBindingFixture(t)
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := base.ValidateSurface(base.Actors[0], surface); err != nil {
		t.Fatal(err)
	}
	// Reach: retained narrowed evidence is inspectable but cannot re-admit work.
	narrowed, err := surface.ObserveMismatch(managedcapabilities.DeliveryMismatch{BindingKind: managedcapabilities.BindingAPIDefinition, ExactName: "completion", Kind: "unavailable"})
	if err != nil {
		t.Fatal(err)
	}
	if err := narrowed.CanAdvanceFrom(surface); err != nil {
		t.Fatal(err)
	}
	if err := base.ValidateHistoricalSurface(base.Actors[0], narrowed); err != nil {
		t.Fatalf("valid narrowed history became fresh-admission refusal: %v", err)
	}
	if err := base.ValidateSurface(base.Actors[0], narrowed); err == nil {
		t.Fatal("historical narrowing restored fresh preparation authority")
	}
	for _, test := range []struct {
		name   string
		mutate func(*SelectedForkPreparationBinding)
	}{
		{"zero_process_generation", func(b *SelectedForkPreparationBinding) { b.ProcessGeneration = 0 }},
		{"missing_preparation", func(b *SelectedForkPreparationBinding) { b.PreparationID = "" }},
		{"source_as_fork", func(b *SelectedForkPreparationBinding) { b.ForkRunID = b.SourceRunID }},
		{"missing_census", func(b *SelectedForkPreparationBinding) { b.Actors = nil }},
		{"duplicate_actor", func(b *SelectedForkPreparationBinding) { b.Actors = append(b.Actors, b.Actors[0]) }},
		{"missing_receipt", func(b *SelectedForkPreparationBinding) { b.Actors[0].SurfaceID = "" }},
		{"missing_integrity", func(b *SelectedForkPreparationBinding) { b.Actors[0].SurfaceIntegrity = "" }},
		{"unknown_backend", func(b *SelectedForkPreparationBinding) { b.Actors[0].Backend = "unknown" }},
		{"noncanonical_backend", func(b *SelectedForkPreparationBinding) { b.Actors[0].Backend += " " }},
		{"wrong_mode", func(b *SelectedForkPreparationBinding) { b.Actors[0].Mode = executionmode.Mock }},
		{"native_with_receipt", func(b *SelectedForkPreparationBinding) { b.Actors[0].Backend = selection.BackendAnthropic }},
		{"mock_without_receipt", func(b *SelectedForkPreparationBinding) {
			b.Actors[0].Backend = selection.BackendMock
			b.Actors[0].Mode = executionmode.Mock
			b.Actors[0].SurfaceID = ""
			b.Actors[0].SurfaceIntegrity = ""
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			binding := base
			binding.Actors = append([]SelectedForkPreparedActor(nil), base.Actors...)
			test.mutate(&binding)
			if err := binding.Validate(); err == nil {
				t.Fatal("accepted invalid binding")
			}
		})
	}
	for _, backend := range []string{selection.BackendAnthropic, selection.BackendOpenAICompatible, selection.BackendOpenAIResponses} {
		t.Run(backend, func(t *testing.T) {
			binding := base
			actor := base.Actors[0]
			actor.Backend, actor.SurfaceID, actor.SurfaceIntegrity = backend, "", ""
			profile, err := selection.ResolveActiveBackend(backend)
			if err != nil {
				t.Fatal(err)
			}
			actor.Mode, err = selection.ExecutionModeForProfile(profile)
			if err != nil {
				t.Fatal(err)
			}
			binding.Actors = []SelectedForkPreparedActor{actor}
			if err := binding.Validate(); err != nil {
				t.Fatal(err)
			}
			if len(binding.SurfaceIDs()) != 0 {
				t.Fatal("invented receipt")
			}
		})
	}
	mock := base
	mock.Actors = append([]SelectedForkPreparedActor(nil), base.Actors...)
	mock.Actors[0].Backend, mock.Actors[0].Mode = selection.BackendMock, executionmode.Mock
	if !mock.Actors[0].RequiresProbe() || mock.Validate() != nil || len(mock.SurfaceIDs()) != 1 {
		t.Fatal("subprocess mock lost its exact preparation receipt")
	}
	empty := base
	empty.Actors = []SelectedForkPreparedActor{}
	if err := empty.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSelectedPreparationBindingDeploymentRevisionHasNoEvent(t *testing.T) {
	binding, _ := preparedBindingFixture(t)
	binding.ForkPoint = RunForkPoint{Kind: RunForkPointDeploymentRevision, Revision: 2}
	binding.ForkEventID = ""
	if err := binding.Validate(); err != nil {
		t.Fatalf("eventless preparation rejected: %v", err)
	}
	binding.ForkEventID = uuid.NewString()
	if err := binding.Validate(); err == nil {
		t.Fatal("eventless preparation accepted fabricated event")
	}
}

func TestSelectedPreparationBindingRejectsCrossedReceipt(t *testing.T) {
	base, surface := preparedBindingFixture(t)
	for _, test := range []struct {
		name   string
		mutate func(*SelectedForkPreparationBinding)
	}{
		{"preparation", func(b *SelectedForkPreparationBinding) { b.PreparationID = uuid.NewString() }},
		{"process", func(b *SelectedForkPreparationBinding) { b.Coordinates.ProcessAuthorityID = uuid.NewString() }},
		{"owner", func(b *SelectedForkPreparationBinding) { b.Coordinates.ProcessOwnerID += "other" }},
		{"boot", func(b *SelectedForkPreparationBinding) { b.Coordinates.ProcessBootID = uuid.NewString() }},
		{"source", func(b *SelectedForkPreparationBinding) { b.Coordinates.SourceFingerprint = strings.Repeat("1", 64) }},
		{"plan", func(b *SelectedForkPreparationBinding) {
			b.Coordinates.AdmittedPlanFingerprint = strings.Repeat("1", 64)
		}},
		{"configuration", func(b *SelectedForkPreparationBinding) {
			b.Coordinates.ConfigurationFingerprint = strings.Repeat("1", 64)
		}},
		{"catalog", func(b *SelectedForkPreparationBinding) { b.Coordinates.CatalogFingerprint = strings.Repeat("1", 64) }},
		{"receipt", func(b *SelectedForkPreparationBinding) { b.Actors[0].SurfaceID = uuid.NewString() }},
		{"integrity", func(b *SelectedForkPreparationBinding) { b.Actors[0].SurfaceIntegrity = strings.Repeat("1", 64) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			binding := base
			binding.Actors = append([]SelectedForkPreparedActor(nil), base.Actors...)
			test.mutate(&binding)
			if err := binding.Validate(); err != nil {
				t.Fatal(err)
			}
			if err := binding.ValidateSurface(binding.Actors[0], surface); err == nil {
				t.Fatal("accepted crossed receipt")
			}
		})
	}
	if err := base.ValidateSurfaceIDs([]string{surface.ID}); err != nil {
		t.Fatal(err)
	}
	for _, ids := range [][]string{nil, {uuid.NewString()}, {surface.ID, surface.ID}} {
		if err := base.ValidateSurfaceIDs(ids); err == nil {
			t.Fatal("accepted wrong receipt set")
		}
	}
}
