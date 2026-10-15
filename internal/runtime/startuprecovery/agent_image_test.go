package startuprecovery

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

type agentImageScopeKey struct{}

type agentImageReaderFixture struct {
	t           *testing.T
	head        agenttopology.SourceSetPlan
	headFound   bool
	stamp       startupownership.AuthorityInspection
	active      []deliverylifecycle.Snapshot
	states      map[agentidentity.Identity]manager.AgentLifecycleState
	profiles    map[string]deliverylifecycle.AgentClaimProfile
	selected    map[string]runfork.SelectedForkPreparedActor
	runs        map[string]bus.RunLifecycleSnapshot
	restarts    map[string]runlifecycle.StandingRestartDisposition
	owned       bool
	fail        string
	failure     error
	lateFailure bool
	cancel      context.CancelFunc
	calls       map[string]int
}

func newAgentImageReaderFixture(t *testing.T) *agentImageReaderFixture {
	t.Helper()
	source, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	agent := agentidentitytest.RootRuntime(t, "worker", "recovery-image")
	planIdentity, err := agent.Plan()
	if err != nil {
		t.Fatal(err)
	}
	config := strings.Repeat("b", 64)
	head, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: source.BundleHash()}}, []agenttopology.DesiredAgent{{
		Identity: planIdentity, Source: agenttopology.SourceCoordinate{BundleHash: source.BundleHash()}, ConfigRevision: config,
	}})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := agenttopology.StaticAdmission(head.Revision, source.BundleHash(), agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(source, uuid.NewString(), 3)
	if err != nil {
		t.Fatal(err)
	}
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agent.AgentID()), AgentIdentity: agent}
	snapshot := agentImageSnapshot(t, agent.RunID, route, authority, deliverylifecycle.StatusInProgress, 1)
	process := startupownership.Authority{AuthorityID: uuid.NewString(), AuthorityGeneration: 2, OwnerID: "agent-image", BootID: uuid.NewString(),
		RuntimeInstanceID: uuid.NewString(), Backend: "sqlite_retained_owner"}
	state := manager.AgentLifecycleState{Identity: agent, AgentID: agent.AgentID(), RuntimeEpoch: 2, Generation: 4, Phase: manager.AgentLifecycleRunning,
		ConfigRevision: config, RunMode: manager.AgentRunModeAuthoritativeDeliveryOnly, Topology: admission,
		ProcessBinding: manager.ProcessExecutionBinding{ProcessAuthorityID: process.AuthorityID, ProcessOwnerID: process.OwnerID,
			ProcessBootID: process.BootID, RuntimeInstanceID: process.RuntimeInstanceID, BundleHash: source.BundleHash(),
			GenerationGrantID: authority.ExecutionID(), RuntimeGeneration: authority.Generation()}}
	profile := deliverylifecycle.AgentClaimProfile{DeliveryID: snapshot.DeliveryID, EventID: snapshot.EventID, Identity: agent,
		ProfileID: selection.BackendAnthropic, ConfigurationRevision: config, BundleHash: source.BundleHash(),
		ExecutionID: authority.ExecutionID(), ExecutionGeneration: authority.Generation(), AgentRuntimeEpoch: state.RuntimeEpoch, AgentGeneration: state.Generation}
	return &agentImageReaderFixture{t: t, head: head, headFound: true,
		stamp:  startupownership.AuthorityInspection{Status: startupownership.AuthorityInspectionValid, Backend: process.Backend, Authority: &process},
		active: []deliverylifecycle.Snapshot{snapshot}, states: map[agentidentity.Identity]manager.AgentLifecycleState{agent: state},
		profiles: map[string]deliverylifecycle.AgentClaimProfile{snapshot.DeliveryID: profile}, selected: make(map[string]runfork.SelectedForkPreparedActor),
		runs:     map[string]bus.RunLifecycleSnapshot{agent.RunID: {RunID: agent.RunID, Status: "running"}},
		restarts: map[string]runlifecycle.StandingRestartDisposition{agent.RunID: {Kind: runlifecycle.StandingRestartOrdinary}},
		owned:    true, failure: errors.New("agent image reader failure"), calls: make(map[string]int)}
}

func agentImageSnapshot(t *testing.T, runID string, route events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority, status deliverylifecycle.Status, version int64) deliverylifecycle.Snapshot {
	t.Helper()
	obligation, err := deliverylifecycle.NewObligation(uuid.NewString(), runID, route, authority)
	if err != nil {
		t.Fatal(err)
	}
	return deliverylifecycle.Snapshot{DeliveryID: obligation.DeliveryID(), EventID: obligation.EventID(), RunID: runID,
		Route: obligation.Route(), RouteIdentity: obligation.RouteIdentity(), Authority: authority, Status: status, ClaimVersion: version,
		SubscriberClass: obligation.SubscriberClass(), SubscriberID: obligation.SubscriberID(), MaxRetries: obligation.MaxRetries()}
}

func (f *agentImageReaderFixture) enter(ctx context.Context, method string) error {
	f.t.Helper()
	if ctx.Value(agentImageScopeKey{}) != f {
		f.t.Fatalf("%s escaped the original inspection context", method)
	}
	f.calls[method]++
	if f.fail == method {
		return f.failure
	}
	return ctx.Err()
}

func (f *agentImageReaderFixture) InspectSnapshot(ctx context.Context, read func(context.Context) error) error {
	f.calls["inspection"]++
	if f.fail == "inspection" {
		return f.failure
	}
	if err := read(context.WithValue(ctx, agentImageScopeKey{}, f)); err != nil {
		return err
	}
	if f.cancel != nil {
		f.cancel()
	}
	if f.lateFailure {
		return f.failure
	}
	return nil
}

func (f *agentImageReaderFixture) InspectSourceSetHead(ctx context.Context) (agenttopology.SourceSetPlan, bool, error) {
	return f.head, f.headFound, f.enter(ctx, "head")
}

func (f *agentImageReaderFixture) InspectAuthority(ctx context.Context) (startupownership.AuthorityInspection, error) {
	return f.stamp, f.enter(ctx, "process")
}

func (f *agentImageReaderFixture) ReadActiveDeliverySnapshots(ctx context.Context) ([]deliverylifecycle.Snapshot, error) {
	return f.active, f.enter(ctx, "membership")
}

func (f *agentImageReaderFixture) LoadRunLifecycleSnapshot(ctx context.Context, id string) (bus.RunLifecycleSnapshot, error) {
	return f.runs[id], f.enter(ctx, "run")
}

func (f *agentImageReaderFixture) LoadAgentLifecycleState(ctx context.Context, id agentidentity.Identity) (manager.AgentLifecycleState, bool, error) {
	state, found := f.states[id]
	return state, found, f.enter(ctx, "lifecycle")
}

func (f *agentImageReaderFixture) ObserveOrdinaryRunSource(ctx context.Context, _ correlation.SourceArtifactFact, _ string) (bool, error) {
	return f.owned, f.enter(ctx, "source")
}

func (f *agentImageReaderFixture) StandingRunRestartDisposition(ctx context.Context, id string) (runlifecycle.StandingRestartDisposition, error) {
	return f.restarts[id], f.enter(ctx, "standing")
}

func (f *agentImageReaderFixture) ReadFirstAgentClaimProfile(ctx context.Context, snapshot deliverylifecycle.Snapshot) (deliverylifecycle.AgentClaimProfile, error) {
	return f.profiles[snapshot.DeliveryID], f.enter(ctx, "profile")
}

func (f *agentImageReaderFixture) ReadSelectedAgentPreparation(ctx context.Context, snapshot deliverylifecycle.Snapshot) (runfork.SelectedForkPreparedActor, error) {
	return f.selected[snapshot.DeliveryID], f.enter(ctx, "selected")
}

func TestAgentRecoveryImageClassifiesEveryActiveMemberWithoutAdmission(t *testing.T) {
	f := newAgentImageReaderFixture(t)
	original := f.active[0]
	pending := agentImageSnapshot(t, original.RunID, original.Route, original.Authority, deliverylifecycle.StatusPending, 0)
	retry := agentImageSnapshot(t, original.RunID, original.Route, original.Authority, deliverylifecycle.StatusFailed, 1)
	retry.NextEligibleAt = time.Now().Add(time.Hour)
	profile := f.profiles[original.DeliveryID]
	profile.DeliveryID, profile.EventID = retry.DeliveryID, retry.EventID
	f.profiles[retry.DeliveryID] = profile
	node, err := identity.AdmitExecutableNodeDeclaration(".", "unrelated")
	if err != nil {
		t.Fatal(err)
	}
	nonAgent := agentImageSnapshot(t, original.RunID, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node),
		Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: ".", FlowInstance: "."})}, original.Authority, deliverylifecycle.StatusPending, 0)
	f.active = []deliverylifecycle.Snapshot{nonAgent, pending, original, retry}
	image, err := InspectAgentRecoveryImage(t.Context(), f)
	if err != nil || len(image.Members) != 3 || f.calls["inspection"] != 1 || f.calls["profile"] != 2 || f.calls["selected"] != 0 {
		t.Fatalf("complete classification: %+v calls=%v err=%v", image, f.calls, err)
	}
	for i, want := range []deliverylifecycle.Snapshot{pending, original, retry} {
		member := image.Members[i]
		if !reflect.DeepEqual(member.Delivery, want) {
			t.Fatalf("image dropped/reordered/changed an agent member: %+v want=%+v", member, want)
		}
		if want.ClaimVersion == 0 {
			if member.CaptureKind != AgentRecoveryUnclaimed || member.FirstClaim != nil || member.Selected != nil {
				t.Fatalf("pending/v0 invented capture: %+v", member)
			}
		} else if member.CaptureKind != AgentRecoveryOrdinaryFirstClaim || member.FirstClaim == nil || !member.FirstClaim.MatchesDelivery(want) || member.Selected != nil {
			t.Fatalf("claimed member lost its canonical capture: %+v", member)
		}
	}
}

func appendSelectedAgentImageFixture(t *testing.T, f *agentImageReaderFixture, status deliverylifecycle.Status, version int64) deliverylifecycle.Snapshot {
	t.Helper()
	original := f.active[0]
	agent := agentidentitytest.RootDeclaredForRun(t, uuid.NewString(), "selected", "selected-image")
	plan, err := agent.Plan()
	if err != nil {
		t.Fatal(err)
	}
	state := f.states[original.Route.AgentIdentity]
	declaration, err := agenttopology.NewSelectedDeclarationPlan(original.Authority.SourceArtifact().BundleHash(), []agenttopology.DesiredAgent{{
		Identity: plan, ConfigRevision: state.ConfigRevision, Source: agenttopology.SourceCoordinate{BundleHash: state.ProcessBinding.BundleHash},
	}})
	if err != nil {
		t.Fatal(err)
	}
	state.Topology, err = agenttopology.SelectedDeclarationAdmission(agent.RunID, declaration)
	if err != nil {
		t.Fatal(err)
	}
	state.Identity, state.AgentID = agent, agent.AgentID()
	f.states[agent] = state
	authority, err := deliverylifecycle.NewSelectedExecutionAuthority(original.Authority.SourceArtifact(), uuid.NewString(), agent.RunID, 1)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := agentImageSnapshot(t, agent.RunID, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agent.AgentID()), AgentIdentity: agent}, authority, status, version)
	f.active = append(f.active, snapshot)
	f.runs[agent.RunID] = bus.RunLifecycleSnapshot{RunID: agent.RunID, Status: "running"}
	f.selected[snapshot.DeliveryID] = runfork.SelectedForkPreparedActor{Plan: plan, ConfigurationRevision: state.ConfigRevision,
		Backend: selection.BackendAnthropic, Mode: executionmode.Live}
	return snapshot
}

func TestAgentRecoveryImageSelectedUsesOnlyFixedPreparation(t *testing.T) {
	for _, status := range []deliverylifecycle.Status{deliverylifecycle.StatusPending, deliverylifecycle.StatusInProgress, deliverylifecycle.StatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			f := newAgentImageReaderFixture(t)
			version := int64(1)
			if status == deliverylifecycle.StatusPending {
				version = 0
			}
			snapshot := appendSelectedAgentImageFixture(t, f, status, version)
			image, err := InspectAgentRecoveryImage(t.Context(), f)
			if err != nil || len(image.Members) != 2 || f.calls["source"] != 1 || f.calls["standing"] != 1 || f.calls["profile"] != 1 {
				t.Fatalf("selected member borrowed ordinary/current-source evidence: %+v calls=%v err=%v", image, f.calls, err)
			}
			member := image.Members[1]
			if member.FirstClaim != nil || member.Restart != (runlifecycle.StandingRestartDisposition{}) || !reflect.DeepEqual(member.Delivery, snapshot) {
				t.Fatalf("selected classification changed durable evidence: %+v", member)
			}
			if version == 0 {
				if f.calls["selected"] != 0 || member.Selected != nil || member.CaptureKind != AgentRecoveryUnclaimed {
					t.Fatalf("selected pending/v0 fabricated preparation capture: %+v calls=%v", member, f.calls)
				}
			} else if f.calls["selected"] != 1 || member.Selected == nil || *member.Selected != f.selected[snapshot.DeliveryID] || member.CaptureKind != AgentRecoverySelectedPreparation {
				t.Fatalf("selected claim lost its fixed preparation: %+v calls=%v", member, f.calls)
			}
		})
	}
}

func TestAgentRecoveryImageSelectedFailureDiscardsEarlierOrdinaryMember(t *testing.T) {
	for _, mutation := range []string{"read", "missing", "foreign_plan", "missing_configuration", "foreign_configuration", "missing_backend", "foreign_run", "foreign_source"} {
		t.Run(mutation, func(t *testing.T) {
			f := newAgentImageReaderFixture(t)
			snapshot := appendSelectedAgentImageFixture(t, f, deliverylifecycle.StatusInProgress, 1)
			prepared := f.selected[snapshot.DeliveryID]
			state := f.states[snapshot.Route.AgentIdentity]
			switch mutation {
			case "read":
				f.fail = "selected"
			case "missing":
				prepared = runfork.SelectedForkPreparedActor{}
			case "foreign_plan":
				prepared.Plan.Name.AgentID = "foreign"
			case "missing_configuration":
				prepared.ConfigurationRevision = ""
			case "foreign_configuration":
				prepared.ConfigurationRevision = strings.Repeat("e", 64)
			case "missing_backend":
				prepared.Backend = ""
			case "foreign_run":
				state.Topology.Authority.Selected.RunID = uuid.NewString()
			case "foreign_source":
				state.Topology.Authority.Selected.BundleHash = "bundle-v2:sha256:" + strings.Repeat("e", 64)
			}
			f.states[snapshot.Route.AgentIdentity], f.selected[snapshot.DeliveryID] = state, prepared
			image, err := InspectAgentRecoveryImage(t.Context(), f)
			if err == nil || !reflect.DeepEqual(image, AgentRecoveryImage{}) || f.calls["profile"] != 1 {
				t.Fatalf("selected failure exposed earlier successful member: %+v calls=%v err=%v", image, f.calls, err)
			}
			if mutation == "read" && !errors.Is(err, f.failure) {
				t.Fatalf("lost canonical selected reader error: %v", err)
			}
		})
	}
}

func TestAgentRecoveryImagePreservesStandingRestartDisposition(t *testing.T) {
	for _, suspended := range []bool{false, true} {
		f := newAgentImageReaderFixture(t)
		original := f.active[0]
		state := f.states[original.Route.AgentIdentity]
		agent := agentidentitytest.RuntimeForRun(t, original.RunID, "standing", "standing-image", "service", "one", "service/one")
		snapshot := agentImageSnapshot(t, original.RunID, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agent.AgentID()), AgentIdentity: agent}, original.Authority, original.Status, original.ClaimVersion)
		state.Identity, state.AgentID = agent, agent.AgentID()
		profile := f.profiles[original.DeliveryID]
		profile.Identity, profile.DeliveryID, profile.EventID = agent, snapshot.DeliveryID, snapshot.EventID
		f.active, f.profiles = []deliverylifecycle.Snapshot{snapshot}, map[string]deliverylifecycle.AgentClaimProfile{snapshot.DeliveryID: profile}
		var err error
		state.Topology, err = agenttopology.FlowReadinessAdmission(snapshot.RunID, state.Identity.FlowInstance(), "standing-plan")
		if err != nil {
			t.Fatal(err)
		}
		f.states[state.Identity] = state
		fact := runlifecycle.StandingRestartFact{ExactCurrent: true, ServiceID: uuid.NewString(), RunID: snapshot.RunID, Generation: 3,
			DeclarationPresent: true, BindingEnabled: true, EffectiveState: "active", OperatorOverride: "none", RunState: "running"}
		if suspended {
			fact.EffectiveState, fact.OperatorOverride, fact.RunState = "suspended", "suspended", "paused"
		}
		f.runs[snapshot.RunID] = bus.RunLifecycleSnapshot{RunID: snapshot.RunID, Status: fact.RunState}
		disposition, err := runlifecycle.ClassifyStandingRestart(fact)
		if err != nil {
			t.Fatal(err)
		}
		f.restarts[snapshot.RunID] = disposition
		image, err := InspectAgentRecoveryImage(t.Context(), f)
		if err != nil || len(image.Members) != 1 || image.Members[0].Restart != disposition || image.Members[0].FirstClaim == nil {
			t.Fatalf("image reclassified standing state as recovery permission: %+v err=%v", image, err)
		}
		before := image
		contradictory := disposition
		contradictory.RunState = "completed"
		f.restarts[snapshot.RunID] = contradictory
		image, err = InspectAgentRecoveryImage(t.Context(), f)
		if err == nil || !reflect.DeepEqual(image, AgentRecoveryImage{}) {
			t.Fatalf("standing disposition contradicted the same-cut run: %+v err=%v", image, err)
		}
		disposition.RunID = uuid.NewString()
		f.restarts[snapshot.RunID] = disposition
		image, err = InspectAgentRecoveryImage(t.Context(), f)
		if err == nil || !reflect.DeepEqual(image, AgentRecoveryImage{}) || len(before.Members) != 1 {
			t.Fatalf("foreign standing generation admitted: %+v err=%v", image, err)
		}
	}
}

func TestAgentRecoveryImageKeepsHistoricalCaptureAcrossReplacement(t *testing.T) {
	f := newAgentImageReaderFixture(t)
	snapshot := f.active[0]
	profile := f.profiles[snapshot.DeliveryID]
	profile.AgentRuntimeEpoch--
	profile.AgentGeneration--
	profile.ConfigurationRevision = strings.Repeat("c", 64)
	profile.ExecutionID = uuid.NewString()
	profile.ExecutionGeneration++
	f.profiles[snapshot.DeliveryID] = profile
	image, err := InspectAgentRecoveryImage(t.Context(), f)
	if err != nil || len(image.Members) != 1 || image.Members[0].FirstClaim == nil || *image.Members[0].FirstClaim != profile {
		t.Fatalf("current configuration replaced immutable capture: %+v err=%v", image, err)
	}
}

func TestAgentRecoveryImageRefusesEveryReaderFailureWithoutPartialEvidence(t *testing.T) {
	for _, method := range []string{"inspection", "head", "process", "membership", "run", "lifecycle", "source", "standing", "profile", "close"} {
		t.Run(method, func(t *testing.T) {
			f := newAgentImageReaderFixture(t)
			f.fail, f.lateFailure = method, method == "close"
			image, err := InspectAgentRecoveryImage(t.Context(), f)
			if !errors.Is(err, f.failure) || !reflect.DeepEqual(image, AgentRecoveryImage{}) {
				t.Fatalf("failed read exposed partial image or lost original failure: %+v err=%v", image, err)
			}
		})
	}
}

func TestAgentRecoveryImageRefusesMissingForeignAndContradictoryEvidence(t *testing.T) {
	for _, mutation := range []string{"absent_head", "corrupt_head", "missing_process", "corrupt_process", "nil_membership", "duplicate", "foreign_id", "foreign_class", "terminal", "missing_claim", "claimed_pending", "missing_run", "missing_actor", "foreign_actor", "zero_generation", "foreign_bundle", "missing_source", "foreign_head_source", "foreign_config", "missing_restart", "missing_profile", "foreign_profile", "future_generation", "future_epoch", "same_generation_config"} {
		t.Run(mutation, func(t *testing.T) {
			f := newAgentImageReaderFixture(t)
			snapshot := f.active[0]
			state := f.states[snapshot.Route.AgentIdentity]
			profile := f.profiles[snapshot.DeliveryID]
			switch mutation {
			case "absent_head":
				f.headFound = false
			case "corrupt_head":
				f.head.Revision = "invalid"
			case "missing_process":
				f.stamp.Authority = nil
			case "corrupt_process":
				f.stamp.Authority.AuthorityGeneration = 0
			case "nil_membership":
				f.active = nil
			case "duplicate":
				f.active = append(f.active, snapshot)
			case "foreign_id":
				f.active[0].DeliveryID = uuid.NewString()
			case "foreign_class":
				f.active[0].SubscriberClass = deliverylifecycle.SubscriberNode
			case "terminal":
				f.active[0].Status = deliverylifecycle.StatusDelivered
			case "missing_claim":
				f.active[0].ClaimVersion = 0
			case "claimed_pending":
				f.active[0].Status = deliverylifecycle.StatusPending
			case "missing_run":
				delete(f.runs, snapshot.RunID)
			case "missing_actor":
				delete(f.states, snapshot.Route.AgentIdentity)
			case "foreign_actor":
				state.Identity.Name.AgentID = "foreign"
			case "zero_generation":
				state.Generation = 0
			case "foreign_bundle":
				state.ProcessBinding.BundleHash = "bundle-v2:sha256:" + strings.Repeat("e", 64)
			case "missing_source":
				f.owned = false
			case "foreign_head_source":
				f.head, _ = agenttopology.EmptySourceSetPlan()
			case "foreign_config":
				state.ConfigRevision = strings.Repeat("e", 64)
			case "missing_restart":
				delete(f.restarts, snapshot.RunID)
			case "missing_profile":
				profile = deliverylifecycle.AgentClaimProfile{}
			case "foreign_profile":
				profile.EventID = uuid.NewString()
			case "future_generation":
				profile.AgentGeneration++
			case "future_epoch":
				profile.AgentRuntimeEpoch++
			case "same_generation_config":
				profile.ConfigurationRevision = strings.Repeat("e", 64)
			}
			if mutation != "missing_actor" {
				f.states[snapshot.Route.AgentIdentity] = state
			}
			f.profiles[snapshot.DeliveryID] = profile
			image, err := InspectAgentRecoveryImage(t.Context(), f)
			if err == nil || !reflect.DeepEqual(image, AgentRecoveryImage{}) {
				t.Fatalf("%s admitted partial/invalid image: %+v err=%v", mutation, image, err)
			}
		})
	}
}

func TestAgentRecoveryImageEmptyAndCancellationAreExplicit(t *testing.T) {
	f := newAgentImageReaderFixture(t)
	f.active = []deliverylifecycle.Snapshot{}
	f.head, _ = agenttopology.EmptySourceSetPlan()
	image, err := InspectAgentRecoveryImage(t.Context(), f)
	if err != nil || image.Members == nil || len(image.Members) != 0 {
		t.Fatalf("valid empty image differs from absence/failure: %+v err=%v", image, err)
	}
	for _, late := range []bool{false, true} {
		f := newAgentImageReaderFixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		if late {
			f.cancel = cancel
		} else {
			cancel()
		}
		image, err := InspectAgentRecoveryImage(ctx, f)
		cancel()
		if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(image, AgentRecoveryImage{}) {
			t.Fatalf("canceled inspection exposed partial image: %+v err=%v", image, err)
		}
	}
	if image, err := InspectAgentRecoveryImage(t.Context(), nil); err == nil || !reflect.DeepEqual(image, AgentRecoveryImage{}) {
		t.Fatal("missing reader produced a recovery image")
	}
}
