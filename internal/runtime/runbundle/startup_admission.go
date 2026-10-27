package runbundle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type UpgradeReader interface {
	ActiveAvailabilityReader
	ReadBundleUpgradeState(context.Context, string, semanticview.Source) (UpgradeState, error)
}

// Sources retain declarations, not an admission result. The transition rereads
// domain state and derives the affected runs before performing any mutation.
type UpgradePlan struct {
	BundleHash string                         `json:"bundle_hash"`
	Runs       map[string]string              `json:"runs"`
	Sources    map[string]semanticview.Source `json:"-"`
}

func AdmitPinnedSources(ctx context.Context, reader UpgradeReader, candidate semanticview.Source, load func(context.Context, string) (semanticview.Source, error)) (*UpgradePlan, error) {
	if ctx == nil {
		return nil, errors.New("bundle upgrade context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reader == nil || candidate == nil || load == nil {
		return nil, runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "pinned_source_reader_unavailable", "run-bundle", "admit_sources", nil)
	}
	hash, err := upgradeSourceHash(candidate)
	if err != nil {
		return nil, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "pinned_source_hash_invalid", "run-bundle", "admit_sources", nil, err)
	}
	plan := &UpgradePlan{BundleHash: hash, Runs: map[string]string{}, Sources: map[string]semanticview.Source{hash: candidate}}
	availabilities, err := reader.ActiveNonStandingRunBundleAvailabilities(ctx)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "pinned_source_reader_unavailable", "run-bundle", "admit_sources", nil, err)
	}
	var blockers []UpgradeBlocker
	for _, available := range availabilities {
		if !available.Available() {
			return nil, fmt.Errorf("bundle upgrade source unavailable: %s", available.DetailString())
		}
		if available.Status == string(runlifecycle.StatePaused) || available.BundleHash == hash {
			continue
		}
		current := plan.Sources[available.BundleHash]
		if current == nil {
			current, err = load(ctx, available.BundleHash)
			if err != nil {
				return nil, runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "pinned_source_reader_unavailable", "run-bundle", "admit_sources", nil, err)
			}
			before, err := upgradeSourceHash(current)
			if err != nil || before != available.BundleHash {
				return nil, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "pinned_source_hash_invalid", "run-bundle", "admit_sources", nil, errors.Join(fmt.Errorf("bundle upgrade source differs from run %s", available.RunID), err))
			}
			plan.Sources[available.BundleHash] = current
		}
		state, err := reader.ReadBundleUpgradeState(ctx, available.RunID, current)
		if err != nil {
			return nil, runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "pinned_source_reader_unavailable", "run-bundle", "admit_sources", nil, err)
		}
		stranded, err := CheckUpgrade(ctx, current, candidate, state)
		if err != nil {
			return nil, err
		}
		blockers = append(blockers, stranded...)
		plan.Runs[available.RunID] = available.BundleHash
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(blockers) != 0 {
		return nil, &UpgradeConflictError{Blockers: blockers}
	}
	return plan, nil
}

func upgradeSourceHash(source semanticview.Source) (string, error) {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil || bundle.SourceArtifact == nil {
		return "", errors.New("bundle upgrade requires an admitted artifact")
	}
	identity, err := contracts.BootBundleIdentity(bundle)
	return identity.BundleHash, err
}

// UpgradeState contains ordinary domain readbacks from one selected-store cut.
// It grants neither mutation authority nor permission to change a run's source.
type UpgradeState struct {
	RunID      string
	Instances  []UpgradeInstance
	Deliveries []deliverylifecycle.Snapshot
	Cards      []decisioncard.ListItem
	Barriers   []fanoutbarrier.Barrier
	Events     []events.Event
}

// UpgradeInstance avoids a dependency on the pipeline executor, which itself
// consumes runbundle. Its fields come from the canonical instance decoder.
type UpgradeInstance struct {
	Route        flowidentity.Route
	EntityID     string
	FlowID       string
	Status       string
	Stage        string
	Fields       map[string]any
	StateBuckets map[string]any
}

type UpgradeBlocker struct {
	RunID        string
	FlowInstance string
	EntityID     string
	Kind         string
	Detail       string
	Location     string
}

// UpgradeConflictError reports all stranded obligations without changing them.
type UpgradeConflictError struct {
	Blockers []UpgradeBlocker
}

func (e *UpgradeConflictError) Error() string {
	details := make([]string, 0, len(e.Blockers))
	for _, blocker := range e.Blockers {
		details = append(details, fmt.Sprintf("run %s instance %s entity %s: %s (%s) at %s",
			blocker.RunID, blocker.FlowInstance, blocker.EntityID, blocker.Kind, blocker.Detail, blocker.Location))
	}
	return "bundle upgrade would strand retained work: " + strings.Join(details, "; ")
}

func (e *UpgradeConflictError) Unwrap() error {
	return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "bundle_upgrade_blocked", "run-bundle", "upgrade", nil)
}

// CheckUpgrade is shared by verify and serve. The native transition owner must
// reread at its fenced commit boundary; this result is not a reusable admission.
func CheckUpgrade(ctx context.Context, current, candidate semanticview.Source, state UpgradeState) ([]UpgradeBlocker, error) {
	if ctx == nil || current == nil || candidate == nil || strings.TrimSpace(state.RunID) == "" {
		return nil, errors.New("bundle upgrade requires context, both sources and the exact run")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	before, _ := semanticview.Bundle(current)
	after, _ := semanticview.Bundle(candidate)
	if before == nil || after == nil {
		return nil, errors.New("bundle upgrade requires both admitted declarations")
	}
	if before == after || (before.SourceArtifact != nil && after.SourceArtifact != nil && before.SourceArtifact.BundleHash() == after.SourceArtifact.BundleHash()) {
		return nil, nil
	}
	check := upgradeCheck{current: current, candidate: candidate, runID: state.RunID}
	for _, instance := range state.Instances {
		if err := check.instance(instance); err != nil {
			return nil, err
		}
	}
	byEvent := make(map[string]events.Event, len(state.Events))
	for _, event := range state.Events {
		if event.RunID() != state.RunID || event.ID() == "" {
			return nil, errors.New("bundle upgrade event belongs to another run or lacks identity")
		}
		byEvent[event.ID()] = event
		changed, err := upgradeEventSchemaChanged(current, candidate, event)
		if err != nil {
			return nil, err
		}
		if changed {
			route := event.RoutingSource().Route()
			check.add(route.FlowInstance, event.EntityID(), route.FlowID, "queued_event_schema_changed", string(event.Type()), "events", string(event.Type()))
		}
	}
	for _, delivery := range state.Deliveries {
		if err := check.delivery(delivery, byEvent); err != nil {
			return nil, err
		}
	}
	for _, card := range state.Cards {
		if err := check.card(card); err != nil {
			return nil, err
		}
	}
	for _, barrier := range state.Barriers {
		if barrier.Registration.IntentKey.RunID != state.RunID {
			return nil, errors.New("bundle upgrade barrier belongs to another run")
		}
		if barrier.Status.BlocksCompletion() {
			registration := barrier.Registration
			check.add(registration.Route.InstancePath, registration.EntityID, registration.IntentKey.ElementRef.FlowPath, "join_in_flight", registration.IntentKey.String(), "nodes", "")
		}
	}
	return check.blockers, ctx.Err()
}

type upgradeCheck struct {
	current, candidate semanticview.Source
	runID              string
	blockers           []UpgradeBlocker
}

func (c *upgradeCheck) add(instance, entity, flow, kind, detail, family, member string) {
	c.blockers = append(c.blockers, UpgradeBlocker{
		RunID: c.runID, FlowInstance: instance, EntityID: entity, Kind: kind,
		Detail: detail, Location: upgradeLocation(c.candidate, flow, family, member),
	})
}

func (c *upgradeCheck) instance(instance UpgradeInstance) error {
	if !instance.Route.Valid() || instance.EntityID == "" || instance.FlowID == "" {
		return errors.New("bundle upgrade instance readback is incomplete")
	}
	if _, exists := c.current.FlowSchemaByID(instance.FlowID); !exists {
		return errors.New("bundle upgrade instance has no admitted source declaration")
	}
	if instance.Status != "active" {
		return nil
	}
	if instance.Stage != "" && !slices.Contains(c.candidate.FlowStates(instance.FlowID), instance.Stage) {
		c.add(instance.Route.InstancePath, instance.EntityID, instance.FlowID, "stage_removed", instance.Stage, "schema", "stages")
	}
	if err := checkUpgradeFields(c.candidate, instance); err != nil {
		c.add(instance.Route.InstancePath, instance.EntityID, instance.FlowID, "field_value_incompatible", err.Error(), "entities", "")
	}
	buckets, err := joinruntime.PersistedBuckets(instance.StateBuckets)
	if err != nil {
		return err
	}
	joins, err := joinruntime.List(buckets)
	if err != nil {
		return err
	}
	for _, join := range joins {
		if join.Status == joinruntime.StatusOpen || join.OutcomePending {
			c.add(instance.Route.InstancePath, instance.EntityID, instance.FlowID, "join_in_flight", join.JoinID(), "nodes", "")
		}
	}
	return nil
}

func (c *upgradeCheck) delivery(delivery deliverylifecycle.Snapshot, byEvent map[string]events.Event) error {
	if delivery.RunID != c.runID {
		return errors.New("bundle upgrade delivery belongs to another run")
	}
	if delivery.Terminal() {
		return nil
	}
	event, found := byEvent[delivery.EventID]
	if !found {
		return errors.New("bundle upgrade delivery lacks its exact event readback")
	}
	if !upgradeDeliveryTargetPresent(c.candidate, delivery, event) {
		route := delivery.Route.Target.Route()
		c.add(route.FlowInstance, route.EntityID, route.FlowID, "delivery_target_removed", delivery.SubscriberID+"/"+string(event.Type()), "nodes", "")
	}
	return nil
}

func (c *upgradeCheck) card(card decisioncard.ListItem) error {
	if card.RunID != c.runID {
		return errors.New("bundle upgrade decision card belongs to another run")
	}
	if card.Status != "pending" || card.Anchor.Kind() != decisioncard.AnchorKindStageGate {
		return nil
	}
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return err
	}
	if _, exists := c.candidate.WorkflowGateForStage(anchor.FlowID, anchor.Stage); !exists {
		c.add(anchor.Route.InstancePath, anchor.EntityID, anchor.FlowID, "gate_removed", anchor.Stage, "schema", "stages")
	}
	return nil
}

func checkUpgradeFields(candidate semanticview.Source, instance UpgradeInstance) error {
	if len(instance.Fields) == 0 {
		return nil
	}
	contract, found := entityruntime.ResolveForFlow(candidate, instance.FlowID)
	if !found && len(instance.Fields) != 0 {
		return errors.New("held entity fields have no candidate declaration")
	}
	normalized, err := entityruntime.NormalizeState(contract, instance.Fields)
	if err != nil {
		return err
	}
	before, err := canonicaljson.Bytes(instance.Fields)
	if err != nil {
		return err
	}
	after, err := canonicaljson.Bytes(normalized)
	if err != nil {
		return err
	}
	if string(before) != string(after) {
		return errors.New("held fields would require a value migration")
	}
	return nil
}

func upgradeDeliveryTargetPresent(candidate semanticview.Source, delivery deliverylifecycle.Snapshot, event events.Event) bool {
	if node, isNode := delivery.Route.Recipient.Node(); isNode {
		handlerEvent := event.Type()
		if selected, found := delivery.Route.ConnectClaim.NodeHandlerEvent(node); found {
			handlerEvent = selected
		}
		_, exists := candidate.ExecutableNodeEventHandler(node, string(handlerEvent))
		return exists
	}
	if delivery.Route.Recipient.IsAgent() {
		flow := delivery.Route.Target.Route().FlowID
		_, _, exists := semanticview.ResolveAgentRegistryEntry(candidate, actors.AgentConfig{ID: delivery.SubscriberID, FlowID: flow})
		return exists
	}
	return false
}

func upgradeEventSchemaChanged(current, candidate semanticview.Source, event events.Event) (bool, error) {
	flow := event.RoutingSource().Route().FlowID
	if flow == "" {
		flow = "."
	}
	before, found, err := upgradeEventSchema(current, flow, string(event.Type()))
	if err != nil || !found {
		return false, errors.Join(errors.New("queued event has no admitted source schema"), err)
	}
	after, found, err := upgradeEventSchema(candidate, flow, string(event.Type()))
	return !found || !bytes.Equal(before, after), err
}

func upgradeEventSchema(source semanticview.Source, flow, event string) ([]byte, bool, error) {
	resolved := semanticview.ResolveEventSchema(source, flow, event)
	if resolved.HasSchema {
		if err := resolved.UnresolvedTypeError(); err != nil {
			return nil, true, err
		}
		value, err := canonicaljson.Bytes(eventschema.CanonicalAcceptanceSchema(resolved.Schema.Schema))
		return value, true, err
	}
	// Reference-only platform entries retain their subtype owner; do not invent
	// an empty payload schema for them during bundle compatibility checks.
	proof := semanticview.ResolveFlowEventProof(source, flow, event)
	if !proof.HasSchema || !contracts.PlatformEventCatalogContains(source.PlatformSpec(), proof.CatalogKey) {
		return nil, false, nil
	}
	value, err := canonicaljson.Bytes(proof.Entry)
	return value, true, err
}

func upgradeLocation(source semanticview.Source, flow, family, member string) string {
	bundle, _ := semanticview.Bundle(source)
	if bundle == nil {
		return ""
	}
	if flow == "" {
		flow = "."
	}
	declaration, found := bundle.FlowSources[flow]
	if !found {
		flow, declaration, member = ".", bundle.FlowSources["."], ""
	}
	file := map[string]string{"schema": declaration.Schema, "entities": declaration.Entities, "events": declaration.Events, "nodes": declaration.Nodes}[family]
	if file == "" {
		file, member = declaration.Schema, ""
	}
	if family == "events" && member != "" {
		proof := semanticview.ResolveFlowEventProof(source, flow, member)
		if proof.Local != "" {
			member = "[" + strconv.Quote(proof.Local) + "]"
		}
	}
	if location, found := bundle.IntroductionLocation(file, member, false); found {
		return fmt.Sprintf("%s:%d:%d", file, location.Line, location.Column)
	}
	if location, found := bundle.IntroductionLocation(file, "", false); found {
		return fmt.Sprintf("%s:%d:%d", file, location.Line, location.Column)
	}
	return file
}
