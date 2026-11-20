package runfork

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
)

type historicalRouteSelectors struct {
	runID  string
	point  RunForkPoint
	events map[string]events.Event
}

// HistoricalRouteSelectorRequired narrows this optional projection using the
// existing event/schema classes. Invalid declared business facts still require
// canonical decoding; absence is not a way to repair their admission.
func HistoricalRouteSelectorRequired(class events.EventAdmissionClass, schemaClass events.PayloadSchemaClass, source events.RoutingSource) bool {
	if source.Empty() || source.Kind() == events.RoutingSourcePlatformControl || schemaClass == events.PayloadSchemaPlatform || schemaClass == events.PayloadSchemaSchemaLess {
		return false
	}
	switch class {
	case events.EventAdmissionRootIngress, events.EventAdmissionOperatorInjected,
		events.EventAdmissionChild, events.EventAdmissionInstancePublication,
		events.EventAdmissionReplay, events.EventAdmissionSelectedForkReplay, events.EventAdmissionInheritedFanOut:
		return true
	default:
		return false
	}
}

// WithHistoricalRouteSelectors retains decoded fixed-cut publications for
// selector evaluation only. It conveys neither source recipients nor replay work.
func (p RunForkPlan) WithHistoricalRouteSelectors(revision int64, publications []events.Event) (RunForkPlan, error) {
	ids, known := p.HistoricalEventIDs(revision)
	if !known || revision != p.ForkPoint.Revision || p.ForkPoint.Validate() != nil {
		return RunForkPlan{}, fmt.Errorf("route selectors require the exact fixed cut")
	}
	members := make(map[string]bool, len(ids))
	for _, id := range ids {
		members[id] = true
	}
	bound := &historicalRouteSelectors{runID: p.SourceRunID, point: p.ForkPoint, events: make(map[string]events.Event, len(publications))}
	for _, event := range publications {
		admission, admitted := event.PayloadAdmission()
		if !HistoricalRouteSelectorRequired(event.AdmissionClass(), admission.Binding().SchemaClass(), event.RoutingSource()) {
			continue
		}
		if event.RunID() != p.SourceRunID || !members[event.ID()] || event.ID() == "" || !admitted || !admission.Valid() {
			return RunForkPlan{}, fmt.Errorf("route selector publication is outside admitted fixed source history")
		}
		if err := events.ValidatePersistentEvent(event); err != nil {
			return RunForkPlan{}, fmt.Errorf("route selector publication %s: %w", event.ID(), err)
		}
		if _, duplicate := bound.events[event.ID()]; duplicate {
			return RunForkPlan{}, fmt.Errorf("duplicate route selector publication %s", event.ID())
		}
		bound.events[event.ID()] = event.Clone()
	}
	p.historicalRouteSelectors = bound
	return p, nil
}

func (p RunForkPlan) HistoricalRouteSelector(eventID string) (events.Event, bool, error) {
	var absent events.Event
	bound := p.historicalRouteSelectors
	if bound == nil {
		return absent, false, nil
	}
	if bound.runID != p.SourceRunID || bound.point != p.ForkPoint || bound.point.Revision != p.historicalRevision {
		return absent, false, fmt.Errorf("route selectors do not belong to this fixed cut")
	}
	event, found := bound.events[eventID]
	if !found {
		return absent, false, nil
	}
	ids, known := p.HistoricalEventIDs(bound.point.Revision)
	if !known || !slices.Contains(ids, eventID) || event.RunID() != p.SourceRunID {
		return absent, false, fmt.Errorf("route selector is outside fixed source history")
	}
	return event.Clone(), true, nil
}

// ConstructedRouteEvidence is an invocation-private binding, not an executable
// recipient grant. Only the route owner derives recipients before binding them.
type ConstructedRouteEvidence struct {
	proof               RunForkSelectedContractDynamicTopologyProof
	frontierFingerprint string
	selection           RunForkContractSelection
	completedRoutes     []string
}

// BindEagerChildRouteEvidence seals the completed edge checked by the fixed-cut
// admission owner. The keyed constructor binding is unchanged.
func (p RunForkPlan) BindEagerChildRouteEvidence(entityID, parentID, eventID string, recipients []RunForkContractFrontierRecipient, routes []events.DeliveryRoute, frontier RunForkContractFrontierAdmission) (ConstructedRouteEvidence, error) {
	bound, err := p.BindConstructedRouteEvidence(entityID, eventID, recipients, frontier)
	if err != nil {
		return ConstructedRouteEvidence{}, err
	}
	var parent *RunForkMaterializedEntitySnapshotMetadata
	for _, entity := range p.Entities {
		if entity.EntityID == parentID {
			parent = entity.MaterializationMetadata
		}
	}
	path := bound.proof.FlowInstance
	observed, err := completedConstructedRouteKeys(path, routes)
	if err != nil || parent == nil || len(observed) == 0 {
		return ConstructedRouteEvidence{}, fmt.Errorf("eager child topology lacks exact parent or completed edge: %v", err)
	}
	var cutRoutes []events.DeliveryRoute
	for _, work := range p.PendingWork {
		if work.EventID == eventID && work.Classification == RunForkPendingClassificationDeliveredCompleted {
			cutRoutes = append(cutRoutes, work.DeliveryRoute)
		}
	}
	retained, err := completedConstructedRouteKeys(path, cutRoutes)
	if err != nil || !slices.Equal(observed, retained) {
		return ConstructedRouteEvidence{}, fmt.Errorf("eager child route differs from the completed fixed-cut edge: %v", err)
	}
	raw, err := json.Marshal(struct {
		Binding string
		Parent  *RunForkMaterializedEntitySnapshotMetadata
		Routes  []string
	}{bound.proof.FixedCutEvidenceFingerprint, parent, retained})
	if err != nil {
		return ConstructedRouteEvidence{}, err
	}
	digest := sha256.Sum256(raw)
	bound.proof.FixedCutEvidenceFingerprint = hex.EncodeToString(digest[:])
	bound.completedRoutes = retained
	return bound, nil
}

func completedConstructedRouteKeys(path string, routes []events.DeliveryRoute) ([]string, error) {
	var keys []string
	for _, route := range routes {
		if route.Target.Route().FlowInstance != path {
			continue
		}
		id, err := route.Identity()
		if err != nil {
			return nil, err
		}
		keys = append(keys, events.EncodeDeliveryRouteIdentity(id))
	}
	slices.Sort(keys)
	return keys, nil
}

func (p RunForkPlan) BindConstructedRouteEvidence(entityID, eventID string, recipients []RunForkContractFrontierRecipient, frontier RunForkContractFrontierAdmission) (ConstructedRouteEvidence, error) {
	_, _, frontierFingerprint, err := RunForkContractFrontierEvidenceBinding(frontier)
	if err != nil {
		return ConstructedRouteEvidence{}, err
	}
	event, found, err := p.HistoricalRouteSelector(eventID)
	if err != nil {
		return ConstructedRouteEvidence{}, err
	}
	if !found {
		return ConstructedRouteEvidence{}, fmt.Errorf("constructed topology requires its retained selector")
	}
	var entity *RunForkEntityState
	for i := range p.Entities {
		if p.Entities[i].EntityID == entityID {
			if entity != nil {
				return ConstructedRouteEvidence{}, fmt.Errorf("duplicate constructor evidence")
			}
			entity = &p.Entities[i]
		}
	}
	if entity == nil || entity.MaterializationMetadata == nil {
		return ConstructedRouteEvidence{}, fmt.Errorf("constructed topology requires its captured constructor")
	}
	meta := entity.MaterializationMetadata
	native := len(bytes.TrimSpace(meta.InitialMaterialization)) != 0 && !bytes.Equal(bytes.TrimSpace(meta.InitialMaterialization), []byte("null"))
	historical := meta.HistoricalConstruction
	if meta.Owner != RunForkMaterializedEntitySnapshotMetadataOwner || meta.Source != RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance ||
		meta.FlowInstance == "" || len(meta.FlowConfig) == 0 || native == (historical != nil) {
		return ConstructedRouteEvidence{}, fmt.Errorf("constructed topology has incomplete fixed header")
	}
	// The route owner has admitted the complete captured chain before deriving
	// recipients. Bind that unchanged provenance, never a synthetic native receipt.
	if historical != nil && (historical.SourceRunID == "" || historical.SourceRunID == p.SourceRunID || historical.SourceRevision <= 0 || historical.SourceEntity.MaterializationMetadata == nil) {
		return ConstructedRouteEvidence{}, fmt.Errorf("constructed topology lacks its exact historical cut")
	}
	canonical, err := forkrecipient.CanonicalSet(recipients)
	if err != nil || len(canonical) == 0 {
		return ConstructedRouteEvidence{}, fmt.Errorf("constructed topology lacks derived recipients")
	}
	for _, recipient := range canonical {
		if recipient.Path != meta.FlowInstance {
			return ConstructedRouteEvidence{}, fmt.Errorf("constructed topology recipient has foreign instance")
		}
	}
	admission, _ := event.PayloadAdmission()
	binding := admission.Binding()
	raw, err := json.Marshal(struct {
		RunID               string
		Point               RunForkPoint
		EntityID            string
		Metadata            *RunForkMaterializedEntitySnapshotMetadata
		Event               events.Event
		Payload             json.RawMessage
		RoutingSource       events.RoutingSource
		Schema              events.PayloadSchemaBindingInput
		Recipients          []RunForkContractFrontierRecipient
		FrontierFingerprint string
		Selection           RunForkContractSelection
	}{p.SourceRunID, p.ForkPoint, entityID, meta, event, event.Payload(), event.RoutingSource(), events.PayloadSchemaBindingInput{
		BundleHash: binding.BundleHash(), FlowID: binding.FlowID(), EventKey: binding.EventKey(), SchemaDigest: binding.SchemaDigest(), SchemaClass: binding.SchemaClass()}, canonical, frontierFingerprint, frontier.ContractSelection})
	if err != nil {
		return ConstructedRouteEvidence{}, err
	}
	digest := sha256.Sum256(raw)
	return ConstructedRouteEvidence{proof: RunForkSelectedContractDynamicTopologyProof{
		FlowInstance: meta.FlowInstance, SourceEventIDs: []string{eventID}, EventNames: []string{string(event.Type())},
		DerivedRecipients: canonical, Disposition: RunForkSelectedContractDispositionForkLocalTruth,
		FixedCutEvidenceFingerprint: hex.EncodeToString(digest[:]),
	}, frontierFingerprint: frontierFingerprint, selection: frontier.ContractSelection}, nil
}

func (e ConstructedRouteEvidence) ProofFor(instance string, admission RunForkSelectedContractRouteAdmission) (RunForkSelectedContractDynamicTopologyProof, bool, error) {
	proof := e.proof
	if proof.FlowInstance != instance || proof.FixedCutEvidenceFingerprint == "" {
		return RunForkSelectedContractDynamicTopologyProof{}, false, nil
	}
	if e.frontierFingerprint != admission.FrontierEvidenceFingerprint || e.selection != admission.ContractSelection {
		return RunForkSelectedContractDynamicTopologyProof{}, false, fmt.Errorf("constructed route binding belongs to another frontier/selection")
	}
	if len(proof.SourceEventIDs) != 1 || len(proof.EventNames) != 1 {
		return RunForkSelectedContractDynamicTopologyProof{}, false, fmt.Errorf("invalid constructed route binding")
	}
	for _, event := range admission.SelectedRouteEvents {
		if event.SourceEventID != proof.SourceEventIDs[0] || event.EventName != proof.EventNames[0] || event.Disposition == RunForkSelectedContractDispositionFailClosed {
			continue
		}
		var recipients []RunForkContractFrontierRecipient
		for _, recipient := range event.DerivedRecipients {
			if recipient.Path == instance {
				recipients = append(recipients, recipient)
			}
		}
		left, err := selectedContractModelRecipientKeys(recipients)
		if err != nil {
			return RunForkSelectedContractDynamicTopologyProof{}, false, err
		}
		right, err := selectedContractModelRecipientKeys(proof.DerivedRecipients)
		if err != nil {
			return RunForkSelectedContractDynamicTopologyProof{}, false, err
		}
		if !slices.Equal(left, right) {
			return RunForkSelectedContractDynamicTopologyProof{}, false, fmt.Errorf("constructed route recipients changed after selector admission")
		}
		if len(e.completedRoutes) != 0 {
			routes, err := completedConstructedRouteKeys(instance, event.HistoricalDeliveryRoutes)
			if err != nil || !slices.Equal(routes, e.completedRoutes) {
				return RunForkSelectedContractDynamicTopologyProof{}, false, fmt.Errorf("eager child completed routes changed after selector admission: %v", err)
			}
		}
		proof.SourceEventIDs = slices.Clone(proof.SourceEventIDs)
		proof.EventNames = slices.Clone(proof.EventNames)
		proof.DerivedRecipients = slices.Clone(proof.DerivedRecipients)
		return proof, true, nil
	}
	return RunForkSelectedContractDynamicTopologyProof{}, false, fmt.Errorf("constructed route lost its retained selector event")
}
