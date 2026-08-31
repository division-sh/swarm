package contracts

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

const (
	eventConsumerProjectionRule = "compiled_connect_event_consumer_projection/v1"
	eventReceiverProjectionRule = "compiled_receiver_delivery_projection/v1"
)

type eventSchemaOwnershipRow struct {
	ownerFlowPath       string
	producerEndpoint    string
	producerFlowID      string
	producerEvent       string
	producerName        string
	producer            EventCatalogEntry
	receiverEndpoint    string
	receiverFlowID      string
	receiverEvent       string
	receiverName        string
	receiver            EventCatalogEntry
	receiverRestatement bool
}

// compileEventSchemaOwnershipRows is the sole non-behavioral reader of
// authored connect rows. It derives schema ownership only; routing behavior
// remains confined to the pin-routing compiler.
func compileEventSchemaOwnershipRows(bundle *WorkflowContractBundle) []eventSchemaOwnershipRow {
	if bundle == nil {
		return nil
	}
	rows := make([]eventSchemaOwnershipRow, 0, len(bundle.Semantics.CompositionConnects))
	for _, connect := range bundle.Semantics.CompositionConnects {
		if row, ok := compileEventSchemaOwnershipRow(bundle, connect); ok {
			rows = append(rows, row)
		}
	}
	return rows
}

func populateEventSchemaOwnershipIndex(bundle *WorkflowContractBundle) {
	if bundle == nil {
		return
	}
	rows := compileEventSchemaOwnershipRows(bundle)
	byReceiver := make(map[string][]eventSchemaOwnershipRow)
	for _, row := range rows {
		byReceiver[row.receiverFlowID] = append(byReceiver[row.receiverFlowID], row)
	}
	bundle.eventOwnership = rows
	bundle.eventOwnersByFlow = byReceiver
}

func effectiveEventSchemaOwnershipRows(bundle *WorkflowContractBundle) []eventSchemaOwnershipRow {
	if bundle == nil {
		return nil
	}
	if bundle.eventOwnersByFlow != nil {
		return bundle.eventOwnership
	}
	return compileEventSchemaOwnershipRows(bundle)
}

func eventSchemaOwnershipRowsForReceiver(bundle *WorkflowContractBundle, flowID string) []eventSchemaOwnershipRow {
	if bundle == nil {
		return nil
	}
	flowID = strings.TrimSpace(flowID)
	if bundle.eventOwnersByFlow != nil {
		return bundle.eventOwnersByFlow[flowID]
	}
	var rows []eventSchemaOwnershipRow
	for _, row := range compileEventSchemaOwnershipRows(bundle) {
		if row.receiverFlowID == flowID {
			rows = append(rows, row)
		}
	}
	return rows
}

func compileEventSchemaOwnershipRow(bundle *WorkflowContractBundle, connect FlowConnect) (eventSchemaOwnershipRow, bool) {
	producerEvent := connect.Event
	producerFlowID := connectEndpointFlowID(connect.OwnerFlowPath, connect.From)
	producer, producerName, producerOK := connectEndpointEventDeclaration(bundle, producerFlowID, producerEvent, false)
	if !producerOK {
		return eventSchemaOwnershipRow{}, false
	}
	receiverEvent := connect.Event
	if strings.TrimSpace(connect.Rename) != "" {
		receiverEvent = connect.Rename
	}
	receiverFlowID := connectEndpointFlowID(connect.OwnerFlowPath, connect.To)
	receiver, receiverName, receiverOK := connectEndpointEventDeclaration(bundle, receiverFlowID, receiverEvent, true)
	return eventSchemaOwnershipRow{
		ownerFlowPath:       normalizedConnectOwnerFlowPath(connect.OwnerFlowPath),
		producerEndpoint:    strings.TrimSpace(connect.From),
		producerFlowID:      producerFlowID,
		producerEvent:       eventidentity.Normalize(producerEvent),
		producerName:        producerName,
		producer:            producer,
		receiverEndpoint:    strings.TrimSpace(connect.To),
		receiverFlowID:      receiverFlowID,
		receiverEvent:       eventidentity.Normalize(receiverEvent),
		receiverName:        receiverName,
		receiver:            receiver,
		receiverRestatement: receiverOK,
	}, true
}

func validateCompiledConnectEventSchemaOwnership(bundle *WorkflowContractBundle) []error {
	var errs []error
	rows := effectiveEventSchemaOwnershipRows(bundle)
	ownersByReceiver := make(map[string]map[string]eventSchemaOwnershipRow)
	for _, row := range rows {
		receiverKey := eventSchemaReceiverOwnerKey(bundle, row)
		producerKey := eventSchemaProducerOwnerKey(row)
		if receiverKey != "" && producerKey != "" {
			if ownersByReceiver[receiverKey] == nil {
				ownersByReceiver[receiverKey] = map[string]eventSchemaOwnershipRow{}
			}
			ownersByReceiver[receiverKey][producerKey] = row
		}
		if !row.receiverRestatement {
			continue
		}
		producerLocation := eventDeclarationLocation(row.producer)
		receiverLocation := eventDeclarationLocation(row.receiver)
		errs = append(errs, fmt.Errorf(
			"%w: event %s consumer %s under schema owner %s restates producer-owned schema %s at %s with %s at %s; remove the consumer declaration so its compiled projection derives from the producer",
			ErrInvalidField,
			row.producerEvent,
			packageEndpointLabel(row.receiverEndpoint),
			row.ownerFlowPath,
			row.producerName,
			producerLocation,
			row.receiverName,
			receiverLocation,
		))
	}
	receiverKeys := make([]string, 0, len(ownersByReceiver))
	for receiverKey := range ownersByReceiver {
		receiverKeys = append(receiverKeys, receiverKey)
	}
	sort.Strings(receiverKeys)
	for _, receiverKey := range receiverKeys {
		owners := ownersByReceiver[receiverKey]
		if len(owners) <= 1 {
			continue
		}
		labels := make([]string, 0, len(owners))
		var receiver eventSchemaOwnershipRow
		for _, owner := range owners {
			receiver = owner
			labels = append(labels, fmt.Sprintf("%s event %s at %s", packageEndpointLabel(owner.producerEndpoint), owner.producerName, eventDeclarationLocation(owner.producer)))
		}
		sort.Strings(labels)
		errs = append(errs, fmt.Errorf(
			"%w: receiver %s event %s under schema owner %s has multiple connected producer schema owners: %s; one effective receiver event must have exactly one producer-owned schema",
			ErrMultipleAuthoritativeOwners,
			packageEndpointLabel(receiver.receiverEndpoint),
			receiver.receiverEvent,
			receiver.ownerFlowPath,
			strings.Join(labels, ", "),
		))
	}
	return errs
}

func eventSchemaReceiverOwnerKey(bundle *WorkflowContractBundle, row eventSchemaOwnershipRow) string {
	receiverEvent := eventidentity.Normalize(row.receiverEvent)
	if bundle != nil && row.receiverFlowID != "" {
		local := packageEndpointLocalEvent(bundle, row.receiverFlowID, receiverEvent, true)
		receiverEvent = eventidentity.ExternalizeForFlow(bundle.FlowPath(row.receiverFlowID), bundle.FlowInputEvents(row.receiverFlowID), local)
	}
	if receiverEvent == "" {
		return ""
	}
	return strings.Join([]string{strings.TrimSpace(row.ownerFlowPath), strings.TrimSpace(row.receiverFlowID), receiverEvent}, "\x00")
}

func eventSchemaProducerOwnerKey(row eventSchemaOwnershipRow) string {
	producerName := eventidentity.Normalize(row.producerName)
	if producerName == "" {
		return ""
	}
	return strings.Join([]string{strings.TrimSpace(row.ownerFlowPath), strings.TrimSpace(row.producerFlowID), producerName}, "\x00")
}

func sameEventSchemaProducerOwner(left, right eventSchemaOwnershipRow) bool {
	leftKey := eventSchemaProducerOwnerKey(left)
	return leftKey != "" && leftKey == eventSchemaProducerOwnerKey(right)
}

func connectEndpointEventDeclaration(bundle *WorkflowContractBundle, flowID, eventName string, input bool) (EventCatalogEntry, string, bool) {
	flowID = strings.TrimSpace(flowID)
	eventName = eventidentity.Normalize(eventName)
	if bundle == nil || eventName == "" {
		return EventCatalogEntry{}, "", false
	}
	viewID := flowID
	if viewID == "" {
		viewID = "."
	}
	view, ok := bundle.FlowTree.ByID[viewID]
	if !ok || view == nil {
		return EventCatalogEntry{}, "", false
	}
	localName := packageEndpointLocalEvent(bundle, flowID, eventName, input)
	return eventDeclarationByCandidates(view.Events, localName, eventName, eventidentity.LeafName(eventName))
}

func normalizedConnectOwnerFlowPath(raw string) string {
	owner := strings.Trim(strings.TrimSpace(raw), "/")
	if owner == "" {
		return "."
	}
	return owner
}

func connectEndpointFlowID(owner, endpoint string) string {
	owner = normalizedConnectOwnerFlowPath(owner)
	endpoint = strings.Trim(strings.TrimSpace(endpoint), "/")
	if endpoint == "." {
		return owner
	}
	if owner == "." {
		return endpoint
	}
	return owner + "/" + endpoint
}

func packageEndpointLocalEvent(bundle *WorkflowContractBundle, endpoint, eventName string, input bool) string {
	endpoint = strings.TrimSpace(endpoint)
	eventName = eventidentity.Normalize(eventName)
	if bundle == nil || endpoint == "." || endpoint == "" {
		return eventName
	}
	scope := eventidentity.Scope{Path: bundle.FlowPath(endpoint)}
	if input {
		scope.InputEvents = bundle.FlowInputEvents(endpoint)
		return scope.LocalizeInput(eventName)
	}
	scope.OutputEvents = bundle.FlowOutputEvents(endpoint)
	return scope.LocalizeOutput(eventName)
}

func eventDeclarationByCandidates(entries map[string]EventCatalogEntry, candidates ...string) (EventCatalogEntry, string, bool) {
	for _, candidate := range candidates {
		candidate = eventidentity.Normalize(candidate)
		if candidate == "" {
			continue
		}
		if entry, ok := entries[candidate]; ok {
			return entry, candidate, true
		}
	}
	return EventCatalogEntry{}, "", false
}

func eventDeclarationLocation(entry EventCatalogEntry) string {
	provenance, ok := entry.admissionProvenance["declaration"]
	if !ok || strings.TrimSpace(provenance.SourceFile) == "" {
		return "unknown source"
	}
	if provenance.SourceLine <= 0 {
		return provenance.SourceFile
	}
	return fmt.Sprintf("%s:%d", provenance.SourceFile, provenance.SourceLine)
}

func packageEndpointLabel(endpoint string) string {
	if strings.TrimSpace(endpoint) == "." {
		return "owner flow"
	}
	return "flow " + strings.TrimSpace(endpoint)
}

func effectiveEventDeclarationForFlowEvent(bundle *WorkflowContractBundle, flowID, eventType string) (EventCatalogEntry, string, TypeCatalogDocument, bool) {
	entry, key, types, _, ok := resolveEffectiveEventDeclarationForFlowEvent(bundle, flowID, eventType)
	return entry, key, types, ok
}

func resolveEffectiveEventDeclarationForFlowEvent(bundle *WorkflowContractBundle, flowID, eventType string) (EventCatalogEntry, string, TypeCatalogDocument, bool, bool) {
	flowID = strings.TrimSpace(flowID)
	var entry EventCatalogEntry
	var key string
	var types TypeCatalogDocument
	connected := false
	var ok bool
	if row, found, ambiguous := connectedEventSchemaOwnershipRow(bundle, flowID, eventType); ambiguous {
		return EventCatalogEntry{}, "", TypeCatalogDocument{}, true, false
	} else if found {
		entry = row.producer
		key = row.producerName
		types = bundle.RootTypeCatalog()
		if row.producerFlowID != "" {
			types = bundle.ResolvedTypeCatalogForFlow(row.producerFlowID)
		}
		connected = true
		ok = true
	}
	if !ok {
		entry, key, types, ok = eventSchemaDeclarationForFlowEvent(bundle, flowID, eventType)
	}
	if ok && bundle != nil {
		if pin, found := bundle.flowInputEventPinForResolvedEvent(flowID, eventType); found {
			if projection, projected := pin.Projection(); projected {
				entry = cloneEventCatalogEntry(entry)
				if entry.Payload.Properties == nil {
					entry.Payload.Properties = map[string]EventFieldSpec{}
				}
				entry.Payload.Properties[projection.Field] = EventFieldSpec{Type: projection.SourceType}
				entry.Payload.Required = normalizeStrings(append(entry.Payload.Required, projection.Field))
			}
		}
	}
	return entry, key, types, connected, ok
}

func (b *WorkflowContractBundle) flowInputEventPinForResolvedEvent(flowID, eventType string) (CompiledFlowInputPin, bool) {
	requested := eventidentity.Normalize(eventType)
	for _, pin := range b.FlowInputEventPins(flowID) {
		local := eventidentity.Normalize(pin.EventType())
		resolved := eventidentity.Normalize(b.ResolveFlowEventReference(flowID, local))
		if requested == local || requested == resolved {
			return pin, true
		}
	}
	return CompiledFlowInputPin{}, false
}

func (b *WorkflowContractBundle) flowInputEventMatches(flowID, subscription, eventType string) bool {
	if b == nil {
		return false
	}
	scope := eventidentity.Scope{
		Path:        b.FlowPath(flowID),
		InputEvents: b.FlowInputEvents(flowID),
	}
	return scope.Matches(subscription, eventType, nil)
}

func connectedEventSchemaOwnershipRow(bundle *WorkflowContractBundle, flowID, eventType string) (eventSchemaOwnershipRow, bool, bool) {
	if bundle == nil {
		return eventSchemaOwnershipRow{}, false, false
	}
	flowID = strings.TrimSpace(flowID)
	var selected eventSchemaOwnershipRow
	found := false
	for _, row := range eventSchemaOwnershipRowsForReceiver(bundle, flowID) {
		localEvent := eventidentity.Normalize(row.receiverEvent)
		resolvedEvent := eventidentity.Normalize(bundle.ResolveFlowEventReference(flowID, localEvent))
		requestedEvent := eventidentity.Normalize(eventType)
		if localEvent != requestedEvent && resolvedEvent != requestedEvent {
			continue
		}
		if !found {
			selected = row
			found = true
			continue
		}
		if !sameEventSchemaProducerOwner(selected, row) {
			return eventSchemaOwnershipRow{}, false, true
		}
	}
	return selected, found, false
}

// ResolveFlowEventCatalogEntry returns the producer-owned event declaration
// projected into the named flow's delivery context.
func (b *WorkflowContractBundle) ResolveFlowEventCatalogEntry(flowID, eventType string) (EventCatalogEntry, string, bool) {
	entry, key, _, connected, ok := resolveEffectiveEventDeclarationForFlowEvent(b, flowID, eventType)
	if ok && !connected {
		if _, authoredKey, found := b.resolveAuthoredFlowEventCatalogEntry(flowID, eventType); found {
			key = authoredKey
		}
	}
	return entry, key, ok
}

// EffectiveEventCatalogEntryForFlowEvent is the contracts-facing name for the
// same canonical resolver exposed through semanticview.Source.
func (b *WorkflowContractBundle) EffectiveEventCatalogEntryForFlowEvent(flowID, eventType string) (EventCatalogEntry, string, bool) {
	return b.ResolveFlowEventCatalogEntry(flowID, eventType)
}

// ResolveExecutableNodeEventCatalogEntry resolves executable-node spelling,
// then delegates schema semantics to the same effective flow resolver.
func (b *WorkflowContractBundle) ResolveExecutableNodeEventCatalogEntry(ref runtimeidentity.ExecutableNode, eventType string) (EventCatalogEntry, string, bool) {
	entry, key, _, ok := b.resolveEffectiveExecutableNodeEventDeclaration(ref, eventType)
	return entry, key, ok
}

func (b *WorkflowContractBundle) resolveEffectiveExecutableNodeEventDeclaration(ref runtimeidentity.ExecutableNode, eventType string) (EventCatalogEntry, string, TypeCatalogDocument, bool) {
	if b == nil || !ref.Valid() {
		return EventCatalogEntry{}, "", TypeCatalogDocument{}, false
	}
	canonical := b.ResolveExecutableNodeEventReference(ref, eventType)
	if strings.TrimSpace(ref.FlowPath()) != "" {
		if entry, key, types, _, ok := resolveEffectiveEventDeclarationForFlowEvent(b, ref.FlowPath(), canonical); ok {
			return entry, key, types, true
		}
	}
	entry, key, ok := b.resolveAuthoredExecutableNodeEventCatalogEntry(ref, canonical)
	return entry, key, b.RootTypeCatalog(), ok
}
