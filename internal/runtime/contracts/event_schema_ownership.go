package contracts

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

const (
	eventConsumerProjectionRule = "intra_package_event_consumer_projection/v1"
	eventDeliveryCarryRule      = "receiver_delivery_carry_projection/v1"
)

type eventSchemaOwnershipRow struct {
	packageKey          string
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

func compileEventSchemaOwnershipRow(bundle *WorkflowContractBundle, connect FlowPackageConnect) (eventSchemaOwnershipRow, bool) {
	producer, producerName, producerOK := packageEndpointEventDeclaration(bundle, connect.PackageKey, connect.From, connect.Event, false)
	if !producerOK {
		return eventSchemaOwnershipRow{}, false
	}
	receiverEvent := connect.Event
	if strings.TrimSpace(connect.Rename) != "" {
		receiverEvent = connect.Rename
	}
	receiver, receiverName, receiverOK := packageEndpointEventDeclaration(bundle, connect.PackageKey, connect.To, receiverEvent, true)
	receiverFlowID := packageEndpointOwningFlowID(bundle, connect.PackageKey, connect.To)
	return eventSchemaOwnershipRow{
		packageKey:          strings.TrimSpace(connect.PackageKey),
		producerEndpoint:    strings.TrimSpace(connect.From),
		producerFlowID:      packageEndpointOwningFlowID(bundle, connect.PackageKey, connect.From),
		producerEvent:       eventidentity.Normalize(connect.Event),
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

func validateIntraPackageEventSchemaOwnership(bundle *WorkflowContractBundle) []error {
	var errs []error
	for _, row := range effectiveEventSchemaOwnershipRows(bundle) {
		if !row.receiverRestatement {
			continue
		}
		producerLocation := eventDeclarationLocation(row.producer)
		receiverLocation := eventDeclarationLocation(row.receiver)
		errs = append(errs, fmt.Errorf(
			"%w: event %s consumer %s in package %s restates producer-owned schema %s at %s with %s at %s; remove the consumer declaration so its compiled projection derives from the producer",
			ErrInvalidField,
			row.producerEvent,
			packageEndpointLabel(row.receiverEndpoint),
			row.packageKey,
			row.producerName,
			producerLocation,
			row.receiverName,
			receiverLocation,
		))
	}
	return errs
}

func packageEndpointOwningFlowID(bundle *WorkflowContractBundle, packageKey, endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint != "." {
		return endpoint
	}
	if bundle == nil {
		return ""
	}
	view, ok := bundle.projectContracts[strings.TrimSpace(packageKey)]
	if !ok {
		return ""
	}
	return strings.TrimSpace(view.Paths.OwningFlowID)
}

func packageEndpointEventDeclaration(bundle *WorkflowContractBundle, packageKey, endpoint, eventName string, input bool) (EventCatalogEntry, string, bool) {
	packageKey = strings.TrimSpace(packageKey)
	endpoint = strings.TrimSpace(endpoint)
	eventName = eventidentity.Normalize(eventName)
	if bundle == nil || packageKey == "" || endpoint == "" || eventName == "" {
		return EventCatalogEntry{}, "", false
	}
	if endpoint == "." {
		view, ok := bundle.projectContracts[packageKey]
		if !ok {
			return EventCatalogEntry{}, "", false
		}
		return eventDeclarationByCandidates(view.Events, eventName, eventidentity.LeafName(eventName))
	}
	view, ok := bundle.FlowTree.ByID[endpoint]
	if !ok || view == nil || strings.TrimSpace(view.Paths.PackageKey) != packageKey {
		return EventCatalogEntry{}, "", false
	}
	localName := packageEndpointLocalEvent(bundle, endpoint, eventName, input)
	return eventDeclarationByCandidates(view.Events, localName, eventName, eventidentity.LeafName(eventName))
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
		return "package root"
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
	if row, found := connectedEventSchemaOwnershipRow(bundle, flowID, eventType); found {
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
	if !ok || bundle == nil || strings.TrimSpace(flowID) == "" {
		return entry, key, types, connected, ok
	}
	for _, pin := range bundle.FlowInputEventPins(flowID) {
		if !bundle.flowInputEventMatches(flowID, pin.EventType(), eventType) {
			continue
		}
		return projectReceiverDeliveryCarries(entry, pin), key, types, connected, true
	}
	return entry, key, types, connected, true
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

func connectedEventSchemaOwnershipRow(bundle *WorkflowContractBundle, flowID, eventType string) (eventSchemaOwnershipRow, bool) {
	if bundle == nil {
		return eventSchemaOwnershipRow{}, false
	}
	flowID = strings.TrimSpace(flowID)
	for _, row := range eventSchemaOwnershipRowsForReceiver(bundle, flowID) {
		if bundle.flowInputEventMatches(flowID, row.receiverEvent, eventType) {
			return row, true
		}
	}
	return eventSchemaOwnershipRow{}, false
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
	if strings.TrimSpace(ref.FlowID()) != "" {
		if entry, key, types, _, ok := resolveEffectiveEventDeclarationForFlowEvent(b, ref.FlowID(), canonical); ok {
			return entry, key, types, true
		}
	}
	entry, key, ok := b.resolveAuthoredExecutableNodeEventCatalogEntry(ref, canonical)
	return entry, key, b.RootTypeCatalog(), ok
}

func projectReceiverDeliveryCarries(owner EventCatalogEntry, pin FlowInputEventPin) EventCatalogEntry {
	projected := cloneEventCatalogEntry(owner)
	if projected.Payload.Properties == nil {
		projected.Payload.Properties = map[string]EventFieldSpec{}
	}
	carryNames := make([]string, 0, len(pin.Carries))
	for name := range pin.Carries {
		carryNames = append(carryNames, strings.TrimSpace(name))
	}
	sort.Strings(carryNames)
	for _, name := range carryNames {
		if name == "" {
			continue
		}
		carry := pin.Carries[name]
		source := strings.TrimSpace(carry.From)
		field, exists := projected.Payload.Properties[name]
		required := slices.Contains(projected.Payload.Required, name)
		switch {
		case strings.HasPrefix(source, "payload."):
			sourceName := strings.TrimPrefix(source, "payload.")
			if sourceField, ok := owner.Payload.Properties[sourceName]; ok {
				field = sourceField
				exists = true
				required = slices.Contains(owner.Payload.Required, sourceName)
			}
		case source == FlowInputCarrySourceGeneratedUUID, source == FlowInputCarrySourceEventID:
			field = EventFieldSpec{Type: "uuid"}
			exists = true
			required = !carry.Optional
		}
		if !exists {
			continue
		}
		projected.Payload.Properties[name] = field
		if !required {
			projected.Payload.Required = slices.DeleteFunc(projected.Payload.Required, func(required string) bool {
				return strings.TrimSpace(required) == name
			})
		} else if !slices.Contains(projected.Payload.Required, name) {
			projected.Payload.Required = append(projected.Payload.Required, name)
		}
	}
	projected.Payload.Required = normalizeStrings(projected.Payload.Required)
	return projected
}
