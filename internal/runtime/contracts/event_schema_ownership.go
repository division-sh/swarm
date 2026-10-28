package contracts

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

type eventSchemaOwnershipRow struct {
	connect               FlowConnect
	ownerFlowPath         string
	producerEndpoint      string
	producerFlowID        string
	producerEvent         string
	producerName          string
	producer              EventCatalogEntry
	receiverEndpoint      string
	receiverFlowID        string
	receiverEvent         string
	receiverQualifiedName string
	receiverName          string
	receiver              EventCatalogEntry
	receiverRestatement   bool
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
	return bundle.eventOwnership
}

func eventSchemaOwnershipRowsForReceiver(bundle *WorkflowContractBundle, flowID string) []eventSchemaOwnershipRow {
	if bundle == nil {
		return nil
	}
	flowID = strings.TrimSpace(flowID)
	return bundle.eventOwnersByFlow[flowID]
}

func compileEventSchemaOwnershipRow(bundle *WorkflowContractBundle, connect FlowConnect) (eventSchemaOwnershipRow, bool) {
	producerEvent := connect.Event
	producerFlowID := connectEndpointFlowID(connect.OwnerFlowPath, connect.From)
	producer, producerName, _ := connectEndpointEventDeclaration(bundle, producerFlowID, producerEvent, false)
	if producerName == "" {
		producerName = packageEndpointLocalEvent(bundle, producerFlowID, producerEvent, false)
	}
	receiverEvent := connect.Event
	if strings.TrimSpace(connect.Rename) != "" {
		receiverEvent = connect.Rename
	}
	receiverFlowID := connectEndpointFlowID(connect.OwnerFlowPath, connect.To)
	receiverEvent = packageEndpointLocalEvent(bundle, receiverFlowID, receiverEvent, true)
	receiver, receiverName, receiverOK := connectEndpointEventDeclaration(bundle, receiverFlowID, receiverEvent, true)
	return eventSchemaOwnershipRow{
		connect:               connect,
		ownerFlowPath:         normalizedConnectOwnerFlowPath(connect.OwnerFlowPath),
		producerEndpoint:      strings.TrimSpace(connect.From),
		producerFlowID:        producerFlowID,
		producerEvent:         eventidentity.Normalize(producerEvent),
		producerName:          producerName,
		producer:              producer,
		receiverEndpoint:      strings.TrimSpace(connect.To),
		receiverFlowID:        receiverFlowID,
		receiverEvent:         eventidentity.Normalize(receiverEvent),
		receiverQualifiedName: compiledPinEventName(bundle.FlowPath(receiverFlowID), receiverEvent),
		receiverName:          receiverName,
		receiver:              receiver,
		receiverRestatement:   receiverOK,
	}, true
}

// BindConnectedProviderInputPins completes only schema evidence for the
// already-owned edges; the returned pins retain their authored provenance.
func (b *WorkflowContractBundle) BindConnectedProviderInputPins(outputs map[string][]CompiledFlowOutputPin) (map[string][]CompiledFlowInputPin, error) {
	if b == nil {
		return nil, fmt.Errorf("connected provider input binding requires its compiled bundle")
	}
	if errs := validateCompiledConnectEventSchemaOwnership(b); len(errs) != 0 {
		return nil, errors.Join(errs...)
	}
	bound := make(map[string][]CompiledFlowInputPin, len(b.Semantics.flowInputEventPins))
	for flow, pins := range b.Semantics.flowInputEventPins {
		bound[flow] = cloneCompiledFlowInputPins(pins)
	}
	for _, row := range effectiveEventSchemaOwnershipRows(b) {
		var schema CompiledEventSchema
		for _, output := range outputs[row.producerFlowID] {
			if output.EventType() == packageEndpointLocalEvent(b, row.producerFlowID, row.producerEvent, false) {
				schema, _ = output.EventSchema()
				if output.FlowID() != row.producerFlowID || (schema.Classification() == CompiledEventSchemaImported && schema.FlowPath() != row.producerFlowID) {
					return nil, fmt.Errorf("connected producer %s:%s has foreign output schema evidence", row.producerFlowID, row.producerEvent)
				}
				break
			}
		}
		if schema.Classification() != CompiledEventSchemaImported {
			continue
		}
		for index, pin := range bound[row.receiverFlowID] {
			if pin.EventType() != row.receiverEvent {
				continue
			}
			if existing, present := pin.ProducerEventSchema(); present {
				if existing.FlowPath() != schema.FlowPath() || existing.EventName() != schema.EventName() || existing.AcceptanceSchemaDigest() != schema.AcceptanceSchemaDigest() {
					return nil, fmt.Errorf("connected input %s:%s conflicts with its admitted producer schema", row.receiverFlowID, row.receiverEvent)
				}
				continue
			}
			value := *pin.value
			value.producerEventSchema, value.receiverEventSchema = schema, schema
			var err error
			value.digest, err = compiledFlowPinDigest("input", value.context, value.event, schema, schema)
			if err != nil {
				return nil, err
			}
			bound[row.receiverFlowID][index] = CompiledFlowInputPin{value: &value}
		}
	}
	return bound, nil
}

func validateCompiledConnectEventSchemaOwnership(bundle *WorkflowContractBundle) []error {
	if bundle != nil && len(bundle.Semantics.CompositionConnects) != 0 && bundle.eventOwnersByFlow == nil {
		return []error{fmt.Errorf("%w: missing compiled connect schema ownership; compile the exact source before validation", ErrInvalidField)}
	}
	var errs []error
	rows := effectiveEventSchemaOwnershipRows(bundle)
	ownersByReceiver := make(map[string]map[string]eventSchemaOwnershipRow)
	for _, row := range rows {
		receiverKey := eventSchemaReceiverOwnerKey(row)
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
		producerLocation := eventDeclarationLocation(bundle, row.producerFlowID, row.producerEvent)
		receiverLocation := eventDeclarationLocation(bundle, row.receiverFlowID, row.receiverEvent)
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
			labels = append(labels, fmt.Sprintf("%s event %s at %s", packageEndpointLabel(owner.producerEndpoint), owner.producerName, eventDeclarationLocation(bundle, owner.producerFlowID, owner.producerEvent)))
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

func eventSchemaReceiverOwnerKey(row eventSchemaOwnershipRow) string {
	receiverEvent := eventidentity.Normalize(row.receiverEvent)
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
	localName := packageEndpointLocalEvent(bundle, flowID, eventName, input)
	compiled, ok, err := bundle.ResolveCompiledFlowEventSchema(flowID, localName)
	if err != nil || !ok {
		return EventCatalogEntry{}, "", false
	}
	return cloneEventCatalogEntry(compiled.value.declaration), compiled.EventName(), true
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

func eventDeclarationLocation(bundle *WorkflowContractBundle, flowID, event string) string {
	compiled, ok, err := bundle.ResolveCompiledFlowEventSchema(flowID, event)
	if err != nil || !ok || compiled.Source().File == "" {
		return "unknown source"
	}
	for _, declaration := range bundle.canonicalCurrentEventDeclarationRecords() {
		if declaration.flowPath != compiled.FlowPath() || declaration.qualifiedName != compiled.EventName() || declaration.sourceFile != compiled.Source().File {
			continue
		}
		slot := NodeProvenanceMapPath("", declaration.localName)
		if location, found := bundle.IntroductionLocation(declaration.sourceFile, slot, true); found {
			return fmt.Sprintf("%s:%d", declaration.sourceFile, location.Line)
		}
		break
	}
	return "unknown source"
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
	if _, ok := bundle.exactFlowEventDeclarationView(flowID); !ok {
		return EventCatalogEntry{}, "", TypeCatalogDocument{}, false, false
	}
	_, connected, ambiguous := connectedEventSchemaOwnershipRow(bundle, flowID, eventType)
	if ambiguous {
		return EventCatalogEntry{}, "", TypeCatalogDocument{}, true, false
	}
	if bundle != nil {
		if compiled, ok, err := bundle.ResolveEffectiveCompiledFlowEventSchema(flowID, eventType); err != nil {
			return EventCatalogEntry{}, "", TypeCatalogDocument{}, false, false
		} else if ok {
			return cloneEventCatalogEntry(compiled.value.declaration), compiled.EventName(), bundle.ResolvedTypeCatalogForFlow(compiled.FlowPath()), connected, true
		}
	}
	if connected {
		return EventCatalogEntry{}, "", TypeCatalogDocument{}, true, false
	}
	entry, key, types, ok := eventSchemaDeclarationForFlowEvent(bundle, flowID, eventType)
	return entry, key, types, false, ok
}

func (b *WorkflowContractBundle) flowInputEventPinForResolvedEvent(flowID, eventType string) (CompiledFlowInputPin, bool) {
	requested := eventidentity.Normalize(eventType)
	if local, owned := b.connectionInputs.ReceiverEvent(flowID, requested); owned {
		return b.FlowInputEventPin(flowID, local)
	}
	for _, pin := range b.FlowInputEventPins(flowID) {
		local := eventidentity.Normalize(pin.EventType())
		resolved := pin.QualifiedEventName()
		if requested == local || requested == resolved {
			return pin, true
		}
	}
	return CompiledFlowInputPin{}, false
}

func connectedEventSchemaOwnershipRow(bundle *WorkflowContractBundle, flowID, eventType string) (eventSchemaOwnershipRow, bool, bool) {
	if bundle == nil {
		return eventSchemaOwnershipRow{}, false, false
	}
	var selected eventSchemaOwnershipRow
	found := false
	for _, row := range eventSchemaOwnershipRowsForReceiver(bundle, flowID) {
		localEvent := eventidentity.Normalize(row.receiverEvent)
		resolvedEvent := row.receiverQualifiedName
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
