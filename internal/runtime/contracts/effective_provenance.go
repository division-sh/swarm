package contracts

import (
	"sort"
	"strconv"
	"strings"
)

type EffectiveValueOrigin string

const (
	EffectiveValueOriginAuthored         EffectiveValueOrigin = "authored"
	EffectiveValueOriginDerived          EffectiveValueOrigin = "derived"
	EffectiveValueOriginBoundarySnapshot EffectiveValueOrigin = "boundary_snapshot"
)

type EffectiveValueProvenance struct {
	Origin         EffectiveValueOrigin
	RuleID         string
	InputPaths     []string
	PackIdentity   string
	SourceFile     string
	SourceLine     int
	SourceColumn   int
	SourcePresence string
}

type EffectiveProvenanceEntry struct {
	Path       string
	Provenance EffectiveValueProvenance
}

// EffectiveProvenanceLedger is the immutable provenance owner for admitted
// effective values. Typed structures carry values; this ledger explains where
// each value came from without wrapping every field in another value type.
type EffectiveProvenanceLedger struct {
	entries map[string]EffectiveValueProvenance
}

func (l EffectiveProvenanceLedger) Lookup(path string) (EffectiveValueProvenance, bool) {
	provenance, ok := l.entries[strings.TrimSpace(path)]
	if !ok {
		return EffectiveValueProvenance{}, false
	}
	return cloneEffectiveValueProvenance(provenance), true
}

func (l EffectiveProvenanceLedger) Entries() []EffectiveProvenanceEntry {
	paths := make([]string, 0, len(l.entries))
	for path := range l.entries {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	out := make([]EffectiveProvenanceEntry, 0, len(paths))
	for _, path := range paths {
		out = append(out, EffectiveProvenanceEntry{Path: path, Provenance: cloneEffectiveValueProvenance(l.entries[path])})
	}
	return out
}

func (b *WorkflowContractBundle) EffectiveProvenance() EffectiveProvenanceLedger {
	if b == nil {
		return EffectiveProvenanceLedger{}
	}
	return cloneEffectiveProvenanceLedger(b.effectiveProvenance)
}

func cloneEffectiveProvenanceLedger(in EffectiveProvenanceLedger) EffectiveProvenanceLedger {
	out := EffectiveProvenanceLedger{entries: make(map[string]EffectiveValueProvenance, len(in.entries))}
	for path, provenance := range in.entries {
		out.entries[path] = cloneEffectiveValueProvenance(provenance)
	}
	return out
}

func cloneEffectiveValueProvenance(in EffectiveValueProvenance) EffectiveValueProvenance {
	out := in
	out.InputPaths = append([]string(nil), in.InputPaths...)
	return out
}

type effectiveProvenanceBuilder struct {
	entries map[string]EffectiveValueProvenance
}

func newEffectiveProvenanceBuilder() *effectiveProvenanceBuilder {
	return &effectiveProvenanceBuilder{entries: map[string]EffectiveValueProvenance{}}
}

func (b *effectiveProvenanceBuilder) set(path string, provenance EffectiveValueProvenance) {
	path = strings.TrimSpace(path)
	if b == nil || path == "" {
		return
	}
	b.entries[path] = cloneEffectiveValueProvenance(provenance)
}

func (b *effectiveProvenanceBuilder) ledger() EffectiveProvenanceLedger {
	if b == nil {
		return EffectiveProvenanceLedger{}
	}
	return cloneEffectiveProvenanceLedger(EffectiveProvenanceLedger{entries: b.entries})
}

func populateEffectiveEventProvenance(bundle *WorkflowContractBundle) {
	if bundle == nil {
		return
	}
	builder := newEffectiveProvenanceBuilder()
	owners := map[string]string{}
	for _, record := range bundle.currentEventDeclarationRecords() {
		prefix := effectiveEventProvenancePrefix(record.packageKey, record.qualifiedName)
		if prefix == "" {
			continue
		}
		if previousLayer, exists := owners[prefix]; exists && previousLayer == "flow" && record.layer != "flow" {
			continue
		}
		owners[prefix] = record.layer
		packIdentity, boundarySnapshot := eventBoundarySnapshotPackIdentity(bundle, record)
		for relativePath, provenance := range record.entry.admissionProvenance {
			provenance.InputPaths = qualifyEffectiveEventInputPaths(prefix, provenance.InputPaths)
			if boundarySnapshot {
				provenance.Origin = EffectiveValueOriginBoundarySnapshot
				provenance.PackIdentity = packIdentity
			}
			builder.set(prefix+"."+relativePath, provenance)
		}
	}
	populateEffectiveEventProjectionProvenance(bundle, builder)
	bundle.effectiveProvenance = builder.ledger()
}

func populateEffectiveEventProjectionProvenance(bundle *WorkflowContractBundle, builder *effectiveProvenanceBuilder) {
	if bundle == nil || builder == nil {
		return
	}
	for _, row := range effectiveEventSchemaOwnershipRows(bundle) {
		ownerEvent := resolvedEventSchemaKey(bundle, row.producerFlowID, row.producerEvent)
		ownerPrefix := effectiveEventProvenancePrefix(row.packageKey, ownerEvent)
		projectionPrefix := effectiveEventProjectionProvenancePrefix(row.packageKey, row.receiverFlowID, row.receiverEvent)
		if ownerPrefix == "" || projectionPrefix == "" {
			continue
		}
		for relativePath := range row.producer.admissionProvenance {
			builder.set(projectionPrefix+"."+relativePath, EffectiveValueProvenance{
				Origin:     EffectiveValueOriginDerived,
				RuleID:     eventConsumerProjectionRule,
				InputPaths: []string{ownerPrefix + "." + relativePath},
			})
		}
		if row.receiverFlowID == "" {
			continue
		}
		for _, pin := range bundle.FlowInputEventPins(row.receiverFlowID) {
			if !bundle.FlowEventMatches(row.receiverFlowID, pin.EventType(), row.receiverEvent) {
				continue
			}
			carryInputPaths := make([]string, 0, len(pin.Carries)+1)
			carryInputPaths = append(carryInputPaths, ownerPrefix+".payload.required")
			for fieldName, carry := range pin.Carries {
				source := strings.TrimSpace(carry.From)
				inputPath := "flows[" + strconv.Quote(row.receiverFlowID) + "].pins.inputs[" + strconv.Quote(pin.PinName()) + "].carries[" + strconv.Quote(strings.TrimSpace(fieldName)) + "]"
				carryInputPaths = append(carryInputPaths, inputPath)
				targetPrefix := projectionPrefix + ".fields." + strings.TrimSpace(fieldName) + "."
				switch {
				case strings.HasPrefix(source, "payload."):
					sourceName := strings.TrimSpace(strings.TrimPrefix(source, "payload."))
					sourcePrefix := "fields." + sourceName + "."
					for relativePath := range row.producer.admissionProvenance {
						if !strings.HasPrefix(relativePath, sourcePrefix) {
							continue
						}
						suffix := strings.TrimPrefix(relativePath, sourcePrefix)
						builder.set(targetPrefix+suffix, EffectiveValueProvenance{
							Origin:     EffectiveValueOriginDerived,
							RuleID:     eventDeliveryCarryRule,
							InputPaths: []string{ownerPrefix + "." + relativePath, inputPath},
						})
					}
				case source == FlowInputCarrySourceGeneratedUUID, source == FlowInputCarrySourceEventID:
					for _, suffix := range []string{"type", "is_optional"} {
						builder.set(targetPrefix+suffix, EffectiveValueProvenance{
							Origin:     EffectiveValueOriginDerived,
							RuleID:     eventDeliveryCarryRule,
							InputPaths: []string{inputPath},
						})
					}
				}
			}
			builder.set(projectionPrefix+".payload.required", EffectiveValueProvenance{
				Origin:     EffectiveValueOriginDerived,
				RuleID:     eventDeliveryCarryRule,
				InputPaths: carryInputPaths,
			})
		}
	}
}

func eventBoundarySnapshotPackIdentity(bundle *WorkflowContractBundle, record currentEventDeclarationRecord) (string, bool) {
	packageKey := strings.TrimSpace(record.packageKey)
	if bundle == nil || packageKey == "" || packageKey == "." || record.layer != "project" {
		return "", false
	}
	view, ok := bundle.projectContracts[packageKey]
	if !ok {
		return "", false
	}
	candidates := map[string]struct{}{
		strings.TrimSpace(record.localName):     {},
		strings.TrimSpace(record.qualifiedName): {},
	}
	for _, name := range view.Manifest.Requires.Inputs {
		if _, ok := candidates[strings.TrimSpace(name)]; ok {
			return packageKey, true
		}
	}
	return "", false
}

func effectiveEventProvenancePrefix(packageKey, eventName string) string {
	packageKey = strings.TrimSpace(packageKey)
	eventName = strings.TrimSpace(eventName)
	if packageKey == "" || eventName == "" {
		return ""
	}
	return "events[" + strconv.Quote(packageKey+":"+eventName) + "]"
}

func effectiveEventProjectionProvenancePrefix(packageKey, receiver, eventName string) string {
	packageKey = strings.TrimSpace(packageKey)
	receiver = strings.TrimSpace(receiver)
	eventName = strings.TrimSpace(eventName)
	if packageKey == "" || receiver == "" || eventName == "" {
		return ""
	}
	return "event_projections[" + strconv.Quote(packageKey+":"+receiver+":"+eventName) + "]"
}

func qualifyEffectiveEventInputPaths(prefix string, paths []string) []string {
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path != "" {
			out = append(out, prefix+"."+path)
		}
	}
	return out
}
