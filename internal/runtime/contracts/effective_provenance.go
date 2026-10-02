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
	Origin         EffectiveValueOrigin `json:"origin" yaml:"origin"`
	RuleID         string               `json:"rule_id,omitempty" yaml:"rule_id,omitempty"`
	InputPaths     []string             `json:"input_paths,omitempty" yaml:"input_paths,omitempty"`
	PackIdentity   string               `json:"pack_identity,omitempty" yaml:"pack_identity,omitempty"`
	SourceFile     string               `json:"source_file,omitempty" yaml:"source_file,omitempty"`
	SourceLine     int                  `json:"source_line,omitempty" yaml:"source_line,omitempty"`
	SourceColumn   int                  `json:"source_column,omitempty" yaml:"source_column,omitempty"`
	SourcePresence string               `json:"source_presence,omitempty" yaml:"source_presence,omitempty"`
}

type EffectiveProvenanceEntry struct {
	Path       string                   `json:"path" yaml:"path"`
	Provenance EffectiveValueProvenance `json:"provenance" yaml:"provenance"`
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

func populateEffectiveProvenance(bundle *WorkflowContractBundle) error {
	if bundle == nil {
		return nil
	}
	builder := newEffectiveProvenanceBuilder()
	if err := populatePackPlatformProvenance(bundle, builder); err != nil {
		return err
	}
	populateEffectiveSchemaProvenance(bundle, builder)
	for _, record := range bundle.ScopedNodeRecords() {
		prefix := effectiveNodeProvenancePrefix(record.Source.FlowPath, record.LogicalID)
		for relativePath, provenance := range record.Entry.admissionProvenance {
			builder.set(prefix+"."+relativePath, provenance)
		}
	}
	owners := map[string]string{}
	for _, record := range bundle.canonicalCurrentEventDeclarationRecords() {
		prefix := effectiveEventProvenancePrefix(record.flowPath, record.qualifiedName)
		if prefix == "" {
			continue
		}
		if previousLayer, exists := owners[prefix]; exists && previousLayer == "flow" && record.layer != "flow" {
			continue
		}
		owners[prefix] = record.layer
		for relativePath, provenance := range record.entry.admissionProvenance {
			provenance.InputPaths = qualifyEffectiveEventInputPaths(prefix, provenance.InputPaths)
			builder.set(prefix+"."+relativePath, provenance)
		}
	}
	populateEffectiveEventProjectionProvenance(bundle, builder)
	for key, entry := range bundle.scopedTools {
		prefix := "tools[" + strconv.Quote(key) + "]"
		for path, provenance := range entry.admissionProvenance {
			builder.set(prefix+"."+path, provenance)
		}
	}
	for _, record := range bundle.AgentDeclarationRecords() {
		prefix := "agents[" + strconv.Quote(record.Source.FlowPath+":"+record.LogicalID) + "]"
		for path, provenance := range record.Entry.admissionProvenance {
			builder.set(prefix+"."+path, provenance)
		}
		for field, source := range record.Entry.EffectiveFieldSources {
			if source == AgentFieldSourcePlatformDefault {
				builder.set(prefix+"."+field, EffectiveValueProvenance{Origin: EffectiveValueOriginDerived, RuleID: "agent.platform_default." + field})
			}
		}
	}
	if err := populatePolicyRulesProvenance(bundle, builder); err != nil {
		return err
	}
	bundle.effectiveProvenance = builder.ledger()
	return nil
}

func effectiveNodeProvenancePrefix(flowPath, nodeID string) string {
	return "nodes[" + strconv.Quote(strings.TrimSpace(flowPath)+":"+strings.TrimSpace(nodeID)) + "]"
}

func populateEffectiveEventProjectionProvenance(bundle *WorkflowContractBundle, builder *effectiveProvenanceBuilder) {
	if bundle == nil || builder == nil {
		return
	}
	projectionInputs := map[string][]string{}
	for _, row := range effectiveEventSchemaOwnershipRows(bundle) {
		ownerEvent := resolvedEventSchemaKey(bundle, row.producerFlowID, row.producerEvent)
		ownerPrefix := effectiveEventProvenancePrefix(row.ownerFlowPath, ownerEvent)
		projectionPrefix := effectiveEventProjectionProvenancePrefix(row.ownerFlowPath, row.receiverFlowID, row.receiverEvent)
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
		if input, ok, err := bundle.ConnectionInput(row.connect); err == nil && ok {
			evidence := input.SourceEvidence()
			if evidence.Source.RequiresDeliveryProjection() {
				path := projectionPrefix + ".fields." + evidence.Field.Path() + ".type"
				projectionInputs[path] = append(projectionInputs[path], effectiveConnectionSourcePath(bundle, row.connect))
			}
		}
	}
	for path, inputs := range projectionInputs {
		sort.Strings(inputs)
		builder.set(path, EffectiveValueProvenance{
			Origin: EffectiveValueOriginDerived, RuleID: eventReceiverProjectionRule, InputPaths: inputs,
		})
	}
}

func effectiveConnectionSourcePath(bundle *WorkflowContractBundle, connect FlowConnect) string {
	schema, ok := bundle.FlowSchemaByID(connect.OwnerFlowPath)
	if ok {
		for index, authored := range schema.Connect {
			if authored.WithOwnerSource(connect.OwnerFlowPath, connect.SourceFile) == connect {
				return "schemas[" + strconv.Quote(connect.OwnerFlowPath) + "].connect[" + strconv.Itoa(index) + "].key_from"
			}
		}
	}
	return connect.AuthoredLocation()
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
