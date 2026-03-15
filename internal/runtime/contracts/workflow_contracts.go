package contracts

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"empireai/internal/runtime/core/paths"
	flowmodel "empireai/internal/runtime/flowmodel"

	"gopkg.in/yaml.v3"
)

type ContractPaths struct {
	ContractsRoot         string
	WorkflowDir           string
	RootSchemaFile        string
	ProjectPackageFile    string
	ProjectPackages       []ProjectPackagePaths
	ProjectNodesFile      string
	ProjectEventsFile     string
	ProjectAgentsFile     string
	ProjectToolsFile      string
	ProjectPolicyFile     string
	ProjectPromptsDir     string
	PlatformSpecFile      string
	VerificationGatesFile string
	ToolingLockFile       string
	DDLFile               string
	AgentConfigMapFile    string
	Flows                 []FlowContractPaths
}

type WorkflowContractBundle struct {
	Paths                 ContractPaths
	Package               ProjectPackageDocument
	PackageTree           []LoadedProjectPackage
	projectContracts      map[string]ProjectContractView
	scopedNodes           map[string]SystemNodeContract
	scopedEvents          map[string]EventCatalogEntry
	scopedAgents          map[string]AgentRegistryEntry
	scopedTools           map[string]ToolSchemaEntry
	scopedNodeSources     map[string]ContractItemSource
	scopedEventSources    map[string]ContractItemSource
	scopedAgentSources    map[string]ContractItemSource
	scopedToolSources     map[string]ContractItemSource
	nodeSources           map[string]ContractItemSource
	eventSources          map[string]ContractItemSource
	agentSources          map[string]ContractItemSource
	toolSources           map[string]ContractItemSource
	ambiguousNodeAliases  map[string]struct{}
	ambiguousEventAliases map[string]struct{}
	ambiguousAgentAliases map[string]struct{}
	ambiguousToolAliases  map[string]struct{}
	Semantics             WorkflowSemanticView
	Nodes                 map[string]SystemNodeContract
	Events                map[string]EventCatalogEntry
	Agents                map[string]AgentRegistryEntry
	Tools                 map[string]ToolSchemaEntry
	Policy                PolicyDocument
	Platform              PlatformSpecDocument
	RootSchema            *FlowSchemaDocument
	FlowSchemas           map[string]FlowSchemaDocument
	FlowTree              FlowTree
	URIRegistry           ContractURIRegistry
}

type WorkflowSemanticView struct {
	Name                   string
	Version                string
	InitialStage           string
	EntitySchema           EntitySchema
	Stages                 []WorkflowStageContract
	TerminalStages         []string
	Transitions            []WorkflowTransitionContract
	Timers                 []WorkflowTimerContract
	Guards                 []GuardActionEntry
	Actions                []GuardActionEntry
	GuardByID              map[string]GuardActionEntry
	ActionByID             map[string]GuardActionEntry
	FlowInitial            map[string]string
	FlowStates             map[string][]string
	FlowTerminal           map[string][]string
	FlowNamespace          map[string]string
	FlowPrefix             map[string]string
	FlowRules              map[string]string
	FlowInputs             map[string][]string
	FlowOutputs            map[string][]string
	FlowReads              map[string][]string
	FlowWrites             map[string][]string
	FlowAgents             map[string][]FlowRequiredAgent
	WritePinOwners         map[string][]string
	NodeHandlers           map[string]map[string]SystemNodeEventHandler
	EventOwners            map[string][]string
	HandlerTransitions     []HandlerTransitionSemantic
	HandlerTransitionIndex map[string]map[string]HandlerTransitionSemantic
}

type HandlerTransitionSemantic struct {
	ID               string
	NodeID           string
	FlowID           string
	EventType        string
	Action           ActionSpec
	Guard            *GuardSpec
	AdvancesTo       string
	SetsGate         *GateSpec
	ClearGates       []string
	DataAccumulation WorkflowDataAccumulation
	Emits            EventEmission
	Condition        string
	CompletionRule   string
	OnComplete       []HandlerRuleEntry
	Rules            []HandlerRuleEntry
	Accumulate       *AccumulateSpec
	Compute          *ComputeSpec
	Query            *QuerySpec
	FanOut           *FanOutSpec
	Filter           *FilterSpec
	Reduce           *ReduceSpec
	Count            *CountSpec
	Clear            *ClearSpec
	PayloadTransform *PayloadTransformSpec
	Branch           []BranchSpec
}

type HandlerRuleEntry struct {
	ID               string                   `yaml:"id"`
	Description      string                   `yaml:"description"`
	Condition        string                   `yaml:"condition"`
	AdvancesTo       string                   `yaml:"advances_to"`
	Emits            EventEmission            `yaml:"emits"`
	DataAccumulation WorkflowDataAccumulation `yaml:"data_accumulation"`
}

func (r *HandlerRuleEntry) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		r.Description = strings.TrimSpace(node.Value)
		return nil
	}
	type alias HandlerRuleEntry
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*r = HandlerRuleEntry(aux)
	return nil
}

type GuardSpec struct {
	ID        string       `yaml:"id"`
	Check     string       `yaml:"check"`
	OnFail    string       `yaml:"on_fail"`
	Checks    []GuardCheck `yaml:"checks"`
	PolicyRef string       `yaml:"policy_ref"`
}

type GuardCheck struct {
	ID    string `yaml:"id"`
	Check string `yaml:"check"`
}

func (g *GuardSpec) UnmarshalYAML(node *yaml.Node) error {
	if g == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*g = GuardSpec{}
			return nil
		}
		*g = GuardSpec{ID: strings.TrimSpace(node.Value)}
		return nil
	case yaml.MappingNode:
		type alias GuardSpec
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*g = GuardSpec(aux)
		return nil
	default:
		return fmt.Errorf("unsupported guard yaml node kind %d", node.Kind)
	}
}

type AccumulateSpec struct {
	ExpectedFrom string               `yaml:"expected_from"`
	ExpectedPath paths.Path           `yaml:"-"`
	DedupBy      string               `yaml:"dedup_by"`
	DedupPath    paths.Path           `yaml:"-"`
	Completion   AccumulateCompletion `yaml:"completion"`
	OnComplete   []HandlerRuleEntry   `yaml:"on_complete"`
	OnTimeout    *HandlerRuleEntry    `yaml:"on_timeout"`
}

func (a *AccumulateSpec) UnmarshalYAML(node *yaml.Node) error {
	if a == nil {
		return nil
	}
	var aux struct {
		ExpectedFrom string    `yaml:"expected_from"`
		DedupBy      string    `yaml:"dedup_by"`
		Completion   string    `yaml:"completion"`
		OnComplete   yaml.Node `yaml:"on_complete"`
		OnTimeout    yaml.Node `yaml:"on_timeout"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*a = AccumulateSpec{
		ExpectedFrom: strings.TrimSpace(aux.ExpectedFrom),
		ExpectedPath: paths.Parse(aux.ExpectedFrom),
		DedupBy:      strings.TrimSpace(aux.DedupBy),
		DedupPath:    paths.Parse(aux.DedupBy),
		Completion:   ParseAccumulateCompletion(aux.Completion),
	}
	var err error
	if a.OnComplete, err = decodeHandlerRuleEntriesNode(&aux.OnComplete); err != nil {
		return err
	}
	if a.OnTimeout, err = decodeHandlerRuleEntryNode(&aux.OnTimeout); err != nil {
		return err
	}
	return nil
}

type ComputeSpec struct {
	Operation ComputeOperation `yaml:"operation"`
	Tiers     []ComputeTier    `yaml:"tiers"`
	Keys      ComputeKeyConfig `yaml:"keys"`
	StoreAs   string           `yaml:"store_as"`
}

type ComputeTier struct {
	Dimensions []string `yaml:"dimensions"`
	Weight     float64  `yaml:"weight"`
}

type ComputeKeyConfig struct {
	DimensionKey string   `yaml:"dimension_key"`
	ScoreKeys    []string `yaml:"score_keys"`
	NumericKeys  []string `yaml:"numeric_keys"`
}

type FanOutSpec struct {
	ItemsFrom   string            `yaml:"items_from"`
	ItemsPath   paths.Path        `yaml:"-"`
	Target      string            `yaml:"target"`
	EmitPerItem string            `yaml:"emit_per_item"`
	EmitMapping map[string]string `yaml:"emit_mapping"`
}

func (f *FanOutSpec) UnmarshalYAML(node *yaml.Node) error {
	if f == nil {
		return nil
	}
	type alias FanOutSpec
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*f = FanOutSpec(aux)
	f.ItemsFrom = strings.TrimSpace(f.ItemsFrom)
	f.ItemsPath = paths.Parse(f.ItemsFrom)
	f.Target = strings.TrimSpace(f.Target)
	f.EmitPerItem = strings.TrimSpace(f.EmitPerItem)
	return nil
}

type FilterSpec struct {
	Predicate  string     `yaml:"predicate"`
	Source     string     `yaml:"source"`
	SourcePath paths.Path `yaml:"-"`
	ItemsFrom  string     `yaml:"items_from"`
	ItemsPath  paths.Path `yaml:"-"`
	Condition  string     `yaml:"condition"`
	StoreAs    string     `yaml:"store_as"`
	StorePath  paths.Path `yaml:"-"`
}

type ReduceSpec struct {
	Operation  string                     `yaml:"operation"`
	Source     string                     `yaml:"source"`
	SourcePath paths.Path                 `yaml:"-"`
	StoreAs    string                     `yaml:"store_as"`
	StorePath  paths.Path                 `yaml:"-"`
	ItemsFrom  string                     `yaml:"items_from"`
	ItemsPath  paths.Path                 `yaml:"-"`
	Params     map[string]ExpressionValue `yaml:"params"`
}

type CountSpec struct {
	Source     string     `yaml:"source"`
	SourcePath paths.Path `yaml:"-"`
	StoreAs    string     `yaml:"store_as"`
	StorePath  paths.Path `yaml:"-"`
	ItemsFrom  string     `yaml:"items_from"`
	ItemsPath  paths.Path `yaml:"-"`
	Condition  string     `yaml:"condition"`
}

type ClearSpec struct {
	Target  string   `yaml:"target"`
	Targets []string `yaml:"targets"`
}

type PayloadTransformSpec struct {
	Mappings map[string]string `yaml:"mappings"`
	Fields   map[string]string `yaml:"fields"`
	Entries  []TransformSpec   `yaml:"-"`
}

type TransformSpec struct {
	Target     string
	TargetPath paths.Path
	Source     string
	SourcePath paths.Path
}

type TransformBinding = TransformSpec

func (p *PayloadTransformSpec) UnmarshalYAML(node *yaml.Node) error {
	if p == nil {
		return nil
	}
	type alias PayloadTransformSpec
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*p = PayloadTransformSpec(aux)
	p.Entries = p.TransformEntries()
	return nil
}

func (p PayloadTransformSpec) TransformEntries() []TransformSpec {
	mappings := p.Mappings
	if len(mappings) == 0 && len(p.Fields) > 0 {
		mappings = p.Fields
	}
	out := make([]TransformSpec, 0, len(mappings))
	for target, source := range mappings {
		cleanTarget := strings.TrimSpace(target)
		cleanSource := strings.TrimSpace(source)
		if cleanTarget == "" || cleanSource == "" {
			continue
		}
		out = append(out, TransformSpec{
			Target:     cleanTarget,
			TargetPath: paths.Parse(cleanTarget),
			Source:     cleanSource,
			SourcePath: paths.Parse(cleanSource),
		})
	}
	return out
}

func (p PayloadTransformSpec) TransformBindings() []TransformBinding {
	return p.TransformEntries()
}

type ConfigFromSpec struct {
	PolicyKeys []string          `yaml:"policy_keys"`
	Bindings   map[string]string `yaml:",inline"`
	Entries    []ConfigBinding   `yaml:"-"`
}

type ConfigBinding struct {
	Key     string
	Ref     string
	RefPath paths.Path
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

type BranchSpec struct {
	Condition string            `yaml:"condition"`
	Then      *HandlerRuleEntry `yaml:"then"`
	Else      *HandlerRuleEntry `yaml:"else"`
}

type GateSpec struct {
	Name  string `yaml:"name"`
	Value any    `yaml:"value"`
}

func (g *GateSpec) UnmarshalYAML(node *yaml.Node) error {
	if g == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*g = GateSpec{}
			return nil
		}
		*g = GateSpec{Name: strings.TrimSpace(node.Value), Value: true}
		return nil
	case yaml.MappingNode:
		type alias GateSpec
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*g = GateSpec(aux)
		return nil
	default:
		return fmt.Errorf("unsupported gate yaml node kind %d", node.Kind)
	}
}

type QuerySpec struct {
	Operation    string      `yaml:"operation"`
	Source       string      `yaml:"source"`
	SourcePath   paths.Path  `yaml:"-"`
	StoreAs      string      `yaml:"store_as"`
	StorePath    paths.Path  `yaml:"-"`
	Entities     string      `yaml:"entities"`
	EntitiesPath paths.Path  `yaml:"-"`
	Filter       string      `yaml:"filter"`
	GroupBy      string      `yaml:"group_by"`
	GroupByPath  paths.Path  `yaml:"-"`
	Count        bool        `yaml:"count"`
	Select       []string    `yaml:"select"`
	Queries      []QuerySpec `yaml:"-"`
}

func (s *FilterSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	type alias FilterSpec
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = FilterSpec(aux)
	s.Source = strings.TrimSpace(s.Source)
	s.SourcePath = paths.Parse(s.Source)
	s.ItemsFrom = strings.TrimSpace(s.ItemsFrom)
	s.ItemsPath = paths.Parse(s.ItemsFrom)
	s.StoreAs = strings.TrimSpace(s.StoreAs)
	s.StorePath = paths.Parse(s.StoreAs)
	s.Predicate = strings.TrimSpace(s.Predicate)
	s.Condition = strings.TrimSpace(s.Condition)
	return nil
}

func (s *ReduceSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	type alias ReduceSpec
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = ReduceSpec(aux)
	s.Operation = strings.TrimSpace(s.Operation)
	s.Source = strings.TrimSpace(s.Source)
	s.SourcePath = paths.Parse(s.Source)
	s.StoreAs = strings.TrimSpace(s.StoreAs)
	s.StorePath = paths.Parse(s.StoreAs)
	s.ItemsFrom = strings.TrimSpace(s.ItemsFrom)
	s.ItemsPath = paths.Parse(s.ItemsFrom)
	return nil
}

func (s *CountSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	type alias CountSpec
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = CountSpec(aux)
	s.Source = strings.TrimSpace(s.Source)
	s.SourcePath = paths.Parse(s.Source)
	s.StoreAs = strings.TrimSpace(s.StoreAs)
	s.StorePath = paths.Parse(s.StoreAs)
	s.ItemsFrom = strings.TrimSpace(s.ItemsFrom)
	s.ItemsPath = paths.Parse(s.ItemsFrom)
	s.Condition = strings.TrimSpace(s.Condition)
	return nil
}

func (s *QuerySpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	type alias QuerySpec
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = QuerySpec(aux)
	s.hydratePaths()
	return nil
}

func (s *QuerySpec) hydratePaths() {
	if s == nil {
		return
	}
	s.Operation = strings.TrimSpace(s.Operation)
	s.Source = strings.TrimSpace(s.Source)
	s.SourcePath = paths.Parse(s.Source)
	s.StoreAs = strings.TrimSpace(s.StoreAs)
	s.StorePath = paths.Parse(s.StoreAs)
	s.Entities = strings.TrimSpace(s.Entities)
	s.EntitiesPath = paths.Parse(s.Entities)
	s.Filter = strings.TrimSpace(s.Filter)
	s.GroupBy = strings.TrimSpace(s.GroupBy)
	s.GroupByPath = paths.Parse(s.GroupBy)
	for i := range s.Queries {
		s.Queries[i].hydratePaths()
	}
}

type ActionSpec struct {
	ID             string          `yaml:"id"`
	Template       string          `yaml:"template"`
	InstanceIDFrom string          `yaml:"instance_id_from"`
	InstanceIDPath paths.Path      `yaml:"-"`
	ConfigFrom     *ConfigFromSpec `yaml:"config_from"`
}

type EntitySchema struct {
	Groups []EntitySchemaGroup `yaml:"groups"`
}

func (s EntitySchema) Empty() bool {
	return len(s.Groups) == 0
}

type EntitySchemaGroup struct {
	Name   string              `yaml:"name"`
	Fields []EntitySchemaField `yaml:"fields"`
}

type EntitySchemaField struct {
	Name     string `yaml:"name"`
	Type     string `yaml:"type"`
	Primary  bool   `yaml:"primary"`
	Indexed  bool   `yaml:"indexed"`
	Nullable bool   `yaml:"nullable"`
}

type NodeStateSchema struct {
	Description string           `yaml:"description"`
	Fields      []NodeStateField `yaml:"fields"`
}

type NodeStateField struct {
	Name    string `yaml:"name"`
	Type    string `yaml:"type"`
	Default any    `yaml:"default"`
}

type PolicyDocument = flowmodel.PolicyDocument
type PolicyValue = flowmodel.PolicyValue
type ContractURIRegistry = flowmodel.URIRegistry
type ContractURIRef = flowmodel.URIRef

type ExpressionValue struct {
	Literal any    `yaml:"literal,omitempty"`
	CEL     string `yaml:"cel,omitempty"`
}

func (e ExpressionValue) IsZero() bool {
	return strings.TrimSpace(e.CEL) == "" && e.Literal == nil
}

type FlowVariable struct {
	Type        string `yaml:"type"`
	Default     any    `yaml:"default"`
	Description string `yaml:"description"`
}

func (v *FlowVariable) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		v.Description = strings.TrimSpace(node.Value)
		return nil
	}
	type alias FlowVariable
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*v = FlowVariable(aux)
	return nil
}

type EventEmitterRef struct {
	AgentID string `yaml:"agent_id"`
	NodeID  string `yaml:"node_id"`
}

type EventPayloadSpec struct {
	Type       string                    `yaml:"type"`
	Properties map[string]EventFieldSpec `yaml:"properties"`
	Required   []string                  `yaml:"required"`
}

type EventFieldSpec struct {
	Type        string `yaml:"type"`
	Description string `yaml:"description"`
}

type SchemaLiteral struct {
	Node yaml.Node
}

type ToolAdditionalProperties struct {
	Allowed *bool
	Schema  *ToolInputSchema
}

type ToolInputSchema struct {
	Type                 string                     `yaml:"type"`
	Description          string                     `yaml:"description"`
	Properties           map[string]ToolInputSchema `yaml:"properties"`
	Required             []string                   `yaml:"required"`
	Items                *ToolInputSchema           `yaml:"items"`
	Enum                 []SchemaLiteral            `yaml:"enum"`
	AdditionalProperties ToolAdditionalProperties   `yaml:"additionalProperties"`
	Minimum              *float64                   `yaml:"minimum"`
	Maximum              *float64                   `yaml:"maximum"`
}

func (b *WorkflowContractBundle) WorkflowName() string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.Name)
}

func (b *WorkflowContractBundle) WorkflowVersion() string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.Version)
}

func (b *WorkflowContractBundle) WorkflowEntitySchema() EntitySchema {
	if b == nil {
		return EntitySchema{}
	}
	return b.Semantics.EntitySchema
}

func (b *WorkflowContractBundle) WorkflowStages() []WorkflowStageContract {
	if b == nil {
		return nil
	}
	return b.Semantics.Stages
}

func (b *WorkflowContractBundle) WorkflowTerminalStages() []string {
	if b == nil {
		return nil
	}
	return b.Semantics.TerminalStages
}

func (b *WorkflowContractBundle) WorkflowTransitions() []WorkflowTransitionContract {
	if b == nil {
		return nil
	}
	return b.Semantics.Transitions
}

func (b *WorkflowContractBundle) WorkflowInitialStage() string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.InitialStage)
}

func (b *WorkflowContractBundle) WorkflowTimers() []WorkflowTimerContract {
	if b == nil {
		return nil
	}
	return b.Semantics.Timers
}

func (b *WorkflowContractBundle) WorkflowTimerByID(id string) (WorkflowTimerContract, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return WorkflowTimerContract{}, false
	}
	for _, timer := range b.Semantics.Timers {
		if strings.TrimSpace(timer.ID) == id {
			return timer, true
		}
	}
	return WorkflowTimerContract{}, false
}

func (b *WorkflowContractBundle) FlowViewByID(id string) (*FlowContractView, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return nil, false
	}
	if view, ok := b.FlowTree.ByID[id]; ok && view != nil {
		return view, true
	}
	return nil, false
}

func (b *WorkflowContractBundle) FlowSchemaByID(id string) (FlowSchemaDocument, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return FlowSchemaDocument{}, false
	}
	schema, ok := b.FlowSchemas[id]
	return schema, ok
}

func (b *WorkflowContractBundle) HasFlow(id string) bool {
	_, ok := b.FlowViewByID(id)
	return ok
}

func (b *WorkflowContractBundle) ProjectViews() []ProjectContractView {
	if b == nil || len(b.projectContracts) == 0 {
		return nil
	}
	keys := make([]string, 0, len(b.projectContracts))
	for key := range b.projectContracts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	views := make([]ProjectContractView, 0, len(keys))
	for _, key := range keys {
		views = append(views, b.projectContracts[key])
	}
	return views
}

func (b *WorkflowContractBundle) ProjectViewByKey(key string) (ProjectContractView, bool) {
	key = strings.TrimSpace(key)
	if b == nil || key == "" {
		return ProjectContractView{}, false
	}
	view, ok := b.projectContracts[key]
	return view, ok
}

func (b *WorkflowContractBundle) RootProjectViews() []ProjectContractView {
	if b == nil || len(b.PackageTree) == 0 {
		return nil
	}
	views := make([]ProjectContractView, 0, len(b.PackageTree))
	for _, pkg := range b.PackageTree {
		if strings.TrimSpace(pkg.ParentKey) != "" {
			continue
		}
		if view, ok := b.ProjectViewByKey(pkg.Key); ok {
			views = append(views, view)
		}
	}
	return views
}

func (b *WorkflowContractBundle) FlowViews() []FlowContractView {
	if b == nil {
		return nil
	}
	return flowmodel.ViewsByPath(
		b.FlowTree,
		func(view *FlowContractView) string { return strings.TrimSpace(view.Paths.ID) },
		func(view *FlowContractView) string { return strings.TrimSpace(view.Path) },
		flowViewChildren,
	)
}

func (b *WorkflowContractBundle) NodeEntries() map[string]SystemNodeContract {
	if b == nil {
		return nil
	}
	return cloneSystemNodeContractMap(b.Nodes)
}

func (b *WorkflowContractBundle) NodeEntry(id string) (SystemNodeContract, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return SystemNodeContract{}, false
	}
	entry, ok := b.Nodes[id]
	return entry, ok
}

func (b *WorkflowContractBundle) HasNode(id string) bool {
	_, ok := b.NodeEntry(id)
	return ok
}

func (b *WorkflowContractBundle) AgentEntries() map[string]AgentRegistryEntry {
	if b == nil {
		return nil
	}
	return cloneAgentRegistryEntryMap(b.Agents)
}

func (b *WorkflowContractBundle) AgentEntry(id string) (AgentRegistryEntry, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return AgentRegistryEntry{}, false
	}
	entry, ok := b.Agents[id]
	return entry, ok
}

func (b *WorkflowContractBundle) HasAgent(id string) bool {
	_, ok := b.AgentEntry(id)
	return ok
}

func (b *WorkflowContractBundle) ToolEntries() map[string]ToolSchemaEntry {
	if b == nil {
		return nil
	}
	return cloneToolSchemaEntryMap(b.Tools)
}

func (b *WorkflowContractBundle) EventEntries() map[string]EventCatalogEntry {
	if b == nil {
		return nil
	}
	return cloneEventCatalogEntryMap(b.Events)
}

func (b *WorkflowContractBundle) EventEntry(eventType string) (EventCatalogEntry, bool) {
	eventType = strings.TrimSpace(eventType)
	if b == nil || eventType == "" {
		return EventCatalogEntry{}, false
	}
	entry, ok := b.Events[eventType]
	return entry, ok
}

func (b *WorkflowContractBundle) HasEvent(eventType string) bool {
	_, ok := b.EventEntry(eventType)
	return ok
}

func (b *WorkflowContractBundle) ResolvedPolicyForFlow(flowID string) PolicyDocument {
	if b == nil {
		return PolicyDocument{Values: map[string]PolicyValue{}}
	}
	return flowmodel.ResolvePolicyByID(
		b.Policy,
		b.FlowTree,
		flowID,
		func(view *FlowContractView) string { return strings.TrimSpace(view.Paths.ID) },
		func(view *FlowContractView) PolicyDocument { return view.Policy },
		flowViewChildren,
	)
}

func (b *WorkflowContractBundle) PolicyValueForFlow(flowID, key string) (PolicyValue, bool) {
	doc := b.ResolvedPolicyForFlow(flowID)
	value, ok := doc.Values[strings.TrimSpace(key)]
	return value, ok
}

func (b *WorkflowContractBundle) ResolvedPolicyForNode(nodeID string) PolicyDocument {
	if b == nil {
		return PolicyDocument{Values: map[string]PolicyValue{}}
	}
	if source, ok := b.NodeContractSource(nodeID); ok {
		return b.ResolvedPolicyForFlow(source.FlowID)
	}
	return b.ResolvedPolicyForFlow("")
}

func (b *WorkflowContractBundle) PolicyValueForNode(nodeID, key string) (PolicyValue, bool) {
	doc := b.ResolvedPolicyForNode(nodeID)
	value, ok := doc.Values[strings.TrimSpace(key)]
	return value, ok
}

func (b *WorkflowContractBundle) FlowPath(flowID string) string {
	if b == nil {
		return ""
	}
	return flowmodel.PathForID(b.FlowTree, flowID, func(view *FlowContractView) string { return view.Path })
}

func (b *WorkflowContractBundle) ResolvedEventCatalog() map[string]EventCatalogEntry {
	if b == nil {
		return nil
	}
	if b.FlowTree.Root == nil {
		return cloneEventCatalogEntryMap(b.Events)
	}
	return flowmodel.ResolveEntries(
		b.FlowTree,
		flowViewChildren,
		func(view *FlowContractView) map[string]EventCatalogEntry { return view.Events },
	)
}

func clonePolicyDocument(in PolicyDocument) PolicyDocument {
	return flowmodel.ClonePolicyDocument(in)
}

func flowViewChildren(view *FlowContractView) []*FlowContractView {
	if view == nil || len(view.Children) == 0 {
		return nil
	}
	children := make([]*FlowContractView, 0, len(view.Children))
	for i := range view.Children {
		children = append(children, &view.Children[i])
	}
	return children
}

func rootWorkflowPolicy(bundle *WorkflowContractBundle) PolicyDocument {
	if bundle == nil {
		return PolicyDocument{Values: map[string]PolicyValue{}}
	}
	for _, view := range bundle.RootProjectViews() {
		return clonePolicyDocument(view.Policy)
	}
	if bundle.FlowTree.Root != nil {
		return clonePolicyDocument(bundle.FlowTree.Root.Policy)
	}
	return PolicyDocument{Values: map[string]PolicyValue{}}
}

func (b *WorkflowContractBundle) GuardEntries() []GuardActionEntry {
	if b == nil {
		return nil
	}
	return b.Semantics.Guards
}

func (b *WorkflowContractBundle) ActionEntries() []GuardActionEntry {
	if b == nil {
		return nil
	}
	return b.Semantics.Actions
}

func (b *WorkflowContractBundle) GuardEntryByID(id string) (GuardActionEntry, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return GuardActionEntry{}, false
	}
	entry, ok := b.Semantics.GuardByID[id]
	return entry, ok
}

func (b *WorkflowContractBundle) ActionEntryByID(id string) (GuardActionEntry, bool) {
	id = strings.TrimSpace(id)
	if b == nil || id == "" {
		return GuardActionEntry{}, false
	}
	entry, ok := b.Semantics.ActionByID[id]
	return entry, ok
}

func (b *WorkflowContractBundle) FlowInitialStage(flowID string) string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.FlowInitial[strings.TrimSpace(flowID)])
}

func (b *WorkflowContractBundle) FlowStates(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowStates[strings.TrimSpace(flowID)]...)
}

func (b *WorkflowContractBundle) FlowTerminalStages(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowTerminal[strings.TrimSpace(flowID)]...)
}

func (b *WorkflowContractBundle) FlowNamespace(flowID string) string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.FlowNamespace[strings.TrimSpace(flowID)])
}

func (b *WorkflowContractBundle) FlowNamespacePrefix(flowID string) string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.FlowPrefix[strings.TrimSpace(flowID)])
}

func (b *WorkflowContractBundle) FlowNamespaceRule(flowID string) string {
	if b == nil {
		return ""
	}
	return strings.TrimSpace(b.Semantics.FlowRules[strings.TrimSpace(flowID)])
}

func (b *WorkflowContractBundle) FlowInputEvents(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowInputs[strings.TrimSpace(flowID)]...)
}

func (b *WorkflowContractBundle) FlowOutputEvents(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowOutputs[strings.TrimSpace(flowID)]...)
}

func (b *WorkflowContractBundle) FlowReadPins(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowReads[strings.TrimSpace(flowID)]...)
}

func (b *WorkflowContractBundle) FlowWritePins(flowID string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.FlowWrites[strings.TrimSpace(flowID)]...)
}

func (b *WorkflowContractBundle) FlowRequiredAgents(flowID string) []FlowRequiredAgent {
	if b == nil {
		return nil
	}
	agents := b.Semantics.FlowAgents[strings.TrimSpace(flowID)]
	out := make([]FlowRequiredAgent, len(agents))
	copy(out, agents)
	return out
}

func (b *WorkflowContractBundle) RootRequiredAgents() []FlowRequiredAgent {
	if b == nil || b.RootSchema == nil {
		return nil
	}
	out := make([]FlowRequiredAgent, len(b.RootSchema.RequiredAgents))
	copy(out, b.RootSchema.RequiredAgents)
	return out
}

func (b *WorkflowContractBundle) WritePinOwners(pin string) []string {
	if b == nil {
		return nil
	}
	return append([]string{}, b.Semantics.WritePinOwners[strings.TrimSpace(pin)]...)
}

func (b *WorkflowContractBundle) NodeContractSource(nodeID string) (ContractItemSource, bool) {
	if b == nil {
		return ContractItemSource{}, false
	}
	source, ok := b.nodeSources[strings.TrimSpace(nodeID)]
	return source, ok
}

func (b *WorkflowContractBundle) EventContractSource(eventType string) (ContractItemSource, bool) {
	if b == nil {
		return ContractItemSource{}, false
	}
	source, ok := b.eventSources[strings.TrimSpace(eventType)]
	return source, ok
}

func (b *WorkflowContractBundle) AgentContractSource(agentID string) (ContractItemSource, bool) {
	if b == nil {
		return ContractItemSource{}, false
	}
	source, ok := b.agentSources[strings.TrimSpace(agentID)]
	return source, ok
}

func (b *WorkflowContractBundle) ScopedAgentEntries() map[string]AgentRegistryEntry {
	if b == nil {
		return nil
	}
	return cloneAgentRegistryEntryMap(b.scopedAgents)
}

func (b *WorkflowContractBundle) ScopedEventEntries() map[string]EventCatalogEntry {
	if b == nil {
		return nil
	}
	return cloneEventCatalogEntryMap(b.scopedEvents)
}

func (b *WorkflowContractBundle) NodeEventHandlers(nodeID string) map[string]SystemNodeEventHandler {
	if b == nil {
		return nil
	}
	nodeID = strings.TrimSpace(nodeID)
	handlers, ok := b.Semantics.NodeHandlers[nodeID]
	if !ok {
		return nil
	}
	out := make(map[string]SystemNodeEventHandler, len(handlers))
	for eventType, handler := range handlers {
		out[eventType] = handler
	}
	return out
}

func (b *WorkflowContractBundle) NodeEventHandler(nodeID, eventType string) (SystemNodeEventHandler, bool) {
	if b == nil {
		return SystemNodeEventHandler{}, false
	}
	nodeID = strings.TrimSpace(nodeID)
	eventType = strings.TrimSpace(eventType)
	handlers, ok := b.Semantics.NodeHandlers[nodeID]
	if !ok {
		return SystemNodeEventHandler{}, false
	}
	if handler, ok := handlers[eventType]; ok {
		return handler, true
	}
	for pattern, handler := range handlers {
		if handlerPatternMatches(pattern, eventType) {
			return handler, true
		}
	}
	return SystemNodeEventHandler{}, false
}

func (b *WorkflowContractBundle) RuntimeEventOwners(eventType string) []string {
	if b == nil {
		return nil
	}
	eventType = strings.TrimSpace(eventType)
	owners := append([]string{}, b.Semantics.EventOwners[eventType]...)
	for nodeID, handlers := range b.Semantics.NodeHandlers {
		for pattern := range handlers {
			if strings.TrimSpace(pattern) == eventType {
				continue
			}
			if handlerPatternMatches(pattern, eventType) {
				owners = appendIfMissingString(owners, nodeID)
				break
			}
		}
	}
	return owners
}

func (b *WorkflowContractBundle) DerivedHandlerTransitions() []HandlerTransitionSemantic {
	if b == nil {
		return nil
	}
	out := make([]HandlerTransitionSemantic, len(b.Semantics.HandlerTransitions))
	copy(out, b.Semantics.HandlerTransitions)
	return out
}

func (b *WorkflowContractBundle) DerivedHandlerTransition(nodeID, eventType string) (HandlerTransitionSemantic, bool) {
	if b == nil {
		return HandlerTransitionSemantic{}, false
	}
	nodeID = strings.TrimSpace(nodeID)
	eventType = strings.TrimSpace(eventType)
	transitions, ok := b.Semantics.HandlerTransitionIndex[nodeID]
	if !ok {
		return HandlerTransitionSemantic{}, false
	}
	if transition, ok := transitions[eventType]; ok {
		return transition, true
	}
	for pattern, transition := range transitions {
		if handlerPatternMatches(pattern, eventType) {
			return transition, true
		}
	}
	return HandlerTransitionSemantic{}, false
}

type ProjectPackagePaths struct {
	Key               string
	ParentKey         string
	Depth             int
	Dir               string
	PackageFile       string
	ProjectNodesFile  string
	ProjectEventsFile string
	ProjectAgentsFile string
	ProjectToolsFile  string
	ProjectPolicyFile string
	ProjectPromptsDir string
	Flows             []FlowContractPaths
}

type FlowContractPaths struct {
	ID         string
	Flow       string
	Mode       string
	Namespace  string
	PackageKey string
	PackageDir string
	Dir        string
	SchemaFile string
	NodesFile  string
	EventsFile string
	AgentsFile string
	ToolsFile  string
	PolicyFile string
	PromptsDir string
}

type ProjectPackageDocument struct {
	Name            string              `yaml:"name"`
	Version         string              `yaml:"version"`
	PlatformVersion string              `yaml:"platform_version"`
	Author          string              `yaml:"author"`
	Description     string              `yaml:"description"`
	Flows           []ProjectFlowRef    `yaml:"flows"`
	Packages        []ProjectPackageRef `yaml:"packages"`
	Children        []ProjectPackageRef `yaml:"children"`
	Subpackages     []ProjectPackageRef `yaml:"subpackages"`
	Handoffs        []ProjectHandoff    `yaml:"handoffs"`
	EntitySchema    EntitySchema        `yaml:"entity_schema"`
}

const maxDiscoveredPackageDepth = 99

type ProjectPackageRef struct {
	ID      string `yaml:"id"`
	Path    string `yaml:"path"`
	Package string `yaml:"package"`
	Dir     string `yaml:"dir"`
}

type ProjectFlowRef struct {
	ID        string `yaml:"id"`
	Flow      string `yaml:"flow"`
	Namespace string `yaml:"namespace"`
	Mode      string `yaml:"mode"`
}

type ProjectHandoff struct {
	Event       string `yaml:"event"`
	CreatesFlow string `yaml:"creates_flow"`
	Namespace   string `yaml:"namespace"`
}

type LoadedProjectPackage struct {
	Key       string
	ParentKey string
	Depth     int
	Paths     ProjectPackagePaths
	Manifest  ProjectPackageDocument
}

type ProjectContractView = flowmodel.PackageView[
	ProjectPackagePaths,
	ProjectPackageDocument,
	SystemNodeContract,
	EventCatalogEntry,
	AgentRegistryEntry,
	ToolSchemaEntry,
]

type FlowContractView = flowmodel.View[
	FlowContractPaths,
	FlowSchemaDocument,
	SystemNodeContract,
	EventCatalogEntry,
	AgentRegistryEntry,
	ToolSchemaEntry,
]

type FlowTree = flowmodel.Tree[FlowContractView]

type ContractItemSource struct {
	PackageKey string
	FlowID     string
	Layer      string
	File       string
}

func contractScopeKey(source ContractItemSource, localID string) string {
	localID = strings.TrimSpace(localID)
	parts := make([]string, 0, 3)
	if pkg := strings.TrimSpace(source.PackageKey); pkg != "" {
		parts = append(parts, pkg)
	}
	if flowID := strings.TrimSpace(source.FlowID); flowID != "" {
		parts = append(parts, flowID)
	}
	if localID != "" {
		parts = append(parts, localID)
	}
	return strings.Join(parts, "::")
}

func contractSameScope(a, b ContractItemSource) bool {
	return strings.TrimSpace(a.PackageKey) == strings.TrimSpace(b.PackageKey) &&
		strings.TrimSpace(a.FlowID) == strings.TrimSpace(b.FlowID) &&
		strings.TrimSpace(a.Layer) == strings.TrimSpace(b.Layer)
}

type FlowSchemaDocument struct {
	Name              string                   `yaml:"name"`
	Mode              string                   `yaml:"mode"`
	InitialState      string                   `yaml:"initial_state"`
	NamespacePrefix   string                   `yaml:"namespace_prefix"`
	NamespaceRule     string                   `yaml:"namespace_rule"`
	TerminalStates    []string                 `yaml:"terminal_states"`
	States            []string                 `yaml:"states"`
	Pins              FlowPins                 `yaml:"pins"`
	RequiredAgents    []FlowRequiredAgent      `yaml:"required_agents"`
	InstanceVariables FlowInstanceVariables    `yaml:"instance_variables"`
	AutoEmitOnCreate  AutoEmitOnCreateContract `yaml:"auto_emit_on_create"`
}

type FlowInstanceVariables struct {
	Description string                  `yaml:"description"`
	Variables   map[string]FlowVariable `yaml:"variables"`
}

type AutoEmitOnCreateContract struct {
	Event       string `yaml:"event"`
	Description string `yaml:"description"`
}

type FlowPins struct {
	Inputs  FlowInputPins  `yaml:"inputs"`
	Outputs FlowOutputPins `yaml:"outputs"`
}

type FlowInputPins struct {
	Events []string `yaml:"events"`
	Reads  []string `yaml:"reads"`
}

type FlowOutputPins struct {
	Events []string `yaml:"events"`
	Writes []string `yaml:"writes"`
}

type FlowRequiredAgent struct {
	Role         string   `yaml:"role"`
	SubscribesTo []string `yaml:"subscribes_to"`
	Emits        []string `yaml:"emits"`
	Description  string   `yaml:"description"`
}

type WorkflowStageContract struct {
	ID          string `yaml:"id"`
	Phase       string `yaml:"phase"`
	Description string `yaml:"description"`
}

type WorkflowTransitionContract struct {
	ID                string                   `yaml:"id"`
	From              []string                 `yaml:"from"`
	To                string                   `yaml:"to"`
	Trigger           string                   `yaml:"trigger"`
	Node              string                   `yaml:"node"`
	Guards            []string                 `yaml:"guards"`
	Actions           []string                 `yaml:"actions"`
	DataAccumulation  WorkflowDataAccumulation `yaml:"data_accumulation"`
	AllowTerminalExit bool                     `yaml:"allow_terminal_exit"`
}

func (t *WorkflowTransitionContract) UnmarshalYAML(node *yaml.Node) error {
	if t == nil {
		return nil
	}
	// Decode all fields except From using a shadow type that replaces From with a yaml.Node.
	type shadow struct {
		ID                string                   `yaml:"id"`
		From              yaml.Node                `yaml:"from"`
		To                string                   `yaml:"to"`
		Trigger           string                   `yaml:"trigger"`
		Node              string                   `yaml:"node"`
		Guards            []string                 `yaml:"guards"`
		Actions           []string                 `yaml:"actions"`
		DataAccumulation  WorkflowDataAccumulation `yaml:"data_accumulation"`
		AllowTerminalExit bool                     `yaml:"allow_terminal_exit"`
	}
	var aux shadow
	if err := node.Decode(&aux); err != nil {
		return err
	}
	t.ID = aux.ID
	t.To = aux.To
	t.Trigger = aux.Trigger
	t.Node = aux.Node
	t.Guards = aux.Guards
	t.Actions = aux.Actions
	t.DataAccumulation = aux.DataAccumulation
	t.AllowTerminalExit = aux.AllowTerminalExit
	from, err := decodeStringListNode(&aux.From)
	if err != nil {
		return err
	}
	t.From = from
	return nil
}

type WorkflowDataAccumulation struct {
	Writes      []WorkflowDataWrite `yaml:"writes"`
	SourceEvent string              `yaml:"source_event"`
	Value       ExpressionValue     `yaml:"value,omitempty"`
}

type WorkflowDataWrite struct {
	Field       string          `yaml:"-" json:"field,omitempty"`
	SourceField string          `yaml:"source_field,omitempty" json:"source_field,omitempty"`
	SourcePath  paths.Path      `yaml:"-" json:"-"`
	TargetField string          `yaml:"target_field,omitempty" json:"target_field,omitempty"`
	TargetPath  paths.Path      `yaml:"-" json:"-"`
	Value       ExpressionValue `yaml:"value,omitempty" json:"value,omitempty"`
}

func (w *WorkflowDataWrite) UnmarshalYAML(node *yaml.Node) error {
	if w == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*w = WorkflowDataWrite{}
			return nil
		}
		w.Field = strings.TrimSpace(node.Value)
		w.SourcePath = paths.Parse(w.Field)
		w.TargetPath = paths.Parse(w.Field)
		return nil
	case yaml.MappingNode:
		type alias WorkflowDataWrite
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*w = WorkflowDataWrite(aux)
		w.Field = strings.TrimSpace(w.Field)
		w.SourceField = strings.TrimSpace(w.SourceField)
		w.TargetField = strings.TrimSpace(w.TargetField)
		w.SourcePath = paths.Parse(w.Source())
		w.TargetPath = paths.Parse(w.Target())
		return nil
	default:
		return fmt.Errorf("unsupported workflow data write yaml node kind %d", node.Kind)
	}
}

func (w WorkflowDataWrite) Source() string {
	switch {
	case strings.TrimSpace(w.SourceField) != "":
		return strings.TrimSpace(w.SourceField)
	case strings.TrimSpace(w.Field) != "":
		return strings.TrimSpace(w.Field)
	default:
		return strings.TrimSpace(w.TargetField)
	}
}

func (w WorkflowDataWrite) Target() string {
	switch {
	case strings.TrimSpace(w.TargetField) != "":
		return strings.TrimSpace(w.TargetField)
	case strings.TrimSpace(w.Field) != "":
		return strings.TrimSpace(w.Field)
	default:
		return strings.TrimSpace(w.SourceField)
	}
}

func (w WorkflowDataWrite) HasLiteralValue() bool {
	return w.Value.Literal != nil
}

func (a WorkflowDataAccumulation) TargetFields() []string {
	if len(a.Writes) == 0 {
		return nil
	}
	out := make([]string, 0, len(a.Writes))
	for _, write := range a.Writes {
		if target := strings.TrimSpace(write.Target()); target != "" {
			out = append(out, target)
		}
	}
	return out
}

func (a WorkflowDataAccumulation) HasWrites() bool {
	return len(a.Writes) > 0
}

func (e *ExpressionValue) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*e = ExpressionValue{}
			return nil
		}
		switch strings.TrimSpace(node.Tag) {
		case "!!str", "":
			*e = ExpressionValue{CEL: strings.TrimSpace(node.Value)}
		default:
			var literal any
			if err := node.Decode(&literal); err != nil {
				return err
			}
			*e = ExpressionValue{Literal: literal}
		}
		return nil
	case yaml.MappingNode:
		type alias ExpressionValue
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*e = ExpressionValue(aux)
		return nil
	case yaml.SequenceNode:
		var literal any
		if err := node.Decode(&literal); err != nil {
			return err
		}
		*e = ExpressionValue{Literal: literal}
		return nil
	default:
		return fmt.Errorf("unsupported expression value yaml node kind %d", node.Kind)
	}
}

func (s *EntitySchema) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	if node.Kind != yaml.MappingNode {
		type alias EntitySchema
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*s = EntitySchema(aux)
		return nil
	}
	if hasYAMLMappingKey(node, "groups") {
		type alias EntitySchema
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*s = EntitySchema(aux)
		return nil
	}
	if looksLikeEntitySchemaFieldMap(node) {
		fields, err := decodeEntitySchemaFields(node)
		if err != nil {
			return err
		}
		s.Groups = []EntitySchemaGroup{{Name: "default", Fields: fields}}
		return nil
	}
	groups := make([]EntitySchemaGroup, 0, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		groupName := strings.TrimSpace(node.Content[i].Value)
		if groupName == "" || groupName == "description" {
			continue
		}
		// Skip scalar values (e.g. description text) — groups are mappings.
		if node.Content[i+1].Kind == yaml.ScalarNode {
			continue
		}
		fields, err := decodeEntitySchemaFields(node.Content[i+1])
		if err != nil {
			return err
		}
		groups = append(groups, EntitySchemaGroup{Name: groupName, Fields: fields})
	}
	s.Groups = groups
	return nil
}

func (s *NodeStateSchema) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	var aux struct {
		Description string    `yaml:"description"`
		Fields      yaml.Node `yaml:"fields"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	s.Description = strings.TrimSpace(aux.Description)
	fields, err := decodeNodeStateFields(&aux.Fields)
	if err != nil {
		return err
	}
	s.Fields = fields
	return nil
}

func (s *EventFieldSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		*s = EventFieldSpec{Type: strings.TrimSpace(node.Value)}
		return nil
	case yaml.MappingNode:
		type alias EventFieldSpec
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*s = EventFieldSpec(aux)
		return nil
	default:
		return fmt.Errorf("unsupported event field yaml node kind %d", node.Kind)
	}
}

func (p *EventPayloadSpec) UnmarshalYAML(node *yaml.Node) error {
	if p == nil {
		return nil
	}
	type alias EventPayloadSpec
	if node.Kind != yaml.MappingNode {
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*p = EventPayloadSpec(aux)
		return nil
	}
	if hasYAMLMappingKey(node, "properties") || hasYAMLMappingKey(node, "required") {
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*p = EventPayloadSpec(aux)
		return nil
	}
	spec := EventPayloadSpec{Properties: map[string]EventFieldSpec{}}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		switch key {
		case "type":
			spec.Type = strings.TrimSpace(node.Content[i+1].Value)
		case "required":
			var required []string
			if err := node.Content[i+1].Decode(&required); err != nil {
				return err
			}
			spec.Required = normalizeStrings(required)
		default:
			var field EventFieldSpec
			if err := node.Content[i+1].Decode(&field); err != nil {
				return err
			}
			spec.Properties[key] = field
		}
	}
	*p = spec
	return nil
}

func (v *SchemaLiteral) UnmarshalYAML(node *yaml.Node) error {
	if v == nil || node == nil {
		return nil
	}
	v.Node = *node
	return nil
}

func (a *ToolAdditionalProperties) UnmarshalYAML(node *yaml.Node) error {
	if a == nil || node == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*a = ToolAdditionalProperties{}
			return nil
		}
		var allowed bool
		if err := node.Decode(&allowed); err != nil {
			return err
		}
		a.Allowed = &allowed
		a.Schema = nil
		return nil
	case yaml.MappingNode:
		var schema ToolInputSchema
		if err := node.Decode(&schema); err != nil {
			return err
		}
		a.Allowed = nil
		a.Schema = &schema
		return nil
	default:
		return fmt.Errorf("unsupported additionalProperties yaml node kind %d", node.Kind)
	}
}

func (s *ToolInputSchema) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	type alias ToolInputSchema
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = ToolInputSchema(aux)
	return nil
}

type WorkflowTimerContract struct {
	ID           string `yaml:"id"`
	Stage        string `yaml:"stage"`
	Event        string `yaml:"event"`
	Owner        string `yaml:"owner"`
	FlowID       string `yaml:"-"`
	NodeID       string `yaml:"-"`
	Action       string `yaml:"action"`
	Cancellation string `yaml:"cancellation"`
	Delay        string `yaml:"delay"`
	StartOn      string `yaml:"start_on"`
	CancelOn     string `yaml:"cancel_on"`
	DelaySeconds int    `yaml:"delay_seconds"`
	DelayMinutes int    `yaml:"delay_minutes"`
	DelayHours   int    `yaml:"delay_hours"`
	DelayDays    int    `yaml:"delay_days"`
	Recurring    bool   `yaml:"recurring"`
}

func (t *WorkflowTimerContract) UnmarshalYAML(node *yaml.Node) error {
	if t == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*t = WorkflowTimerContract{}
			return nil
		}
		t.ID = strings.TrimSpace(node.Value)
		return nil
	case yaml.MappingNode:
		type alias WorkflowTimerContract
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*t = WorkflowTimerContract(aux)
		return nil
	default:
		return fmt.Errorf("unsupported workflow timer yaml node kind %d", node.Kind)
	}
}

type EventEmission struct {
	Single string   `yaml:"-" json:"single,omitempty"`
	Many   []string `yaml:"-" json:"many,omitempty"`
}

func (e *EventEmission) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*e = EventEmission{}
			return nil
		}
		e.Single = strings.TrimSpace(node.Value)
		return nil
	case yaml.SequenceNode:
		var many []string
		if err := node.Decode(&many); err != nil {
			return err
		}
		e.Many = normalizeStrings(many)
		return nil
	default:
		return fmt.Errorf("unsupported event emission yaml node kind %d", node.Kind)
	}
}

func (e EventEmission) First() string {
	if strings.TrimSpace(e.Single) != "" {
		return strings.TrimSpace(e.Single)
	}
	if len(e.Many) > 0 {
		return strings.TrimSpace(e.Many[0])
	}
	return ""
}

func (e EventEmission) Values() []string {
	if len(e.Many) > 0 {
		return append([]string{}, normalizeStrings(e.Many)...)
	}
	if first := strings.TrimSpace(e.Single); first != "" {
		return []string{first}
	}
	return nil
}

func (e EventEmission) Empty() bool {
	return len(e.Values()) == 0
}

type GuardActionEntry struct {
	ID              string `yaml:"id"`
	Category        string `yaml:"category"`
	Description     string `yaml:"description"`
	Input           string `yaml:"input"`
	Check           string `yaml:"check"`
	PolicyRef       string `yaml:"policy_ref"`
	PlatformBuiltin string `yaml:"platform_builtin"`
	Effect          string `yaml:"effect"`
	Emits           string `yaml:"emits"`
}

type SystemNodeContract struct {
	ID               string                            `yaml:"id"`
	Description      string                            `yaml:"description"`
	ExecutionType    string                            `yaml:"execution_type"`
	Implementation   string                            `yaml:"implementation"`
	SubscribesTo     []string                          `yaml:"subscribes_to"`
	Produces         []string                          `yaml:"produces"`
	OwnedTransitions []string                          `yaml:"owned_transitions"`
	StateTable       string                            `yaml:"state_table"`
	IdempotencyTable string                            `yaml:"idempotency_table"`
	Timers           []WorkflowTimerContract           `yaml:"timers"`
	EventHandlers    map[string]SystemNodeEventHandler `yaml:"event_handlers"`
	StateSchema      NodeStateSchema                   `yaml:"state_schema"`
}

type SystemNodeEventHandler struct {
	Action           ActionSpec               `yaml:"action"`
	Description      string                   `yaml:"description"`
	Emits            EventEmission            `yaml:"emits"`
	Guard            *GuardSpec               `yaml:"guard"`
	AdvancesTo       string                   `yaml:"advances_to"`
	SetsGate         *GateSpec                `yaml:"sets_gate"`
	ClearGates       []string                 `yaml:"clear_gates"`
	DataAccumulation WorkflowDataAccumulation `yaml:"data_accumulation"`
	Condition        string                   `yaml:"condition"`
	CompletionRule   string                   `yaml:"completion_rule"`
	Logic            string                   `yaml:"logic"`
	PolicyRef        string                   `yaml:"policy_ref"`
	OnComplete       []HandlerRuleEntry       `yaml:"on_complete"`
	Rules            []HandlerRuleEntry       `yaml:"rules"`
	Accumulate       *AccumulateSpec          `yaml:"accumulate"`
	Compute          *ComputeSpec             `yaml:"compute"`
	Query            *QuerySpec               `yaml:"query"`
	FanOut           *FanOutSpec              `yaml:"fan_out"`
	Filter           *FilterSpec              `yaml:"filter"`
	Reduce           *ReduceSpec              `yaml:"reduce"`
	Count            *CountSpec               `yaml:"count"`
	Clear            *ClearSpec               `yaml:"clear"`
	PayloadTransform *PayloadTransformSpec    `yaml:"payload_transform"`
	Branch           []BranchSpec             `yaml:"branch"`
}

type EventCatalogEntry struct {
	Emitter           EventEmitterRef  `yaml:"emitter"`
	EmitterType       string           `yaml:"emitter_type"`
	AlternateEmitters []string         `yaml:"alternate_emitters"`
	Consumer          []string         `yaml:"consumer"`
	ConsumerType      []string         `yaml:"consumer_type"`
	Source            string           `yaml:"_source"`
	Status            string           `yaml:"_status"`
	Intercepted       bool             `yaml:"intercepted"`
	Passthrough       bool             `yaml:"passthrough"`
	RuntimeHandling   string           `yaml:"runtime_handling"`
	OwningNode        string           `yaml:"owning_node"`
	DeliveryChannel   string           `yaml:"delivery_channel"`
	Payload           EventPayloadSpec `yaml:"payload"`
	Required          []string         `yaml:"required"`
}

type AgentRegistryEntry struct {
	ID                     string   `yaml:"id"`
	Type                   string   `yaml:"type"`
	Role                   string   `yaml:"role"`
	Permissions            []string `yaml:"permissions" json:"permissions,omitempty"`
	PermissionsBundle      string   `yaml:"permissions_bundle" json:"permissions_bundle,omitempty"`
	WorkspaceClass         string   `yaml:"workspace_class"`
	ManagerFallback        string   `yaml:"manager_fallback"`
	NodeType               string   `yaml:"node_type"`
	ModelTier              string   `yaml:"model_tier"`
	ConversationMode       string   `yaml:"conversation_mode"`
	MaxTurnsPerTask        int      `yaml:"max_turns_per_task"`
	Subscriptions          []string `yaml:"subscriptions"`
	SubscriptionsBootstrap []string `yaml:"subscriptions_bootstrap"`
	SubscribesTo           []string `yaml:"subscribes_to"`
	ToolsTier2             []string `yaml:"tools_tier2"`
	EmitEvents             []string `yaml:"emit_events"`
	Implementation         string   `yaml:"implementation"`
}

type ToolSchemaEntry struct {
	Category    string          `yaml:"category"`
	Description string          `yaml:"description"`
	InputSchema ToolInputSchema `yaml:"input_schema"`
}

func (h *SystemNodeEventHandler) UnmarshalYAML(node *yaml.Node) error {
	if h == nil {
		return nil
	}
	if err := validateHandlerFieldNodes(node); err != nil {
		return err
	}
	var aux struct {
		Action           yaml.Node                `yaml:"action"`
		Description      string                   `yaml:"description"`
		Emits            EventEmission            `yaml:"emits"`
		Guard            yaml.Node                `yaml:"guard"`
		AdvancesTo       yaml.Node                `yaml:"advances_to"`
		SetsGate         yaml.Node                `yaml:"sets_gate"`
		ClearGates       yaml.Node                `yaml:"clear_gates"`
		DataAccumulation WorkflowDataAccumulation `yaml:"data_accumulation"`
		Condition        string                   `yaml:"condition"`
		CompletionRule   string                   `yaml:"completion_rule"`
		Logic            string                   `yaml:"logic"`
		PolicyRef        string                   `yaml:"policy_ref"`
		OnComplete       yaml.Node                `yaml:"on_complete"`
		Rules            yaml.Node                `yaml:"rules"`
		Accumulate       *AccumulateSpec          `yaml:"accumulate"`
		Compute          *ComputeSpec             `yaml:"compute"`
		Query            yaml.Node                `yaml:"query"`
		FanOut           *FanOutSpec              `yaml:"fan_out"`
		Filter           *FilterSpec              `yaml:"filter"`
		Reduce           *ReduceSpec              `yaml:"reduce"`
		Count            *CountSpec               `yaml:"count"`
		Clear            yaml.Node                `yaml:"clear"`
		PayloadTransform *PayloadTransformSpec    `yaml:"payload_transform"`
		Branch           yaml.Node                `yaml:"branch"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*h = SystemNodeEventHandler{
		Description:      strings.TrimSpace(aux.Description),
		Emits:            aux.Emits,
		DataAccumulation: aux.DataAccumulation,
		Condition:        strings.TrimSpace(aux.Condition),
		CompletionRule:   strings.TrimSpace(aux.CompletionRule),
		Logic:            strings.TrimSpace(aux.Logic),
		PolicyRef:        strings.TrimSpace(aux.PolicyRef),
		Accumulate:       aux.Accumulate,
		Compute:          aux.Compute,
		FanOut:           aux.FanOut,
		Filter:           aux.Filter,
		Reduce:           aux.Reduce,
		Count:            aux.Count,
		PayloadTransform: aux.PayloadTransform,
	}
	var err error
	if h.Action, err = decodeActionSpecNode(&aux.Action); err != nil {
		return err
	}
	if h.Guard, err = decodeGuardSpecNode(&aux.Guard); err != nil {
		return err
	}
	if h.AdvancesTo, err = decodeAdvancesToNode(&aux.AdvancesTo); err != nil {
		return err
	}
	if h.SetsGate, err = decodeGateSpecNode(&aux.SetsGate); err != nil {
		return err
	}
	if h.ClearGates, err = decodeClearGatesNode(&aux.ClearGates); err != nil {
		return err
	}
	if h.OnComplete, err = decodeHandlerRuleEntriesNode(&aux.OnComplete); err != nil {
		return err
	}
	if h.Rules, err = decodeHandlerRuleEntriesNode(&aux.Rules); err != nil {
		return err
	}
	if h.Query, err = decodeQuerySpecNode(&aux.Query); err != nil {
		return err
	}
	if h.Clear, err = decodeClearSpecNode(&aux.Clear); err != nil {
		return err
	}
	if h.Branch, err = decodeBranchSpecsNode(&aux.Branch); err != nil {
		return err
	}
	return nil
}

func (e *EventCatalogEntry) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	var aux struct {
		Emitter           yaml.Node `yaml:"emitter"`
		EmitterType       string    `yaml:"emitter_type"`
		AlternateEmitters []string  `yaml:"alternate_emitters"`
		Consumer          yaml.Node `yaml:"consumer"`
		ConsumerType      yaml.Node `yaml:"consumer_type"`
		Source            string    `yaml:"_source"`
		Status            string    `yaml:"_status"`
		Intercepted       yaml.Node `yaml:"intercepted"`
		Passthrough       yaml.Node `yaml:"passthrough"`
		RuntimeHandling   string    `yaml:"runtime_handling"`
		OwningNode        string    `yaml:"owning_node"`
		DeliveryChannel   yaml.Node `yaml:"delivery_channel"`
		Payload           yaml.Node `yaml:"payload"`
		Required          []string  `yaml:"required"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	emitter, alternates, err := decodeEventEmitterNode(&aux.Emitter)
	if err != nil {
		return err
	}
	consumer, err := decodeStringListNode(&aux.Consumer)
	if err != nil {
		return err
	}
	consumerType, err := decodeStringListNode(&aux.ConsumerType)
	if err != nil {
		return err
	}
	intercepted, err := decodeBoolNode(&aux.Intercepted)
	if err != nil {
		return err
	}
	passthrough, err := decodeBoolNode(&aux.Passthrough)
	if err != nil {
		return err
	}
	deliveryChannel, err := decodeScalarStringNode(&aux.DeliveryChannel)
	if err != nil {
		return err
	}
	payload, err := decodeEventPayloadSpecNode(&aux.Payload)
	if err != nil {
		return err
	}
	e.Emitter = emitter
	e.EmitterType = strings.TrimSpace(aux.EmitterType)
	e.AlternateEmitters = mergeStringLists(aux.AlternateEmitters, alternates)
	e.Consumer = consumer
	e.ConsumerType = consumerType
	e.Source = strings.TrimSpace(aux.Source)
	e.Status = strings.TrimSpace(aux.Status)
	e.Intercepted = intercepted
	e.Passthrough = passthrough
	e.RuntimeHandling = strings.TrimSpace(aux.RuntimeHandling)
	e.OwningNode = strings.TrimSpace(aux.OwningNode)
	e.DeliveryChannel = deliveryChannel
	e.Payload = payload
	e.Required = normalizeStrings(aux.Required)
	return nil
}

type PlatformSpecDocument struct {
	Platform struct {
		Name    string `yaml:"name"`
		Version string `yaml:"version"`
	} `yaml:"platform"`
	PermissionsModel struct {
		Permissions []string `yaml:"permissions"`
	} `yaml:"permissions_model"`
	Vocabulary struct {
		Participant struct {
			Types map[string]struct {
				Execution string `yaml:"execution"`
			} `yaml:"types"`
		} `yaml:"participant"`
	} `yaml:"vocabulary"`
	ContractFormats yaml.Node `yaml:"contract_formats"`
	WorkflowState   struct {
		DDL    string `yaml:"ddl"`
		Fields map[string]struct {
			Type string `yaml:"type"`
		} `yaml:"fields"`
	} `yaml:"workflow_state"`
	PlatformTables struct {
		Tables map[string]struct {
			Description string `yaml:"description"`
			DDL         string `yaml:"ddl"`
		} `yaml:"tables"`
	} `yaml:"platform_tables"`
	BuiltinHooks struct {
		Guards []struct {
			ID string `yaml:"id"`
		} `yaml:"guards"`
		Actions []struct {
			ID string `yaml:"id"`
		} `yaml:"actions"`
	} `yaml:"builtin_hooks"`
	ComplianceRules yaml.Node `yaml:"compliance_rules"`
	FileLayout      struct {
		MigrationNote string `yaml:"migration_note"`
	} `yaml:"file_layout"`
}

func ResolveWorkflowContractPaths(repoRoot string) ContractPaths {
	return ResolveWorkflowContractPathsWithOverrides(repoRoot, "", "")
}

func DefaultPlatformContractsDir(repoRoot string) string {
	return filepath.Join(repoRoot, "docs", "specs", "mas-platform", "platform", "contracts")
}

func DefaultPlatformSpecFile(repoRoot string) string {
	return filepath.Join(DefaultPlatformContractsDir(repoRoot), "platform-spec.yaml")
}

func defaultMASAuxFile(repoRoot, envKey string, pathParts ...string) string {
	if env := strings.TrimSpace(os.Getenv(envKey)); env != "" {
		return env
	}
	return filepath.Join(append([]string{repoRoot}, pathParts...)...)
}

func DefaultWorkflowContractsDir(repoRoot string) string {
	if env := strings.TrimSpace(os.Getenv("MAS_CONTRACTS_DIR")); env != "" {
		return env
	}
	base := filepath.Join(repoRoot, "docs", "specs", "mas-platform")
	entries, err := os.ReadDir(base)
	if err == nil {
		candidates := make([]string, 0, len(entries))
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			name := strings.TrimSpace(entry.Name())
			if name == "" || name == "platform" || name == "tests" {
				continue
			}
			dir := filepath.Join(base, name, "contracts")
			if existingFile(filepath.Join(dir, "package.yaml")) != "" {
				candidates = append(candidates, dir)
			}
		}
		sort.Strings(candidates)
		if len(candidates) > 0 {
			return candidates[0]
		}
	}
	return ""
}

func RepoRootHasMASContracts(repoRoot string) bool {
	return existingFile(filepath.Join(DefaultWorkflowContractsDir(repoRoot), "package.yaml")) != ""
}

func ResolveWorkflowContractPathsWithOverrides(repoRoot, workflowDirOverride, platformSpecFileOverride string) ContractPaths {
	workflowDir := DefaultWorkflowContractsDir(repoRoot)
	overrideActive := strings.TrimSpace(workflowDirOverride) != ""
	if overrideActive {
		workflowDir = workflowDirOverride
	}
	platformSpecFile := DefaultPlatformSpecFile(repoRoot)
	if strings.TrimSpace(platformSpecFileOverride) != "" {
		platformSpecFile = platformSpecFileOverride
	}
	paths := ContractPaths{
		ContractsRoot:         workflowDir,
		WorkflowDir:           workflowDir,
		RootSchemaFile:        existingFile(filepath.Join(workflowDir, "schema.yaml")),
		ProjectPackageFile:    existingFile(filepath.Join(workflowDir, "package.yaml")),
		ProjectNodesFile:      existingFile(filepath.Join(workflowDir, "nodes.yaml")),
		ProjectEventsFile:     existingFile(filepath.Join(workflowDir, "events.yaml")),
		ProjectAgentsFile:     existingFile(filepath.Join(workflowDir, "agents.yaml")),
		ProjectToolsFile:      existingFile(filepath.Join(workflowDir, "tools.yaml")),
		ProjectPolicyFile:     existingFile(filepath.Join(workflowDir, "policy.yaml")),
		ProjectPromptsDir:     existingDir(filepath.Join(workflowDir, "prompts")),
		PlatformSpecFile:      platformSpecFile,
		VerificationGatesFile: defaultMASAuxFile(repoRoot, "MAS_VERIFICATION_GATES_FILE", "docs", "specs", "mas-platform", "verification-gates.yaml"),
		ToolingLockFile:       defaultMASAuxFile(repoRoot, "MAS_TOOLING_LOCK_FILE", "docs", "specs", "mas-platform", "tooling.lock"),
		DDLFile:               "",
		AgentConfigMapFile:    defaultMASAuxFile(repoRoot, "MAS_AGENT_CONFIG_MAP_FILE", "docs", "specs", "mas-platform", "agent-config-map.yaml"),
	}
	if paths.ProjectPackageFile != "" {
		paths.ProjectPackages = discoverProjectPackagePaths(paths.ProjectPackageFile, workflowDir)
		for _, pkg := range paths.ProjectPackages {
			paths.Flows = append(paths.Flows, pkg.Flows...)
		}
		sort.Slice(paths.Flows, func(i, j int) bool {
			if paths.Flows[i].ID == paths.Flows[j].ID {
				if paths.Flows[i].PackageKey == paths.Flows[j].PackageKey {
					return strings.Compare(paths.Flows[i].Flow, paths.Flows[j].Flow) < 0
				}
				return strings.Compare(paths.Flows[i].PackageKey, paths.Flows[j].PackageKey) < 0
			}
			return strings.Compare(paths.Flows[i].ID, paths.Flows[j].ID) < 0
		})
	}
	return paths
}

func LoadWorkflowContractBundle(repoRoot string) (*WorkflowContractBundle, error) {
	return loadWorkflowContractBundleForPaths(ResolveWorkflowContractPaths(repoRoot))
}

func LoadWorkflowContractBundleWithOverrides(repoRoot, workflowDirOverride, platformSpecFileOverride string) (*WorkflowContractBundle, error) {
	return loadWorkflowContractBundleForPaths(ResolveWorkflowContractPathsWithOverrides(repoRoot, workflowDirOverride, platformSpecFileOverride))
}

func loadWorkflowContractBundleForPaths(paths ContractPaths) (*WorkflowContractBundle, error) {
	bundle := &WorkflowContractBundle{
		Paths:                 paths,
		projectContracts:      map[string]ProjectContractView{},
		scopedNodes:           map[string]SystemNodeContract{},
		scopedEvents:          map[string]EventCatalogEntry{},
		scopedAgents:          map[string]AgentRegistryEntry{},
		scopedTools:           map[string]ToolSchemaEntry{},
		scopedNodeSources:     map[string]ContractItemSource{},
		scopedEventSources:    map[string]ContractItemSource{},
		scopedAgentSources:    map[string]ContractItemSource{},
		scopedToolSources:     map[string]ContractItemSource{},
		nodeSources:           map[string]ContractItemSource{},
		eventSources:          map[string]ContractItemSource{},
		agentSources:          map[string]ContractItemSource{},
		toolSources:           map[string]ContractItemSource{},
		ambiguousNodeAliases:  map[string]struct{}{},
		ambiguousEventAliases: map[string]struct{}{},
		ambiguousAgentAliases: map[string]struct{}{},
		ambiguousToolAliases:  map[string]struct{}{},
		Nodes:                 map[string]SystemNodeContract{},
		Events:                map[string]EventCatalogEntry{},
		Agents:                map[string]AgentRegistryEntry{},
		Tools:                 map[string]ToolSchemaEntry{},
		FlowSchemas:           map[string]FlowSchemaDocument{},
	}
	flowViewsByID := map[string]FlowContractView{}
	if paths.ProjectPackageFile != "" {
		if strings.TrimSpace(paths.RootSchemaFile) != "" {
			var rootSchema FlowSchemaDocument
			if err := loadYAMLFile(paths.RootSchemaFile, &rootSchema); err != nil {
				return nil, err
			}
			bundle.RootSchema = &rootSchema
		}
		for i, pkgPaths := range paths.ProjectPackages {
			var manifest ProjectPackageDocument
			if err := loadYAMLFile(pkgPaths.PackageFile, &manifest); err != nil {
				return nil, err
			}
			if i == 0 {
				bundle.Package = manifest
			}
			bundle.PackageTree = append(bundle.PackageTree, LoadedProjectPackage{
				Key:       pkgPaths.Key,
				ParentKey: pkgPaths.ParentKey,
				Depth:     pkgPaths.Depth,
				Paths:     pkgPaths,
				Manifest:  manifest,
			})
			projectView, err := loadProjectContractView(pkgPaths, manifest)
			if err != nil {
				return nil, err
			}
			bundle.projectContracts[pkgPaths.Key] = projectView
		}
		if err := validateDiscoveredPackageTree(bundle.PackageTree); err != nil {
			return nil, err
		}
		for _, flow := range paths.Flows {
			if strings.TrimSpace(flow.ID) == "" || strings.TrimSpace(flow.SchemaFile) == "" {
				continue
			}
			if _, exists := bundle.FlowSchemas[flow.ID]; exists {
				return nil, fmt.Errorf("duplicate flow id %q discovered in package tree", flow.ID)
			}
			var schema FlowSchemaDocument
			if err := loadYAMLFile(flow.SchemaFile, &schema); err != nil {
				return nil, err
			}
			if schema.Mode == "" {
				schema.Mode = strings.TrimSpace(flow.Mode)
			}
			bundle.FlowSchemas[flow.ID] = schema
			flowView, err := loadFlowContractView(flow, schema)
			if err != nil {
				return nil, err
			}
			flowViewsByID[flow.ID] = flowView
		}
		if err := buildFlowTree(bundle, flowViewsByID); err != nil {
			return nil, err
		}
		if err := populateMergedPackageViews(bundle, flowViewsByID); err != nil {
			return nil, err
		}
	}
	bundle.Policy = rootWorkflowPolicy(bundle)
	if err := loadYAMLFile(paths.PlatformSpecFile, &bundle.Platform); err != nil {
		return nil, err
	}
	populateWorkflowSemantics(bundle)
	if err := validateWorkflowContractBundleLoadConstraints(bundle); err != nil {
		return nil, err
	}
	return bundle, nil
}

func validateWorkflowContractBundleLoadConstraints(bundle *WorkflowContractBundle) error {
	if bundle == nil {
		return nil
	}
	errs := make([]string, 0, 8)
	for nodeID, node := range bundle.Nodes {
		nodeID = strings.TrimSpace(nodeID)
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			if workflowHandlerDeclaresConflictingCompletion(handler) {
				errs = append(errs, fmt.Sprintf("node %s handler %s declares both on_complete and rules", nodeID, eventType))
			}
			if usesDeprecatedGuardFallback(handler.Guard) {
				errs = append(errs, fmt.Sprintf("node %s handler %s uses deprecated id-only guard; migrate to check:", nodeID, eventType))
			}
		}
	}
	for eventType, owners := range bundle.Semantics.EventOwners {
		if len(normalizeStrings(owners)) > 1 {
			errs = append(errs, fmt.Sprintf("event %s has multiple authoritative system node owners: %s", strings.TrimSpace(eventType), strings.Join(normalizeStrings(owners), ", ")))
		}
	}
	if len(errs) > 0 {
		sort.Strings(errs)
		return fmt.Errorf("workflow contract load validation failed:\n- %s", strings.Join(errs, "\n- "))
	}
	return nil
}

func workflowHandlerDeclaresConflictingCompletion(handler SystemNodeEventHandler) bool {
	return len(handler.Rules) > 0 && workflowHandlerHasOnComplete(handler)
}

func workflowHandlerHasOnComplete(handler SystemNodeEventHandler) bool {
	if len(handler.OnComplete) > 0 {
		return true
	}
	return handler.Accumulate != nil && len(handler.Accumulate.OnComplete) > 0
}

func usesDeprecatedGuardFallback(spec *GuardSpec) bool {
	if spec == nil {
		return false
	}
	if strings.TrimSpace(spec.Check) != "" {
		return false
	}
	for _, check := range spec.Checks {
		if strings.TrimSpace(check.Check) != "" {
			return false
		}
	}
	return strings.TrimSpace(spec.ID) != ""
}

func populateWorkflowSemantics(bundle *WorkflowContractBundle) {
	if bundle == nil {
		return
	}
	name := strings.TrimSpace(bundle.Package.Name)
	version := strings.TrimSpace(bundle.Package.Version)
	entitySchema := bundle.Package.EntitySchema
	semantics := WorkflowSemanticView{
		Name:                   name,
		Version:                version,
		InitialStage:           "",
		EntitySchema:           entitySchema,
		Stages:                 deriveWorkflowStagesFromFlows(bundle.Paths.Flows, bundle.FlowSchemas),
		TerminalStages:         deriveWorkflowTerminalStagesFromFlows(bundle.Paths.Flows, bundle.FlowSchemas),
		Transitions:            nil,
		Timers:                 deriveWorkflowSemanticTimers(bundle),
		Guards:                 deriveWorkflowGuardEntries(bundle),
		Actions:                deriveWorkflowActionEntries(bundle),
		GuardByID:              map[string]GuardActionEntry{},
		ActionByID:             map[string]GuardActionEntry{},
		FlowInitial:            map[string]string{},
		FlowStates:             map[string][]string{},
		FlowTerminal:           map[string][]string{},
		FlowNamespace:          map[string]string{},
		FlowPrefix:             map[string]string{},
		FlowRules:              map[string]string{},
		FlowInputs:             map[string][]string{},
		FlowOutputs:            map[string][]string{},
		FlowReads:              map[string][]string{},
		FlowWrites:             map[string][]string{},
		FlowAgents:             map[string][]FlowRequiredAgent{},
		WritePinOwners:         map[string][]string{},
		NodeHandlers:           map[string]map[string]SystemNodeEventHandler{},
		EventOwners:            map[string][]string{},
		HandlerTransitionIndex: map[string]map[string]HandlerTransitionSemantic{},
	}
	semantics.Guards = appendPlatformBuiltinGuardEntries(semantics.Guards, bundle.Platform.BuiltinHooks.Guards)
	semantics.Actions = appendPlatformBuiltinActionEntries(semantics.Actions, bundle.Platform.BuiltinHooks.Actions)
	for _, entry := range semantics.Guards {
		if id := strings.TrimSpace(entry.ID); id != "" {
			semantics.GuardByID[id] = entry
		}
	}
	for _, entry := range semantics.Actions {
		if id := strings.TrimSpace(entry.ID); id != "" {
			semantics.ActionByID[id] = entry
		}
	}
	for flowID, schema := range bundle.FlowSchemas {
		flowID = strings.TrimSpace(flowID)
		if flowID == "" {
			continue
		}
		semantics.FlowInitial[flowID] = strings.TrimSpace(schema.InitialState)
		semantics.FlowStates[flowID] = append([]string{}, schema.States...)
		semantics.FlowTerminal[flowID] = append([]string{}, schema.TerminalStates...)
		assignedNamespace := strings.TrimSpace(flowAssignedNamespace(bundle.Paths.Flows, flowID))
		if assignedNamespace == "" {
			assignedNamespace = strings.TrimSpace(schema.NamespacePrefix)
		}
		semantics.FlowNamespace[flowID] = assignedNamespace
		semantics.FlowPrefix[flowID] = strings.TrimSpace(schema.NamespacePrefix)
		semantics.FlowRules[flowID] = strings.TrimSpace(schema.NamespaceRule)
		semantics.FlowInputs[flowID] = append([]string{}, schema.Pins.Inputs.Events...)
		semantics.FlowOutputs[flowID] = append([]string{}, schema.Pins.Outputs.Events...)
		semantics.FlowReads[flowID] = append([]string{}, schema.Pins.Inputs.Reads...)
		semantics.FlowWrites[flowID] = append([]string{}, schema.Pins.Outputs.Writes...)
		semantics.FlowAgents[flowID] = append([]FlowRequiredAgent{}, schema.RequiredAgents...)
		for _, writePin := range schema.Pins.Outputs.Writes {
			writePin = strings.TrimSpace(writePin)
			if writePin == "" {
				continue
			}
			semantics.WritePinOwners[writePin] = appendIfMissingString(semantics.WritePinOwners[writePin], flowID)
		}
	}
	for nodeID, node := range bundle.Nodes {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" || len(node.EventHandlers) == 0 {
			continue
		}
		handlers := make(map[string]SystemNodeEventHandler, len(node.EventHandlers))
		source, _ := bundle.NodeContractSource(nodeID)
		for eventType, handler := range node.EventHandlers {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			handlers[eventType] = handler
			semantics.EventOwners[eventType] = appendIfMissingString(semantics.EventOwners[eventType], nodeID)
			transition := HandlerTransitionSemantic{
				ID:               fmt.Sprintf("%s:%s", nodeID, eventType),
				NodeID:           nodeID,
				FlowID:           strings.TrimSpace(source.FlowID),
				EventType:        eventType,
				Action:           handler.Action,
				Guard:            handler.Guard,
				AdvancesTo:       strings.TrimSpace(handler.AdvancesTo),
				SetsGate:         handler.SetsGate,
				ClearGates:       handler.ClearGates,
				DataAccumulation: handler.DataAccumulation,
				Emits:            handler.Emits,
				Condition:        strings.TrimSpace(handler.Condition),
				CompletionRule:   strings.TrimSpace(handler.CompletionRule),
				OnComplete:       handler.OnComplete,
				Rules:            handler.Rules,
				Accumulate:       handler.Accumulate,
				Compute:          handler.Compute,
				Query:            handler.Query,
				FanOut:           handler.FanOut,
				Filter:           handler.Filter,
				Reduce:           handler.Reduce,
				Count:            handler.Count,
				Clear:            handler.Clear,
				PayloadTransform: handler.PayloadTransform,
				Branch:           append([]BranchSpec{}, handler.Branch...),
			}
			semantics.HandlerTransitions = append(semantics.HandlerTransitions, transition)
			if derivedTransition, ok := deriveWorkflowTransitionContract(transition); ok {
				semantics.Transitions = append(semantics.Transitions, derivedTransition)
			}
			if semantics.HandlerTransitionIndex[nodeID] == nil {
				semantics.HandlerTransitionIndex[nodeID] = map[string]HandlerTransitionSemantic{}
			}
			semantics.HandlerTransitionIndex[nodeID][eventType] = transition
		}
		semantics.NodeHandlers[nodeID] = handlers
	}
	bundle.Semantics = semantics
}

func deriveWorkflowGuardEntries(bundle *WorkflowContractBundle) []GuardActionEntry {
	if bundle == nil {
		return nil
	}
	seen := map[string]GuardActionEntry{}
	for _, nodeID := range sortedContractKeys(bundle.Nodes) {
		node := bundle.Nodes[nodeID]
		for _, eventType := range sortedContractKeys(node.EventHandlers) {
			handler := node.EventHandlers[eventType]
			if handler.Guard == nil {
				continue
			}
			id := strings.TrimSpace(handler.Guard.ID)
			if id == "" {
				continue
			}
			seen[id] = GuardActionEntry{
				ID:        id,
				Check:     strings.TrimSpace(handler.Guard.Check),
				PolicyRef: strings.TrimSpace(handler.Guard.PolicyRef),
			}
		}
	}
	return sortedGuardActionEntries(seen)
}

func deriveWorkflowActionEntries(bundle *WorkflowContractBundle) []GuardActionEntry {
	if bundle == nil {
		return nil
	}
	seen := map[string]GuardActionEntry{}
	for _, nodeID := range sortedContractKeys(bundle.Nodes) {
		node := bundle.Nodes[nodeID]
		for _, eventType := range sortedContractKeys(node.EventHandlers) {
			handler := node.EventHandlers[eventType]
			id := strings.TrimSpace(handler.Action.ID)
			if id == "" {
				continue
			}
			seen[id] = GuardActionEntry{ID: id}
		}
	}
	return sortedGuardActionEntries(seen)
}

func sortedGuardActionEntries(entries map[string]GuardActionEntry) []GuardActionEntry {
	if len(entries) == 0 {
		return nil
	}
	ids := make([]string, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]GuardActionEntry, 0, len(ids))
	for _, id := range ids {
		out = append(out, entries[id])
	}
	return out
}

func deriveWorkflowTransitionContract(transition HandlerTransitionSemantic) (WorkflowTransitionContract, bool) {
	to := strings.TrimSpace(transition.AdvancesTo)
	if to == "" {
		return WorkflowTransitionContract{}, false
	}
	out := WorkflowTransitionContract{
		ID:               strings.TrimSpace(transition.ID),
		From:             []string{"*"},
		To:               to,
		Trigger:          strings.TrimSpace(transition.EventType),
		Node:             strings.TrimSpace(transition.NodeID),
		DataAccumulation: transition.DataAccumulation,
	}
	if guardID := strings.TrimSpace(firstTransitionGuardID(transition.Guard)); guardID != "" {
		out.Guards = []string{guardID}
	}
	if actionID := strings.TrimSpace(transition.Action.ID); actionID != "" {
		out.Actions = []string{actionID}
	}
	return out, strings.TrimSpace(out.ID) != "" && strings.TrimSpace(out.Trigger) != ""
}

func firstTransitionGuardID(guard *GuardSpec) string {
	if guard == nil {
		return ""
	}
	return strings.TrimSpace(guard.ID)
}

func deriveWorkflowSemanticTimers(bundle *WorkflowContractBundle) []WorkflowTimerContract {
	if bundle == nil {
		return nil
	}
	out := make([]WorkflowTimerContract, 0, 8)
	indexByID := map[string]int{}
	addTimer := func(timer WorkflowTimerContract) {
		timer = normalizeWorkflowSemanticTimer(bundle, timer)
		id := strings.TrimSpace(timer.ID)
		if id == "" {
			return
		}
		if idx, ok := indexByID[id]; ok {
			out[idx] = mergeWorkflowSemanticTimer(out[idx], timer)
			return
		}
		indexByID[id] = len(out)
		out = append(out, timer)
	}
	for _, timer := range deriveNodeWorkflowTimers(bundle) {
		addTimer(timer)
	}
	return out
}

func deriveNodeWorkflowTimers(bundle *WorkflowContractBundle) []WorkflowTimerContract {
	if bundle == nil {
		return nil
	}
	type scopedNodeEntry struct {
		Key    string
		NodeID string
		Node   SystemNodeContract
		Source ContractItemSource
	}
	scopedNodes := make([]scopedNodeEntry, 0, len(bundle.scopedNodes))
	for scopedKey, node := range bundle.scopedNodes {
		source := bundle.scopedNodeSources[scopedKey]
		nodeID := strings.TrimSpace(node.ID)
		if nodeID == "" {
			parts := strings.Split(scopedKey, "::")
			if len(parts) > 0 {
				nodeID = strings.TrimSpace(parts[len(parts)-1])
			}
		}
		scopedNodes = append(scopedNodes, scopedNodeEntry{
			Key:    scopedKey,
			NodeID: nodeID,
			Node:   node,
			Source: source,
		})
	}
	if len(scopedNodes) == 0 {
		for nodeID, node := range bundle.Nodes {
			scopedNodes = append(scopedNodes, scopedNodeEntry{
				Key:    nodeID,
				NodeID: strings.TrimSpace(nodeID),
				Node:   node,
				Source: bundle.nodeSources[nodeID],
			})
		}
	}
	if len(scopedNodes) == 0 && len(bundle.Nodes) > 0 {
		for nodeID, node := range bundle.Nodes {
			scopedNodes = append(scopedNodes, scopedNodeEntry{
				Key:    nodeID,
				NodeID: strings.TrimSpace(nodeID),
				Node:   node,
			})
		}
	}
	if len(scopedNodes) == 0 {
		return nil
	}
	out := make([]WorkflowTimerContract, 0, 8)
	sort.Slice(scopedNodes, func(i, j int) bool {
		return strings.Compare(scopedNodes[i].Key, scopedNodes[j].Key) < 0
	})
	for _, item := range scopedNodes {
		nodeID := strings.TrimSpace(item.NodeID)
		node := item.Node
		if len(node.Timers) == 0 {
			continue
		}
		flowID := strings.TrimSpace(item.Source.FlowID)
		for _, timer := range node.Timers {
			timer.NodeID = nodeID
			timer.FlowID = flowID
			if strings.TrimSpace(timer.Owner) == "" {
				timer.Owner = nodeID
			}
			if strings.TrimSpace(timer.Event) == "" {
				timer.Event = inferWorkflowTimerEvent(bundle, node, timer)
			}
			out = append(out, timer)
		}
	}
	return out
}

func normalizeWorkflowSemanticTimer(bundle *WorkflowContractBundle, timer WorkflowTimerContract) WorkflowTimerContract {
	timer.ID = strings.TrimSpace(timer.ID)
	timer.Stage = strings.TrimSpace(timer.Stage)
	timer.Event = strings.TrimSpace(timer.Event)
	timer.Owner = strings.TrimSpace(timer.Owner)
	timer.FlowID = strings.TrimSpace(timer.FlowID)
	timer.NodeID = strings.TrimSpace(timer.NodeID)
	timer.Action = strings.TrimSpace(timer.Action)
	timer.Cancellation = strings.TrimSpace(timer.Cancellation)
	timer.Delay = strings.TrimSpace(timer.Delay)
	timer.StartOn = strings.TrimSpace(timer.StartOn)
	timer.CancelOn = strings.TrimSpace(timer.CancelOn)
	if timer.Event == "" && timer.NodeID != "" {
		node := bundle.Nodes[timer.NodeID]
		timer.Event = inferWorkflowTimerEvent(bundle, node, timer)
	}
	return timer
}

func mergeWorkflowSemanticTimer(existing, incoming WorkflowTimerContract) WorkflowTimerContract {
	if strings.TrimSpace(existing.ID) == "" {
		return incoming
	}
	if strings.TrimSpace(existing.Stage) == "" {
		existing.Stage = incoming.Stage
	}
	if strings.TrimSpace(existing.Event) == "" {
		existing.Event = incoming.Event
	}
	if strings.TrimSpace(existing.Owner) == "" {
		existing.Owner = incoming.Owner
	}
	if strings.TrimSpace(existing.FlowID) == "" {
		existing.FlowID = incoming.FlowID
	}
	if strings.TrimSpace(existing.NodeID) == "" {
		existing.NodeID = incoming.NodeID
	}
	if strings.TrimSpace(existing.Action) == "" {
		existing.Action = incoming.Action
	}
	if strings.TrimSpace(existing.Cancellation) == "" {
		existing.Cancellation = incoming.Cancellation
	}
	if strings.TrimSpace(existing.Delay) == "" {
		existing.Delay = incoming.Delay
	}
	if strings.TrimSpace(existing.StartOn) == "" {
		existing.StartOn = incoming.StartOn
	}
	if strings.TrimSpace(existing.CancelOn) == "" {
		existing.CancelOn = incoming.CancelOn
	}
	if existing.DelaySeconds == 0 {
		existing.DelaySeconds = incoming.DelaySeconds
	}
	if existing.DelayMinutes == 0 {
		existing.DelayMinutes = incoming.DelayMinutes
	}
	if existing.DelayHours == 0 {
		existing.DelayHours = incoming.DelayHours
	}
	if existing.DelayDays == 0 {
		existing.DelayDays = incoming.DelayDays
	}
	existing.Recurring = existing.Recurring || incoming.Recurring
	return existing
}

func inferWorkflowTimerEvent(bundle *WorkflowContractBundle, node SystemNodeContract, timer WorkflowTimerContract) string {
	if eventType := strings.TrimSpace(timer.Event); eventType != "" {
		return eventType
	}
	timerID := strings.TrimSpace(timer.ID)
	if timerID == "" {
		return ""
	}
	candidates := []string{timerID}
	if !strings.HasPrefix(timerID, "timer.") {
		candidates = append([]string{"timer." + timerID}, candidates...)
	}
	for _, candidate := range candidates {
		if _, ok := node.EventHandlers[candidate]; ok {
			return candidate
		}
	}
	for _, candidate := range candidates {
		if workflowTimerEventDefined(bundle, candidate) {
			return candidate
		}
	}
	for _, subscribed := range node.SubscribesTo {
		subscribed = strings.TrimSpace(subscribed)
		if subscribed == "" {
			continue
		}
		for _, candidate := range candidates {
			if subscribed == candidate {
				return candidate
			}
		}
	}
	return ""
}

func workflowTimerEventDefined(bundle *WorkflowContractBundle, eventType string) bool {
	eventType = strings.TrimSpace(eventType)
	if bundle == nil || eventType == "" {
		return false
	}
	for scopedKey := range bundle.scopedEvents {
		if strings.HasSuffix(scopedKey, "::"+eventType) {
			return true
		}
	}
	if _, ok := bundle.Events[eventType]; ok {
		return true
	}
	return false
}

func appendPlatformBuiltinGuardEntries(existing []GuardActionEntry, builtins []struct {
	ID string `yaml:"id"`
}) []GuardActionEntry {
	out := append([]GuardActionEntry{}, existing...)
	seen := make(map[string]struct{}, len(out))
	for _, entry := range out {
		if id := strings.TrimSpace(entry.ID); id != "" {
			seen[id] = struct{}{}
		}
	}
	for _, builtin := range builtins {
		id := strings.TrimSpace(builtin.ID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, GuardActionEntry{
			ID:              id,
			Category:        "platform",
			PlatformBuiltin: id,
		})
	}
	return out
}

func appendPlatformBuiltinActionEntries(existing []GuardActionEntry, builtins []struct {
	ID string `yaml:"id"`
}) []GuardActionEntry {
	out := append([]GuardActionEntry{}, existing...)
	seen := make(map[string]struct{}, len(out))
	for _, entry := range out {
		if id := strings.TrimSpace(entry.ID); id != "" {
			seen[id] = struct{}{}
		}
	}
	for _, builtin := range builtins {
		id := strings.TrimSpace(builtin.ID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, GuardActionEntry{
			ID:              id,
			Category:        "platform",
			PlatformBuiltin: id,
		})
	}
	return out
}

func deriveWorkflowStagesFromFlows(paths []FlowContractPaths, schemas map[string]FlowSchemaDocument) []WorkflowStageContract {
	if len(paths) == 0 || len(schemas) == 0 {
		return nil
	}
	out := make([]WorkflowStageContract, 0)
	seen := make(map[string]struct{})
	for _, flow := range paths {
		flowID := strings.TrimSpace(flow.ID)
		schema, ok := schemas[flowID]
		if !ok {
			continue
		}
		for _, state := range schema.States {
			state = strings.TrimSpace(state)
			if state == "" {
				continue
			}
			if _, exists := seen[state]; exists {
				continue
			}
			seen[state] = struct{}{}
			out = append(out, WorkflowStageContract{
				ID:    state,
				Phase: flowID,
			})
		}
	}
	return out
}

func deriveWorkflowTerminalStagesFromFlows(paths []FlowContractPaths, schemas map[string]FlowSchemaDocument) []string {
	if len(paths) == 0 || len(schemas) == 0 {
		return nil
	}
	out := make([]string, 0)
	seen := make(map[string]struct{})
	for _, flow := range paths {
		flowID := strings.TrimSpace(flow.ID)
		schema, ok := schemas[flowID]
		if !ok {
			continue
		}
		for _, state := range schema.TerminalStates {
			state = strings.TrimSpace(state)
			if state == "" {
				continue
			}
			if _, exists := seen[state]; exists {
				continue
			}
			seen[state] = struct{}{}
			out = append(out, state)
		}
	}
	return out
}

func flowAssignedNamespace(paths []FlowContractPaths, flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return ""
	}
	for _, flow := range paths {
		if strings.TrimSpace(flow.ID) == flowID {
			return strings.TrimSpace(flow.Namespace)
		}
	}
	return ""
}

func appendIfMissingString(items []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return items
	}
	for _, item := range items {
		if strings.TrimSpace(item) == value {
			return items
		}
	}
	return append(items, value)
}

func handlerPatternMatches(pattern, eventType string) bool {
	pattern = strings.TrimSpace(pattern)
	eventType = strings.TrimSpace(eventType)
	if pattern == "" || eventType == "" {
		return false
	}
	if pattern == eventType {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return false
	}
	matched, err := path.Match(pattern, eventType)
	return err == nil && matched
}

func loadProjectContractView(paths ProjectPackagePaths, manifest ProjectPackageDocument) (ProjectContractView, error) {
	view := ProjectContractView{
		Paths:    paths,
		Manifest: manifest,
		Nodes:    map[string]SystemNodeContract{},
		Events:   map[string]EventCatalogEntry{},
		Agents:   map[string]AgentRegistryEntry{},
		Tools:    map[string]ToolSchemaEntry{},
		Policy:   PolicyDocument{Values: map[string]PolicyValue{}},
	}
	if err := loadOptionalYAMLMap(paths.ProjectNodesFile, &view.Nodes); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.ProjectEventsFile, &view.Events); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.ProjectAgentsFile, &view.Agents); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.ProjectToolsFile, &view.Tools); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.ProjectPolicyFile, &view.Policy); err != nil {
		return view, err
	}
	return view, nil
}

func loadFlowContractView(paths FlowContractPaths, schema FlowSchemaDocument) (FlowContractView, error) {
	view := FlowContractView{
		Paths:     paths,
		Schema:    schema,
		Nodes:     map[string]SystemNodeContract{},
		Events:    map[string]EventCatalogEntry{},
		Agents:    map[string]AgentRegistryEntry{},
		Tools:     map[string]ToolSchemaEntry{},
		Policy:    PolicyDocument{Values: map[string]PolicyValue{}},
		NodeURIs:  map[string]string{},
		AgentURIs: map[string]string{},
		EventURIs: map[string]string{},
		Children:  nil,
		Parent:    nil,
	}
	if err := loadOptionalYAMLMap(paths.NodesFile, &view.Nodes); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.EventsFile, &view.Events); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.AgentsFile, &view.Agents); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.ToolsFile, &view.Tools); err != nil {
		return view, err
	}
	if err := loadOptionalYAMLMap(paths.PolicyFile, &view.Policy); err != nil {
		return view, err
	}
	return view, nil
}

func buildFlowTree(bundle *WorkflowContractBundle, flowViewsByID map[string]FlowContractView) error {
	if bundle == nil {
		return nil
	}
	tree := FlowTree{
		ByPath: map[string]*FlowContractView{},
		ByID:   map[string]*FlowContractView{},
	}
	registry := ContractURIRegistry{
		Scheme: flowTreeURIScheme(bundle),
		Nodes:  map[string]ContractURIRef{},
		Agents: map[string]ContractURIRef{},
		Events: map[string]ContractURIRef{},
		ByURI:  map[string]ContractURIRef{},
	}
	if len(flowViewsByID) == 0 {
		bundle.FlowTree = tree
		bundle.URIRegistry = registry
		return nil
	}

	hasPackageNodes := false
	for _, pkg := range bundle.PackageTree {
		if _, ok := bundle.ProjectViewByKey(pkg.Key); ok {
			hasPackageNodes = true
			break
		}
	}
	if !hasPackageNodes {
		root := &FlowContractView{Children: make([]FlowContractView, 0, len(bundle.Paths.Flows))}
		for _, flow := range bundle.Paths.Flows {
			view, ok := flowViewsByID[flow.ID]
			if !ok {
				continue
			}
			root.Children = append(root.Children, view)
		}
		tree.Root = root
		flowmodel.IndexAndPopulateScopedURIs(
			tree.Root,
			&tree,
			&registry,
			func(view *FlowContractView) string { return strings.TrimSpace(view.Paths.ID) },
			flowViewChildren,
			nearestFlowTreeAncestor,
			func(view *FlowContractView, parent *FlowContractView) { view.Parent = parent },
			func(view *FlowContractView, path string) { view.Path = path },
			func(view *FlowContractView, uri string) { view.URI = uri },
			func(view *FlowContractView) string { return strings.TrimSpace(view.Paths.ID) },
			func(view *FlowContractView) string { return strings.Trim(strings.TrimSpace(view.Path), "/") },
			func(view *FlowContractView) map[string]SystemNodeContract { return view.Nodes },
			func(view *FlowContractView) map[string]AgentRegistryEntry { return view.Agents },
			func(view *FlowContractView) map[string]EventCatalogEntry { return view.Events },
			func(view *FlowContractView) *map[string]string { return &view.NodeURIs },
			func(view *FlowContractView) *map[string]string { return &view.AgentURIs },
			func(view *FlowContractView) *map[string]string { return &view.EventURIs },
		)
		if len(tree.ByPath) == 0 {
			return fmt.Errorf("flow tree build produced no indexed paths")
		}
		bundle.FlowTree = tree
		bundle.URIRegistry = registry
		return nil
	}

	rootNode, err := flowmodel.AssemblePackageTree(
		bundle.PackageTree,
		func(pkg LoadedProjectPackage) string { return strings.TrimSpace(pkg.Key) },
		func(pkg LoadedProjectPackage) string { return strings.TrimSpace(pkg.ParentKey) },
		func(pkg LoadedProjectPackage) string { return strings.TrimSpace(pkg.Paths.Dir) },
		func(pkg LoadedProjectPackage) []FlowContractPaths { return pkg.Paths.Flows },
		func(flow FlowContractPaths) string { return strings.TrimSpace(flow.ID) },
		func(flow FlowContractPaths) string { return strings.TrimSpace(flow.Flow) },
		func(flow FlowContractPaths) string { return strings.TrimSpace(flow.Dir) },
		func(pkg LoadedProjectPackage) *flowmodel.BuildNode[FlowContractView] {
			view, ok := bundle.ProjectViewByKey(pkg.Key)
			if !ok {
				return nil
			}
			return &flowmodel.BuildNode[FlowContractView]{View: flowmodel.ProjectAsFlowView[
				ProjectPackagePaths,
				ProjectPackageDocument,
				FlowContractPaths,
				FlowSchemaDocument,
				SystemNodeContract,
				EventCatalogEntry,
				AgentRegistryEntry,
				ToolSchemaEntry,
			](
				FlowContractPaths{
					PackageKey: pkg.Key,
					PackageDir: pkg.Paths.Dir,
					Dir:        pkg.Paths.Dir,
				},
				view,
			)}
		},
		func(flow FlowContractPaths) *flowmodel.BuildNode[FlowContractView] {
			view, ok := flowViewsByID[flow.ID]
			if !ok {
				return nil
			}
			return &flowmodel.BuildNode[FlowContractView]{View: view}
		},
	)
	if err != nil {
		return err
	}

	root := materializeFlowTree(rootNode)
	tree.Root = &root
	flowmodel.IndexAndPopulateScopedURIs(
		tree.Root,
		&tree,
		&registry,
		func(view *FlowContractView) string { return strings.TrimSpace(view.Paths.ID) },
		flowViewChildren,
		nearestFlowTreeAncestor,
		func(view *FlowContractView, parent *FlowContractView) { view.Parent = parent },
		func(view *FlowContractView, path string) { view.Path = path },
		func(view *FlowContractView, uri string) { view.URI = uri },
		func(view *FlowContractView) string { return strings.TrimSpace(view.Paths.ID) },
		func(view *FlowContractView) string { return strings.Trim(strings.TrimSpace(view.Path), "/") },
		func(view *FlowContractView) map[string]SystemNodeContract { return view.Nodes },
		func(view *FlowContractView) map[string]AgentRegistryEntry { return view.Agents },
		func(view *FlowContractView) map[string]EventCatalogEntry { return view.Events },
		func(view *FlowContractView) *map[string]string { return &view.NodeURIs },
		func(view *FlowContractView) *map[string]string { return &view.AgentURIs },
		func(view *FlowContractView) *map[string]string { return &view.EventURIs },
	)
	if len(tree.ByPath) == 0 {
		return fmt.Errorf("flow tree build produced no indexed paths")
	}
	bundle.FlowTree = tree
	bundle.URIRegistry = registry
	return nil
}

func materializeFlowTree(node *flowmodel.BuildNode[FlowContractView]) FlowContractView {
	return flowmodel.Materialize(
		node,
		func(view *FlowContractView, children int) {
			view.Children = make([]FlowContractView, 0, children)
			view.Parent = nil
		},
		func(view *FlowContractView, child FlowContractView) {
			view.Children = append(view.Children, child)
		},
	)
}

func flowTreeURIScheme(bundle *WorkflowContractBundle) string {
	if bundle == nil {
		return "mas"
	}
	for _, candidate := range []string{bundle.Package.Name, bundle.Semantics.Name} {
		candidate = strings.TrimSpace(candidate)
		if candidate != "" {
			return candidate
		}
	}
	return "mas"
}

func sortedContractKeys[T any](m map[string]T) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func nearestFlowTreeAncestor(node *FlowContractView) *FlowContractView {
	return flowmodel.NearestAncestor(
		node,
		func(view *FlowContractView) *FlowContractView { return view.Parent },
		func(view *FlowContractView) bool { return strings.TrimSpace(view.Paths.ID) != "" },
	)
}

func populateMergedPackageViews(bundle *WorkflowContractBundle, flowViewsByID map[string]FlowContractView) error {
	for _, view := range bundle.RootProjectViews() {
		pkgKey := strings.TrimSpace(view.Paths.Key)
		if pkgKey == "" {
			continue
		}
		if err := mergeNodeContracts(bundle, view.Nodes, ContractItemSource{PackageKey: pkgKey, Layer: "project", File: view.Paths.ProjectNodesFile}); err != nil {
			return err
		}
		if err := mergeEventContracts(bundle, view.Events, ContractItemSource{PackageKey: pkgKey, Layer: "project", File: view.Paths.ProjectEventsFile}); err != nil {
			return err
		}
		if err := mergeAgentContracts(bundle, view.Agents, ContractItemSource{PackageKey: pkgKey, Layer: "project", File: view.Paths.ProjectAgentsFile}); err != nil {
			return err
		}
		if err := mergeToolContracts(bundle, view.Tools, ContractItemSource{PackageKey: pkgKey, Layer: "project", File: view.Paths.ProjectToolsFile}); err != nil {
			return err
		}
	}
	for _, flow := range bundle.Paths.Flows {
		view, ok := flowViewsByID[flow.ID]
		if !ok {
			continue
		}
		sourcePrefix := ContractItemSource{PackageKey: flow.PackageKey, FlowID: flow.ID, Layer: "flow"}
		if err := mergeNodeContracts(bundle, view.Nodes, contractSourceWithFile(sourcePrefix, view.Paths.NodesFile)); err != nil {
			return err
		}
		if err := mergeEventContracts(bundle, view.Events, contractSourceWithFile(sourcePrefix, view.Paths.EventsFile)); err != nil {
			return err
		}
		if err := mergeAgentContracts(bundle, view.Agents, contractSourceWithFile(sourcePrefix, view.Paths.AgentsFile)); err != nil {
			return err
		}
		if err := mergeToolContracts(bundle, view.Tools, contractSourceWithFile(sourcePrefix, view.Paths.ToolsFile)); err != nil {
			return err
		}
	}
	return nil
}

func mergeNodeContracts(bundle *WorkflowContractBundle, entries map[string]SystemNodeContract, source ContractItemSource) error {
	for id, entry := range entries {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		scopedKey := contractScopeKey(source, key)
		if existing, ok := bundle.scopedNodeSources[scopedKey]; ok {
			if reflect.DeepEqual(bundle.scopedNodes[scopedKey], entry) {
				continue
			}
			return fmt.Errorf("duplicate scoped node id %q from %s and %s", scopedKey, existing.File, source.File)
		}
		bundle.scopedNodes[scopedKey] = entry
		bundle.scopedNodeSources[scopedKey] = source
		if _, ambiguous := bundle.ambiguousNodeAliases[key]; ambiguous {
			continue
		}
		if existing, ok := bundle.nodeSources[key]; ok {
			if contractSameScope(existing, source) {
				if reflect.DeepEqual(bundle.Nodes[key], entry) {
					continue
				}
				return fmt.Errorf("duplicate merged node id %q from %s and %s", key, existing.File, source.File)
			}
			delete(bundle.Nodes, key)
			delete(bundle.nodeSources, key)
			bundle.ambiguousNodeAliases[key] = struct{}{}
			continue
		}
		bundle.Nodes[key] = entry
		bundle.nodeSources[key] = source
	}
	return nil
}

func mergeEventContracts(bundle *WorkflowContractBundle, entries map[string]EventCatalogEntry, source ContractItemSource) error {
	for id, entry := range entries {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		scopedKey := contractScopeKey(source, key)
		if existing, ok := bundle.scopedEventSources[scopedKey]; ok {
			if reflect.DeepEqual(bundle.scopedEvents[scopedKey], entry) {
				continue
			}
			return fmt.Errorf("duplicate scoped event id %q from %s and %s", scopedKey, existing.File, source.File)
		}
		bundle.scopedEvents[scopedKey] = entry
		bundle.scopedEventSources[scopedKey] = source
		if _, ambiguous := bundle.ambiguousEventAliases[key]; ambiguous {
			continue
		}
		if existing, ok := bundle.eventSources[key]; ok {
			if contractSameScope(existing, source) {
				if reflect.DeepEqual(bundle.Events[key], entry) {
					continue
				}
				merged, ok := mergeEventCatalogEntry(bundle.Events[key], entry)
				if !ok {
					return fmt.Errorf("duplicate merged event id %q from %s and %s", key, existing.File, source.File)
				}
				bundle.Events[key] = merged
				bundle.eventSources[key] = source
				continue
			}
			delete(bundle.Events, key)
			delete(bundle.eventSources, key)
			bundle.ambiguousEventAliases[key] = struct{}{}
			continue
		}
		bundle.Events[key] = entry
		bundle.eventSources[key] = source
	}
	return nil
}

func mergeEventCatalogEntry(existing EventCatalogEntry, incoming EventCatalogEntry) (EventCatalogEntry, bool) {
	merged := existing
	var ok bool
	if merged.Emitter, ok = mergeEventEmitterRef(existing.Emitter, incoming.Emitter); !ok {
		return EventCatalogEntry{}, false
	}
	if merged.EmitterType, ok = mergeStringValue(existing.EmitterType, incoming.EmitterType); !ok {
		return EventCatalogEntry{}, false
	}
	merged.AlternateEmitters = mergeStringLists(existing.AlternateEmitters, incoming.AlternateEmitters)
	if merged.Consumer, ok = mergeStringSliceValue(existing.Consumer, incoming.Consumer); !ok {
		return EventCatalogEntry{}, false
	}
	if merged.ConsumerType, ok = mergeStringSliceValue(existing.ConsumerType, incoming.ConsumerType); !ok {
		return EventCatalogEntry{}, false
	}
	if merged.Intercepted, ok = mergeBoolValue(existing.Intercepted, incoming.Intercepted); !ok {
		return EventCatalogEntry{}, false
	}
	if merged.Passthrough, ok = mergeBoolValue(existing.Passthrough, incoming.Passthrough); !ok {
		return EventCatalogEntry{}, false
	}
	if merged.RuntimeHandling, ok = mergeStringValue(existing.RuntimeHandling, incoming.RuntimeHandling); !ok {
		return EventCatalogEntry{}, false
	}
	if merged.OwningNode, ok = mergeStringValue(existing.OwningNode, incoming.OwningNode); !ok {
		return EventCatalogEntry{}, false
	}
	if merged.DeliveryChannel, ok = mergeStringValue(existing.DeliveryChannel, incoming.DeliveryChannel); !ok {
		return EventCatalogEntry{}, false
	}
	if merged.Payload, ok = mergeEventPayloadSpec(existing.Payload, incoming.Payload); !ok {
		return EventCatalogEntry{}, false
	}
	merged.Required = mergeStringLists(existing.Required, incoming.Required)
	return merged, true
}

func mergeStringValue(existing, incoming string) (string, bool) {
	existing = strings.TrimSpace(existing)
	incoming = strings.TrimSpace(incoming)
	switch {
	case existing == "":
		return incoming, true
	case incoming == "":
		return existing, true
	case existing == incoming:
		return existing, true
	default:
		return "", false
	}
}

func mergeStringLists(existing, incoming []string) []string {
	return normalizeStrings(append(append([]string{}, existing...), incoming...))
}

func mergeStringSliceValue(existing, incoming []string) ([]string, bool) {
	switch {
	case len(existing) == 0:
		return append([]string{}, incoming...), true
	case len(incoming) == 0:
		return append([]string{}, existing...), true
	default:
		return mergeStringLists(existing, incoming), true
	}
}

func mergeBoolValue(existing, incoming bool) (bool, bool) {
	if !existing {
		return incoming, true
	}
	if !incoming {
		return existing, true
	}
	return existing == incoming, existing == incoming
}

func mergeEventEmitterRef(existing, incoming EventEmitterRef) (EventEmitterRef, bool) {
	switch {
	case isEmptyEventEmitterRef(existing):
		return incoming, true
	case isEmptyEventEmitterRef(incoming):
		return existing, true
	case reflect.DeepEqual(existing, incoming):
		return existing, true
	default:
		return EventEmitterRef{}, false
	}
}

func mergeEventPayloadSpec(existing, incoming EventPayloadSpec) (EventPayloadSpec, bool) {
	switch {
	case isEmptyEventPayloadSpec(existing):
		return incoming, true
	case isEmptyEventPayloadSpec(incoming):
		return existing, true
	case reflect.DeepEqual(existing, incoming):
		return existing, true
	default:
		return EventPayloadSpec{}, false
	}
}

func isEmptyEventEmitterRef(ref EventEmitterRef) bool {
	return strings.TrimSpace(ref.AgentID) == "" && strings.TrimSpace(ref.NodeID) == ""
}

func isEmptyEventPayloadSpec(spec EventPayloadSpec) bool {
	return strings.TrimSpace(spec.Type) == "" && len(spec.Properties) == 0 && len(spec.Required) == 0
}

func hasYAMLMappingKey(node *yaml.Node, key string) bool {
	if node == nil || node.Kind != yaml.MappingNode {
		return false
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if strings.TrimSpace(node.Content[i].Value) == key {
			return true
		}
	}
	return false
}

func decodeStringListNode(node *yaml.Node) ([]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			return nil, nil
		}
		return []string{strings.TrimSpace(node.Value)}, nil
	case yaml.SequenceNode:
		var values []string
		if err := node.Decode(&values); err != nil {
			return nil, err
		}
		return normalizeStrings(values), nil
	default:
		return nil, fmt.Errorf("unsupported string list yaml node kind %d", node.Kind)
	}
}

func decodeScalarStringNode(node *yaml.Node) (string, error) {
	if node == nil || node.Kind == 0 {
		return "", nil
	}
	if node.Kind != yaml.ScalarNode {
		return "", fmt.Errorf("unsupported scalar string yaml node kind %d", node.Kind)
	}
	if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		return "", nil
	}
	return strings.TrimSpace(node.Value), nil
}

func decodeBoolNode(node *yaml.Node) (bool, error) {
	if node == nil || node.Kind == 0 {
		return false, nil
	}
	if node.Kind != yaml.ScalarNode {
		return false, fmt.Errorf("unsupported bool yaml node kind %d", node.Kind)
	}
	if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
		return false, nil
	}
	var value bool
	if err := node.Decode(&value); err == nil {
		return value, nil
	}
	switch strings.ToLower(strings.TrimSpace(node.Value)) {
	case "true", "yes", "on", "conditional":
		return true, nil
	case "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("unsupported bool value %q", node.Value)
	}
}

func decodeGuardSpecNode(node *yaml.Node) (*GuardSpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	if node.Kind == yaml.ScalarNode && !strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") && strings.TrimSpace(node.Value) != "" {
		return nil, fmt.Errorf("DIALECT-GUARD: guard is string, must be {id, check}")
	}
	var spec GuardSpec
	if err := node.Decode(&spec); err != nil {
		return nil, err
	}
	if strings.TrimSpace(spec.ID) == "" && strings.TrimSpace(spec.Check) == "" && len(spec.Checks) == 0 && strings.TrimSpace(spec.OnFail) == "" && strings.TrimSpace(spec.PolicyRef) == "" {
		return nil, nil
	}
	return &spec, nil
}

func decodeAdvancesToNode(node *yaml.Node) (string, error) {
	if node == nil || node.Kind == 0 {
		return "", nil
	}
	if node.Kind == yaml.SequenceNode {
		return "", fmt.Errorf("DIALECT-ADV-LIST: advances_to is list, must be string")
	}
	return decodeScalarStringNode(node)
}

func validateHandlerFieldNodes(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	allowed := map[string]struct{}{
		"action":            {},
		"description":       {},
		"_note":             {},
		"emits":             {},
		"guard":             {},
		"advances_to":       {},
		"sets_gate":         {},
		"clear_gates":       {},
		"data_accumulation": {},
		"condition":         {},
		"completion_rule":   {},
		"logic":             {},
		"policy_ref":        {},
		"on_complete":       {},
		"rules":             {},
		"accumulate":        {},
		"compute":           {},
		"query":             {},
		"fan_out":           {},
		"filter":            {},
		"reduce":            {},
		"count":             {},
		"clear":             {},
		"template":          {},
		"instance_id_from":  {},
		"config_from":       {},
		"from":              {},
		"payload_transform": {},
		"branch":            {},
		"dedup_by":          {},
	}
	deprecated := map[string]struct{}{
		"condition":          {},
		"logic":              {},
		"on_below_threshold": {},
		"on_dedup":           {},
		"on_pass":            {},
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := deprecated[key]; ok {
			return fmt.Errorf("DEPRECATED: handler uses deprecated field %q", key)
		}
		if key == "on_complete" && node.Content[i+1].Kind == yaml.MappingNode {
			return fmt.Errorf("DIALECT-OC-ORDER: on_complete is dict, must be ordered list")
		}
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("UNDEFINED-FIELD: handler field %q not in platform spec", key)
		}
	}
	return nil
}

func decodeGateSpecNode(node *yaml.Node) (*GateSpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	var spec GateSpec
	if err := node.Decode(&spec); err != nil {
		return nil, err
	}
	if strings.TrimSpace(spec.Name) == "" && spec.Value == nil {
		return nil, nil
	}
	return &spec, nil
}

func decodeClearGatesNode(node *yaml.Node) ([]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			return nil, nil
		}
		var all bool
		if err := node.Decode(&all); err == nil {
			if all {
				return []string{"*"}, nil
			}
			return nil, nil
		}
		return []string{strings.TrimSpace(node.Value)}, nil
	case yaml.SequenceNode:
		return decodeStringListNode(node)
	default:
		return nil, fmt.Errorf("unsupported clear_gates yaml node kind %d", node.Kind)
	}
}

func decodeHandlerRuleEntryNode(node *yaml.Node) (*HandlerRuleEntry, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	var rule HandlerRuleEntry
	if err := node.Decode(&rule); err != nil {
		return nil, err
	}
	if strings.TrimSpace(rule.ID) == "" && strings.TrimSpace(rule.Description) == "" && strings.TrimSpace(rule.Condition) == "" && strings.TrimSpace(rule.AdvancesTo) == "" && rule.Emits.Empty() && !rule.DataAccumulation.HasWrites() && rule.DataAccumulation.Value.IsZero() {
		return nil, nil
	}
	return &rule, nil
}

func decodeHandlerRuleEntriesNode(node *yaml.Node) ([]HandlerRuleEntry, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.SequenceNode:
		var rules []HandlerRuleEntry
		if err := node.Decode(&rules); err != nil {
			return nil, err
		}
		return rules, nil
	case yaml.MappingNode:
		if hasAnyYAMLMappingKey(node, "condition", "advances_to", "emits", "data_accumulation") {
			rule, err := decodeHandlerRuleEntryNode(node)
			if err != nil || rule == nil {
				return nil, err
			}
			return []HandlerRuleEntry{*rule}, nil
		}
		rules := make([]HandlerRuleEntry, 0, len(node.Content)/2)
		for i := 0; i+1 < len(node.Content); i += 2 {
			id := strings.TrimSpace(node.Content[i].Value)
			if id == "" {
				continue
			}
			var rule HandlerRuleEntry
			if err := node.Content[i+1].Decode(&rule); err != nil {
				return nil, err
			}
			if strings.TrimSpace(rule.ID) == "" {
				rule.ID = id
			}
			rules = append(rules, rule)
		}
		return rules, nil
	default:
		return nil, fmt.Errorf("unsupported rules yaml node kind %d", node.Kind)
	}
}

func decodeQuerySpecNode(node *yaml.Node) (*QuerySpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.MappingNode:
		var spec QuerySpec
		if err := node.Decode(&spec); err != nil {
			return nil, err
		}
		spec.hydratePaths()
		return &spec, nil
	case yaml.SequenceNode:
		var queries []QuerySpec
		if err := node.Decode(&queries); err != nil {
			return nil, err
		}
		for i := range queries {
			queries[i].hydratePaths()
		}
		return &QuerySpec{Queries: queries}, nil
	default:
		return nil, fmt.Errorf("unsupported query yaml node kind %d", node.Kind)
	}
}

func decodeActionSpecNode(node *yaml.Node) (ActionSpec, error) {
	if node == nil || node.Kind == 0 {
		return ActionSpec{}, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			return ActionSpec{}, nil
		}
		return ActionSpec{ID: strings.TrimSpace(node.Value)}, nil
	case yaml.MappingNode:
		var aux struct {
			ID             string    `yaml:"id"`
			Template       string    `yaml:"template"`
			InstanceIDFrom string    `yaml:"instance_id_from"`
			ConfigFrom     yaml.Node `yaml:"config_from"`
		}
		if err := node.Decode(&aux); err != nil {
			return ActionSpec{}, err
		}
		configFrom, err := decodeConfigFromSpecNode(&aux.ConfigFrom)
		if err != nil {
			return ActionSpec{}, err
		}
		return ActionSpec{
			ID:             strings.TrimSpace(aux.ID),
			Template:       strings.TrimSpace(aux.Template),
			InstanceIDFrom: strings.TrimSpace(aux.InstanceIDFrom),
			InstanceIDPath: paths.Parse(aux.InstanceIDFrom),
			ConfigFrom:     configFrom,
		}, nil
	default:
		return ActionSpec{}, fmt.Errorf("unsupported action yaml node kind %d", node.Kind)
	}
}

func decodeClearSpecNode(node *yaml.Node) (*ClearSpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	var spec ClearSpec
	if err := node.Decode(&spec); err != nil {
		return nil, err
	}
	if strings.TrimSpace(spec.Target) == "" && len(spec.Targets) == 0 {
		return nil, nil
	}
	return &spec, nil
}

func decodeConfigFromSpecNode(node *yaml.Node) (*ConfigFromSpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("unsupported config_from yaml node kind %d", node.Kind)
	}
	spec := &ConfigFromSpec{Bindings: map[string]string{}}
	if hasYAMLMappingKey(node, "policy_keys") {
		type alias ConfigFromSpec
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return nil, err
		}
		spec.PolicyKeys = normalizeStrings(aux.PolicyKeys)
		for key, value := range aux.Bindings {
			spec.Bindings[key] = value
		}
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" || key == "policy_keys" {
			continue
		}
		spec.Bindings[key] = strings.TrimSpace(node.Content[i+1].Value)
	}
	if len(spec.PolicyKeys) == 0 && len(spec.Bindings) == 0 {
		return nil, nil
	}
	spec.Entries = spec.ConfigEntries()
	return spec, nil
}

func (s ConfigFromSpec) ConfigEntries() []ConfigBinding {
	out := make([]ConfigBinding, 0, len(s.Bindings))
	for key, value := range s.Bindings {
		cleanKey := strings.TrimSpace(key)
		cleanValue := strings.TrimSpace(value)
		if cleanKey == "" || cleanValue == "" {
			continue
		}
		out = append(out, ConfigBinding{
			Key:     cleanKey,
			Ref:     cleanValue,
			RefPath: paths.Parse(cleanValue),
		})
	}
	return out
}

func decodeBranchSpecsNode(node *yaml.Node) ([]BranchSpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	var specs []BranchSpec
	if err := node.Decode(&specs); err != nil {
		return nil, err
	}
	return specs, nil
}

func decodeEventEmitterNode(node *yaml.Node) (EventEmitterRef, []string, error) {
	if node == nil || node.Kind == 0 {
		return EventEmitterRef{}, nil, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		value := strings.TrimSpace(node.Value)
		if value == "" {
			return EventEmitterRef{}, nil, nil
		}
		return EventEmitterRef{AgentID: value}, nil, nil
	case yaml.SequenceNode:
		values, err := decodeStringListNode(node)
		if err != nil || len(values) == 0 {
			return EventEmitterRef{}, nil, err
		}
		return EventEmitterRef{AgentID: values[0]}, values[1:], nil
	case yaml.MappingNode:
		var ref EventEmitterRef
		if err := node.Decode(&ref); err != nil {
			return EventEmitterRef{}, nil, err
		}
		return ref, nil, nil
	default:
		return EventEmitterRef{}, nil, fmt.Errorf("unsupported emitter yaml node kind %d", node.Kind)
	}
}

func decodeEventPayloadSpecNode(node *yaml.Node) (EventPayloadSpec, error) {
	if node == nil || node.Kind == 0 {
		return EventPayloadSpec{}, nil
	}
	var spec EventPayloadSpec
	if err := node.Decode(&spec); err != nil {
		return EventPayloadSpec{}, err
	}
	return spec, nil
}

func hasAnyYAMLMappingKey(node *yaml.Node, keys ...string) bool {
	for _, key := range keys {
		if hasYAMLMappingKey(node, key) {
			return true
		}
	}
	return false
}

func looksLikeEntitySchemaFieldMap(node *yaml.Node) bool {
	if node == nil || node.Kind != yaml.MappingNode {
		return false
	}
	if len(node.Content) == 0 {
		return true
	}
	for i := 1; i < len(node.Content); i += 2 {
		value := node.Content[i]
		switch value.Kind {
		case yaml.ScalarNode:
			continue
		case yaml.MappingNode:
			if !hasAnyYAMLMappingKey(value, "type", "primary", "indexed", "nullable") {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func decodeEntitySchemaFields(node *yaml.Node) ([]EntitySchemaField, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.SequenceNode:
		var fields []EntitySchemaField
		if err := node.Decode(&fields); err != nil {
			return nil, err
		}
		return fields, nil
	case yaml.MappingNode:
		fields := make([]EntitySchemaField, 0, len(node.Content)/2)
		for i := 0; i+1 < len(node.Content); i += 2 {
			name := strings.TrimSpace(node.Content[i].Value)
			if name == "" {
				continue
			}
			field, err := decodeEntitySchemaField(name, node.Content[i+1])
			if err != nil {
				return nil, err
			}
			fields = append(fields, field)
		}
		return fields, nil
	default:
		return nil, fmt.Errorf("unsupported entity schema fields yaml node kind %d", node.Kind)
	}
}

func decodeEntitySchemaField(name string, node *yaml.Node) (EntitySchemaField, error) {
	field := EntitySchemaField{Name: strings.TrimSpace(name)}
	switch node.Kind {
	case yaml.ScalarNode:
		parsed := parseTypedFieldString(node.Value)
		field.Type = parsed.Type
		field.Primary = parsed.Primary
		field.Indexed = parsed.Indexed
		field.Nullable = parsed.Nullable
		return field, nil
	case yaml.MappingNode:
		type alias EntitySchemaField
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return EntitySchemaField{}, err
		}
		field.Type = aux.Type
		field.Primary = aux.Primary
		field.Indexed = aux.Indexed
		field.Nullable = aux.Nullable
		return field, nil
	default:
		return EntitySchemaField{}, fmt.Errorf("unsupported entity schema field yaml node kind %d", node.Kind)
	}
}

func decodeNodeStateFields(node *yaml.Node) ([]NodeStateField, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.SequenceNode:
		var fields []NodeStateField
		if err := node.Decode(&fields); err != nil {
			return nil, err
		}
		return fields, nil
	case yaml.MappingNode:
		fields := make([]NodeStateField, 0, len(node.Content)/2)
		for i := 0; i+1 < len(node.Content); i += 2 {
			name := strings.TrimSpace(node.Content[i].Value)
			if name == "" {
				continue
			}
			field, err := decodeNodeStateField(name, node.Content[i+1])
			if err != nil {
				return nil, err
			}
			fields = append(fields, field)
		}
		return fields, nil
	default:
		return nil, fmt.Errorf("unsupported node state fields yaml node kind %d", node.Kind)
	}
}

func decodeNodeStateField(name string, node *yaml.Node) (NodeStateField, error) {
	field := NodeStateField{Name: strings.TrimSpace(name)}
	switch node.Kind {
	case yaml.ScalarNode:
		parsed := parseTypedFieldString(node.Value)
		field.Type = parsed.Type
		field.Default = parsed.Default
		return field, nil
	case yaml.MappingNode:
		type alias NodeStateField
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return NodeStateField{}, err
		}
		field.Type = aux.Type
		field.Default = aux.Default
		return field, nil
	default:
		return NodeStateField{}, fmt.Errorf("unsupported node state field yaml node kind %d", node.Kind)
	}
}

type parsedTypedField struct {
	Type     string
	Primary  bool
	Indexed  bool
	Nullable bool
	Default  any
}

func parseTypedFieldString(value string) parsedTypedField {
	value = strings.TrimSpace(value)
	if value == "" {
		return parsedTypedField{}
	}
	out := parsedTypedField{Type: value}
	lower := strings.ToLower(value)
	if idx := strings.Index(lower, " default "); idx >= 0 {
		out.Type = strings.TrimSpace(value[:idx])
		out.Default = strings.TrimSpace(value[idx+len(" default "):])
		lower = strings.ToLower(out.Type)
	}
	if strings.Contains(lower, "primary key") {
		out.Primary = true
		out.Type = strings.TrimSpace(strings.ReplaceAll(strings.ToLower(out.Type), "(primary key)", ""))
	}
	if strings.Contains(lower, "nullable") || strings.Contains(lower, "null until") {
		out.Nullable = true
	}
	if strings.Contains(lower, "indexed") {
		out.Indexed = true
	}
	out.Type = strings.TrimSpace(strings.TrimSuffix(out.Type, ","))
	return out
}

func mergeAgentContracts(bundle *WorkflowContractBundle, entries map[string]AgentRegistryEntry, source ContractItemSource) error {
	for id, entry := range entries {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		scopedKey := contractScopeKey(source, key)
		if existing, ok := bundle.scopedAgentSources[scopedKey]; ok {
			if reflect.DeepEqual(bundle.scopedAgents[scopedKey], entry) {
				continue
			}
			return fmt.Errorf("duplicate scoped agent id %q from %s and %s", scopedKey, existing.File, source.File)
		}
		bundle.scopedAgents[scopedKey] = entry
		bundle.scopedAgentSources[scopedKey] = source
		if _, ambiguous := bundle.ambiguousAgentAliases[key]; ambiguous {
			continue
		}
		if existing, ok := bundle.agentSources[key]; ok {
			if contractSameScope(existing, source) {
				if reflect.DeepEqual(bundle.Agents[key], entry) {
					continue
				}
				return fmt.Errorf("duplicate merged agent id %q from %s and %s", key, existing.File, source.File)
			}
			delete(bundle.Agents, key)
			delete(bundle.agentSources, key)
			bundle.ambiguousAgentAliases[key] = struct{}{}
			continue
		}
		bundle.Agents[key] = entry
		bundle.agentSources[key] = source
	}
	return nil
}

func mergeToolContracts(bundle *WorkflowContractBundle, entries map[string]ToolSchemaEntry, source ContractItemSource) error {
	for id, entry := range entries {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		scopedKey := contractScopeKey(source, key)
		if existing, ok := bundle.scopedToolSources[scopedKey]; ok {
			if reflect.DeepEqual(bundle.scopedTools[scopedKey], entry) {
				continue
			}
			return fmt.Errorf("duplicate scoped tool id %q from %s and %s", scopedKey, existing.File, source.File)
		}
		bundle.scopedTools[scopedKey] = entry
		bundle.scopedToolSources[scopedKey] = source
		if _, ambiguous := bundle.ambiguousToolAliases[key]; ambiguous {
			continue
		}
		if existing, ok := bundle.toolSources[key]; ok {
			if contractSameScope(existing, source) {
				if reflect.DeepEqual(bundle.Tools[key], entry) {
					continue
				}
				return fmt.Errorf("duplicate merged tool id %q from %s and %s", key, existing.File, source.File)
			}
			delete(bundle.Tools, key)
			delete(bundle.toolSources, key)
			bundle.ambiguousToolAliases[key] = struct{}{}
			continue
		}
		bundle.Tools[key] = entry
		bundle.toolSources[key] = source
	}
	return nil
}

func contractSourceWithFile(source ContractItemSource, file string) ContractItemSource {
	source.File = file
	return source
}

func (b *WorkflowContractBundle) TransitionIDsByOwner() map[string][]string {
	out := map[string][]string{}
	if b == nil {
		return out
	}
	for _, transition := range b.WorkflowTransitions() {
		owner := strings.TrimSpace(transition.Node)
		if owner == "" {
			continue
		}
		out[owner] = append(out[owner], strings.TrimSpace(transition.ID))
	}
	for owner := range out {
		sort.Strings(out[owner])
	}
	return out
}

func ContractFilesExist(repoRoot string) []string {
	paths := ResolveWorkflowContractPaths(repoRoot)
	files := []string{
		paths.PlatformSpecFile,
		paths.VerificationGatesFile,
		paths.ToolingLockFile,
		paths.DDLFile,
		paths.RootSchemaFile,
	}
	if paths.ProjectPackageFile != "" {
		files = append(files,
			paths.ProjectPackageFile,
			paths.ProjectNodesFile,
			paths.ProjectEventsFile,
			paths.ProjectAgentsFile,
		)
		for _, pkg := range paths.ProjectPackages {
			files = append(files,
				pkg.PackageFile,
				pkg.ProjectNodesFile,
				pkg.ProjectEventsFile,
				pkg.ProjectAgentsFile,
				pkg.ProjectToolsFile,
				pkg.ProjectPolicyFile,
			)
		}
		for _, flow := range paths.Flows {
			files = append(files, flow.SchemaFile, flow.NodesFile, flow.EventsFile, flow.AgentsFile)
		}
	}
	missing := make([]string, 0)
	for _, path := range files {
		if strings.TrimSpace(path) == "" {
			continue
		}
		if _, err := os.Stat(path); err != nil {
			missing = append(missing, path)
		}
	}
	sort.Strings(missing)
	return missing
}

func existingFile(path string) string {
	if path == "" {
		return ""
	}
	if stat, err := os.Stat(path); err == nil && !stat.IsDir() {
		return path
	}
	return ""
}

func existingDir(path string) string {
	if path == "" {
		return ""
	}
	if stat, err := os.Stat(path); err == nil && stat.IsDir() {
		return path
	}
	return ""
}

func (p ProjectPackageDocument) ChildPackages() []ProjectPackageRef {
	out := make([]ProjectPackageRef, 0, len(p.Packages)+len(p.Children)+len(p.Subpackages))
	out = append(out, p.Packages...)
	out = append(out, p.Children...)
	out = append(out, p.Subpackages...)
	return out
}

func (p ProjectPackageRef) ResolveLocation() string {
	for _, candidate := range []string{p.Path, p.Package, p.Dir} {
		if resolved := strings.TrimSpace(candidate); resolved != "" {
			return resolved
		}
	}
	return ""
}

func discoverProjectPackagePaths(packageFile, workflowDir string) []ProjectPackagePaths {
	rootFile := existingFile(packageFile)
	rootDir := filepath.Dir(rootFile)
	if strings.TrimSpace(rootFile) == "" || strings.TrimSpace(workflowDir) == "" {
		return nil
	}
	visited := map[string]bool{}
	var out []ProjectPackagePaths
	var walk func(packageFile, parentKey string, depth int)
	walk = func(packageFile, parentKey string, depth int) {
		packageFile = existingFile(packageFile)
		if packageFile == "" || visited[packageFile] {
			return
		}
		visited[packageFile] = true

		var manifest ProjectPackageDocument
		if err := loadYAMLFile(packageFile, &manifest); err != nil {
			return
		}

		packageDir := filepath.Dir(packageFile)
		key := "."
		if rel, err := filepath.Rel(rootDir, packageDir); err == nil && strings.TrimSpace(rel) != "" {
			key = filepath.Clean(rel)
		}
		pkg := ProjectPackagePaths{
			Key:               key,
			ParentKey:         parentKey,
			Depth:             depth,
			Dir:               packageDir,
			PackageFile:       packageFile,
			ProjectNodesFile:  existingFile(filepath.Join(packageDir, "nodes.yaml")),
			ProjectEventsFile: existingFile(filepath.Join(packageDir, "events.yaml")),
			ProjectAgentsFile: existingFile(filepath.Join(packageDir, "agents.yaml")),
			ProjectToolsFile:  existingFile(filepath.Join(packageDir, "tools.yaml")),
			ProjectPolicyFile: existingFile(filepath.Join(packageDir, "policy.yaml")),
			ProjectPromptsDir: existingDir(filepath.Join(packageDir, "prompts")),
		}
		for _, flow := range manifest.Flows {
			flowDirName := strings.TrimSpace(flow.Flow)
			if flowDirName == "" {
				continue
			}
			dir := filepath.Join(packageDir, "flows", flowDirName)
			pkg.Flows = append(pkg.Flows, FlowContractPaths{
				ID:         strings.TrimSpace(flow.ID),
				Flow:       flowDirName,
				Mode:       strings.TrimSpace(flow.Mode),
				Namespace:  strings.TrimSpace(flow.Namespace),
				PackageKey: pkg.Key,
				PackageDir: packageDir,
				Dir:        dir,
				SchemaFile: existingFile(filepath.Join(dir, "schema.yaml")),
				NodesFile:  existingFile(filepath.Join(dir, "nodes.yaml")),
				EventsFile: existingFile(filepath.Join(dir, "events.yaml")),
				AgentsFile: existingFile(filepath.Join(dir, "agents.yaml")),
				ToolsFile:  existingFile(filepath.Join(dir, "tools.yaml")),
				PolicyFile: existingFile(filepath.Join(dir, "policy.yaml")),
				PromptsDir: existingDir(filepath.Join(dir, "prompts")),
			})
		}
		sort.Slice(pkg.Flows, func(i, j int) bool {
			return strings.Compare(pkg.Flows[i].ID, pkg.Flows[j].ID) < 0
		})
		out = append(out, pkg)

		var flowPackageFiles []string
		for _, flow := range pkg.Flows {
			if flowPackage := existingFile(filepath.Join(flow.Dir, "package.yaml")); flowPackage != "" {
				flowPackageFiles = append(flowPackageFiles, flowPackage)
			}
		}
		sort.Strings(flowPackageFiles)
		for _, flowPackage := range flowPackageFiles {
			walk(flowPackage, pkg.Key, depth+1)
		}

		for _, child := range manifest.ChildPackages() {
			location := child.ResolveLocation()
			if strings.TrimSpace(location) == "" {
				continue
			}
			childPath := filepath.Join(packageDir, location)
			if strings.HasSuffix(strings.ToLower(location), ".yaml") {
				walk(childPath, pkg.Key, depth+1)
				continue
			}
			walk(filepath.Join(childPath, "package.yaml"), pkg.Key, depth+1)
		}
	}
	walk(rootFile, "", 0)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Depth == out[j].Depth {
			return strings.Compare(out[i].Key, out[j].Key) < 0
		}
		return out[i].Depth < out[j].Depth
	})
	return out
}

func validateDiscoveredPackageTree(pkgs []LoadedProjectPackage) error {
	seenPackages := map[string]struct{}{}
	seenFlows := map[string]string{}
	for _, pkg := range pkgs {
		if pkg.Depth > maxDiscoveredPackageDepth {
			return fmt.Errorf("package tree depth %d exceeds max depth %d at %s", pkg.Depth, maxDiscoveredPackageDepth, pkg.Key)
		}
		if _, exists := seenPackages[pkg.Key]; exists {
			return fmt.Errorf("duplicate package key %q discovered in package tree", pkg.Key)
		}
		seenPackages[pkg.Key] = struct{}{}
		for _, flow := range pkg.Paths.Flows {
			flowID := strings.TrimSpace(flow.ID)
			if flowID == "" {
				continue
			}
			if existing, exists := seenFlows[flowID]; exists {
				return fmt.Errorf("duplicate flow id %q discovered in package tree (%s, %s)", flowID, existing, pkg.Key)
			}
			seenFlows[flowID] = pkg.Key
		}
	}
	return nil
}

func cloneSystemNodeContractMap(in map[string]SystemNodeContract) map[string]SystemNodeContract {
	if len(in) == 0 {
		return map[string]SystemNodeContract{}
	}
	out := make(map[string]SystemNodeContract, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneEventCatalogEntryMap(in map[string]EventCatalogEntry) map[string]EventCatalogEntry {
	if len(in) == 0 {
		return map[string]EventCatalogEntry{}
	}
	out := make(map[string]EventCatalogEntry, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneAgentRegistryEntryMap(in map[string]AgentRegistryEntry) map[string]AgentRegistryEntry {
	if len(in) == 0 {
		return map[string]AgentRegistryEntry{}
	}
	out := make(map[string]AgentRegistryEntry, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneToolSchemaEntryMap(in map[string]ToolSchemaEntry) map[string]ToolSchemaEntry {
	if len(in) == 0 {
		return map[string]ToolSchemaEntry{}
	}
	out := make(map[string]ToolSchemaEntry, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func normalizeStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, item := range in {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func loadYAMLFile(path string, target any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := yaml.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}

func loadOptionalYAMLMap(path string, target any) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	return loadYAMLFile(path, target)
}
