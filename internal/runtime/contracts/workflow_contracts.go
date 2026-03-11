package contracts

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type ContractPaths struct {
	ContractsRoot         string
	WorkflowDir           string
	ProjectPackageFile    string
	ProjectPackages       []ProjectPackagePaths
	ProjectNodesFile      string
	ProjectEventsFile     string
	ProjectAgentsFile     string
	ProjectToolsFile      string
	ProjectPolicyFile     string
	ProjectPromptsDir     string
	WorkflowSchemaFile    string
	GuardRegistryFile     string
	SystemNodesFile       string
	EventCatalogFile      string
	AgentRegistryFile     string
	ToolSchemasFile       string
	PolicyFile            string
	PromptsDir            string
	PlatformSpecFile      string
	VerificationGatesFile string
	ToolingLockFile       string
	DDLFile               string
	AgentConfigMapFile    string
	RuntimeBridge         RuntimeBridgePaths
	Flows                 []FlowContractPaths
}

type WorkflowContractBundle struct {
	Paths                 ContractPaths
	Package               ProjectPackageDocument
	PackageTree           []LoadedProjectPackage
	ProjectContracts      map[string]ProjectContractView
	FlowContracts         map[string]FlowContractView
	ScopedNodes           map[string]SystemNodeContract
	ScopedEvents          map[string]EventCatalogEntry
	ScopedAgents          map[string]AgentRegistryEntry
	ScopedTools           map[string]ToolSchemaEntry
	MergedNodes           map[string]SystemNodeContract
	MergedEvents          map[string]EventCatalogEntry
	MergedAgents          map[string]AgentRegistryEntry
	MergedTools           map[string]ToolSchemaEntry
	MergedPolicy          PolicyDocument
	ScopedNodeSources     map[string]ContractItemSource
	ScopedEventSources    map[string]ContractItemSource
	ScopedAgentSources    map[string]ContractItemSource
	ScopedToolSources     map[string]ContractItemSource
	NodeSources           map[string]ContractItemSource
	EventSources          map[string]ContractItemSource
	AgentSources          map[string]ContractItemSource
	ToolSources           map[string]ContractItemSource
	PolicySources         map[string]ContractItemSource
	ambiguousNodeAliases  map[string]struct{}
	ambiguousEventAliases map[string]struct{}
	ambiguousAgentAliases map[string]struct{}
	ambiguousToolAliases  map[string]struct{}
	Semantics             WorkflowSemanticView
	Workflow              WorkflowSchemaDocument
	Hooks                 GuardActionRegistryDocument
	Nodes                 map[string]SystemNodeContract
	Events                map[string]EventCatalogEntry
	Agents                map[string]AgentRegistryEntry
	Tools                 map[string]ToolSchemaEntry
	Policy                PolicyDocument
	Platform              PlatformSpecDocument
	FlowSchemas           map[string]FlowSchemaDocument
	FlowTree              FlowTree
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
	Action           string
	Guard            *GuardSpec
	AdvancesTo       string
	SetsGate         *GateSpec
	ClearGates       []string
	DataAccumulation WorkflowDataAccumulation
	Emits            EventEmission
	Condition        string
	CompletionRule   string
	OnComplete       *HandlerRuleEntry
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
	Template         string
	InstanceIDFrom   string
	ConfigFrom       *ConfigFromSpec
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
	ExpectedFrom string            `yaml:"expected_from"`
	Completion   string            `yaml:"completion"`
	OnComplete   *HandlerRuleEntry `yaml:"on_complete"`
	OnTimeout    *HandlerRuleEntry `yaml:"on_timeout"`
}

type ComputeSpec struct {
	Operation string        `yaml:"operation"`
	Tiers     []ComputeTier `yaml:"tiers"`
	StoreAs   string        `yaml:"store_as"`
}

type ComputeTier struct {
	Dimensions []string `yaml:"dimensions"`
	Weight     float64  `yaml:"weight"`
}

type FanOutSpec struct {
	ItemsFrom   string            `yaml:"items_from"`
	Target      string            `yaml:"target"`
	EmitPerItem string            `yaml:"emit_per_item"`
	EmitMapping map[string]string `yaml:"emit_mapping"`
}

type FilterSpec struct {
	Predicate string `yaml:"predicate"`
	Source    string `yaml:"source"`
	ItemsFrom string `yaml:"items_from"`
	Condition string `yaml:"condition"`
	StoreAs   string `yaml:"store_as"`
}

type ReduceSpec struct {
	Operation string         `yaml:"operation"`
	Source    string         `yaml:"source"`
	StoreAs   string         `yaml:"store_as"`
	ItemsFrom string         `yaml:"items_from"`
	Params    map[string]any `yaml:"params"`
}

type CountSpec struct {
	Source    string `yaml:"source"`
	StoreAs   string `yaml:"store_as"`
	ItemsFrom string `yaml:"items_from"`
	Condition string `yaml:"condition"`
}

type ClearSpec struct {
	Target  string   `yaml:"target"`
	Targets []string `yaml:"targets"`
}

type PayloadTransformSpec struct {
	Mappings map[string]string `yaml:"mappings"`
	Fields   map[string]string `yaml:"fields"`
}

type ConfigFromSpec struct {
	PolicyKeys []string          `yaml:"policy_keys"`
	Bindings   map[string]string `yaml:",inline"`
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
	Operation string      `yaml:"operation"`
	Source    string      `yaml:"source"`
	StoreAs   string      `yaml:"store_as"`
	Entities  string      `yaml:"entities"`
	Filter    string      `yaml:"filter"`
	GroupBy   string      `yaml:"group_by"`
	Count     bool        `yaml:"count"`
	Select    []string    `yaml:"select"`
	Queries   []QuerySpec `yaml:"-"`
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

type PolicyDocument struct {
	Values map[string]PolicyValue `yaml:",inline"`
}

type PolicyValue struct {
	Value       any    `yaml:"value"`
	Description string `yaml:"description"`
	Override    bool   `yaml:"override"`
}

type FlowTree struct {
	Root   *FlowContractView
	ByPath map[string]*FlowContractView
	ByID   map[string]*FlowContractView
}

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

type ToolInputSchema struct {
	Type                 string                     `yaml:"type"`
	Description          string                     `yaml:"description"`
	Properties           map[string]ToolInputSchema `yaml:"properties"`
	Required             []string                   `yaml:"required"`
	Items                *ToolInputSchema           `yaml:"items"`
	Enum                 []any                      `yaml:"enum"`
	AdditionalProperties any                        `yaml:"additionalProperties"`
	Minimum              *float64                   `yaml:"minimum"`
	Maximum              *float64                   `yaml:"maximum"`
	Raw                  map[string]any             `yaml:"-"`
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
	source, ok := b.NodeSources[strings.TrimSpace(nodeID)]
	return source, ok
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

type RuntimeBridgePaths struct {
	Dir        string
	NodesFile  string
	EventsFile string
	AgentsFile string
	ToolsFile  string
	PolicyFile string
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
	RuntimeBridge     RuntimeBridgePaths
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
	Name             string              `yaml:"name"`
	Version          string              `yaml:"version"`
	PlatformVersion  string              `yaml:"platform_version"`
	Author           string              `yaml:"author"`
	Description      string              `yaml:"description"`
	Flows            []ProjectFlowRef    `yaml:"flows"`
	Packages         []ProjectPackageRef `yaml:"packages"`
	Children         []ProjectPackageRef `yaml:"children"`
	Subpackages      []ProjectPackageRef `yaml:"subpackages"`
	Handoffs         []ProjectHandoff    `yaml:"handoffs"`
	RuntimeContracts struct {
		Nodes  string `yaml:"nodes"`
		Events string `yaml:"events"`
		Agents string `yaml:"agents"`
		Tools  string `yaml:"tools"`
		Policy string `yaml:"policy"`
	} `yaml:"runtime_contracts"`
	TargetContracts struct {
		Nodes  string `yaml:"nodes"`
		Events string `yaml:"events"`
		Agents string `yaml:"agents"`
		Tools  string `yaml:"tools"`
		Policy string `yaml:"policy"`
		Note   string `yaml:"note"`
	} `yaml:"target_contracts"`
	EntitySchema EntitySchema `yaml:"entity_schema"`
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

type ProjectContractView struct {
	Paths    ProjectPackagePaths
	Manifest ProjectPackageDocument
	Nodes    map[string]SystemNodeContract
	Events   map[string]EventCatalogEntry
	Agents   map[string]AgentRegistryEntry
	Tools    map[string]ToolSchemaEntry
	Policy   PolicyDocument
}

type FlowContractView struct {
	Paths    FlowContractPaths
	Schema   FlowSchemaDocument
	Nodes    map[string]SystemNodeContract
	Events   map[string]EventCatalogEntry
	Agents   map[string]AgentRegistryEntry
	Tools    map[string]ToolSchemaEntry
	Policy   PolicyDocument
	Children []FlowContractView
	Parent   *FlowContractView
}

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

type WorkflowSchemaDocument struct {
	Workflow struct {
		Name           string                       `yaml:"name"`
		Version        string                       `yaml:"version"`
		Entity         string                       `yaml:"entity"`
		EntityTable    string                       `yaml:"entity_table"`
		StateField     string                       `yaml:"state_field"`
		InitialStage   string                       `yaml:"initial_stage"`
		Stages         []WorkflowStageContract      `yaml:"stages"`
		TerminalStages []string                     `yaml:"terminal_stages"`
		Transitions    []WorkflowTransitionContract `yaml:"transitions"`
		Timers         []WorkflowTimerContract      `yaml:"timers"`
		EntitySchema   EntitySchema                 `yaml:"entity_schema"`
	} `yaml:"workflow"`
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
	TargetField string          `yaml:"target_field,omitempty" json:"target_field,omitempty"`
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
		return nil
	case yaml.MappingNode:
		type alias WorkflowDataWrite
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*w = WorkflowDataWrite(aux)
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

func (d *PolicyDocument) UnmarshalYAML(node *yaml.Node) error {
	if d == nil {
		return nil
	}
	values := map[string]PolicyValue{}
	if node == nil || node.Kind == 0 {
		d.Values = values
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("unsupported policy yaml node kind %d", node.Kind)
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		var value PolicyValue
		if err := node.Content[i+1].Decode(&value); err != nil {
			return err
		}
		values[key] = value
	}
	d.Values = values
	return nil
}

func (v *PolicyValue) UnmarshalYAML(node *yaml.Node) error {
	if v == nil {
		return nil
	}
	if node.Kind == yaml.MappingNode && (hasYAMLMappingKey(node, "value") || hasYAMLMappingKey(node, "description") || hasYAMLMappingKey(node, "override")) {
		type alias PolicyValue
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*v = PolicyValue(aux)
		return nil
	}
	var raw any
	if err := node.Decode(&raw); err != nil {
		return err
	}
	*v = PolicyValue{Value: raw}
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

func (s *ToolInputSchema) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	type alias ToolInputSchema
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	var raw map[string]any
	if err := node.Decode(&raw); err == nil {
		aux.Raw = raw
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

type GuardActionRegistryDocument struct {
	Guards  []GuardActionEntry `yaml:"guards"`
	Actions []GuardActionEntry `yaml:"actions"`
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
	Action           string                   `yaml:"action"`
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
	OnComplete       *HandlerRuleEntry        `yaml:"on_complete"`
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
	Template         string                   `yaml:"template"`
	InstanceIDFrom   string                   `yaml:"instance_id_from"`
	ConfigFrom       *ConfigFromSpec          `yaml:"config_from"`
	Branch           []BranchSpec             `yaml:"branch"`
}

type EventCatalogEntry struct {
	Emitter           EventEmitterRef  `yaml:"emitter"`
	EmitterType       string           `yaml:"emitter_type"`
	AlternateEmitters []string         `yaml:"alternate_emitters"`
	Consumer          []string         `yaml:"consumer"`
	ConsumerType      []string         `yaml:"consumer_type"`
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
	var aux struct {
		Action           string                   `yaml:"action"`
		Description      string                   `yaml:"description"`
		Emits            EventEmission            `yaml:"emits"`
		Guard            yaml.Node                `yaml:"guard"`
		AdvancesTo       string                   `yaml:"advances_to"`
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
		Template         string                   `yaml:"template"`
		InstanceIDFrom   string                   `yaml:"instance_id_from"`
		ConfigFrom       yaml.Node                `yaml:"config_from"`
		Branch           yaml.Node                `yaml:"branch"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*h = SystemNodeEventHandler{
		Action:           strings.TrimSpace(aux.Action),
		Description:      strings.TrimSpace(aux.Description),
		Emits:            aux.Emits,
		AdvancesTo:       strings.TrimSpace(aux.AdvancesTo),
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
		Template:         strings.TrimSpace(aux.Template),
		InstanceIDFrom:   strings.TrimSpace(aux.InstanceIDFrom),
	}
	var err error
	if h.Guard, err = decodeGuardSpecNode(&aux.Guard); err != nil {
		return err
	}
	if h.SetsGate, err = decodeGateSpecNode(&aux.SetsGate); err != nil {
		return err
	}
	if h.ClearGates, err = decodeClearGatesNode(&aux.ClearGates); err != nil {
		return err
	}
	if h.OnComplete, err = decodeHandlerRuleEntryNode(&aux.OnComplete); err != nil {
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
	if h.ConfigFrom, err = decodeConfigFromSpecNode(&aux.ConfigFrom); err != nil {
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
	Vocabulary struct {
		Participant struct {
			Types map[string]struct {
				Execution string `yaml:"execution"`
			} `yaml:"types"`
		} `yaml:"participant"`
	} `yaml:"vocabulary"`
	ContractFormats yaml.Node `yaml:"contract_formats"`
	WorkflowState   struct {
		Fields map[string]struct {
			Type string `yaml:"type"`
		} `yaml:"fields"`
	} `yaml:"workflow_state"`
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

func ResolveWorkflowContractPathsWithOverrides(repoRoot, workflowDirOverride, platformSpecFileOverride string) ContractPaths {
	contractsRoot := filepath.Join(repoRoot, "contracts")
	workflowDir := defaultWorkflowContractsDir(contractsRoot)
	overrideActive := strings.TrimSpace(workflowDirOverride) != ""
	if overrideActive {
		workflowDir = workflowDirOverride
	}
	workflowSchemaCandidates := []string{
		filepath.Join(workflowDir, "workflow.yaml"),
		filepath.Join(workflowDir, "workflow-schema.yaml"),
		filepath.Join(workflowDir, "workflow-empire.yaml"),
	}
	guardRegistryCandidates := []string{
		filepath.Join(workflowDir, "hooks.yaml"),
		filepath.Join(workflowDir, "hooks-empire.yaml"),
	}
	systemNodesCandidates := []string{
		filepath.Join(workflowDir, "runtime", "nodes.yaml"),
		filepath.Join(workflowDir, "nodes.yaml"),
		filepath.Join(workflowDir, "nodes-empire.yaml"),
	}
	eventCatalogCandidates := []string{
		filepath.Join(workflowDir, "runtime", "events.yaml"),
		filepath.Join(workflowDir, "events.yaml"),
		filepath.Join(workflowDir, "events-empire.yaml"),
	}
	agentRegistryCandidates := []string{
		filepath.Join(workflowDir, "runtime", "agents.yaml"),
		filepath.Join(workflowDir, "agents.yaml"),
		filepath.Join(workflowDir, "agents-empire.yaml"),
	}
	toolSchemaCandidates := []string{
		filepath.Join(workflowDir, "runtime", "tools.yaml"),
		filepath.Join(workflowDir, "tools.yaml"),
		filepath.Join(workflowDir, "tools-empire.yaml"),
	}
	policyCandidates := []string{
		filepath.Join(workflowDir, "runtime", "policy.yaml"),
		filepath.Join(workflowDir, "policy.yaml"),
		filepath.Join(workflowDir, "policy-empire.yaml"),
	}
	promptsCandidates := []string{
		filepath.Join(workflowDir, "prompts"),
	}
	if !overrideActive {
		workflowSchemaCandidates = append(workflowSchemaCandidates, filepath.Join(contractsRoot, "workflow-schema.yaml"))
		guardRegistryCandidates = append(guardRegistryCandidates, filepath.Join(contractsRoot, "guard-action-registry.yaml"))
		systemNodesCandidates = append(systemNodesCandidates, filepath.Join(contractsRoot, "system-nodes.yaml"))
		eventCatalogCandidates = append(eventCatalogCandidates, filepath.Join(contractsRoot, "event-catalog.yaml"))
		agentRegistryCandidates = append(agentRegistryCandidates, filepath.Join(contractsRoot, "agent-tools.yaml"))
		toolSchemaCandidates = append(toolSchemaCandidates, filepath.Join(contractsRoot, "tool-schemas.yaml"))
		policyCandidates = append(policyCandidates, filepath.Join(contractsRoot, "prompt-variables.yaml"))
		promptsCandidates = append(promptsCandidates, filepath.Join(contractsRoot, "prompts"))
	}
	workflowSchemaFile := firstExistingFile(workflowSchemaCandidates...)
	guardRegistryFile := firstExistingFile(guardRegistryCandidates...)
	platformSpecFile := filepath.Join(contractsRoot, "platform", "platform-spec.yaml")
	if strings.TrimSpace(platformSpecFileOverride) != "" {
		platformSpecFile = platformSpecFileOverride
	}
	paths := ContractPaths{
		ContractsRoot:         contractsRoot,
		WorkflowDir:           workflowDir,
		ProjectPackageFile:    existingFile(filepath.Join(workflowDir, "package.yaml")),
		ProjectNodesFile:      existingFile(filepath.Join(workflowDir, "nodes.yaml")),
		ProjectEventsFile:     existingFile(filepath.Join(workflowDir, "events.yaml")),
		ProjectAgentsFile:     existingFile(filepath.Join(workflowDir, "agents.yaml")),
		ProjectToolsFile:      existingFile(filepath.Join(workflowDir, "tools.yaml")),
		ProjectPolicyFile:     existingFile(filepath.Join(workflowDir, "policy.yaml")),
		ProjectPromptsDir:     existingDir(filepath.Join(workflowDir, "prompts")),
		WorkflowSchemaFile:    workflowSchemaFile,
		GuardRegistryFile:     guardRegistryFile,
		SystemNodesFile:       firstExistingFile(systemNodesCandidates...),
		EventCatalogFile:      firstExistingFile(eventCatalogCandidates...),
		AgentRegistryFile:     firstExistingFile(agentRegistryCandidates...),
		ToolSchemasFile:       firstExistingFile(toolSchemaCandidates...),
		PolicyFile:            firstExistingFile(policyCandidates...),
		PromptsDir:            firstExistingDir(promptsCandidates...),
		PlatformSpecFile:      platformSpecFile,
		VerificationGatesFile: filepath.Join(contractsRoot, "verification-gates.yaml"),
		ToolingLockFile:       filepath.Join(contractsRoot, "tooling.lock"),
		DDLFile:               filepath.Join(contractsRoot, "ddl-canonical.sql"),
		AgentConfigMapFile:    filepath.Join(contractsRoot, "agent-config-map.yaml"),
	}
	if paths.ProjectPackageFile != "" {
		paths.ProjectPackages = discoverProjectPackagePaths(paths.ProjectPackageFile, workflowDir)
		if len(paths.ProjectPackages) > 0 {
			rootPackage := paths.ProjectPackages[0]
			paths.RuntimeBridge = rootPackage.RuntimeBridge
			if paths.RuntimeBridge.NodesFile != "" {
				paths.SystemNodesFile = paths.RuntimeBridge.NodesFile
			}
			if paths.RuntimeBridge.EventsFile != "" {
				paths.EventCatalogFile = paths.RuntimeBridge.EventsFile
			}
			if paths.RuntimeBridge.AgentsFile != "" {
				paths.AgentRegistryFile = paths.RuntimeBridge.AgentsFile
			}
			if paths.RuntimeBridge.ToolsFile != "" {
				paths.ToolSchemasFile = paths.RuntimeBridge.ToolsFile
			}
			if paths.RuntimeBridge.PolicyFile != "" {
				paths.PolicyFile = paths.RuntimeBridge.PolicyFile
			}
		}
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
	paths.WorkflowSchemaFile = existingFile(paths.WorkflowSchemaFile)
	paths.GuardRegistryFile = existingFile(paths.GuardRegistryFile)
	return paths
}

func preferredWorkflowContractsDir(preferred, fallback string) string {
	if dir := existingDir(preferred); dir != "" {
		return dir
	}
	return fallback
}

func defaultWorkflowContractsDir(contractsRoot string) string {
	if file := existingFile(filepath.Join(contractsRoot, "package.yaml")); file != "" {
		return contractsRoot
	}
	entries, err := os.ReadDir(contractsRoot)
	if err != nil {
		return contractsRoot
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		candidate := filepath.Join(contractsRoot, entry.Name())
		if file := existingFile(filepath.Join(candidate, "package.yaml")); file != "" {
			return candidate
		}
		if file := firstExistingFile(
			filepath.Join(candidate, "workflow.yaml"),
			filepath.Join(candidate, "workflow-empire.yaml"),
			filepath.Join(candidate, "hooks.yaml"),
			filepath.Join(candidate, "hooks-empire.yaml"),
			filepath.Join(candidate, "nodes-empire.yaml"),
			filepath.Join(candidate, "events-empire.yaml"),
			filepath.Join(candidate, "agents-empire.yaml"),
			filepath.Join(candidate, "tools-empire.yaml"),
			filepath.Join(candidate, "policy-empire.yaml"),
		); file != "" {
			return candidate
		}
	}
	return contractsRoot
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
		ProjectContracts:      map[string]ProjectContractView{},
		FlowContracts:         map[string]FlowContractView{},
		ScopedNodes:           map[string]SystemNodeContract{},
		ScopedEvents:          map[string]EventCatalogEntry{},
		ScopedAgents:          map[string]AgentRegistryEntry{},
		ScopedTools:           map[string]ToolSchemaEntry{},
		MergedNodes:           map[string]SystemNodeContract{},
		MergedEvents:          map[string]EventCatalogEntry{},
		MergedAgents:          map[string]AgentRegistryEntry{},
		MergedTools:           map[string]ToolSchemaEntry{},
		MergedPolicy:          PolicyDocument{Values: map[string]PolicyValue{}},
		ScopedNodeSources:     map[string]ContractItemSource{},
		ScopedEventSources:    map[string]ContractItemSource{},
		ScopedAgentSources:    map[string]ContractItemSource{},
		ScopedToolSources:     map[string]ContractItemSource{},
		NodeSources:           map[string]ContractItemSource{},
		EventSources:          map[string]ContractItemSource{},
		AgentSources:          map[string]ContractItemSource{},
		ToolSources:           map[string]ContractItemSource{},
		PolicySources:         map[string]ContractItemSource{},
		ambiguousNodeAliases:  map[string]struct{}{},
		ambiguousEventAliases: map[string]struct{}{},
		ambiguousAgentAliases: map[string]struct{}{},
		ambiguousToolAliases:  map[string]struct{}{},
		FlowSchemas:           map[string]FlowSchemaDocument{},
	}
	if paths.ProjectPackageFile != "" {
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
			bundle.ProjectContracts[pkgPaths.Key] = projectView
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
			bundle.FlowContracts[flow.ID] = flowView
		}
		if err := populateMergedPackageViews(bundle); err != nil {
			return nil, err
		}
	}
	if paths.WorkflowSchemaFile != "" {
		if err := loadYAMLFile(paths.WorkflowSchemaFile, &bundle.Workflow); err != nil {
			return nil, err
		}
	}
	if paths.GuardRegistryFile != "" {
		if err := loadYAMLFile(paths.GuardRegistryFile, &bundle.Hooks); err != nil {
			return nil, err
		}
	}
	if err := loadYAMLFile(paths.SystemNodesFile, &bundle.Nodes); err != nil {
		return nil, err
	}
	if err := loadYAMLFile(paths.EventCatalogFile, &bundle.Events); err != nil {
		return nil, err
	}
	if err := loadYAMLFile(paths.AgentRegistryFile, &bundle.Agents); err != nil {
		return nil, err
	}
	if err := loadYAMLFile(paths.ToolSchemasFile, &bundle.Tools); err != nil {
		return nil, err
	}
	if err := loadYAMLFile(paths.PolicyFile, &bundle.Policy); err != nil {
		return nil, err
	}
	if err := loadYAMLFile(paths.PlatformSpecFile, &bundle.Platform); err != nil {
		return nil, err
	}
	populateWorkflowSemantics(bundle)
	return bundle, nil
}

func populateWorkflowSemantics(bundle *WorkflowContractBundle) {
	if bundle == nil {
		return
	}
	name := strings.TrimSpace(bundle.Workflow.Workflow.Name)
	if name == "" {
		name = strings.TrimSpace(bundle.Package.Name)
	}
	version := strings.TrimSpace(bundle.Package.Version)
	if version == "" {
		version = strings.TrimSpace(bundle.Workflow.Workflow.Version)
	}
	entitySchema := bundle.Package.EntitySchema
	if entitySchema.Empty() {
		entitySchema = bundle.Workflow.Workflow.EntitySchema
	}
	semantics := WorkflowSemanticView{
		Name:                   name,
		Version:                version,
		InitialStage:           strings.TrimSpace(bundle.Workflow.Workflow.InitialStage),
		EntitySchema:           entitySchema,
		Stages:                 append([]WorkflowStageContract{}, bundle.Workflow.Workflow.Stages...),
		TerminalStages:         append([]string{}, bundle.Workflow.Workflow.TerminalStages...),
		Transitions:            append([]WorkflowTransitionContract{}, bundle.Workflow.Workflow.Transitions...),
		Timers:                 deriveWorkflowSemanticTimers(bundle),
		Guards:                 append([]GuardActionEntry{}, bundle.Hooks.Guards...),
		Actions:                append([]GuardActionEntry{}, bundle.Hooks.Actions...),
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
				Action:           strings.TrimSpace(handler.Action),
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
				Template:         strings.TrimSpace(handler.Template),
				InstanceIDFrom:   strings.TrimSpace(handler.InstanceIDFrom),
				ConfigFrom:       handler.ConfigFrom,
				Branch:           append([]BranchSpec{}, handler.Branch...),
			}
			semantics.HandlerTransitions = append(semantics.HandlerTransitions, transition)
			if semantics.HandlerTransitionIndex[nodeID] == nil {
				semantics.HandlerTransitionIndex[nodeID] = map[string]HandlerTransitionSemantic{}
			}
			semantics.HandlerTransitionIndex[nodeID][eventType] = transition
		}
		semantics.NodeHandlers[nodeID] = handlers
	}
	if len(semantics.Stages) == 0 {
		semantics.Stages = deriveWorkflowStagesFromFlows(bundle.Paths.Flows, bundle.FlowSchemas)
	}
	if len(semantics.TerminalStages) == 0 {
		semantics.TerminalStages = deriveWorkflowTerminalStagesFromFlows(bundle.Paths.Flows, bundle.FlowSchemas)
	}
	bundle.Semantics = semantics
}

func deriveWorkflowSemanticTimers(bundle *WorkflowContractBundle) []WorkflowTimerContract {
	if bundle == nil {
		return nil
	}
	out := make([]WorkflowTimerContract, 0, len(bundle.Workflow.Workflow.Timers)+8)
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
	for _, timer := range bundle.Workflow.Workflow.Timers {
		addTimer(timer)
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
	scopedNodes := make([]scopedNodeEntry, 0, len(bundle.ScopedNodes))
	for scopedKey, node := range bundle.ScopedNodes {
		source := bundle.ScopedNodeSources[scopedKey]
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
		for nodeID, node := range bundle.MergedNodes {
			scopedNodes = append(scopedNodes, scopedNodeEntry{
				Key:    nodeID,
				NodeID: strings.TrimSpace(nodeID),
				Node:   node,
				Source: bundle.NodeSources[nodeID],
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
		if len(bundle.MergedNodes) > 0 {
			if merged, ok := bundle.MergedNodes[timer.NodeID]; ok {
				node = merged
			}
		}
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

func firstExistingFile(paths ...string) string {
	for _, path := range paths {
		if file := existingFile(path); file != "" {
			return file
		}
	}
	return ""
}

func firstExistingDir(paths ...string) string {
	for _, path := range paths {
		if dir := existingDir(path); dir != "" {
			return dir
		}
	}
	return ""
}

func workflowTimerEventDefined(bundle *WorkflowContractBundle, eventType string) bool {
	eventType = strings.TrimSpace(eventType)
	if bundle == nil || eventType == "" {
		return false
	}
	if _, ok := bundle.MergedEvents[eventType]; ok {
		return true
	}
	for scopedKey := range bundle.ScopedEvents {
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
		Paths:    paths,
		Schema:   schema,
		Nodes:    map[string]SystemNodeContract{},
		Events:   map[string]EventCatalogEntry{},
		Agents:   map[string]AgentRegistryEntry{},
		Tools:    map[string]ToolSchemaEntry{},
		Policy:   PolicyDocument{Values: map[string]PolicyValue{}},
		Children: nil,
		Parent:   nil,
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

func populateMergedPackageViews(bundle *WorkflowContractBundle) error {
	for _, pkg := range bundle.PackageTree {
		view, ok := bundle.ProjectContracts[pkg.Key]
		if !ok {
			continue
		}
		if err := mergeNodeContracts(bundle, view.Nodes, ContractItemSource{PackageKey: pkg.Key, Layer: "project", File: view.Paths.ProjectNodesFile}); err != nil {
			return err
		}
		if err := mergeEventContracts(bundle, view.Events, ContractItemSource{PackageKey: pkg.Key, Layer: "project", File: view.Paths.ProjectEventsFile}); err != nil {
			return err
		}
		if err := mergeAgentContracts(bundle, view.Agents, ContractItemSource{PackageKey: pkg.Key, Layer: "project", File: view.Paths.ProjectAgentsFile}); err != nil {
			return err
		}
		if err := mergeToolContracts(bundle, view.Tools, ContractItemSource{PackageKey: pkg.Key, Layer: "project", File: view.Paths.ProjectToolsFile}); err != nil {
			return err
		}
		mergePolicyContracts(bundle, view.Policy, ContractItemSource{PackageKey: pkg.Key, Layer: "project", File: view.Paths.ProjectPolicyFile})
	}
	for _, flow := range bundle.Paths.Flows {
		view, ok := bundle.FlowContracts[flow.ID]
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
		mergePolicyContracts(bundle, view.Policy, contractSourceWithFile(sourcePrefix, view.Paths.PolicyFile))
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
		if existing, ok := bundle.ScopedNodeSources[scopedKey]; ok {
			if reflect.DeepEqual(bundle.ScopedNodes[scopedKey], entry) {
				continue
			}
			return fmt.Errorf("duplicate scoped node id %q from %s and %s", scopedKey, existing.File, source.File)
		}
		bundle.ScopedNodes[scopedKey] = entry
		bundle.ScopedNodeSources[scopedKey] = source
		if _, ambiguous := bundle.ambiguousNodeAliases[key]; ambiguous {
			continue
		}
		if existing, ok := bundle.NodeSources[key]; ok {
			if contractSameScope(existing, source) {
				if reflect.DeepEqual(bundle.MergedNodes[key], entry) {
					continue
				}
				return fmt.Errorf("duplicate merged node id %q from %s and %s", key, existing.File, source.File)
			}
			delete(bundle.MergedNodes, key)
			delete(bundle.NodeSources, key)
			bundle.ambiguousNodeAliases[key] = struct{}{}
			continue
		}
		bundle.MergedNodes[key] = entry
		bundle.NodeSources[key] = source
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
		if existing, ok := bundle.ScopedEventSources[scopedKey]; ok {
			if reflect.DeepEqual(bundle.ScopedEvents[scopedKey], entry) {
				continue
			}
			return fmt.Errorf("duplicate scoped event id %q from %s and %s", scopedKey, existing.File, source.File)
		}
		bundle.ScopedEvents[scopedKey] = entry
		bundle.ScopedEventSources[scopedKey] = source
		if _, ambiguous := bundle.ambiguousEventAliases[key]; ambiguous {
			continue
		}
		if existing, ok := bundle.EventSources[key]; ok {
			if contractSameScope(existing, source) {
				if reflect.DeepEqual(bundle.MergedEvents[key], entry) {
					continue
				}
				merged, ok := mergeEventCatalogEntry(bundle.MergedEvents[key], entry)
				if !ok {
					return fmt.Errorf("duplicate merged event id %q from %s and %s", key, existing.File, source.File)
				}
				bundle.MergedEvents[key] = merged
				bundle.EventSources[key] = source
				continue
			}
			delete(bundle.MergedEvents, key)
			delete(bundle.EventSources, key)
			bundle.ambiguousEventAliases[key] = struct{}{}
			continue
		}
		bundle.MergedEvents[key] = entry
		bundle.EventSources[key] = source
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

func mergeDynamicValue(existing, incoming any) (any, bool) {
	switch {
	case isEmptyDynamicValue(existing):
		return cloneDynamicValue(incoming), true
	case isEmptyDynamicValue(incoming):
		return cloneDynamicValue(existing), true
	case reflect.DeepEqual(existing, incoming):
		return cloneDynamicValue(existing), true
	}
	existingMap, existingOK := dynamicStringMap(existing)
	incomingMap, incomingOK := dynamicStringMap(incoming)
	if existingOK && incomingOK {
		merged := cloneStringAnyMap(existingMap)
		for key, value := range incomingMap {
			current, exists := merged[key]
			if !exists {
				merged[key] = cloneDynamicValue(value)
				continue
			}
			next, ok := mergeDynamicValue(current, value)
			if !ok {
				return nil, false
			}
			merged[key] = next
		}
		return merged, true
	}
	if existingOK && !incomingOK {
		if _, ok := incoming.(string); ok {
			return cloneDynamicValue(existing), true
		}
	}
	if !existingOK && incomingOK {
		if _, ok := existing.(string); ok {
			return cloneDynamicValue(incoming), true
		}
	}
	return nil, false
}

func dynamicStringMap(value any) (map[string]any, bool) {
	if value == nil {
		return nil, false
	}
	typed, ok := value.(map[string]any)
	return typed, ok
}

func cloneDynamicValue(value any) any {
	if typed, ok := dynamicStringMap(value); ok {
		cloned := make(map[string]any, len(typed))
		for key, entry := range typed {
			cloned[key] = cloneDynamicValue(entry)
		}
		return cloned
	}
	return value
}

func isEmptyDynamicValue(value any) bool {
	if value == nil {
		return true
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed) == ""
	case []string:
		return len(typed) == 0
	case []any:
		return len(typed) == 0
	case map[string]any:
		return len(typed) == 0
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		return rv.Len() == 0
	case reflect.Pointer, reflect.Interface:
		return rv.IsNil()
	}
	return false
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
	var spec GuardSpec
	if err := node.Decode(&spec); err != nil {
		return nil, err
	}
	if strings.TrimSpace(spec.ID) == "" && strings.TrimSpace(spec.Check) == "" && len(spec.Checks) == 0 && strings.TrimSpace(spec.OnFail) == "" && strings.TrimSpace(spec.PolicyRef) == "" {
		return nil, nil
	}
	return &spec, nil
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
		return &spec, nil
	case yaml.SequenceNode:
		var queries []QuerySpec
		if err := node.Decode(&queries); err != nil {
			return nil, err
		}
		return &QuerySpec{Queries: queries}, nil
	default:
		return nil, fmt.Errorf("unsupported query yaml node kind %d", node.Kind)
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
	return spec, nil
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
		if existing, ok := bundle.ScopedAgentSources[scopedKey]; ok {
			if reflect.DeepEqual(bundle.ScopedAgents[scopedKey], entry) {
				continue
			}
			return fmt.Errorf("duplicate scoped agent id %q from %s and %s", scopedKey, existing.File, source.File)
		}
		bundle.ScopedAgents[scopedKey] = entry
		bundle.ScopedAgentSources[scopedKey] = source
		if _, ambiguous := bundle.ambiguousAgentAliases[key]; ambiguous {
			continue
		}
		if existing, ok := bundle.AgentSources[key]; ok {
			if contractSameScope(existing, source) {
				if reflect.DeepEqual(bundle.MergedAgents[key], entry) {
					continue
				}
				return fmt.Errorf("duplicate merged agent id %q from %s and %s", key, existing.File, source.File)
			}
			delete(bundle.MergedAgents, key)
			delete(bundle.AgentSources, key)
			bundle.ambiguousAgentAliases[key] = struct{}{}
			continue
		}
		bundle.MergedAgents[key] = entry
		bundle.AgentSources[key] = source
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
		if existing, ok := bundle.ScopedToolSources[scopedKey]; ok {
			if reflect.DeepEqual(bundle.ScopedTools[scopedKey], entry) {
				continue
			}
			return fmt.Errorf("duplicate scoped tool id %q from %s and %s", scopedKey, existing.File, source.File)
		}
		bundle.ScopedTools[scopedKey] = entry
		bundle.ScopedToolSources[scopedKey] = source
		if _, ambiguous := bundle.ambiguousToolAliases[key]; ambiguous {
			continue
		}
		if existing, ok := bundle.ToolSources[key]; ok {
			if contractSameScope(existing, source) {
				if reflect.DeepEqual(bundle.MergedTools[key], entry) {
					continue
				}
				return fmt.Errorf("duplicate merged tool id %q from %s and %s", key, existing.File, source.File)
			}
			delete(bundle.MergedTools, key)
			delete(bundle.ToolSources, key)
			bundle.ambiguousToolAliases[key] = struct{}{}
			continue
		}
		bundle.MergedTools[key] = entry
		bundle.ToolSources[key] = source
	}
	return nil
}

func mergePolicyContracts(bundle *WorkflowContractBundle, entries PolicyDocument, source ContractItemSource) {
	for id, entry := range entries.Values {
		key := strings.TrimSpace(id)
		if key == "" {
			continue
		}
		if bundle.MergedPolicy.Values == nil {
			bundle.MergedPolicy.Values = map[string]PolicyValue{}
		}
		bundle.MergedPolicy.Values[key] = entry
		bundle.PolicySources[key] = source
	}
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
		paths.WorkflowSchemaFile,
		paths.GuardRegistryFile,
		paths.SystemNodesFile,
		paths.EventCatalogFile,
		paths.AgentRegistryFile,
		paths.ToolSchemasFile,
		paths.PolicyFile,
		paths.PromptsDir,
		paths.PlatformSpecFile,
		paths.VerificationGatesFile,
		paths.ToolingLockFile,
		paths.DDLFile,
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

func preferredContractPath(preferred, legacy string) string {
	if preferred != "" {
		if _, err := os.Stat(preferred); err == nil {
			return preferred
		}
	}
	return legacy
}

func preferredContractDir(preferred, legacy string) string {
	if preferred != "" {
		if stat, err := os.Stat(preferred); err == nil && stat.IsDir() {
			return preferred
		}
	}
	return legacy
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
		if runtime := resolveRuntimeBridgePaths(packageDir, manifest); runtime.Dir != "" {
			pkg.RuntimeBridge = runtime
		} else if runtimeDir := existingDir(filepath.Join(packageDir, "runtime")); runtimeDir != "" {
			pkg.RuntimeBridge = RuntimeBridgePaths{
				Dir:        runtimeDir,
				NodesFile:  existingFile(filepath.Join(runtimeDir, "nodes.yaml")),
				EventsFile: existingFile(filepath.Join(runtimeDir, "events.yaml")),
				AgentsFile: existingFile(filepath.Join(runtimeDir, "agents.yaml")),
				ToolsFile:  existingFile(filepath.Join(runtimeDir, "tools.yaml")),
				PolicyFile: existingFile(filepath.Join(runtimeDir, "policy.yaml")),
			}
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

func resolveRuntimeBridgePaths(packageDir string, manifest ProjectPackageDocument) RuntimeBridgePaths {
	resolve := func(rel string) string {
		rel = strings.TrimSpace(rel)
		if rel == "" {
			return ""
		}
		return existingFile(filepath.Join(packageDir, filepath.Clean(rel)))
	}
	runtime := RuntimeBridgePaths{
		NodesFile:  resolve(manifest.RuntimeContracts.Nodes),
		EventsFile: resolve(manifest.RuntimeContracts.Events),
		AgentsFile: resolve(manifest.RuntimeContracts.Agents),
		ToolsFile:  resolve(manifest.RuntimeContracts.Tools),
		PolicyFile: resolve(manifest.RuntimeContracts.Policy),
	}
	for _, candidate := range []string{
		runtime.NodesFile,
		runtime.EventsFile,
		runtime.AgentsFile,
		runtime.ToolsFile,
		runtime.PolicyFile,
	} {
		if candidate != "" {
			runtime.Dir = filepath.Dir(candidate)
			break
		}
	}
	return runtime
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

func cloneStringAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
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
