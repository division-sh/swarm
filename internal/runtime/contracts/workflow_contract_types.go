package contracts

import (
	"gopkg.in/yaml.v3"
	"strings"
	"swarm/internal/runtime/core/paths"
	flowmodel "swarm/internal/runtime/flowmodel"
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

type FlowInputAutoWireResolution struct {
	EventType     string
	Patterns      []string
	ProducerFlows []string
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
	GroupBy          *GroupBySpec
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
	Compute          *ComputeSpec             `yaml:"compute"`
	FanOut           *FanOutSpec              `yaml:"fan_out"`
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
type AccumulateSpec struct {
	ExpectedFrom string               `yaml:"expected_from"`
	ExpectedPath paths.Path           `yaml:"-"`
	DedupBy      string               `yaml:"dedup_by"`
	DedupPath    paths.Path           `yaml:"-"`
	Threshold    int                  `yaml:"threshold"`
	TimeoutMS    int                  `yaml:"timeout_ms"`
	Completion   AccumulateCompletion `yaml:"completion"`
	OnComplete   []HandlerRuleEntry   `yaml:"on_complete"`
	OnTimeout    *HandlerRuleEntry    `yaml:"on_timeout"`
}
type ComputeSpec struct {
	Operation   ComputeOperation `yaml:"operation"`
	Tiers       []ComputeTier    `yaml:"tiers"`
	Keys        ComputeKeyConfig `yaml:"keys"`
	Params      map[string]any   `yaml:"params"`
	StoreAs     string           `yaml:"store_as"`
	Description string           `yaml:"description"`
	ValueField  string           `yaml:"value_field"`
	WeightField string           `yaml:"weight_field"`
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
	ItemsFrom      string            `yaml:"items_from"`
	ItemsPath      paths.Path        `yaml:"-"`
	Target         string            `yaml:"target"`
	EmitPerItem    string            `yaml:"emit_per_item"`
	EmitMapping    map[string]string `yaml:"emit_mapping"`
	EmitMappingKey string            `yaml:"-"`
}
type GroupBySpec struct {
	ItemsFrom string     `yaml:"items_from"`
	ItemsPath paths.Path `yaml:"-"`
	Key       string     `yaml:"key"`
	KeyPath   paths.Path `yaml:"-"`
	StoreAs   string     `yaml:"store_as"`
	StorePath paths.Path `yaml:"-"`
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
	Entries  []TransformSpec   `yaml:"entries,omitempty"`
}
type TransformSpec struct {
	Target     string
	TargetPath paths.Path
	Value      ExpressionValue `yaml:"value,omitempty"`
}
type TransformBinding = TransformSpec

func (p PayloadTransformSpec) TransformEntries() []TransformSpec {
	mappings := p.Mappings
	if len(mappings) == 0 && len(p.Fields) > 0 {
		mappings = p.Fields
	}
	out := make([]TransformSpec, 0, len(mappings)+len(p.Entries))
	for target, source := range mappings {
		cleanTarget := strings.TrimSpace(target)
		cleanSource := strings.TrimSpace(source)
		if cleanTarget == "" || cleanSource == "" {
			continue
		}
		out = append(out, TransformSpec{
			Target:     cleanTarget,
			TargetPath: paths.Parse(cleanTarget),
			Value:      CELExpression(cleanSource),
		})
	}
	for _, entry := range p.Entries {
		if strings.TrimSpace(entry.Target) == "" {
			continue
		}
		clone := entry
		clone.Target = strings.TrimSpace(clone.Target)
		clone.TargetPath = paths.Parse(clone.Target)
		clone.Value.hydrate()
		out = append(out, clone)
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
	Name        string `yaml:"name"`
	Type        string `yaml:"type"`
	Initial     any    `yaml:"initial"`
	Primary     bool   `yaml:"primary"`
	Indexed     bool   `yaml:"indexed"`
	Nullable    bool   `yaml:"nullable"`
	Description string `yaml:"description"`
}
type NodeStateSchema struct {
	Description string           `yaml:"description"`
	Fields      []NodeStateField `yaml:"fields"`
}
type NodeGateStateSchema struct {
	Description string          `yaml:"description"`
	Gates       []NodeGateField `yaml:"gates"`
	Storage     string          `yaml:"storage"`
}
type NodeStateField struct {
	Name    string `yaml:"name"`
	Type    string `yaml:"type"`
	Default any    `yaml:"default"`
}
type NodeGateField struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
}
type PolicyDocument = flowmodel.PolicyDocument
type PolicyValue = flowmodel.PolicyValue
type ContractURIRegistry = flowmodel.URIRegistry
type ContractURIRef = flowmodel.URIRef
type ExpressionKind string

const (
	ExpressionKindLiteral ExpressionKind = "literal"
	ExpressionKindRef     ExpressionKind = "ref"
	ExpressionKindCEL     ExpressionKind = "cel"
)

type ExpressionValue struct {
	Kind    ExpressionKind `yaml:"kind,omitempty"`
	Literal any            `yaml:"literal,omitempty"`
	Ref     string         `yaml:"ref,omitempty"`
	RefPath paths.Path     `yaml:"-"`
	CEL     string         `yaml:"cel,omitempty"`
}

func (e ExpressionValue) IsZero() bool {
	return e.Kind == "" && strings.TrimSpace(e.CEL) == "" && strings.TrimSpace(e.Ref) == "" && e.Literal == nil
}

func LiteralExpression(value any) ExpressionValue {
	return ExpressionValue{Kind: ExpressionKindLiteral, Literal: value}
}

func RefExpression(ref string) ExpressionValue {
	expr := ExpressionValue{Kind: ExpressionKindRef, Ref: strings.TrimSpace(ref)}
	expr.hydrate()
	return expr
}

func CELExpression(expression string) ExpressionValue {
	return ExpressionValue{Kind: ExpressionKindCEL, CEL: strings.TrimSpace(expression)}
}

func (e *ExpressionValue) hydrate() {
	if e == nil {
		return
	}
	e.Ref = strings.TrimSpace(e.Ref)
	e.CEL = strings.TrimSpace(e.CEL)
	e.RefPath = paths.Parse(e.Ref)
}

func (e ExpressionValue) HasLiteralValue() bool {
	return e.Kind == ExpressionKindLiteral
}

func (e ExpressionValue) HasRefValue() bool {
	return e.Kind == ExpressionKindRef
}

func (e ExpressionValue) HasCELValue() bool {
	return e.Kind == ExpressionKindCEL
}

type FlowVariable struct {
	Type        string `yaml:"type"`
	Default     any    `yaml:"default"`
	Description string `yaml:"description"`
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
type HTTPToolRetrySpec struct {
	MaxRetries int    `yaml:"max_retries"`
	Backoff    string `yaml:"backoff"`
}
type HTTPToolSpec struct {
	Method         string            `yaml:"method"`
	URL            string            `yaml:"url"`
	Headers        map[string]string `yaml:"headers"`
	Body           any               `yaml:"body"`
	TimeoutSeconds int               `yaml:"timeout_seconds"`
	Retry          HTTPToolRetrySpec `yaml:"retry"`
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
type WorkflowDataAccumulation struct {
	Writes      []WorkflowDataWrite `yaml:"writes"`
	SourceEvent string              `yaml:"source_event"`
}
type WorkflowDataWrite struct {
	Field       string          `yaml:"-" json:"field,omitempty"`
	SourceField string          `yaml:"source_field,omitempty" json:"source_field,omitempty"`
	SourcePath  paths.Path      `yaml:"-" json:"-"`
	TargetField string          `yaml:"target_field,omitempty" json:"target_field,omitempty"`
	TargetPath  paths.Path      `yaml:"-" json:"-"`
	Value       ExpressionValue `yaml:"value,omitempty" json:"value,omitempty"`
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
	return w.Value.HasLiteralValue()
}

func (w WorkflowDataWrite) SourceExpression() ExpressionValue {
	if !w.Value.IsZero() {
		return w.Value
	}
	source := strings.TrimSpace(w.Source())
	if source == "" {
		return ExpressionValue{}
	}
	expr := RefExpression("payload." + source)
	return expr
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
type EventEmission struct {
	Single string   `yaml:"-" json:"single,omitempty"`
	Many   []string `yaml:"-" json:"many,omitempty"`
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
	GateState        NodeGateStateSchema               `yaml:"gate_state"`
}
type SystemNodeEventHandler struct {
	Action           ActionSpec               `yaml:"action"`
	CreateEntity     bool                     `yaml:"create_entity"`
	Description      string                   `yaml:"description"`
	EvidenceTarget   string                   `yaml:"evidence_target"`
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
	GroupBy          *GroupBySpec             `yaml:"group_by"`
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
	Producer          []string         `yaml:"producer"`
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
	ID                     string         `yaml:"id"`
	Type                   string         `yaml:"type"`
	Role                   string         `yaml:"role"`
	PromptRef              string         `yaml:"prompt_ref"`
	Permissions            []string       `yaml:"permissions" json:"permissions,omitempty"`
	PermissionsBundle      string         `yaml:"permissions_bundle" json:"permissions_bundle,omitempty"`
	WorkspaceClass         string         `yaml:"workspace_class"`
	ManagerFallback        string         `yaml:"manager_fallback"`
	NodeType               string         `yaml:"node_type"`
	ModelTier              string         `yaml:"model_tier"`
	ConversationMode       string         `yaml:"conversation_mode"`
	SessionScope           string         `yaml:"session_scope"`
	MaxTurnsPerTask        int            `yaml:"max_turns_per_task"`
	Subscriptions          []string       `yaml:"subscriptions"`
	SubscriptionsBootstrap []string       `yaml:"subscriptions_bootstrap"`
	SubscribesTo           []string       `yaml:"subscribes_to"`
	Tools                  []string       `yaml:"tools"`
	ToolsTier2             []string       `yaml:"tools_tier2"`
	NativeTools            map[string]any `yaml:"native_tools"`
	EmitEvents             []string       `yaml:"emit_events"`
	Implementation         string         `yaml:"implementation"`
}

func (e AgentRegistryEntry) ConfiguredTools() []string {
	if len(e.Tools) > 0 {
		return append([]string{}, e.Tools...)
	}
	if len(e.ToolsTier2) > 0 {
		return append([]string{}, e.ToolsTier2...)
	}
	return nil
}

type ToolSchemaEntry struct {
	Category           string          `yaml:"category"`
	Description        string          `yaml:"description"`
	HandlerType        string          `yaml:"handler_type"`
	Permission         string          `yaml:"permission"`
	RequiredPermission string          `yaml:"required_permission"`
	InputSchema        ToolInputSchema `yaml:"input_schema"`
	OutputSchema       ToolInputSchema `yaml:"output_schema"`
	HTTP               *HTTPToolSpec   `yaml:"http"`
	ResponseMapping    map[string]any  `yaml:"response_mapping"`
	Credentials        []string        `yaml:"credentials"`
}
type PlatformSpecDocument struct {
	Platform struct {
		Name    string `yaml:"name"`
		Version string `yaml:"version"`
	} `yaml:"platform"`
	PlatformEvents struct {
		Catalog map[string]yaml.Node `yaml:"catalog"`
	} `yaml:"platform_events"`
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
