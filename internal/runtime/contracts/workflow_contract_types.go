package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
	flowmodel "github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"gopkg.in/yaml.v3"
)

type ContractPaths struct {
	ContractsRoot         string
	WorkflowDir           string
	RootSchemaFile        string
	RootTypesFile         string
	RootEntitiesFile      string
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
	flowTypes             map[string]TypeCatalogDocument
	flowEntities          map[string]EntityContractsDocument
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
	RootTypes             TypeCatalogDocument
	RootEntities          EntityContractsDocument
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
	FlowInputEventPins     map[string][]FlowInputEventPin
	FlowOutputEventPins    map[string][]FlowOutputEventPin
	FlowReads              map[string][]string
	FlowWrites             map[string][]string
	CompositionConnects    []FlowPackageConnect
	FlowAgents             map[string][]FlowRequiredAgent
	WritePinOwners         map[string][]string
	EffectiveNodes         map[string]SystemNodeEffectiveSemantics
	NodeHandlers           map[string]map[string]SystemNodeEventHandler
	EventOwners            map[string][]string
	HandlerTransitions     []HandlerTransitionSemantic
	HandlerTransitionIndex map[string]map[string]HandlerTransitionSemantic
}

type SystemNodeEffectiveSemantics struct {
	ID                   string
	ExecutionType        string
	RuntimeSubscriptions []string
	Produces             []string
}

type FlowInputAutoWireResolution struct {
	EventType     string
	Patterns      []string
	ProducerFlows []string
}
type HandlerTransitionSemantic struct {
	ID                   string
	NodeID               string
	FlowID               string
	EventType            string
	Action               ActionSpec
	SelectEntity         *SelectEntitySpec
	SelectOrCreateEntity *SelectOrCreateEntitySpec
	Guard                *GuardSpec
	AdvancesTo           string
	SetsGate             *GateSpec
	ClearGates           []string
	DataAccumulation     WorkflowDataAccumulation
	Emit                 EmitSpec
	Condition            string
	CompletionRule       string
	OnComplete           []HandlerRuleEntry
	Rules                []HandlerRuleEntry
	Accumulate           *AccumulateSpec
	Compute              *ComputeSpec
	Query                *QuerySpec
	FanOut               *FanOutSpec
	GroupBy              *GroupBySpec
	Filter               *FilterSpec
	Reduce               *ReduceSpec
	Count                *CountSpec
	Clear                *ClearSpec
	Branch               []BranchSpec
}
type HandlerRuleEntry struct {
	ID               string                   `yaml:"id"`
	Description      string                   `yaml:"description"`
	Condition        string                   `yaml:"condition"`
	AdvancesTo       string                   `yaml:"advances_to"`
	Emit             EmitSpec                 `yaml:"emit"`
	Action           ActionSpec               `yaml:"action"`
	DataAccumulation WorkflowDataAccumulation `yaml:"data_accumulation"`
	Compute          *ComputeSpec             `yaml:"compute"`
	FanOut           *FanOutSpec              `yaml:"fan_out"`
}
type GuardSpec struct {
	ID         string           `yaml:"id"`
	Check      string           `yaml:"check"`
	OnFail     string           `yaml:"on_fail"`
	OnFailSpec GuardFailureSpec `yaml:"-"`
	Checks     []GuardCheck     `yaml:"checks"`
	PolicyRef  string           `yaml:"policy_ref"`
}
type GuardCheck struct {
	ID    string `yaml:"id"`
	Check string `yaml:"check"`
}

type GuardFailureAction string

const (
	GuardFailureActionReject   GuardFailureAction = "reject"
	GuardFailureActionDiscard  GuardFailureAction = "discard"
	GuardFailureActionKill     GuardFailureAction = "kill"
	GuardFailureActionEscalate GuardFailureAction = "escalate"
)

type GuardFailureSpec struct {
	Action          GuardFailureAction `yaml:"action"`
	Escalation      EmitSpec           `yaml:"escalate"`
	AuthoredMapping bool               `yaml:"-"`
}

func (s GuardFailureSpec) Empty() bool {
	return strings.TrimSpace(string(s.Action)) == "" && s.Escalation.Empty() && len(s.Escalation.Fields) == 0
}

func (s GuardFailureSpec) EscalationEmitSpec() EmitSpec {
	if s.Action != GuardFailureActionEscalate {
		return EmitSpec{}
	}
	return cloneEmitSpec(s.Escalation)
}

func (g *GuardSpec) FailureSpec() (GuardFailureSpec, error) {
	if g == nil {
		return ParseGuardFailureSpec("")
	}
	if !g.OnFailSpec.Empty() || g.OnFailSpec.AuthoredMapping {
		return g.OnFailSpec, nil
	}
	return ParseGuardFailureSpec(g.OnFail)
}

func ParseGuardFailureSpec(action string) (GuardFailureSpec, error) {
	normalized := strings.TrimSpace(strings.ToLower(action))
	switch normalized {
	case "", "reject":
		return GuardFailureSpec{Action: GuardFailureActionReject}, nil
	case "discard":
		return GuardFailureSpec{Action: GuardFailureActionDiscard}, nil
	case "kill":
		return GuardFailureSpec{Action: GuardFailureActionKill}, nil
	}
	if strings.HasPrefix(normalized, "escalate:") {
		eventType := strings.TrimSpace(strings.TrimPrefix(normalized, "escalate:"))
		if eventType == "" {
			return GuardFailureSpec{}, fmt.Errorf("guard on_fail escalate requires event type")
		}
		return GuardFailureSpec{
			Action:     GuardFailureActionEscalate,
			Escalation: EmitSpec{Event: eventType},
		}, nil
	}
	return GuardFailureSpec{}, fmt.Errorf("unsupported guard on_fail action %q", action)
}

type AccumulateSpec struct {
	Into         string               `yaml:"into"`
	ExpectedFrom string               `yaml:"expected_from"`
	From         string               `yaml:"from"`
	Description  string               `yaml:"description"`
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
	ItemsFrom string     `yaml:"items_from"`
	ItemsPath paths.Path `yaml:"-"`
	Target    string     `yaml:"target"`
	Emit      EmitSpec   `yaml:"emit"`
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

type SelectEntitySpec struct {
	By       map[string]string        `yaml:"by"`
	Bindings []SelectEntityKeyBinding `yaml:"-"`
}

type SelectOrCreateEntitySpec struct {
	By       map[string]string        `yaml:"by"`
	Bindings []SelectEntityKeyBinding `yaml:"-"`
}

type SelectEntityKeyBinding struct {
	Field   string
	Ref     string
	RefPath paths.Path
}

func (s *SelectEntitySpec) Empty() bool {
	return s == nil || len(s.Bindings) == 0
}

func (s *SelectOrCreateEntitySpec) Empty() bool {
	return s == nil || len(s.Bindings) == 0
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
	ID             string            `yaml:"id"`
	Template       string            `yaml:"template"`
	InstanceIDFrom string            `yaml:"instance_id_from"`
	InstanceIDPath paths.Path        `yaml:"-"`
	ConfigFrom     *ConfigFromSpec   `yaml:"config_from"`
	Mailbox        *MailboxWriteSpec `yaml:"mailbox"`
	ArtifactRepo   *ArtifactRepoSpec `yaml:"artifact_repo"`
}

type MailboxWriteSpec struct {
	ItemType     ExpressionValue            `yaml:"item_type"`
	Severity     ExpressionValue            `yaml:"severity"`
	Summary      ExpressionValue            `yaml:"summary"`
	EntityID     ExpressionValue            `yaml:"entity_id"`
	FlowInstance ExpressionValue            `yaml:"flow_instance"`
	Payload      map[string]ExpressionValue `yaml:"payload"`
}

type ArtifactRepoSpec struct {
	Provider       string                     `yaml:"provider"`
	RepoID         ExpressionValue            `yaml:"repo_id"`
	Namespace      ExpressionValue            `yaml:"namespace"`
	PartitionKey   ExpressionValue            `yaml:"partition_key"`
	DisplaySlug    ExpressionValue            `yaml:"display_slug"`
	RequestID      ExpressionValue            `yaml:"request_id"`
	Author         ExpressionValue            `yaml:"author"`
	Provenance     map[string]ExpressionValue `yaml:"provenance"`
	AllowedPaths   []string                   `yaml:"allowed_paths"`
	Files          []ArtifactRepoFileSpec     `yaml:"files"`
	Output         ArtifactRepoOutputSpec     `yaml:"output"`
	Limits         ArtifactRepoLimitsSpec     `yaml:"limits"`
	SuccessEvent   string                     `yaml:"success_event"`
	SuccessPayload map[string]ExpressionValue `yaml:"success_payload"`
	FailureEvent   string                     `yaml:"failure_event"`
	FailurePayload map[string]ExpressionValue `yaml:"failure_payload"`
}

type ArtifactRepoFileSpec struct {
	Path        ExpressionValue        `yaml:"path"`
	Content     ExpressionValue        `yaml:"content"`
	ContentType string                 `yaml:"content_type"`
	Schema      ArtifactRepoSchemaSpec `yaml:"schema"`
	MaxBytes    int                    `yaml:"max_bytes"`
}

type ArtifactRepoSchemaSpec struct {
	Type           string   `yaml:"type"`
	RequiredFields []string `yaml:"required_fields"`
}

type ArtifactRepoOutputSpec struct {
	RepoURL           string `yaml:"repo_url"`
	CurrentRef        string `yaml:"current_ref"`
	FileManifest      string `yaml:"file_manifest"`
	Status            string `yaml:"status"`
	FailureReason     string `yaml:"failure_reason"`
	LastRequestID     string `yaml:"last_request_id"`
	LastSourceEventID string `yaml:"last_source_event_id"`
}

type ArtifactRepoLimitsSpec struct {
	MaxYAMLBytes     int `yaml:"max_yaml_bytes"`
	MaxMarkdownBytes int `yaml:"max_markdown_bytes"`
	MaxTextBytes     int `yaml:"max_text_bytes"`
	MaxRepoBytes     int `yaml:"max_repo_bytes"`
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

type WorkflowDataOperation string

const (
	WorkflowDataOperationSet    WorkflowDataOperation = "set"
	WorkflowDataOperationMerge  WorkflowDataOperation = "merge"
	WorkflowDataOperationDelete WorkflowDataOperation = "delete"
	WorkflowDataOperationAppend WorkflowDataOperation = "append"
	WorkflowDataOperationUpdate WorkflowDataOperation = "update"
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
	ID           string
	Flow         string
	Mode         string
	Namespace    string
	PackageKey   string
	PackageDir   string
	Dir          string
	DataDir      string
	SchemaFile   string
	TypesFile    string
	EntitiesFile string
	NodesFile    string
	EventsFile   string
	AgentsFile   string
	ToolsFile    string
	PolicyFile   string
	PromptsDir   string
}
type ProjectPackageDocument struct {
	Name            string               `yaml:"name"`
	Version         string               `yaml:"version"`
	PlatformVersion string               `yaml:"platform_version"`
	Author          string               `yaml:"author"`
	Description     string               `yaml:"description"`
	Requires        FlowPackageRequires  `yaml:"requires"`
	Flows           []ProjectFlowRef     `yaml:"flows"`
	Packages        []ProjectPackageRef  `yaml:"packages"`
	Children        []ProjectPackageRef  `yaml:"children"`
	Subpackages     []ProjectPackageRef  `yaml:"subpackages"`
	Connect         []FlowPackageConnect `yaml:"connect"`
	Handoffs        []ProjectHandoff     `yaml:"handoffs"`
	EntitySchema    EntitySchema         `yaml:"entity_schema"`
}

type TypeCatalogDocument struct {
	Scalars map[string]ScalarTypeDecl `yaml:"scalars"`
	Enums   map[string]EnumTypeDecl   `yaml:"enums"`
	Types   map[string]NamedTypeDecl  `yaml:"types"`
}

type ScalarTypeDecl struct {
	Base string `yaml:"-"`
}

type EnumTypeDecl struct {
	Values []string `yaml:"-"`
}

type NamedTypeDecl struct {
	Description string                   `yaml:"-"`
	Fields      map[string]TypeFieldSpec `yaml:"-"`
}

type TypeFieldSpec struct {
	Type        string `yaml:"type"`
	Description string `yaml:"description"`
}

type EntityContractsDocument map[string]EntityContract

type EntityContract struct {
	Description string                     `yaml:"-"`
	Owner       string                     `yaml:"-"`
	Fields      map[string]EntityFieldDecl `yaml:"-"`
}

type EntityFieldDecl struct {
	Type               string         `yaml:"type"`
	Initial            any            `yaml:"initial"`
	Indexed            bool           `yaml:"indexed"`
	Immutable          bool           `yaml:"immutable"`
	Description        string         `yaml:"description"`
	MaterializeFrom    string         `yaml:"materialize_from"`
	Project            map[string]any `yaml:"project"`
	UnusedReason       string         `yaml:"_unused_reason"`
	UnusedReaderReason string         `yaml:"_unused_reader_reason"`
}
type ProjectPackageRef struct {
	ID      string          `yaml:"id"`
	Path    string          `yaml:"path"`
	Package string          `yaml:"package"`
	Dir     string          `yaml:"dir"`
	Bind    FlowPackageBind `yaml:"bind"`
}
type ProjectFlowRef struct {
	ID        string          `yaml:"id"`
	Flow      string          `yaml:"flow"`
	Namespace string          `yaml:"namespace"`
	Mode      string          `yaml:"mode"`
	Bind      FlowPackageBind `yaml:"bind"`
}
type FlowPackageRequires struct {
	Inputs          []string               `yaml:"inputs"`
	Outputs         []string               `yaml:"outputs"`
	Policy          []string               `yaml:"policy"`
	PolicyDefaults  map[string]PolicyValue `yaml:"-"`
	Credentials     []string               `yaml:"credentials"`
	PlatformVersion string                 `yaml:"platform_version"`
}
type FlowPackageBind struct {
	Inputs      map[string]string         `yaml:"inputs"`
	Outputs     map[string]string         `yaml:"outputs"`
	Policy      map[string]string         `yaml:"policy"`
	Credentials map[string]string         `yaml:"credentials"`
	Observe     []FlowPackageObserveGrant `yaml:"observe"`
}
type FlowPackageObserveGrant struct {
	Source string   `yaml:"source"`
	Events []string `yaml:"events"`
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
	Name              string                          `yaml:"name"`
	Mode              string                          `yaml:"mode"`
	Entity            string                          `yaml:"entity"`
	Instance          FlowTemplateInstanceDeclaration `yaml:"instance"`
	InitialState      string                          `yaml:"initial_state"`
	NamespacePrefix   string                          `yaml:"namespace_prefix"`
	NamespaceRule     string                          `yaml:"namespace_rule"`
	TerminalStates    []string                        `yaml:"terminal_states"`
	States            []string                        `yaml:"states"`
	Pins              FlowPins                        `yaml:"pins"`
	ToolSurface       FlowToolSurfaceContract         `yaml:"tool_surface"`
	RequiredAgents    []FlowRequiredAgent             `yaml:"required_agents"`
	InstanceVariables FlowInstanceVariables           `yaml:"instance_variables"`
	AutoEmitOnCreate  AutoEmitOnCreateContract        `yaml:"auto_emit_on_create"`
}

const (
	FlowModeStatic    = "static"
	FlowModeTemplate  = "template"
	FlowModeSingleton = "singleton"
)

type FlowToolSurfaceContract struct {
	RoleScopedEntityTools bool `yaml:"role_scoped_entity_tools"`
}

type FlowTemplateInstanceDeclaration struct {
	Declared   bool     `yaml:"-"`
	By         []string `yaml:"by"`
	OnMissing  string   `yaml:"on_missing"`
	OnConflict string   `yaml:"on_conflict"`
}

func (i FlowTemplateInstanceDeclaration) Empty() bool {
	return !i.Declared && len(i.By) == 0 && strings.TrimSpace(i.OnMissing) == "" && strings.TrimSpace(i.OnConflict) == ""
}

type TemplateInstanceContract struct {
	FlowID        string
	By            []string
	OnMissing     string
	OnConflict    string
	PrimaryEntity PrimaryEntityContract
}

type SingletonCoordinatorContract struct {
	FlowID         string
	PrimaryEntity  PrimaryEntityContract
	ContainedState []SingletonCoordinatorContainedField
}

type SingletonCoordinatorContainedField struct {
	Name string
	Type string
	Kind string
}

type TemplateInstanceKeyValue struct {
	Field string
	Value string
}

func (c TemplateInstanceContract) CanonicalKeyMaterial(values map[string]any) ([]TemplateInstanceKeyValue, error) {
	if len(c.By) == 0 {
		return nil, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance.by is required", defaultPrimaryEntityFlowLabel(c.FlowID))
	}
	out := make([]TemplateInstanceKeyValue, 0, len(c.By))
	for _, field := range c.By {
		field = strings.TrimSpace(field)
		if field == "" {
			return nil, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance.by contains an empty field", defaultPrimaryEntityFlowLabel(c.FlowID))
		}
		value, ok := values[field]
		if !ok || value == nil {
			return nil, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance key field %q is missing", defaultPrimaryEntityFlowLabel(c.FlowID), field)
		}
		valueText := strings.TrimSpace(fmt.Sprint(value))
		if valueText == "" {
			return nil, fmt.Errorf("INVALID-TEMPLATE-INSTANCE: flow %s instance key field %q is empty", defaultPrimaryEntityFlowLabel(c.FlowID), field)
		}
		out = append(out, TemplateInstanceKeyValue{Field: field, Value: valueText})
	}
	return out, nil
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
	Events    []string            `yaml:"events"`
	EventPins []FlowInputEventPin `yaml:"-"`
	Reads     []string            `yaml:"reads"`
}
type FlowOutputPins struct {
	Events    []string             `yaml:"events"`
	EventPins []FlowOutputEventPin `yaml:"-"`
	Writes    []string             `yaml:"writes"`
}
type FlowInputEventPin struct {
	Name    string               `yaml:"name"`
	Event   string               `yaml:"event"`
	Address *FlowInputPinAddress `yaml:"address"`
}
type FlowOutputEventPin struct {
	Name    string   `yaml:"name"`
	Event   string   `yaml:"event"`
	Key     string   `yaml:"key"`
	Carries []string `yaml:"carries"`
}
type FlowInputPinAddress struct {
	By          string `yaml:"by"`
	Source      string `yaml:"source"`
	Target      string `yaml:"target"`
	Cardinality string `yaml:"cardinality"`
	Mode        string `yaml:"mode"`
}
type FlowPackageConnect struct {
	PackageKey string                           `yaml:"-"`
	From       string                           `yaml:"from"`
	To         string                           `yaml:"to"`
	Adapter    string                           `yaml:"adapter"`
	Using      FlowPackageConnectUsing          `yaml:"using"`
	Map        map[string]FlowPackageConnectMap `yaml:"map"`
	Delivery   string                           `yaml:"delivery"`
	Reply      map[string]string                `yaml:"reply"`
}
type FlowPackageConnectUsing struct {
	Instance FlowPackageConnectInstanceAdapter `yaml:"instance"`
}
type FlowPackageConnectInstanceAdapter struct {
	Declared bool     `yaml:"-"`
	Source   []string `yaml:"source"`
	Target   []string `yaml:"target"`
}
type FlowPackageConnectMap struct {
	Source string `yaml:"source"`
	Target string `yaml:"target"`
}
type FlowPackagePinRef struct {
	Root   bool
	FlowID string
	Pin    string
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
	Field         string                `yaml:"-" json:"field,omitempty"`
	SourceField   string                `yaml:"source_field,omitempty" json:"source_field,omitempty"`
	SourcePath    paths.Path            `yaml:"-" json:"-"`
	Operation     WorkflowDataOperation `yaml:"op,omitempty" json:"op,omitempty"`
	TargetRef     string                `yaml:"target,omitempty" json:"target,omitempty"`
	TargetField   string                `yaml:"target_field,omitempty" json:"target_field,omitempty"`
	TargetPathRef string                `yaml:"target_path,omitempty" json:"target_path,omitempty"`
	TargetPath    paths.Path            `yaml:"-" json:"-"`
	Value         ExpressionValue       `yaml:"value,omitempty" json:"value,omitempty"`
	Key           ExpressionValue       `yaml:"key,omitempty" json:"key,omitempty"`
	Index         ExpressionValue       `yaml:"index,omitempty" json:"index,omitempty"`
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
	case strings.TrimSpace(w.TargetRef) != "":
		return strings.TrimSpace(w.TargetRef)
	case strings.TrimSpace(w.TargetPathRef) != "":
		return strings.TrimSpace(w.TargetPathRef)
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

func (w WorkflowDataWrite) IsContainedOperation() bool {
	return strings.TrimSpace(string(w.Operation)) != ""
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

type EmitSpec struct {
	Event     string                     `yaml:"event"`
	Fields    map[string]ExpressionValue `yaml:"fields"`
	Target    EmitTargetSpec             `yaml:"target"`
	Broadcast bool                       `yaml:"broadcast"`
}

func (e EmitSpec) EventType() string {
	return strings.TrimSpace(e.Event)
}

func (e EmitSpec) Empty() bool {
	return strings.TrimSpace(e.Event) == ""
}

func (e EmitSpec) HasFields() bool {
	return len(e.Fields) > 0
}

func (e EmitSpec) HasTarget() bool {
	return !e.Target.Empty()
}

type EmitTargetKind string

const (
	EmitTargetKindSender     EmitTargetKind = "sender"
	EmitTargetKindInstanceID EmitTargetKind = "instance_id"
	EmitTargetKindFlowMatch  EmitTargetKind = "flow_match"
)

type EmitTargetSpec struct {
	Kind        EmitTargetKind             `yaml:"-"`
	InstanceID  string                     `yaml:"instance_id,omitempty"`
	Flow        string                     `yaml:"flow,omitempty"`
	Match       map[string]ExpressionValue `yaml:"match,omitempty"`
	AllowFanout bool                       `yaml:"allow_fanout,omitempty"`
}

func (t EmitTargetSpec) Empty() bool {
	t = t.Normalized()
	return t.Kind == "" && t.InstanceID == "" && t.Flow == "" && len(t.Match) == 0 && !t.AllowFanout
}

func (t EmitTargetSpec) Normalized() EmitTargetSpec {
	t.Kind = EmitTargetKind(strings.TrimSpace(string(t.Kind)))
	t.InstanceID = strings.TrimSpace(t.InstanceID)
	t.Flow = strings.TrimSpace(t.Flow)
	if len(t.Match) > 0 {
		out := make(map[string]ExpressionValue, len(t.Match))
		for key, value := range t.Match {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			value.hydrate()
			out[key] = value
		}
		t.Match = out
	}
	switch {
	case t.Kind == "" && t.InstanceID != "":
		t.Kind = EmitTargetKindInstanceID
	case t.Kind == "" && (t.Flow != "" || len(t.Match) > 0):
		t.Kind = EmitTargetKindFlowMatch
	}
	return t
}

func cloneExpressionValueMap(in map[string]ExpressionValue) map[string]ExpressionValue {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]ExpressionValue, len(in))
	for key, value := range in {
		value.hydrate()
		out[key] = value
	}
	return out
}

func cloneEmitSpec(spec EmitSpec) EmitSpec {
	target := spec.Target.Normalized()
	return EmitSpec{
		Event:     strings.TrimSpace(spec.Event),
		Fields:    cloneExpressionValueMap(spec.Fields),
		Target:    cloneEmitTargetSpec(target),
		Broadcast: spec.Broadcast,
	}
}

func cloneEmitTargetSpec(spec EmitTargetSpec) EmitTargetSpec {
	spec = spec.Normalized()
	return EmitTargetSpec{
		Kind:        spec.Kind,
		InstanceID:  strings.TrimSpace(spec.InstanceID),
		Flow:        strings.TrimSpace(spec.Flow),
		Match:       cloneExpressionValueMap(spec.Match),
		AllowFanout: spec.AllowFanout,
	}
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
	Action               ActionSpec                `yaml:"action"`
	CreateEntity         bool                      `yaml:"create_entity"`
	SelectEntity         *SelectEntitySpec         `yaml:"select_entity"`
	SelectOrCreateEntity *SelectOrCreateEntitySpec `yaml:"select_or_create_entity"`
	Description          string                    `yaml:"description"`
	EvidenceTarget       string                    `yaml:"evidence_target"`
	Emit                 EmitSpec                  `yaml:"emit"`
	Guard                *GuardSpec                `yaml:"guard"`
	AdvancesTo           string                    `yaml:"advances_to"`
	SetsGate             *GateSpec                 `yaml:"sets_gate"`
	ClearGates           []string                  `yaml:"clear_gates"`
	DataAccumulation     WorkflowDataAccumulation  `yaml:"data_accumulation"`
	Condition            string                    `yaml:"condition"`
	CompletionRule       string                    `yaml:"completion_rule"`
	Logic                string                    `yaml:"logic"`
	PolicyRef            string                    `yaml:"policy_ref"`
	OnComplete           []HandlerRuleEntry        `yaml:"on_complete"`
	Rules                []HandlerRuleEntry        `yaml:"rules"`
	Accumulate           *AccumulateSpec           `yaml:"accumulate"`
	Compute              *ComputeSpec              `yaml:"compute"`
	Query                *QuerySpec                `yaml:"query"`
	FanOut               *FanOutSpec               `yaml:"fan_out"`
	GroupBy              *GroupBySpec              `yaml:"group_by"`
	Filter               *FilterSpec               `yaml:"filter"`
	Reduce               *ReduceSpec               `yaml:"reduce"`
	Count                *CountSpec                `yaml:"count"`
	Clear                *ClearSpec                `yaml:"clear"`
	Branch               []BranchSpec              `yaml:"branch"`
}
type EventCatalogEntry struct {
	Swarm             EventSwarmMetadata `yaml:"swarm"`
	Note              string             `yaml:"_note"`
	Emitter           EventEmitterRef    `yaml:"emitter"`
	EmitterType       string             `yaml:"emitter_type"`
	Producer          []string           `yaml:"producer"`
	AlternateEmitters []string           `yaml:"alternate_emitters"`
	Consumer          []string           `yaml:"consumer"`
	ConsumerType      []string           `yaml:"consumer_type"`
	Source            string             `yaml:"_source"`
	Status            string             `yaml:"_status"`
	Intercepted       bool               `yaml:"intercepted"`
	Passthrough       bool               `yaml:"passthrough"`
	RuntimeHandling   string             `yaml:"runtime_handling"`
	OwningNode        string             `yaml:"owning_node"`
	DeliveryChannel   string             `yaml:"delivery_channel"`
	Payload           EventPayloadSpec   `yaml:"payload"`
	Required          []string           `yaml:"required"`
}
type EventSwarmMetadata struct {
	Note     string   `yaml:"note,omitempty"`
	Source   string   `yaml:"source,omitempty"`
	Producer []string `yaml:"producer,omitempty"`
	Consumer []string `yaml:"consumer,omitempty"`
	Status   string   `yaml:"status,omitempty"`
}

func (e EventCatalogEntry) SwarmNote() string {
	return strings.TrimSpace(e.Swarm.Note)
}

func (e EventCatalogEntry) SwarmSource() string {
	return strings.TrimSpace(e.Swarm.Source)
}

func (e EventCatalogEntry) SwarmProducer() []string {
	return normalizeStrings(e.Swarm.Producer)
}

func (e EventCatalogEntry) SwarmConsumer() []string {
	return normalizeStrings(e.Swarm.Consumer)
}

func (e EventCatalogEntry) SwarmStatus() string {
	return strings.TrimSpace(e.Swarm.Status)
}

type AgentRegistryEntry struct {
	ID                     string                          `yaml:"id"`
	Type                   string                          `yaml:"type"`
	Role                   string                          `yaml:"role"`
	PromptRef              string                          `yaml:"prompt_ref"`
	EntityWrites           map[string]AgentEntityWriteDecl `yaml:"entity_writes"`
	Permissions            []string                        `yaml:"permissions" json:"permissions,omitempty"`
	PermissionsBundle      string                          `yaml:"permissions_bundle" json:"permissions_bundle,omitempty"`
	WorkspaceClass         string                          `yaml:"workspace_class"`
	ManagerFallback        string                          `yaml:"manager_fallback"`
	NodeType               string                          `yaml:"node_type"`
	Model                  string                          `yaml:"model"`
	Mode                   string                          `yaml:"mode"`
	ConversationMode       string                          `yaml:"conversation_mode"`
	SessionScope           string                          `yaml:"session_scope"`
	MaxTurnsPerTask        int                             `yaml:"max_turns_per_task"`
	Subscriptions          []string                        `yaml:"subscriptions"`
	SubscriptionsBootstrap []string                        `yaml:"subscriptions_bootstrap"`
	SubscribesTo           []string                        `yaml:"subscribes_to"`
	Tools                  []string                        `yaml:"tools"`
	ToolsTier2             []string                        `yaml:"tools_tier2"`
	NativeTools            map[string]any                  `yaml:"native_tools"`
	FlowDataAccess         []string                        `yaml:"flow_data_access" json:"flow_data_access,omitempty"`
	EmitEvents             []string                        `yaml:"emit_events"`
	Implementation         string                          `yaml:"implementation"`
}

type agentRegistryEntryYAML AgentRegistryEntry

func (e *AgentRegistryEntry) UnmarshalYAML(value *yaml.Node) error {
	if value != nil && value.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(value.Content); i += 2 {
			switch strings.TrimSpace(value.Content[i].Value) {
			case "model_tier":
				return fmt.Errorf("RETIRED: agent field model_tier is retired; use model")
			case "conversation_mode":
				return fmt.Errorf("RETIRED: agent field conversation_mode is retired; use mode")
			case "session_scope":
				return fmt.Errorf("RETIRED: agent field session_scope is runtime-derived from mode")
			case "session_scope_authority":
				return fmt.Errorf("RETIRED: agent field session_scope_authority is platform-internal runtime state")
			}
		}
	}
	var decoded agentRegistryEntryYAML
	if err := value.Decode(&decoded); err != nil {
		return err
	}
	mode, scope, err := runtimesessions.ResolveAuthoredAgentMemoryMode(decoded.Mode)
	if err != nil {
		return fmt.Errorf("agent field mode: %w", err)
	}
	decoded.Mode = mode.String()
	decoded.ConversationMode = mode.String()
	decoded.SessionScope = scope.String()
	*e = AgentRegistryEntry(decoded)
	return nil
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
	RateLimit          string          `yaml:"rate_limit"`
	RateLimitMaxWait   string          `yaml:"rate_limit_max_wait"`
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
	APISpecification yaml.Node `yaml:"api_specification"`
	ComplianceRules  yaml.Node `yaml:"compliance_rules"`
	FileLayout       struct {
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
