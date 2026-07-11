package authoringview

import (
	"context"
	"fmt"
	"sort"
	"strings"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type View struct {
	WorkflowName      string                 `json:"workflow_name,omitempty"`
	WorkflowVersion   string                 `json:"workflow_version,omitempty"`
	ContractsRoot     string                 `json:"contracts_root,omitempty"`
	SourceAuthority   string                 `json:"source_authority"`
	Root              RootView               `json:"root"`
	Flows             []FlowView             `json:"flows"`
	StageGraphs       []StageGraphView       `json:"stage_graphs,omitempty"`
	ConnectRoutePlans []ConnectRoutePlanView `json:"connect_route_plans"`
	RoutePlanIssues   []RoutePlanIssueView   `json:"route_plan_issues,omitempty"`
	Diagnostics       []DiagnosticView       `json:"diagnostics,omitempty"`
	Equivalence       EquivalenceView        `json:"equivalence"`
}

type EquivalenceView struct {
	ProjectionOnly  bool     `json:"projection_only"`
	CanonicalOwners []string `json:"canonical_owners"`
}

type RootView struct {
	SourceFiles        RootSourceFiles    `json:"source_files"`
	Agents             []AgentView        `json:"agents,omitempty"`
	RequiredAgents     RequiredAgentsView `json:"required_agents"`
	PrimaryEntity      *PrimaryEntityView `json:"primary_entity,omitempty"`
	PrimaryEntityError string             `json:"primary_entity_error,omitempty"`
}

type RootSourceFiles struct {
	Schema   string `json:"schema,omitempty"`
	Entities string `json:"entities,omitempty"`
	Package  string `json:"package,omitempty"`
	Agents   string `json:"agents,omitempty"`
}

type FlowView struct {
	ID                   string                    `json:"id"`
	Path                 string                    `json:"path,omitempty"`
	Mode                 string                    `json:"mode,omitempty"`
	SourceFiles          FlowSourceFiles           `json:"source_files"`
	Agents               []AgentView               `json:"agents,omitempty"`
	RequiredAgents       RequiredAgentsView        `json:"required_agents"`
	PrimaryEntity        *PrimaryEntityView        `json:"primary_entity,omitempty"`
	PrimaryEntityError   string                    `json:"primary_entity_error,omitempty"`
	TemplateInstance     *TemplateInstanceView     `json:"template_instance,omitempty"`
	TemplateError        string                    `json:"template_instance_error,omitempty"`
	SingletonCoordinator *SingletonCoordinatorView `json:"singleton_coordinator,omitempty"`
	SingletonError       string                    `json:"singleton_coordinator_error,omitempty"`
	InputPins            []InputPinView            `json:"input_pins,omitempty"`
	OutputPins           []OutputPinView           `json:"output_pins,omitempty"`
	ContainedOperations  []ContainedOperationView  `json:"contained_operations,omitempty"`
}

type FlowSourceFiles struct {
	Package  string `json:"package,omitempty"`
	Schema   string `json:"schema,omitempty"`
	Entities string `json:"entities,omitempty"`
	Nodes    string `json:"nodes,omitempty"`
	Events   string `json:"events,omitempty"`
	Agents   string `json:"agents,omitempty"`
}

type StageGraphView struct {
	FlowID   string                 `json:"flow_id"`
	FlowPath string                 `json:"flow_path,omitempty"`
	Nodes    []StageGraphNodeView   `json:"nodes"`
	Edges    []StageGraphEdgeView   `json:"edges"`
	Timers   []StageGraphTimerView  `json:"timers,omitempty"`
	FanOuts  []StageGraphFanOutView `json:"fan_outs,omitempty"`
}

type StageGraphNodeView struct {
	ID          string `json:"id"`
	Initial     bool   `json:"initial,omitempty"`
	Terminal    bool   `json:"terminal,omitempty"`
	Description string `json:"description,omitempty"`
}

type StageGraphEdgeView struct {
	From      []string `json:"from,omitempty"`
	To        string   `json:"to"`
	Source    string   `json:"source"`
	NodeID    string   `json:"node_id,omitempty"`
	EventType string   `json:"event_type,omitempty"`
	TimerID   string   `json:"timer_id,omitempty"`
	After     string   `json:"after,omitempty"`
	Timed     bool     `json:"timed,omitempty"`
}

type StageGraphTimerView struct {
	Stage      string `json:"stage"`
	TimerID    string `json:"timer_id"`
	After      string `json:"after"`
	Emit       string `json:"emit,omitempty"`
	AdvancesTo string `json:"advances_to,omitempty"`
}

type StageGraphFanOutView struct {
	From      []string `json:"from,omitempty"`
	Emit      string   `json:"emit"`
	ItemsFrom string   `json:"items_from"`
	ItemAlias string   `json:"as"`
	Identity  string   `json:"identity"`
	Source    string   `json:"source"`
	NodeID    string   `json:"node_id,omitempty"`
	EventType string   `json:"event_type,omitempty"`
}

type RequiredAgentsView struct {
	Source     string              `json:"source"`
	SourceFile string              `json:"source_file,omitempty"`
	Agents     []RequiredAgentView `json:"agents"`
}

type RequiredAgentView struct {
	Role         string   `json:"role"`
	SubscribesTo []string `json:"subscribes_to,omitempty"`
	Emits        []string `json:"emits,omitempty"`
	Description  string   `json:"description,omitempty"`
	Source       string   `json:"source"`
	SourceFile   string   `json:"source_file,omitempty"`
}

type AgentView struct {
	ID         string                    `json:"id"`
	SourceFile string                    `json:"source_file,omitempty"`
	Fields     map[string]AgentFieldView `json:"fields,omitempty"`
}

type AgentFieldView struct {
	Value  any    `json:"value"`
	Source string `json:"source"`
}

type PrimaryEntityView struct {
	Type       string            `json:"type"`
	Fields     map[string]string `json:"fields,omitempty"`
	SourceFile string            `json:"source_file,omitempty"`
}

type TemplateInstanceView struct {
	By            []string `json:"by"`
	OnMissing     string   `json:"on_missing"`
	OnConflict    string   `json:"on_conflict"`
	PrimaryEntity string   `json:"primary_entity"`
	SourceFile    string   `json:"source_file,omitempty"`
}

type SingletonCoordinatorView struct {
	PrimaryEntity  string                        `json:"primary_entity"`
	ContainedState []SingletonContainedFieldView `json:"contained_state,omitempty"`
	SourceFile     string                        `json:"source_file,omitempty"`
}

type SingletonContainedFieldView struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Kind string `json:"kind"`
}

type InputPinView struct {
	Name          string               `json:"name"`
	Event         string               `json:"event"`
	ResolvedEvent string               `json:"resolved_event"`
	Address       *InputPinAddressView `json:"address,omitempty"`
}

type InputPinAddressView struct {
	By          string `json:"by,omitempty"`
	Source      string `json:"source,omitempty"`
	Target      string `json:"target,omitempty"`
	Cardinality string `json:"cardinality,omitempty"`
	Mode        string `json:"mode,omitempty"`
}

type OutputPinView struct {
	Name          string   `json:"name"`
	Event         string   `json:"event"`
	ResolvedEvent string   `json:"resolved_event"`
	Key           string   `json:"key,omitempty"`
	Carries       []string `json:"carries,omitempty"`
}

type ConnectRoutePlanView struct {
	PackageKey                string                  `json:"package_key,omitempty"`
	Source                    ConnectEndpointView     `json:"source"`
	Receiver                  ConnectEndpointView     `json:"receiver"`
	Adapter                   string                  `json:"adapter,omitempty"`
	Delivery                  string                  `json:"delivery"`
	TargetKind                string                  `json:"target_kind"`
	ResolutionKind            string                  `json:"resolution_kind"`
	Address                   *ConnectAddressView     `json:"address,omitempty"`
	InstanceKey               *ConnectInstanceKeyView `json:"instance_key,omitempty"`
	Reply                     *ConnectReplyView       `json:"reply,omitempty"`
	Map                       []ConnectMapEntryView   `json:"map,omitempty"`
	RequiresRuntimeResolution bool                    `json:"requires_runtime_resolution"`
	SourceFile                string                  `json:"source_file,omitempty"`
}

type ConnectEndpointView struct {
	Root          bool     `json:"root,omitempty"`
	FlowID        string   `json:"flow_id,omitempty"`
	FlowPath      string   `json:"flow_path,omitempty"`
	Mode          string   `json:"mode,omitempty"`
	Pin           string   `json:"pin"`
	Event         string   `json:"event"`
	ResolvedEvent string   `json:"resolved_event"`
	Key           string   `json:"key,omitempty"`
	Carries       []string `json:"carries,omitempty"`
}

type ConnectAddressView struct {
	By          string `json:"by,omitempty"`
	Source      string `json:"source,omitempty"`
	Target      string `json:"target,omitempty"`
	Cardinality string `json:"cardinality,omitempty"`
	Mode        string `json:"mode,omitempty"`
}

type ConnectInstanceKeyView struct {
	Fields     []string                        `json:"fields,omitempty"`
	Mappings   []ConnectInstanceKeyMappingView `json:"mappings,omitempty"`
	OnMissing  string                          `json:"on_missing,omitempty"`
	OnConflict string                          `json:"on_conflict,omitempty"`
}

type ConnectReplyView struct {
	Role              string `json:"role"`
	RequesterFlowID   string `json:"requester_flow_id"`
	RequestOutputPin  string `json:"request_output_pin"`
	ReplyInputPin     string `json:"reply_input_pin"`
	ProviderFlowID    string `json:"provider_flow_id"`
	ProviderInputPin  string `json:"provider_input_pin"`
	ProviderOutputPin string `json:"provider_output_pin"`
	CorrelationKey    string `json:"correlation_key,omitempty"`
}

type ConnectInstanceKeyMappingView struct {
	Source   string `json:"source"`
	Target   string `json:"target"`
	Explicit bool   `json:"explicit"`
}

type ConnectMapEntryView struct {
	Key    string `json:"key"`
	Source string `json:"source,omitempty"`
	Target string `json:"target,omitempty"`
}

type RoutePlanIssueView struct {
	PackageKey       string `json:"package_key,omitempty"`
	From             string `json:"from,omitempty"`
	To               string `json:"to,omitempty"`
	Failure          string `json:"failure"`
	Detail           string `json:"detail,omitempty"`
	AuthoredLocation string `json:"authored_location,omitempty"`
}

type ContainedOperationView struct {
	FlowID       string `json:"flow_id"`
	NodeID       string `json:"node_id"`
	Event        string `json:"event"`
	Scope        string `json:"scope"`
	Operation    string `json:"operation"`
	Target       string `json:"target"`
	HasKey       bool   `json:"has_key"`
	HasIndex     bool   `json:"has_index"`
	RootField    string `json:"root_field,omitempty"`
	TargetType   string `json:"target_type,omitempty"`
	MapKeyType   string `json:"map_key_type,omitempty"`
	MapValueType string `json:"map_value_type,omitempty"`
	ListItemType string `json:"list_item_type,omitempty"`
	MapScoped    bool   `json:"map_scoped,omitempty"`
	SourceFile   string `json:"source_file,omitempty"`
	Error        string `json:"error,omitempty"`
}

type DiagnosticView struct {
	CheckID          string   `json:"check_id"`
	Severity         string   `json:"severity"`
	Location         string   `json:"location,omitempty"`
	AuthoredLocation string   `json:"authored_location,omitempty"`
	Message          string   `json:"message"`
	Remediation      string   `json:"remediation,omitempty"`
	Evidence         []string `json:"evidence,omitempty"`
}

type BuildOptions struct {
	BootReport        *runtimebootverify.Report
	IncludeStageGraph bool
}

func Build(_ context.Context, source semanticview.Source, opts BuildOptions) (View, error) {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return View{}, fmt.Errorf("authoring view requires a workflow contract bundle source")
	}
	plans, routeIssues := pinrouting.LowerCompositionConnectRoutePlans(source)
	view := View{
		WorkflowName:      bundle.WorkflowName(),
		WorkflowVersion:   bundle.WorkflowVersion(),
		ContractsRoot:     strings.TrimSpace(bundle.Paths.ContractsRoot),
		SourceAuthority:   "projection_only_existing_contract_owners",
		Root:              buildRoot(bundle),
		Flows:             buildFlows(source, bundle),
		ConnectRoutePlans: buildConnectRoutePlans(bundle, plans),
		RoutePlanIssues:   buildRoutePlanIssues(bundle, routeIssues),
		Diagnostics:       buildDiagnostics(bundle, opts.BootReport),
		Equivalence: EquivalenceView{
			ProjectionOnly: true,
			CanonicalOwners: []string{
				"runtime/contracts.WorkflowContractBundle",
				"runtime/contracts effective required-agent facts",
				"runtime/contracts primary entity/template/singleton accessors",
				"runtime/core/pinrouting.LowerCompositionConnectRoutePlans",
				"runtime/entityruntime.ResolveContainedOperationTarget",
				"runtime/bootverify.Report",
			},
		},
	}
	if opts.IncludeStageGraph {
		view.StageGraphs = buildStageGraphs(source, bundle)
	}
	return view, nil
}

func buildRoot(bundle *runtimecontracts.WorkflowContractBundle) RootView {
	rootAgents, rootAgentsFile := rootAgentViewEntries(bundle)
	out := RootView{
		SourceFiles: RootSourceFiles{
			Schema:   strings.TrimSpace(bundle.Paths.RootSchemaFile),
			Entities: strings.TrimSpace(bundle.Paths.RootEntitiesFile),
			Package:  strings.TrimSpace(bundle.Paths.ProjectPackageFile),
			Agents:   strings.TrimSpace(bundle.Paths.ProjectAgentsFile),
		},
		Agents: agentViews(rootAgents, rootAgentsFile),
	}
	if bundle.RootSchema != nil {
		out.RequiredAgents = requiredAgentsView(*bundle.RootSchema, bundle.RootRequiredAgentFacts(), bundle.Paths.RootSchemaFile, bundle.Paths.ProjectAgentsFile)
	}
	declared := ""
	if bundle.RootSchema != nil {
		declared = strings.TrimSpace(bundle.RootSchema.Entity)
	}
	if declared == "" && len(bundle.RootEntities) == 0 {
		return out
	}
	primary, err := bundle.ResolveRootPrimaryEntity()
	if err != nil {
		out.PrimaryEntityError = err.Error()
		return out
	}
	out.PrimaryEntity = primaryEntityView(primary, bundle.Paths.RootEntitiesFile)
	return out
}

func buildFlows(source semanticview.Source, bundle *runtimecontracts.WorkflowContractBundle) []FlowView {
	opsByFlow := containedOperationsByFlow(source, bundle)
	views := bundle.FlowViews()
	out := make([]FlowView, 0, len(views))
	for _, flow := range views {
		flowID := strings.TrimSpace(flow.Paths.ID)
		schema := flow.Schema
		item := FlowView{
			ID:          flowID,
			Path:        strings.Trim(strings.TrimSpace(flow.Path), "/"),
			Mode:        strings.TrimSpace(schema.Mode),
			SourceFiles: flowSourceFiles(bundle, flow),
			Agents:      agentViews(flow.Agents, flow.Paths.AgentsFile),
			RequiredAgents: requiredAgentsView(
				schema,
				bundle.FlowRequiredAgentFacts(flowID),
				flow.Paths.SchemaFile,
				flow.Paths.AgentsFile,
			),
			InputPins:  inputPinViews(source, flowID, schema.Pins.Inputs.EventPins),
			OutputPins: outputPinViews(source, flowID, schema.Pins.Outputs.EventPins),
		}
		if primary, err := bundle.ResolveFlowPrimaryEntity(flowID); err == nil {
			item.PrimaryEntity = primaryEntityView(primary, flow.Paths.EntitiesFile)
		} else {
			item.PrimaryEntityError = err.Error()
		}
		if strings.TrimSpace(schema.Mode) == runtimecontracts.FlowModeTemplate || !schema.Instance.Empty() {
			if instance, err := bundle.ResolveFlowTemplateInstance(flowID); err == nil {
				item.TemplateInstance = &TemplateInstanceView{
					By:            append([]string{}, instance.By...),
					OnMissing:     strings.TrimSpace(instance.OnMissing),
					OnConflict:    strings.TrimSpace(instance.OnConflict),
					PrimaryEntity: strings.TrimSpace(instance.PrimaryEntity.EntityType),
					SourceFile:    strings.TrimSpace(flow.Paths.SchemaFile),
				}
			} else {
				item.TemplateError = err.Error()
			}
		}
		if strings.TrimSpace(schema.Mode) == runtimecontracts.FlowModeSingleton {
			if singleton, err := bundle.ResolveFlowSingletonCoordinator(flowID); err == nil {
				item.SingletonCoordinator = singletonCoordinatorView(singleton, flow.Paths.SchemaFile)
			} else {
				item.SingletonError = err.Error()
			}
		}
		item.ContainedOperations = opsByFlow[flowID]
		out = append(out, item)
	}
	return out
}

func buildStageGraphs(source semanticview.Source, bundle *runtimecontracts.WorkflowContractBundle) []StageGraphView {
	if source == nil || bundle == nil {
		return nil
	}
	graphs := make([]StageGraphView, 0, len(bundle.FlowViews())+1)
	if graph := buildStageGraphForFlow(source, "", "root", ""); len(graph.Nodes) > 0 || len(graph.Edges) > 0 || len(graph.Timers) > 0 || len(graph.FanOuts) > 0 {
		graphs = append(graphs, graph)
	}
	for _, flow := range bundle.FlowViews() {
		flowID := strings.TrimSpace(flow.Paths.ID)
		if flowID == "" {
			continue
		}
		path := strings.Trim(strings.TrimSpace(flow.Path), "/")
		if graph := buildStageGraphForFlow(source, flowID, flowID, path); len(graph.Nodes) > 0 || len(graph.Edges) > 0 || len(graph.Timers) > 0 || len(graph.FanOuts) > 0 {
			graphs = append(graphs, graph)
		}
	}
	return graphs
}

func buildStageGraphForFlow(source semanticview.Source, flowID, label, path string) StageGraphView {
	initial := strings.TrimSpace(source.FlowInitialStage(flowID))
	terminalSet := authoringStringSet(source.FlowTerminalStages(flowID))
	states := source.FlowStates(flowID)
	nodes := make([]StageGraphNodeView, 0, len(states))
	stageDescriptions := stageDescriptionsForFlow(source, flowID)
	for _, state := range states {
		state = strings.TrimSpace(state)
		if state == "" {
			continue
		}
		_, terminal := terminalSet[state]
		nodes = append(nodes, StageGraphNodeView{
			ID:          state,
			Initial:     initial != "" && state == initial,
			Terminal:    terminal,
			Description: strings.TrimSpace(stageDescriptions[state]),
		})
	}
	return StageGraphView{
		FlowID:   strings.TrimSpace(label),
		FlowPath: strings.TrimSpace(path),
		Nodes:    nodes,
		Edges:    buildStageGraphEdgesForFlow(source, flowID, initial, states, terminalSet),
		Timers:   buildStageGraphTimersForFlow(source, flowID),
		FanOuts:  buildStageGraphFanOutsForFlow(source, flowID, initial, states, terminalSet),
	}
}

func stageDescriptionsForFlow(source semanticview.Source, flowID string) map[string]string {
	out := map[string]string{}
	if source == nil {
		return out
	}
	flowID = strings.TrimSpace(flowID)
	schema, ok := stageDescriptionSchemaForFlow(source, flowID)
	if !ok || !schema.StageDeclarations.Declared {
		return out
	}
	for _, stage := range schema.StageDeclarations.Entries {
		id := strings.TrimSpace(stage.ID)
		if id == "" {
			continue
		}
		out[id] = strings.TrimSpace(stage.Description)
	}
	return out
}

type stageDescriptionRootSchemaProvider interface {
	RootFlowSchema() (runtimecontracts.FlowSchemaDocument, bool)
}

func stageDescriptionSchemaForFlow(source semanticview.Source, flowID string) (runtimecontracts.FlowSchemaDocument, bool) {
	if flowID == "" {
		if provider, ok := source.(stageDescriptionRootSchemaProvider); ok {
			return provider.RootFlowSchema()
		}
		return runtimecontracts.FlowSchemaDocument{}, false
	}
	return source.FlowSchemaByID(flowID)
}

func buildStageGraphEdgesForFlow(source semanticview.Source, flowID, initial string, states []string, terminalSet map[string]struct{}) []StageGraphEdgeView {
	stateSet := authoringStringSet(states)
	nonTerminal := make([]string, 0, len(states))
	for _, state := range states {
		state = strings.TrimSpace(state)
		if state == "" {
			continue
		}
		if _, ok := terminalSet[state]; ok {
			continue
		}
		nonTerminal = append(nonTerminal, state)
	}
	edges := make([]StageGraphEdgeView, 0)
	nodes := source.NodeEntries()
	nodeIDs := make([]string, 0, len(nodes))
	for nodeID := range nodes {
		if nodeID = strings.TrimSpace(nodeID); nodeID != "" {
			nodeIDs = append(nodeIDs, nodeID)
		}
	}
	sort.Strings(nodeIDs)
	for _, nodeID := range nodeIDs {
		node := nodes[nodeID]
		if nodeID == "" {
			continue
		}
		if !authoringNodeBelongsToFlow(source, nodeID, flowID) {
			continue
		}
		eventTypes := make([]string, 0, len(node.EventHandlers))
		for eventType := range node.EventHandlers {
			if eventType = strings.TrimSpace(eventType); eventType != "" {
				eventTypes = append(eventTypes, eventType)
			}
		}
		sort.Strings(eventTypes)
		for _, eventType := range eventTypes {
			handler := node.EventHandlers[eventType]
			from := append([]string{}, nonTerminal...)
			if handler.CreateEntity {
				from = nil
				if strings.TrimSpace(initial) != "" {
					from = []string{strings.TrimSpace(initial)}
				}
			}
			for _, carrier := range runtimecontracts.HandlerAdvanceCarriers(handler) {
				carrierFrom := from
				carrierEvent := eventType
				joinTimed := false
				if handler.Join != nil && (carrier.Kind == runtimecontracts.HandlerAdvanceCarrierJoinOnComplete || carrier.Kind == runtimecontracts.HandlerAdvanceCarrierJoinTimeout) {
					carrierFrom = []string{strings.TrimSpace(handler.Join.Stage)}
				}
				if handler.Join != nil && carrier.Kind == runtimecontracts.HandlerAdvanceCarrierJoinTimeout {
					carrierEvent = "platform.join_timeout"
					joinTimed = true
				}
				before := len(edges)
				edges = appendStageGraphEdge(edges, carrierFrom, carrier.AdvancesTo, stateSet, carrier.Source(), nodeID, carrierEvent)
				if joinTimed && len(edges) > before {
					edge := &edges[len(edges)-1]
					edge.After = strings.TrimSpace(handler.Join.Timeout.After)
					edge.TimerID = strings.TrimSpace(handler.Join.EffectiveID())
					edge.Timed = true
				}
			}
		}
	}
	for _, timer := range source.WorkflowTimers() {
		if strings.TrimSpace(timer.FlowID) != strings.TrimSpace(flowID) || !timer.StageOwned || strings.TrimSpace(timer.AdvancesTo) == "" {
			continue
		}
		edges = appendStageGraphTimedEdge(edges, []string{strings.TrimSpace(timer.Stage)}, timer.AdvancesTo, stateSet, timer)
	}
	sort.Slice(edges, func(i, j int) bool {
		left := edges[i]
		right := edges[j]
		if strings.Join(left.From, "\x00") != strings.Join(right.From, "\x00") {
			return strings.Join(left.From, "\x00") < strings.Join(right.From, "\x00")
		}
		if left.To != right.To {
			return left.To < right.To
		}
		if left.Source != right.Source {
			return left.Source < right.Source
		}
		if left.NodeID != right.NodeID {
			return left.NodeID < right.NodeID
		}
		if left.EventType != right.EventType {
			return left.EventType < right.EventType
		}
		if left.TimerID != right.TimerID {
			return left.TimerID < right.TimerID
		}
		return left.After < right.After
	})
	return edges
}

func appendStageGraphEdge(edges []StageGraphEdgeView, from []string, target string, stateSet map[string]struct{}, source, nodeID, eventType string) []StageGraphEdgeView {
	target = strings.TrimSpace(target)
	if target == "" {
		return edges
	}
	if len(stateSet) > 0 {
		if _, ok := stateSet[target]; !ok {
			return edges
		}
	}
	return append(edges, StageGraphEdgeView{
		From:      append([]string{}, from...),
		To:        target,
		Source:    strings.TrimSpace(source),
		NodeID:    strings.TrimSpace(nodeID),
		EventType: strings.TrimSpace(eventType),
	})
}

func appendStageGraphTimedEdge(edges []StageGraphEdgeView, from []string, target string, stateSet map[string]struct{}, timer runtimecontracts.WorkflowTimerContract) []StageGraphEdgeView {
	edgeCount := len(edges)
	edges = appendStageGraphEdge(edges, from, target, stateSet, "timer", "runtime", "timer:"+strings.TrimSpace(timer.ID))
	if len(edges) == edgeCount {
		return edges
	}
	edge := &edges[len(edges)-1]
	edge.TimerID = strings.TrimSpace(timer.ID)
	edge.After = strings.TrimSpace(timer.Delay)
	edge.Timed = true
	return edges
}

func buildStageGraphTimersForFlow(source semanticview.Source, flowID string) []StageGraphTimerView {
	if source == nil {
		return nil
	}
	out := make([]StageGraphTimerView, 0)
	for _, timer := range source.WorkflowTimers() {
		if strings.TrimSpace(timer.FlowID) != strings.TrimSpace(flowID) || !timer.StageOwned {
			continue
		}
		emit := strings.TrimSpace(timer.Event)
		if emit == runtimecontracts.WorkflowStageTimerInternalEvent {
			emit = ""
		}
		out = append(out, StageGraphTimerView{
			Stage:      strings.TrimSpace(timer.Stage),
			TimerID:    strings.TrimSpace(timer.ID),
			After:      strings.TrimSpace(timer.Delay),
			Emit:       emit,
			AdvancesTo: strings.TrimSpace(timer.AdvancesTo),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Stage != out[j].Stage {
			return out[i].Stage < out[j].Stage
		}
		if out[i].After != out[j].After {
			return out[i].After < out[j].After
		}
		return out[i].TimerID < out[j].TimerID
	})
	return out
}

func buildStageGraphFanOutsForFlow(source semanticview.Source, flowID, initial string, states []string, terminalSet map[string]struct{}) []StageGraphFanOutView {
	if source == nil {
		return nil
	}
	nonTerminal := make([]string, 0, len(states))
	for _, state := range states {
		state = strings.TrimSpace(state)
		if state == "" {
			continue
		}
		if _, ok := terminalSet[state]; ok {
			continue
		}
		nonTerminal = append(nonTerminal, state)
	}
	out := make([]StageGraphFanOutView, 0)
	nodes := source.NodeEntries()
	for _, nodeID := range sortedAuthoringNodeIDs(nodes) {
		node := nodes[nodeID]
		if !authoringNodeBelongsToFlow(source, nodeID, flowID) {
			continue
		}
		eventTypes := make([]string, 0, len(node.EventHandlers))
		for eventType := range node.EventHandlers {
			if eventType = strings.TrimSpace(eventType); eventType != "" {
				eventTypes = append(eventTypes, eventType)
			}
		}
		sort.Strings(eventTypes)
		for _, eventType := range eventTypes {
			handler := node.EventHandlers[eventType]
			from := append([]string{}, nonTerminal...)
			if handler.CreateEntity {
				from = nil
				if strings.TrimSpace(initial) != "" {
					from = []string{strings.TrimSpace(initial)}
				}
			}
			out = append(out, fanOutViewsForHandler(from, strings.TrimSpace(nodeID), eventType, handler)...)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		left := out[i]
		right := out[j]
		if strings.Join(left.From, "\x00") != strings.Join(right.From, "\x00") {
			return strings.Join(left.From, "\x00") < strings.Join(right.From, "\x00")
		}
		if left.Emit != right.Emit {
			return left.Emit < right.Emit
		}
		if left.Source != right.Source {
			return left.Source < right.Source
		}
		if left.NodeID != right.NodeID {
			return left.NodeID < right.NodeID
		}
		return left.EventType < right.EventType
	})
	return out
}

func authoringNodeBelongsToFlow(source semanticview.Source, nodeID, flowID string) bool {
	sourceInfo, ok := source.NodeContractSource(nodeID)
	if ok {
		return strings.TrimSpace(sourceInfo.FlowID) == strings.TrimSpace(flowID)
	}
	return strings.TrimSpace(flowID) == ""
}

func fanOutViewsForHandler(from []string, nodeID, eventType string, handler runtimecontracts.SystemNodeEventHandler) []StageGraphFanOutView {
	out := make([]StageGraphFanOutView, 0, 5)
	add := func(source string, spec *runtimecontracts.FanOutSpec) {
		if spec == nil {
			return
		}
		emit := strings.TrimSpace(spec.Emit.EventType())
		if emit == "" {
			return
		}
		out = append(out, StageGraphFanOutView{
			From:      append([]string{}, from...),
			Emit:      emit,
			ItemsFrom: strings.TrimSpace(spec.ItemsFrom),
			ItemAlias: strings.TrimSpace(spec.As),
			Identity:  strings.TrimSpace(spec.Identity),
			Source:    strings.TrimSpace(source),
			NodeID:    strings.TrimSpace(nodeID),
			EventType: strings.TrimSpace(eventType),
		})
	}
	add("handler.fan_out", handler.FanOut)
	for idx := range handler.Rules {
		add(indexedHandlerGraphSource("handler.rules", idx, handler.Rules[idx].ID)+".fan_out", handler.Rules[idx].FanOut)
	}
	for idx := range handler.OnComplete {
		add(indexedHandlerGraphSource("handler.on_complete", idx, handler.OnComplete[idx].ID)+".fan_out", handler.OnComplete[idx].FanOut)
	}
	if handler.Accumulate != nil {
		for idx := range handler.Accumulate.OnComplete {
			add(indexedHandlerGraphSource("handler.accumulate.on_complete", idx, handler.Accumulate.OnComplete[idx].ID)+".fan_out", handler.Accumulate.OnComplete[idx].FanOut)
		}
		if handler.Accumulate.OnTimeout != nil {
			add(indexedHandlerGraphSource("handler.accumulate.on_timeout", 0, handler.Accumulate.OnTimeout.ID)+".fan_out", handler.Accumulate.OnTimeout.FanOut)
		}
	}
	return out
}

func sortedAuthoringNodeIDs(nodes map[string]runtimecontracts.SystemNodeContract) []string {
	nodeIDs := make([]string, 0, len(nodes))
	for nodeID := range nodes {
		if nodeID = strings.TrimSpace(nodeID); nodeID != "" {
			nodeIDs = append(nodeIDs, nodeID)
		}
	}
	sort.Strings(nodeIDs)
	return nodeIDs
}

func indexedHandlerGraphSource(prefix string, idx int, id string) string {
	id = strings.TrimSpace(id)
	if id != "" {
		return fmt.Sprintf("%s[%d:%s]", strings.TrimSpace(prefix), idx, id)
	}
	return fmt.Sprintf("%s[%d]", strings.TrimSpace(prefix), idx)
}

func authoringStringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func rootAgentViewEntries(bundle *runtimecontracts.WorkflowContractBundle) (map[string]runtimecontracts.AgentRegistryEntry, string) {
	if bundle == nil {
		return nil, ""
	}
	for _, view := range bundle.ProjectViews() {
		if strings.TrimSpace(view.Paths.ParentKey) == "" && view.Paths.Depth == 0 {
			return view.Agents, strings.TrimSpace(view.Paths.ProjectAgentsFile)
		}
	}
	for _, view := range bundle.ProjectViews() {
		if strings.TrimSpace(view.Paths.ParentKey) == "" {
			return view.Agents, strings.TrimSpace(view.Paths.ProjectAgentsFile)
		}
	}
	return bundle.AgentEntries(), strings.TrimSpace(bundle.Paths.ProjectAgentsFile)
}

func agentViews(entries map[string]runtimecontracts.AgentRegistryEntry, sourceFile string) []AgentView {
	if len(entries) == 0 {
		return nil
	}
	ids := make([]string, 0, len(entries))
	for id := range entries {
		if id = strings.TrimSpace(id); id != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	out := make([]AgentView, 0, len(ids))
	for _, id := range ids {
		entry := runtimecontracts.EffectiveAgentRegistryEntry(id, entries[id])
		fields := map[string]AgentFieldView{}
		addAgentField(fields, entry, "type", entry.Type)
		addAgentField(fields, entry, "model", entry.Model)
		addAgentField(fields, entry, "mode", entry.Mode)
		addAgentField(fields, entry, "session_scope", entry.SessionScope)
		addAgentField(fields, entry, "max_turns_per_task", entry.MaxTurnsPerTask)
		addAgentField(fields, entry, "workspace_class", entry.WorkspaceClass)
		addAgentField(fields, entry, "manager_fallback", entry.ManagerFallback)
		out = append(out, AgentView{
			ID:         id,
			SourceFile: strings.TrimSpace(sourceFile),
			Fields:     fields,
		})
	}
	return out
}

func addAgentField(fields map[string]AgentFieldView, entry runtimecontracts.AgentRegistryEntry, name string, value any) {
	source := entry.EffectiveSourceForField(name)
	if strings.TrimSpace(source) == "" {
		switch typed := value.(type) {
		case string:
			if strings.TrimSpace(typed) == "" {
				return
			}
			source = runtimecontracts.AgentFieldSourceAuthored
		case int:
			if typed == 0 {
				return
			}
			source = runtimecontracts.AgentFieldSourceAuthored
		default:
			if value == nil {
				return
			}
			source = runtimecontracts.AgentFieldSourceAuthored
		}
	}
	fields[name] = AgentFieldView{
		Value:  value,
		Source: source,
	}
}

func flowSourceFiles(bundle *runtimecontracts.WorkflowContractBundle, flow runtimecontracts.FlowContractView) FlowSourceFiles {
	return FlowSourceFiles{
		Package:  packageSourceFile(bundle, flow.Paths.PackageKey),
		Schema:   strings.TrimSpace(flow.Paths.SchemaFile),
		Entities: strings.TrimSpace(flow.Paths.EntitiesFile),
		Nodes:    strings.TrimSpace(flow.Paths.NodesFile),
		Events:   strings.TrimSpace(flow.Paths.EventsFile),
		Agents:   strings.TrimSpace(flow.Paths.AgentsFile),
	}
}

func requiredAgentsView(schema runtimecontracts.FlowSchemaDocument, facts []runtimecontracts.RequiredAgentFact, schemaFile, agentsFile string) RequiredAgentsView {
	source := runtimecontracts.RequiredAgentSourceInferred
	sourceFile := strings.TrimSpace(agentsFile)
	if runtimecontracts.RequiredAgentsDeclared(schema) {
		source = runtimecontracts.RequiredAgentSourceExplicit
		sourceFile = strings.TrimSpace(schemaFile)
	}
	out := RequiredAgentsView{
		Source:     source,
		SourceFile: sourceFile,
		Agents:     make([]RequiredAgentView, 0, len(facts)),
	}
	for _, fact := range facts {
		out.Agents = append(out.Agents, RequiredAgentView{
			Role:         strings.TrimSpace(fact.Role),
			SubscribesTo: normalizedStrings(fact.SubscribesTo),
			Emits:        normalizedStrings(fact.Emits),
			Description:  strings.TrimSpace(fact.Description),
			Source:       strings.TrimSpace(fact.Source),
			SourceFile:   strings.TrimSpace(fact.SourceFile),
		})
	}
	return out
}

func primaryEntityView(primary runtimecontracts.PrimaryEntityContract, sourceFile string) *PrimaryEntityView {
	fields := map[string]string{}
	for _, field := range sortedEntityFields(primary.Contract.Fields) {
		fields[field] = strings.TrimSpace(primary.Contract.Fields[field].Type)
	}
	return &PrimaryEntityView{
		Type:       strings.TrimSpace(primary.EntityType),
		Fields:     fields,
		SourceFile: strings.TrimSpace(sourceFile),
	}
}

func singletonCoordinatorView(singleton runtimecontracts.SingletonCoordinatorContract, sourceFile string) *SingletonCoordinatorView {
	fields := make([]SingletonContainedFieldView, 0, len(singleton.ContainedState))
	for _, field := range singleton.ContainedState {
		fields = append(fields, SingletonContainedFieldView{
			Name: strings.TrimSpace(field.Name),
			Type: strings.TrimSpace(field.Type),
			Kind: strings.TrimSpace(field.Kind),
		})
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })
	return &SingletonCoordinatorView{
		PrimaryEntity:  strings.TrimSpace(singleton.PrimaryEntity.EntityType),
		ContainedState: fields,
		SourceFile:     strings.TrimSpace(sourceFile),
	}
}

func inputPinViews(source semanticview.Source, flowID string, pins []runtimecontracts.FlowInputEventPin) []InputPinView {
	out := make([]InputPinView, 0, len(pins))
	for _, pin := range pins {
		item := InputPinView{
			Name:          strings.TrimSpace(pin.PinName()),
			Event:         strings.TrimSpace(pin.EventType()),
			ResolvedEvent: strings.TrimSpace(source.ResolveFlowEventReference(flowID, pin.EventType())),
		}
		if pin.Address != nil {
			item.Address = &InputPinAddressView{
				By:          strings.TrimSpace(pin.Address.By),
				Source:      strings.TrimSpace(pin.Address.Source),
				Target:      strings.TrimSpace(pin.Address.Target),
				Cardinality: strings.TrimSpace(pin.Address.Cardinality),
				Mode:        strings.TrimSpace(pin.Address.Mode),
			}
		}
		out = append(out, item)
	}
	return out
}

func outputPinViews(source semanticview.Source, flowID string, pins []runtimecontracts.FlowOutputEventPin) []OutputPinView {
	out := make([]OutputPinView, 0, len(pins))
	for _, pin := range pins {
		out = append(out, OutputPinView{
			Name:          strings.TrimSpace(pin.PinName()),
			Event:         strings.TrimSpace(pin.EventType()),
			ResolvedEvent: strings.TrimSpace(source.ResolveFlowEventReference(flowID, pin.EventType())),
			Key:           strings.TrimSpace(pin.Key),
			Carries:       normalizedStrings(pin.Carries),
		})
	}
	return out
}

func buildConnectRoutePlans(bundle *runtimecontracts.WorkflowContractBundle, plans []pinrouting.ConnectRoutePlan) []ConnectRoutePlanView {
	out := make([]ConnectRoutePlanView, 0, len(plans))
	for _, plan := range plans {
		item := ConnectRoutePlanView{
			PackageKey:                strings.TrimSpace(plan.PackageKey),
			Source:                    connectEndpointView(plan.Source),
			Receiver:                  connectEndpointView(plan.Receiver),
			Adapter:                   strings.TrimSpace(plan.Adapter),
			Delivery:                  string(plan.Delivery),
			TargetKind:                string(plan.TargetKind),
			ResolutionKind:            string(plan.ResolutionKind),
			Map:                       connectMapEntryViews(plan.Map),
			RequiresRuntimeResolution: plan.RequiresRuntimeResolution,
			SourceFile:                packageSourceFile(bundle, plan.PackageKey),
		}
		if plan.Address != nil {
			item.Address = &ConnectAddressView{
				By:          strings.TrimSpace(plan.Address.By),
				Source:      strings.TrimSpace(plan.Address.Source),
				Target:      strings.TrimSpace(plan.Address.Target),
				Cardinality: strings.TrimSpace(plan.Address.Cardinality),
				Mode:        strings.TrimSpace(plan.Address.Mode),
			}
		}
		if plan.InstanceKey != nil {
			item.InstanceKey = connectInstanceKeyView(plan.InstanceKey)
		}
		if plan.ReplyResolution != nil {
			item.Reply = &ConnectReplyView{
				Role:              strings.TrimSpace(plan.ReplyResolution.Role),
				RequesterFlowID:   strings.TrimSpace(plan.ReplyResolution.RequesterFlowID),
				RequestOutputPin:  strings.TrimSpace(plan.ReplyResolution.RequestOutputPin),
				ReplyInputPin:     strings.TrimSpace(plan.ReplyResolution.ReplyInputPin),
				ProviderFlowID:    strings.TrimSpace(plan.ReplyResolution.ProviderFlowID),
				ProviderInputPin:  strings.TrimSpace(plan.ReplyResolution.ProviderInputPin),
				ProviderOutputPin: strings.TrimSpace(plan.ReplyResolution.ProviderOutputPin),
				CorrelationKey:    strings.TrimSpace(plan.ReplyResolution.CorrelationKey),
			}
		}
		out = append(out, item)
	}
	return out
}

func connectEndpointView(endpoint pinrouting.ConnectRoutePlanEndpoint) ConnectEndpointView {
	return ConnectEndpointView{
		Root:          endpoint.Root,
		FlowID:        strings.TrimSpace(endpoint.FlowID),
		FlowPath:      strings.TrimSpace(endpoint.FlowPath),
		Mode:          strings.TrimSpace(endpoint.Mode),
		Pin:           strings.TrimSpace(endpoint.Pin),
		Event:         strings.TrimSpace(endpoint.Event),
		ResolvedEvent: strings.TrimSpace(endpoint.ResolvedEvent),
		Key:           strings.TrimSpace(endpoint.Key),
		Carries:       normalizedStrings(endpoint.Carries),
	}
}

func connectInstanceKeyView(instance *pinrouting.ConnectRoutePlanInstanceKey) *ConnectInstanceKeyView {
	if instance == nil {
		return nil
	}
	mappings := make([]ConnectInstanceKeyMappingView, 0, len(instance.Mappings))
	for _, mapping := range instance.Mappings {
		mappings = append(mappings, ConnectInstanceKeyMappingView{
			Source:   strings.TrimSpace(mapping.Source),
			Target:   strings.TrimSpace(mapping.Target),
			Explicit: mapping.Explicit,
		})
	}
	return &ConnectInstanceKeyView{
		Fields:     normalizedStrings(instance.Fields),
		Mappings:   mappings,
		OnMissing:  strings.TrimSpace(instance.OnMissing),
		OnConflict: strings.TrimSpace(instance.OnConflict),
	}
}

func connectMapEntryViews(entries []pinrouting.ConnectRoutePlanMapEntry) []ConnectMapEntryView {
	out := make([]ConnectMapEntryView, 0, len(entries))
	for _, entry := range entries {
		out = append(out, ConnectMapEntryView{
			Key:    strings.TrimSpace(entry.Key),
			Source: strings.TrimSpace(entry.Source),
			Target: strings.TrimSpace(entry.Target),
		})
	}
	return out
}

func buildRoutePlanIssues(bundle *runtimecontracts.WorkflowContractBundle, issues []pinrouting.ConnectRoutePlanIssue) []RoutePlanIssueView {
	out := make([]RoutePlanIssueView, 0, len(issues))
	for _, issue := range issues {
		out = append(out, RoutePlanIssueView{
			PackageKey:       strings.TrimSpace(issue.Connect.PackageKey),
			From:             strings.TrimSpace(issue.Connect.From),
			To:               strings.TrimSpace(issue.Connect.To),
			Failure:          string(issue.Failure),
			Detail:           strings.TrimSpace(issue.Detail),
			AuthoredLocation: packageSourceFile(bundle, issue.Connect.PackageKey),
		})
	}
	return out
}

type containedOperationRef struct {
	FlowID    string
	NodeID    string
	EventType string
	Scope     string
	Write     runtimecontracts.WorkflowDataWrite
}

func containedOperationsByFlow(source semanticview.Source, bundle *runtimecontracts.WorkflowContractBundle) map[string][]ContainedOperationView {
	out := map[string][]ContainedOperationView{}
	if source == nil || bundle == nil {
		return out
	}
	for _, project := range bundle.ProjectViews() {
		for _, nodeID := range sortedNodeIDs(project.Nodes) {
			flowID := ""
			sourceFile := strings.TrimSpace(project.Paths.ProjectNodesFile)
			if sourceRef, ok := source.NodeContractSource(nodeID); ok {
				flowID = strings.TrimSpace(sourceRef.FlowID)
				sourceFile = firstNonEmpty(authoredFileForSource(bundle, sourceRef), sourceFile)
			}
			appendContainedOperations(out, source, flowID, nodeID, sourceFile, project.Nodes[nodeID])
		}
	}
	for _, flow := range bundle.FlowViews() {
		flowID := strings.TrimSpace(flow.Paths.ID)
		sourceFile := strings.TrimSpace(flow.Paths.NodesFile)
		for _, nodeID := range sortedNodeIDs(flow.Nodes) {
			appendContainedOperations(out, source, flowID, nodeID, sourceFile, flow.Nodes[nodeID])
		}
	}
	return out
}

func appendContainedOperations(out map[string][]ContainedOperationView, source semanticview.Source, flowID, nodeID, sourceFile string, node runtimecontracts.SystemNodeContract) {
	for _, ref := range containedOperationRefs(flowID, nodeID, node) {
		op := ContainedOperationView{
			FlowID:     ref.FlowID,
			NodeID:     ref.NodeID,
			Event:      ref.EventType,
			Scope:      ref.Scope,
			Operation:  strings.TrimSpace(string(ref.Write.Operation)),
			Target:     strings.TrimSpace(ref.Write.Target()),
			HasKey:     !ref.Write.Key.IsZero(),
			HasIndex:   !ref.Write.Index.IsZero(),
			SourceFile: strings.TrimSpace(sourceFile),
		}
		contract, ok := entityruntime.ResolveForFlow(source, ref.FlowID)
		if !ok {
			op.Error = "flow has no declared entity contract"
			out[ref.FlowID] = append(out[ref.FlowID], op)
			continue
		}
		target, err := entityruntime.ResolveContainedOperationTarget(contract, ref.Write.Target(), string(ref.Write.Operation), !ref.Write.Key.IsZero(), !ref.Write.Index.IsZero())
		if err != nil {
			op.Error = err.Error()
			out[ref.FlowID] = append(out[ref.FlowID], op)
			continue
		}
		op.RootField = strings.TrimSpace(target.RootField)
		op.TargetType = strings.TrimSpace(target.TargetType)
		op.MapKeyType = strings.TrimSpace(target.MapKeyType)
		op.MapValueType = strings.TrimSpace(target.MapValueType)
		op.ListItemType = strings.TrimSpace(target.ListItemType)
		op.MapScoped = target.MapScoped
		out[ref.FlowID] = append(out[ref.FlowID], op)
	}
}

func containedOperationRefs(flowID, nodeID string, node runtimecontracts.SystemNodeContract) []containedOperationRef {
	out := make([]containedOperationRef, 0)
	for _, eventType := range sortedHandlerEvents(node.EventHandlers) {
		handler := node.EventHandlers[eventType]
		out = append(out, writeRefs(flowID, nodeID, eventType, "handler.data_accumulation", handler.DataAccumulation.Writes)...)
		for idx, rule := range handler.Rules {
			out = append(out, writeRefs(flowID, nodeID, eventType, ruleScope("handler.rules", idx, rule.ID)+".data_accumulation", rule.DataAccumulation.Writes)...)
		}
		for idx, rule := range handler.OnComplete {
			out = append(out, writeRefs(flowID, nodeID, eventType, ruleScope("handler.on_complete", idx, rule.ID)+".data_accumulation", rule.DataAccumulation.Writes)...)
		}
		if handler.Accumulate != nil {
			for idx, rule := range handler.Accumulate.OnComplete {
				out = append(out, writeRefs(flowID, nodeID, eventType, ruleScope("handler.accumulate.on_complete", idx, rule.ID)+".data_accumulation", rule.DataAccumulation.Writes)...)
			}
			if handler.Accumulate.OnTimeout != nil {
				scope := "handler.accumulate.on_timeout"
				if id := strings.TrimSpace(handler.Accumulate.OnTimeout.ID); id != "" {
					scope += "[" + id + "]"
				}
				out = append(out, writeRefs(flowID, nodeID, eventType, scope+".data_accumulation", handler.Accumulate.OnTimeout.DataAccumulation.Writes)...)
			}
		}
	}
	return out
}

func writeRefs(flowID, nodeID, eventType, scope string, writes []runtimecontracts.WorkflowDataWrite) []containedOperationRef {
	out := make([]containedOperationRef, 0, len(writes))
	for _, write := range writes {
		if !write.IsContainedOperation() {
			continue
		}
		out = append(out, containedOperationRef{
			FlowID:    strings.TrimSpace(flowID),
			NodeID:    strings.TrimSpace(nodeID),
			EventType: strings.TrimSpace(eventType),
			Scope:     strings.TrimSpace(scope),
			Write:     write,
		})
	}
	return out
}

func ruleScope(prefix string, idx int, id string) string {
	if id = strings.TrimSpace(id); id != "" {
		return prefix + "[" + id + "]"
	}
	return fmt.Sprintf("%s[%d]", prefix, idx)
}

func buildDiagnostics(bundle *runtimecontracts.WorkflowContractBundle, report *runtimebootverify.Report) []DiagnosticView {
	if report == nil {
		return nil
	}
	findings := append([]runtimebootverify.Finding{}, report.Findings...)
	out := make([]DiagnosticView, 0, len(findings))
	for _, finding := range findings {
		out = append(out, DiagnosticView{
			CheckID:          strings.TrimSpace(finding.CheckID),
			Severity:         strings.TrimSpace(finding.Severity),
			Location:         strings.TrimSpace(finding.Location),
			AuthoredLocation: authoredLocationForFinding(bundle, finding),
			Message:          strings.TrimSpace(finding.Message),
			Remediation:      strings.TrimSpace(finding.Remediation),
			Evidence:         trimDiagnosticEvidence(finding.Evidence),
		})
	}
	return out
}

func trimDiagnosticEvidence(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func authoredLocationForFinding(bundle *runtimecontracts.WorkflowContractBundle, finding runtimebootverify.Finding) string {
	if bundle == nil {
		return ""
	}
	location := strings.TrimSpace(finding.Location)
	if location == "" || location == "<root>" || location == "root" {
		return firstNonEmpty(bundle.Paths.RootSchemaFile, bundle.Paths.ProjectPackageFile)
	}
	if source, ok := bundle.NodeContractSource(location); ok {
		return authoredFileForSource(bundle, source)
	}
	if flow, ok := bundle.FlowViewByID(location); ok {
		return strings.TrimSpace(flow.Paths.SchemaFile)
	}
	if project, ok := bundle.ProjectViewByKey(location); ok {
		return strings.TrimSpace(project.Paths.PackageFile)
	}
	for _, flow := range bundle.FlowViews() {
		if strings.Contains(location, strings.TrimSpace(flow.Paths.ID)) {
			return strings.TrimSpace(flow.Paths.SchemaFile)
		}
	}
	return firstNonEmpty(bundle.Paths.ProjectPackageFile, bundle.Paths.RootSchemaFile)
}

func authoredFileForSource(bundle *runtimecontracts.WorkflowContractBundle, source runtimecontracts.ContractItemSource) string {
	if file := strings.TrimSpace(source.File); file != "" {
		return file
	}
	if flowID := strings.TrimSpace(source.FlowID); flowID != "" {
		if flow, ok := bundle.FlowViewByID(flowID); ok {
			switch strings.TrimSpace(source.Layer) {
			case "nodes", "node", "flow":
				return firstNonEmpty(flow.Paths.NodesFile, flow.Paths.SchemaFile)
			case "events", "event":
				return firstNonEmpty(flow.Paths.EventsFile, flow.Paths.SchemaFile)
			default:
				return firstNonEmpty(flow.Paths.NodesFile, flow.Paths.SchemaFile)
			}
		}
	}
	if packageKey := strings.TrimSpace(source.PackageKey); packageKey != "" {
		return packageSourceFile(bundle, packageKey)
	}
	return ""
}

func packageSourceFile(bundle *runtimecontracts.WorkflowContractBundle, packageKey string) string {
	if bundle == nil {
		return ""
	}
	packageKey = strings.TrimSpace(packageKey)
	if packageKey != "" {
		if project, ok := bundle.ProjectViewByKey(packageKey); ok {
			return strings.TrimSpace(project.Paths.PackageFile)
		}
	}
	return strings.TrimSpace(bundle.Paths.ProjectPackageFile)
}

func sortedEntityFields(fields map[string]runtimecontracts.EntityFieldDecl) []string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		key = strings.TrimSpace(key)
		if key != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func sortedNodeIDs(nodes map[string]runtimecontracts.SystemNodeContract) []string {
	keys := make([]string, 0, len(nodes))
	for key := range nodes {
		key = strings.TrimSpace(key)
		if key != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func sortedHandlerEvents(handlers map[string]runtimecontracts.SystemNodeEventHandler) []string {
	keys := make([]string, 0, len(handlers))
	for key := range handlers {
		key = strings.TrimSpace(key)
		if key != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func normalizedStrings(in []string) []string {
	out := make([]string, 0, len(in))
	for _, value := range in {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
