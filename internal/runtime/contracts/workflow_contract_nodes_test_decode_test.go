package contracts

import (
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
	"strings"
)

func decodeNodeTestSnippet(t interface {
	Helper()
	Fatal(...any)
}, snippet interface{ SourceBytes() ([]byte, error) }, target any) error {
	t.Helper()
	body, err := snippet.SourceBytes()
	if err != nil {
		t.Fatal(err)
	}
	return decodeNodeTestYAML(body, target)
}

// decodeNodeTestYAML keeps fragment fixtures on the supported nodes.yaml
// projection. Other contract families still use their own YAML admission.
func decodeNodeTestYAML(body []byte, target any) error {
	switch out := target.(type) {
	case *ToolInputSchema:
		snapshot, err := yamlsource.Load(body)
		if err != nil {
			return err
		}
		*out, err = AdmitToolInputSchemaValue(snapshot.Document("tools.yaml").Root())
		return err
	case *ToolSchemaEntry:
		snapshot, err := yamlsource.Load(body)
		if err != nil {
			return err
		}
		*out, err = projectToolValue(snapshot.Document("tools.yaml").Root())
		return err
	case *map[string]ToolSchemaEntry:
		snapshot, err := yamlsource.Load(body)
		if err != nil {
			return err
		}
		*out, err = projectToolDeclarationsValue(snapshot.Document("tools.yaml").Root())
		return err
	case *struct {
		Schema ToolInputSchema `yaml:"schema"`
	}:
		snapshot, err := yamlsource.Load(body)
		if err != nil {
			return err
		}
		lookup, err := snapshot.Document("tools.yaml").Root().Lookup("schema")
		if err != nil {
			return err
		}
		out.Schema, err = AdmitToolInputSchemaValue(lookup.Value)
		return err
	case *AgentRegistryEntry:
		snapshot, err := yamlsource.Load(body)
		if err != nil {
			return err
		}
		*out, err = projectAgentValue("worker", snapshot.Document("agents.yaml").Root())
		return err
	case *map[string]AgentRegistryEntry:
		snapshot, err := yamlsource.Load(body)
		if err != nil {
			return err
		}
		*out, err = projectAgentDeclarationsValue(snapshot.Document("agents.yaml").Root())
		return err
	case *AgentEntityWriteRule:
		snapshot, err := yamlsource.Load(body)
		if err != nil {
			return err
		}
		*out, err = projectAgentWriteRuleValue(snapshot.Document("agents.yaml").Root())
		return err
	case *agentintent.Source:
		snapshot, err := yamlsource.Load(body)
		if err != nil {
			return err
		}
		*out, err = agentintent.AdmitSource(snapshot.Document("agents.yaml").Root())
		return err
	}
	// Schema fragments enter the same complete root as bundle loading; wrapping
	// is test fixture construction, never a production admission fallback.
	var envelope string
	var project func(FlowSchemaDocument)
	switch out := target.(type) {
	case *FlowConnect:
		envelope, project = canonicalrouting.SchemaConnectParserEnvelope, func(s FlowSchemaDocument) { *out = s.Connect[0] }
	case *FlowPins:
		envelope, project = canonicalrouting.SchemaPinsParserEnvelope, func(s FlowSchemaDocument) { *out = s.Pins }
	case *FlowInputPins:
		envelope, project = canonicalrouting.SchemaInputPinsParserEnvelope, func(s FlowSchemaDocument) { *out = s.Pins.Inputs }
	case *FlowOutputPins:
		envelope, project = canonicalrouting.SchemaOutputPinsParserEnvelope, func(s FlowSchemaDocument) { *out = s.Pins.Outputs }
	case *FlowInstanceVariables:
		envelope, project = "instance_variables:", func(s FlowSchemaDocument) { *out = s.InstanceVariables }
	case *FlowVariable:
		envelope, project = "instance_variables:\n  variables:\n    value:", func(s FlowSchemaDocument) { *out = s.InstanceVariables.Variables["value"] }
	case *FlowStageDeclarations:
		envelope, project = "stages:", func(s FlowSchemaDocument) { *out = s.StageDeclarations }
	case *FlowStageGateDeclaration:
		envelope, project = "stages:\n  test:\n    gate:", func(s FlowSchemaDocument) { *out = *s.StageDeclarations.Entries[0].Gate }
	case *WorkflowGateInputField:
		envelope, project = "stages:\n  test:\n    gate:\n      decision: test\n      outcomes:\n        accept:\n          advances_to: done\n          input:\n            value:", func(s FlowSchemaDocument) {
			*out = s.StageDeclarations.Entries[0].Gate.Outcomes["accept"].Input["value"]
		}
	case *FlowLoopDeclarations:
		envelope, project = "loops:", func(s FlowSchemaDocument) { *out = s.LoopDeclarations }
	case *LoopOperationSpec:
		snapshot, err := yamlsource.Load(body)
		if err != nil {
			return err
		}
		value, err := projectNodeLoopValue(snapshot.Document("nodes.yaml").Root())
		if err == nil && value != nil {
			*out = *value
		}
		return err
	}
	if envelope != "" {
		indent := strings.Repeat("  ", strings.Count(envelope, "\n")+1)
		text := envelope + "\n" + indent + strings.ReplaceAll(strings.TrimSpace(string(body)), "\n", "\n"+indent) + "\n"
		schema, err := admitSchemaFragment(text)
		if err == nil {
			project(schema)
		}
		return err
	}
	switch target.(type) {
	case *SystemNodeContract, *map[string]SystemNodeContract,
		*SystemNodeEventHandler, *HandlerRuleEntry, *[]HandlerRuleEntry,
		*NodeStateSchema, *NodeGateStateSchema, *WorkflowTimerContract,
		*GuardSpec, *GateSpec, *AccumulateSpec, *FanOutSpec,
		*GroupBySpec, *FilterSpec, *ReduceSpec, *CountSpec,
		*QuerySpec, *ComputeSpec, *WorkflowDataWrite,
		*WorkflowDataAccumulation, *ActivitySpec, *JoinSpec, *FlowSchemaDocument,
		*ExpressionValue, *EmitSpec:
	default:
		return yaml.Unmarshal(body, target)
	}
	snapshot, err := yamlsource.Load(body)
	if err != nil {
		return err
	}
	root := snapshot.Document("nodes.yaml").Root()
	if err := root.ValidateAcyclic(); err != nil {
		return err
	}
	switch out := target.(type) {
	case *FlowSchemaDocument:
		*out, err = projectFlowSchemaValue(root)
	case *ExpressionValue:
		*out, err = projectNodeExpressionValue(root)
	case *EmitSpec:
		*out, err = projectNodeEmitValue(root)
	case *SystemNodeContract:
		*out, err = projectSystemNodeValue(root)
	case *map[string]SystemNodeContract:
		*out, err = projectNodeDeclarationsValue(root)
	case *SystemNodeEventHandler:
		*out, err = projectNodeHandlerValue(root)
	case *HandlerRuleEntry:
		*out, err = projectNodeRuleEntryValue(root, handlerRuleDecodeContextRules)
	case *[]HandlerRuleEntry:
		*out, err = projectNodeRuleRowsValue(root, handlerRuleDecodeContextRules)
	case *NodeStateSchema:
		*out, err = projectNodeStateSchemaValue(root)
	case *NodeGateStateSchema:
		*out, err = projectNodeGateStateValue(root)
	case *WorkflowTimerContract:
		*out, err = projectNodeTimerValue(root)
	case *GuardSpec:
		var spec *GuardSpec
		spec, err = projectNodeGuardValue(root)
		if spec != nil {
			*out = *spec
		}
	case *GateSpec:
		var spec *GateSpec
		spec, err = projectNodeGateEffectValue(root)
		if spec != nil {
			*out = *spec
		}
	case *AccumulateSpec:
		var spec *AccumulateSpec
		spec, err = projectNodeAccumulateValue(root)
		if spec != nil {
			*out = *spec
		}
	case *FanOutSpec:
		var spec *FanOutSpec
		spec, err = projectNodeFanOutValue(root)
		if spec != nil {
			*out = *spec
		}
	case *GroupBySpec:
		var spec *GroupBySpec
		spec, err = projectNodeGroupByValue(root)
		if spec != nil {
			*out = *spec
		}
	case *FilterSpec:
		var spec *FilterSpec
		spec, err = projectNodeFilterValue(root)
		if spec != nil {
			*out = *spec
		}
	case *ReduceSpec:
		var spec *ReduceSpec
		spec, err = projectNodeReduceValue(root)
		if spec != nil {
			*out = *spec
		}
	case *CountSpec:
		var spec *CountSpec
		spec, err = projectNodeCountValue(root)
		if spec != nil {
			*out = *spec
		}
	case *QuerySpec:
		var spec *QuerySpec
		spec, err = projectNodeQueryValue(root)
		if spec != nil {
			*out = *spec
		}
	case *ComputeSpec:
		var spec *ComputeSpec
		spec, err = projectNodeComputeValue(root)
		if spec != nil {
			*out = *spec
		}
	case *WorkflowDataWrite:
		*out, err = projectNodeDataWriteValue(root)
	case *WorkflowDataAccumulation:
		*out, err = projectNodeDataAccumulationValue(root)
	case *ActivitySpec:
		*out, err = projectNodeActivityValue(root)
	case *JoinSpec:
		var spec *JoinSpec
		spec, err = projectNodeJoinValue(root)
		if spec != nil {
			*out = *spec
		}
	}
	return err
}

func decodeNodeTestMember(body []byte, name string, target any) error {
	snapshot, err := yamlsource.Load(body)
	if err != nil {
		return err
	}
	root := snapshot.Document("nodes.yaml").Root()
	if err := root.ValidateAcyclic(); err != nil {
		return err
	}
	member, err := root.Lookup(name)
	if err != nil {
		return err
	}
	switch out := target.(type) {
	case *ExpressionValue:
		*out, err = projectNodeExpressionValue(member.Value)
	case *SystemNodeContract:
		*out, err = projectSystemNodeValue(member.Value)
	case *SystemNodeEventHandler:
		*out, err = projectNodeHandlerValue(member.Value)
	case *WorkflowTimerContract:
		*out, err = projectNodeTimerValue(member.Value)
	}
	return err
}
