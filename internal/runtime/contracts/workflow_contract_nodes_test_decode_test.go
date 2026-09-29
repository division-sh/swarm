package contracts

import (
	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

// decodeNodeTestYAML keeps fragment fixtures on the supported nodes.yaml
// projection. Other contract families still use their own YAML admission.
func decodeNodeTestYAML(body []byte, target any) error {
	switch target.(type) {
	case *SystemNodeContract, *map[string]SystemNodeContract,
		*SystemNodeEventHandler, *HandlerRuleEntry, *[]HandlerRuleEntry,
		*NodeStateSchema, *NodeGateStateSchema, *WorkflowTimerContract,
		*GuardSpec, *GateSpec, *AccumulateSpec, *FanOutSpec,
		*GroupBySpec, *FilterSpec, *ReduceSpec, *CountSpec,
		*QuerySpec, *ComputeSpec, *WorkflowDataWrite,
		*WorkflowDataAccumulation, *ActivitySpec, *JoinSpec:
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
	case *SystemNodeContract:
		*out, err = projectSystemNodeValue(member.Value)
	case *SystemNodeEventHandler:
		*out, err = projectNodeHandlerValue(member.Value)
	case *WorkflowTimerContract:
		*out, err = projectNodeTimerValue(member.Value)
	}
	return err
}
