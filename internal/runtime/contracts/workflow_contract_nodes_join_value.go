package contracts

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectNodeJoinValue(value yamlsource.Value) (*JoinSpec, error) {
	fields, err := nodeValueFields(value, "join", joinFieldOptions, nil)
	if err != nil {
		return nil, err
	}
	if err := joinTextScalarFields(fields, "id", "stage", "output", "until"); err != nil {
		return nil, err
	}
	out := &JoinSpec{}
	if err := nodeValueTexts(fields, map[string]*string{
		"id": &out.ID, "stage": &out.Stage, "output": &out.Output,
		"until": &out.Until,
	}, true); err != nil {
		return nil, err
	}
	out.OutputPath = paths.Parse(out.Output)
	_, out.stageFound = fields["stage"]
	_, out.outputFound = fields["output"]
	_, out.deadlineFound = fields["deadline"]
	_, out.onDeadlineFound = fields["on_deadline"]
	_, out.untilFound = fields["until"]
	if out.ID == "" {
		out.ID = out.Stage
	}
	if out.ID != "" && !joinIDPattern.MatchString(out.ID) {
		return nil, fmt.Errorf("join.id %q at %s must be a simple stable identifier", out.ID, value.Location())
	}
	if members, present := fields["members"]; present {
		out.Members, err = projectNodeJoinMembersValue(members)
		if err != nil {
			return nil, err
		}
	}
	if completion, present := fields["on_complete"]; present && completion.Presence() != yamlsource.PresenceNull {
		out.OnComplete, err = projectNodeJoinOutcomeValue(completion, "join.on_complete")
		if err != nil {
			return nil, err
		}
		out.OnCompleteFound = !joinOutcomeEmpty(out.OnComplete)
	}
	if deadline, present := fields["deadline"]; present {
		deadlineFields, err := nodeValueFields(deadline, "join.deadline", joinDeadlineFieldOptions, nil)
		if err != nil {
			return nil, err
		}
		out.Deadline = &JoinDeadlineSpec{}
		if err := joinTextScalarFields(deadlineFields, "after", "from"); err != nil {
			return nil, err
		}
		if err := nodeValueTexts(deadlineFields, map[string]*string{"after": &out.Deadline.After, "from": &out.Deadline.From}, true, "after", "from"); err != nil {
			return nil, err
		}
	}
	if outcome, present := fields["on_deadline"]; present {
		out.OnDeadline, err = projectNodeJoinOutcomeValue(outcome, "join.on_deadline")
		if err != nil {
			return nil, err
		}
		out.OnDeadlineFound = !joinOutcomeEmpty(out.OnDeadline)
	}
	if err := out.ValidateAuthoredShape(); err != nil {
		return nil, fmt.Errorf("join at %s: %w", value.Location(), err)
	}
	return out, nil
}

func projectNodeJoinMembersValue(value yamlsource.Value) (JoinMembersSpec, error) {
	fields, err := nodeValueFields(value, "join.members", joinMembersFieldOptions, nil)
	if err != nil {
		return JoinMembersSpec{}, err
	}
	if err := joinTextScalarFields(fields, "from", "by"); err != nil {
		return JoinMembersSpec{}, err
	}
	var out JoinMembersSpec
	if err := nodeValueTexts(fields, map[string]*string{"from": &out.From, "by": &out.By}, true); err != nil {
		return JoinMembersSpec{}, err
	}
	out.FromPath, out.ByPath = joinMembersSourcePath(out.From), paths.Parse(out.By)
	_, out.fromFound = fields["from"]
	_, out.BySet = fields["by"]
	if count, present := fields["count"]; present {
		scalar, err := count.Scalar()
		if err != nil || scalar.Tag != "!!int" {
			return JoinMembersSpec{}, nodeValueError(count, fmt.Errorf("join.members.count must be a nonnegative integer literal"))
		}
		out.Count = new(int)
		if err := count.Project(out.Count); err != nil {
			return JoinMembersSpec{}, nodeValueError(count, err)
		}
	}
	if fanOut, present := fields["from_fan_out"]; present {
		scalar, scalarErr := fanOut.Scalar()
		if scalarErr != nil || scalar.Tag != "!!bool" {
			return JoinMembersSpec{}, nodeValueError(fanOut, fmt.Errorf("join.members.from_fan_out must be literal true"))
		}
		out.FromFanOut, err = nodeValueBool(fanOut, "join.members.from_fan_out")
		if err != nil {
			return JoinMembersSpec{}, err
		}
		out.fromFanOutFound = true
	}
	return out, nil
}

func joinTextScalarFields(fields map[string]yamlsource.Value, names ...string) error {
	for _, name := range names {
		if value, present := fields[name]; present {
			scalar, err := value.Scalar()
			if err != nil || scalar.Tag != "!!str" {
				return nodeValueError(value, fmt.Errorf("join %s must be text", name))
			}
		}
	}
	return nil
}

func projectNodeJoinOutcomeValue(value yamlsource.Value, owner string) (HandlerRuleEntry, error) {
	fields, err := nodeValueFields(value, owner, joinOutcomeFieldOptions, nil)
	if err != nil {
		return HandlerRuleEntry{}, err
	}
	var out HandlerRuleEntry
	out.authored = true
	if advances, present := fields["advances_to"]; present {
		out.AdvancesTo, err = nodeValueText(advances, "join outcome advances_to")
		if err != nil {
			return HandlerRuleEntry{}, err
		}
	}
	if emit, present := fields["emit"]; present {
		out.Emit, err = projectNodeEmitValue(emit)
		if err != nil {
			return HandlerRuleEntry{}, err
		}
	}
	if writes, present := fields["data_accumulation"]; present {
		out.DataAccumulation, err = projectNodeDataAccumulationValue(writes)
		if err != nil {
			return HandlerRuleEntry{}, err
		}
	}
	return out, nil
}
