package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectNodeJoinValue(value yamlsource.Value) (*JoinSpec, error) {
	fields, err := nodeValueFields(value, "join", joinFieldOptions, nil)
	if err != nil {
		return nil, err
	}
	out := &JoinSpec{}
	if err := nodeValueTexts(fields, map[string]*string{
		"id": &out.ID, "stage": &out.Stage, "output": &out.Output,
		"complete_when": &out.CompleteWhen, "remaining": &out.Remaining,
	}, true); err != nil {
		return nil, err
	}
	out.OutputPath = paths.Parse(out.Output)
	out.Remaining = strings.ToLower(out.Remaining)
	_, out.stageFound = fields["stage"]
	_, out.windowFound = fields["window"]
	_, out.outputFound = fields["output"]
	_, out.completeFound = fields["complete_when"]
	_, out.remainingFound = fields["remaining"]
	_, out.timeoutFound = fields["timeout"]
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
	if window, present := fields["window"]; present && window.Presence() != yamlsource.PresenceNull {
		out.Window, err = projectNodeJoinWindowValue(window)
		if err != nil {
			return nil, err
		}
	}
	if completion, present := fields["on_complete"]; present && completion.Presence() != yamlsource.PresenceNull {
		out.OnComplete, err = projectNodeJoinOutcomeValue(completion)
		if err != nil {
			return nil, err
		}
		out.OnCompleteFound = !joinOutcomeEmpty(out.OnComplete)
	}
	if timeout, present := fields["timeout"]; present && timeout.Presence() != yamlsource.PresenceNull {
		timeoutFields, err := nodeValueFields(timeout, "join.timeout", joinTimeoutFieldOptions, nil)
		if err != nil {
			return nil, err
		}
		out.TimeoutFound = true
		if after, present := timeoutFields["after"]; present {
			out.Timeout.After, err = nodeValueText(after, "join.timeout.after")
			if err != nil {
				return nil, err
			}
			out.Timeout.After = strings.TrimSpace(out.Timeout.After)
		}
		out.Timeout.Outcome, err = projectNodeJoinOutcomeFields(timeoutFields)
		if err != nil {
			return nil, err
		}
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
	var out JoinMembersSpec
	if err := nodeValueTexts(fields, map[string]*string{"from": &out.From, "by": &out.By}, true); err != nil {
		return JoinMembersSpec{}, err
	}
	out.FromPath, out.ByPath = paths.Parse(out.From), paths.Parse(out.By)
	_, out.fromFound = fields["from"]
	_, out.BySet = fields["by"]
	if fanOut, present := fields["from_fan_out"]; present {
		out.FromFanOut, err = nodeValueBool(fanOut, "join.members.from_fan_out")
		if err != nil {
			return JoinMembersSpec{}, err
		}
		out.fromFanOutFound = true
	}
	return out, nil
}

func projectNodeJoinWindowValue(value yamlsource.Value) (*JoinWindowSpec, error) {
	fields, err := nodeValueFields(value, "join.window", joinWindowFieldOptions, nil)
	if err != nil {
		return nil, err
	}
	out := &JoinWindowSpec{}
	if err := nodeValueTexts(fields, map[string]*string{"from": &out.From, "by": &out.By}, true); err != nil {
		return nil, err
	}
	out.FromPath, out.ByPath = paths.Parse(out.From), paths.Parse(out.By)
	_, out.BySet = fields["by"]
	return out, nil
}

func projectNodeJoinOutcomeValue(value yamlsource.Value) (HandlerRuleEntry, error) {
	fields, err := nodeValueFields(value, "join.on_complete", joinOutcomeFieldOptions, nil)
	if err != nil {
		return HandlerRuleEntry{}, err
	}
	return projectNodeJoinOutcomeFields(fields)
}

func projectNodeJoinOutcomeFields(fields map[string]yamlsource.Value) (HandlerRuleEntry, error) {
	var out HandlerRuleEntry
	out.authored = true
	var err error
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
