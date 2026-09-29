package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectNodeActivityValue(value yamlsource.Value) (ActivitySpec, error) {
	if value.Presence() == yamlsource.PresenceNull {
		return ActivitySpec{}, nil
	}
	fields, err := nodeValueFields(value, "activity", activityFieldOptions, nil)
	if err != nil {
		return ActivitySpec{}, err
	}
	var out ActivitySpec
	if id, present := fields["id"]; present {
		out.ID, err = nodeValueText(id, "activity.id")
		if err != nil {
			return ActivitySpec{}, err
		}
		out.ID = strings.TrimSpace(out.ID)
	}
	if tool, present := fields["tool"]; present {
		out.Tool, err = nodeValueText(tool, "activity.tool")
		if err != nil {
			return ActivitySpec{}, err
		}
		out.Tool = strings.TrimSpace(out.Tool)
	}
	if input, present := fields["input"]; present {
		out.Input, err = projectNodeExpressionFields(input, "activity.input")
		if err != nil {
			return ActivitySpec{}, err
		}
	}
	if approval, present := fields["approval"]; present {
		approvalFields, err := nodeValueFields(approval, "activity.approval", activityApprovalFieldOptions, nil)
		if err != nil {
			return ActivitySpec{}, err
		}
		decision, present := approvalFields["decision"]
		if !present {
			return ActivitySpec{}, fmt.Errorf("INVALID-ACTIVITY-APPROVAL: activity.approval.decision is required at %s", approval.Location())
		}
		raw, err := nodeValueText(decision, "activity.approval.decision")
		if err != nil {
			return ActivitySpec{}, err
		}
		canonical := strings.TrimSpace(raw)
		if canonical == "" || canonical != raw {
			return ActivitySpec{}, fmt.Errorf("INVALID-ACTIVITY-APPROVAL: activity.approval.decision %q is not canonical at %s (must be non-empty without surrounding whitespace)", raw, decision.Location())
		}
		out.Approval = &ActivityApprovalSpec{Decision: canonical}
	}
	return out, nil
}

func projectNodeGuardValue(value yamlsource.Value) (*GuardSpec, error) {
	if value.Presence() == yamlsource.PresenceNull {
		return nil, nil
	}
	if value.Presence() != yamlsource.PresenceMapping && value.Presence() != yamlsource.PresenceEmptyMapping {
		return nil, fmt.Errorf("DIALECT-GUARD: guard at %s must be {id, check}, got %s", value.Location(), value.Presence())
	}
	fields, err := nodeValueFields(value, "guard", map[string]struct{}{
		"id": {}, "check": {}, "checks": {}, "policy_ref": {}, "on_fail": {},
	}, nil)
	if err != nil {
		return nil, err
	}
	out := &GuardSpec{}
	for _, entry := range []struct {
		key    string
		target *string
	}{
		{"id", &out.ID}, {"check", &out.Check}, {"policy_ref", &out.PolicyRef},
	} {
		if field, present := fields[entry.key]; present {
			*entry.target, err = nodeValueText(field, "guard."+entry.key)
			if err != nil {
				return nil, err
			}
			*entry.target = strings.TrimSpace(*entry.target)
		}
	}
	if checks, present := fields["checks"]; present {
		items, err := checks.Sequence()
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			checkFields, err := nodeValueFields(item, "guard.checks", map[string]struct{}{"id": {}, "check": {}}, nil)
			if err != nil {
				return nil, err
			}
			var check GuardCheck
			if id, present := checkFields["id"]; present {
				check.ID, err = nodeValueText(id, "guard.checks.id")
				if err != nil {
					return nil, err
				}
			}
			if expression, present := checkFields["check"]; present {
				check.Check, err = nodeValueText(expression, "guard.checks.check")
				if err != nil {
					return nil, err
				}
			}
			out.Checks = append(out.Checks, check)
		}
	}
	if onFail, present := fields["on_fail"]; present {
		out.OnFail, out.OnFailSpec, err = projectNodeGuardOnFailValue(onFail)
		if err != nil {
			return nil, err
		}
	}
	if out.ID == "" && out.Check == "" && len(out.Checks) == 0 && out.OnFail == "" && out.PolicyRef == "" {
		return nil, nil
	}
	return out, nil
}

func projectNodeGuardOnFailValue(value yamlsource.Value) (string, GuardFailureSpec, error) {
	switch value.Presence() {
	case yamlsource.PresenceNull:
		return "", GuardFailureSpec{}, nil
	case yamlsource.PresenceScalar, yamlsource.PresenceEmptyScalar:
		text, err := nodeValueText(value, "guard.on_fail")
		return strings.TrimSpace(text), GuardFailureSpec{}, err
	case yamlsource.PresenceMapping, yamlsource.PresenceEmptyMapping:
		fields, err := nodeValueFields(value, "guard.on_fail", guardOnFailFieldOptions, nil)
		if err != nil {
			return "", GuardFailureSpec{}, err
		}
		escalation, present := fields["escalate"]
		if !present {
			return "", GuardFailureSpec{}, fmt.Errorf("guard.on_fail object form requires escalate at %s", value.Location())
		}
		emit, err := projectNodeGuardEscalationValue(escalation)
		if err != nil {
			return "", GuardFailureSpec{}, err
		}
		return "escalate:" + strings.TrimSpace(emit.Event), GuardFailureSpec{
			Action: GuardFailureActionEscalate, Escalation: cloneEmitSpec(emit), AuthoredMapping: true,
		}, nil
	default:
		return "", GuardFailureSpec{}, fmt.Errorf("guard.on_fail at %s must be a scalar or mapping", value.Location())
	}
}

func projectNodeGuardEscalationValue(value yamlsource.Value) (EmitSpec, error) {
	if value.Presence() != yamlsource.PresenceMapping && value.Presence() != yamlsource.PresenceEmptyMapping {
		return EmitSpec{}, fmt.Errorf("guard.on_fail.escalate must be a mapping at %s", value.Location())
	}
	fields, err := nodeValueFields(value, "guard.on_fail.escalate", guardOnFailEscalateFieldOptions, nil)
	if err != nil {
		return EmitSpec{}, err
	}
	return projectNodeEmitFields(fields, "guard.on_fail.escalate")
}
