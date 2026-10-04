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
	fields, err := nodeValueFields(value, "activity", activityFieldOptions)
	if err != nil {
		return ActivitySpec{}, err
	}
	var out ActivitySpec
	if err := nodeValueTexts(fields, map[string]*string{"id": &out.ID, "tool": &out.Tool}, true); err != nil {
		return ActivitySpec{}, err
	}
	if input, present := fields["input"]; present {
		out.Input, err = projectNodeExpressionFields(input, "activity.input")
		if err != nil {
			return ActivitySpec{}, err
		}
	}
	if approval, present := fields["approval"]; present {
		approvalFields, err := nodeValueFields(approval, "activity.approval", activityApprovalFieldOptions)
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
	})
	if err != nil {
		return nil, err
	}
	out := &GuardSpec{}
	if err := nodeValueTexts(fields, map[string]*string{
		"id": &out.ID, "policy_ref": &out.PolicyRef,
	}, true); err != nil {
		return nil, err
	}
	if check, present := fields["check"]; present {
		out.Check, err = projectNodeScalarExpression(check, "guard.check", true)
		if err != nil {
			return nil, err
		}
	}
	if checks, present := fields["checks"]; present {
		out.Checks, err = projectNodeGuardChecksValue(checks)
		if err != nil {
			return nil, err
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

func projectNodeGuardChecksValue(value yamlsource.Value) ([]GuardCheck, error) {
	items, err := value.Sequence()
	if err != nil {
		return nil, err
	}
	var out []GuardCheck
	for _, item := range items {
		fields, err := nodeValueFields(item, "guard.checks", map[string]struct{}{"id": {}, "check": {}})
		if err != nil {
			return nil, err
		}
		var check GuardCheck
		if err := nodeValueTexts(fields, map[string]*string{"id": &check.ID}, false); err != nil {
			return nil, err
		}
		if expression, present := fields["check"]; present {
			check.Check, err = projectNodeScalarExpression(expression, "guard.checks.check", true)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, check)
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
		fields, err := nodeValueFields(value, "guard.on_fail", guardOnFailFieldOptions)
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
	fields, err := nodeValueFields(value, "guard.on_fail.escalate", guardOnFailEscalateFieldOptions)
	if err != nil {
		return EmitSpec{}, err
	}
	return projectNodeEmitFields(fields, "guard.on_fail.escalate")
}
