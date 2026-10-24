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
	id, present := fields["id"]
	if !present {
		return ActivitySpec{}, nodeValueError(value, fmt.Errorf("activity.id is required; declare an explicit activity.id naming its result events"))
	}
	scalar, err := id.Scalar()
	if err != nil || scalar.Tag != "!!str" || strings.TrimSpace(scalar.Value) == "" {
		return ActivitySpec{}, nodeValueError(id, fmt.Errorf("activity.id must be non-empty text"))
	}
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
	check, err := projectNodeScalarExpression(value, "guard", true)
	if err != nil {
		return nil, fmt.Errorf("DIALECT-GUARD: %w; write guard: <condition>, joining conditions with &&. A false guard records rejection; use rules rows to terminate or emit", err)
	}
	if strings.TrimSpace(check) == "" {
		return nil, fmt.Errorf("guard at %s requires a scalar boolean expression", value.Location())
	}
	return &GuardSpec{Check: check}, nil
}
