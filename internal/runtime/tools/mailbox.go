package tools

import (
	"fmt"
	"strings"
)

func NormalizeMailboxType(raw string) (string, error) {
	t := strings.ToLower(strings.TrimSpace(raw))
	switch t {
	case "vertical_decision", "vertical_promotion_review", "vertical-promotion-review", "vertical.promotion_review", "promotion_review", "approval":
		t = "vertical_approval"
	case "template_migration_review", "template_migration":
		t = "migration_approval"
	case "escalation_request", "customer_escalation", "health_warning":
		t = "escalation"
	case "geography_expansion", "vertical_geography_expansion", "expansion_recommendation":
		t = "domain_approval"
	case "product_spec_review", "deploy_review", "founder_input", "human_task":
		t = "review"
	case "capacity_warning":
		t = "budget_increase"
	}
	switch t {
	case "review", "escalation", "spend_request", "budget_increase", "digest", "vertical_approval", "migration_approval", "domain_approval":
		return t, nil
	default:
		return "", fmt.Errorf("invalid mailbox type %q", raw)
	}
}

func NormalizeMailboxPriority(raw string) (string, error) {
	p := strings.ToLower(strings.TrimSpace(raw))
	switch p {
	case "", "normal":
		return "normal", nil
	case "medium":
		return "normal", nil
	case "urgent":
		return "high", nil
	case "low", "high", "critical":
		return p, nil
	default:
		return "", fmt.Errorf("invalid mailbox priority %q", raw)
	}
}
