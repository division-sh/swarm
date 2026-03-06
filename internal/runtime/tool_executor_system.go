package runtime

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"empireai/internal/models"
)

func (e *RuntimeToolExecutor) execNginxReload(ctx context.Context, actor models.AgentConfig, _ any) (any, error) {
	if actor.Role != "holding-devops" {
		return nil, errors.New("nginx_reload is restricted to holding-devops")
	}
	if out, err := exec.CommandContext(ctx, "nginx", "-t").CombinedOutput(); err != nil {
		return nil, fmt.Errorf("nginx config test failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	if out, err := exec.CommandContext(ctx, "systemctl", "reload", "nginx").CombinedOutput(); err != nil {
		return nil, fmt.Errorf("nginx reload failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return map[string]any{"status": "reloaded"}, nil
}

func (e *RuntimeToolExecutor) execSystemdControl(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	if actor.Role != "holding-devops" {
		return nil, errors.New("systemd_control is restricted to holding-devops")
	}
	var in struct {
		Action  string `json:"action"`
		Unit    string `json:"unit"`
		Service string `json:"service"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	action := strings.TrimSpace(strings.ToLower(in.Action))
	unit := strings.TrimSpace(in.Unit)
	if unit == "" {
		unit = strings.TrimSpace(in.Service)
	}
	switch action {
	case "start", "stop", "restart", "enable", "disable", "status":
	default:
		return nil, fmt.Errorf("unsupported systemd action: %s", action)
	}
	if !strings.HasPrefix(unit, "empireai-") {
		return nil, errors.New("systemd unit must start with empireai-")
	}
	if action == "status" {
		out, err := exec.CommandContext(ctx, "systemctl", "is-active", unit).CombinedOutput()
		state := strings.TrimSpace(string(out))
		if state == "" {
			state = "unknown"
		}
		if err != nil && state == "unknown" {
			return nil, fmt.Errorf("systemctl status %s failed: %w", unit, err)
		}
		return map[string]any{"status": "ok", "action": action, "unit": unit, "state": state}, nil
	}
	out, err := exec.CommandContext(ctx, "systemctl", action, unit).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("systemctl %s %s failed: %w: %s", action, unit, err, strings.TrimSpace(string(out)))
	}
	return map[string]any{"status": "ok", "action": action, "unit": unit}, nil
}

func (e *RuntimeToolExecutor) execCertbotExecute(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	if actor.Role != "holding-devops" {
		return nil, errors.New("certbot_execute is restricted to holding-devops")
	}
	var in struct {
		Domain string `json:"domain"`
		Action string `json:"action"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	domain := strings.TrimSpace(in.Domain)
	if domain == "" {
		return nil, errors.New("domain is required")
	}
	action := strings.TrimSpace(strings.ToLower(in.Action))
	if action == "" {
		action = "provision"
	}
	var cmd *exec.Cmd
	switch action {
	case "provision":
		cmd = exec.CommandContext(ctx, "certbot", "--nginx", "-d", domain, "--non-interactive", "--agree-tos")
	case "renew":
		cmd = exec.CommandContext(ctx, "certbot", "renew", "--cert-name", domain, "--non-interactive")
	case "revoke":
		cmd = exec.CommandContext(ctx, "certbot", "revoke", "--cert-name", domain, "--non-interactive")
	default:
		return nil, fmt.Errorf("unsupported certbot action: %s", action)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("certbot failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return map[string]any{"status": "ok", "domain": domain, "action": action}, nil
}
