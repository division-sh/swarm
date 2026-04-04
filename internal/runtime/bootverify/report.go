package bootverify

import (
	"context"
	"fmt"
	"sort"
	"strings"

	runtimecredentials "swarm/internal/runtime/credentials"
	"swarm/internal/runtime/semanticview"
)

type Finding struct {
	CheckID  string
	Severity string
	Message  string
	Location string
}

type Report struct {
	Findings []Finding
}

type Options struct {
	Credentials       runtimecredentials.Store
	CheckMCPReachable bool
}

func Run(ctx context.Context, source semanticview.Source, opts Options) Report {
	report := Report{}
	if source == nil {
		report.Add(Finding{
			CheckID:  "workflow_contract_validation",
			Severity: "error",
			Message:  "semantic source is not configured",
			Location: "global",
		})
		return report
	}
	checkCtx := newCheckerContext(ctx, source, opts)
	for _, check := range bootCheckRegistry {
		for _, finding := range check.Run(checkCtx) {
			report.Add(finding)
		}
	}
	for _, check := range supplementalChecks {
		for _, finding := range check.Run(checkCtx) {
			report.Add(finding)
		}
	}

	report.Sort()
	return report
}

func (r *Report) Add(f Finding) {
	f.CheckID = strings.TrimSpace(f.CheckID)
	if f.CheckID == "" {
		f.CheckID = "workflow_contract_validation"
	}
	f.Severity = strings.TrimSpace(strings.ToLower(f.Severity))
	if f.Severity == "" {
		f.Severity = "error"
	}
	f.Message = strings.TrimSpace(f.Message)
	f.Location = strings.TrimSpace(f.Location)
	r.Findings = append(r.Findings, f)
}

func (r Report) Errors() []Finding {
	out := make([]Finding, 0)
	for _, finding := range r.Findings {
		if finding.Severity == "error" {
			out = append(out, finding)
		}
	}
	return out
}

func (r Report) Warnings() []Finding {
	out := make([]Finding, 0)
	for _, finding := range r.Findings {
		if finding.Severity == "warning" {
			out = append(out, finding)
		}
	}
	return out
}

func (r Report) HasErrors() bool {
	for _, finding := range r.Findings {
		if finding.Severity == "error" {
			return true
		}
	}
	return false
}

func (r *Report) Sort() {
	sort.Slice(r.Findings, func(i, j int) bool {
		if r.Findings[i].Severity == r.Findings[j].Severity {
			if r.Findings[i].CheckID == r.Findings[j].CheckID {
				if r.Findings[i].Location == r.Findings[j].Location {
					return r.Findings[i].Message < r.Findings[j].Message
				}
				return r.Findings[i].Location < r.Findings[j].Location
			}
			return r.Findings[i].CheckID < r.Findings[j].CheckID
		}
		return r.Findings[i].Severity < r.Findings[j].Severity
	})
}

func locationFromMessage(message string) string {
	fields := strings.Fields(strings.TrimSpace(message))
	if len(fields) < 2 {
		return "global"
	}
	switch fields[0] {
	case "agent", "node", "flow", "event", "timer", "transition", "root", "write":
		return strings.TrimSpace(strings.Trim(fields[1], "\"'"))
	default:
		return "global"
	}
}

func workspaceClassFindings(source semanticview.Source) []Finding {
	if source == nil {
		return nil
	}
	classes, err := rootWorkspaceClasses(source)
	if err != nil {
		return []Finding{{
			CheckID:  "workspace_class_exists",
			Severity: "error",
			Message:  err.Error(),
			Location: "global",
		}}
	}
	out := make([]Finding, 0)
	for agentID, entry := range source.AgentEntries() {
		agentID = strings.TrimSpace(agentID)
		class := strings.TrimSpace(entry.WorkspaceClass)
		if class == "" {
			continue
		}
		scope, ok := classes[class]
		if !ok {
			out = append(out, Finding{
				CheckID:  "workspace_class_exists",
				Severity: "error",
				Message:  fmt.Sprintf("agent %s references undefined workspace_class %q", agentID, class),
				Location: agentID,
			})
			continue
		}
		if scope != "per-agent" && scope != "per-flow-instance" {
			out = append(out, Finding{
				CheckID:  "workspace_class_exists",
				Severity: "error",
				Message:  fmt.Sprintf("workspace_class %q declares unsupported workspace_scope %q", class, scope),
				Location: class,
			})
		}
	}
	return out
}

func rootWorkspaceClasses(source semanticview.Source) (map[string]string, error) {
	value, ok := semanticview.PolicyValueForFlow(source, "", "workspace_classes")
	if !ok {
		return map[string]string{}, nil
	}
	root, ok := anyMap(value.Value)
	if !ok {
		return nil, fmt.Errorf("workspace_classes must be a mapping")
	}
	out := make(map[string]string, len(root))
	for className, raw := range root {
		entry, ok := anyMap(raw)
		if !ok {
			return nil, fmt.Errorf("workspace_classes.%s must be a mapping", strings.TrimSpace(className))
		}
		out[strings.TrimSpace(className)] = strings.TrimSpace(anyString(entry["workspace_scope"]))
	}
	return out, nil
}

func anyMap(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	default:
		return nil, false
	}
}

func anyString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

func fmtCredentialWarning(key string, requiredBy []string) string {
	key = strings.TrimSpace(key)
	if len(requiredBy) == 0 {
		return fmt.Sprintf("credential %s is missing", key)
	}
	sort.Strings(requiredBy)
	return fmt.Sprintf("credential %s is missing (required by %s)", key, strings.Join(requiredBy, ", "))
}
