package bootverify

import (
	"context"
	"fmt"
	"sort"
	"strings"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type Finding struct {
	CheckID  string
	Severity string
	Message  string
	Location string
}

const (
	SeverityHardInvalidity    = "hard_invalidity"
	SeveritySemanticDriftWarn = "semantic_drift_warning"
	SeverityAuditAnalysis     = "audit_analysis"
	SeverityLintEvidence      = "lint_evidence"
	legacySeverityError       = "error"
	legacySeverityWarning     = "warning"
)

type Report struct {
	Findings []Finding
}

type Options struct {
	Credentials             runtimecredentials.Store
	CheckMCPReachable       bool
	ValidateModelResolution bool
	LLMProfile              llmselection.Profile
	ModelAliases            llmselection.ModelAliases
}

func Run(ctx context.Context, source semanticview.Source, opts Options) Report {
	report := Report{}
	if source == nil {
		report.Add(Finding{
			CheckID:  "workflow_contract_validation",
			Severity: SeverityHardInvalidity,
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
	f.Severity = normalizeFindingSeverity(f.Severity)
	f.Message = strings.TrimSpace(f.Message)
	f.Location = strings.TrimSpace(f.Location)
	r.Findings = append(r.Findings, f)
}

func (r Report) Errors() []Finding {
	return r.HardInvalidities()
}

func (r Report) HardInvalidities() []Finding {
	out := make([]Finding, 0)
	for _, finding := range r.Findings {
		if finding.Severity == SeverityHardInvalidity {
			out = append(out, finding)
		}
	}
	return out
}

func (r Report) Warnings() []Finding {
	return r.SemanticDriftWarnings()
}

func (r Report) SemanticDriftWarnings() []Finding {
	out := make([]Finding, 0)
	for _, finding := range r.Findings {
		if finding.Severity == SeveritySemanticDriftWarn {
			out = append(out, finding)
		}
	}
	return out
}

func (r Report) AuditAnalyses() []Finding {
	out := make([]Finding, 0)
	for _, finding := range r.Findings {
		if finding.Severity == SeverityAuditAnalysis {
			out = append(out, finding)
		}
	}
	return out
}

func (r Report) LintEvidence() []Finding {
	out := make([]Finding, 0)
	for _, finding := range r.Findings {
		if finding.Severity == SeverityLintEvidence {
			out = append(out, finding)
		}
	}
	return out
}

func (r Report) HasErrors() bool {
	return r.HasHardInvalidities()
}

func (r Report) HasHardInvalidities() bool {
	for _, finding := range r.Findings {
		if finding.Severity == SeverityHardInvalidity {
			return true
		}
	}
	return false
}

func (r *Report) Sort() {
	sort.Slice(r.Findings, func(i, j int) bool {
		leftSeverity := severityRank(r.Findings[i].Severity)
		rightSeverity := severityRank(r.Findings[j].Severity)
		if leftSeverity == rightSeverity {
			if r.Findings[i].CheckID == r.Findings[j].CheckID {
				if r.Findings[i].Location == r.Findings[j].Location {
					return r.Findings[i].Message < r.Findings[j].Message
				}
				return r.Findings[i].Location < r.Findings[j].Location
			}
			return r.Findings[i].CheckID < r.Findings[j].CheckID
		}
		return leftSeverity < rightSeverity
	})
}

func normalizeFindingSeverity(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", legacySeverityError, SeverityHardInvalidity:
		return SeverityHardInvalidity
	case legacySeverityWarning, SeveritySemanticDriftWarn:
		return SeveritySemanticDriftWarn
	case SeverityAuditAnalysis:
		return SeverityAuditAnalysis
	case SeverityLintEvidence:
		return SeverityLintEvidence
	default:
		return SeverityHardInvalidity
	}
}

func severityRank(severity string) int {
	switch normalizeFindingSeverity(severity) {
	case SeverityHardInvalidity:
		return 0
	case SeveritySemanticDriftWarn:
		return 1
	case SeverityAuditAnalysis:
		return 2
	case SeverityLintEvidence:
		return 3
	default:
		return 4
	}
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
			Severity: SeverityHardInvalidity,
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
				Severity: SeverityHardInvalidity,
				Message:  fmt.Sprintf("agent %s references undefined workspace_class %q", agentID, class),
				Location: agentID,
			})
			continue
		}
		if scope != "per-agent" && scope != "per-flow-instance" {
			out = append(out, Finding{
				CheckID:  "workspace_class_exists",
				Severity: SeverityHardInvalidity,
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
