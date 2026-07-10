package packs

import (
	"fmt"
	"sort"
	"strings"
)

type SubjectKind string

const (
	SubjectProviderTrigger   SubjectKind = "provider_trigger"
	SubjectProviderConnector SubjectKind = "provider_connector"
)

type SubjectStatus string

const (
	StatusReady     SubjectStatus = "READY"
	StatusNotReady  SubjectStatus = "NOT_READY"
	StatusAvailable SubjectStatus = "AVAILABLE"
)

const (
	RequirementSecret            = "secret"
	RequirementManagedCredential = "managed_credential"
	RequirementImport            = "import"
	RequirementScopeTarget       = "target"
)

const (
	CapabilityReceiveHTTPSRoute    = "receive_https_route"
	CapabilityVerifySecret         = "verify_secret"
	CapabilityEmitEvent            = "emit_event"
	CapabilityPersistDedupeMarkers = "persist_dedupe_markers"
	CapabilityCallProviderAction   = "call_provider_action"
	CapabilityLowerThroughActivity = "lower_through_activity"
	CapabilityJournalAttempts      = "journal_activity_attempts"
)

const (
	GuaranteeEmitDeclaredEventsOnly = "emit_undeclared_events"
	GuaranteeAdmissionBeforeCode    = "run_code_before_admission"
	GuaranteeBoundResourcesOnly     = "touch_unbound_resources"
	GuaranteeActivityJournal        = "bypass_activity_attempts"
	GuaranteeNoAutomaticWriteRetry  = "retry_non_idempotent_write_automatically"
	GuaranteeCredentialRedaction    = "expose_credential_values"
)

type Subject struct {
	ID            string        `json:"id"`
	Kind          SubjectKind   `json:"kind"`
	Provider      string        `json:"provider"`
	Action        string        `json:"action,omitempty"`
	Source        string        `json:"source"`
	Provenance    string        `json:"provenance,omitempty"`
	SourcePath    string        `json:"source_path,omitempty"`
	Applicability string        `json:"applicability"`
	Status        SubjectStatus `json:"status"`
	Capabilities  []Capability  `json:"capabilities,omitempty"`
	Guarantees    []Guarantee   `json:"guarantees,omitempty"`
	Requirements  []Requirement `json:"requirements,omitempty"`
	Evidence      []Evidence    `json:"evidence,omitempty"`
}

type Capability struct {
	Code   string `json:"code"`
	Target string `json:"target,omitempty"`
}

type Guarantee struct {
	Code       string `json:"code"`
	EnforcedBy string `json:"enforced_by"`
}

type Requirement struct {
	Kind                string               `json:"kind"`
	Name                string               `json:"name"`
	Scope               string               `json:"scope"`
	Status              string               `json:"status,omitempty"`
	Satisfied           *bool                `json:"satisfied,omitempty"`
	Remediation         string               `json:"remediation,omitempty"`
	Source              string               `json:"source,omitempty"`
	GrantType           string               `json:"grant_type,omitempty"`
	Scopes              []string             `json:"scopes,omitempty"`
	GrantModel          string               `json:"grant_model,omitempty"`
	TokenRequest        *TokenRequestProfile `json:"token_request,omitempty"`
	InstallationIDInput string               `json:"installation_id_input,omitempty"`
}

type TokenRequestProfile struct {
	ClientAuth    string            `json:"client_auth,omitempty"`
	Body          string            `json:"body,omitempty"`
	StaticHeaders map[string]string `json:"static_headers,omitempty"`
}

type Evidence struct {
	Kind   string            `json:"kind"`
	Fields map[string]string `json:"fields"`
}

var capabilityPhrases = map[string]string{
	CapabilityReceiveHTTPSRoute:    "receive HTTPS route",
	CapabilityVerifySecret:         "verify named secret",
	CapabilityEmitEvent:            "emit named event",
	CapabilityPersistDedupeMarkers: "persist dedupe markers",
	CapabilityCallProviderAction:   "call provider action",
	CapabilityLowerThroughActivity: "lower through platform.activity_requested",
	CapabilityJournalAttempts:      "journal non-idempotent attempts in activity_attempts",
}

var guaranteeRegistry = map[string]struct {
	phrase     string
	enforcedBy string
}{
	GuaranteeEmitDeclaredEventsOnly: {"emit undeclared events", "provider_trigger_event_name_policy"},
	GuaranteeAdmissionBeforeCode:    {"run code before admission", "provider_trigger_admission_sequence"},
	GuaranteeBoundResourcesOnly:     {"touch unbound resources", "provider_trigger_resource_binding_gate"},
	GuaranteeActivityJournal:        {"bypass activity_attempts", "activity_attempts"},
	GuaranteeNoAutomaticWriteRetry:  {"retry non_idempotent_write automatically", "activity_effect_retry_policy"},
	GuaranteeCredentialRedaction:    {"expose credential values", "credential_redaction_boundary"},
}

func NewGuarantee(code string) (Guarantee, error) {
	code = strings.TrimSpace(code)
	entry, ok := guaranteeRegistry[code]
	if !ok {
		return Guarantee{}, fmt.Errorf("capability guarantee %q has no registered enforcement owner", code)
	}
	return Guarantee{Code: code, EnforcedBy: entry.enforcedBy}, nil
}

func RequirementWithStatus(kind, name, scope, status, source string) Requirement {
	kind = strings.TrimSpace(kind)
	name = strings.TrimSpace(name)
	status = strings.ToUpper(strings.TrimSpace(status))
	satisfied := requirementSatisfied(kind, status)
	return Requirement{
		Kind:        kind,
		Name:        name,
		Scope:       strings.TrimSpace(scope),
		Status:      status,
		Satisfied:   &satisfied,
		Remediation: requirementRemediation(kind, name, status),
		Source:      strings.TrimSpace(source),
	}
}

func TargetScopedRequirement(kind, name string) Requirement {
	return Requirement{Kind: strings.TrimSpace(kind), Name: strings.TrimSpace(name), Scope: RequirementScopeTarget}
}

func requirementSatisfied(kind, status string) bool {
	switch strings.TrimSpace(kind) {
	case RequirementSecret:
		return status == "BOUND"
	case RequirementManagedCredential:
		return status == "CONNECTED"
	default:
		return false
	}
}

func requirementRemediation(kind, name, status string) string {
	name = strings.TrimSpace(name)
	switch strings.TrimSpace(kind) {
	case RequirementSecret:
		if status != "BOUND" {
			return "swarm secrets set " + name
		}
	case RequirementManagedCredential:
		switch status {
		case "CONNECTED":
			return ""
		case "REFRESH_FAILED":
			return "swarm connections disconnect " + name + " && swarm connections connect " + name
		default:
			return "swarm connections connect " + name
		}
	case RequirementImport:
		return "add connector_packs.imports entry for " + name
	}
	return ""
}

func NormalizeSubjects(subjects []Subject) ([]Subject, error) {
	out := append([]Subject(nil), subjects...)
	for i := range out {
		if err := normalizeSubject(&out[i]); err != nil {
			return nil, err
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].ID < out[j].ID
	})
	for i := 1; i < len(out); i++ {
		if out[i-1].Kind == out[i].Kind && out[i-1].ID == out[i].ID {
			return nil, fmt.Errorf("duplicate capability subject %s/%s", out[i].Kind, out[i].ID)
		}
	}
	return out, nil
}

func normalizeSubject(subject *Subject) error {
	subject.ID = strings.TrimSpace(subject.ID)
	subject.Provider = strings.TrimSpace(subject.Provider)
	subject.Action = strings.TrimSpace(subject.Action)
	subject.Source = strings.TrimSpace(subject.Source)
	subject.Provenance = strings.TrimSpace(subject.Provenance)
	subject.SourcePath = strings.TrimSpace(subject.SourcePath)
	subject.Applicability = strings.TrimSpace(subject.Applicability)
	if subject.ID == "" || subject.Provider == "" || subject.Source == "" || subject.Applicability == "" {
		return fmt.Errorf("capability subject id, provider, source, and applicability are required")
	}
	switch subject.Kind {
	case SubjectProviderTrigger:
		if subject.Status != StatusAvailable {
			return fmt.Errorf("provider trigger subject %q must be AVAILABLE without a selected target", subject.ID)
		}
		for _, requirement := range subject.Requirements {
			if requirement.Scope != RequirementScopeTarget || requirement.Satisfied != nil || requirement.Status != "" || requirement.Remediation != "" {
				return fmt.Errorf("provider trigger subject %q requirement %q must remain target-scoped and unevaluated", subject.ID, requirement.Name)
			}
		}
	case SubjectProviderConnector:
		if subject.Status != StatusReady && subject.Status != StatusNotReady && subject.Status != StatusAvailable {
			return fmt.Errorf("provider connector subject %q has invalid status %q", subject.ID, subject.Status)
		}
	default:
		return fmt.Errorf("capability subject %q has unsupported kind %q", subject.ID, subject.Kind)
	}
	for _, requirement := range subject.Requirements {
		if requirement.Satisfied != nil && !*requirement.Satisfied && strings.TrimSpace(requirement.Remediation) == "" {
			return fmt.Errorf("capability subject %q unsatisfied requirement %q has no registered remediation", subject.ID, requirement.Name)
		}
	}
	for i := range subject.Capabilities {
		subject.Capabilities[i].Code = strings.TrimSpace(subject.Capabilities[i].Code)
		subject.Capabilities[i].Target = strings.TrimSpace(subject.Capabilities[i].Target)
	}
	for i := range subject.Guarantees {
		guarantee, err := NewGuarantee(subject.Guarantees[i].Code)
		if err != nil {
			return err
		}
		if current := strings.TrimSpace(subject.Guarantees[i].EnforcedBy); current != "" && current != guarantee.EnforcedBy {
			return fmt.Errorf("capability guarantee %q enforcement owner %q does not match registry owner %q", guarantee.Code, current, guarantee.EnforcedBy)
		}
		subject.Guarantees[i] = guarantee
	}
	sort.SliceStable(subject.Capabilities, func(i, j int) bool {
		if subject.Capabilities[i].Code != subject.Capabilities[j].Code {
			return subject.Capabilities[i].Code < subject.Capabilities[j].Code
		}
		return subject.Capabilities[i].Target < subject.Capabilities[j].Target
	})
	sort.SliceStable(subject.Guarantees, func(i, j int) bool { return subject.Guarantees[i].Code < subject.Guarantees[j].Code })
	sort.SliceStable(subject.Requirements, func(i, j int) bool {
		leftUnsatisfied := subject.Requirements[i].Satisfied != nil && !*subject.Requirements[i].Satisfied
		rightUnsatisfied := subject.Requirements[j].Satisfied != nil && !*subject.Requirements[j].Satisfied
		if leftUnsatisfied != rightUnsatisfied {
			return leftUnsatisfied
		}
		if subject.Requirements[i].Kind != subject.Requirements[j].Kind {
			return subject.Requirements[i].Kind < subject.Requirements[j].Kind
		}
		return subject.Requirements[i].Name < subject.Requirements[j].Name
	})
	return nil
}

func RenderSubject(subject Subject, verbose bool) string {
	normalized, err := NormalizeSubjects([]Subject{subject})
	if err != nil {
		return "invalid capability subject: " + err.Error()
	}
	subject = normalized[0]
	parts := []string{fmt.Sprintf("%s %s %s", humanSubjectKind(subject.Kind), subject.ID, humanSubjectStatus(subject.Status))}
	if subject.Provenance != "" {
		parts = append(parts, "provenance="+subject.Provenance)
	}
	if subject.SourcePath != "" {
		parts = append(parts, "source_path="+subject.SourcePath)
	}
	for _, requirement := range subject.Requirements {
		parts = append(parts, renderRequirement(requirement))
	}
	for _, capability := range subject.Capabilities {
		phrase := capabilityPhrases[capability.Code]
		if phrase == "" {
			phrase = capability.Code
		}
		if capability.Target != "" {
			phrase += " " + capability.Target
		}
		parts = append(parts, "CAN "+phrase)
	}
	for _, guarantee := range subject.Guarantees {
		phrase := guarantee.Code
		if entry, ok := guaranteeRegistry[guarantee.Code]; ok {
			phrase = entry.phrase
		}
		parts = append(parts, "guarantee: cannot "+phrase+" - enforced by "+guarantee.EnforcedBy)
	}
	if verbose {
		for _, evidence := range subject.Evidence {
			keys := make([]string, 0, len(evidence.Fields))
			for key := range evidence.Fields {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			values := make([]string, 0, len(keys))
			for _, key := range keys {
				values = append(values, key+"="+evidence.Fields[key])
			}
			parts = append(parts, "evidence "+evidence.Kind+": "+strings.Join(values, " "))
		}
	}
	return strings.Join(parts, "; ")
}

func humanSubjectKind(kind SubjectKind) string {
	switch kind {
	case SubjectProviderTrigger:
		return "provider trigger pack"
	case SubjectProviderConnector:
		return "provider connector"
	default:
		return string(kind)
	}
}

func humanSubjectStatus(status SubjectStatus) string {
	if status == StatusNotReady {
		return "NOT READY"
	}
	return string(status)
}

func renderRequirement(requirement Requirement) string {
	name := requirement.Name
	if requirement.Kind == RequirementManagedCredential {
		name = "managed_credential:" + name
	}
	if requirement.Scope == RequirementScopeTarget && requirement.Satisfied == nil {
		return "requires " + name + " (target-scoped)"
	}
	status := requirement.Status
	if status == "" {
		status = "UNKNOWN"
	}
	text := "requires " + name + "=" + status
	if requirement.Remediation != "" && requirement.Satisfied != nil && !*requirement.Satisfied {
		text += " (fix: " + requirement.Remediation + ")"
	}
	return text
}
