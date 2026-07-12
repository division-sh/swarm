package packs

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/userfacing"
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
	RequirementStatusBound             = "BOUND"
	RequirementStatusUnbound           = "UNBOUND"
	RequirementStatusConnected         = "CONNECTED"
	RequirementStatusUnconnected       = "UNCONNECTED"
	RequirementStatusPendingConsent    = "PENDING_CONSENT"
	RequirementStatusRefreshFailed     = "REFRESH_FAILED"
	RequirementStatusScopeInsufficient = "SCOPE_INSUFFICIENT"
	RequirementStatusNotImported       = "NOT_IMPORTED"
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
	ID               string            `json:"id"`
	Kind             SubjectKind       `json:"kind"`
	Provider         string            `json:"provider"`
	Action           string            `json:"action,omitempty"`
	Source           string            `json:"source"`
	Provenance       string            `json:"provenance,omitempty"`
	SourcePath       string            `json:"source_path,omitempty"`
	Applicability    string            `json:"applicability"`
	Status           SubjectStatus     `json:"status"`
	Capabilities     []Capability      `json:"capabilities,omitempty"`
	Guarantees       []Guarantee       `json:"guarantees,omitempty"`
	Requirements     []Requirement     `json:"requirements,omitempty"`
	Evidence         []Evidence        `json:"evidence,omitempty"`
	TriggerAdmission *TriggerAdmission `json:"trigger_admission,omitempty"`
}

type TriggerAdmission struct {
	BundleHash            string               `json:"bundle_hash"`
	Alias                 string               `json:"alias"`
	CatalogGeneration     string               `json:"catalog_generation"`
	PolicySource          string               `json:"policy_source"`
	RequestAuthentication string               `json:"request_authentication"`
	Event                 string               `json:"event"`
	SignedPayload         string               `json:"signed_payload,omitempty"`
	DigestEncoding        string               `json:"digest_encoding,omitempty"`
	Pack                  *TriggerPackIdentity `json:"pack,omitempty"`
}

type TriggerPackIdentity struct {
	ID           string `json:"id"`
	Version      string `json:"version"`
	ManifestHash string `json:"manifest_hash"`
	Provenance   string `json:"provenance"`
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

var guaranteeRegistry = map[string]struct {
	enforcedBy string
}{
	GuaranteeEmitDeclaredEventsOnly: {"internal/providertriggers.InboundAdmissionPlan.Accept"},
	GuaranteeAdmissionBeforeCode:    {"internal/providertriggers.InboundAdmissionPlan.Accept"},
	GuaranteeBoundResourcesOnly:     {"internal/runtime.InboundGateway.HandleResolvedWebhook"},
	GuaranteeActivityJournal:        {"internal/runtime/pipeline.pipelineActivityDispatcher.executeNonIdempotentActivityIntent"},
	GuaranteeNoAutomaticWriteRetry:  {"internal/runtime/pipeline.pipelineActivityDispatcher.executeNonIdempotentActivityIntent"},
	GuaranteeCredentialRedaction:    {"internal/runtime/pipeline.executePreparedActivityHTTPTool"},
}

var connectorRequirementStatuses = map[string]map[string]bool{
	RequirementSecret: {
		RequirementStatusBound: true, RequirementStatusUnbound: true,
	},
	RequirementManagedCredential: {
		RequirementStatusConnected: true, RequirementStatusUnconnected: true,
		RequirementStatusPendingConsent: true, RequirementStatusRefreshFailed: true,
		RequirementStatusScopeInsufficient: true,
	},
	RequirementImport: {
		RequirementStatusNotImported: true,
	},
}

func ProviderHumanCodeValues() map[userfacing.HumanCodeFamily][]string {
	out := map[userfacing.HumanCodeFamily][]string{
		userfacing.HumanCodeProviderSubjectKind: {
			string(SubjectProviderTrigger), string(SubjectProviderConnector),
		},
		userfacing.HumanCodeProviderSubjectStatus: {
			string(StatusReady), string(StatusNotReady), string(StatusAvailable),
		},
		userfacing.HumanCodeProviderCapability: {
			CapabilityReceiveHTTPSRoute, CapabilityVerifySecret, CapabilityEmitEvent,
			CapabilityPersistDedupeMarkers, CapabilityCallProviderAction,
			CapabilityLowerThroughActivity, CapabilityJournalAttempts,
		},
	}
	for code := range guaranteeRegistry {
		out[userfacing.HumanCodeProviderGuarantee] = append(out[userfacing.HumanCodeProviderGuarantee], code)
	}
	for _, statuses := range connectorRequirementStatuses {
		for status := range statuses {
			out[userfacing.HumanCodeProviderRequirementStatus] = append(out[userfacing.HumanCodeProviderRequirementStatus], status)
		}
	}
	out[userfacing.HumanCodeProviderRequirementStatus] = append(out[userfacing.HumanCodeProviderRequirementStatus], "UNKNOWN")
	for family := range out {
		sort.Strings(out[family])
	}
	return out
}

func GuaranteeEnforcementOwners() map[string]string {
	out := make(map[string]string, len(guaranteeRegistry))
	for code, entry := range guaranteeRegistry {
		out[code] = entry.enforcedBy
	}
	return out
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
	out := CloneSubjects(subjects)
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

func CloneSubjects(subjects []Subject) []Subject {
	out := make([]Subject, len(subjects))
	for i, subject := range subjects {
		out[i] = subject
		out[i].Capabilities = append([]Capability(nil), subject.Capabilities...)
		out[i].Guarantees = append([]Guarantee(nil), subject.Guarantees...)
		out[i].Requirements = make([]Requirement, len(subject.Requirements))
		for j, requirement := range subject.Requirements {
			out[i].Requirements[j] = requirement
			out[i].Requirements[j].Scopes = append([]string(nil), requirement.Scopes...)
			if requirement.Satisfied != nil {
				value := *requirement.Satisfied
				out[i].Requirements[j].Satisfied = &value
			}
			if requirement.TokenRequest != nil {
				profile := *requirement.TokenRequest
				profile.StaticHeaders = cloneStringMap(requirement.TokenRequest.StaticHeaders)
				out[i].Requirements[j].TokenRequest = &profile
			}
		}
		out[i].Evidence = make([]Evidence, len(subject.Evidence))
		for j, evidence := range subject.Evidence {
			out[i].Evidence[j] = evidence
			out[i].Evidence[j].Fields = cloneStringMap(evidence.Fields)
		}
		if subject.TriggerAdmission != nil {
			admission := *subject.TriggerAdmission
			if subject.TriggerAdmission.Pack != nil {
				identity := *subject.TriggerAdmission.Pack
				admission.Pack = &identity
			}
			out[i].TriggerAdmission = &admission
		}
	}
	return out
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
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
	providedStatus := subject.Status
	for i := range subject.Requirements {
		requirement := &subject.Requirements[i]
		requirement.Kind = strings.TrimSpace(requirement.Kind)
		requirement.Name = strings.TrimSpace(requirement.Name)
		requirement.Scope = strings.TrimSpace(requirement.Scope)
		requirement.Status = strings.ToUpper(strings.TrimSpace(requirement.Status))
		requirement.Remediation = strings.TrimSpace(requirement.Remediation)
		requirement.Source = strings.TrimSpace(requirement.Source)
		if requirement.Kind == "" || requirement.Name == "" || requirement.Scope == "" {
			return fmt.Errorf("capability subject %q requirement kind, name, and scope are required", subject.ID)
		}
	}
	var derivedStatus SubjectStatus
	switch subject.Kind {
	case SubjectProviderTrigger:
		if err := normalizeProviderTriggerSubject(subject); err != nil {
			return err
		}
		derivedStatus = StatusAvailable
	case SubjectProviderConnector:
		switch subject.Applicability {
		case "installed":
			if subject.Source != "connector_pack" {
				return fmt.Errorf("installed provider connector subject %q must use connector_pack source", subject.ID)
			}
			derivedStatus = StatusAvailable
		case "effective":
			if subject.Source != "flow_local" && subject.Source != "connector_pack_import" {
				return fmt.Errorf("effective provider connector subject %q has invalid source %q", subject.ID, subject.Source)
			}
			derivedStatus = StatusReady
		default:
			return fmt.Errorf("provider connector subject %q has invalid applicability %q", subject.ID, subject.Applicability)
		}
		hasImport := false
		for _, requirement := range subject.Requirements {
			if err := validateConnectorRequirement(subject.ID, requirement); err != nil {
				return err
			}
			if requirement.Kind == RequirementImport {
				hasImport = true
				if subject.Applicability != "installed" {
					return fmt.Errorf("effective provider connector subject %q must not carry import requirement %q", subject.ID, requirement.Name)
				}
			}
			if subject.Applicability == "effective" && requirement.Satisfied != nil && !*requirement.Satisfied {
				derivedStatus = StatusNotReady
			}
		}
		if subject.Applicability == "installed" && !hasImport {
			return fmt.Errorf("installed provider connector subject %q must carry an import requirement", subject.ID)
		}
	default:
		return fmt.Errorf("capability subject %q has unsupported kind %q", subject.ID, subject.Kind)
	}
	if providedStatus != "" && providedStatus != derivedStatus {
		return fmt.Errorf("capability subject %q status %q contradicts derived status %q", subject.ID, providedStatus, derivedStatus)
	}
	subject.Status = derivedStatus
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

func normalizeProviderTriggerSubject(subject *Subject) error {
	switch subject.Applicability {
	case "installed":
		if subject.Source != "trigger_pack" || subject.TriggerAdmission != nil {
			return fmt.Errorf("installed provider trigger subject %q must use trigger_pack source and must not carry target admission", subject.ID)
		}
	case "effective":
		if subject.Source != "trigger_pack_binding" && subject.Source != "raw_declaration" {
			return fmt.Errorf("effective provider trigger subject %q has invalid source %q", subject.ID, subject.Source)
		}
		if subject.TriggerAdmission == nil {
			return fmt.Errorf("effective provider trigger subject %q requires typed trigger_admission", subject.ID)
		}
		admission := subject.TriggerAdmission
		admission.BundleHash = strings.TrimSpace(admission.BundleHash)
		admission.Alias = strings.Trim(strings.TrimSpace(admission.Alias), "/")
		admission.CatalogGeneration = strings.TrimSpace(admission.CatalogGeneration)
		admission.PolicySource = strings.TrimSpace(admission.PolicySource)
		admission.RequestAuthentication = strings.TrimSpace(admission.RequestAuthentication)
		admission.Event = strings.TrimSpace(admission.Event)
		admission.SignedPayload = strings.TrimSpace(admission.SignedPayload)
		admission.DigestEncoding = strings.TrimSpace(admission.DigestEncoding)
		if admission.BundleHash == "" || admission.Alias == "" || admission.CatalogGeneration == "" || admission.Event == "" {
			return fmt.Errorf("effective provider trigger subject %q requires bundle_hash, alias, catalog_generation, and event", subject.ID)
		}
		wantID := "ingress:" + admission.BundleHash + ":" + admission.Alias + ":" + subject.Provider
		if subject.ID != wantID {
			return fmt.Errorf("effective provider trigger subject %q must use stable target id %q", subject.ID, wantID)
		}
		if admission.PolicySource != "verified_pack" && admission.PolicySource != "raw_declaration" {
			return fmt.Errorf("effective provider trigger subject %q has invalid policy_source %q", subject.ID, admission.PolicySource)
		}
		allowedAuth := map[string]bool{"TOKEN_EQUALITY": true, "TOKEN": true, "HMAC_SHA256": true, "HMAC_SHA1": true, "UNAUTHENTICATED": true}
		if !allowedAuth[admission.RequestAuthentication] {
			return fmt.Errorf("effective provider trigger subject %q has invalid request_authentication %q", subject.ID, admission.RequestAuthentication)
		}
		if admission.PolicySource == "verified_pack" {
			if subject.Source != "trigger_pack_binding" || admission.Pack == nil {
				return fmt.Errorf("effective verified-pack trigger subject %q requires trigger_pack_binding source and pack identity", subject.ID)
			}
			pack := admission.Pack
			pack.ID = strings.TrimSpace(pack.ID)
			pack.Version = strings.TrimSpace(pack.Version)
			pack.ManifestHash = strings.TrimSpace(pack.ManifestHash)
			pack.Provenance = strings.TrimSpace(pack.Provenance)
			if pack.ID == "" || pack.Version == "" || pack.ManifestHash == "" || pack.Provenance == "" {
				return fmt.Errorf("effective verified-pack trigger subject %q has incomplete pack identity", subject.ID)
			}
		} else if subject.Source != "raw_declaration" || admission.Pack != nil {
			return fmt.Errorf("effective raw trigger subject %q must use raw_declaration source without pack identity", subject.ID)
		}
	default:
		return fmt.Errorf("provider trigger subject %q has invalid applicability %q", subject.ID, subject.Applicability)
	}
	for _, requirement := range subject.Requirements {
		if requirement.Scope != RequirementScopeTarget || requirement.Satisfied != nil || requirement.Status != "" || requirement.Remediation != "" {
			return fmt.Errorf("provider trigger subject %q requirement %q must remain target-scoped and unevaluated", subject.ID, requirement.Name)
		}
	}
	if subject.Applicability == "effective" {
		unauthenticated := subject.TriggerAdmission.RequestAuthentication == "UNAUTHENTICATED"
		if unauthenticated && len(subject.Requirements) != 0 {
			return fmt.Errorf("effective unauthenticated provider trigger subject %q must not carry secret requirements", subject.ID)
		}
		if !unauthenticated && len(subject.Requirements) != 1 {
			return fmt.Errorf("effective authenticated provider trigger subject %q must carry exactly one unevaluated secret requirement", subject.ID)
		}
	}
	return nil
}

func validateConnectorRequirement(subjectID string, requirement Requirement) error {
	if requirement.Satisfied == nil || requirement.Status == "" {
		return fmt.Errorf("provider connector subject %q requirement %q must be evaluated", subjectID, requirement.Name)
	}
	allowed := false
	if statuses, ok := connectorRequirementStatuses[requirement.Kind]; ok {
		allowed = statuses[requirement.Status]
	}
	if !allowed {
		return fmt.Errorf("provider connector subject %q requirement %q has invalid %s status %q", subjectID, requirement.Name, requirement.Kind, requirement.Status)
	}
	wantSatisfied := requirementSatisfied(requirement.Kind, requirement.Status)
	if *requirement.Satisfied != wantSatisfied {
		return fmt.Errorf("provider connector subject %q requirement %q status %q contradicts satisfied=%t", subjectID, requirement.Name, requirement.Status, *requirement.Satisfied)
	}
	wantRemediation := requirementRemediation(requirement.Kind, requirement.Name, requirement.Status)
	if requirement.Remediation != wantRemediation {
		return fmt.Errorf("provider connector subject %q requirement %q remediation does not match canonical %s/%s remediation", subjectID, requirement.Name, requirement.Kind, requirement.Status)
	}
	return nil
}

func RenderSubject(subject Subject, verbose bool) string {
	normalized, err := NormalizeSubjects([]Subject{subject})
	if err != nil {
		return "invalid capability subject: " + err.Error()
	}
	subject = normalized[0]
	parts := []string{fmt.Sprintf("%s %s %s",
		userfacing.ProjectHumanCode(userfacing.HumanCodeProviderSubjectKind, string(subject.Kind)),
		subject.ID,
		userfacing.ProjectHumanCode(userfacing.HumanCodeProviderSubjectStatus, string(subject.Status)),
	)}
	if subject.Provenance != "" {
		parts = append(parts, "provenance="+subject.Provenance)
	}
	if subject.SourcePath != "" {
		parts = append(parts, "source_path="+subject.SourcePath)
	}
	if subject.TriggerAdmission != nil {
		admission := subject.TriggerAdmission
		parts = append(parts,
			"alias="+admission.Alias,
			"policy_source="+admission.PolicySource,
			"request_authentication="+admission.RequestAuthentication,
			"event="+admission.Event,
			"catalog_generation="+admission.CatalogGeneration,
		)
		if admission.Pack != nil {
			parts = append(parts, "pack="+admission.Pack.ID+"@"+admission.Pack.Version+" manifest_hash="+admission.Pack.ManifestHash)
		}
	}
	for _, requirement := range subject.Requirements {
		parts = append(parts, renderRequirement(requirement))
	}
	for _, capability := range subject.Capabilities {
		phrase := userfacing.ProjectHumanCode(userfacing.HumanCodeProviderCapability, capability.Code)
		if capability.Target != "" {
			phrase += " " + capability.Target
		}
		parts = append(parts, "CAN "+phrase)
	}
	for _, guarantee := range subject.Guarantees {
		phrase := userfacing.ProjectHumanCode(userfacing.HumanCodeProviderGuarantee, guarantee.Code)
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
	status = userfacing.ProjectHumanCode(userfacing.HumanCodeProviderRequirementStatus, status)
	text := "requires " + name + "=" + status
	if requirement.Remediation != "" && requirement.Satisfied != nil && !*requirement.Satisfied {
		text += " (fix: " + requirement.Remediation + ")"
	}
	return text
}
