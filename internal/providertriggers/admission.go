package providertriggers

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
)

type AdmissionKind string

const (
	AdmissionKindPack AdmissionKind = "pack"
	AdmissionKindRaw  AdmissionKind = "raw"
)

type PolicySource string

const (
	PolicySourceVerifiedPack   PolicySource = "verified_pack"
	PolicySourceRawDeclaration PolicySource = "raw_declaration"
)

type RequestAuthentication string

const (
	RequestAuthenticationTokenEquality RequestAuthentication = "TOKEN_EQUALITY"
	RequestAuthenticationToken         RequestAuthentication = "TOKEN"
	RequestAuthenticationHMACSHA256    RequestAuthentication = "HMAC_SHA256"
	RequestAuthenticationHMACSHA1      RequestAuthentication = "HMAC_SHA1"
	RequestAuthenticationNone          RequestAuthentication = "UNAUTHENTICATED"
)

const UnsignedWebhookAcknowledgement = "unsigned_webhook"

type AdmissionDeclaration struct {
	Kind           string
	PackID         string
	Acknowledge    string
	Authentication RawAuthenticationDeclaration
	Event          string
	DeliveryID     RawDeliveryIDDeclaration
	Payload        string
}

type RawAuthenticationDeclaration struct {
	Kind     string
	Header   string
	Prefix   string
	Encoding string
}

type RawDeliveryIDDeclaration struct {
	Source   string
	Header   string
	JSONPath string
}

type CompileAdmissionRequest struct {
	Alias         string
	Provider      string
	SigningSecret string
	Declaration   AdmissionDeclaration
}

type RawAdmissionPolicy struct {
	Authentication RawAuthenticationDeclaration
	Event          string
	DeliveryID     RawDeliveryIDDeclaration
	Payload        string
}

type InboundAdmissionPlan struct {
	generationID          string
	provider              string
	policySource          PolicySource
	requestAuthentication RequestAuthentication
	packIdentity          *PackIdentity
	manifest              *Manifest
	raw                   *RawAdmissionPolicy
	requiresSecret        bool
	outputs               []OutputManifest
	acknowledgedUnsigned  bool
}

// AdmittedRequest is the authenticated, retry-stable request identity. Its
// private projection state can only be consumed by the plan that admitted it.
type AdmittedRequest struct {
	ProviderEventID           string
	ProviderEventType         string
	SemanticContentDigest     string
	Response                  *Response
	AcknowledgeBeforeDispatch bool

	generationID      string
	provider          string
	manifestOwner     *Manifest
	rawOwner          *RawAdmissionPolicy
	manifestAdmission *manifestAdmission
	rawAdmission      *rawRequestAdmission
}

type rawRequestAdmission struct {
	request    Request
	deliveryID string
	eventType  string
	payload    any
}

func (s *CatalogSnapshot) CompileAdmission(req CompileAdmissionRequest) (InboundAdmissionPlan, error) {
	alias := strings.Trim(strings.TrimSpace(req.Alias), "/")
	provider := NormalizeProviderName(req.Provider)
	if alias == "" || provider == "" {
		return InboundAdmissionPlan{}, fmt.Errorf("ingress admission requires alias and provider")
	}
	if s == nil {
		return InboundAdmissionPlan{}, fmt.Errorf("ingress alias %q provider %q cannot compile admission: provider trigger catalog snapshot is required", alias, provider)
	}
	kind := strings.ToLower(strings.TrimSpace(req.Declaration.Kind))
	if kind == "" {
		kind = string(AdmissionKindPack)
	}
	switch AdmissionKind(kind) {
	case AdmissionKindPack:
		return s.compilePackAdmission(alias, provider, strings.TrimSpace(req.SigningSecret), req.Declaration)
	case AdmissionKindRaw:
		return s.compileRawAdmission(alias, provider, strings.TrimSpace(req.SigningSecret), req.Declaration)
	default:
		return InboundAdmissionPlan{}, fmt.Errorf("ingress alias %q provider %q admission.kind must be pack or raw; got %q", alias, provider, req.Declaration.Kind)
	}
}

func (s *CatalogSnapshot) compilePackAdmission(alias, provider, signingSecret string, declaration AdmissionDeclaration) (InboundAdmissionPlan, error) {
	if s == nil {
		return InboundAdmissionPlan{}, fmt.Errorf("ingress alias %q provider %q cannot compile pack-required admission: provider trigger catalog snapshot is required", alias, provider)
	}
	if hasRawFields(declaration) {
		return InboundAdmissionPlan{}, fmt.Errorf("ingress alias %q provider %q uses pack admission and must not declare authentication, event, delivery_id, or payload; remove raw-only fields", alias, provider)
	}
	var (
		entry CatalogEntry
		ok    bool
	)
	if pin := strings.TrimSpace(declaration.PackID); pin != "" {
		entry, ok = s.EntryByID(pin)
		if !ok {
			if installed, exists := s.EntryByProvider(provider); exists {
				return InboundAdmissionPlan{}, fmt.Errorf("ingress alias %q provider %q pins pack %q, but that id is not loaded; verified pack for %q is %q; fix admission.pack.id or provider_triggers.packs.*", alias, provider, pin, provider, installed.Identity.ID)
			}
			return InboundAdmissionPlan{}, fmt.Errorf("ingress alias %q provider %q pins pack %q, but that id is not loaded; fix admission.pack.id or provider_triggers.packs.*", alias, provider, pin)
		}
		entryProvider := NormalizeProviderName(entry.Manifest.Provider)
		if entryProvider != provider {
			return InboundAdmissionPlan{}, fmt.Errorf("ingress alias %q provider %q pins pack %q, which provides %q; use a pack for %q or change provider to %q", alias, provider, pin, entryProvider, provider, entryProvider)
		}
	} else {
		entry, ok = s.EntryByProvider(provider)
		if !ok {
			return InboundAdmissionPlan{}, fmt.Errorf("ingress alias %q provider %q is pack-required, but no verified trigger pack provides %q; configure that pack in provider_triggers.packs.external_dirs, or declare admission.kind: raw with an explicit raw policy", alias, provider, provider)
		}
	}
	auth, err := manifestRequestAuthentication(entry.Manifest)
	if err != nil {
		return InboundAdmissionPlan{}, err
	}
	ack := strings.TrimSpace(declaration.Acknowledge)
	if err := validateAcknowledgement(alias, provider, auth, ack, true); err != nil {
		return InboundAdmissionPlan{}, err
	}
	requiresSecret := auth != RequestAuthenticationNone
	if requiresSecret && signingSecret == "" {
		return InboundAdmissionPlan{}, fmt.Errorf("ingress alias %q provider %q requires signing_secret for %s request authentication", alias, provider, auth)
	}
	if !requiresSecret && signingSecret != "" {
		return InboundAdmissionPlan{}, fmt.Errorf("ingress alias %q provider %q is UNAUTHENTICATED and must not declare signing_secret; remove signing_secret", alias, provider)
	}
	manifest := entry.Manifest
	identity := entry.Identity
	return InboundAdmissionPlan{
		generationID: s.GenerationID(), provider: provider, policySource: PolicySourceVerifiedPack,
		requestAuthentication: auth, packIdentity: &identity, manifest: &manifest,
		requiresSecret: requiresSecret, outputs: manifest.OutputManifest(),
		acknowledgedUnsigned: ack == UnsignedWebhookAcknowledgement,
	}, nil
}

func (s *CatalogSnapshot) compileRawAdmission(alias, provider, signingSecret string, declaration AdmissionDeclaration) (InboundAdmissionPlan, error) {
	if strings.TrimSpace(declaration.PackID) != "" {
		return InboundAdmissionPlan{}, fmt.Errorf("raw ingress alias %q provider %q must not pin a pack; remove admission.pack", alias, provider)
	}
	if s != nil {
		if entry, exists := s.EntryByProvider(provider); exists {
			return InboundAdmissionPlan{}, fmt.Errorf("raw ingress alias %q provider %q conflicts with installed pack %q; use pack admission or rename the intentional raw namespace to %q", alias, provider, entry.Identity.ID, provider+"-raw")
		}
	}
	policy, auth, err := compileRawPolicy(alias, provider, declaration)
	if err != nil {
		return InboundAdmissionPlan{}, err
	}
	ack := strings.TrimSpace(declaration.Acknowledge)
	if err := validateAcknowledgement(alias, provider, auth, ack, false); err != nil {
		return InboundAdmissionPlan{}, err
	}
	requiresSecret := auth != RequestAuthenticationNone
	if requiresSecret && signingSecret == "" {
		return InboundAdmissionPlan{}, fmt.Errorf("ingress alias %q provider %q requires signing_secret for %s request authentication", alias, provider, auth)
	}
	if !requiresSecret && signingSecret != "" {
		return InboundAdmissionPlan{}, fmt.Errorf("ingress alias %q provider %q is UNAUTHENTICATED and must not declare signing_secret; remove signing_secret", alias, provider)
	}
	generationID := ""
	if s != nil {
		generationID = s.GenerationID()
	}
	return InboundAdmissionPlan{
		generationID: generationID, provider: provider, policySource: PolicySourceRawDeclaration,
		requestAuthentication: auth, raw: &policy, requiresSecret: requiresSecret,
		outputs: []OutputManifest{{Kind: OutputKindRaw, EventName: EventNameManifest{Literal: policy.Event}}}, acknowledgedUnsigned: ack == UnsignedWebhookAcknowledgement,
	}, nil
}

func hasRawFields(declaration AdmissionDeclaration) bool {
	return strings.TrimSpace(declaration.Authentication.Kind) != "" || strings.TrimSpace(declaration.Authentication.Header) != "" ||
		strings.TrimSpace(declaration.Authentication.Prefix) != "" || strings.TrimSpace(declaration.Authentication.Encoding) != "" ||
		strings.TrimSpace(declaration.Event) != "" || strings.TrimSpace(declaration.DeliveryID.Source) != "" ||
		strings.TrimSpace(declaration.DeliveryID.Header) != "" || strings.TrimSpace(declaration.DeliveryID.JSONPath) != "" ||
		strings.TrimSpace(declaration.Payload) != ""
}

func manifestRequestAuthentication(manifest Manifest) (RequestAuthentication, error) {
	switch strings.TrimSpace(manifest.Signature.Type) {
	case signatureTypeTokenEquality:
		return RequestAuthenticationTokenEquality, nil
	case signatureTypeHMACSHA256:
		return RequestAuthenticationHMACSHA256, nil
	case signatureTypeHMACSHA1:
		return RequestAuthenticationHMACSHA1, nil
	case "":
		return RequestAuthenticationNone, nil
	default:
		return "", fmt.Errorf("provider trigger manifest %q has unsupported request authentication %q", NormalizeProviderName(manifest.Provider), manifest.Signature.Type)
	}
}

func validateAcknowledgement(alias, provider string, auth RequestAuthentication, acknowledge string, pack bool) error {
	if acknowledge != "" && acknowledge != UnsignedWebhookAcknowledgement {
		return fmt.Errorf("ingress alias %q provider %q admission.acknowledge must be %q when present; remove it or use the canonical token", alias, provider, UnsignedWebhookAcknowledgement)
	}
	if auth != RequestAuthenticationNone && acknowledge != "" {
		return fmt.Errorf("ingress alias %q provider %q acknowledges unsigned_webhook, but the compiled admission uses %s; remove admission.acknowledge", alias, provider, auth)
	}
	if pack && auth == RequestAuthenticationNone && acknowledge == "" {
		return fmt.Errorf("ingress alias %q provider %q resolves an unauthenticated verified pack; add admission.acknowledge: unsigned_webhook to authorize this public endpoint, or install an authenticated pack", alias, provider)
	}
	return nil
}

func compileRawPolicy(alias, provider string, declaration AdmissionDeclaration) (RawAdmissionPolicy, RequestAuthentication, error) {
	auth := declaration.Authentication
	auth.Kind = strings.ToLower(strings.TrimSpace(auth.Kind))
	auth.Header = http.CanonicalHeaderKey(strings.TrimSpace(auth.Header))
	auth.Encoding = strings.ToLower(strings.TrimSpace(auth.Encoding))
	var projected RequestAuthentication
	switch auth.Kind {
	case "none":
		projected = RequestAuthenticationNone
		if auth.Header != "" || auth.Prefix != "" || auth.Encoding != "" {
			return RawAdmissionPolicy{}, "", fmt.Errorf("raw ingress alias %q provider %q authentication.kind none must not declare header, prefix, or encoding", alias, provider)
		}
	case "token":
		projected = RequestAuthenticationToken
		if auth.Header == "" {
			return RawAdmissionPolicy{}, "", fmt.Errorf("raw ingress alias %q provider %q requires admission.authentication.header for token authentication", alias, provider)
		}
		if auth.Encoding != "" {
			return RawAdmissionPolicy{}, "", fmt.Errorf("raw ingress alias %q provider %q token authentication must not declare encoding", alias, provider)
		}
	case "hmac_sha256":
		projected = RequestAuthenticationHMACSHA256
		if auth.Header == "" {
			return RawAdmissionPolicy{}, "", fmt.Errorf("raw ingress alias %q provider %q requires admission.authentication.header for hmac_sha256 authentication", alias, provider)
		}
		if auth.Encoding == "" {
			auth.Encoding = "hex"
		}
		if auth.Encoding != "hex" && auth.Encoding != "base64" {
			return RawAdmissionPolicy{}, "", fmt.Errorf("raw ingress alias %q provider %q admission.authentication.encoding must be hex or base64", alias, provider)
		}
	default:
		return RawAdmissionPolicy{}, "", fmt.Errorf("raw ingress alias %q provider %q requires admission.authentication.kind: none, token, or hmac_sha256", alias, provider)
	}
	event := strings.TrimSpace(declaration.Event)
	if event == "" {
		return RawAdmissionPolicy{}, "", fmt.Errorf("raw ingress alias %q provider %q requires admission.event; add the exact literal event name", alias, provider)
	}
	delivery := declaration.DeliveryID
	delivery.Source = strings.ToLower(strings.TrimSpace(delivery.Source))
	delivery.Header = http.CanonicalHeaderKey(strings.TrimSpace(delivery.Header))
	switch delivery.Source {
	case "header":
		if delivery.Header == "" || strings.TrimSpace(delivery.JSONPath) != "" {
			return RawAdmissionPolicy{}, "", fmt.Errorf("raw ingress alias %q provider %q delivery_id source header requires header and forbids json_path", alias, provider)
		}
	case "json_path":
		if strings.TrimSpace(delivery.JSONPath) == "" || delivery.Header != "" {
			return RawAdmissionPolicy{}, "", fmt.Errorf("raw ingress alias %q provider %q delivery_id source json_path requires json_path and forbids header", alias, provider)
		}
		if err := validateJSONPath(provider, "raw delivery_id.json_path", delivery.JSONPath); err != nil {
			return RawAdmissionPolicy{}, "", err
		}
	case "body_sha256":
		if delivery.Header != "" || strings.TrimSpace(delivery.JSONPath) != "" {
			return RawAdmissionPolicy{}, "", fmt.Errorf("raw ingress alias %q provider %q delivery_id source body_sha256 forbids header and json_path", alias, provider)
		}
	default:
		return RawAdmissionPolicy{}, "", fmt.Errorf("raw ingress alias %q provider %q requires admission.delivery_id.source: header, json_path, or body_sha256", alias, provider)
	}
	payload := strings.ToLower(strings.TrimSpace(declaration.Payload))
	if payload != "json" && payload != "raw" {
		return RawAdmissionPolicy{}, "", fmt.Errorf("raw ingress alias %q provider %q requires admission.payload: json or raw", alias, provider)
	}
	return RawAdmissionPolicy{Authentication: auth, Event: event, DeliveryID: delivery, Payload: payload}, projected, nil
}

func (p InboundAdmissionPlan) Valid() bool {
	return p.provider != "" && p.generationID != "" && (p.manifest != nil || p.raw != nil)
}

func (p InboundAdmissionPlan) GenerationID() string       { return p.generationID }
func (p InboundAdmissionPlan) Provider() string           { return p.provider }
func (p InboundAdmissionPlan) PolicySource() PolicySource { return p.policySource }
func (p InboundAdmissionPlan) RequestAuthentication() RequestAuthentication {
	return p.requestAuthentication
}
func (p InboundAdmissionPlan) RequiresSecret() bool { return p.requiresSecret }
func (p InboundAdmissionPlan) Outputs() []OutputManifest {
	out := make([]OutputManifest, 0, len(p.outputs))
	for _, output := range p.outputs {
		fields := make(map[string]runtimecontracts.FieldProjection, len(output.Fields))
		for name, field := range output.Fields {
			fields[name] = field
		}
		output.Fields = fields
		output.When.Exists = append([]string{}, output.When.Exists...)
		output.When.Absent = append([]string{}, output.When.Absent...)
		out = append(out, output)
	}
	return out
}
func (p InboundAdmissionPlan) RawOutput() (OutputManifest, bool) {
	for _, output := range p.Outputs() {
		if output.Kind == OutputKindRaw {
			return output, true
		}
	}
	return OutputManifest{}, false
}
func (p InboundAdmissionPlan) PackIdentity() (PackIdentity, bool) {
	if p.packIdentity == nil {
		return PackIdentity{}, false
	}
	return *p.packIdentity, true
}
func (p InboundAdmissionPlan) AcknowledgedUnsigned() bool { return p.acknowledgedUnsigned }

type EffectiveSubjectRequest struct {
	BundleHash    string
	Alias         string
	SigningSecret string
	SourcePath    string
}

func (s *CatalogSnapshot) InstalledCapabilitySubjects() ([]packs.Subject, error) {
	entries := s.Entries()
	subjects := make([]packs.Subject, 0, len(entries))
	for _, entry := range entries {
		loaded := LoadedPack{
			Envelope: packs.Envelope{
				ID: entry.Identity.ID, Version: entry.Identity.Version, ManifestHash: entry.Identity.ManifestHash,
				Provenance: packs.Provenance{Source: entry.Identity.Provenance},
			},
			Manifest: entry.Manifest, SourcePath: entry.SourcePath, Source: entry.Source,
		}
		subject, err := loaded.CapabilitySubject()
		if err != nil {
			return nil, err
		}
		subjects = append(subjects, subject)
	}
	return packs.NormalizeSubjects(subjects)
}

func (p InboundAdmissionPlan) EffectiveCapabilitySubject(req EffectiveSubjectRequest) (packs.Subject, error) {
	if !p.Valid() {
		return packs.Subject{}, fmt.Errorf("compiled inbound admission plan is required")
	}
	bundleHash := strings.TrimSpace(req.BundleHash)
	alias := strings.Trim(strings.TrimSpace(req.Alias), "/")
	if bundleHash == "" || alias == "" {
		return packs.Subject{}, fmt.Errorf("effective inbound admission subject requires bundle_hash and alias")
	}
	eventName := rawOutputName(p.outputs)
	source := "raw_declaration"
	provenance := "project"
	admission := &packs.TriggerAdmission{
		BundleHash: bundleHash, Alias: alias, CatalogGeneration: p.generationID,
		PolicySource: string(p.policySource), RequestAuthentication: string(p.requestAuthentication), Event: eventName,
	}
	if p.manifest != nil {
		source = "trigger_pack_binding"
		provenance = p.packIdentity.Provenance
		admission.SignedPayload = strings.TrimSpace(p.manifest.Signature.SignedPayload)
		admission.DigestEncoding = strings.TrimSpace(p.manifest.Signature.digestEncoding())
		if strings.TrimSpace(p.manifest.Signature.Type) == signatureTypeTokenEquality || p.requestAuthentication == RequestAuthenticationNone {
			admission.DigestEncoding = ""
		}
		admission.Pack = &packs.TriggerPackIdentity{
			ID: p.packIdentity.ID, Version: p.packIdentity.Version,
			ManifestHash: p.packIdentity.ManifestHash, Provenance: p.packIdentity.Provenance,
		}
	} else if p.raw != nil && p.requestAuthentication == RequestAuthenticationHMACSHA256 {
		admission.SignedPayload = "raw_body"
		admission.DigestEncoding = p.raw.Authentication.Encoding
	}
	subject := packs.Subject{
		ID:   "ingress:" + bundleHash + ":" + alias + ":" + p.provider,
		Kind: packs.SubjectProviderTrigger, Provider: p.provider, Source: source,
		Provenance: provenance, SourcePath: strings.TrimSpace(req.SourcePath), Applicability: "effective",
		TriggerAdmission: admission,
		Capabilities: []packs.Capability{
			{Code: packs.CapabilityReceiveHTTPSRoute, Target: "/webhooks/" + alias + "/" + p.provider},
			{Code: packs.CapabilityEmitEvent, Target: eventName},
			{Code: packs.CapabilityPersistDedupeMarkers},
		},
	}
	if p.manifest != nil {
		subject.TriggerEvents = triggerEventDescriptors(*p.manifest)
	} else {
		subject.TriggerEvents = []packs.TriggerEventDescriptor{{Event: eventName, Kind: string(OutputKindRaw)}}
	}
	for _, output := range p.outputs {
		name := strings.TrimSpace(output.Event)
		if output.Kind == OutputKindRaw {
			name = strings.TrimSpace(output.EventName.Literal)
			if name == "" {
				name = strings.TrimSpace(output.EventName.Template)
			}
		}
		if name != "" && name != eventName {
			subject.Capabilities = append(subject.Capabilities, packs.Capability{Code: packs.CapabilityEmitEvent, Target: name})
		}
	}
	if p.requiresSecret {
		subject.Capabilities = append(subject.Capabilities, packs.Capability{Code: packs.CapabilityVerifySecret, Target: strings.TrimSpace(req.SigningSecret)})
		subject.Requirements = append(subject.Requirements, packs.TargetScopedRequirement(packs.RequirementSecret, strings.TrimSpace(req.SigningSecret)))
	}
	for _, code := range []string{cannotEmitUndeclaredEvents, cannotRunCodeBeforeAdmission, cannotTouchUnboundResources} {
		guarantee, err := packs.NewGuarantee(code)
		if err != nil {
			return packs.Subject{}, err
		}
		subject.Guarantees = append(subject.Guarantees, guarantee)
	}
	normalized, err := packs.NormalizeSubjects([]packs.Subject{subject})
	if err != nil {
		return packs.Subject{}, err
	}
	return normalized[0], nil
}

func (p InboundAdmissionPlan) Accept(req Request) (Delivery, error) {
	admitted, err := p.AdmitRequest(req)
	if err != nil {
		return Delivery{}, err
	}
	return p.ProjectDelivery(admitted)
}

// AdmitRequest authenticates a request and resolves only the provider-owned
// identity needed to consult the durable publication ledger. It deliberately
// does not run normalized projection or construct executable output events.
func (p InboundAdmissionPlan) AdmitRequest(req Request) (AdmittedRequest, error) {
	if !p.Valid() {
		return AdmittedRequest{}, badRequest("compiled inbound admission plan is required")
	}
	provider := NormalizeProviderName(req.Provider)
	if provider == "" {
		provider = p.provider
	}
	if provider != p.provider {
		return AdmittedRequest{}, badRequest(fmt.Sprintf("compiled inbound admission plan provider %q does not match request provider %q", p.provider, provider))
	}
	req = req.withProvider(provider)
	if p.requiresSecret && strings.TrimSpace(req.Target.WebhookSecret) == "" {
		return AdmittedRequest{}, unauthorized(provider + " webhook signing secret is required")
	}
	if p.manifest != nil {
		manifestAdmission, err := p.manifest.admitRequest(req)
		if err != nil {
			return AdmittedRequest{}, err
		}
		if p.packIdentity == nil {
			return AdmittedRequest{}, badRequest("compiled pack admission requires verified pack identity")
		}
		admitted := AdmittedRequest{
			ProviderEventID: manifestAdmission.deliveryID, ProviderEventType: manifestAdmission.eventType,
			Response:                  manifestAdmission.response,
			AcknowledgeBeforeDispatch: strings.TrimSpace(p.manifest.Ack.Mode) == "durable_before_dispatch",
			generationID:              p.generationID, provider: p.provider, manifestOwner: p.manifest,
			manifestAdmission: &manifestAdmission,
		}
		if admitted.Response == nil {
			semanticContent := req.Payload
			if strings.TrimSpace(p.manifest.PayloadSource) == "form" {
				semanticContent = formValuesPayload(req.Form)
			}
			admitted.SemanticContentDigest, err = semanticContentDigest(semanticContent)
			if err != nil {
				return AdmittedRequest{}, err
			}
		}
		return admitted, nil
	}
	rawAdmission, err := p.admitExplicitRaw(req)
	if err != nil {
		return AdmittedRequest{}, err
	}
	digest, err := semanticContentDigest(rawAdmission.payload)
	if err != nil {
		return AdmittedRequest{}, err
	}
	return AdmittedRequest{
		ProviderEventID: rawAdmission.deliveryID, ProviderEventType: rawAdmission.eventType,
		SemanticContentDigest: digest, generationID: p.generationID, provider: p.provider,
		rawOwner: p.raw, rawAdmission: &rawAdmission,
	}, nil
}

// ProjectDelivery constructs the raw and optional normalized executable
// outputs only after the durable publication ledger has reported a miss.
func (p InboundAdmissionPlan) ProjectDelivery(admitted AdmittedRequest) (Delivery, error) {
	if admitted.generationID != p.generationID || admitted.provider != p.provider {
		return Delivery{}, badRequest("admitted request belongs to a different compiled admission plan")
	}
	if admitted.Response != nil {
		return Delivery{Response: admitted.Response}, nil
	}
	if p.manifest != nil {
		if admitted.manifestOwner != p.manifest || admitted.manifestAdmission == nil {
			return Delivery{}, badRequest("admitted request does not belong to the compiled pack admission plan")
		}
		delivery, err := p.manifest.projectAdmission(*admitted.manifestAdmission)
		if err != nil {
			var normalizationErr NormalizationError
			if errors.As(err, &normalizationErr) && p.packIdentity != nil {
				return Delivery{}, badRequest(fmt.Sprintf("pack %s version=%s manifest_hash=%s normalized event %q path %q failed: %s", p.packIdentity.ID, p.packIdentity.Version, p.packIdentity.ManifestHash, normalizationErr.Event, normalizationErr.Path, normalizationErr.Cause))
			}
			return Delivery{}, err
		}
		for index := range delivery.Events {
			if delivery.Events[index].Kind != OutputKindNormalized {
				continue
			}
			delivery.Events[index].Authorization = runtimeprovideroutput.Authorization{
				Provider: p.provider, Event: string(delivery.Events[index].Name),
				PackID: p.packIdentity.ID, PackVersion: p.packIdentity.Version,
				ManifestHash: p.packIdentity.ManifestHash, GenerationID: p.generationID,
			}.Normalized()
		}
		return delivery, nil
	}
	if admitted.rawOwner != p.raw || admitted.rawAdmission == nil {
		return Delivery{}, badRequest("admitted request does not belong to the compiled raw admission plan")
	}
	raw := admitted.rawAdmission
	return Delivery{
		ProviderEventID: raw.deliveryID, ProviderEventType: raw.eventType,
		Events: []DeliveryEvent{{Name: events.EventType(p.raw.Event), Kind: OutputKindRaw, Payload: map[string]any{
			"provider": p.provider, "provider_event_id": raw.deliveryID,
			"provider_event_type": raw.eventType, "data": raw.payload,
		}}},
	}, nil
}

func (p InboundAdmissionPlan) admitExplicitRaw(req Request) (rawRequestAdmission, error) {
	policy := p.raw
	if policy == nil {
		return rawRequestAdmission{}, badRequest("compiled raw admission policy is required")
	}
	if err := verifyRawAuthentication(policy.Authentication, req.Target.WebhookSecret, req.Headers, req.Body); err != nil {
		return rawRequestAdmission{}, err
	}
	var parsed any
	if policy.Payload == "json" {
		if err := json.Unmarshal(req.Body, &parsed); err != nil {
			return rawRequestAdmission{}, badRequest("raw webhook payload must be valid JSON")
		}
	} else {
		parsed = string(req.Body)
	}
	deliverySource := parsed
	if policy.DeliveryID.Source == "json_path" && policy.Payload == "raw" {
		if err := json.Unmarshal(req.Body, &deliverySource); err != nil {
			return rawRequestAdmission{}, badRequest("raw webhook delivery id JSON path requires a valid JSON request body")
		}
	}
	deliveryID, err := rawDeliveryID(policy.DeliveryID, req.Headers, deliverySource, req.Body)
	if err != nil {
		return rawRequestAdmission{}, err
	}
	return rawRequestAdmission{request: req, deliveryID: deliveryID, eventType: NormalizeEventToken(policy.Event), payload: parsed}, nil
}

func semanticContentDigest(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal provider-authored semantic content: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func rawOutputName(outputs []OutputManifest) string {
	for _, output := range outputs {
		if output.Kind != OutputKindRaw {
			continue
		}
		if literal := strings.TrimSpace(output.EventName.Literal); literal != "" {
			return literal
		}
		return strings.TrimSpace(output.EventName.Template)
	}
	return ""
}

func verifyRawAuthentication(auth RawAuthenticationDeclaration, secret string, headers http.Header, body []byte) error {
	if auth.Kind == "none" {
		return nil
	}
	if strings.TrimSpace(secret) == "" {
		return unauthorized("webhook signing secret is required")
	}
	values := headers.Values(auth.Header)
	if len(values) != 1 || strings.TrimSpace(values[0]) == "" {
		return unauthorized("invalid webhook authentication header")
	}
	candidate := strings.TrimSpace(values[0])
	if auth.Prefix != "" {
		if !strings.HasPrefix(candidate, auth.Prefix) {
			return unauthorized("invalid webhook authentication header")
		}
		candidate = strings.TrimSpace(strings.TrimPrefix(candidate, auth.Prefix))
	}
	switch auth.Kind {
	case "token":
		if hmac.Equal([]byte(candidate), []byte(strings.TrimSpace(secret))) {
			return nil
		}
	case "hmac_sha256":
		mac := hmac.New(sha256.New, []byte(strings.TrimSpace(secret)))
		_, _ = mac.Write(body)
		digest := mac.Sum(nil)
		expected := hex.EncodeToString(digest)
		if auth.Encoding == "base64" {
			expected = base64.StdEncoding.EncodeToString(digest)
		}
		if auth.Encoding == "hex" {
			candidate = strings.ToLower(candidate)
			expected = strings.ToLower(expected)
		}
		if hmac.Equal([]byte(candidate), []byte(expected)) {
			return nil
		}
	}
	return unauthorized("invalid webhook authentication")
}

func rawDeliveryID(policy RawDeliveryIDDeclaration, headers http.Header, payload any, body []byte) (string, error) {
	switch policy.Source {
	case "header":
		values := headers.Values(policy.Header)
		if len(values) != 1 || strings.TrimSpace(values[0]) == "" {
			return "", badRequest("raw webhook delivery id header is required and must occur exactly once")
		}
		return strings.TrimSpace(values[0]), nil
	case "json_path":
		value, ok := stringFromJSONPath(payload, policy.JSONPath)
		if !ok || strings.TrimSpace(value) == "" {
			return "", badRequest("raw webhook delivery id JSON path is required")
		}
		return strings.TrimSpace(value), nil
	case "body_sha256":
		sum := sha256.Sum256(body)
		return hex.EncodeToString(sum[:]), nil
	default:
		return "", badRequest("compiled raw webhook delivery id policy is invalid")
	}
}
