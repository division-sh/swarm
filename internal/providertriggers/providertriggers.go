package providertriggers

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packs"
	"gopkg.in/yaml.v3"
)

const (
	cannotRunCodeBeforeAdmission = "run_code_before_admission"
	cannotEmitUndeclaredEvents   = "emit_undeclared_events"
	cannotTouchUnboundResources  = "touch_unbound_resources"

	signatureTypeHMACSHA256    = "hmac_sha256"
	signatureTypeHMACSHA1      = "hmac_sha1"
	signatureTypeTokenEquality = "token_equality"
)

type Target struct {
	EntityID      string
	EntitySlug    string
	WebhookSecret string
}

func (t Target) EffectiveEntityID() string {
	return firstNonEmpty(t.EntityID, t.EntitySlug)
}

type Request struct {
	Provider        string
	Target          Target
	Method          string
	URL             string
	Body            []byte
	Headers         http.Header
	Payload         any
	ContentType     string
	Query           url.Values
	QueryParseError string
	Form            url.Values
	FormParsed      bool
	FormParseError  string
	Received        time.Time
	UserAgent       string
}

type Delivery struct {
	ProviderEventID           string
	ProviderEventType         string
	EventName                 events.EventType
	Payload                   map[string]any
	Response                  *Response
	AcknowledgeBeforeDispatch bool
}

type Response struct {
	Status      int
	ContentType string
	Body        []byte
}

type Error struct {
	Status  int
	Message string
}

func (e Error) Error() string {
	return e.Message
}

type Registry struct {
	manifests map[string]Manifest
	sources   map[string]string
}

type ManifestSource struct {
	Manifest Manifest
	Source   string
}

type LoadedPack struct {
	Envelope     packs.Envelope
	Manifest     Manifest
	ManifestBody []byte
	Directory    string
	SourcePath   string
	Source       string
}

type PlatformPackIdentity struct {
	ID       string `yaml:"id"`
	Provider string `yaml:"provider"`
}

var requiredPlatformPackIdentities = []PlatformPackIdentity{
	{ID: "provider.github", Provider: "github"},
	{ID: "provider.intercom", Provider: "intercom"},
	{ID: "provider.shopify", Provider: "shopify"},
	{ID: "provider.slack", Provider: "slack"},
	{ID: "provider.stripe", Provider: "stripe"},
	{ID: "provider.telegram", Provider: "telegram"},
	{ID: "provider.twilio", Provider: "twilio"},
	{ID: "provider.typeform", Provider: "typeform"},
}

func RequiredPlatformPackIdentities() []PlatformPackIdentity {
	return append([]PlatformPackIdentity(nil), requiredPlatformPackIdentities...)
}

func NewRegistry(manifests ...Manifest) (*Registry, error) {
	sources := make([]ManifestSource, 0, len(manifests))
	for _, manifest := range manifests {
		sources = append(sources, ManifestSource{Manifest: manifest, Source: "direct"})
	}
	return NewRegistryFromSources(sources...)
}

func NewRegistryFromSources(sources ...ManifestSource) (*Registry, error) {
	r := &Registry{
		manifests: make(map[string]Manifest, len(sources)),
		sources:   make(map[string]string, len(sources)),
	}
	for _, source := range sources {
		manifest := source.Manifest
		if err := manifest.Validate(); err != nil {
			return nil, err
		}
		provider := NormalizeProviderName(manifest.Provider)
		if existingSource, exists := r.sources[provider]; exists {
			return nil, fmt.Errorf("duplicate provider trigger manifest for %q from %s and %s", provider, existingSource, firstNonEmpty(source.Source, "unknown"))
		}
		manifest.Provider = provider
		r.manifests[provider] = manifest
		r.sources[provider] = firstNonEmpty(source.Source, "unknown")
	}
	return r, nil
}

func LoadPlatformPackDirs(runningPlatformVersion string, dirs ...string) ([]LoadedPack, error) {
	return loadPackDirs(runningPlatformVersion, packs.ProvenancePlatform, dirs...)
}

func LoadExternalPackDirs(runningPlatformVersion string, dirs ...string) ([]LoadedPack, error) {
	return loadPackDirs(runningPlatformVersion, packs.ProvenanceExternal, dirs...)
}

func loadPackDirs(runningPlatformVersion, expectedProvenance string, dirs ...string) ([]LoadedPack, error) {
	loaded := make([]LoadedPack, 0, len(dirs))
	for _, dir := range dirs {
		dir = filepath.Clean(strings.TrimSpace(dir))
		if dir == "" || dir == "." {
			return nil, fmt.Errorf("%s provider trigger pack directory is required", expectedProvenance)
		}
		pack, err := LoadPackFS(os.DirFS(dir), ".", runningPlatformVersion)
		if err != nil {
			return nil, fmt.Errorf("load %s provider trigger pack %q: %w", expectedProvenance, dir, err)
		}
		if pack.Envelope.Provenance.Source != expectedProvenance {
			return nil, fmt.Errorf("%s provider trigger pack %q at %q declares provenance %q; expected %q", expectedProvenance, pack.Envelope.ID, dir, pack.Envelope.Provenance.Source, expectedProvenance)
		}
		pack.Directory = dir
		pack.SourcePath = dir
		pack.Source = loadedPackSource(pack)
		loaded = append(loaded, pack)
	}
	return loaded, nil
}

func NewRegistryFromPackDirs(runningPlatformVersion string, platformDirs, externalDirs []string) (*Registry, []LoadedPack, error) {
	return newRegistryFromPackDirs(runningPlatformVersion, platformDirs, externalDirs, false)
}

func NewRegistryFromRequiredPlatformPackDirs(runningPlatformVersion string, platformDirs, externalDirs []string) (*Registry, []LoadedPack, error) {
	return newRegistryFromPackDirs(runningPlatformVersion, platformDirs, externalDirs, true)
}

func newRegistryFromPackDirs(runningPlatformVersion string, platformDirs, externalDirs []string, requireCompletePlatformInventory bool) (*Registry, []LoadedPack, error) {
	if err := rejectDuplicatePackDirectories(platformDirs, externalDirs); err != nil {
		return nil, nil, err
	}
	platformPacks, err := LoadPlatformPackDirs(runningPlatformVersion, platformDirs...)
	if err != nil {
		return nil, nil, err
	}
	if requireCompletePlatformInventory {
		if err := validateLoadedPackIdentities(platformPacks); err != nil {
			return nil, nil, err
		}
		if err := validateRequiredPlatformPackInventory(platformPacks); err != nil {
			return nil, nil, err
		}
	}
	externalPacks, err := LoadExternalPackDirs(runningPlatformVersion, externalDirs...)
	if err != nil {
		return nil, nil, err
	}
	loaded := make([]LoadedPack, 0, len(platformPacks)+len(externalPacks))
	loaded = append(loaded, platformPacks...)
	loaded = append(loaded, externalPacks...)
	if err := validateLoadedPackIdentities(loaded); err != nil {
		return nil, nil, err
	}
	registry, err := NewRegistryFromSources(SourcesFromLoadedPacks(loaded...)...)
	if err != nil {
		return nil, nil, err
	}
	return registry, loaded, nil
}

func validateRequiredPlatformPackInventory(loaded []LoadedPack) error {
	expected := make(map[string]string, len(requiredPlatformPackIdentities))
	for _, identity := range requiredPlatformPackIdentities {
		expected[strings.TrimSpace(identity.ID)] = NormalizeProviderName(identity.Provider)
	}

	seen := make(map[string]struct{}, len(loaded))
	unexpected := make([]string, 0)
	mismatched := make([]string, 0)
	for _, pack := range loaded {
		id := strings.TrimSpace(pack.Envelope.ID)
		provider := NormalizeProviderName(pack.Manifest.Provider)
		expectedProvider, ok := expected[id]
		if !ok {
			unexpected = append(unexpected, fmt.Sprintf("%q provider=%q from %s", id, provider, loadedPackSource(pack)))
			continue
		}
		seen[id] = struct{}{}
		if provider != expectedProvider {
			mismatched = append(mismatched, fmt.Sprintf("%q declares provider=%q, expected provider=%q from %s", id, provider, expectedProvider, loadedPackSource(pack)))
		}
	}

	missing := make([]string, 0)
	for _, identity := range requiredPlatformPackIdentities {
		if _, ok := seen[identity.ID]; !ok {
			missing = append(missing, fmt.Sprintf("%q provider=%q", identity.ID, identity.Provider))
		}
	}
	if len(missing) == 0 && len(unexpected) == 0 && len(mismatched) == 0 {
		return nil
	}
	sort.Strings(missing)
	sort.Strings(unexpected)
	sort.Strings(mismatched)
	parts := make([]string, 0, 3)
	if len(missing) > 0 {
		parts = append(parts, "missing identities "+strings.Join(missing, ", "))
	}
	if len(unexpected) > 0 {
		parts = append(parts, "unexpected identities "+strings.Join(unexpected, ", "))
	}
	if len(mismatched) > 0 {
		parts = append(parts, "identity/provider mismatches "+strings.Join(mismatched, ", "))
	}
	return fmt.Errorf("required provider trigger platform inventory mismatch: %s; configure elevated provider_triggers.packs.platform_dirs with exactly the required first-party identities", strings.Join(parts, "; "))
}

func rejectDuplicatePackDirectories(platformDirs, externalDirs []string) error {
	type source struct {
		provenance string
	}
	seen := map[string]source{}
	for _, group := range []struct {
		provenance string
		dirs       []string
	}{
		{provenance: packs.ProvenancePlatform, dirs: platformDirs},
		{provenance: packs.ProvenanceExternal, dirs: externalDirs},
	} {
		for _, dir := range group.dirs {
			cleaned := filepath.Clean(strings.TrimSpace(dir))
			canonical, err := filepath.Abs(cleaned)
			if err != nil {
				canonical = cleaned
			}
			if resolved, err := filepath.EvalSymlinks(canonical); err == nil {
				canonical = resolved
			}
			if previous, ok := seen[canonical]; ok {
				return fmt.Errorf("duplicate provider trigger pack directory %q declared as provenance %q and %q", cleaned, previous.provenance, group.provenance)
			}
			seen[canonical] = source{provenance: group.provenance}
		}
	}
	return nil
}

func validateLoadedPackIdentities(loaded []LoadedPack) error {
	seen := map[string]LoadedPack{}
	for _, pack := range loaded {
		id := strings.TrimSpace(pack.Envelope.ID)
		if previous, ok := seen[id]; ok {
			if strings.TrimSpace(previous.Envelope.Version) == strings.TrimSpace(pack.Envelope.Version) && strings.TrimSpace(previous.Envelope.ManifestHash) != strings.TrimSpace(pack.Envelope.ManifestHash) {
				return fmt.Errorf("competing immutable provider trigger pack identity (%q, %q) from %s and %s has manifest hashes %q and %q", id, pack.Envelope.Version, loadedPackSource(previous), loadedPackSource(pack), previous.Envelope.ManifestHash, pack.Envelope.ManifestHash)
			}
			return fmt.Errorf("duplicate provider trigger pack id %q from %s and %s", id, loadedPackSource(previous), loadedPackSource(pack))
		}
		seen[id] = pack
	}
	return nil
}

func loadedPackSource(pack LoadedPack) string {
	provenance := strings.TrimSpace(pack.Envelope.Provenance.Source)
	if provenance == "" {
		provenance = "unknown"
	}
	sourcePath := strings.TrimSpace(pack.SourcePath)
	if sourcePath == "" {
		sourcePath = strings.TrimSpace(pack.Directory)
	}
	if sourcePath == "" {
		sourcePath = "unknown"
	}
	return fmt.Sprintf("provenance=%s path=%q pack=%q", provenance, sourcePath, strings.TrimSpace(pack.Envelope.ID))
}

func SourcesFromLoadedPacks(loaded ...LoadedPack) []ManifestSource {
	sources := make([]ManifestSource, 0, len(loaded))
	for _, pack := range loaded {
		sources = append(sources, ManifestSource{
			Manifest: pack.Manifest,
			Source:   firstNonEmpty(pack.Source, loadedPackSource(pack)),
		})
	}
	return sources
}

func LoadPackFS(fsys fs.FS, dir, runningPlatformVersion string) (LoadedPack, error) {
	loaded, err := packs.Load(fsys, dir, runningPlatformVersion)
	if err != nil {
		return LoadedPack{}, err
	}
	manifest, err := parseManifestStrict(loaded.ManifestBody)
	if err != nil {
		return LoadedPack{}, fmt.Errorf("parse trigger manifest for pack %q: %w", loaded.Envelope.ID, err)
	}
	if err := manifest.Validate(); err != nil {
		return LoadedPack{}, fmt.Errorf("validate trigger manifest for pack %q: %w", loaded.Envelope.ID, err)
	}
	expectedCapabilities := DerivedCapabilities(manifest)
	if !packs.CapabilitiesEqual(loaded.Envelope.Capabilities, expectedCapabilities) {
		return LoadedPack{}, fmt.Errorf("pack %q capabilities do not match trigger manifest", loaded.Envelope.ID)
	}
	expectedRequires := DerivedRequires(manifest)
	if !packs.RequiresEqual(loaded.Envelope.Requires, expectedRequires) {
		return LoadedPack{}, fmt.Errorf("pack %q requires do not match trigger manifest", loaded.Envelope.ID)
	}
	return LoadedPack{
		Envelope:     loaded.Envelope,
		Manifest:     manifest,
		ManifestBody: loaded.ManifestBody,
		Directory:    loaded.Directory,
		SourcePath:   loaded.Directory,
		Source:       fmt.Sprintf("provenance=%s path=%q pack=%q", loaded.Envelope.Provenance.Source, loaded.Directory, loaded.Envelope.ID),
	}, nil
}

func DerivedCapabilities(manifest Manifest) packs.Capabilities {
	provider := NormalizeProviderName(manifest.Provider)
	eventName := strings.TrimSpace(manifest.EventName.Literal)
	if eventName == "" {
		eventName = strings.TrimSpace(manifest.EventName.Template)
	}
	verifySecret := ""
	if manifest.Secret.Required {
		verifySecret = "webhook_signing." + provider
	}
	return packs.Capabilities{
		Can: packs.CanCapabilities{
			ReceiveHTTPSRoute:    "/webhooks/{entity}/" + provider,
			VerifySecret:         verifySecret,
			EmitEvents:           []string{eventName},
			PersistDedupeMarkers: true,
		},
		Cannot: []string{
			cannotEmitUndeclaredEvents,
			cannotRunCodeBeforeAdmission,
			cannotTouchUnboundResources,
		},
	}
}

func DerivedRequires(manifest Manifest) packs.Requires {
	provider := NormalizeProviderName(manifest.Provider)
	if manifest.Secret.Required {
		return packs.Requires{Secrets: []string{"webhook_signing." + provider}}
	}
	return packs.Requires{}
}

func (p LoadedPack) CapabilitySubject() (packs.Subject, error) {
	capabilities := DerivedCapabilities(p.Manifest)
	subject := packs.Subject{
		ID:            strings.TrimSpace(p.Envelope.ID),
		Kind:          packs.SubjectProviderTrigger,
		Provider:      NormalizeProviderName(p.Manifest.Provider),
		Source:        "trigger_pack",
		Provenance:    strings.TrimSpace(p.Envelope.Provenance.Source),
		SourcePath:    strings.TrimSpace(p.SourcePath),
		Applicability: "installed",
	}
	if route := strings.TrimSpace(capabilities.Can.ReceiveHTTPSRoute); route != "" {
		subject.Capabilities = append(subject.Capabilities, packs.Capability{Code: packs.CapabilityReceiveHTTPSRoute, Target: route})
	}
	if secret := strings.TrimSpace(capabilities.Can.VerifySecret); secret != "" {
		subject.Capabilities = append(subject.Capabilities, packs.Capability{Code: packs.CapabilityVerifySecret, Target: secret})
	}
	for _, event := range capabilities.Can.EmitEvents {
		subject.Capabilities = append(subject.Capabilities, packs.Capability{Code: packs.CapabilityEmitEvent, Target: strings.TrimSpace(event)})
	}
	if capabilities.Can.PersistDedupeMarkers {
		subject.Capabilities = append(subject.Capabilities, packs.Capability{Code: packs.CapabilityPersistDedupeMarkers})
	}
	for _, code := range capabilities.Cannot {
		guarantee, err := packs.NewGuarantee(code)
		if err != nil {
			return packs.Subject{}, err
		}
		subject.Guarantees = append(subject.Guarantees, guarantee)
	}
	for _, secret := range DerivedRequires(p.Manifest).Secrets {
		subject.Requirements = append(subject.Requirements, packs.TargetScopedRequirement(packs.RequirementSecret, secret))
	}
	normalized, err := packs.NormalizeSubjects([]packs.Subject{subject})
	if err != nil {
		return packs.Subject{}, err
	}
	return normalized[0], nil
}

func (r *Registry) Accept(req Request) (Delivery, error) {
	provider := NormalizeProviderName(req.Provider)
	if provider == "" {
		return Delivery{}, badRequest("provider is required")
	}
	if r != nil {
		if manifest, ok := r.manifests[provider]; ok {
			return manifest.Accept(req.withProvider(provider))
		}
	}
	return acceptRaw(req.withProvider(provider))
}

func (r *Registry) Manifest(provider string) (Manifest, bool) {
	if r == nil {
		return Manifest{}, false
	}
	manifest, ok := r.manifests[NormalizeProviderName(provider)]
	return manifest, ok
}

func (r *Registry) Source(provider string) string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.sources[NormalizeProviderName(provider)])
}

func (req Request) withProvider(provider string) Request {
	req.Provider = provider
	return req
}

type Manifest struct {
	Provider              string             `yaml:"provider"`
	PayloadObjectRequired bool               `yaml:"payload_object_required"`
	PayloadObjectError    string             `yaml:"payload_object_error"`
	PayloadSource         string             `yaml:"payload_source"`
	Secret                SecretManifest     `yaml:"secret"`
	Signature             SignatureManifest  `yaml:"signature"`
	Challenge             *ChallengeManifest `yaml:"challenge"`
	DeliveryCondition     *ConditionManifest `yaml:"delivery_condition"`
	DeliveryID            ValueSource        `yaml:"delivery_id"`
	EventType             ValueSource        `yaml:"event_type"`
	EventName             EventNameManifest  `yaml:"event_name"`
	Ack                   AckManifest        `yaml:"ack"`
	RedactKeys            []string           `yaml:"redact_keys"`
	Metadata              map[string]string  `yaml:"metadata"`
}

type SecretManifest struct {
	Required bool `yaml:"required"`
}

type SignatureManifest struct {
	Type           string             `yaml:"type"`
	Encoding       string             `yaml:"encoding"`
	Header         string             `yaml:"header"`
	Prefix         string             `yaml:"prefix"`
	SignedPayload  string             `yaml:"signed_payload"`
	SignatureParam string             `yaml:"signature_param"`
	MissingError   string             `yaml:"missing_error"`
	InvalidError   string             `yaml:"invalid_error"`
	Timestamp      *TimestampManifest `yaml:"timestamp"`
}

type TimestampManifest struct {
	Header       string `yaml:"header"`
	Param        string `yaml:"param"`
	Tolerance    string `yaml:"tolerance"`
	MissingError string `yaml:"missing_error"`
	InvalidError string `yaml:"invalid_error"`
	StaleError   string `yaml:"stale_error"`
}

type ChallengeManifest struct {
	When     ConditionManifest `yaml:"when"`
	Response ResponseManifest  `yaml:"response"`
}

type ResponseManifest struct {
	JSONPath    string `yaml:"json_path"`
	MissingErr  string `yaml:"missing_error"`
	ContentType string `yaml:"content_type"`
	Status      int    `yaml:"status"`
}

type ConditionManifest struct {
	JSONPath     string `yaml:"json_path"`
	Equals       string `yaml:"equals"`
	Normalize    bool   `yaml:"normalize"`
	MissingError string `yaml:"missing_error"`
	MismatchErr  string `yaml:"mismatch_error"`
}

type ValueSource struct {
	Header       string `yaml:"header"`
	JSONPath     string `yaml:"json_path"`
	FormParam    string `yaml:"form_param"`
	QueryParam   string `yaml:"query_param"`
	Literal      string `yaml:"literal"`
	Required     bool   `yaml:"required"`
	MissingError string `yaml:"missing_error"`
}

type EventNameManifest struct {
	Literal  string `yaml:"literal"`
	Template string `yaml:"template"`
}

type AckManifest struct {
	Mode string `yaml:"mode"`
}

func ParseManifest(body []byte) (Manifest, error) {
	var manifest Manifest
	if err := yaml.Unmarshal(body, &manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func parseManifestStrict(body []byte) (Manifest, error) {
	var manifest Manifest
	decoder := yaml.NewDecoder(bytes.NewReader(body))
	decoder.KnownFields(true)
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func (m Manifest) Validate() error {
	provider := NormalizeProviderName(m.Provider)
	if provider == "" {
		return fmt.Errorf("provider is required")
	}
	signatureType := strings.TrimSpace(m.Signature.Type)
	if m.Secret.Required && signatureType == "" {
		return fmt.Errorf("%s manifest requires signature when secret is required", provider)
	}
	switch strings.TrimSpace(m.PayloadSource) {
	case "", "payload", "form":
	default:
		return fmt.Errorf("%s manifest has unsupported payload_source %q", provider, m.PayloadSource)
	}
	if signatureType != "" {
		switch signatureType {
		case signatureTypeHMACSHA256, signatureTypeHMACSHA1, signatureTypeTokenEquality:
		default:
			return fmt.Errorf("%s manifest has unsupported signature type %q", provider, m.Signature.Type)
		}
		if strings.TrimSpace(m.Signature.Header) == "" {
			return fmt.Errorf("%s manifest signature header is required", provider)
		}
		switch signatureType {
		case signatureTypeHMACSHA256, signatureTypeHMACSHA1:
			switch m.Signature.digestEncoding() {
			case "hex", "base64":
			default:
				return fmt.Errorf("%s manifest has unsupported signature encoding %q", provider, m.Signature.Encoding)
			}
			switch strings.TrimSpace(m.Signature.SignedPayload) {
			case "raw_body", "slack_v0", "timestamp_dot_raw_body", "url_plus_sorted_form":
			default:
				return fmt.Errorf("%s manifest has unsupported signed_payload %q", provider, m.Signature.SignedPayload)
			}
			switch strings.TrimSpace(m.Signature.SignedPayload) {
			case "slack_v0", "timestamp_dot_raw_body":
				if m.Signature.Timestamp == nil {
					return fmt.Errorf("%s manifest timestamp is required for %s", provider, m.Signature.SignedPayload)
				}
			}
			if strings.TrimSpace(m.Signature.SignedPayload) == "url_plus_sorted_form" && m.Signature.digestEncoding() != "base64" {
				return fmt.Errorf("%s manifest url_plus_sorted_form signatures require base64 encoding", provider)
			}
			if strings.TrimSpace(m.Signature.SignedPayload) == "url_plus_sorted_form" && signatureType != signatureTypeHMACSHA1 {
				return fmt.Errorf("%s manifest url_plus_sorted_form signatures require hmac_sha1", provider)
			}
		case signatureTypeTokenEquality:
			if !m.Secret.Required {
				return fmt.Errorf("%s manifest token_equality requires secret.required", provider)
			}
			if strings.TrimSpace(m.Signature.Encoding) != "" {
				return fmt.Errorf("%s manifest token_equality must not set encoding", provider)
			}
			if strings.TrimSpace(m.Signature.Prefix) != "" {
				return fmt.Errorf("%s manifest token_equality must not set prefix", provider)
			}
			if strings.TrimSpace(m.Signature.SignedPayload) != "" {
				return fmt.Errorf("%s manifest token_equality must not set signed_payload", provider)
			}
			if strings.TrimSpace(m.Signature.SignatureParam) != "" {
				return fmt.Errorf("%s manifest token_equality must not set signature_param", provider)
			}
			if m.Signature.Timestamp != nil {
				return fmt.Errorf("%s manifest token_equality must not set timestamp", provider)
			}
		}
	}
	if err := validateValueSource(provider, "delivery_id", m.DeliveryID); err != nil {
		return err
	}
	if m.DeliveryID.Required && m.DeliveryID.sourceCount() == 0 {
		return fmt.Errorf("%s manifest delivery_id source is required", provider)
	}
	if err := validateValueSource(provider, "event_type", m.EventType); err != nil {
		return err
	}
	if m.EventType.Required && m.EventType.sourceCount() == 0 {
		return fmt.Errorf("%s manifest event_type source is required", provider)
	}
	if m.Challenge != nil {
		if err := validateJSONPath(provider, "challenge.when.json_path", m.Challenge.When.JSONPath); err != nil {
			return err
		}
		if err := validateJSONPath(provider, "challenge.response.json_path", m.Challenge.Response.JSONPath); err != nil {
			return err
		}
	}
	if m.DeliveryCondition != nil {
		if err := validateJSONPath(provider, "delivery_condition.json_path", m.DeliveryCondition.JSONPath); err != nil {
			return err
		}
	}
	eventNameLiteral := strings.TrimSpace(m.EventName.Literal)
	eventNameTemplate := strings.TrimSpace(m.EventName.Template)
	if eventNameLiteral == "" && eventNameTemplate == "" {
		return fmt.Errorf("%s manifest event_name is required", provider)
	}
	if eventNameLiteral != "" && eventNameTemplate != "" {
		return fmt.Errorf("%s manifest event_name must use literal or template, not both", provider)
	}
	if eventNameTemplate != "" && provider != "github" && provider != "slack" {
		return fmt.Errorf("%s manifest event_name template is reserved for grandfathered GitHub/Slack compatibility", provider)
	}
	switch strings.TrimSpace(m.Ack.Mode) {
	case "", "after_publish", "durable_before_dispatch":
	default:
		return fmt.Errorf("%s manifest has unsupported ack mode %q", provider, m.Ack.Mode)
	}
	for key, source := range m.Metadata {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("%s manifest metadata key is required", provider)
		}
		switch strings.TrimSpace(source) {
		case "user_agent", "delivery_id", "event_type":
		default:
			return fmt.Errorf("%s manifest metadata %q has unsupported source %q", provider, key, source)
		}
	}
	return nil
}

func (m Manifest) Accept(req Request) (Delivery, error) {
	provider := NormalizeProviderName(m.Provider)
	secret := strings.TrimSpace(req.Target.WebhookSecret)
	if m.Secret.Required && secret == "" {
		return Delivery{}, unauthorized(provider + " webhook signing secret is required")
	}
	if m.Signature.Type != "" {
		if err := m.verifySignature(secret, req); err != nil {
			return Delivery{}, err
		}
	}
	if m.PayloadObjectRequired {
		if _, ok := req.Payload.(map[string]any); !ok {
			return Delivery{}, badRequest(firstNonEmpty(m.PayloadObjectError, provider+" payload object is required"))
		}
	}
	if m.Challenge != nil {
		matched, err := m.Challenge.When.Evaluate(req.Payload)
		if err != nil {
			return Delivery{}, err
		}
		if matched {
			value, ok := stringFromJSONPath(req.Payload, m.Challenge.Response.JSONPath)
			if !ok || strings.TrimSpace(value) == "" {
				return Delivery{}, badRequest(firstNonEmpty(m.Challenge.Response.MissingErr, provider+" challenge is required"))
			}
			status := m.Challenge.Response.Status
			if status == 0 {
				status = http.StatusOK
			}
			contentType := firstNonEmpty(m.Challenge.Response.ContentType, "text/plain; charset=utf-8")
			return Delivery{Response: &Response{Status: status, ContentType: contentType, Body: []byte(value)}}, nil
		}
	}
	if m.DeliveryCondition != nil {
		matched, err := m.DeliveryCondition.Evaluate(req.Payload)
		if err != nil {
			return Delivery{}, err
		}
		if !matched {
			return Delivery{}, badRequest(firstNonEmpty(m.DeliveryCondition.MismatchErr, "unsupported "+provider+" payload type"))
		}
	}
	deliveryID, ok := m.DeliveryID.Resolve(req)
	if !ok || strings.TrimSpace(deliveryID) == "" {
		return Delivery{}, badRequest(firstNonEmpty(m.DeliveryID.MissingError, provider+" delivery id is required"))
	}
	rawEventType, ok := m.EventType.Resolve(req)
	if !ok || strings.TrimSpace(rawEventType) == "" {
		return Delivery{}, badRequest(firstNonEmpty(m.EventType.MissingError, provider+" event type is required"))
	}
	eventType := NormalizeEventToken(rawEventType)
	entityID := req.Target.EffectiveEntityID()
	payload := m.buildPublishPayload(provider, entityID, deliveryID, eventType, req)
	return Delivery{
		ProviderEventID:           deliveryID,
		ProviderEventType:         eventType,
		EventName:                 events.EventType(m.resolveEventName(eventType)),
		Payload:                   payload,
		AcknowledgeBeforeDispatch: strings.TrimSpace(m.Ack.Mode) == "durable_before_dispatch",
	}, nil
}

func (m Manifest) verifySignature(secret string, req Request) error {
	if strings.TrimSpace(m.Signature.Type) == signatureTypeTokenEquality {
		return m.verifyTokenEquality(secret, req)
	}
	sigHeader := strings.TrimSpace(req.Headers.Get(m.Signature.Header))
	if sigHeader == "" {
		return unauthorized(firstNonEmpty(m.Signature.MissingError, "signature is required"))
	}
	var (
		timestamp  string
		candidates []string
	)
	if strings.TrimSpace(m.Signature.SignatureParam) != "" {
		params, err := parseHeaderParams(sigHeader)
		if err != nil {
			return unauthorized(firstNonEmpty(m.Signature.InvalidError, "invalid signature"))
		}
		timestampValues := params.Values(firstNonEmpty(m.Signature.TimestampParam(), "t"))
		if len(timestampValues) > 1 {
			return unauthorized(firstNonEmpty(m.Signature.InvalidError, "invalid signature"))
		}
		if len(timestampValues) > 0 {
			timestamp = timestampValues[0]
		}
		candidates = params.Values(strings.TrimSpace(m.Signature.SignatureParam))
	} else {
		if m.Signature.Prefix != "" {
			lower := strings.ToLower(sigHeader)
			prefix := strings.ToLower(m.Signature.Prefix)
			if !strings.HasPrefix(lower, prefix) {
				return unauthorized(firstNonEmpty(m.Signature.MissingError, "signature is required"))
			}
			candidates = []string{strings.TrimSpace(sigHeader[len(m.Signature.Prefix):])}
		} else {
			candidates = []string{sigHeader}
		}
	}
	if m.Signature.Timestamp != nil {
		var err error
		timestamp, err = m.Signature.Timestamp.Resolve(timestamp, req)
		if err != nil {
			return err
		}
	}
	if len(candidates) == 0 {
		return unauthorized(firstNonEmpty(m.Signature.MissingError, "signature is required"))
	}
	signedPayload, err := m.Signature.signedPayload(timestamp, req)
	if err != nil {
		return err
	}
	hashFunc, err := m.Signature.hashFunc()
	if err != nil {
		return err
	}
	mac := hmac.New(hashFunc, []byte(strings.TrimSpace(secret)))
	_, _ = mac.Write(signedPayload)
	expected := m.Signature.encodeDigest(mac.Sum(nil))
	for _, candidate := range candidates {
		if m.Signature.signatureEqual(candidate, expected) {
			return nil
		}
	}
	return unauthorized(firstNonEmpty(m.Signature.InvalidError, "invalid signature"))
}

func (m Manifest) verifyTokenEquality(secret string, req Request) error {
	values := req.Headers.Values(m.Signature.Header)
	if len(values) == 0 || strings.TrimSpace(values[0]) == "" {
		return unauthorized(firstNonEmpty(m.Signature.MissingError, "signature is required"))
	}
	if len(values) != 1 {
		return unauthorized(firstNonEmpty(m.Signature.InvalidError, "invalid signature"))
	}
	token := strings.TrimSpace(values[0])
	if hmac.Equal([]byte(token), []byte(strings.TrimSpace(secret))) {
		return nil
	}
	return unauthorized(firstNonEmpty(m.Signature.InvalidError, "invalid signature"))
}

func (s SignatureManifest) TimestampParam() string {
	if s.Timestamp == nil {
		return ""
	}
	return strings.TrimSpace(s.Timestamp.Param)
}

func (s SignatureManifest) hashFunc() (func() hash.Hash, error) {
	switch strings.TrimSpace(s.Type) {
	case signatureTypeHMACSHA256:
		return sha256.New, nil
	case signatureTypeHMACSHA1:
		return sha1.New, nil
	default:
		return nil, unauthorized(firstNonEmpty(s.InvalidError, "invalid signature"))
	}
}

func (s SignatureManifest) digestEncoding() string {
	encoding := strings.TrimSpace(s.Encoding)
	if encoding == "" {
		return "hex"
	}
	return encoding
}

func (s SignatureManifest) encodeDigest(sum []byte) string {
	switch s.digestEncoding() {
	case "base64":
		return base64.StdEncoding.EncodeToString(sum)
	default:
		return hex.EncodeToString(sum)
	}
}

func (s SignatureManifest) signatureEqual(candidate, expected string) bool {
	candidate = strings.TrimSpace(candidate)
	if s.digestEncoding() == "hex" {
		candidate = strings.ToLower(candidate)
		expected = strings.ToLower(expected)
	}
	return hmac.Equal([]byte(candidate), []byte(expected))
}

func (s SignatureManifest) signedPayload(timestamp string, req Request) ([]byte, error) {
	switch strings.TrimSpace(s.SignedPayload) {
	case "raw_body":
		return req.Body, nil
	case "slack_v0":
		return []byte("v0:" + timestamp + ":" + string(req.Body)), nil
	case "timestamp_dot_raw_body":
		return []byte(timestamp + "." + string(req.Body)), nil
	case "url_plus_sorted_form":
		return signedPayloadURLPlusSortedForm(s, req)
	default:
		return nil, unauthorized(firstNonEmpty(s.InvalidError, "invalid signature"))
	}
}

func (t TimestampManifest) Resolve(paramTimestamp string, req Request) (string, error) {
	raw := strings.TrimSpace(paramTimestamp)
	if strings.TrimSpace(t.Header) != "" {
		raw = strings.TrimSpace(req.Headers.Get(t.Header))
	}
	if raw == "" {
		return "", unauthorized(firstNonEmpty(t.MissingError, "signature timestamp is required"))
	}
	secs, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return "", unauthorized(firstNonEmpty(t.InvalidError, "invalid signature timestamp"))
	}
	if strings.TrimSpace(t.Tolerance) != "" {
		tolerance, err := time.ParseDuration(strings.TrimSpace(t.Tolerance))
		if err != nil {
			return "", unauthorized(firstNonEmpty(t.InvalidError, "invalid signature timestamp"))
		}
		requestTime := time.Unix(secs, 0).UTC()
		now := req.Received.UTC()
		if requestTime.After(now.Add(tolerance)) || requestTime.Before(now.Add(-tolerance)) {
			return "", unauthorized(firstNonEmpty(t.StaleError, "stale signature timestamp"))
		}
	}
	return raw, nil
}

func (c ConditionManifest) Evaluate(payload any) (bool, error) {
	value, ok := stringFromJSONPath(payload, c.JSONPath)
	if !ok || strings.TrimSpace(value) == "" {
		if c.MissingError != "" {
			return false, badRequest(c.MissingError)
		}
		return false, nil
	}
	if c.Normalize {
		value = NormalizeEventToken(value)
	}
	expected := c.Equals
	if c.Normalize {
		expected = NormalizeEventToken(expected)
	}
	return value == expected, nil
}

func (s ValueSource) Resolve(req Request) (string, bool) {
	if strings.TrimSpace(s.Literal) != "" {
		return strings.TrimSpace(s.Literal), true
	}
	if strings.TrimSpace(s.Header) != "" {
		value := strings.TrimSpace(req.Headers.Get(s.Header))
		return value, value != ""
	}
	if strings.TrimSpace(s.JSONPath) != "" {
		return stringFromJSONPath(req.Payload, s.JSONPath)
	}
	if strings.TrimSpace(s.FormParam) != "" {
		return singleParamValue(req.Form, s.FormParam)
	}
	if strings.TrimSpace(s.QueryParam) != "" {
		return singleParamValue(req.Query, s.QueryParam)
	}
	return "", false
}

func (m Manifest) resolveEventName(eventType string) string {
	if name := strings.TrimSpace(m.EventName.Literal); name != "" {
		return name
	}
	name := strings.TrimSpace(m.EventName.Template)
	name = strings.ReplaceAll(name, "{event_type}", eventType)
	return name
}

func (m Manifest) buildPublishPayload(provider, entityID, deliveryID, eventType string, req Request) map[string]any {
	rawPayload := redactPayload(req.Payload, m.RedactKeys)
	if strings.TrimSpace(m.PayloadSource) == "form" {
		rawPayload = redactPayload(formValuesPayload(req.Form), m.RedactKeys)
	}
	headers := make(map[string]any, len(m.Metadata))
	for key, source := range m.Metadata {
		switch source {
		case "user_agent":
			headers[key] = req.UserAgent
		case "delivery_id":
			headers[key] = deliveryID
		case "event_type":
			headers[key] = eventType
		}
	}
	return map[string]any{
		"entity_id":            strings.TrimSpace(entityID),
		"provider":             strings.TrimSpace(provider),
		"event_type":           strings.TrimSpace(eventType),
		"provider_event_type":  strings.TrimSpace(eventType),
		"provider_event_id":    strings.TrimSpace(deliveryID),
		"provider_delivery_id": strings.TrimSpace(deliveryID),
		"payload":              rawPayload,
		"headers":              headers,
		"received_at":          req.Received.UTC().Format(time.RFC3339),
	}
}

func signedPayloadURLPlusSortedForm(s SignatureManifest, req Request) ([]byte, error) {
	if strings.TrimSpace(req.URL) == "" {
		return nil, unauthorized(firstNonEmpty(s.InvalidError, "invalid signature"))
	}
	if strings.TrimSpace(req.QueryParseError) != "" || strings.TrimSpace(req.FormParseError) != "" {
		return nil, unauthorized(firstNonEmpty(s.InvalidError, "invalid signature"))
	}
	if !req.FormParsed {
		return nil, unauthorized(firstNonEmpty(s.InvalidError, "invalid signature"))
	}
	if hasDuplicateValues(req.Query) || hasDuplicateValues(req.Form) {
		return nil, unauthorized(firstNonEmpty(s.InvalidError, "invalid signature"))
	}
	keys := make([]string, 0, len(req.Form))
	for key := range req.Form {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(req.URL)
	for _, key := range keys {
		b.WriteString(key)
		b.WriteString(req.Form.Get(key))
	}
	return []byte(b.String()), nil
}

func hasDuplicateValues(values url.Values) bool {
	for _, items := range values {
		if len(items) > 1 {
			return true
		}
	}
	return false
}

func singleParamValue(values url.Values, key string) (string, bool) {
	items := values[strings.TrimSpace(key)]
	if len(items) != 1 {
		return "", false
	}
	value := strings.TrimSpace(items[0])
	return value, value != ""
}

func formValuesPayload(values url.Values) map[string]any {
	payload := make(map[string]any, len(values))
	for key, items := range values {
		if len(items) == 1 {
			payload[key] = items[0]
			continue
		}
		copied := make([]any, 0, len(items))
		for _, item := range items {
			copied = append(copied, item)
		}
		payload[key] = copied
	}
	return payload
}

func acceptRaw(req Request) (Delivery, error) {
	provider := NormalizeProviderName(req.Provider)
	if provider == "" {
		return Delivery{}, badRequest("provider is required")
	}
	if !verifyRawWebhookSignature(req.Target.WebhookSecret, req.Body, req.Headers) {
		return Delivery{}, unauthorized("invalid signature")
	}
	entityID := req.Target.EffectiveEntityID()
	deliveryID := firstNonEmpty(
		req.Headers.Get("X-Provider-Event-ID"),
		req.Headers.Get("X-Request-ID"),
		extractProviderEventID(req.Payload),
		fingerprintInbound(entityID, provider, req.Body),
	)
	eventType := resolveProviderEventType(req.Payload)
	payload := map[string]any{
		"entity_id":            strings.TrimSpace(entityID),
		"provider":             provider,
		"event_type":           eventType,
		"provider_event_type":  eventType,
		"provider_event_id":    deliveryID,
		"provider_delivery_id": deliveryID,
		"payload":              req.Payload,
		"headers":              map[string]any{"user_agent": req.UserAgent},
		"received_at":          req.Received.UTC().Format(time.RFC3339),
	}
	return Delivery{
		ProviderEventID:   deliveryID,
		ProviderEventType: eventType,
		EventName:         events.EventType("inbound." + provider),
		Payload:           payload,
	}, nil
}

func verifyRawWebhookSignature(secret string, body []byte, headers http.Header) bool {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return true
	}
	if sig := strings.TrimSpace(headers.Get("X-Hub-Signature-256")); strings.HasPrefix(strings.ToLower(sig), "sha256=") {
		given := strings.TrimSpace(sig[len("sha256="):])
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(body)
		expected := hex.EncodeToString(mac.Sum(nil))
		return hmac.Equal([]byte(strings.ToLower(given)), []byte(strings.ToLower(expected)))
	}
	token := strings.TrimSpace(headers.Get("X-Webhook-Token"))
	if token == "" {
		auth := strings.TrimSpace(headers.Get("Authorization"))
		if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			token = strings.TrimSpace(auth[7:])
		}
	}
	return hmac.Equal([]byte(token), []byte(secret))
}

func extractProviderEventID(payload any) string {
	m, ok := payload.(map[string]any)
	if !ok {
		return ""
	}
	for _, key := range []string{"id", "event_id", "message_id"} {
		if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func fingerprintInbound(entityID, provider string, body []byte) string {
	h := sha1.New()
	_, _ = h.Write([]byte(entityID))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(provider))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write(body)
	return "fp:" + hex.EncodeToString(h.Sum(nil))
}

func resolveProviderEventType(payload any) string {
	m, _ := payload.(map[string]any)
	for _, key := range []string{"event_type", "type", "status", "kind", "action"} {
		if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
			return NormalizeEventToken(v)
		}
	}
	return "event"
}

func stringFromJSONPath(payload any, path string) (string, bool) {
	value, ok := valueFromJSONPath(payload, path)
	if !ok {
		return "", false
	}
	switch t := value.(type) {
	case string:
		return strings.TrimSpace(t), strings.TrimSpace(t) != ""
	case json.Number:
		return strings.TrimSpace(t.String()), strings.TrimSpace(t.String()) != ""
	case float64:
		return strings.TrimSpace(strconv.FormatFloat(t, 'f', -1, 64)), true
	default:
		return "", false
	}
}

func valueFromJSONPath(payload any, path string) (any, bool) {
	path = strings.TrimSpace(path)
	if path == "$" {
		return payload, true
	}
	if !strings.HasPrefix(path, "$.") {
		return nil, false
	}
	current := payload
	for _, part := range strings.Split(strings.TrimPrefix(path, "$."), ".") {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = obj[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func validateJSONPath(provider, field, path string) error {
	path = strings.TrimSpace(path)
	if path == "" || path == "$" {
		return nil
	}
	if !strings.HasPrefix(path, "$.") {
		return fmt.Errorf("%s manifest %s has unsupported json_path %q", provider, field, path)
	}
	parts := strings.Split(strings.TrimPrefix(path, "$."), ".")
	for _, part := range parts {
		if strings.TrimSpace(part) == "" || !validJSONPathSegment(part) {
			return fmt.Errorf("%s manifest %s has unsupported json_path %q", provider, field, path)
		}
	}
	return nil
}

func validateValueSource(provider, field string, source ValueSource) error {
	if source.sourceCount() > 1 {
		return fmt.Errorf("%s manifest %s must use exactly one source", provider, field)
	}
	return validateJSONPath(provider, field+".json_path", source.JSONPath)
}

func (s ValueSource) sourceCount() int {
	count := 0
	for _, value := range []string{s.Header, s.JSONPath, s.FormParam, s.QueryParam, s.Literal} {
		if strings.TrimSpace(value) != "" {
			count++
		}
	}
	return count
}

func validJSONPathSegment(part string) bool {
	for _, r := range part {
		if r >= 'a' && r <= 'z' {
			continue
		}
		if r >= 'A' && r <= 'Z' {
			continue
		}
		if r >= '0' && r <= '9' {
			continue
		}
		if r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
}

func redactPayload(payload any, keys []string) any {
	if len(keys) == 0 {
		return payload
	}
	keySet := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		keySet[strings.ToLower(strings.TrimSpace(key))] = struct{}{}
	}
	return redactValue(payload, keySet)
}

func redactValue(payload any, keys map[string]struct{}) any {
	switch v := payload.(type) {
	case map[string]any:
		redacted := make(map[string]any, len(v))
		for key, value := range v {
			if _, ok := keys[strings.ToLower(key)]; ok {
				redacted[key] = "[redacted]"
				continue
			}
			redacted[key] = redactValue(value, keys)
		}
		return redacted
	case []any:
		redacted := make([]any, len(v))
		for i, value := range v {
			redacted[i] = redactValue(value, keys)
		}
		return redacted
	default:
		return v
	}
}

type headerParams map[string][]string

func parseHeaderParams(header string) (headerParams, error) {
	out := make(headerParams)
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("empty signature parameter")
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("malformed signature parameter")
		}
		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])
		if key == "" || value == "" {
			return nil, fmt.Errorf("empty signature parameter")
		}
		out[key] = append(out[key], value)
	}
	return out, nil
}

func (p headerParams) Values(key string) []string {
	values := p[strings.TrimSpace(key)]
	out := make([]string, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			out = append(out, strings.TrimSpace(value))
		}
	}
	return out
}

func NormalizeProviderName(raw string) string {
	token := strings.TrimSpace(strings.ToLower(raw))
	token = strings.ReplaceAll(token, ".", "_")
	token = strings.ReplaceAll(token, "-", "_")
	token = strings.ReplaceAll(token, " ", "_")
	return token
}

func NormalizeEventToken(raw string) string {
	token := strings.TrimSpace(strings.ToLower(raw))
	token = strings.ReplaceAll(token, ".", "_")
	token = strings.ReplaceAll(token, "-", "_")
	token = strings.ReplaceAll(token, "/", "_")
	token = strings.ReplaceAll(token, " ", "_")
	if token == "" {
		return "event"
	}
	return token
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func badRequest(message string) Error {
	return Error{Status: http.StatusBadRequest, Message: message}
}

func unauthorized(message string) Error {
	return Error{Status: http.StatusUnauthorized, Message: message}
}
