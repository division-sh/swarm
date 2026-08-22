package packmodel

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"

	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/runtime/core/manifesthash"
	"github.com/division-sh/swarm/internal/runtime/core/packidentity"
	"gopkg.in/yaml.v3"
)

const (
	EnvelopeFileName          = "pack.yaml"
	TriggerManifestFileName   = "trigger.yaml"
	ConnectorManifestFileName = "connector.yaml"
	ChannelManifestFileName   = "channel.yaml"

	TypeTrigger   = "trigger"
	TypeConnector = "connector"
	TypeChannel   = "channel"

	ProvenancePlatform = "platform"
	ProvenanceExternal = "external"
	ProvenanceProject  = "project"
)

type Envelope struct {
	ID              string       `yaml:"id"`
	Version         string       `yaml:"version"`
	PlatformVersion string       `yaml:"platform_version"`
	Type            string       `yaml:"type"`
	Implements      []string     `yaml:"implements,omitempty"`
	ManifestHash    string       `yaml:"manifest_hash"`
	Provenance      Provenance   `yaml:"provenance"`
	Capabilities    Capabilities `yaml:"capabilities"`
	Requires        Requires     `yaml:"requires"`
	Tests           []string     `yaml:"tests"`
}

type Provenance struct {
	Source string `yaml:"source"`
}

type Capabilities struct {
	Can    CanCapabilities `yaml:"can" json:"can"`
	Cannot []string        `yaml:"cannot" json:"cannot"`
}

type CanCapabilities struct {
	ReceiveHTTPSRoute       string   `yaml:"receive_https_route,omitempty" json:"receive_https_route,omitempty"`
	VerifySecret            string   `yaml:"verify_secret,omitempty" json:"verify_secret,omitempty"`
	EmitEvents              []string `yaml:"emit_events,omitempty" json:"emit_events,omitempty"`
	PersistDedupeMarkers    bool     `yaml:"persist_dedupe_markers,omitempty" json:"persist_dedupe_markers,omitempty"`
	CallProviderActions     []string `yaml:"call_provider_actions,omitempty" json:"call_provider_actions,omitempty"`
	LowerThroughActivity    bool     `yaml:"lower_through_activity,omitempty" json:"lower_through_activity,omitempty"`
	JournalActivityAttempts bool     `yaml:"journal_activity_attempts,omitempty" json:"journal_activity_attempts,omitempty"`
}

type Requires struct {
	Secrets            []string          `yaml:"secrets,omitempty" json:"secrets"`
	ManagedCredentials []string          `yaml:"managed_credentials,omitempty" json:"managed_credentials"`
	Packs              map[string]string `yaml:"packs,omitempty" json:"packs,omitempty"`
}

type Loaded struct {
	Envelope     Envelope
	ManifestBody []byte
	Directory    string
}

func Load(fsys fs.FS, dir, runningPlatformVersion string) (Loaded, error) {
	dir = cleanDir(dir)
	envelopeBody, err := fs.ReadFile(fsys, path.Join(dir, EnvelopeFileName))
	if err != nil {
		return Loaded{}, fmt.Errorf("read pack envelope %q: %w", path.Join(dir, EnvelopeFileName), err)
	}
	envelope, err := ParseEnvelope(envelopeBody)
	if err != nil {
		return Loaded{}, fmt.Errorf("parse pack envelope %q: %w", path.Join(dir, EnvelopeFileName), err)
	}
	if err := envelope.ValidateCommon(runningPlatformVersion); err != nil {
		return Loaded{}, err
	}
	manifestFile := ManifestFileNameForType(envelope.Type)
	if manifestFile == "" {
		return Loaded{}, fmt.Errorf("pack %q has unsupported type %q", envelope.ID, envelope.Type)
	}
	manifestBody, err := fs.ReadFile(fsys, path.Join(dir, manifestFile))
	if err != nil {
		return Loaded{}, fmt.Errorf("read pack manifest %q: %w", path.Join(dir, manifestFile), err)
	}
	if err := envelope.VerifyManifestHash(manifestBody); err != nil {
		return Loaded{}, err
	}
	return Loaded{Envelope: envelope, ManifestBody: manifestBody, Directory: dir}, nil
}

func ManifestFileNameForType(packType string) string {
	switch strings.TrimSpace(packType) {
	case TypeTrigger:
		return TriggerManifestFileName
	case TypeConnector:
		return ConnectorManifestFileName
	case TypeChannel:
		return ChannelManifestFileName
	default:
		return ""
	}
}

func ParseEnvelope(body []byte) (Envelope, error) {
	var envelope Envelope
	decoder := yaml.NewDecoder(bytes.NewReader(body))
	decoder.KnownFields(true)
	if err := decoder.Decode(&envelope); err != nil {
		return Envelope{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Envelope{}, fmt.Errorf("pack envelope contains multiple YAML documents")
		}
		return Envelope{}, fmt.Errorf("parse pack envelope trailing document: %w", err)
	}
	return envelope, nil
}

func (e Envelope) ValidateCommon(runningPlatformVersion string) error {
	id, err := packidentity.ParseID(e.ID)
	if err != nil {
		return err
	}
	if _, err := packidentity.ParseVersion(e.Version); err != nil {
		return fmt.Errorf("pack %q %w", id.String(), err)
	}
	if err := platform.ValidateProductPlatformVersion(e.PlatformVersion, runningPlatformVersion); err != nil {
		return fmt.Errorf("pack %q platform_version is incompatible: %w", id.String(), err)
	}
	switch e.Type {
	case TypeTrigger, TypeConnector, TypeChannel:
	default:
		return fmt.Errorf("pack %q has unsupported type %q", id.String(), e.Type)
	}
	if _, err := manifesthash.Parse(e.ManifestHash); err != nil {
		return fmt.Errorf("pack %q %w", id.String(), err)
	}
	switch e.Provenance.Source {
	case ProvenancePlatform, ProvenanceExternal, ProvenanceProject:
	default:
		return fmt.Errorf("pack %q has unsupported provenance source %q", id.String(), e.Provenance.Source)
	}
	if err := e.Capabilities.ValidateForType(id.String(), e.Type); err != nil {
		return err
	}
	if err := e.Requires.Validate(id.String()); err != nil {
		return err
	}
	if e.Type == TypeChannel {
		if len(e.Implements) != 1 || strings.TrimSpace(e.Implements[0]) == "" {
			return fmt.Errorf("pack %q type channel must declare exactly one implements identity", id.String())
		}
	} else if len(e.Implements) != 0 {
		return fmt.Errorf("pack %q type %s must not declare implements", id.String(), e.Type)
	}
	if len(e.Tests) == 0 {
		return fmt.Errorf("pack %q tests are required", id.String())
	}
	for _, test := range e.Tests {
		if strings.TrimSpace(test) == "" {
			return fmt.Errorf("pack %q tests must not contain empty entries", id.String())
		}
	}
	return nil
}

func (e Envelope) VerifyManifestHash(manifestBody []byte) error {
	want, err := manifesthash.Parse(e.ManifestHash)
	if err != nil {
		return fmt.Errorf("pack %q %w", e.ID, err)
	}
	got := manifesthash.FromBytes(manifestBody)
	if !got.Equal(want) {
		return fmt.Errorf("pack %q manifest_hash mismatch: got %s want %s", e.ID, got.String(), want.String())
	}
	return nil
}

// StampEnvelope derives the exact-byte body digest and emits the canonical
// checked-in envelope representation used by every pack body kind.
func StampEnvelope(envelope Envelope, manifestBody []byte) (Envelope, []byte, error) {
	envelope.ManifestHash = ManifestHash(manifestBody)
	body, err := yaml.Marshal(envelope)
	if err != nil {
		return Envelope{}, nil, fmt.Errorf("marshal pack %q envelope: %w", envelope.ID, err)
	}
	return envelope, body, nil
}

func ManifestHash(manifestBody []byte) string {
	return manifesthash.FromBytes(manifestBody).String()
}

func (c Capabilities) Validate(packID string) error {
	return c.ValidateForType(packID, TypeTrigger)
}

func (c Capabilities) ValidateForType(packID, packType string) error {
	switch packType {
	case TypeTrigger:
		return c.validateTrigger(packID)
	case TypeConnector:
		return c.validateConnector(packID)
	case TypeChannel:
		return c.validateChannel(packID)
	default:
		return fmt.Errorf("pack %q has unsupported type %q", packID, packType)
	}
}

func (c Capabilities) validateChannel(packID string) error {
	if strings.TrimSpace(c.Can.ReceiveHTTPSRoute) != "" || strings.TrimSpace(c.Can.VerifySecret) != "" || len(c.Can.EmitEvents) != 0 || c.Can.PersistDedupeMarkers || len(c.Can.CallProviderActions) != 0 || c.Can.LowerThroughActivity || c.Can.JournalActivityAttempts {
		return fmt.Errorf("pack %q channel capabilities are derived from its satisfied trigger and connector dependencies", packID)
	}
	if len(c.Cannot) == 0 {
		return fmt.Errorf("pack %q capabilities.cannot is required", packID)
	}
	for _, item := range c.Cannot {
		if strings.TrimSpace(item) == "" {
			return fmt.Errorf("pack %q capabilities.cannot must not contain empty entries", packID)
		}
	}
	return nil
}

func (c Capabilities) validateTrigger(packID string) error {
	if strings.TrimSpace(c.Can.ReceiveHTTPSRoute) == "" {
		return fmt.Errorf("pack %q capabilities.can.receive_https_route is required", packID)
	}
	if len(c.Can.CallProviderActions) > 0 || c.Can.LowerThroughActivity || c.Can.JournalActivityAttempts {
		return fmt.Errorf("pack %q trigger capabilities must not declare connector capability fields", packID)
	}
	if len(c.Can.EmitEvents) == 0 {
		return fmt.Errorf("pack %q capabilities.can.emit_events is required", packID)
	}
	for _, event := range c.Can.EmitEvents {
		if strings.TrimSpace(event) == "" {
			return fmt.Errorf("pack %q capabilities.can.emit_events must not contain empty entries", packID)
		}
	}
	if !c.Can.PersistDedupeMarkers {
		return fmt.Errorf("pack %q capabilities.can.persist_dedupe_markers must be true", packID)
	}
	if len(c.Cannot) == 0 {
		return fmt.Errorf("pack %q capabilities.cannot is required", packID)
	}
	for _, item := range c.Cannot {
		if strings.TrimSpace(item) == "" {
			return fmt.Errorf("pack %q capabilities.cannot must not contain empty entries", packID)
		}
	}
	return nil
}

func (c Capabilities) validateConnector(packID string) error {
	if len(c.Can.CallProviderActions) == 0 {
		return fmt.Errorf("pack %q capabilities.can.call_provider_actions is required", packID)
	}
	seenActions := map[string]struct{}{}
	for _, action := range c.Can.CallProviderActions {
		action = strings.TrimSpace(action)
		if action == "" {
			return fmt.Errorf("pack %q capabilities.can.call_provider_actions must not contain empty entries", packID)
		}
		if _, exists := seenActions[action]; exists {
			return fmt.Errorf("pack %q capabilities.can.call_provider_actions contains duplicate %q", packID, action)
		}
		seenActions[action] = struct{}{}
	}
	if strings.TrimSpace(c.Can.ReceiveHTTPSRoute) != "" || strings.TrimSpace(c.Can.VerifySecret) != "" || len(c.Can.EmitEvents) > 0 || c.Can.PersistDedupeMarkers {
		return fmt.Errorf("pack %q connector capabilities must not declare trigger capability fields", packID)
	}
	if !c.Can.LowerThroughActivity {
		return fmt.Errorf("pack %q capabilities.can.lower_through_activity must be true", packID)
	}
	if !c.Can.JournalActivityAttempts {
		return fmt.Errorf("pack %q capabilities.can.journal_activity_attempts must be true", packID)
	}
	if len(c.Cannot) == 0 {
		return fmt.Errorf("pack %q capabilities.cannot is required", packID)
	}
	for _, item := range c.Cannot {
		if strings.TrimSpace(item) == "" {
			return fmt.Errorf("pack %q capabilities.cannot must not contain empty entries", packID)
		}
	}
	return nil
}

func (r Requires) Validate(packID string) error {
	seen := map[string]struct{}{}
	for _, secret := range r.Secrets {
		secret = strings.TrimSpace(secret)
		if secret == "" {
			return fmt.Errorf("pack %q requires.secrets must not contain empty entries", packID)
		}
		if _, exists := seen[secret]; exists {
			return fmt.Errorf("pack %q requires.secrets contains duplicate %q", packID, secret)
		}
		seen[secret] = struct{}{}
	}
	managedSeen := map[string]struct{}{}
	for _, credential := range r.ManagedCredentials {
		credential = strings.TrimSpace(credential)
		if credential == "" {
			return fmt.Errorf("pack %q requires.managed_credentials must not contain empty entries", packID)
		}
		if _, exists := managedSeen[credential]; exists {
			return fmt.Errorf("pack %q requires.managed_credentials contains duplicate %q", packID, credential)
		}
		managedSeen[credential] = struct{}{}
	}
	for role, id := range r.Packs {
		role = strings.TrimSpace(role)
		id = strings.TrimSpace(id)
		if role != TypeTrigger && role != TypeConnector {
			return fmt.Errorf("pack %q requires.packs role %q is unsupported", packID, role)
		}
		if id == "" {
			return fmt.Errorf("pack %q requires.packs.%s is required", packID, role)
		}
	}
	return nil
}

func CapabilitiesEqual(a, b Capabilities) bool {
	return canonicalJSON(a) == canonicalJSON(b)
}

func RequiresEqual(a, b Requires) bool {
	return canonicalJSON(a) == canonicalJSON(b)
}

func canonicalJSON(v any) string {
	body, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(body)
}

func cleanDir(dir string) string {
	dir = path.Clean(strings.TrimSpace(dir))
	if dir == "." {
		return "."
	}
	return strings.TrimPrefix(dir, "./")
}
