package scenarioexecution

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

const (
	EffectiveSourceIdentityVersion = "effective-source-identity/v1"
	ExecutionProfileVersion        = "scenario-execution-profile/v1"
)

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// EffectiveSourceIdentity names the complete admitted source used to compile
// and execute one scenario. The base bundle fact alone is not this identity.
type EffectiveSourceIdentity struct {
	sourceFact runtimecorrelation.SourceArtifactFact
	digest     string
}

func NewEffectiveSourceIdentity(sourceFact runtimecorrelation.SourceArtifactFact, digest string) (EffectiveSourceIdentity, error) {
	if err := sourceFact.Validate(); err != nil {
		return EffectiveSourceIdentity{}, fmt.Errorf("effective source bundle fact: %w", err)
	}
	if digest != strings.TrimSpace(digest) || !digestPattern.MatchString(digest) {
		return EffectiveSourceIdentity{}, fmt.Errorf("effective_source_digest must use canonical sha256 encoding")
	}
	return EffectiveSourceIdentity{sourceFact: sourceFact, digest: digest}, nil
}

func (i EffectiveSourceIdentity) Validate() error {
	_, err := NewEffectiveSourceIdentity(i.sourceFact, i.digest)
	return err
}

func (i EffectiveSourceIdentity) SourceArtifactFact() runtimecorrelation.SourceArtifactFact {
	return i.sourceFact
}

func (i EffectiveSourceIdentity) Digest() string { return i.digest }

func (i EffectiveSourceIdentity) Equal(other EffectiveSourceIdentity) bool {
	return i.Validate() == nil && other.Validate() == nil && i.sourceFact.Matches(other.sourceFact) && i.digest == other.digest
}

func (i EffectiveSourceIdentity) CanonicalValue() (map[string]any, error) {
	if err := i.Validate(); err != nil {
		return nil, err
	}
	return map[string]any{
		"version":                 EffectiveSourceIdentityVersion,
		"bundle_hash":             i.sourceFact.BundleHash(),
		"effective_source_digest": i.digest,
	}, nil
}

type ConnectorResponse struct {
	ToolID             string
	OutputSchemaDigest string
	Response           json.RawMessage
}

type profileDocument struct {
	Version            string                    `json:"version"`
	Source             profileSourceDocument     `json:"source"`
	ProfileID          string                    `json:"profile_id"`
	ConnectorResponses []profileResponseDocument `json:"connector_responses"`
}

type profileSourceDocument struct {
	BundleHash            string `json:"bundle_hash"`
	EffectiveSourceDigest string `json:"effective_source_digest"`
}

type profileResponseDocument struct {
	ToolID             string          `json:"tool_id"`
	OutputSchemaDigest string          `json:"output_schema_digest"`
	Response           json.RawMessage `json:"response"`
}

// Profile is one immutable canonical execution artifact. raw is persisted
// verbatim and is verified rather than reconstructed by storage readers.
type Profile struct {
	identity  EffectiveSourceIdentity
	id        string
	digest    string
	raw       json.RawMessage
	responses map[string]ConnectorResponse
}

func NewProfile(identity EffectiveSourceIdentity, profileID string, responses []ConnectorResponse) (Profile, error) {
	if err := identity.Validate(); err != nil {
		return Profile{}, err
	}
	if profileID != strings.TrimSpace(profileID) || profileID == "" {
		return Profile{}, fmt.Errorf("scenario profile_id must be non-empty canonical text")
	}
	doc := profileDocument{
		Version: ExecutionProfileVersion,
		Source: profileSourceDocument{
			BundleHash: identity.SourceArtifactFact().BundleHash(), EffectiveSourceDigest: identity.Digest(),
		},
		ProfileID:          profileID,
		ConnectorResponses: make([]profileResponseDocument, 0, len(responses)),
	}
	seen := make(map[string]struct{}, len(responses))
	responseMap := make(map[string]ConnectorResponse, len(responses))
	for _, response := range responses {
		if response.ToolID != strings.TrimSpace(response.ToolID) || response.ToolID == "" {
			return Profile{}, fmt.Errorf("scenario connector response tool_id must be non-empty canonical text")
		}
		if _, duplicate := seen[response.ToolID]; duplicate {
			return Profile{}, fmt.Errorf("duplicate scenario connector response for tool %q", response.ToolID)
		}
		if response.OutputSchemaDigest != strings.TrimSpace(response.OutputSchemaDigest) || !digestPattern.MatchString(response.OutputSchemaDigest) {
			return Profile{}, fmt.Errorf("scenario connector response for tool %q has invalid output_schema_digest", response.ToolID)
		}
		canonicalResponse, err := canonicaljson.Canonicalize(response.Response)
		if err != nil {
			return Profile{}, fmt.Errorf("scenario connector response for tool %q: %w", response.ToolID, err)
		}
		seen[response.ToolID] = struct{}{}
		admitted := ConnectorResponse{
			ToolID: response.ToolID, OutputSchemaDigest: response.OutputSchemaDigest,
			Response: append(json.RawMessage(nil), canonicalResponse...),
		}
		responseMap[response.ToolID] = admitted
		doc.ConnectorResponses = append(doc.ConnectorResponses, profileResponseDocument(admitted))
	}
	sort.Slice(doc.ConnectorResponses, func(left, right int) bool {
		return doc.ConnectorResponses[left].ToolID < doc.ConnectorResponses[right].ToolID
	})
	raw, err := canonicaljson.Bytes(doc)
	if err != nil {
		return Profile{}, fmt.Errorf("canonicalize scenario execution profile: %w", err)
	}
	return Profile{
		identity: identity, id: profileID, digest: canonicaljson.HashBytes(raw),
		raw: append(json.RawMessage(nil), raw...), responses: responseMap,
	}, nil
}

func DecodeProfile(raw []byte, expectedDigest string) (Profile, error) {
	if expectedDigest != strings.TrimSpace(expectedDigest) || !digestPattern.MatchString(expectedDigest) {
		return Profile{}, fmt.Errorf("profile_digest must use canonical sha256 encoding")
	}
	canonical, err := canonicaljson.Canonicalize(raw)
	if err != nil {
		return Profile{}, fmt.Errorf("decode scenario execution profile: %w", err)
	}
	if !bytes.Equal(raw, canonical) {
		return Profile{}, fmt.Errorf("scenario execution profile bytes are not canonical")
	}
	if actual := canonicaljson.HashBytes(raw); actual != expectedDigest {
		return Profile{}, fmt.Errorf("scenario execution profile digest mismatch: stored=%s actual=%s", expectedDigest, actual)
	}
	var doc profileDocument
	if err := canonicaljson.DecodeInto(raw, &doc); err != nil {
		return Profile{}, fmt.Errorf("decode scenario execution profile: %w", err)
	}
	if doc.Version != ExecutionProfileVersion {
		return Profile{}, fmt.Errorf("unsupported scenario execution profile version %q", doc.Version)
	}
	sourceFact, err := runtimecorrelation.DecodeSourceArtifactFact(doc.Source.BundleHash)
	if err != nil {
		return Profile{}, fmt.Errorf("decode scenario execution profile source: %w", err)
	}
	identity, err := NewEffectiveSourceIdentity(sourceFact, doc.Source.EffectiveSourceDigest)
	if err != nil {
		return Profile{}, err
	}
	responses := make([]ConnectorResponse, 0, len(doc.ConnectorResponses))
	for _, response := range doc.ConnectorResponses {
		responses = append(responses, ConnectorResponse(response))
	}
	decoded, err := NewProfile(identity, doc.ProfileID, responses)
	if err != nil {
		return Profile{}, err
	}
	if !bytes.Equal(decoded.raw, raw) || decoded.digest != expectedDigest {
		return Profile{}, fmt.Errorf("scenario execution profile canonical reconstruction mismatch")
	}
	return decoded, nil
}

func (p Profile) Validate() error {
	if p.id == "" || p.digest == "" || len(p.raw) == 0 {
		return fmt.Errorf("scenario execution profile is missing")
	}
	_, err := DecodeProfile(p.raw, p.digest)
	return err
}

func (p Profile) ID() string                                       { return p.id }
func (p Profile) Digest() string                                   { return p.digest }
func (p Profile) EffectiveSourceIdentity() EffectiveSourceIdentity { return p.identity }
func (p Profile) CanonicalBytes() []byte                           { return append([]byte(nil), p.raw...) }

func (p Profile) ConnectorResponses() []ConnectorResponse {
	ids := make([]string, 0, len(p.responses))
	for id := range p.responses {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]ConnectorResponse, 0, len(ids))
	for _, id := range ids {
		response := p.responses[id]
		response.Response = append(json.RawMessage(nil), response.Response...)
		out = append(out, response)
	}
	return out
}

type Selector struct {
	ProfileID             string `json:"profile_id"`
	ProfileDigest         string `json:"profile_digest"`
	EffectiveSourceDigest string `json:"effective_source_digest"`
}

func NewSelector(profile Profile) (Selector, error) {
	if err := profile.Validate(); err != nil {
		return Selector{}, err
	}
	return Selector{ProfileID: profile.ID(), ProfileDigest: profile.Digest(), EffectiveSourceDigest: profile.identity.Digest()}, nil
}

func (s Selector) Validate() error {
	if s.ProfileID != strings.TrimSpace(s.ProfileID) || s.ProfileID == "" {
		return fmt.Errorf("scenario_execution.profile_id is required")
	}
	if s.ProfileDigest != strings.TrimSpace(s.ProfileDigest) || !digestPattern.MatchString(s.ProfileDigest) {
		return fmt.Errorf("scenario_execution.profile_digest must use canonical sha256 encoding")
	}
	if s.EffectiveSourceDigest != strings.TrimSpace(s.EffectiveSourceDigest) || !digestPattern.MatchString(s.EffectiveSourceDigest) {
		return fmt.Errorf("scenario_execution.effective_source_digest must use canonical sha256 encoding")
	}
	return nil
}

type Catalog struct {
	identity EffectiveSourceIdentity
	profiles map[string]Profile
}

func NewCatalog(identity EffectiveSourceIdentity, profiles []Profile) (*Catalog, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	catalog := &Catalog{identity: identity, profiles: make(map[string]Profile, len(profiles))}
	for _, profile := range profiles {
		if err := profile.Validate(); err != nil {
			return nil, err
		}
		if !identity.Equal(profile.identity) {
			return nil, fmt.Errorf("scenario profile %q effective source identity mismatch", profile.ID())
		}
		if _, duplicate := catalog.profiles[profile.ID()]; duplicate {
			return nil, fmt.Errorf("duplicate scenario profile_id %q", profile.ID())
		}
		catalog.profiles[profile.ID()] = profile
	}
	return catalog, nil
}

func (c *Catalog) EffectiveSourceIdentity() EffectiveSourceIdentity {
	if c == nil {
		return EffectiveSourceIdentity{}
	}
	return c.identity
}

func (c *Catalog) Resolve(selector Selector) (Profile, error) {
	if c == nil {
		return Profile{}, fmt.Errorf("scenario execution profile catalog is unavailable")
	}
	if err := selector.Validate(); err != nil {
		return Profile{}, err
	}
	if selector.EffectiveSourceDigest != c.identity.Digest() {
		return Profile{}, fmt.Errorf("scenario execution effective source mismatch: selector=%s runtime=%s", selector.EffectiveSourceDigest, c.identity.Digest())
	}
	profile, ok := c.profiles[selector.ProfileID]
	if !ok {
		return Profile{}, fmt.Errorf("scenario execution profile %q is not present in the selected runtime", selector.ProfileID)
	}
	if profile.Digest() != selector.ProfileDigest {
		return Profile{}, fmt.Errorf("scenario execution profile %q digest mismatch: selector=%s runtime=%s", selector.ProfileID, selector.ProfileDigest, profile.Digest())
	}
	return profile, nil
}

type admittedProfileContextKey struct{}

func WithAdmittedProfile(ctx context.Context, profile Profile) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	return context.WithValue(ctx, admittedProfileContextKey{}, profile), nil
}

func AdmittedProfileFromContext(ctx context.Context) (Profile, bool) {
	if ctx == nil {
		return Profile{}, false
	}
	profile, ok := ctx.Value(admittedProfileContextKey{}).(Profile)
	return profile, ok && profile.Validate() == nil
}
