package agentintent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
	"gopkg.in/yaml.v3"
)

type SourceKind string

const (
	SourceLocal  SourceKind = "local"
	SourceInline SourceKind = "inline"
	SourceImport SourceKind = "import"
)

var windowsAbsolutePath = regexp.MustCompile(`^[A-Za-z]:`)

// Source is the closed authored source union accepted by agents.yaml.
type Source struct {
	Kind     SourceKind `json:"kind"`
	Local    string     `json:"local,omitempty"`
	Inline   string     `json:"inline,omitempty"`
	Import   string     `json:"import,omitempty"`
	Override string     `json:"override,omitempty"`
}

func (s *Source) UnmarshalYAML(node *yaml.Node) error {
	if node == nil {
		return fmt.Errorf("agent intent source is required")
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if node.Tag != "" && node.Tag != "!!str" {
			return fmt.Errorf("agent intent scalar must be a local file path")
		}
		*s = Source{Kind: SourceLocal, Local: strings.TrimSpace(node.Value)}
		return s.ValidateSyntax()
	case yaml.MappingNode:
		values := map[string]string{}
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			if _, exists := values[key]; exists {
				return fmt.Errorf("agent intent source repeats key %q", key)
			}
			if key != "inline" && key != "import" && key != "override" {
				return fmt.Errorf("agent intent source key %q is unsupported; use local scalar, inline, or import", key)
			}
			value := node.Content[i+1]
			if value.Kind != yaml.ScalarNode || (value.Tag != "" && value.Tag != "!!str") {
				return fmt.Errorf("agent intent source %s must be text", key)
			}
			if key == "inline" {
				values[key] = value.Value
			} else {
				values[key] = strings.TrimSpace(value.Value)
			}
		}
		_, hasInline := values["inline"]
		_, hasImport := values["import"]
		_, hasOverride := values["override"]
		switch {
		case hasInline && !hasImport && !hasOverride && len(values) == 1:
			*s = Source{Kind: SourceInline, Inline: values["inline"]}
		case hasImport && !hasInline && len(values) <= 2 && (!hasOverride || len(values) == 2):
			*s = Source{Kind: SourceImport, Import: values["import"], Override: values["override"]}
		default:
			return fmt.Errorf("agent intent source must be exactly inline, import, or import with override")
		}
		return s.ValidateSyntax()
	default:
		return fmt.Errorf("agent intent source must be a local path scalar or a typed mapping")
	}
}

func (s Source) ValidateSyntax() error {
	switch s.Kind {
	case SourceLocal:
		if strings.TrimSpace(s.Local) == "" {
			return fmt.Errorf("agent intent local path must not be empty")
		}
		if s.Inline != "" || s.Import != "" || s.Override != "" {
			return fmt.Errorf("agent intent local source contains conflicting fields")
		}
	case SourceInline:
		if strings.TrimSpace(s.Inline) == "" {
			return fmt.Errorf("agent intent inline content must not be blank")
		}
		if !utf8.ValidString(s.Inline) {
			return fmt.Errorf("agent intent inline content must be valid UTF-8")
		}
		if s.Local != "" || s.Import != "" || s.Override != "" {
			return fmt.Errorf("agent intent inline source contains conflicting fields")
		}
	case SourceImport:
		if strings.TrimSpace(s.Import) == "" {
			return fmt.Errorf("agent intent import must not be empty")
		}
		separator := strings.LastIndexByte(s.Import, '@')
		if separator <= 0 || separator == len(s.Import)-1 || strings.IndexFunc(s.Import, unicode.IsSpace) >= 0 {
			return fmt.Errorf("agent intent import %q must name an explicitly versioned source such as support-drafter@1", s.Import)
		}
		if s.Local != "" || s.Inline != "" {
			return fmt.Errorf("agent intent import source contains conflicting fields")
		}
	case "":
		return fmt.Errorf("agent intent source is required; declare exactly one intent: source")
	default:
		return fmt.Errorf("agent intent source kind %q is unsupported", s.Kind)
	}
	return nil
}

// Resolved is the immutable intent artifact shared by verification, bundle,
// runtime, recovery, fork, and inspection consumers.
type Resolved struct {
	Kind        SourceKind `json:"kind"`
	Coordinate  string     `json:"coordinate"`
	Provenance  string     `json:"provenance"`
	Content     string     `json:"content"`
	ContentHash string     `json:"content_hash"`
	Identity    string     `json:"identity"`
}

func Resolve(kind SourceKind, coordinate, provenance, content string) (Resolved, error) {
	coordinate, err := validateCoordinate(kind, coordinate)
	if err != nil {
		return Resolved{}, err
	}
	provenance, err = ValidateDeclarationProvenance(provenance)
	if err != nil {
		return Resolved{}, err
	}
	resolved := Resolved{
		Kind:       kind,
		Coordinate: coordinate,
		Provenance: provenance,
		Content:    content,
	}
	if !utf8.ValidString(content) {
		return Resolved{}, fmt.Errorf("agent intent content must be valid UTF-8")
	}
	if strings.TrimSpace(content) == "" {
		return Resolved{}, fmt.Errorf("agent intent content must not be blank")
	}
	contentSum := sha256.Sum256([]byte(content))
	resolved.ContentHash = "sha256:" + hex.EncodeToString(contentSum[:])
	identityInput := struct {
		Domain      string     `json:"domain"`
		Version     int        `json:"version"`
		Kind        SourceKind `json:"kind"`
		Coordinate  string     `json:"coordinate"`
		Provenance  string     `json:"provenance"`
		ContentHash string     `json:"content_hash"`
	}{
		Domain:      "division-sh.swarm.agent-intent",
		Version:     1,
		Kind:        resolved.Kind,
		Coordinate:  resolved.Coordinate,
		Provenance:  resolved.Provenance,
		ContentHash: resolved.ContentHash,
	}
	raw, err := json.Marshal(identityInput)
	if err != nil {
		return Resolved{}, fmt.Errorf("encode agent intent identity: %w", err)
	}
	identitySum := sha256.Sum256(raw)
	resolved.Identity = "agent-intent:v1:sha256:" + hex.EncodeToString(identitySum[:])
	return resolved, nil
}

// NewDeclarationProvenance is the canonical owner of resolved-intent
// declaration coordinates.
func NewDeclarationProvenance(declarationPath, agentID string) (string, error) {
	declarationPath, err := validateCanonicalRelativePath(declarationPath, "agent intent declaration path")
	if err != nil {
		return "", err
	}
	if path.Base(declarationPath) != "agents.yaml" {
		return "", fmt.Errorf("agent intent declaration path %q must name agents.yaml", declarationPath)
	}
	if agentID == "" || agentID != strings.TrimSpace(agentID) || !utf8.ValidString(agentID) || !norm.NFC.IsNormalString(agentID) {
		return "", fmt.Errorf("agent intent declaration agent id %q is not canonical", agentID)
	}
	if strings.ContainsAny(agentID, "/\\#\x00") || strings.IndexFunc(agentID, unicode.IsControl) >= 0 {
		return "", fmt.Errorf("agent intent declaration agent id %q contains an unsupported character", agentID)
	}
	return declarationPath + "#agents." + agentID + ".intent", nil
}

func ValidateDeclarationProvenance(raw string) (string, error) {
	const marker = "#agents."
	if raw == "" || raw != strings.TrimSpace(raw) || !utf8.ValidString(raw) || !norm.NFC.IsNormalString(raw) {
		return "", fmt.Errorf("agent intent declaration provenance %q is not canonical", raw)
	}
	separator := strings.Index(raw, marker)
	if separator <= 0 || strings.Index(raw[separator+len(marker):], marker) >= 0 || !strings.HasSuffix(raw, ".intent") {
		return "", fmt.Errorf("agent intent declaration provenance %q must be <agents.yaml path>#agents.<id>.intent", raw)
	}
	declarationPath := raw[:separator]
	agentID := strings.TrimSuffix(raw[separator+len(marker):], ".intent")
	canonical, err := NewDeclarationProvenance(declarationPath, agentID)
	if err != nil {
		return "", err
	}
	if canonical != raw {
		return "", fmt.Errorf("agent intent declaration provenance %q is not canonical", raw)
	}
	return canonical, nil
}

func validateCoordinate(kind SourceKind, coordinate string) (string, error) {
	switch kind {
	case SourceInline:
		if coordinate != "inline" {
			return "", fmt.Errorf("inline agent intent coordinate must be exactly %q", "inline")
		}
		return coordinate, nil
	case SourceLocal:
		return validateCanonicalRelativePath(coordinate, "local agent intent coordinate")
	default:
		return "", fmt.Errorf("agent intent kind %q cannot be resolved", kind)
	}
}

func validateCanonicalRelativePath(raw, label string) (string, error) {
	if raw == "" || raw != strings.TrimSpace(raw) || !utf8.ValidString(raw) || !norm.NFC.IsNormalString(raw) {
		return "", fmt.Errorf("%s %q is not canonical UTF-8 NFC text", label, raw)
	}
	if strings.ContainsRune(raw, '\x00') || strings.Contains(raw, `\`) {
		return "", fmt.Errorf("%s %q must use canonical forward separators", label, raw)
	}
	if path.IsAbs(raw) || strings.HasPrefix(raw, "//") || windowsAbsolutePath.MatchString(raw) {
		return "", fmt.Errorf("%s %q must be relative", label, raw)
	}
	for _, segment := range strings.Split(raw, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", fmt.Errorf("%s %q contains an empty, dot, or traversal segment", label, raw)
		}
	}
	if path.Clean(raw) != raw {
		return "", fmt.Errorf("%s %q is not canonical", label, raw)
	}
	return raw, nil
}

// DerivedPrompt is a runtime-only rendering of immutable intent plus
// separately owned contract criteria. Its fields are intentionally private so
// persistence cannot become another authored prompt surface.
type DerivedPrompt struct {
	intentIdentity string
	criteria       []string
	text           string
}

func NewDerivedPrompt(intent Resolved, criteria []string, criteriaSection string) (DerivedPrompt, error) {
	if err := intent.Validate(); err != nil {
		return DerivedPrompt{}, err
	}
	criteria = normalizeCriteria(criteria)
	if len(criteria) == 0 {
		if criteriaSection != "" {
			return DerivedPrompt{}, fmt.Errorf("derived agent prompt cannot carry criteria text without criteria references")
		}
	} else {
		if !utf8.ValidString(criteriaSection) || !strings.HasPrefix(criteriaSection, "\n\n## Contract Criteria\n\n") {
			return DerivedPrompt{}, fmt.Errorf("derived agent prompt criteria section is not canonical")
		}
	}
	return DerivedPrompt{
		intentIdentity: intent.Identity,
		criteria:       append([]string(nil), criteria...),
		text:           intent.Content + criteriaSection,
	}, nil
}

func IntentOnlyPrompt(intent Resolved) (DerivedPrompt, error) {
	return NewDerivedPrompt(intent, nil, "")
}

func (p DerivedPrompt) Empty() bool {
	return p.intentIdentity == "" && len(p.criteria) == 0 && p.text == ""
}

func (p DerivedPrompt) Validate(intent Resolved, criteria []string) error {
	if p.Empty() {
		return fmt.Errorf("derived agent prompt is required")
	}
	if err := intent.Validate(); err != nil {
		return err
	}
	if p.intentIdentity != intent.Identity {
		return fmt.Errorf("derived agent prompt intent identity does not match resolved intent")
	}
	canonicalCriteria := normalizeCriteria(criteria)
	if !slices.Equal(criteria, canonicalCriteria) {
		return fmt.Errorf("agent criteria references are not canonical")
	}
	if !slices.Equal(p.criteria, canonicalCriteria) {
		return fmt.Errorf("derived agent prompt criteria do not match agent criteria references")
	}
	if !strings.HasPrefix(p.text, intent.Content) {
		return fmt.Errorf("derived agent prompt does not contain the exact resolved intent")
	}
	return nil
}

func (p DerivedPrompt) Text(intent Resolved, criteria []string) (string, error) {
	if err := p.Validate(intent, criteria); err != nil {
		return "", err
	}
	return p.text, nil
}

func normalizeCriteria(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, raw := range in {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	slices.Sort(out)
	return out
}

func (r Resolved) Empty() bool {
	return r.Kind == "" && r.Coordinate == "" && r.Provenance == "" && r.Content == "" && r.ContentHash == "" && r.Identity == ""
}

func (r Resolved) Validate() error {
	if r.Empty() {
		return fmt.Errorf("resolved agent intent is required")
	}
	expected, err := Resolve(r.Kind, r.Coordinate, r.Provenance, r.Content)
	if err != nil {
		return err
	}
	if r.Coordinate != expected.Coordinate {
		return fmt.Errorf("resolved agent intent coordinate is not canonical")
	}
	if r.Provenance != expected.Provenance {
		return fmt.Errorf("resolved agent intent provenance is not canonical")
	}
	if r.ContentHash != expected.ContentHash {
		return fmt.Errorf("resolved agent intent content hash does not match content")
	}
	if r.Identity != expected.Identity {
		return fmt.Errorf("resolved agent intent identity does not match its canonical facts")
	}
	return nil
}
