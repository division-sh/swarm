package agentintent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

type SourceKind string

const (
	SourceLocal  SourceKind = "local"
	SourceInline SourceKind = "inline"
	SourceImport SourceKind = "import"
)

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
	resolved := Resolved{
		Kind:       kind,
		Coordinate: strings.TrimSpace(coordinate),
		Provenance: strings.TrimSpace(provenance),
		Content:    content,
	}
	if resolved.Kind != SourceLocal && resolved.Kind != SourceInline {
		return Resolved{}, fmt.Errorf("agent intent kind %q cannot be resolved", kind)
	}
	if resolved.Coordinate == "" || resolved.Provenance == "" {
		return Resolved{}, fmt.Errorf("agent intent coordinate and provenance are required")
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
