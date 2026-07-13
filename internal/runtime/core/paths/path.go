package paths

import (
	"fmt"
	"regexp"
	"strings"
)

var strictRelativeSegmentPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)

type PathRoot uint8

const (
	RootUnknown PathRoot = iota
	RootPayload
	RootEvent
	RootEntity
	RootPlatformEntity
	RootPolicy
	RootMetadata
	RootGates
	RootAccumulated
	RootFanOut
	RootJoin
	RootLoop
	RootComputed
)

func (r PathRoot) String() string {
	switch r {
	case RootPayload:
		return "payload"
	case RootEvent:
		return "event"
	case RootEntity:
		return "entity"
	case RootPlatformEntity:
		return "_entity"
	case RootPolicy:
		return "policy"
	case RootMetadata:
		return "metadata"
	case RootGates:
		return "gates"
	case RootAccumulated:
		return "accumulated"
	case RootFanOut:
		return "fan_out"
	case RootJoin:
		return "join"
	case RootLoop:
		return "loop"
	case RootComputed:
		return "computed"
	default:
		return ""
	}
}

type Path struct {
	Root     PathRoot
	Segments []string
	Raw      string
}

func Parse(text string) Path {
	raw := strings.TrimSpace(text)
	if raw == "" {
		return Path{}
	}
	parts := strings.Split(raw, ".")
	segments := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		segments = append(segments, part)
	}
	if len(segments) == 0 {
		return Path{}
	}
	root := parseRoot(segments[0])
	if root != RootUnknown {
		return Path{Root: root, Segments: append([]string(nil), segments[1:]...), Raw: raw}
	}
	return Path{Root: RootUnknown, Segments: append([]string(nil), segments...), Raw: raw}
}

// ParseStrictRelative parses declaration-time paths that are relative to an
// already selected value. Unlike Parse, it rejects empty segments and runtime
// expression roots instead of silently normalizing them.
func ParseStrictRelative(text string) (Path, error) {
	raw := strings.TrimSpace(text)
	if raw == "" {
		return Path{}, fmt.Errorf("relative dotted path is required")
	}
	if strings.HasPrefix(raw, "$") {
		return Path{}, fmt.Errorf("relative dotted path %q must not use JSONPath syntax", raw)
	}
	parts := strings.Split(raw, ".")
	segments := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return Path{}, fmt.Errorf("relative dotted path %q contains an empty segment", raw)
		}
		if !strictRelativeSegmentPattern.MatchString(part) {
			return Path{}, fmt.Errorf("relative dotted path %q contains unsupported segment %q", raw, part)
		}
		segments = append(segments, part)
	}
	if root := parseRoot(segments[0]); root != RootUnknown {
		return Path{}, fmt.Errorf("relative dotted path %q must not use runtime namespace %q", raw, segments[0])
	}
	return Path{Root: RootUnknown, Segments: segments, Raw: raw}, nil
}

func parseRoot(text string) PathRoot {
	switch strings.ToLower(strings.TrimSpace(text)) {
	case "payload":
		return RootPayload
	case "event":
		return RootEvent
	case "entity":
		return RootEntity
	case "_entity":
		return RootPlatformEntity
	case "policy":
		return RootPolicy
	case "metadata":
		return RootMetadata
	case "gates":
		return RootGates
	case "accumulated":
		return RootAccumulated
	case "fan_out":
		return RootFanOut
	case "join":
		return RootJoin
	case "loop":
		return RootLoop
	case "computed":
		return RootComputed
	default:
		return RootUnknown
	}
}

func (p Path) IsZero() bool {
	return p.Root == RootUnknown && len(p.Segments) == 0 && strings.TrimSpace(p.Raw) == ""
}

func (p Path) HasExplicitRoot() bool {
	return p.Root != RootUnknown
}

func (p Path) String() string {
	if strings.TrimSpace(p.Raw) != "" {
		return strings.TrimSpace(p.Raw)
	}
	if len(p.Segments) == 0 {
		return ""
	}
	if p.Root == RootUnknown {
		return strings.Join(p.Segments, ".")
	}
	return p.Root.String() + "." + strings.Join(p.Segments, ".")
}
