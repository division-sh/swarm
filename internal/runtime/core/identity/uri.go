package identity

import (
	"fmt"
	"strings"
)

type WorkflowURI struct {
	Segments []string
}

func ParseWorkflowURI(raw string) (WorkflowURI, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return WorkflowURI{}, nil
	}
	parts := strings.Split(raw, "/")
	segments := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return WorkflowURI{}, fmt.Errorf("invalid workflow uri %q", raw)
		}
		segments = append(segments, part)
	}
	return WorkflowURI{Segments: segments}, nil
}

func MustParseWorkflowURI(raw string) WorkflowURI {
	uri, err := ParseWorkflowURI(raw)
	if err != nil {
		panic(err)
	}
	return uri
}

func (u WorkflowURI) String() string {
	if len(u.Segments) == 0 {
		return ""
	}
	return strings.Join(u.Segments, "/")
}

func (u WorkflowURI) IsZero() bool {
	return len(u.Segments) == 0
}

func (u WorkflowURI) Parent() WorkflowURI {
	if len(u.Segments) <= 1 {
		return WorkflowURI{}
	}
	return WorkflowURI{Segments: append([]string{}, u.Segments[:len(u.Segments)-1]...)}
}

func (u WorkflowURI) Last() string {
	if len(u.Segments) == 0 {
		return ""
	}
	return u.Segments[len(u.Segments)-1]
}

func (u WorkflowURI) Join(parts ...string) WorkflowURI {
	segments := append([]string{}, u.Segments...)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		for _, piece := range strings.Split(part, "/") {
			piece = strings.TrimSpace(piece)
			if piece == "" {
				continue
			}
			segments = append(segments, piece)
		}
	}
	return WorkflowURI{Segments: segments}
}

func (u WorkflowURI) Resolve(ref string) (WorkflowURI, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return u, nil
	}
	if strings.HasPrefix(ref, "/") {
		return ParseWorkflowURI(strings.TrimPrefix(ref, "/"))
	}
	base := append([]string{}, u.Segments...)
	for _, part := range strings.Split(ref, "/") {
		part = strings.TrimSpace(part)
		switch part {
		case "", ".":
			continue
		case "..":
			if len(base) == 0 {
				return WorkflowURI{}, fmt.Errorf("cannot resolve %q from %q", ref, u.String())
			}
			base = base[:len(base)-1]
		default:
			base = append(base, part)
		}
	}
	return WorkflowURI{Segments: base}, nil
}
