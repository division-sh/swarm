package values

import (
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
)

type Bucket struct {
	data map[string]any
}

func Wrap(data map[string]any) Bucket {
	if data == nil {
		data = map[string]any{}
	}
	return Bucket{data: data}
}

func (b Bucket) Raw() map[string]any {
	if b.data == nil {
		return map[string]any{}
	}
	return b.data
}

func (b Bucket) Clone() Bucket {
	if len(b.data) == 0 {
		return Wrap(map[string]any{})
	}
	out := make(map[string]any, len(b.data))
	for key, value := range b.data {
		out[key] = value
	}
	return Wrap(out)
}

func (b Bucket) Map(key string) (Bucket, bool) {
	value, ok := b.data[strings.TrimSpace(key)]
	if !ok {
		return Bucket{}, false
	}
	raw, ok := asMap(value)
	if !ok {
		return Bucket{}, false
	}
	return Wrap(raw), true
}

func (b Bucket) EnsureMap(key string) Bucket {
	key = strings.TrimSpace(key)
	if key == "" {
		return Wrap(map[string]any{})
	}
	if existing, ok := b.Map(key); ok {
		return existing
	}
	child := map[string]any{}
	b.data[key] = child
	return Wrap(child)
}

func (b Bucket) String(key string) string {
	return strings.TrimSpace(asString(b.data[strings.TrimSpace(key)]))
}

func (b Bucket) Int(key string) int {
	return asInt(b.data[strings.TrimSpace(key)])
}

func (b Bucket) Bool(key string) bool {
	return truthy(b.data[strings.TrimSpace(key)])
}

func (b Bucket) Set(key string, value any) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	b.data[key] = value
}

func (b Bucket) Keys() []string {
	if len(b.data) == 0 {
		return nil
	}
	out := make([]string, 0, len(b.data))
	for key := range b.data {
		out = append(out, strings.TrimSpace(key))
	}
	return out
}

func (b Bucket) Lookup(path paths.Path) (any, bool) {
	if path.IsZero() {
		return nil, false
	}
	current := any(b.Raw())
	for _, segment := range path.Segments {
		object, ok := asMap(current)
		if !ok {
			return nil, false
		}
		current = object[segment]
	}
	return current, current != nil
}

func (b Bucket) SetPath(path paths.Path, value any) {
	if path.IsZero() {
		return
	}
	current := b.Raw()
	for i, segment := range path.Segments {
		if i == len(path.Segments)-1 {
			current[segment] = value
			return
		}
		next, ok := asMap(current[segment])
		if !ok || next == nil {
			next = map[string]any{}
			current[segment] = next
		}
		current = next
	}
}

func asMap(v any) (map[string]any, bool) {
	switch typed := v.(type) {
	case map[string]any:
		return typed, true
	case Bucket:
		return typed.Raw(), true
	default:
		return nil, false
	}
}

func asString(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	default:
		return ""
	}
}

func truthy(v any) bool {
	switch typed := v.(type) {
	case bool:
		return typed
	case string:
		switch strings.TrimSpace(strings.ToLower(typed)) {
		case "true", "1", "yes", "y":
			return true
		default:
			return false
		}
	case int:
		return typed != 0
	case int64:
		return typed != 0
	case float64:
		return typed != 0
	default:
		return false
	}
}

func asInt(v any) int {
	switch typed := v.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case float32:
		return int(typed)
	default:
		return 0
	}
}
