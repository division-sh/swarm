package contracts

import (
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

// IntroductionLocation resolves one diagnostic slot from exact retained bytes.
// Missing locations never replace the refusal that requested them.
func (b *WorkflowContractBundle) IntroductionLocation(file, slot string, declarationKey bool) (yamlsource.Location, bool) {
	if b == nil {
		return yamlsource.Location{}, false
	}
	var raw []byte
	if entry, ok := b.SourceArtifact.Entry(file); ok {
		raw = entry.Bytes()
	} else {
		for _, entry := range b.PackInventory.Entries() {
			for label, body := range entry.Provider().Members() {
				if file == path.Join(entry.Directory(), label) {
					raw = body
					break
				}
			}
			if raw != nil {
				break
			}
		}
	}
	if raw == nil {
		return yamlsource.Location{}, false
	}
	// This store is invocation-local and retains no parsed document or positions.
	snapshot, err := yamlsource.NewStore(yamlsource.Limits{}).Load(raw)
	if err != nil {
		return yamlsource.Location{}, false
	}
	value := snapshot.Document(file).Root()
	location := value.IntroductionLocation()
	for slot != "" {
		key, index, rest, ok := introductionPathSegment(slot)
		if !ok {
			return yamlsource.Location{}, false
		}
		slot = rest
		if index >= 0 {
			items, err := value.Sequence()
			if err != nil || index >= len(items) {
				return yamlsource.Location{}, false
			}
			value = items[index]
			location = value.IntroductionLocation()
		} else {
			fields, err := value.Mapping()
			if err != nil {
				return yamlsource.Location{}, false
			}
			fieldIndex := slices.IndexFunc(fields, func(field yamlsource.MappingField) bool { return field.Name == key })
			if fieldIndex < 0 {
				return yamlsource.Location{}, false
			}
			field := fields[fieldIndex]
			value = field.Value
			location = value.IntroductionLocation()
			if declarationKey && slot == "" {
				location = field.IntroductionLocation()
			}
		}
	}
	return location, location.Line > 0 && location.Column > 0
}

// Decode the existing diagnostic slot spelling without normalizing map keys.
func introductionPathSegment(slot string) (key string, index int, rest string, ok bool) {
	index = -1
	switch {
	case !strings.HasPrefix(slot, "["):
		end := strings.IndexAny(slot, ".[")
		if end < 0 {
			end = len(slot)
		}
		if end == 0 {
			return "", -1, "", false
		}
		key, rest = slot[:end], slot[end:]
	case strings.HasPrefix(slot, `["`):
		quoted, err := strconv.QuotedPrefix(slot[1:])
		end := len(quoted) + 1
		if err != nil || len(slot) <= end || slot[end] != ']' {
			return "", -1, "", false
		}
		key, err = strconv.Unquote(quoted)
		if err != nil {
			return "", -1, "", false
		}
		rest = slot[end+1:]
	default:
		end := strings.IndexByte(slot, ']')
		if end < 0 {
			return "", -1, "", false
		}
		var err error
		index, err = strconv.Atoi(slot[1:end])
		if err != nil || index < 0 {
			return "", -1, "", false
		}
		rest = slot[end+1:]
	}
	if strings.HasPrefix(rest, ".") {
		rest = rest[1:]
		if rest == "" {
			return "", -1, "", false
		}
	} else if rest != "" && rest[0] != '[' {
		return "", -1, "", false
	}
	return key, index, rest, true
}
