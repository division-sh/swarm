package contracts

import (
	"path"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/packartifact"
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
			switch file {
			case path.Join(entry.Directory(), packartifact.EnvelopeFileName):
				raw = entry.EnvelopeBody()
			case path.Join(entry.Directory(), packartifact.ManifestFileNameForType(entry.Type())):
				raw = entry.ManifestBody()
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
			found := false
			for _, field := range fields {
				if field.Name == key {
					value, found = field.Value, true
					location = value.IntroductionLocation()
					if declarationKey && slot == "" {
						location = field.IntroductionLocation()
					}
					break
				}
			}
			if !found {
				return yamlsource.Location{}, false
			}
		}
		if strings.HasPrefix(slot, ".") {
			slot = slot[1:]
			if slot == "" {
				return yamlsource.Location{}, false
			}
		} else if slot != "" && slot[0] != '[' {
			return yamlsource.Location{}, false
		}
	}
	return location, location.Line > 0 && location.Column > 0
}

// Decode the existing diagnostic slot spelling without normalizing map keys.
func introductionPathSegment(slot string) (key string, index int, rest string, ok bool) {
	if !strings.HasPrefix(slot, "[") {
		end := strings.IndexAny(slot, ".[")
		if end < 0 {
			end = len(slot)
		}
		return slot[:end], -1, slot[end:], end > 0
	}
	if strings.HasPrefix(slot, `["`) {
		quoted, err := strconv.QuotedPrefix(slot[1:])
		end := len(quoted) + 1
		if err != nil || len(slot) <= end || slot[end] != ']' {
			return "", -1, "", false
		}
		key, err = strconv.Unquote(quoted)
		return key, -1, slot[end+1:], err == nil
	}
	end := strings.IndexByte(slot, ']')
	if end < 0 {
		return "", -1, "", false
	}
	index, err := strconv.Atoi(slot[1:end])
	return "", index, slot[end+1:], err == nil && index >= 0
}
