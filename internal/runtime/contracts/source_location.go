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
		var key string
		index := -1
		if slot[0] == '[' {
			if len(slot) > 1 && slot[1] == '"' {
				quoted, err := strconv.QuotedPrefix(slot[1:])
				if err != nil || len(slot) <= len(quoted)+1 || slot[len(quoted)+1] != ']' {
					return yamlsource.Location{}, false
				}
				key, err = strconv.Unquote(quoted)
				if err != nil {
					return yamlsource.Location{}, false
				}
				slot = slot[len(quoted)+2:]
			} else {
				end := strings.IndexByte(slot, ']')
				if end < 0 {
					return yamlsource.Location{}, false
				}
				index, err = strconv.Atoi(slot[1:end])
				if err != nil || index < 0 {
					return yamlsource.Location{}, false
				}
				slot = slot[end+1:]
			}
		} else {
			end := strings.IndexAny(slot, ".[")
			if end < 0 {
				end = len(slot)
			}
			if end == 0 {
				return yamlsource.Location{}, false
			}
			key, slot = slot[:end], slot[end:]
		}
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
