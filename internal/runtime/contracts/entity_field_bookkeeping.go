package contracts

// EntityFieldBookkeepingKeys enumerates the platform-owned entity field keys
// that runtime machinery injects into an entity's field surface as
// bookkeeping, as opposed to workflow-declared content fields.
//
// Ownership test (recorded so every future ambiguous key inherits a rule
// rather than precedent-by-vibes): if runtime machinery writes the key into
// the entity field surface as part of platform bookkeeping (bundle provenance,
// activation identity, engine accumulation metadata), the key is BOOKKEEPING
// and is hidden from default human output (shown under --verbose). If the key
// names a workflow-declared field, it is CONTENT and stays default-visible.
//
// Current platform injection sites:
//   - activation, bundle_hash, package_key:      internal/runtime/standing_targets.go
//   - last_data_accumulation_event:              internal/runtime/engine/executor.go
//   - last_data_accumulation_source:             internal/runtime/engine/helpers.go
//   - fan_out_count:                             internal/runtime/engine/executor.go
//
// `last_word` is CONTENT: no runtime machinery writes it; flows declare it.
// This list must stay adjacent to the injection sites; a platform-injected key
// forgotten here SHOWS UP in default output and is caught by eyes (fail-visible),
// rather than content silently vanishing.
var EntityFieldBookkeepingKeys = []string{
	"activation",
	"bundle_hash",
	"package_key",
	"entity_id",
	"last_data_accumulation_event",
	"last_data_accumulation_source",
	"fan_out_count",
}

// IsEntityFieldBookkeepingKey reports whether key is in EntityFieldBookkeepingKeys.
func IsEntityFieldBookkeepingKey(key string) bool {
	for _, candidate := range EntityFieldBookkeepingKeys {
		if candidate == key {
			return true
		}
	}
	return false
}
