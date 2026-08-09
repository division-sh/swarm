package contracts

// EntityFieldBookkeepingKeys enumerates the platform-owned entity field keys
// that runtime machinery injects into an entity's field surface as
// bookkeeping, as opposed to workflow-declared content fields.
//
// This is a DISPLAY classification only: the CLI uses it to hide bookkeeping
// below the fold in default human output (shown under --verbose and always in
// --json). It does NOT reserve or reject these names at contract admission; a
// workflow-declared field remains content, full stop. The fail-visible design
// exists precisely so a forgotten platform key SHOWS UP in default output and
// is caught by eyes rather than names becoming reserved.
//
// Ownership test (recorded so every future ambiguous key inherits a rule
// rather than precedent-by-vibes): if runtime machinery writes the key into
// the entity field surface as platform bookkeeping, it is BOOKKEEPING; if the
// key names a workflow-declared field, it is CONTENT.
//
// Current platform injection sites:
//   - activation, bundle_hash, package_key:       internal/runtime/standing_targets.go
//   - last_data_accumulation_event:               internal/runtime/engine/executor.go
//   - last_data_accumulation_source:              internal/runtime/engine/helpers.go
//   - fan_out_count:                              internal/runtime/engine/executor.go
//   - last_source_event:                          internal/runtime/bus/template_instance_lifecycle.go
//
// `last_word` is CONTENT: no runtime machinery writes it; flows declare it.
//
// `entity_id` is deliberately NOT listed: no verified field-surface writer
// exists, and the expression contract permits entity.entity_id when entity_id
// is declared as business data, so it is a legal declared field and content.
var EntityFieldBookkeepingKeys = []string{
	"activation",
	"bundle_hash",
	"package_key",
	"last_data_accumulation_event",
	"last_data_accumulation_source",
	"fan_out_count",
	"last_source_event",
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
