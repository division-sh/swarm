package contracts

// EntityFieldBookkeepingKeys enumerates the platform-owned entity field keys
// that runtime machinery injects into an entity's field surface as
// bookkeeping, as opposed to workflow-declared content fields.
//
// This is a DISPLAY classification only: the CLI uses it to hide bookkeeping
// below the fold in default human output (shown under --verbose and always in
// --json). It does not reserve or reject names at contract admission.
//
// entity.get currently combines authored fields and platform bookkeeping in
// one map, so this key-only classifier cannot distinguish an authored collision
// from a platform value. Such collisions remain available under --verbose and
// --json but are hidden from the default Fields section. Issue #2242 owns the
// API-side separation and removal of this temporary classifier.
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
