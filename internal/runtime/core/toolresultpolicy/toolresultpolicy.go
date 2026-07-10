package toolresultpolicy

import (
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

const (
	// Keep this above the #571 40KB proof target while still bounding a single
	// inline typed read result with enough room for the LLM tool-call envelope.
	MaxCompleteTypedReadResultBytes = 56 * 1024

	RoleScopedEntityToolAuthorizationClass = "role_scoped_entity_tool"

	TypedReadResultTooLargeCode     = "typed_read_result_too_large"
	TypedReadResultMarshalErrorCode = "typed_read_result_marshal_failed"
)

func IsRoleScopedTypedReadCapability(cap toolcapabilities.Capability) bool {
	if strings.TrimSpace(cap.AuthorizationClass) != RoleScopedEntityToolAuthorizationClass {
		return false
	}
	return IsRoleScopedTypedReadName(cap.Name)
}

func IsRoleScopedTypedReadName(name string) bool {
	name = toolidentity.CanonicalName(name)
	if name == "" || name == "read_file" {
		return false
	}
	return strings.HasPrefix(name, "read_")
}

func IsRoleScopedTypedReadInContext(set toolcapabilities.Set, name string) bool {
	cap, ok := set.Capability(name)
	if !ok {
		return false
	}
	return IsRoleScopedTypedReadCapability(cap)
}

func NewTypedReadResultTooLargeError(component, operation, toolName string, bytes int) error {
	return failures.NewDetail(
		TypedReadResultTooLargeCode,
		component,
		operation,
		map[string]any{
			"limit_kind": "typed_read_result_bytes",
			"tool":       strings.TrimSpace(toolName),
			"actual":     bytes,
			"limit":      MaxCompleteTypedReadResultBytes,
		},
	)
}

func NewTypedReadResultMarshalError(component, operation, toolName string, cause error) error {
	return failures.WrapDetail(
		TypedReadResultMarshalErrorCode,
		component,
		operation,
		map[string]any{"tool": strings.TrimSpace(toolName)},
		cause,
	)
}
