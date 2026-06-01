package toolresultpolicy

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	runtimerterr "github.com/division-sh/swarm/internal/runtime/rterrors"
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
	return runtimerterr.NewRuntimeError(
		TypedReadResultTooLargeCode,
		component,
		operation,
		false,
		"role-scoped typed read %s produced %d bytes, exceeding the complete delivery limit of %d bytes",
		strings.TrimSpace(toolName),
		bytes,
		MaxCompleteTypedReadResultBytes,
	)
}

func NewTypedReadResultMarshalError(component, operation, toolName string, cause error) error {
	if cause == nil {
		cause = fmt.Errorf("marshal typed read result")
	}
	return runtimerterr.WrapRuntimeError(
		TypedReadResultMarshalErrorCode,
		component,
		operation,
		false,
		cause,
		"role-scoped typed read %s result cannot be serialized completely",
		strings.TrimSpace(toolName),
	)
}
