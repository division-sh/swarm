package tools

import "errors"

var (
	ErrToolNotAllowed     = errors.New("tools: tool not allowed for agent")
	ErrUnknownEntityType  = errors.New("tools: unknown entity type")
	ErrUnknownEntityField = errors.New("tools: unknown entity field")
)
