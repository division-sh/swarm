package forkpoint

import (
	"fmt"

	"github.com/google/uuid"
)

// Kind identifies the two admitted historical fork coordinates.
type Kind string

const (
	Event              Kind = "event"
	DeploymentRevision Kind = "deployment_revision"
)

// ValidateIdentity is the shared persisted identity law for event and revision points.
func ValidateIdentity(kind Kind, revision int64, eventID string) error {
	if revision <= 0 {
		return fmt.Errorf("fork point requires a positive revision")
	}
	switch kind {
	case Event:
		if _, err := uuid.Parse(eventID); err != nil {
			return fmt.Errorf("event fork point requires an event UUID: %w", err)
		}
	case DeploymentRevision:
		if eventID != "" {
			return fmt.Errorf("deployment revision fork point forbids event ID")
		}
	default:
		return fmt.Errorf("unsupported fork point kind %q", kind)
	}
	return nil
}
