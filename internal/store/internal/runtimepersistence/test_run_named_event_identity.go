package runtimepersistence

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

func validateRunNamedEventObservation(selected any, runID, eventName string) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID || eventName == "" || strings.TrimSpace(eventName) != eventName {
		return fmt.Errorf("run event identity read requires a canonical run and exact event name")
	}
	return nil
}
