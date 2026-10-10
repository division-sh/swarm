package testplanning

import (
	"fmt"
	"strings"
)

func validateEnvironmentDeclaration(fixed string, venues map[string]string) error {
	if (fixed == "") == (len(venues) == 0) {
		return fmt.Errorf("declare exactly one fixed environment_id or venue-specific environment_ids")
	}
	if fixed != "" && strings.TrimSpace(fixed) != fixed {
		return fmt.Errorf("environment_id must be canonical and non-empty")
	}
	for venue, id := range venues {
		if venue != VenueCI && venue != VenueLocal {
			return fmt.Errorf("environment_ids has unsupported venue %q", venue)
		}
		if id == "" || strings.TrimSpace(id) != id {
			return fmt.Errorf("environment_ids.%s must be canonical and non-empty", venue)
		}
	}
	return nil
}

// A fixed recipe is venue-invariant. Venue-specific recipes must name the
// selected venue explicitly; neither unit names nor receipt aliases resolve it.
func environmentForVenue(fixed string, venues map[string]string, venue string) (string, error) {
	if venue != VenueCI && venue != VenueLocal {
		return "", fmt.Errorf("unsupported execution venue %q", venue)
	}
	if err := validateEnvironmentDeclaration(fixed, venues); err != nil {
		return "", err
	}
	if fixed != "" {
		return fixed, nil
	}
	id, ok := venues[venue]
	if !ok {
		return "", fmt.Errorf("environment_ids has no recipe for venue %q", venue)
	}
	return id, nil
}
