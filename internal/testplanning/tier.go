package testplanning

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	ProfileCore      = "core"
	ProfileLifecycle = "lifecycle"
	ProfileFull      = "full"
	VenueLocal       = "local"
	VenueCI          = "ci"
)

func TierRank(tier string) int {
	switch tier {
	case ProfileCore:
		return 1
	case ProfileLifecycle:
		return 2
	case ProfileFull:
		return 3
	default:
		return 0
	}
}

// CITier reads data, not an executable expression or an approval identity.
func CITier(body string) (string, string) {
	var declarations []string
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSuffix(line, "\r")
		if strings.HasPrefix(strings.TrimSpace(line), "CI-Tier") {
			declarations = append(declarations, line)
		}
	}
	if len(declarations) == 0 {
		return ProfileCore, "no CI-Tier declaration; core feedback"
	}
	if len(declarations) != 1 {
		return ProfileFull, fmt.Sprintf("CI-Tier declaration count %d; conservative full", len(declarations))
	}
	for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
		if declarations[0] == "CI-Tier: "+tier {
			return tier, "explicit CI-Tier: " + tier
		}
	}
	return ProfileFull, "malformed CI-Tier; conservative full"
}

func PREventBody(raw []byte) (string, error) {
	var event struct {
		PullRequest *struct {
			Body *string `json:"body"`
		} `json:"pull_request"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return "", fmt.Errorf("decode PR event data: %w", err)
	}
	if event.PullRequest == nil || event.PullRequest.Body == nil {
		return "", nil
	}
	return *event.PullRequest.Body, nil
}
