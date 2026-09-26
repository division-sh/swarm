package processbinding

import (
	"fmt"
	"strings"

	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/google/uuid"
)

// Binding seals durable work to the selected-store process and generation grant.
type Binding struct {
	ProcessAuthorityID string `json:"process_authority_id"`
	ProcessOwnerID     string `json:"process_owner_id"`
	ProcessBootID      string `json:"process_boot_id"`
	GenerationGrantID  string `json:"generation_grant_id"`
	BundleHash         string `json:"bundle_hash"`
	RuntimeInstanceID  string `json:"runtime_instance_id"`
	RuntimeGeneration  uint64 `json:"runtime_generation"`
}

func (b Binding) IsZero() bool {
	return strings.TrimSpace(b.ProcessAuthorityID) == "" && strings.TrimSpace(b.ProcessOwnerID) == "" &&
		strings.TrimSpace(b.ProcessBootID) == "" && strings.TrimSpace(b.GenerationGrantID) == "" &&
		strings.TrimSpace(b.BundleHash) == "" && strings.TrimSpace(b.RuntimeInstanceID) == "" && b.RuntimeGeneration == 0
}

func (b Binding) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(b.ProcessAuthorityID)); err != nil {
		return fmt.Errorf("process execution authority is invalid: %w", err)
	}
	if strings.TrimSpace(b.ProcessOwnerID) == "" {
		return fmt.Errorf("process execution owner is required")
	}
	if _, err := uuid.Parse(strings.TrimSpace(b.ProcessBootID)); err != nil {
		return fmt.Errorf("process execution boot is invalid: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(b.GenerationGrantID)); err != nil {
		return fmt.Errorf("process execution generation grant is invalid: %w", err)
	}
	if err := runtimebundleidentity.ValidateCanonicalHash(strings.TrimSpace(b.BundleHash)); err != nil {
		return fmt.Errorf("process execution bundle hash is invalid: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(b.RuntimeInstanceID)); err != nil {
		return fmt.Errorf("process execution runtime instance is invalid: %w", err)
	}
	if b.RuntimeGeneration == 0 {
		return fmt.Errorf("process execution runtime generation is required")
	}
	return nil
}

func (b Binding) Equal(other Binding) bool {
	return strings.TrimSpace(b.ProcessAuthorityID) == strings.TrimSpace(other.ProcessAuthorityID) &&
		strings.TrimSpace(b.ProcessOwnerID) == strings.TrimSpace(other.ProcessOwnerID) &&
		strings.TrimSpace(b.ProcessBootID) == strings.TrimSpace(other.ProcessBootID) &&
		strings.TrimSpace(b.GenerationGrantID) == strings.TrimSpace(other.GenerationGrantID) &&
		strings.TrimSpace(b.BundleHash) == strings.TrimSpace(other.BundleHash) &&
		strings.TrimSpace(b.RuntimeInstanceID) == strings.TrimSpace(other.RuntimeInstanceID) &&
		b.RuntimeGeneration == other.RuntimeGeneration
}
