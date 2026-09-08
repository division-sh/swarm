package managedcapabilities

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/google/uuid"
)

// PreparedSelectedForkProbeAuthority identifies a non-executable preparation.
// Validation checks evidence shape, not possession: the process owner must prove
// current possession when admitting the probe and when binding its receipt.
type PreparedSelectedForkProbeAuthority struct {
	ProcessAuthorityID       string `json:"process_authority_id"`
	ProcessOwnerID           string `json:"process_owner_id"`
	ProcessBootID            string `json:"process_boot_id"`
	BundleHash               string `json:"bundle_hash"`
	SourceFingerprint        string `json:"source_fingerprint"`
	AdmittedPlanFingerprint  string `json:"admitted_plan_fingerprint"`
	ConfigurationFingerprint string `json:"configuration_fingerprint"`
	CatalogFingerprint       string `json:"catalog_fingerprint"`
	ActorPlanFingerprint     string `json:"actor_plan_fingerprint"`
}

func (p PreparedSelectedForkProbeAuthority) Validate() error {
	for _, coordinate := range []struct{ name, value string }{
		{"process authority", p.ProcessAuthorityID},
		{"process boot", p.ProcessBootID},
	} {
		id, err := uuid.Parse(coordinate.value)
		if err != nil || id == uuid.Nil || id.String() != coordinate.value {
			return fmt.Errorf("prepared selected-fork probe %s must be a canonical nonzero UUID", coordinate.name)
		}
	}
	if p.ProcessOwnerID == "" || strings.TrimSpace(p.ProcessOwnerID) != p.ProcessOwnerID {
		return fmt.Errorf("prepared selected-fork probe process owner is required and must be canonical")
	}
	if err := bundleidentity.ValidateCanonicalHash(p.BundleHash); err != nil {
		return fmt.Errorf("prepared selected-fork probe bundle hash: %w", err)
	}
	for _, fingerprint := range []struct{ name, value string }{
		{"source", p.SourceFingerprint},
		{"admitted plan", p.AdmittedPlanFingerprint},
		{"configuration", p.ConfigurationFingerprint},
		{"catalog", p.CatalogFingerprint},
		{"actor plan", p.ActorPlanFingerprint},
	} {
		decoded, err := hex.DecodeString(fingerprint.value)
		if err != nil || len(decoded) != sha256.Size || strings.ToLower(fingerprint.value) != fingerprint.value {
			return fmt.Errorf("prepared selected-fork probe %s must be a canonical SHA-256 fingerprint", fingerprint.name)
		}
	}
	return nil
}

func (a Authority) clone() Authority {
	if a.Preparation != nil {
		preparation := *a.Preparation
		a.Preparation = &preparation
	}
	return a
}
