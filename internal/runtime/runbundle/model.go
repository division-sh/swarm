package runbundle

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// RuntimeCatalogReader loads the persisted source required to boot a selected
// bundle without exposing the selected backend.
type RuntimeCatalogReader interface {
	LoadBundleCatalogRuntimeRecord(context.Context, string) (BundleCatalogRuntimeRecord, error)
}

// AvailabilityStore is the complete availability projection consumed by
// runtime selection, startup recovery, and fixed-bundle admission.
type AvailabilityStore interface {
	LoadRunBundleAvailability(context.Context, string) (Availability, error)
	ActiveRunBundleAvailabilities(context.Context) ([]Availability, error)
	ActiveRunBundleAvailabilityConflicts(context.Context) ([]Availability, error)
}

const (
	CodeBundleUnavailable        = "BUNDLE_UNAVAILABLE"
	CodeBundleDataIntegrityError = "BUNDLE_DATA_INTEGRITY_ERROR"
)

var (
	ErrRunNotFound    = errors.New("run bundle: run not found")
	ErrBundleNotFound = errors.New("run bundle: bundle not found")
)

type AvailabilitySource uint8

const (
	AvailabilitySourcePersisted AvailabilitySource = iota + 1
	AvailabilitySourceEphemeral
	AvailabilitySourceDeleted
)

func DecodeAvailabilitySource(raw string) (AvailabilitySource, error) {
	if raw != strings.TrimSpace(raw) {
		return 0, fmt.Errorf("bundle_source must not contain surrounding whitespace")
	}
	switch raw {
	case "persisted":
		return AvailabilitySourcePersisted, nil
	case "ephemeral":
		return AvailabilitySourceEphemeral, nil
	case "deleted":
		return AvailabilitySourceDeleted, nil
	default:
		return 0, fmt.Errorf("bundle_source must be persisted, ephemeral, or deleted")
	}
}

func (s AvailabilitySource) String() string {
	switch s {
	case AvailabilitySourcePersisted:
		return "persisted"
	case AvailabilitySourceEphemeral:
		return "ephemeral"
	case AvailabilitySourceDeleted:
		return "deleted"
	default:
		return ""
	}
}

func (s AvailabilitySource) IsPersisted() bool {
	return s == AvailabilitySourcePersisted
}

func (s AvailabilitySource) IsEphemeral() bool {
	return s == AvailabilitySourceEphemeral
}

func (s AvailabilitySource) IsDeleted() bool {
	return s == AvailabilitySourceDeleted
}

type Availability struct {
	RunID            string
	Status           string
	BundleHash       string
	BundleSource     AvailabilitySource
	BundleRowPresent bool
	ErrorCode        string
	Cause            string
}

func (a Availability) Available() bool {
	return a.ErrorCode == "" &&
		a.BundleSource.IsPersisted() &&
		a.BundleHash != "" &&
		a.BundleRowPresent
}

func (a Availability) Unavailable() bool {
	return a.ErrorCode == CodeBundleUnavailable
}

func (a Availability) DataIntegrityError() bool {
	return a.ErrorCode == CodeBundleDataIntegrityError
}

func (a Availability) DetailString() string {
	parts := []string{
		"run_id=" + strings.TrimSpace(a.RunID),
		"status=" + strings.TrimSpace(a.Status),
		"bundle_source=" + a.BundleSource.String(),
	}
	if a.BundleHash != "" {
		parts = append(parts, "bundle_hash="+a.BundleHash)
	}
	if a.ErrorCode != "" {
		parts = append(parts, "code="+a.ErrorCode)
	}
	if a.Cause != "" {
		parts = append(parts, "cause="+a.Cause)
	}
	return strings.Join(parts, " ")
}

type BundleCatalogRuntimeRecord struct {
	BundleHash  string
	ContentYAML string
	DataBlob    []byte
}
