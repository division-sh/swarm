package runbundle

import (
	"context"
	"errors"
	"strings"
)

// AvailabilityStore separates exact run diagnosis from the generic
// non-standing active-run projection used by startup and admission owners.
type AvailabilityStore interface {
	LoadRunBundleAvailability(context.Context, string) (Availability, error)
	ActiveNonStandingRunBundleAvailabilities(context.Context) ([]Availability, error)
}

const (
	CodeBundleUnavailable        = "BUNDLE_UNAVAILABLE"
	CodeBundleDataIntegrityError = "BUNDLE_DATA_INTEGRITY_ERROR"
)

var (
	ErrRunNotFound    = errors.New("run bundle: run not found")
	ErrBundleNotFound = errors.New("run bundle: bundle not found")
)

type Availability struct {
	RunID                 string
	Status                string
	BundleHash            string
	SourceArtifactPresent bool
	ErrorCode             string
	Cause                 string
}

func (a Availability) Available() bool {
	return a.ErrorCode == "" &&
		a.BundleHash != "" &&
		a.SourceArtifactPresent
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
