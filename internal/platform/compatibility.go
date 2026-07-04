package platform

import (
	"fmt"
	"strings"

	semver "github.com/Masterminds/semver/v3"
)

const CompatibilityConstraintGrammar = "Masterminds semver/v3 constraints"

type CompatibilityErrorKind string

const (
	CompatibilityErrorMissingRange    CompatibilityErrorKind = "missing_range"
	CompatibilityErrorInvalidRange    CompatibilityErrorKind = "invalid_range"
	CompatibilityErrorMissingPlatform CompatibilityErrorKind = "missing_platform_version"
	CompatibilityErrorInvalidPlatform CompatibilityErrorKind = "invalid_platform_version"
	CompatibilityErrorOutOfRange      CompatibilityErrorKind = "out_of_range"
)

type CompatibilityError struct {
	Kind            CompatibilityErrorKind
	DeclaredRange   string
	PlatformVersion string
	Err             error
}

func (e *CompatibilityError) Error() string {
	switch e.Kind {
	case CompatibilityErrorMissingRange:
		return fmt.Sprintf("platform_version missing; declare a %s range compatible with running platform %q", CompatibilityConstraintGrammar, e.PlatformVersion)
	case CompatibilityErrorInvalidRange:
		return fmt.Sprintf("platform_version range %q is not valid %s: %v", e.DeclaredRange, CompatibilityConstraintGrammar, e.Err)
	case CompatibilityErrorMissingPlatform:
		return "running platform.version missing"
	case CompatibilityErrorInvalidPlatform:
		return fmt.Sprintf("running platform.version %q is not valid semver: %v", e.PlatformVersion, e.Err)
	case CompatibilityErrorOutOfRange:
		return fmt.Sprintf("platform_version range %q does not include running platform %q", e.DeclaredRange, e.PlatformVersion)
	default:
		if e.Err != nil {
			return e.Err.Error()
		}
		return "platform version compatibility failed"
	}
}

func (e *CompatibilityError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func ValidateProductPlatformVersion(declaredRange, platformVersion string) error {
	declaredRange = strings.TrimSpace(declaredRange)
	platformVersion = strings.TrimSpace(platformVersion)
	if platformVersion == "" {
		return &CompatibilityError{Kind: CompatibilityErrorMissingPlatform}
	}
	version, err := semver.NewVersion(platformVersion)
	if err != nil {
		return &CompatibilityError{Kind: CompatibilityErrorInvalidPlatform, PlatformVersion: platformVersion, Err: err}
	}
	if declaredRange == "" {
		return &CompatibilityError{Kind: CompatibilityErrorMissingRange, PlatformVersion: platformVersion}
	}
	constraint, err := semver.NewConstraint(declaredRange)
	if err != nil {
		return &CompatibilityError{Kind: CompatibilityErrorInvalidRange, DeclaredRange: declaredRange, PlatformVersion: platformVersion, Err: err}
	}
	if !constraint.Check(version) {
		return &CompatibilityError{Kind: CompatibilityErrorOutOfRange, DeclaredRange: declaredRange, PlatformVersion: platformVersion}
	}
	return nil
}
