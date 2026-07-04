package contracts

import (
	"fmt"
	"strings"

	runtimeplatform "github.com/division-sh/swarm/internal/platform"
)

type PlatformVersionCompatibilityViolation struct {
	PackageKey      string
	PackageName     string
	DeclaredRange   string
	PlatformVersion string
	Err             error
}

func BundlePlatformVersionCompatibilityViolations(bundle *WorkflowContractBundle) []PlatformVersionCompatibilityViolation {
	runningVersion := ""
	if bundle != nil {
		runningVersion = strings.TrimSpace(bundle.Platform.Platform.Version)
	}
	return BundlePlatformVersionCompatibilityViolationsForPlatformVersion(bundle, runningVersion)
}

func BundlePlatformVersionCompatibilityViolationsForPlatformVersion(bundle *WorkflowContractBundle, runningVersion string) []PlatformVersionCompatibilityViolation {
	if bundle == nil {
		return []PlatformVersionCompatibilityViolation{{
			PackageKey: ".",
			Err:        fmt.Errorf("workflow contract bundle is required"),
		}}
	}
	runningVersion = strings.TrimSpace(runningVersion)
	violations := make([]PlatformVersionCompatibilityViolation, 0)
	for _, pkg := range bundle.PackageTree {
		packageKey := strings.TrimSpace(pkg.Key)
		if packageKey == "" {
			packageKey = "."
		}
		declaredRange := strings.TrimSpace(pkg.Manifest.PlatformVersion)
		if err := runtimeplatform.ValidateProductPlatformVersion(declaredRange, runningVersion); err != nil {
			violations = append(violations, PlatformVersionCompatibilityViolation{
				PackageKey:      packageKey,
				PackageName:     strings.TrimSpace(pkg.Manifest.Name),
				DeclaredRange:   declaredRange,
				PlatformVersion: runningVersion,
				Err:             err,
			})
		}
	}
	return violations
}

func ValidateBundlePlatformVersionCompatibility(bundle *WorkflowContractBundle) error {
	violations := BundlePlatformVersionCompatibilityViolations(bundle)
	return platformVersionCompatibilityError(violations)
}

func ValidateBundlePlatformVersionCompatibilityForPlatformVersion(bundle *WorkflowContractBundle, runningVersion string) error {
	violations := BundlePlatformVersionCompatibilityViolationsForPlatformVersion(bundle, runningVersion)
	return platformVersionCompatibilityError(violations)
}

func platformVersionCompatibilityError(violations []PlatformVersionCompatibilityViolation) error {
	if len(violations) == 0 {
		return nil
	}
	lines := make([]string, 0, len(violations))
	for _, violation := range violations {
		lines = append(lines, violation.Message())
	}
	return fmt.Errorf("platform version compatibility failed:\n%s", strings.Join(lines, "\n"))
}

func (v PlatformVersionCompatibilityViolation) Location() string {
	if strings.TrimSpace(v.PackageKey) == "." || strings.TrimSpace(v.PackageKey) == "" {
		return "package.yaml"
	}
	return fmt.Sprintf("%s/package.yaml", strings.TrimSpace(v.PackageKey))
}

func (v PlatformVersionCompatibilityViolation) Message() string {
	detail := "platform version compatibility failed"
	if v.Err != nil {
		detail = strings.TrimSpace(v.Err.Error())
	}
	runningVersion := strings.TrimSpace(v.PlatformVersion)
	remediation := fmt.Sprintf("remediation: update package.yaml platform_version after re-verifying against platform %s, or use a Swarm binary whose embedded platform.version satisfies the declared range", runningVersion)
	if runningVersion == "" {
		remediation = "remediation: fix the embedded platform-spec.yaml platform.version before admitting product packages"
	}
	return fmt.Sprintf("package %s declares platform_version %q but running platform.version is %q: %s; %s", packageLabel(v.PackageKey, v.PackageName), strings.TrimSpace(v.DeclaredRange), runningVersion, detail, remediation)
}

func packageLabel(packageKey, packageName string) string {
	packageKey = strings.TrimSpace(packageKey)
	if packageKey == "" {
		packageKey = "."
	}
	packageName = strings.TrimSpace(packageName)
	if packageName == "" {
		return packageKey
	}
	return fmt.Sprintf("%s (%s)", packageKey, packageName)
}
