package semanticview

import "strings"

// PackageRootFlowID resolves dot-qualified connect endpoints to the flow that
// owns the package. The repository root has no owning flow.
func PackageRootFlowID(source Source, packageKey string) (string, bool) {
	packageKey = normalizeImportPackageKey(packageKey)
	if packageKey == "." {
		return "", true
	}
	if source == nil || packageKey == "" {
		return "", false
	}
	for _, project := range source.ProjectScopes() {
		if normalizeImportPackageKey(project.Key) == packageKey {
			return strings.TrimSpace(project.OwningFlowID), strings.TrimSpace(project.OwningFlowID) != ""
		}
	}
	owner := ""
	for _, flow := range source.FlowScopes() {
		if normalizeImportPackageKey(flow.PackageKey) != packageKey || strings.TrimSpace(flow.ID) == "" {
			continue
		}
		if owner != "" && owner != strings.TrimSpace(flow.ID) {
			return "", false
		}
		owner = strings.TrimSpace(flow.ID)
	}
	if owner != "" {
		return owner, true
	}
	return "", false
}
