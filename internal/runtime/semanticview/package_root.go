package semanticview

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

// ResolvedCompositionConnect carries both authored connect evidence and its
// package-root endpoints resolved to the flows that own them.
type ResolvedCompositionConnect struct {
	Connect runtimecontracts.FlowPackageConnect
	From    runtimecontracts.FlowPackagePinRef
	To      runtimecontracts.FlowPackagePinRef
}

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

func resolveCompositionConnectEndpoints(source Source, connect runtimecontracts.FlowPackageConnect) (ResolvedCompositionConnect, bool) {
	if source == nil {
		return ResolvedCompositionConnect{}, false
	}
	from, err := connect.FromRef()
	if err != nil {
		return ResolvedCompositionConnect{}, false
	}
	to, err := connect.ToRef()
	if err != nil {
		return ResolvedCompositionConnect{}, false
	}
	from, ok := resolveCompositionConnectEndpoint(source, connect.PackageKey, from)
	if !ok {
		return ResolvedCompositionConnect{}, false
	}
	to, ok = resolveCompositionConnectEndpoint(source, connect.PackageKey, to)
	if !ok {
		return ResolvedCompositionConnect{}, false
	}
	return ResolvedCompositionConnect{Connect: connect, From: from, To: to}, true
}

// ResolvedCompositionConnectsTo returns connects whose receiver endpoint owns
// the requested flow and pin after package-root resolution.
func ResolvedCompositionConnectsTo(source Source, flowID, pinName string) []ResolvedCompositionConnect {
	flowID = strings.TrimSpace(flowID)
	pinName = strings.TrimSpace(pinName)
	if source == nil || pinName == "" {
		return nil
	}
	var out []ResolvedCompositionConnect
	for _, connect := range source.CompositionConnects() {
		resolved, ok := resolveCompositionConnectEndpoints(source, connect)
		if !ok || resolved.To.Root != (flowID == "") || strings.TrimSpace(resolved.To.FlowID) != flowID || strings.TrimSpace(resolved.To.Pin) != pinName {
			continue
		}
		out = append(out, resolved)
	}
	return out
}

// ResolvedCompositionConnectsFrom returns connects whose source endpoint owns
// the requested flow and pin after package-root resolution.
func ResolvedCompositionConnectsFrom(source Source, flowID, pinName string) []ResolvedCompositionConnect {
	flowID = strings.TrimSpace(flowID)
	pinName = strings.TrimSpace(pinName)
	if source == nil || pinName == "" {
		return nil
	}
	var out []ResolvedCompositionConnect
	for _, connect := range source.CompositionConnects() {
		resolved, ok := resolveCompositionConnectEndpoints(source, connect)
		if !ok || resolved.From.Root != (flowID == "") || strings.TrimSpace(resolved.From.FlowID) != flowID || strings.TrimSpace(resolved.From.Pin) != pinName {
			continue
		}
		out = append(out, resolved)
	}
	return out
}

func resolveCompositionConnectEndpoint(source Source, packageKey string, ref runtimecontracts.FlowPackagePinRef) (runtimecontracts.FlowPackagePinRef, bool) {
	ref.FlowID = strings.TrimSpace(ref.FlowID)
	ref.Pin = strings.TrimSpace(ref.Pin)
	if !ref.Root {
		return ref, ref.FlowID != "" && ref.Pin != ""
	}
	flowID, ok := PackageRootFlowID(source, packageKey)
	if !ok {
		return runtimecontracts.FlowPackagePinRef{}, false
	}
	ref.FlowID = flowID
	ref.Root = flowID == ""
	return ref, ref.Pin != ""
}
