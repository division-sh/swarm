package eventidentity

import (
	"fmt"
	"strings"
)

// PublicationScope describes business emission only. Protocol and external input
// identities have their own owners and cannot opt into business specialization.
type PublicationScope uint8

const (
	PublicationRoot PublicationScope = iota + 1
	PublicationStatic
	PublicationTemplate
)

// PublicationDeclaration retains declaration identity independently of a running
// instance. A qualified reference must belong to this exact declaration scope.
type PublicationDeclaration struct {
	flow  string
	local string
}

func AdmitPublicationDeclaration(flow, reference string) (PublicationDeclaration, error) {
	if flow != "." && !canonicalPublicationPath(flow) {
		return PublicationDeclaration{}, fmt.Errorf("publication requires an exact declaration flow")
	}
	local := reference
	if flow != "." && strings.HasPrefix(reference, flow+"/") {
		local = strings.TrimPrefix(reference, flow+"/")
	}
	if !IsCanonicalName(local) || strings.ContainsAny(local, "/ \t\r\n") {
		return PublicationDeclaration{}, fmt.Errorf("event %q does not identify a local declaration in flow %q", reference, flow)
	}
	return PublicationDeclaration{flow: flow, local: local}, nil
}

func (d PublicationDeclaration) Flow() string  { return d.flow }
func (d PublicationDeclaration) Local() string { return d.local }

// ProjectPublication computes the single runtime business spelling. Callers
// supply admitted source coordinates; this pure projection grants no routing,
// schema, permission, or receiver authority.
func ProjectPublication(d PublicationDeclaration, scope PublicationScope, sourceFlow, sourceInstance string) (string, error) {
	if d.flow == "" || d.local == "" {
		return "", fmt.Errorf("publication requires an admitted declaration")
	}
	switch scope {
	case PublicationRoot:
		if d.flow != "." || sourceFlow != "." || sourceInstance != "" {
			return "", fmt.Errorf("root publication contradicts its declaration or source")
		}
		return d.local, nil
	case PublicationStatic:
		if d.flow == "." || sourceFlow != d.flow || sourceInstance != d.flow {
			return "", fmt.Errorf("static publication contradicts its declaration or source")
		}
		return d.flow + "/" + d.local, nil
	case PublicationTemplate:
		if d.flow == "." || sourceFlow != d.flow || !canonicalPublicationPath(sourceInstance) || !strings.HasPrefix(sourceInstance, d.flow+"/") {
			return "", fmt.Errorf("template publication requires an exact instance of its declaration")
		}
		return sourceInstance + "/" + d.local, nil
	default:
		return "", fmt.Errorf("publication requires a business execution scope")
	}
}

func canonicalPublicationPath(value string) bool {
	return IsCanonicalName(value) && !strings.ContainsAny(value, " \t\r\n")
}
