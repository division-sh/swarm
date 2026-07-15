package authoractivity

import (
	"context"
	"fmt"
	"strings"
)

type scopeContextKey struct{}

func WithScope(ctx context.Context, scope Scope) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	scope.Kind = ScopeKind(strings.TrimSpace(string(scope.Kind)))
	scope.RuntimeInstanceID = strings.TrimSpace(scope.RuntimeInstanceID)
	scope.BundleHash = strings.TrimSpace(scope.BundleHash)
	return context.WithValue(ctx, scopeContextKey{}, scope)
}

func ScopeFromContext(ctx context.Context) (Scope, bool) {
	if ctx == nil {
		return Scope{}, false
	}
	scope, ok := ctx.Value(scopeContextKey{}).(Scope)
	if !ok {
		return Scope{}, false
	}
	scope.Kind = ScopeKind(strings.TrimSpace(string(scope.Kind)))
	scope.RuntimeInstanceID = strings.TrimSpace(scope.RuntimeInstanceID)
	scope.BundleHash = strings.TrimSpace(scope.BundleHash)
	return scope, scope.Kind != ""
}

// BundleScopeForSource resolves bundle scope only from an already exact scope or
// from the current runtime identity plus a bundle identity owned by the source.
func BundleScopeForSource(ctx context.Context, sourceBundleHash string) (Scope, error) {
	sourceBundleHash = strings.TrimSpace(sourceBundleHash)
	current, ok := ScopeFromContext(ctx)
	if !ok {
		return Scope{}, fmt.Errorf("author activity source requires exact runtime scope in context")
	}
	switch current.Kind {
	case ScopeBundle:
		if current.RuntimeInstanceID == "" || current.BundleHash == "" {
			return Scope{}, fmt.Errorf("author activity source bundle scope requires runtime_instance_id and bundle_hash")
		}
		if sourceBundleHash != "" && sourceBundleHash != current.BundleHash {
			return Scope{}, fmt.Errorf("author activity source bundle_hash %q conflicts with context bundle_hash %q", sourceBundleHash, current.BundleHash)
		}
		return current, nil
	case ScopeRuntime:
		if current.RuntimeInstanceID == "" {
			return Scope{}, fmt.Errorf("author activity source runtime scope requires runtime_instance_id")
		}
		if sourceBundleHash == "" {
			return Scope{}, fmt.Errorf("author activity source requires exact source-owned bundle_hash")
		}
		return BundleScope(current.RuntimeInstanceID, sourceBundleHash), nil
	default:
		return Scope{}, fmt.Errorf("author activity source cannot derive bundle scope from %q scope", current.Kind)
	}
}

// BundleScopeForTarget derives scope for an operation whose canonical target
// bundle may intentionally differ from the caller's current bundle.
func BundleScopeForTarget(ctx context.Context, targetBundleHash string) (Scope, error) {
	targetBundleHash = strings.TrimSpace(targetBundleHash)
	current, ok := ScopeFromContext(ctx)
	if !ok {
		return Scope{}, fmt.Errorf("author activity target requires exact runtime scope in context")
	}
	if current.Kind != ScopeRuntime && current.Kind != ScopeBundle {
		return Scope{}, fmt.Errorf("author activity target cannot derive bundle scope from %q scope", current.Kind)
	}
	if strings.TrimSpace(current.RuntimeInstanceID) == "" {
		return Scope{}, fmt.Errorf("author activity target requires exact runtime scope in context")
	}
	if targetBundleHash == "" {
		return Scope{}, fmt.Errorf("author activity target requires exact target-owned bundle_hash")
	}
	return BundleScope(current.RuntimeInstanceID, targetBundleHash), nil
}

func scopeForDraft(ctx context.Context, kind Kind, transition string, explicit Scope) (Scope, error) {
	contract, ok := kindContracts[kind]
	if !ok {
		return Scope{}, fmt.Errorf("author activity kind %q is not registered", kind)
	}
	want, ok := contract.ScopeByTransition[strings.TrimSpace(transition)]
	if !ok {
		return Scope{}, fmt.Errorf("author activity %s/%s scope policy is not registered", kind, transition)
	}
	if explicit.Kind != "" {
		return explicit, nil
	}
	derived, ok := ScopeFromContext(ctx)
	if !ok {
		return Scope{}, fmt.Errorf("author activity %s/%s requires exact runtime scope in context", kind, transition)
	}
	switch want {
	case ScopeBundle:
		if derived.Kind != ScopeBundle {
			return Scope{}, fmt.Errorf("author activity %s/%s requires exact bundle scope in context", kind, transition)
		}
		return derived, nil
	case ScopeRuntime:
		return RuntimeScope(derived.RuntimeInstanceID), nil
	case ScopeGlobal:
		return Scope{Kind: ScopeGlobal}, nil
	default:
		return Scope{}, fmt.Errorf("author activity %s/%s scope policy %q is not supported", kind, transition, want)
	}
}
