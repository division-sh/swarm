package publicingress

import (
	"context"
	"crypto/subtle"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

const (
	RenewalInterval = 30 * time.Second
	EvidenceTTL     = 90 * time.Second
)

type ExposureEvidence struct {
	GenerationID       string    `json:"generation_id"`
	Mode               string    `json:"mode"`
	PublicOrigin       string    `json:"public_origin"`
	ListenAddress      string    `json:"listen_address"`
	StartupAuthorityID string    `json:"startup_authority_id"`
	ObservedAt         time.Time `json:"observed_at"`
	ExpiresAt          time.Time `json:"expires_at"`
	Failure            string    `json:"failure,omitempty"`
}

type RegistrationEvidence struct {
	BindingID                   string    `json:"binding_id"`
	Target                      string    `json:"target"`
	Provider                    string    `json:"provider"`
	Alias                       string    `json:"alias"`
	IntentID                    string    `json:"intent_id"`
	SlotID                      string    `json:"slot_id"`
	CallbackURL                 string    `json:"callback_url"`
	StartupAuthorityID          string    `json:"startup_authority_id"`
	Phase                       string    `json:"phase"`
	Applied                     bool      `json:"applied"`
	CallbackMatched             bool      `json:"callback_matched"`
	ObservedAt                  time.Time `json:"observed_at"`
	ExpiresAt                   time.Time `json:"expires_at"`
	Failure                     string    `json:"failure,omitempty"`
	ChannelActivationGeneration string    `json:"channel_activation_generation,omitempty"`
}

type Snapshot struct {
	RuntimeReady         bool                   `json:"runtime_ready"`
	PublicIngressEnabled bool                   `json:"public_ingress_enabled"`
	PublicIngressReady   bool                   `json:"public_ingress_ready"`
	Ready                bool                   `json:"ready"`
	Exposure             *ExposureEvidence      `json:"exposure,omitempty"`
	Registrations        []RegistrationEvidence `json:"registrations,omitempty"`
	Failure              string                 `json:"failure,omitempty"`
}

type ChannelRegistrationCurrentness struct {
	Exposure             *ExposureEvidence                             `json:"exposure,omitempty"`
	Registration         RegistrationEvidence                          `json:"registration"`
	ActivationGeneration channelonboarding.ChannelActivationGeneration `json:"-"`
	Current              bool                                          `json:"current"`
}

// registrationProcessSnapshot is replaced as one value. Its maps and states
// are cloned before publication so readers never observe partial selection,
// replacement, or revocation.
type registrationProcessSnapshot struct {
	revision      uint64
	exposure      *ExposureEvidence
	registrations map[string]registrationState
	routes        map[string]struct{}
	failure       string
}

type RegistrationSnapshotOwner struct {
	mu       sync.RWMutex
	snapshot registrationProcessSnapshot
}

func newRegistrationSnapshotOwner() *RegistrationSnapshotOwner {
	return &RegistrationSnapshotOwner{snapshot: registrationProcessSnapshot{
		registrations: map[string]registrationState{},
		routes:        map[string]struct{}{},
	}}
}

func (o *RegistrationSnapshotOwner) capture() registrationProcessSnapshot {
	if o == nil {
		return registrationProcessSnapshot{registrations: map[string]registrationState{}, routes: map[string]struct{}{}}
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return cloneRegistrationProcessSnapshot(o.snapshot)
}

func (o *RegistrationSnapshotOwner) currentRevision(revision uint64) bool {
	if o == nil {
		return false
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.snapshot.revision == revision
}

func (o *RegistrationSnapshotOwner) mutate(change func(*registrationProcessSnapshot)) {
	if o == nil {
		return
	}
	o.mu.Lock()
	next := cloneRegistrationProcessSnapshot(o.snapshot)
	change(&next)
	next.revision = o.snapshot.revision + 1
	o.snapshot = next
	o.mu.Unlock()
}

func (o *RegistrationSnapshotOwner) setExposure(evidence ExposureEvidence) {
	o.mutate(func(next *registrationProcessSnapshot) {
		copy := evidence
		next.exposure = &copy
		next.failure = strings.TrimSpace(evidence.Failure)
	})
}

func (o *RegistrationSnapshotOwner) revokeExposure(cause string) {
	o.mutate(func(next *registrationProcessSnapshot) {
		next.exposure = nil
		next.failure = strings.TrimSpace(cause)
	})
}

func (o *RegistrationSnapshotOwner) replaceSelected(pairs []RegistrationPair) {
	o.mutate(func(next *registrationProcessSnapshot) {
		selected := make(map[string]registrationState, len(pairs))
		routes := make(map[string]struct{}, len(pairs))
		for _, pair := range pairs {
			key := pairKey(pair)
			state := registrationState{Pair: pair, Phase: registrationPhaseNoAttempt}
			if prior, ok := next.registrations[key]; ok {
				state = cloneRegistrationState(prior)
				state.Pair = pair
				if !sameRegistrationSelection(prior.Pair, pair) {
					state.SelectedBase = ""
					state.Failure = "provider registration selection changed and is pending reconciliation"
				}
			}
			selected[key] = state
			routes[registrationRouteKey(pair.Target.Alias, pair.Target.Provider)] = struct{}{}
		}
		next.registrations = selected
		next.routes = routes
	})
}

func sameRegistrationSelection(left, right RegistrationPair) bool {
	return left.BindingID == right.BindingID &&
		left.PlanGeneration.Diagnostic() == right.PlanGeneration.Diagnostic() &&
		left.ChannelActivationGeneration.Diagnostic() == right.ChannelActivationGeneration.Diagnostic() &&
		strings.TrimSpace(left.PrebindingOperationID) == strings.TrimSpace(right.PrebindingOperationID) &&
		maps.Equal(left.CredentialKeys, right.CredentialKeys) &&
		left.Target.Selector == right.Target.Selector &&
		left.Target.BundleHash == right.Target.BundleHash &&
		left.Target.ServiceID == right.Target.ServiceID &&
		left.Target.PackageKey == right.Target.PackageKey &&
		left.Target.FlowID == right.Target.FlowID &&
		left.Target.Alias == right.Target.Alias &&
		left.Target.Provider == right.Target.Provider &&
		left.Target.Generation == right.Target.Generation &&
		left.Target.PublicationSequence == right.Target.PublicationSequence &&
		left.Target.AdmissionPlanGeneration.Diagnostic() == right.Target.AdmissionPlanGeneration.Diagnostic() &&
		left.Target.SigningCredentialKey == right.Target.SigningCredentialKey
}

func (o *RegistrationSnapshotOwner) state(key string) (registrationState, bool) {
	snapshot := o.capture()
	state, ok := snapshot.registrations[strings.TrimSpace(key)]
	return cloneRegistrationState(state), ok
}

func (o *RegistrationSnapshotOwner) publishState(key string, state registrationState) {
	o.mutate(func(next *registrationProcessSnapshot) {
		if _, selected := next.registrations[strings.TrimSpace(key)]; !selected {
			return
		}
		next.registrations[strings.TrimSpace(key)] = cloneRegistrationState(state)
	})
}

func (o *RegistrationSnapshotOwner) routeSelected(alias, provider string) bool {
	snapshot := o.capture()
	_, ok := snapshot.routes[registrationRouteKey(alias, provider)]
	return ok
}

func cloneRegistrationProcessSnapshot(source registrationProcessSnapshot) registrationProcessSnapshot {
	out := registrationProcessSnapshot{
		revision:      source.revision,
		registrations: make(map[string]registrationState, len(source.registrations)),
		routes:        make(map[string]struct{}, len(source.routes)),
		failure:       source.failure,
	}
	if source.exposure != nil {
		copy := *source.exposure
		out.exposure = &copy
	}
	for key, state := range source.registrations {
		out.registrations[key] = cloneRegistrationState(state)
	}
	for route := range source.routes {
		out.routes[route] = struct{}{}
	}
	return out
}

type ReadinessOwner struct {
	mu                      sync.RWMutex
	runtimeReady            bool
	enabled                 bool
	registration            *RegistrationSnapshotOwner
	startupCurrent          func(context.Context, string) (bool, error)
	credentialEpochsCurrent func(context.Context, map[string]string) (bool, error)
	effectAuthorityCurrent  func(context.Context, runtimeeffects.Authority) (bool, error)
}

func NewReadinessOwner(enabled bool) *ReadinessOwner {
	return &ReadinessOwner{enabled: enabled, registration: newRegistrationSnapshotOwner()}
}

func (o *ReadinessOwner) SetRuntimeReady(ready bool) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.runtimeReady = ready
	o.mu.Unlock()
}

func (o *ReadinessOwner) Store(ready bool) {
	o.SetRuntimeReady(ready)
}

func (o *ReadinessOwner) Load() bool {
	return o.Snapshot(time.Now().UTC()).Ready
}

func (o *ReadinessOwner) RuntimeLoad() bool {
	if o == nil {
		return false
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.runtimeReady
}

func (o *ReadinessOwner) SetCurrentnessChecks(
	startup func(context.Context, string) (bool, error),
	credentials func(context.Context, map[string]string) (bool, error),
	effect func(context.Context, runtimeeffects.Authority) (bool, error),
) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.startupCurrent = startup
	o.credentialEpochsCurrent = credentials
	o.effectAuthorityCurrent = effect
	o.mu.Unlock()
}

func (o *ReadinessOwner) SetExposure(evidence ExposureEvidence) {
	if o != nil {
		o.registration.setExposure(evidence)
	}
}

func (o *ReadinessOwner) RevokeExposure(cause string) {
	if o != nil {
		o.registration.revokeExposure(cause)
	}
}

type evaluatedRegistrationSnapshot struct {
	snapshot registrationProcessSnapshot
	output   Snapshot
	current  map[string]bool
}

func (o *ReadinessOwner) evaluate(ctx context.Context, now time.Time) evaluatedRegistrationSnapshot {
	if o == nil {
		return evaluatedRegistrationSnapshot{}
	}
	now = now.UTC()
	o.mu.RLock()
	output := Snapshot{RuntimeReady: o.runtimeReady, PublicIngressEnabled: o.enabled}
	startupCurrent := o.startupCurrent
	credentialsCurrent := o.credentialEpochsCurrent
	effectCurrent := o.effectAuthorityCurrent
	o.mu.RUnlock()

	process := o.registration.capture()
	output.Failure = process.failure
	exposureReady := !o.enabled
	if process.exposure != nil {
		copy := *process.exposure
		output.Exposure = &copy
		exposureReady = copy.Failure == "" && now.Before(copy.ExpiresAt)
	}
	if exposureReady && output.Exposure != nil && startupCurrent != nil {
		current, err := startupCurrent(ctx, output.Exposure.StartupAuthorityID)
		if err != nil || !current {
			exposureReady = false
			if err != nil {
				output.Failure = err.Error()
			} else {
				output.Failure = "public ingress startup authority is no longer current"
			}
		}
	}

	keys := make([]string, 0, len(process.registrations))
	for key := range process.registrations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	registrationsReady := true
	currentByKey := make(map[string]bool, len(keys))
	for _, key := range keys {
		state := process.registrations[key]
		current, failure := registrationStateCurrent(ctx, now, state, process.exposure, exposureReady, credentialsCurrent, effectCurrent)
		currentByKey[key] = current
		if !current {
			registrationsReady = false
		}
		evidence := state.evidence(current)
		if evidence.Failure == "" {
			evidence.Failure = failure
		}
		if output.Failure == "" && evidence.Failure != "" {
			output.Failure = evidence.Failure
		}
		output.Registrations = append(output.Registrations, evidence)
	}
	if !o.registration.currentRevision(process.revision) {
		exposureReady = false
		registrationsReady = false
		output.Failure = "public ingress registration snapshot changed during currentness evaluation"
		for key := range currentByKey {
			currentByKey[key] = false
		}
	}
	output.PublicIngressReady = exposureReady && registrationsReady
	output.Ready = output.RuntimeReady && output.PublicIngressReady
	return evaluatedRegistrationSnapshot{snapshot: process, output: output, current: currentByKey}
}

func registrationStateCurrent(
	ctx context.Context,
	now time.Time,
	state registrationState,
	exposure *ExposureEvidence,
	exposureReady bool,
	credentialsCurrent func(context.Context, map[string]string) (bool, error),
	effectCurrent func(context.Context, runtimeeffects.Authority) (bool, error),
) (bool, string) {
	if state.Failure != "" {
		return false, state.Failure
	}
	if !exposureReady || exposure == nil {
		return false, "public exposure is not current"
	}
	if state.Phase != registrationPhaseVerified || state.LastVerified == nil || state.SelectedBase == "" || state.LastVerified.BaseFingerprint != state.SelectedBase {
		return false, "provider registration is not verified"
	}
	intent := state.LastVerified
	if intent.ExposureGenerationID != exposure.GenerationID || intent.Authority.ServeRegistration.StartupAuthorityID != exposure.StartupAuthorityID {
		return false, "provider registration exposure or startup authority is no longer current"
	}
	if !now.Before(intent.ExpiresAt) {
		return false, "provider registration evidence expired"
	}
	if credentialsCurrent != nil {
		current, err := credentialsCurrent(ctx, intent.CredentialEpochs)
		if err != nil {
			return false, err.Error()
		}
		if !current {
			return false, "public ingress credential snapshots are no longer current"
		}
	}
	if effectCurrent != nil {
		current, err := effectCurrent(ctx, intent.Authority)
		if err != nil {
			return false, err.Error()
		}
		if !current {
			return false, "provider registration effect authority is no longer current"
		}
	}
	return true, ""
}

func (o *ReadinessOwner) Snapshot(now time.Time) Snapshot {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return o.evaluate(ctx, now).output
}

// ChannelRegistrationCurrent resolves one exact connected-channel
// registration from the same evaluated snapshot used by ingress readiness.
func (o *ReadinessOwner) ChannelRegistrationCurrent(ctx context.Context, now time.Time, bindingID, target, provider string) (ChannelRegistrationCurrentness, bool) {
	if o == nil {
		return ChannelRegistrationCurrentness{}, false
	}
	bindingID = strings.TrimSpace(bindingID)
	target = strings.TrimSpace(target)
	provider = strings.TrimSpace(provider)
	evaluated := o.evaluate(ctx, now)
	var result ChannelRegistrationCurrentness
	found := false
	for key, state := range evaluated.snapshot.registrations {
		if state.Pair.BindingID != bindingID || state.Pair.Target.Selector != target || state.Pair.Target.Provider != provider {
			continue
		}
		if found {
			return ChannelRegistrationCurrentness{}, false
		}
		found = true
		result.Registration = state.evidence(evaluated.current[key])
		result.ActivationGeneration = state.Pair.ChannelActivationGeneration
		result.Current = evaluated.current[key]
	}
	if !found {
		return ChannelRegistrationCurrentness{}, false
	}
	if evaluated.output.Exposure != nil {
		copy := *evaluated.output.Exposure
		result.Exposure = &copy
	}
	return result, o.registration.currentRevision(evaluated.snapshot.revision)
}

func (o *ReadinessOwner) CallbackCurrent(ctx context.Context, now time.Time, alias, provider, token string) bool {
	_, current := o.CallbackTargetCurrent(ctx, now, alias, provider, token)
	return current
}

// CallbackTargetCurrent returns the exact selected registration target only
// when the callback token and every registration currentness fence still hold.
func (o *ReadinessOwner) CallbackTargetCurrent(ctx context.Context, now time.Time, alias, provider, token string) (RegistrationTarget, bool) {
	if o == nil {
		return RegistrationTarget{}, false
	}
	evaluated := o.evaluate(ctx, now)
	alias = strings.TrimSpace(alias)
	provider = strings.TrimSpace(provider)
	token = strings.TrimSpace(token)
	var selected RegistrationTarget
	found := false
	for key, state := range evaluated.snapshot.registrations {
		if !evaluated.current[key] || state.Pair.Target.Alias != alias || state.Pair.Target.Provider != provider || state.LastVerified == nil {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(state.LastVerified.CallbackToken), []byte(token)) == 1 {
			if found {
				return RegistrationTarget{}, false
			}
			selected = state.Pair.Target
			found = true
		}
	}
	if !found || !o.registration.currentRevision(evaluated.snapshot.revision) {
		return RegistrationTarget{}, false
	}
	return selected, true
}
