package publicingress

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/packs"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	runtimeregistration "github.com/division-sh/swarm/internal/runtime/registration"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/google/uuid"
)

type TargetSelector = packs.ChannelRegistrationTarget

func ParseTargetSelector(raw string) (TargetSelector, error) {
	return packs.ParseChannelRegistrationTarget(raw)
}

type RegistrationTarget struct {
	Selector                string
	BundleHash              string
	ServiceID               string
	PackageKey              string
	FlowID                  string
	Alias                   string
	Provider                string
	Generation              int64
	PublicationSequence     int64
	AdmissionPlanGeneration triggergeneration.Generation
	SigningCredentialKey    string
}

type RegistrationPair struct {
	BindingID      string
	PlanGeneration plangeneration.Generation
	Registration   packs.CompiledChannelRegistration
	CredentialKeys map[string]string
	Target         RegistrationTarget
}

type RegistrationControllerOptions struct {
	CredentialOwner   *runtimecredentials.SnapshotOwner
	EffectsStore      runtimeeffects.Store
	HTTP              runtimeregistration.HTTPExecutor
	Posture           executionposture.Posture
	RuntimeInstanceID string
	StartupAuthority  func() (runtimestartupownership.Authority, error)
	Readiness         *ReadinessOwner
	Now               func() time.Time
}

type ProviderRegistrationController struct {
	opts          RegistrationControllerOptions
	reconcileMu   sync.Mutex
	mu            sync.RWMutex
	states        map[string]registrationState
	managedRoutes map[string]struct{}
}

type registrationState struct {
	Pair             RegistrationPair
	BaseFingerprint  string
	IntentID         string
	CallbackToken    string
	CallbackURL      string
	SlotID           string
	Applied          bool
	Matched          bool
	ObservedAt       time.Time
	ExpiresAt        time.Time
	Failure          string
	Authority        runtimeeffects.Authority
	CredentialEpochs map[string]string
}

type admittedPair struct {
	pair     RegistrationPair
	provider map[string]runtimecredentials.AdmittedSnapshot
	signing  runtimecredentials.AdmittedSnapshot
	slotID   string
	base     string
}

func NewProviderRegistrationController(opts RegistrationControllerOptions) (*ProviderRegistrationController, error) {
	if opts.CredentialOwner == nil || opts.EffectsStore == nil || opts.StartupAuthority == nil || opts.Readiness == nil {
		return nil, fmt.Errorf("provider registration controller dependencies are incomplete")
	}
	if !opts.Posture.Valid() {
		return nil, fmt.Errorf("provider registration execution posture is required")
	}
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		return nil, fmt.Errorf("provider registration runtime instance identity is required")
	}
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	controller := &ProviderRegistrationController{opts: opts, states: map[string]registrationState{}, managedRoutes: map[string]struct{}{}}
	opts.Readiness.SetStartupCurrent(func(expectedAuthorityID string) (bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return controller.StartupCurrent(ctx, expectedAuthorityID)
	})
	opts.Readiness.SetRegistrationCurrent(func() (bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return controller.CredentialsCurrent(ctx)
	})
	return controller, nil
}

func (c *ProviderRegistrationController) StartupCurrent(ctx context.Context, expectedAuthorityID string) (bool, error) {
	if c == nil {
		return false, fmt.Errorf("provider registration controller is required")
	}
	startup, err := c.opts.StartupAuthority()
	if err != nil {
		return false, err
	}
	if startup.AuthorityID != strings.TrimSpace(expectedAuthorityID) {
		return false, nil
	}
	authority := serveRegistrationAuthority(startup, uuid.NewString(), c.opts.Posture, c.opts.Now())
	return c.opts.EffectsStore.IsExternalEffectAuthorityCurrent(ctx, authority)
}

func (c *ProviderRegistrationController) CredentialsCurrent(ctx context.Context) (bool, error) {
	if c == nil {
		return false, fmt.Errorf("provider registration controller is required")
	}
	c.mu.RLock()
	states := make([]map[string]string, 0, len(c.states))
	for _, state := range c.states {
		epochs := make(map[string]string, len(state.CredentialEpochs))
		for key, epoch := range state.CredentialEpochs {
			epochs[key] = epoch
		}
		states = append(states, epochs)
	}
	c.mu.RUnlock()
	for _, epochs := range states {
		current, err := c.credentialEpochsCurrent(ctx, epochs)
		if err != nil || !current {
			return current, err
		}
	}
	return true, nil
}

func (c *ProviderRegistrationController) credentialEpochsCurrent(ctx context.Context, epochs map[string]string) (bool, error) {
	if len(epochs) == 0 {
		return false, nil
	}
	for key, expected := range epochs {
		current, err := c.opts.CredentialOwner.Observe(ctx, key)
		if err != nil {
			return false, err
		}
		if current.Epoch() != expected {
			return false, nil
		}
	}
	return true, nil
}

func (c *ProviderRegistrationController) Reconcile(ctx context.Context, exposure Generation, pairs []RegistrationPair) error {
	if c == nil {
		return fmt.Errorf("provider registration controller is required")
	}
	c.reconcileMu.Lock()
	defer c.reconcileMu.Unlock()
	startup, err := c.opts.StartupAuthority()
	if err != nil {
		return err
	}
	if err := startup.Validate(); err != nil {
		return fmt.Errorf("provider registration startup authority: %w", err)
	}
	admitted := make([]admittedPair, 0, len(pairs))
	for _, pair := range pairs {
		c.mu.Lock()
		c.managedRoutes[registrationRouteKey(pair.Target.Alias, pair.Target.Provider)] = struct{}{}
		c.mu.Unlock()
		candidate, err := c.admitAndIdentify(ctx, exposure, pair)
		if err != nil {
			c.recordFailure(pairKey(pair), pair, startup.AuthorityID, err)
			return err
		}
		admitted = append(admitted, candidate)
	}
	if err := rejectSlotCollisions(admitted); err != nil {
		for _, pair := range admitted {
			c.recordFailure(pairKey(pair.pair), pair.pair, startup.AuthorityID, err)
		}
		return err
	}
	keys := make([]string, 0, len(admitted))
	for _, pair := range admitted {
		key := pairKey(pair.pair)
		keys = append(keys, key)
		if err := c.reconcilePair(ctx, exposure, startup, pair); err != nil {
			return err
		}
	}
	c.opts.Readiness.ReplaceRegistrationKeys(keys)
	c.mu.Lock()
	keep := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		keep[key] = struct{}{}
	}
	for key := range c.states {
		if _, ok := keep[key]; !ok {
			delete(c.states, key)
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *ProviderRegistrationController) admitAndIdentify(ctx context.Context, exposure Generation, pair RegistrationPair) (admittedPair, error) {
	key := pairKey(pair)
	if key == "" || !pair.PlanGeneration.Valid() || !pair.Target.AdmissionPlanGeneration.Valid() || strings.TrimSpace(pair.Target.BundleHash) == "" {
		return admittedPair{}, fmt.Errorf("provider registration pair identity is incomplete")
	}
	if pair.Registration.Provider() != strings.TrimSpace(pair.Target.Provider) {
		return admittedPair{}, fmt.Errorf("provider registration pair %s provider conflicts with compiled channel registration", key)
	}
	provider := make(map[string]runtimecredentials.AdmittedSnapshot, len(pair.Registration.ProviderCredentials()))
	credentials := make(map[string]any, len(pair.Registration.ProviderCredentials()))
	for _, logical := range pair.Registration.ProviderCredentials() {
		storeKey := pair.CredentialKeys[logical]
		if storeKey == "" {
			return admittedPair{}, fmt.Errorf("provider registration pair %s credential %q has no explicit store-key mapping", key, logical)
		}
		snapshot, err := c.opts.CredentialOwner.Observe(ctx, storeKey)
		if err != nil {
			return admittedPair{}, err
		}
		if !snapshot.Present {
			return admittedPair{}, fmt.Errorf("provider registration pair %s credential %q is missing", key, storeKey)
		}
		provider[logical] = snapshot
		credentials[logical] = snapshot.CredentialValue()
	}
	signingKey := strings.TrimSpace(pair.Target.SigningCredentialKey)
	signing, err := c.opts.CredentialOwner.Observe(ctx, signingKey)
	if err != nil {
		return admittedPair{}, err
	}
	if !signing.Present {
		return admittedPair{}, fmt.Errorf("provider registration pair %s signing credential %q is missing", key, signingKey)
	}
	identify, err := pair.Registration.Operation(packs.RegistrationOperationIdentify)
	if err != nil {
		return admittedPair{}, err
	}
	input, err := identify.Prepare(map[string]any{})
	if err != nil {
		return admittedPair{}, err
	}
	result, err := c.opts.HTTP.Read(ctx, identify.ToolID(), identify.Tool(), input, credentials)
	if err != nil {
		return admittedPair{}, fmt.Errorf("identify provider registration slot for %s: %w", key, err)
	}
	projected, err := identify.Project(result)
	if err != nil {
		return admittedPair{}, err
	}
	resourceID := strings.TrimSpace(fmt.Sprint(projected["resource_id"]))
	slotID := strings.Join([]string{pair.Registration.Provider(), pair.Registration.SlotNamespace(), resourceID}, ":")
	base, err := registrationBaseFingerprint(exposure, pair, provider, signing, slotID)
	if err != nil {
		return admittedPair{}, err
	}
	return admittedPair{pair: pair, provider: provider, signing: signing, slotID: slotID, base: base}, nil
}

func (c *ProviderRegistrationController) reconcilePair(ctx context.Context, exposure Generation, startup runtimestartupownership.Authority, candidate admittedPair) error {
	key := pairKey(candidate.pair)
	c.mu.RLock()
	current, exists := c.states[key]
	c.mu.RUnlock()
	if exists && current.BaseFingerprint == candidate.base {
		current.Authority = serveRegistrationAuthority(startup, current.IntentID, c.opts.Posture, c.opts.Now())
		return c.refreshReadback(ctx, startup, candidate, current, nil)
	}
	intentID := uuid.NewString()
	token, err := randomToken(32)
	if err != nil {
		return err
	}
	callbackURL, err := CallbackURL(exposure, candidate.pair.Target.Alias, candidate.pair.Target.Provider, token)
	if err != nil {
		return err
	}
	state := registrationState{
		Pair: candidate.pair, BaseFingerprint: candidate.base, IntentID: intentID, CallbackToken: token,
		CallbackURL: callbackURL, SlotID: candidate.slotID, CredentialEpochs: candidateCredentialEpochs(candidate),
	}
	c.publishState(key, startup.AuthorityID, state)
	if err := c.revalidateCredentials(ctx, candidate); err != nil {
		state.Failure = err.Error()
		c.publishState(key, startup.AuthorityID, state)
		return err
	}
	apply, err := candidate.pair.Registration.Operation(packs.RegistrationOperationApply)
	if err != nil {
		return err
	}
	input, err := apply.Prepare(map[string]any{"callback_url": callbackURL})
	if err != nil {
		return err
	}
	credentials := candidateCredentials(candidate)
	authority := serveRegistrationAuthority(startup, intentID, c.opts.Posture, c.opts.Now())
	if !authority.Valid() {
		return fmt.Errorf("serve registration effect authority is invalid")
	}
	state.Authority = authority
	effectController := runtimeeffects.NewController(c.opts.EffectsStore).WithExecutionPosture(c.opts.Posture)
	effectCtx := runtimeeffects.WithController(ctx, effectController)
	effectCtx = runtimeeffects.WithAuthority(effectCtx, authority)
	effectCtx = runtimeeffects.WithExecutionMode(effectCtx, authority.ExecutionMode)
	effectCtx = runtimeauthoractivity.WithScope(effectCtx, runtimeauthoractivity.BundleScope(c.opts.RuntimeInstanceID, candidate.pair.Target.BundleHash))
	result, applyErr := c.opts.HTTP.Apply(effectCtx, apply.ToolID(), apply.Tool(), input, credentials, map[string]string{
		"binding_id": candidate.pair.BindingID, "target": candidate.pair.Target.Selector, "intent_id": intentID, "slot_id": candidate.slotID,
	})
	if applyErr != nil && result.Pending == nil {
		state.Failure = applyErr.Error()
		c.publishState(key, startup.AuthorityID, state)
		return applyErr
	}
	state.Applied = result.Acknowledged
	return c.refreshReadback(effectCtx, startup, candidate, state, result.Pending)
}

func (c *ProviderRegistrationController) refreshReadback(ctx context.Context, startup runtimestartupownership.Authority, candidate admittedPair, state registrationState, pending *runtimeregistration.PendingApply) error {
	current, err := c.opts.EffectsStore.IsExternalEffectAuthorityCurrent(ctx, state.Authority)
	if err != nil || !current {
		if err == nil {
			err = fmt.Errorf("provider registration startup authority is no longer current")
		}
		state.Failure = err.Error()
		state.Matched = false
		c.publishState(pairKey(candidate.pair), startup.AuthorityID, state)
		return err
	}
	readback, err := candidate.pair.Registration.Operation(packs.RegistrationOperationReadback)
	if err != nil {
		return err
	}
	input, err := readback.Prepare(map[string]any{})
	if err != nil {
		return err
	}
	result, readErr := c.opts.HTTP.Read(ctx, readback.ToolID(), readback.Tool(), input, candidateCredentials(candidate))
	exact := false
	if readErr == nil {
		projected, projectErr := readback.Project(result)
		if projectErr != nil {
			readErr = projectErr
		} else {
			exact = subtle.ConstantTimeCompare([]byte(strings.TrimSpace(fmt.Sprint(projected["callback_url"]))), []byte(state.CallbackURL)) == 1
			if !exact {
				readErr = fmt.Errorf("provider callback readback does not match the current registration intent")
			}
		}
	}
	if pending != nil {
		if settleErr := pending.SettleReadback(ctx, exact, readErr); settleErr != nil && !exact {
			readErr = settleErr
		} else if settleErr != nil {
			return settleErr
		}
	}
	now := c.opts.Now().UTC()
	state.Applied = state.Applied || exact
	state.Matched = exact
	state.ObservedAt = now
	state.ExpiresAt = now.Add(EvidenceTTL)
	state.Failure = ""
	if readErr != nil {
		state.Failure = readErr.Error()
	}
	c.publishState(pairKey(candidate.pair), startup.AuthorityID, state)
	return readErr
}

func (c *ProviderRegistrationController) revalidateCredentials(ctx context.Context, candidate admittedPair) error {
	for logical, admitted := range candidate.provider {
		current, err := c.opts.CredentialOwner.Observe(ctx, admitted.Key)
		if err != nil {
			return err
		}
		if current.Epoch() != admitted.Epoch() {
			return fmt.Errorf("provider registration credential %q changed before launch", logical)
		}
	}
	current, err := c.opts.CredentialOwner.Observe(ctx, candidate.signing.Key)
	if err != nil {
		return err
	}
	if current.Epoch() != candidate.signing.Epoch() {
		return fmt.Errorf("provider registration signing credential changed before launch")
	}
	return nil
}

func (c *ProviderRegistrationController) CallbackCurrent(ctx context.Context, alias, provider, token string) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	var authority runtimeeffects.Authority
	var epochs map[string]string
	for _, state := range c.states {
		if state.Pair.Target.Alias != strings.TrimSpace(alias) || state.Pair.Target.Provider != strings.TrimSpace(provider) {
			continue
		}
		if !state.Matched || subtle.ConstantTimeCompare([]byte(state.CallbackToken), []byte(strings.TrimSpace(token))) != 1 {
			continue
		}
		authority = state.Authority
		epochs = make(map[string]string, len(state.CredentialEpochs))
		for key, epoch := range state.CredentialEpochs {
			epochs[key] = epoch
		}
		break
	}
	c.mu.RUnlock()
	if !authority.Valid() {
		return false
	}
	credentialsCurrent, err := c.credentialEpochsCurrent(ctx, epochs)
	if err != nil || !credentialsCurrent {
		return false
	}
	current, err := c.opts.EffectsStore.IsExternalEffectAuthorityCurrent(ctx, authority)
	return err == nil && current
}

func (c *ProviderRegistrationController) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
		if len(parts) != 3 || parts[0] != "webhooks" {
			http.NotFound(response, request)
			return
		}
		c.mu.RLock()
		_, managed := c.managedRoutes[registrationRouteKey(parts[1], parts[2])]
		c.mu.RUnlock()
		if !managed || !c.CallbackCurrent(request.Context(), parts[1], parts[2], request.URL.Query().Get("swarm_callback_generation")) {
			http.NotFound(response, request)
			return
		}
		next.ServeHTTP(response, request)
	})
}

func (c *ProviderRegistrationController) publishState(key, startupAuthorityID string, state registrationState) {
	c.mu.Lock()
	c.states[key] = state
	c.mu.Unlock()
	c.opts.Readiness.SetRegistration(key, RegistrationEvidence{
		BindingID: state.Pair.BindingID, Target: state.Pair.Target.Selector, Provider: state.Pair.Target.Provider, Alias: state.Pair.Target.Alias,
		IntentID: state.IntentID, SlotID: state.SlotID, StartupAuthorityID: startupAuthorityID,
		CallbackURL: state.CallbackURL,
		Applied:     state.Applied, CallbackMatched: state.Matched, ObservedAt: state.ObservedAt, ExpiresAt: state.ExpiresAt, Failure: state.Failure,
	})
}

func (c *ProviderRegistrationController) recordFailure(key string, pair RegistrationPair, startupAuthorityID string, err error) {
	state := registrationState{Pair: pair, Failure: err.Error()}
	c.publishState(key, startupAuthorityID, state)
}

func pairKey(pair RegistrationPair) string {
	binding := strings.TrimSpace(pair.BindingID)
	target := strings.TrimSpace(pair.Target.Selector)
	if binding == "" || target == "" {
		return ""
	}
	return binding + "\x00" + target
}

func registrationRouteKey(alias, provider string) string {
	return strings.TrimSpace(alias) + "\x00" + strings.TrimSpace(provider)
}

func rejectSlotCollisions(pairs []admittedPair) error {
	seen := map[string]string{}
	for _, pair := range pairs {
		key := pairKey(pair.pair)
		if prior, duplicate := seen[pair.slotID]; duplicate {
			return fmt.Errorf("provider registration slot %q is selected by both %s and %s; no provider registration was applied", pair.slotID, prior, key)
		}
		seen[pair.slotID] = key
	}
	return nil
}

func registrationBaseFingerprint(exposure Generation, pair RegistrationPair, provider map[string]runtimecredentials.AdmittedSnapshot, signing runtimecredentials.AdmittedSnapshot, slotID string) (string, error) {
	providerEvidence := map[string]any{}
	keys := make([]string, 0, len(provider))
	for logical := range provider {
		keys = append(keys, logical)
	}
	sort.Strings(keys)
	for _, logical := range keys {
		snapshot := provider[logical]
		providerEvidence[logical] = map[string]any{"key": snapshot.Key, "source": snapshot.Source, "epoch": snapshot.Epoch()}
	}
	raw, err := canonicaljson.Bytes(map[string]any{
		"exposure_generation": exposure.ID,
		"public_origin":       exposure.PublicOrigin,
		"binding_id":          pair.BindingID,
		"target": map[string]any{
			"selector": pair.Target.Selector, "bundle_hash": pair.Target.BundleHash, "service_id": pair.Target.ServiceID,
			"callback_alias": pair.Target.Alias, "callback_provider": pair.Target.Provider,
			"generation": pair.Target.Generation, "publication_sequence": pair.Target.PublicationSequence,
			"admission_plan_generation": pair.Target.AdmissionPlanGeneration.Diagnostic(),
		},
		"plan_generation":      pair.PlanGeneration.Diagnostic(),
		"slot_id":              slotID,
		"provider_credentials": providerEvidence,
		"signing_credential":   map[string]any{"key": signing.Key, "source": signing.Source, "epoch": signing.Epoch()},
	})
	if err != nil {
		return "", err
	}
	return runtimeeffects.Fingerprint(raw), nil
}

func candidateCredentials(candidate admittedPair) map[string]any {
	out := make(map[string]any, len(candidate.provider)+1)
	for logical, snapshot := range candidate.provider {
		out[logical] = snapshot.CredentialValue()
	}
	out[candidate.pair.Registration.SigningCredential()] = candidate.signing.CredentialValue()
	return out
}

func candidateCredentialEpochs(candidate admittedPair) map[string]string {
	out := make(map[string]string, len(candidate.provider)+1)
	for _, snapshot := range candidate.provider {
		out[snapshot.Key] = snapshot.Epoch()
	}
	out[candidate.signing.Key] = candidate.signing.Epoch()
	return out
}

func serveRegistrationAuthority(startup runtimestartupownership.Authority, intentID string, posture executionposture.Posture, now time.Time) runtimeeffects.Authority {
	return runtimeeffects.Authority{
		Kind: runtimeeffects.AuthorityServeRegistration, ID: strings.TrimSpace(intentID),
		ExecutionOwner: startup.OwnerID, LeaseExpiresAt: now.UTC().Add(5 * time.Minute), FenceGeneration: startup.Generation,
		ExecutionMode: runtimeeffects.ExecutionMode(posture.RootMode()),
		ServeRegistration: runtimeeffects.ServeRegistrationAuthority{
			IntentID: strings.TrimSpace(intentID), StartupAuthorityID: startup.AuthorityID, StartupStateVersion: startup.StateVersion,
		},
	}
}
