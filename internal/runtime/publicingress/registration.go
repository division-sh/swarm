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
	opts        RegistrationControllerOptions
	reconcileMu sync.Mutex
	snapshot    *RegistrationSnapshotOwner
}

type registrationPhase string

const (
	registrationPhaseNoAttempt         registrationPhase = "no_attempt"
	registrationPhasePrelaunch         registrationPhase = "pre_launch"
	registrationPhasePendingSettlement registrationPhase = "pending_settlement"
	registrationPhaseVerified          registrationPhase = "verified"
	registrationPhaseOutcomeUncertain  registrationPhase = "outcome_uncertain"
)

type registrationState struct {
	Pair         RegistrationPair
	SelectedBase string
	LastVerified *registrationIntent
	Attempt      *registrationAttempt
	Terminal     *registrationIntent
	Phase        registrationPhase
	Failure      string
}

type registrationIntent struct {
	BaseFingerprint      string
	ExposureGenerationID string
	IntentID             string
	CallbackToken        string
	CallbackURL          string
	SlotID               string
	ObservedAt           time.Time
	ExpiresAt            time.Time
	Authority            runtimeeffects.Authority
	CredentialEpochs     map[string]string
	Applied              bool
	Matched              bool
	EffectOperationID    string
	EffectAttemptID      string
	EffectAttemptOrdinal int
	Pending              runtimeregistration.PendingApply
	HasPending           bool
}

type registrationAttempt struct {
	Intent registrationIntent
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
	controller := &ProviderRegistrationController{opts: opts, snapshot: opts.Readiness.registration}
	opts.Readiness.SetCurrentnessChecks(
		controller.StartupCurrent,
		controller.credentialEpochsCurrent,
		opts.EffectsStore.IsExternalEffectAuthorityCurrent,
	)
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
	c.snapshot.replaceSelected(pairs)
	admitted := make([]admittedPair, 0, len(pairs))
	for _, pair := range pairs {
		candidate, err := c.admitAndIdentify(ctx, exposure, pair)
		if err != nil {
			c.recordFailure(pairKey(pair), pair, err)
			return err
		}
		admitted = append(admitted, candidate)
	}
	if err := rejectSlotCollisions(admitted); err != nil {
		for _, pair := range admitted {
			c.recordFailure(pairKey(pair.pair), pair.pair, err)
		}
		return err
	}
	for _, pair := range admitted {
		if err := c.reconcilePair(ctx, exposure, startup, pair); err != nil {
			return err
		}
	}
	return nil
}

// PrepareStartupHandoff settles predecessor-owned registration attempts and
// prevents renewal from opening another attempt until the caller releases it.
func (c *ProviderRegistrationController) PrepareStartupHandoff(ctx context.Context) (func(), error) {
	if c == nil {
		return nil, fmt.Errorf("provider registration controller is required")
	}
	c.reconcileMu.Lock()
	if err := c.prepareStartupHandoffLocked(ctx); err != nil {
		c.reconcileMu.Unlock()
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(c.reconcileMu.Unlock)
	}, nil
}

func (c *ProviderRegistrationController) prepareStartupHandoffLocked(ctx context.Context) error {
	snapshot := c.snapshot.capture()
	keys := make([]string, 0, len(snapshot.registrations))
	for key := range snapshot.registrations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		state := snapshot.registrations[key]
		switch state.Phase {
		case registrationPhaseNoAttempt, registrationPhaseVerified, registrationPhaseOutcomeUncertain:
			continue
		case registrationPhasePrelaunch:
			return fmt.Errorf("provider registration %s has an unresolved pre-launch attempt; startup handoff is refused", key)
		case registrationPhasePendingSettlement:
			candidate, err := c.admitReadbackCandidate(ctx, state)
			if err != nil {
				if settleErr := c.terminalizePendingReadback(ctx, key, state, err); settleErr != nil {
					return fmt.Errorf("terminalize provider registration %s before startup handoff: %w", key, settleErr)
				}
				continue
			}
			if err := c.refreshReadback(ctx, candidate, state, true); err != nil {
				settled, _ := c.snapshot.state(key)
				if settled.Phase != registrationPhaseOutcomeUncertain {
					return fmt.Errorf("settle provider registration %s before startup handoff: %w", key, err)
				}
			}
		default:
			return fmt.Errorf("provider registration %s has unsupported lifecycle phase %q", key, state.Phase)
		}
	}
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
	provider, signing, err := c.admitCredentials(ctx, pair)
	if err != nil {
		return admittedPair{}, err
	}
	credentials := make(map[string]any, len(provider))
	for logical, snapshot := range provider {
		credentials[logical] = snapshot.CredentialValue()
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

func (c *ProviderRegistrationController) admitCredentials(ctx context.Context, pair RegistrationPair) (map[string]runtimecredentials.AdmittedSnapshot, runtimecredentials.AdmittedSnapshot, error) {
	key := pairKey(pair)
	provider := make(map[string]runtimecredentials.AdmittedSnapshot, len(pair.Registration.ProviderCredentials()))
	for _, logical := range pair.Registration.ProviderCredentials() {
		storeKey := pair.CredentialKeys[logical]
		if storeKey == "" {
			return nil, runtimecredentials.AdmittedSnapshot{}, fmt.Errorf("provider registration pair %s credential %q has no explicit store-key mapping", key, logical)
		}
		snapshot, err := c.opts.CredentialOwner.Observe(ctx, storeKey)
		if err != nil {
			return nil, runtimecredentials.AdmittedSnapshot{}, err
		}
		if !snapshot.Present {
			return nil, runtimecredentials.AdmittedSnapshot{}, fmt.Errorf("provider registration pair %s credential %q is missing", key, storeKey)
		}
		provider[logical] = snapshot
	}
	signingKey := strings.TrimSpace(pair.Target.SigningCredentialKey)
	signing, err := c.opts.CredentialOwner.Observe(ctx, signingKey)
	if err != nil {
		return nil, runtimecredentials.AdmittedSnapshot{}, err
	}
	if !signing.Present {
		return nil, runtimecredentials.AdmittedSnapshot{}, fmt.Errorf("provider registration pair %s signing credential %q is missing", key, signingKey)
	}
	return provider, signing, nil
}

func (c *ProviderRegistrationController) admitReadbackCandidate(ctx context.Context, state registrationState) (admittedPair, error) {
	active := state.activeIntent()
	if active == nil || active.BaseFingerprint == "" || active.SlotID == "" {
		return admittedPair{}, fmt.Errorf("provider registration pending readback identity is incomplete")
	}
	provider, signing, err := c.admitCredentials(ctx, state.Pair)
	if err != nil {
		return admittedPair{}, err
	}
	return admittedPair{pair: state.Pair, provider: provider, signing: signing, slotID: active.SlotID, base: active.BaseFingerprint}, nil
}

func (c *ProviderRegistrationController) reconcilePair(ctx context.Context, exposure Generation, startup runtimestartupownership.Authority, candidate admittedPair) error {
	key := pairKey(candidate.pair)
	state, _ := c.snapshot.state(key)
	state.Pair = candidate.pair
	state.SelectedBase = candidate.base
	if state.Phase == registrationPhaseOutcomeUncertain && state.Terminal != nil && state.Terminal.BaseFingerprint == candidate.base {
		c.publishState(key, state)
		return nil
	}
	state.Failure = ""
	if state.Attempt != nil && state.Attempt.Intent.BaseFingerprint == candidate.base {
		if state.Phase == registrationPhasePendingSettlement {
			c.publishState(key, state)
			return c.refreshReadback(ctx, candidate, state, true)
		}
		return c.launchAttempt(ctx, exposure, startup, candidate, state)
	}
	if state.LastVerified != nil && state.LastVerified.BaseFingerprint == candidate.base {
		intent := cloneRegistrationIntent(*state.LastVerified)
		intent.Authority = serveRegistrationAuthority(startup, intent.IntentID, c.opts.Posture, c.opts.Now())
		state.LastVerified = &intent
		state.Attempt = nil
		state.Phase = registrationPhaseVerified
		c.publishState(key, state)
		return c.refreshReadback(ctx, candidate, state, false)
	}
	intent, err := c.newRegistrationIntent(exposure, startup, candidate)
	if err != nil {
		state.Failure = err.Error()
		state.Phase = registrationPhaseNoAttempt
		c.publishState(key, state)
		return err
	}
	state.Attempt = &registrationAttempt{Intent: intent}
	state.Terminal = nil
	state.Phase = registrationPhasePrelaunch
	c.publishState(key, state)
	return c.launchAttempt(ctx, exposure, startup, candidate, state)
}

func (c *ProviderRegistrationController) newRegistrationIntent(exposure Generation, startup runtimestartupownership.Authority, candidate admittedPair) (registrationIntent, error) {
	intentID := uuid.NewString()
	token, err := randomToken(32)
	if err != nil {
		return registrationIntent{}, err
	}
	callbackURL, err := CallbackURL(exposure, candidate.pair.Target.Alias, candidate.pair.Target.Provider, token)
	if err != nil {
		return registrationIntent{}, err
	}
	return registrationIntent{
		BaseFingerprint: candidate.base, ExposureGenerationID: exposure.ID,
		IntentID: intentID, CallbackToken: token, CallbackURL: callbackURL, SlotID: candidate.slotID,
		Authority:        serveRegistrationAuthority(startup, intentID, c.opts.Posture, c.opts.Now()),
		CredentialEpochs: candidateCredentialEpochs(candidate),
	}, nil
}

func (c *ProviderRegistrationController) launchAttempt(ctx context.Context, exposure Generation, startup runtimestartupownership.Authority, candidate admittedPair, state registrationState) error {
	key := pairKey(candidate.pair)
	if state.Attempt == nil {
		return fmt.Errorf("provider registration pre-launch attempt is missing")
	}
	intent := cloneRegistrationIntent(state.Attempt.Intent)
	intent.ExposureGenerationID = exposure.ID
	intent.Authority = serveRegistrationAuthority(startup, intent.IntentID, c.opts.Posture, c.opts.Now())
	state.Attempt.Intent = intent
	state.Phase = registrationPhasePrelaunch
	state.Failure = ""
	c.publishState(key, state)
	if err := c.revalidateCredentials(ctx, candidate); err != nil {
		state.Failure = err.Error()
		c.publishState(key, state)
		return err
	}
	apply, err := candidate.pair.Registration.Operation(packs.RegistrationOperationApply)
	if err != nil {
		state.Failure = err.Error()
		c.publishState(key, state)
		return err
	}
	input, err := apply.Prepare(map[string]any{"callback_url": intent.CallbackURL})
	if err != nil {
		state.Failure = err.Error()
		c.publishState(key, state)
		return err
	}
	if !intent.Authority.Valid() {
		return fmt.Errorf("serve registration effect authority is invalid")
	}
	effectController := runtimeeffects.NewController(c.opts.EffectsStore).WithExecutionPosture(c.opts.Posture)
	effectCtx := runtimeeffects.WithController(ctx, effectController)
	effectCtx = runtimeeffects.WithAuthority(effectCtx, intent.Authority)
	effectCtx = runtimeeffects.WithExecutionMode(effectCtx, intent.Authority.ExecutionMode)
	effectCtx = runtimeauthoractivity.WithScope(effectCtx, runtimeauthoractivity.BundleScope(c.opts.RuntimeInstanceID, candidate.pair.Target.BundleHash))
	result, applyErr := c.opts.HTTP.Apply(effectCtx, apply.ToolID(), apply.Tool(), input, candidateCredentials(candidate), map[string]string{
		"binding_id": candidate.pair.BindingID, "target": candidate.pair.Target.Selector, "intent_id": intent.IntentID, "slot_id": candidate.slotID,
	})
	if applyErr != nil && result.Pending == nil {
		state.Failure = applyErr.Error()
		state.Attempt.Intent = intent
		state.Phase = registrationPhasePrelaunch
		c.publishState(key, state)
		return applyErr
	}
	intent.Applied = result.Acknowledged || result.Pending != nil
	if result.Pending != nil {
		intent.Pending = *result.Pending
		intent.HasPending = true
		attempt := result.Pending.Attempt()
		intent.EffectOperationID = attempt.OperationID
		intent.EffectAttemptID = attempt.AttemptID
		intent.EffectAttemptOrdinal = attempt.Ordinal
	}
	state.Attempt.Intent = intent
	state.Phase = registrationPhasePendingSettlement
	state.Failure = ""
	c.publishState(key, state)
	return c.refreshReadback(effectCtx, candidate, state, true)
}

func (c *ProviderRegistrationController) refreshReadback(ctx context.Context, candidate admittedPair, state registrationState, fromAttempt bool) error {
	key := pairKey(candidate.pair)
	active := state.activeIntent()
	if active == nil {
		return fmt.Errorf("provider registration intent is missing")
	}
	intent := cloneRegistrationIntent(*active)
	current, err := c.opts.EffectsStore.IsExternalEffectAuthorityCurrent(ctx, intent.Authority)
	if err != nil || !current {
		if err == nil {
			err = fmt.Errorf("provider registration startup authority is no longer current")
		}
		state.Failure = err.Error()
		c.publishState(key, state)
		return err
	}
	readback, readErr := candidate.pair.Registration.Operation(packs.RegistrationOperationReadback)
	var input map[string]any
	if readErr == nil {
		input, readErr = readback.Prepare(map[string]any{})
	}
	var result any
	if readErr == nil {
		result, readErr = c.opts.HTTP.Read(ctx, readback.ToolID(), readback.Tool(), input, candidateCredentials(candidate))
	}
	exact := false
	if readErr == nil {
		projected, projectErr := readback.Project(result)
		if projectErr != nil {
			readErr = projectErr
		} else {
			exact = subtle.ConstantTimeCompare([]byte(strings.TrimSpace(fmt.Sprint(projected["callback_url"]))), []byte(intent.CallbackURL)) == 1
			if !exact {
				readErr = fmt.Errorf("provider callback readback does not match the current registration intent")
			}
		}
	}
	if intent.HasPending {
		pending := intent.Pending
		if settleErr := pending.SettleReadback(ctx, exact, readErr); settleErr != nil {
			intent.Pending = pending
			state.setActiveIntent(intent, fromAttempt)
			state.Failure = settleErr.Error()
			c.publishState(key, state)
			return settleErr
		}
		intent.HasPending = false
		intent.Pending = runtimeregistration.PendingApply{}
		if !exact {
			intent.Matched = false
			state.Terminal = &intent
			state.Attempt = nil
			state.Phase = registrationPhaseOutcomeUncertain
			state.Failure = readErr.Error()
			c.publishState(key, state)
			return readErr
		}
	}
	if readErr != nil {
		intent.Matched = false
		state.setActiveIntent(intent, fromAttempt)
		state.Failure = readErr.Error()
		c.publishState(key, state)
		return readErr
	}
	now := c.opts.Now().UTC()
	intent.Applied = true
	intent.Matched = true
	intent.ObservedAt = now
	intent.ExpiresAt = now.Add(EvidenceTTL)
	intent.HasPending = false
	state.LastVerified = &intent
	state.Attempt = nil
	state.Terminal = nil
	state.Phase = registrationPhaseVerified
	state.Failure = ""
	c.publishState(key, state)
	return nil
}

func (c *ProviderRegistrationController) terminalizePendingReadback(ctx context.Context, key string, state registrationState, cause error) error {
	if state.Attempt == nil || !state.Attempt.Intent.HasPending {
		return fmt.Errorf("provider registration pending settlement has no live durable attempt")
	}
	intent := cloneRegistrationIntent(state.Attempt.Intent)
	pending := intent.Pending
	if err := pending.SettleReadback(ctx, false, cause); err != nil {
		state.Failure = err.Error()
		c.publishState(key, state)
		return err
	}
	intent.HasPending = false
	intent.Pending = runtimeregistration.PendingApply{}
	intent.Matched = false
	state.Terminal = &intent
	state.Attempt = nil
	state.Phase = registrationPhaseOutcomeUncertain
	state.Failure = cause.Error()
	c.publishState(key, state)
	return nil
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
	return c != nil && c.opts.Readiness.CallbackCurrent(ctx, c.opts.Now().UTC(), alias, provider, token)
}

func (c *ProviderRegistrationController) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
		if len(parts) != 3 || parts[0] != "webhooks" {
			http.NotFound(response, request)
			return
		}
		managed := c.snapshot.routeSelected(parts[1], parts[2])
		if !managed || !c.CallbackCurrent(request.Context(), parts[1], parts[2], request.URL.Query().Get("swarm_callback_generation")) {
			http.NotFound(response, request)
			return
		}
		next.ServeHTTP(response, request)
	})
}

func (c *ProviderRegistrationController) publishState(key string, state registrationState) {
	c.snapshot.publishState(key, state)
}

func (c *ProviderRegistrationController) recordFailure(key string, pair RegistrationPair, err error) {
	state, _ := c.snapshot.state(key)
	state.Pair = pair
	state.Failure = err.Error()
	if state.Phase == "" {
		state.Phase = registrationPhaseNoAttempt
	}
	c.publishState(key, state)
}

func (s registrationState) activeIntent() *registrationIntent {
	if s.Attempt != nil {
		intent := cloneRegistrationIntent(s.Attempt.Intent)
		return &intent
	}
	if s.Terminal != nil {
		intent := cloneRegistrationIntent(*s.Terminal)
		return &intent
	}
	if s.LastVerified != nil {
		intent := cloneRegistrationIntent(*s.LastVerified)
		return &intent
	}
	return nil
}

func (s *registrationState) setActiveIntent(intent registrationIntent, fromAttempt bool) {
	copy := cloneRegistrationIntent(intent)
	if fromAttempt {
		if s.Attempt == nil {
			s.Attempt = &registrationAttempt{}
		}
		s.Attempt.Intent = copy
		return
	}
	s.LastVerified = &copy
}

func (s registrationState) evidence(current bool) RegistrationEvidence {
	evidence := RegistrationEvidence{
		BindingID: s.Pair.BindingID, Target: s.Pair.Target.Selector, Provider: s.Pair.Target.Provider, Alias: s.Pair.Target.Alias,
		Phase: string(s.Phase), Failure: s.Failure, CallbackMatched: current,
	}
	intent := s.activeIntent()
	if intent == nil {
		return evidence
	}
	evidence.IntentID = intent.IntentID
	evidence.SlotID = intent.SlotID
	evidence.CallbackURL = intent.CallbackURL
	evidence.StartupAuthorityID = intent.Authority.ServeRegistration.StartupAuthorityID
	evidence.Applied = intent.Applied
	evidence.ObservedAt = intent.ObservedAt
	evidence.ExpiresAt = intent.ExpiresAt
	return evidence
}

func cloneRegistrationState(source registrationState) registrationState {
	out := source
	if source.LastVerified != nil {
		intent := cloneRegistrationIntent(*source.LastVerified)
		out.LastVerified = &intent
	}
	if source.Attempt != nil {
		out.Attempt = &registrationAttempt{Intent: cloneRegistrationIntent(source.Attempt.Intent)}
	}
	if source.Terminal != nil {
		intent := cloneRegistrationIntent(*source.Terminal)
		out.Terminal = &intent
	}
	return out
}

func cloneRegistrationIntent(source registrationIntent) registrationIntent {
	out := source
	out.CredentialEpochs = cloneStringMap(source.CredentialEpochs)
	return out
}

func cloneStringMap(source map[string]string) map[string]string {
	out := make(map[string]string, len(source))
	for key, value := range source {
		out[key] = value
	}
	return out
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
