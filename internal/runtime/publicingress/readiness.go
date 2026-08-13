package publicingress

import (
	"sort"
	"strings"
	"sync"
	"time"
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
	BindingID          string    `json:"binding_id"`
	Target             string    `json:"target"`
	Provider           string    `json:"provider"`
	Alias              string    `json:"alias"`
	IntentID           string    `json:"intent_id"`
	SlotID             string    `json:"slot_id"`
	CallbackURL        string    `json:"callback_url"`
	StartupAuthorityID string    `json:"startup_authority_id"`
	Applied            bool      `json:"applied"`
	CallbackMatched    bool      `json:"callback_matched"`
	ObservedAt         time.Time `json:"observed_at"`
	ExpiresAt          time.Time `json:"expires_at"`
	Failure            string    `json:"failure,omitempty"`
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

type ReadinessOwner struct {
	mu                  sync.RWMutex
	runtimeReady        bool
	enabled             bool
	exposure            *ExposureEvidence
	registrations       map[string]RegistrationEvidence
	failure             string
	startupCurrent      func(string) (bool, error)
	registrationCurrent func() (bool, error)
}

func NewReadinessOwner(enabled bool) *ReadinessOwner {
	return &ReadinessOwner{enabled: enabled, registrations: map[string]RegistrationEvidence{}}
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

func (o *ReadinessOwner) SetStartupCurrent(check func(string) (bool, error)) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.startupCurrent = check
	o.mu.Unlock()
}

func (o *ReadinessOwner) SetRegistrationCurrent(check func() (bool, error)) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.registrationCurrent = check
	o.mu.Unlock()
}

func (o *ReadinessOwner) SetExposure(evidence ExposureEvidence) {
	if o == nil {
		return
	}
	copy := evidence
	o.mu.Lock()
	o.exposure = &copy
	o.failure = strings.TrimSpace(evidence.Failure)
	o.mu.Unlock()
}

func (o *ReadinessOwner) RevokeExposure(cause string) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.exposure = nil
	o.failure = strings.TrimSpace(cause)
	for key, registration := range o.registrations {
		registration.CallbackMatched = false
		registration.Failure = strings.TrimSpace(cause)
		o.registrations[key] = registration
	}
	o.mu.Unlock()
}

func (o *ReadinessOwner) SetRegistration(key string, evidence RegistrationEvidence) {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.registrations[strings.TrimSpace(key)] = evidence
	o.mu.Unlock()
}

func (o *ReadinessOwner) ReplaceRegistrationKeys(keys []string) {
	if o == nil {
		return
	}
	keep := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		keep[strings.TrimSpace(key)] = struct{}{}
	}
	o.mu.Lock()
	for key := range o.registrations {
		if _, ok := keep[key]; !ok {
			delete(o.registrations, key)
		}
	}
	o.mu.Unlock()
}

func (o *ReadinessOwner) Snapshot(now time.Time) Snapshot {
	if o == nil {
		return Snapshot{}
	}
	now = now.UTC()
	o.mu.RLock()
	snapshot := Snapshot{RuntimeReady: o.runtimeReady, PublicIngressEnabled: o.enabled, Failure: o.failure}
	startupCurrent := o.startupCurrent
	registrationCurrent := o.registrationCurrent
	exposureReady := !o.enabled
	if o.exposure != nil {
		copy := *o.exposure
		snapshot.Exposure = &copy
		exposureReady = copy.Failure == "" && now.Before(copy.ExpiresAt)
	}
	keys := make([]string, 0, len(o.registrations))
	for key := range o.registrations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	registrationsReady := true
	for _, key := range keys {
		registration := o.registrations[key]
		snapshot.Registrations = append(snapshot.Registrations, registration)
		if registration.Failure != "" || !registration.Applied || !registration.CallbackMatched || !now.Before(registration.ExpiresAt) {
			registrationsReady = false
		}
		if snapshot.Exposure == nil || strings.TrimSpace(registration.StartupAuthorityID) == "" ||
			registration.StartupAuthorityID != snapshot.Exposure.StartupAuthorityID {
			registrationsReady = false
		}
	}
	o.mu.RUnlock()
	if exposureReady && snapshot.Exposure != nil && startupCurrent != nil {
		current, err := startupCurrent(snapshot.Exposure.StartupAuthorityID)
		if err != nil || !current {
			exposureReady = false
			if err != nil {
				snapshot.Failure = err.Error()
			} else {
				snapshot.Failure = "public ingress startup authority is no longer current"
			}
		}
	}
	if registrationsReady && registrationCurrent != nil {
		current, err := registrationCurrent()
		if err != nil || !current {
			registrationsReady = false
			if err != nil {
				snapshot.Failure = err.Error()
			} else {
				snapshot.Failure = "public ingress credential snapshots are no longer current"
			}
		}
	}
	snapshot.PublicIngressReady = exposureReady && registrationsReady
	snapshot.Ready = snapshot.RuntimeReady && snapshot.PublicIngressReady
	return snapshot
}
