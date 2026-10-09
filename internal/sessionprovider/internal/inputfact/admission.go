package inputfact

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

// Capture is accessible only inside the native owner's Go internal boundary.
// It is not exported through the read-only consumption facade.
type Capture struct {
	Store            channelonboarding.Store
	Responsibility   channelonboarding.AdmissionResponsibility
	Scope            channelonboarding.SessionInputScope
	BindingRevision  int64
	PublicationRunID string
	Body             []byte
	OriginalCapture  []byte
	ReceivedAt       time.Time
	Generation       triggergeneration.Generation
	Context          context.Context
	Release          func()
	RunCurrent       func(context.Context) error
	NativeCurrent    func() bool
}

type admittedInput struct {
	capture Capture
	once    sync.Once
}

type Admission struct{ value *admittedInput }

// SealOwnedCapture has one concrete native issuer. Its Go internal visibility
// and the issuer census prohibit raw-data construction by admission consumers.
func SealOwnedCapture(capture Capture) Admission {
	capture.Body = bytes.Clone(capture.Body)
	capture.OriginalCapture = bytes.Clone(capture.OriginalCapture)
	return Admission{value: &admittedInput{capture: capture}}
}

func (a Admission) Validate(ctx context.Context, provider string, generation triggergeneration.Generation) error {
	if !a.LifetimeCurrent(ctx) ||
		a.value.capture.Store == nil || a.value.capture.Release == nil || provider != a.value.capture.Responsibility.Provider ||
		!generation.Equal(a.value.capture.Generation) {
		return fmt.Errorf("exact current native input admission is required")
	}
	current, err := channelonboarding.AdmissionResponsibilityCurrent(ctx, a.value.capture.Store, a.value.capture.Responsibility, true)
	if err != nil {
		return err
	}
	if !current || !a.LifetimeCurrent(ctx) {
		return fmt.Errorf("native input responsibility is no longer current")
	}
	return nil
}

// LifetimeCurrent never reads the selected store; mutation owners can use it
// while holding their own SQL transaction without a read-through deadlock.
func (a Admission) LifetimeCurrent(ctx context.Context) bool {
	return a.value != nil && ctx != nil && ctx.Err() == nil && a.value.capture.Context != nil && a.value.capture.Context.Err() == nil &&
		a.value.capture.NativeCurrent != nil && a.value.capture.NativeCurrent()
}

func (a Admission) RequireBusiness(ctx context.Context, provider string, generation triggergeneration.Generation) error {
	if err := a.Validate(ctx, provider, generation); err != nil {
		return err
	}
	capture := a.value.capture
	if capture.Scope != channelonboarding.SessionInputBusiness || capture.Responsibility.ActivationRevision < 1 ||
		capture.BindingRevision < 1 || capture.PublicationRunID == "" {
		return fmt.Errorf("business publication requires explicit activated native business scope")
	}
	op, err := capture.Store.GetChannelOnboarding(ctx, capture.Responsibility.OperationID)
	if err != nil {
		return err
	}
	activation, err := capture.Store.GetConnectedChannelActivation(ctx, op.SlotKey)
	if err != nil {
		return err
	}
	if op.BindingRevision != capture.BindingRevision || activation.BindingRevision != capture.BindingRevision ||
		ctx.Err() != nil || capture.Context.Err() != nil {
		return fmt.Errorf("native business input contradicts its original binding")
	}
	bindings, ok := capture.Store.(interface {
		ListOperatorChannelBindings(context.Context, string) ([]operatorchannel.Binding, error)
	})
	if !ok {
		return fmt.Errorf("native business input has no canonical identity binding owner")
	}
	rows, err := bindings.ListOperatorChannelBindings(ctx, op.PrincipalID)
	if err != nil {
		return err
	}
	matched := 0
	for _, binding := range rows {
		if binding.Interface.Key() == op.Interface.Key() && binding.PrincipalID == op.PrincipalID &&
			binding.Status == operatorchannel.BindingCurrent && binding.Revision == capture.BindingRevision &&
			binding.ConversationRef == activation.ConversationRef && binding.ProofID == activation.ProofID && binding.ProofRevision == activation.ProofRevision &&
			binding.ProviderAuthority.Kind == operatorchannel.ProviderAuthoritySession && binding.ProviderAuthority.Session == capture.Responsibility.SessionAccount {
			matched++
		}
	}
	if matched != 1 || ctx.Err() != nil || capture.Context.Err() != nil {
		return fmt.Errorf("native business input no longer owns its original operator binding")
	}
	if capture.RunCurrent == nil {
		return fmt.Errorf("native business input has no exact selected run owner")
	}
	return capture.RunCurrent(ctx)
}

func (a Admission) Body() []byte {
	if a.value == nil {
		return nil
	}
	return bytes.Clone(a.value.capture.Body)
}

func (a Admission) OriginalCapture() []byte {
	if a.value == nil {
		return nil
	}
	return bytes.Clone(a.value.capture.OriginalCapture)
}
func (a Admission) ReceivedAt() time.Time {
	if a.value == nil {
		return time.Time{}
	}
	return a.value.capture.ReceivedAt
}
func (a Admission) Coordinate() channelonboarding.ChannelRuntimeContextCoordinate {
	if a.value == nil {
		return channelonboarding.ChannelRuntimeContextCoordinate{}
	}
	return a.value.capture.Responsibility.Coordinate
}
func (a Admission) TargetSelector() string {
	if a.value == nil {
		return ""
	}
	return a.value.capture.Responsibility.TargetSelector
}
func (a Admission) Scope() channelonboarding.SessionInputScope {
	if a.value == nil {
		return ""
	}
	return a.value.capture.Scope
}
func (a Admission) PublicationRunID() string {
	if a.value == nil {
		return ""
	}
	return a.value.capture.PublicationRunID
}
func (a Admission) Context() context.Context {
	if a.value == nil {
		return nil
	}
	return a.value.capture.Context
}
func (a Admission) Close() {
	if a.value != nil {
		a.value.once.Do(a.value.capture.Release)
	}
}
