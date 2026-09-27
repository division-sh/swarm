package channeldelivery

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
)

// Candidate is a selected-store projection for one current destination.
// Dispatch must still acquire current activation and effect authority.
type Candidate struct {
	DeliveryID          string
	SourceKind          string
	SourceID            string
	RequestActivationID string
	BindingRevision     int64
	Audience            Audience
	State               string
	CurrentRenderID     string
	CurrentReceiptID    string
}

type PreparedRender struct {
	RenderID   string
	DeliveryID string
	Frozen     Frozen
	Actions    []Action
}

type SentReceipt struct {
	OperationID       string
	DeliveryID        string
	RenderID          string
	DeliveryReference any
}

type Action struct {
	Token              string
	Kind               string
	Verdict            string
	DraftID            string
	CardID             string
	TextPublicationID  string
	RecoveryDeliveryID string
	Label              string
}

type ResolvedAction struct {
	Action             Action
	DeliveryID         string
	RenderID           string
	RenderHash         string
	ReceiptOperationID string
	SourceKind         string
	SourceID           string
	PrincipalID        string
	BindingRevision    int64
	ActivationID       string
	ActivationRevision int64
	CurrentRender      bool
}

type CardActionDemand struct {
	CardID             string
	PrincipalID        string
	Method             string
	Verdict            string
	DraftID            string
	ReceiptOperationID string
	RenderHash         string
}

type PendingAction struct {
	PublicationID string
	Fact          operatorchannel.InboundAction
	ReceivedAt    time.Time
}

type PendingText struct {
	PublicationID string
	Fact          operatorchannel.InboundText
	ReceivedAt    time.Time
}

type ResolvedText struct {
	PrincipalID     string
	InterfaceKey    string
	BindingRevision int64
}

type InputDraftCandidate struct {
	DraftID            string
	CardID             string
	Verdict            string
	ReceiptOperationID string
}

type ResolvedNativeEntry struct {
	PrincipalID       string
	InterfaceKey      string
	BindingRevision   int64
	ActivationID      string
	SettingID         string
	ResourceSlotID    string
	SettingGeneration int64
	EntryReference    string
}

type ActionDisposition string

const (
	ActionApplied      ActionDisposition = "applied"
	ActionInputStarted ActionDisposition = "input_started"
	ActionStale        ActionDisposition = "stale"
	ActionRejected     ActionDisposition = "rejected"
	ActionUnsupported  ActionDisposition = "unsupported"
)

func (d ActionDisposition) Valid() bool {
	switch d {
	case ActionApplied, ActionInputStarted, ActionStale, ActionRejected, ActionUnsupported:
		return true
	default:
		return false
	}
}

type Store interface {
	CurrentChannelDeliveryActivationID(context.Context) (string, bool, error)
	CurrentChannelCardChangeCursor(context.Context) (int64, bool, error)
	PlanChangedChannelCard(context.Context, int64, string) error
	ListCurrentChannelDeliveryPlans(context.Context, string, int) ([]Candidate, error)
	GetCurrentChannelDeliveryPlan(context.Context, string) (Candidate, bool, error)
	GetCurrentChannelSentReceipt(context.Context, string, string) (SentReceipt, bool, error)
	PlanOpenChannelCard(context.Context, string) (bool, error)
	FreezeAndPersistChannelRender(context.Context, string) (PreparedRender, error)
	ResolveChannelActionFact(context.Context, operatorchannel.ActionFact) (ResolvedAction, bool, error)
	ListPendingChannelActions(context.Context, string, int) ([]PendingAction, error)
	SettleUnappliedChannelAction(context.Context, operatorchannel.InboundAction, ActionDisposition) error
	ListPendingChannelTexts(context.Context, string, int) ([]PendingText, error)
	ResolveCurrentChannelText(context.Context, operatorchannel.InboundText) (ResolvedText, bool, error)
	ListCurrentChannelInputDrafts(context.Context, operatorchannel.InboundText, time.Time, string, int) ([]InputDraftCandidate, string, error)
	PreviewCurrentChannelInputDraftText(context.Context, operatorchannel.InboundText, time.Time, string) (decisioncard.InputFieldProgress, string, error)
	AdvancePartialChannelInputDraftText(context.Context, operatorchannel.InboundText, time.Time, string) (decisioncard.InputFieldProgress, error)
	PreviewChosenChannelInputDraftText(context.Context, operatorchannel.InboundAction, time.Time) (InputDraftCandidate, PendingText, decisioncard.InputFieldProgress, string, error)
	AdvancePartialChosenChannelInputDraftText(context.Context, operatorchannel.InboundAction, time.Time) (decisioncard.InputFieldProgress, error)
	PreviewChannelInputSkip(context.Context, operatorchannel.InboundAction, time.Time) (ResolvedAction, decisioncard.InputFieldProgress, decisioncard.InputDraft, error)
	AdvancePartialChannelInputSkip(context.Context, operatorchannel.InboundAction, time.Time) (decisioncard.InputFieldProgress, error)
	ResolveCurrentNativeInboxEntry(context.Context, operatorchannel.InboundText) (ResolvedNativeEntry, bool, error)
	PlanNativeInboxResponse(context.Context, operatorchannel.InboundText, ResolvedNativeEntry, string) (string, error)
	PlanChannelTextResponse(context.Context, operatorchannel.InboundText, string, string) (string, error)
	PlanChannelDraftChooser(context.Context, operatorchannel.InboundText, time.Time) (string, error)
	PlanChannelActionResponse(context.Context, operatorchannel.InboundAction, ResolvedAction, string) (string, error)
	PlanManualChannelResend(context.Context, operatorchannel.InboundAction, ResolvedAction) (string, error)
	AcknowledgeChannelNotice(context.Context, apiidempotency.Request, operatorchannel.InboundAction) (apiidempotency.Completion, bool, error)
}
