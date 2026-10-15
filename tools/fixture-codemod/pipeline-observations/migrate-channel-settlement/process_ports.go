package main

import (
	"fmt"
	"strings"
)

var processPorts = []port{
	{"ReadChannelNativeProcessSettingStorage", "setting string", "setting", "ChannelNativeProcessSettingStorage", "channel", "ReadNativeProcessSettingStorageTx"},
	{"ReadChannelNativeProcessAttemptStorage", "", "", "ChannelNativeProcessAttemptStorage", "channel", "ReadNativeProcessAttemptStorageTx"},
	{"ReadChannelAcknowledgmentAttemptStorage", "", "", "ChannelAcknowledgmentAttemptStorage", "effect", "ReadChannelAcknowledgmentAttemptStorageTx"},
	{"ReadChannelDeliveryProcessAttemptStorage", "card string", "card", "ChannelDeliveryProcessAttemptStorage", "channel", "ReadDeliveryProcessAttemptStorageTx"},
	{"ReadChannelProcessActionCardStorage", "token string", "token", "ChannelProcessActionCardStorage", "channel", "ReadProcessActionCardStorageTx"},
	{"ReadChannelProcessRecoveredReceiptStorage", "card,operation string", "card,operation", "ChannelProcessRecoveredReceiptStorage", "channel", "ReadProcessRecoveredReceiptStorageTx"},
	{"ReadChannelProcessRetainedReceiptStorage", "card,operation string", "card,operation", "ChannelProcessRetainedReceiptStorage", "channel", "ReadProcessRetainedReceiptStorageTx"},
	{"ReadChannelCurrentStandingRunStorage", "", "", "string", "standing", "ReadChannelCurrentStandingRunStorageTx"},
	{"ReadChannelNativeHistoryStorage", "", "", "[8]string", "channel", "ReadNativeHistoryStorageTx"},
	{"ReadChannelSourceHistoryStorage", "card string", "card", "ChannelSourceHistoryStorage", "channel", "ReadSourceHistoryStorageTx"},
	{"ReadChannelPlanCardStateStorage", "card string", "card", "ChannelPlanCardStateStorage", "channel", "ReadPlanCardStateStorageTx"},
	{"ReadChannelActionChoiceStorage", "token string", "token", "ChannelActionChoiceStorage", "channel", "ReadActionChoiceStorageTx"},
	{"ReadChannelInputCommitStorage", "card string", "card", "ChannelInputCommitStorage", "decision", "ReadChannelInputCommitStorageTx"},
	{"ReadChannelCurrentActivationOperationStorage", "", "", "string", "onboarding", "ReadChannelCurrentActivationOperationStorageTx"},
	{"ReadChannelRecoveryNoticeStorage", "", "", "ChannelRecoveryNoticeStorage", "mailbox", "ReadRecoveryNoticeStorageTx"},
	{"CountChannelRecoveryNoticesStorage", "", "", "int", "mailbox", "CountRecoveryNoticeStorageTx"},
	{"ReadChannelUncertainNoticeAcknowledgmentStorage", "id string", "id", "bool", "mailbox", "ReadUncertainNoticeAcknowledgmentStorageTx"},
	{"ReadChannelUncertainNoticePlanCountsStorage", "id string", "id", "ChannelUncertainNoticePlanCountsStorage", "channel", "ReadUncertainNoticePlanCountsStorageTx"},
	{"ReadChannelActivationConversationStorage", "operation string", "operation", "string", "onboarding", "ReadChannelActivationConversationStorageTx"},
	{"CountChannelCurrentActivationsStorage", "", "", "int", "onboarding", "CountChannelCurrentActivationsStorageTx"},
	{"ReadChannelFirstReceiptedDeliveryStorage", "", "", "string", "channel", "ReadFirstReceiptedDeliveryStorageTx"},
	{"CountChannelRejectedIntentPlansStorage", "", "", "int", "channel", "CountRejectedIntentPlansStorageTx"},
	{"CountChannelTerminalCurrentReceiptsStorage", "card string", "card", "int", "channel", "CountTerminalCurrentReceiptsStorageTx"},
	{"CountChannelSettledActionIntentsStorage", "", "", "int", "operator", "CountSettledChannelActionIntentsStorageTx"},
	{"CountChannelSettledTextIntentsStorage", "", "", "int", "operator", "CountSettledChannelTextIntentsStorageTx"},
	{"ReadChannelResetRecordStorage", "", "", "string", "admin", "ReadChannelResetRecordStorageTx"},
	{"CountChannelRunIdentityStorage", "run string", "run", "int", "lifecycle", "CountRunIdentityStorageTx"},
	{"CountChannelSourceActivationsStorage", "hash string", "hash", "int", "onboarding", "CountSourceActivationsStorageTx"},
	{"CountChannelSourceArtifactsStorage", "hash string", "hash", "int", "source", "CountChannelSourceArtifactsStorageTx"},
	{"CountChannelStandingGenerationsStorage", "", "", "int", "standing", "CountStandingGenerationsStorageTx"},
	{"ReadChannelSourceSuccessorPlanStorage", "card string", "card", "ChannelSourceSuccessorPlanStorage", "channel", "ReadSourceSuccessorPlanStorageTx"},
	{"ReadChannelSourceNativeConsumerStateStorage", "activation string", "activation", "string", "channel", "ReadSourceNativeConsumerStateStorageTx"},
	{"ReadChannelSourceForkFailureStorage", "key string", "key", "sql.NullString", "fork", "ReadChannelSourceForkFailureStorageTx"},
	{"CountChannelForkSupersessionLineageStorage", "run,event string", "run,event", "int", "event", "CountChannelForkSupersessionLineageStorageTx"},
	{"ReadChannelForkSupersessionPayloadStorage", "run string", "run", "[]byte", "event", "ReadChannelForkSupersessionPayloadStorageTx"},
	{"CountChannelSourceRetiredCardStorage", "run,card string", "run,card", "int", "decision", "CountSourceRetiredCardStorageTx"},
	{"ReadChannelSourceParentCardClockStorage", "card string", "card", "[3]string", "decision", "ReadSourceParentCardClockStorageTx"},
	{"CountChannelSourceCardDraftsStorage", "card string", "card", "int", "decision", "CountSourceCardDraftsStorageTx"},
	{"ReadChannelOnboardingOperationDiagnosticStorage", "operation string", "operation", "ChannelOnboardingOperationDiagnosticStorage", "onboarding", "ReadOnboardingOperationDiagnosticStorageTx"},
	{"ReadChannelOnboardingPublicationHandoffStorage", "operation string", "operation", "ChannelOnboardingPublicationHandoffStorage", "onboarding", "ReadOnboardingPublicationHandoffStorageTx"},
	{"ReadChannelOnboardingOperationPhysicalSurfaceStorage", "operation string", "operation", "map[string]string", "onboarding", "ReadOnboardingOperationPhysicalSurfaceStorageTx"},
}

func generateProcessPorts() {
	private := `// Code generated by migrate-channel-settlement -process-ports; DO NOT EDIT.
package runtimepersistence
import("context";"database/sql";channel "github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery";effect "github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence";standing "github.com/division-sh/swarm/internal/store/internal/backend/standingdisposition";operator "github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel";decision "github.com/division-sh/swarm/internal/store/internal/backend/decisioncard";onboarding "github.com/division-sh/swarm/internal/store/internal/backend/channelonboarding";mailbox "github.com/division-sh/swarm/internal/store/internal/mailboxpersistence";admin "github.com/division-sh/swarm/internal/store/internal/adminpersistence";lifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle";source "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission";fork "github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence")
`
	public := `// Code generated by migrate-channel-settlement -process-ports; DO NOT EDIT.
package storetest
import("context";"database/sql";"fmt";private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence")
// ObserveChannelProcessStorage borrows the original inspector without close or reopen authority.
func ObserveChannelProcessStorage(source ChannelObservation)(ChannelProcessObservation,error){native,ok:=source.(channelObservation);if !ok||native.selected==nil{return nil,fmt.Errorf("process observation requires the original selected channel inspector")};return native,nil}
`
	generated := append(append([]port{}, processPorts...), preservedHistoryPorts()...)
	methods := make([]string, 0, len(generated))
	for _, p := range generated {
		if strings.HasPrefix(p.Result, "Channel") {
			ownerType := strings.TrimPrefix(p.Result, "Channel")
			if p.Owner == "effect" || p.Owner == "decision" {
				ownerType = p.Result
			}
			private += fmt.Sprintf("type %s=%s.%s\n", p.Result, p.Owner, ownerType)
			public += fmt.Sprintf("type %s=private.%s\n", p.Result, p.Result)
		}
		params, args := prefixed(p.Parameters), prefixed(p.Arguments)
		methods = append(methods, fmt.Sprintf("%s(context.Context%s)(%s,error)", p.Name, prefixed(parameterTypes(p.Parameters)), p.Result))
		declaration := ""
		if p.Name == "ReadChannelDeliveryProcessAttemptStorage" {
			declaration, args = "_,postgres:=selected.(*PostgresStore);", args+",postgres"
		}
		// Event bridges remain in the existing closed event-support owner.
		if p.Owner != "event" {
			private += fmt.Sprintf(`func %sForTest(ctx context.Context,selected any%s)(%s,error){var result %s;if err:=validateChannelObservationOwner(selected);err!=nil{return result,err};%s missing:=false;err:=readServedDeliveryObservation(ctx,selected,func(ctx context.Context,tx *sql.Tx)error{var err error;result,err=%s.%s(ctx,tx%s);if err==sql.ErrNoRows{missing=true;return nil};return err});if err==nil&&missing{err=sql.ErrNoRows};return result,err}
`, p.Name, params, p.Result, p.Result, declaration, p.Owner, p.Operation, args)
		}
		public += fmt.Sprintf(`func(o channelObservation)%s(ctx context.Context%s)(%s,error){var result %s;missing:=false;err:=o.selected.InspectSnapshot(ctx,func(ctx context.Context)error{var err error;result,err=private.%sForTest(ctx,o.selected%s);if err==sql.ErrNoRows{missing=true;return nil};return err});if err==nil&&missing{err=sql.ErrNoRows};return result,err}
`, p.Name, params, p.Result, p.Result, p.Name, prefixed(p.Arguments))
	}
	public += "type ChannelProcessObservation interface {" + strings.Join(methods, ";") + "}\n"
	writeGo("internal/store/internal/runtimepersistence/test_channel_process_observation.go", private)
	writeGo("internal/store/storetest/channel_process_observation.go", public)
	generatePreservedHistoryOwners()
}
