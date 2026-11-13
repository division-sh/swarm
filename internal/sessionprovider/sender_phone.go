package sessionprovider

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"go.mau.fi/whatsmeow/types"
)

var errSenderPhoneRead = errors.New("WhatsApp sender phone map read failed")

func incomingSenderPhone(ctx context.Context, source types.MessageSource, lookup func(context.Context, types.JID) (types.JID, error)) (string, error) {
	selected := source.Sender.ToNonAD()
	if selected.Server != types.DefaultUserServer {
		selected = source.SenderAlt.ToNonAD()
		if selected.Server != types.DefaultUserServer {
			if source.Sender.Server != types.HiddenUserServer {
				return "", nil
			}
			var err error
			selected, err = lookup(ctx, source.Sender.ToNonAD())
			if err != nil {
				return "", fmt.Errorf("%w: %w", errSenderPhoneRead, err)
			}
			if selected.IsEmpty() {
				return "", nil
			}
		}
	}
	phone := "+" + selected.User
	if selected.Server != types.DefaultUserServer || (channelonboarding.AccountReference{Provider: "whatsapp", Account: phone}).Validate() != nil {
		diaglog.ProcessLog(diaglog.LevelError, "whatsapp", "sender phone evidence malformed", "category", "sender_phone_malformed")
		return "", nil
	}
	return phone, nil
}
