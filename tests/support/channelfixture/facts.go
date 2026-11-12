package channelfixture

import "github.com/division-sh/swarm/internal/channelprovider"

var telegramFacts = channelprovider.Define(channelprovider.Definition{ID: "telegram", DisplayName: "Telegram", Webhook: true, PollClaims: true,
	Credential: true, SigningCredential: true, CanonicalAccount: channelprovider.TelegramAccount,
	ClaimInstructions:            channelprovider.TelegramClaimInstructions,
	RegistrationTeaching:         "Telegram ready. Run `swarm serve` to answer customers. Decisions: answer them in the terminal with `swarm mailbox decide`.",
	OmitBoundRegistrationReceipt: true})

var whatsappFacts = channelprovider.Define(channelprovider.Definition{ID: "whatsapp", DisplayName: "WhatsApp", Session: true,
	CanonicalAccount: channelprovider.WhatsAppAccount, ClaimInstructions: channelprovider.WhatsAppClaimInstructions,
	NativeRead: "whatsapp.read_account", NativeWrite: "whatsapp.send_text", CaptureKey: "whatsapp_capture",
	ReadyTeaching: "WhatsApp ready. Run `swarm serve` to answer customers. Decisions: answer them in the terminal with `swarm mailbox decide`.",
	NotLinked:     "Not linked: run swarm channel connect whatsapp",
	PairingPrompt: "Open WhatsApp > Linked Devices > Link a device, then scan (expires %s):\n%s\n"})

// Facts supplies protocol facts to native unit fixtures, not a shipping installation.
func Facts(provider string) (channelprovider.Binding, bool) {
	switch provider {
	case "telegram":
		return telegramFacts, true
	case "whatsapp":
		return whatsappFacts, true
	default:
		return channelprovider.Binding{}, false
	}
}

func For(provider string) channelprovider.Binding {
	facts, _ := Facts(provider)
	return facts
}
