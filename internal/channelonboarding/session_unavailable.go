package channelonboarding

import (
	"errors"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

var ErrSessionUnavailable = errors.New("WhatsApp retained transport is temporarily unavailable")
var ErrSessionNotLinked = errors.New("Not linked: run swarm channel connect whatsapp")

func SessionUnavailableOnly(err error) bool {
	return failures.OnlyBranches(err, func(e error) bool {
		return e == ErrSessionUnavailable || e == ErrSessionNotLinked
	})
}
