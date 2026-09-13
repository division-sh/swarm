package apiidempotency

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

var ErrConflict = errors.New("api idempotency conflict")

type ActorKind string

const (
	ActorBearerToken       ActorKind = "bearer_token"
	ActorOperatorPrincipal ActorKind = "operator_principal"
)

type Actor struct {
	Kind ActorKind
	ID   string
}

func BearerActor(id string) Actor    { return Actor{Kind: ActorBearerToken, ID: id} }
func PrincipalActor(id string) Actor { return Actor{Kind: ActorOperatorPrincipal, ID: id} }

func IsHumanMailboxMethod(method string) bool {
	switch method {
	case "mailbox.decide", "mailbox.defer", "mailbox.begin_input", "mailbox.cancel_input", "mailbox.acknowledge":
		return true
	default:
		return false
	}
}

func (a Actor) ValidateMethod(method string) error {
	if a.ID == "" || a.ID != strings.TrimSpace(a.ID) || method == "" {
		return fmt.Errorf("exact replay actor and method are required")
	}
	if IsHumanMailboxMethod(method) {
		if a.Kind != ActorOperatorPrincipal {
			return fmt.Errorf("%s requires an operator-principal replay actor", method)
		}
		id, err := uuid.Parse(a.ID)
		if err != nil || id == uuid.Nil || id.String() != a.ID {
			return fmt.Errorf("operator-principal replay actor must be a canonical nonzero UUID")
		}
	} else if a.Kind != ActorBearerToken {
		return fmt.Errorf("%s requires a bearer-token replay actor", method)
	}
	return nil
}

type Request struct {
	Method         string
	Actor          Actor
	IdempotencyKey string
	RequestHash    string
	ResourceID     string
	TTL            time.Duration
	Now            time.Time
}

type Completion struct {
	ResourceID string
	Response   json.RawMessage
}

type ConflictError struct {
	OriginalRequestHash    string
	ConflictingRequestHash string
	Method                 string
	ResourceID             string
}

func (e *ConflictError) Error() string { return "api idempotency conflict" }

func (e *ConflictError) Is(target error) bool { return target == ErrConflict }
