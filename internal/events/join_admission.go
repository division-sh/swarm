package events

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
)

type JoinAdmissionDisposition string

const (
	JoinAdmissionBound JoinAdmissionDisposition = "bound"
	JoinAdmissionEarly JoinAdmissionDisposition = "early"
)

// JoinAdmissionReceipt belongs to one publication route, not its producer's
// ambient context. A retry resumes it; a new business output admits its own.
type JoinAdmissionReceipt struct {
	Ref         timeridentity.JoinRef    `json:"ref"`
	Disposition JoinAdmissionDisposition `json:"disposition"`
}

func (r JoinAdmissionReceipt) Validate() error {
	if !r.Ref.Valid() || r.Ref.Mode() != timeridentity.JoinRefModeArrival {
		return fmt.Errorf("join admission requires an exact arrival declaration")
	}
	switch r.Disposition {
	case JoinAdmissionBound:
		if r.Ref.StageEntry().Empty() {
			return fmt.Errorf("bound join admission requires its retained lifecycle entry")
		}
	case JoinAdmissionEarly:
		if !r.Ref.Equal(r.Ref.Declaration()) {
			return fmt.Errorf("early join admission cannot invent an entry")
		}
	default:
		return fmt.Errorf("join admission disposition %q is invalid", r.Disposition)
	}
	return nil
}

func (c DeliveryContext) Validate() error {
	seen := make(map[string]bool, len(c.Joins))
	for _, receipt := range c.Joins {
		if err := receipt.Validate(); err != nil {
			return err
		}
		key := receipt.Ref.Declaration().Key()
		if seen[key] {
			return fmt.Errorf("delivery route repeats join admission %s", key)
		}
		seen[key] = true
	}
	return nil
}

func (c DeliveryContext) JoinAdmission(declaration timeridentity.JoinRef) (JoinAdmissionReceipt, bool) {
	for _, receipt := range c.Joins {
		if receipt.Ref.Declaration().Equal(declaration.Declaration()) {
			return receipt, true
		}
	}
	return JoinAdmissionReceipt{}, false
}

func (c DeliveryContext) ReplyOnly() DeliveryContext {
	return DeliveryContext{Reply: c.Reply}.Normalized()
}

func (c DeliveryContext) Identity() (string, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(c.Normalized())
	return string(raw), err
}

func normalizeJoinAdmissions(receipts []JoinAdmissionReceipt) []JoinAdmissionReceipt {
	result := append([]JoinAdmissionReceipt(nil), receipts...)
	sort.Slice(result, func(i, j int) bool { return result[i].Ref.Declaration().Key() < result[j].Ref.Declaration().Key() })
	return result
}
