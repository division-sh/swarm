package bus

import (
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

type apiEventPublicationDeliveryCompletion struct {
	DeliveryID     string `json:"delivery_id"`
	SubscriberType string `json:"subscriber_type"`
	SubscriberID   string `json:"subscriber_id"`
	Status         string `json:"status"`
	Attempt        int    `json:"attempt"`
	RetryCount     int    `json:"retry_count"`
	RetryScheduled bool   `json:"retry_scheduled"`
	Terminal       bool   `json:"terminal"`
}

func withAPIEventPublicationDeliveryCompletion(completion apiidempotency.Completion, command PublicationCommand) (apiidempotency.Completion, error) {
	var response map[string]json.RawMessage
	if err := json.Unmarshal(completion.Response, &response); err != nil {
		return apiidempotency.Completion{}, fmt.Errorf("decode API event publication completion: %w", err)
	}
	event := command.Commit.Event.Event()
	routes := events.NormalizeDeliveryRoutes(command.Commit.DeliveryRoutes)
	deliveries := make([]apiEventPublicationDeliveryCompletion, 0, len(routes))
	for index, route := range routes {
		obligation, err := runtimedelivery.NewObligation(event.ID(), event.RunID(), route, command.Commit.DeliveryAuthority)
		if err != nil {
			return apiidempotency.Completion{}, fmt.Errorf("construct API event publication delivery %d: %w", index, err)
		}
		deliveries = append(deliveries, apiEventPublicationDeliveryCompletion{
			DeliveryID:     obligation.DeliveryID(),
			SubscriberType: string(obligation.SubscriberClass()),
			SubscriberID:   obligation.SubscriberID(),
			Status:         string(runtimedelivery.StatusPending),
			Attempt:        1,
		})
	}
	encodedDeliveries, err := json.Marshal(deliveries)
	if err != nil {
		return apiidempotency.Completion{}, fmt.Errorf("encode API event publication deliveries: %w", err)
	}
	response["deliveries"] = encodedDeliveries
	completion.Response, err = json.Marshal(response)
	if err != nil {
		return apiidempotency.Completion{}, fmt.Errorf("encode API event publication completion: %w", err)
	}
	return completion, nil
}
