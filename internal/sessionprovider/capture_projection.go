package sessionprovider

import (
	"github.com/division-sh/swarm/internal/channelprovider"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/sessioncapture"
)

func withCaptureProvenance(event capturedEvent, request runtimeinbound.Request) (runtimeinbound.Request, error) {
	return sessioncapture.WithCaptureProvenance(event, request)
}
func publicationCaptureProvenance(request runtimeinbound.Request, facts channelprovider.Binding) (capturedEvent, error) {
	return sessioncapture.PublicationCaptureProvenance(request, facts)
}
func publicationRequestBytes(request runtimeinbound.Request) ([]byte, error) {
	return sessioncapture.PublicationRequestBytes(request)
}
func verifyHistoricalCapture(event capturedEvent, record runtimeinbound.Record) ([]byte, error) {
	return sessioncapture.VerifyHistoricalCapture(event, record)
}
