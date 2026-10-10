package sessionprovider

import (
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/sessioncapture"
)

func decodeCapture(raw, digest []byte) (capturedEvent, error) {
	return sessioncapture.DecodeCapture(raw, digest)
}
func withCaptureProvenance(event capturedEvent, request runtimeinbound.Request) (runtimeinbound.Request, error) {
	return sessioncapture.WithCaptureProvenance(event, request)
}
func publicationCaptureProvenance(request runtimeinbound.Request) (capturedEvent, error) {
	return sessioncapture.PublicationCaptureProvenance(request)
}
func publicationRequestBytes(request runtimeinbound.Request) ([]byte, error) {
	return sessioncapture.PublicationRequestBytes(request)
}
func verifyHistoricalCapture(event capturedEvent, record runtimeinbound.Record) ([]byte, error) {
	return sessioncapture.VerifyHistoricalCapture(event, record)
}
