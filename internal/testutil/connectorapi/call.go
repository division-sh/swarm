// Package connectorapi shares the concrete provider doubles used by connector journeys.
package connectorapi

import "fmt"

type Call struct {
	Method string
	Path   string
	Auth   string
	Body   map[string]any
	Raw    string
}

func graphMessageSubject(body map[string]any) string {
	message, _ := body["message"].(map[string]any)
	return fmt.Sprint(message["subject"])
}

func graphMessageRecipients(body map[string]any) []any {
	message, _ := body["message"].(map[string]any)
	recipients, _ := message["toRecipients"].([]any)
	return recipients
}
