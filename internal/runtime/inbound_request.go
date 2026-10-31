package runtime

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/division-sh/swarm/internal/providertriggers"
)

type inboundWebhookTransport struct {
	body            []byte
	url             string
	query           url.Values
	queryParseError string
	form            url.Values
	formParsed      bool
	formParseError  string
}

func readInboundWebhookTransport(r *http.Request) (inboundWebhookTransport, *providertriggers.Error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, inboundWebhookMaxBodyBytes+1))
	if err != nil {
		return inboundWebhookTransport{}, &providertriggers.Error{Status: http.StatusBadRequest, Message: "read body failed"}
	}
	if len(body) > inboundWebhookMaxBodyBytes {
		return inboundWebhookTransport{}, &providertriggers.Error{Status: http.StatusRequestEntityTooLarge, Message: "webhook body too large"}
	}
	requestURL := inboundRequestURL(r)
	queryValues, queryParseError := inboundQueryValues(r)
	formValues, formParsed, formParseError := inboundFormValues(r.Header.Get("Content-Type"), body)
	return inboundWebhookTransport{
		body: body, url: requestURL,
		query: queryValues, queryParseError: queryParseError,
		form: formValues, formParsed: formParsed, formParseError: formParseError,
	}, nil
}

func (transport inboundWebhookTransport) providerRequest(r *http.Request) providertriggers.Request {
	var payload any
	decoder := json.NewDecoder(bytes.NewReader(transport.body))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		payload = string(transport.body)
	} else if err := decoder.Decode(&struct{}{}); err != io.EOF {
		payload = string(transport.body)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	return providertriggers.Request{
		Method:          r.Method,
		URL:             transport.url,
		Body:            transport.body,
		Headers:         r.Header,
		Payload:         payload,
		ContentType:     r.Header.Get("Content-Type"),
		Query:           transport.query,
		QueryParseError: transport.queryParseError,
		Form:            transport.form,
		FormParsed:      transport.formParsed,
		FormParseError:  transport.formParseError,
		Received:        now,
		UserAgent:       r.UserAgent(),
	}
}
