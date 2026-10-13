package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
)

type failureLogTransportError struct {
	jobID int64
	phase string
	cause error
}

func (e *failureLogTransportError) Error() string {
	reason := fmt.Sprintf("%T", e.cause)
	if errors.Is(e.cause, context.Canceled) {
		reason = "context canceled"
	} else if errors.Is(e.cause, context.DeadlineExceeded) {
		reason = "context deadline exceeded"
	}
	// URL errors, response bodies and auth stderr can contain credentials. Keep
	// the cause private while persisting only the phase and safe classification.
	return fmt.Sprintf("job %d log %s failed (%s)", e.jobID, e.phase, reason)
}

func (e *failureLogTransportError) Unwrap() error { return e.cause }

func failureLogProblem(jobID int64, phase string, err error) error {
	if err == nil {
		return nil
	}
	return &failureLogTransportError{jobID: jobID, phase: phase, cause: err}
}

func readFailureJobLog(ctx context.Context, jobID int64) ([]byte, error) {
	if jobID <= 0 {
		return nil, fmt.Errorf("invalid job log identity")
	}
	command := exec.CommandContext(ctx, "gh", "auth", "token", "--hostname", "github.com")
	token, err := command.Output()
	if err = errors.Join(err, ctx.Err()); err != nil {
		return nil, failureLogProblem(jobID, "authentication", err)
	}
	credential := strings.TrimSpace(string(token))
	if credential == "" || strings.ContainsAny(credential, "\r\n") {
		return nil, fmt.Errorf("job %d log authentication returned an invalid credential", jobID)
	}
	client := &http.Client{
		Transport: http.DefaultTransport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	location, err := failureLogLocation(ctx, client, jobID, credential)
	if err != nil {
		return nil, err
	}
	return downloadFailureLog(ctx, client, jobID, location)
}

func requestFailureLog(ctx context.Context, client *http.Client, jobID int64, phase, address, token string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return nil, failureLogProblem(jobID, phase, err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Accept", "application/vnd.github+json")
		request.Header.Set("X-GitHub-Api-Version", "2026-03-10")
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, failureLogProblem(jobID, phase, err)
	}
	return response, nil
}

func failureLogLocation(ctx context.Context, client *http.Client, jobID int64, token string) (string, error) {
	address := fmt.Sprintf("https://api.github.com/repos/division-sh/swarm/actions/jobs/%d/logs", jobID)
	response, err := requestFailureLog(ctx, client, jobID, "API request", address, token)
	if err != nil {
		return "", err
	}
	var location string
	if response.StatusCode != http.StatusFound {
		err = fmt.Errorf("job %d log API: HTTP %d", jobID, response.StatusCode)
	} else {
		redirect, parseErr := response.Location()
		if parseErr != nil {
			err = failureLogProblem(jobID, "redirect parsing", parseErr)
		} else if !strings.HasPrefix(response.Header.Get("Location"), "https://") || redirect.Scheme != "https" || redirect.Hostname() == "" || redirect.User != nil || redirect.Fragment != "" || redirect.Opaque != "" {
			err = fmt.Errorf("job %d log API returned an invalid HTTPS destination", jobID)
		} else {
			location = redirect.String()
		}
	}
	err = errors.Join(err, failureLogProblem(jobID, "API close", response.Body.Close()), failureLogProblem(jobID, "API cancellation", ctx.Err()))
	if err != nil {
		return "", err
	}
	return location, nil
}

func downloadFailureLog(ctx context.Context, client *http.Client, jobID int64, address string) ([]byte, error) {
	// This is a new, unauthenticated request, even for a same-host destination.
	// The private client has no cookie jar and follows no additional redirects.
	response, err := requestFailureLog(ctx, client, jobID, "download request", address, "")
	if err != nil {
		return nil, err
	}
	var raw []byte
	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("job %d log download: HTTP %d", jobID, response.StatusCode)
	} else {
		raw, err = io.ReadAll(response.Body)
		err = failureLogProblem(jobID, "download read", err)
	}
	err = errors.Join(err, failureLogProblem(jobID, "download close", response.Body.Close()), failureLogProblem(jobID, "download cancellation", ctx.Err()))
	if err != nil {
		return nil, err
	}
	return raw, nil
}
