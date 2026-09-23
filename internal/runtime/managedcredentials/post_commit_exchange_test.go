package managedcredentials

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

type postCommitCredentialStore struct {
	runtimeeffects.Store
	launchErr        error
	reject           bool
	cancel           context.CancelFunc
	stale            bool
	staleAfterLaunch bool
}

func (s *postCommitCredentialStore) MarkExternalAttemptLaunched(ctx context.Context, attempt runtimeeffects.Attempt, at time.Time) error {
	if s.reject {
		return s.launchErr
	}
	if err := s.Store.MarkExternalAttemptLaunched(ctx, attempt, at); err != nil {
		return err
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.staleAfterLaunch {
		s.stale = true
	}
	return runtimeeffects.NewPostCommitMutationError(runtimeeffects.MutationLaunch, attempt, s.launchErr)
}

func (s *postCommitCredentialStore) IsExternalEffectAuthorityCurrent(ctx context.Context, authority runtimeeffects.Authority) (bool, error) {
	if s.stale {
		return false, nil
	}
	return s.Store.IsExternalEffectAuthorityCurrent(ctx, authority)
}

func singleCredentialAttempt(t *testing.T, harness *effecttest.Harness) runtimeeffects.Attempt {
	t.Helper()
	if len(harness.Attempts) != 1 {
		t.Fatalf("effect attempts=%d, want one", len(harness.Attempts))
	}
	for _, attempt := range harness.Attempts {
		return attempt
	}
	return runtimeeffects.Attempt{}
}

func credentialPhaseDiagnostic(err error, phase runtimeeffects.MutationPhase, attempt runtimeeffects.Attempt) bool {
	var committed *runtimeeffects.PostCommitMutationError
	return errors.As(err, &committed) && committed.Phase == phase &&
		committed.OperationID == attempt.OperationID && committed.AttemptID == attempt.AttemptID
}

type credentialExchangeRoundTripFunc func(*http.Request) (*http.Response, error)

func (f credentialExchangeRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestManagedCredentialAcknowledgedLaunchCleanupKeepsTokenAndOneRequest(t *testing.T) {
	for _, kind := range []string{"client_credentials", "github_app_installation"} {
		t.Run(kind, func(t *testing.T) {
			harness := effecttest.New()
			cleanup := errors.New("injected acknowledged launch cleanup failure")
			effects := &postCommitCredentialStore{Store: harness, launchErr: cleanup}
			ctx := harness.Context("credential-post-commit-" + kind)
			ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(effects).WithExecutionPosture(executionposture.Live))
			calls := 0
			client := &http.Client{Transport: credentialExchangeRoundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				if err := harness.RequireState("managed_credential", runtimeeffects.StateLaunched); err != nil {
					t.Fatal(err)
				}
				body := `{"access_token":"client-token","expires_in":3600}`
				if kind == "github_app_installation" {
					body = `{"token":"installation-token","expires_at":"2030-01-01T00:00:00Z"}`
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			})}
			store := NewMemoryStore()
			source := &TokenSource{Store: store, HTTPClient: client}
			var record Record
			var err error
			wantToken := "client-token"
			if kind == "client_credentials" {
				record, err = source.ConnectClientCredentials(ctx, ClientCredentialsRequest{
					Key: "credential", Provider: "test", TokenURL: "http://effect.test/token", ClientID: "client-id",
				})
			} else {
				privateKey, _ := testGitHubAppPrivateKey(t)
				wantToken = "installation-token"
				record, err = source.ConnectGitHubAppInstallation(ctx, GitHubAppInstallationRequest{
					Key: "credential", Provider: "github", APIBaseURL: "http://effect.test", ClientID: "github-app-client", InstallationID: "1001", PrivateKey: privateKey,
				})
			}
			if !errors.Is(err, cleanup) || !credentialPhaseDiagnostic(err, runtimeeffects.MutationLaunch, singleCredentialAttempt(t, harness)) || record.Status != StatusConnected || record.AccessToken != wantToken {
				t.Fatalf("acknowledged exchange = status=%q token=%q err=%v", record.Status, record.AccessToken, err)
			}
			persisted, found, loadErr := store.Get(ctx, "credential")
			if loadErr != nil || !found || persisted.Status != StatusConnected || persisted.AccessToken != wantToken {
				t.Fatalf("committed credential readback = %+v, %t, %v", persisted.Descriptor(), found, loadErr)
			}
			if calls != 1 {
				t.Fatalf("HTTP calls=%d, want one", calls)
			}
			if err := harness.RequireState("managed_credential", runtimeeffects.StateSettled); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestManagedCredentialUnacknowledgedLaunchStopsBeforeTransport(t *testing.T) {
	harness := effecttest.New()
	fault := errors.New("injected unacknowledged launch failure")
	effects := &postCommitCredentialStore{Store: harness, launchErr: fault, reject: true}
	ctx := harness.Context("credential-unacknowledged-launch")
	ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(effects).WithExecutionPosture(executionposture.Live))
	calls := 0
	source := &TokenSource{Store: NewMemoryStore(), HTTPClient: &http.Client{Transport: credentialExchangeRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return nil, errors.New("unexpected transport")
	})}}
	record, err := source.ConnectClientCredentials(ctx, ClientCredentialsRequest{
		Key: "credential", Provider: "test", TokenURL: "http://effect.test/token", ClientID: "client-id",
	})
	if err == nil || calls != 0 || record.Status != StatusRefreshFailed || credentialPhaseDiagnostic(err, runtimeeffects.MutationLaunch, singleCredentialAttempt(t, harness)) {
		t.Fatalf("unacknowledged launch = calls=%d status=%q err=%v", calls, record.Status, err)
	}
}

func TestManagedCredentialRefreshReturnsCommittedTokenWithLaunchCleanupError(t *testing.T) {
	for _, via := range []string{"access_token", "explicit_refresh"} {
		t.Run(via, func(t *testing.T) {
			harness := effecttest.New()
			cleanup := errors.New("injected acknowledged refresh launch cleanup failure")
			effects := &postCommitCredentialStore{Store: harness, launchErr: cleanup}
			ctx := harness.Context("credential-refresh-post-commit-" + via)
			ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(effects).WithExecutionPosture(executionposture.Live))
			calls := 0
			store := NewMemoryStore()
			if err := store.Put(ctx, Record{
				Key: "credential", Provider: "test", GrantType: GrantClientCredentials,
				TokenURL: "http://effect.test/token", ClientID: "client-id",
				AccessToken: "old-token", Status: StatusConnected, ExpiresAt: time.Now().Add(-time.Minute),
			}); err != nil {
				t.Fatal(err)
			}
			source := &TokenSource{Store: store, HTTPClient: &http.Client{Transport: credentialExchangeRoundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"access_token":"refreshed-token","expires_in":3600}`)), Header: make(http.Header)}, nil
			})}}
			var token string
			var record Record
			var err error
			if via == "explicit_refresh" {
				token, record, err = source.Refresh(ctx, "credential")
			} else {
				token, record, err = source.AccessToken(ctx, AccessTokenRequest{Key: "credential"})
			}
			if !errors.Is(err, cleanup) || !credentialPhaseDiagnostic(err, runtimeeffects.MutationLaunch, singleCredentialAttempt(t, harness)) || token != "refreshed-token" || record.Status != StatusConnected {
				t.Fatalf("committed refresh = token=%q status=%q err=%v", token, record.Status, err)
			}
			persisted, found, loadErr := store.Get(ctx, "credential")
			if loadErr != nil || !found || persisted.AccessToken != token || persisted.Status != StatusConnected || calls != 1 {
				t.Fatalf("refresh readback = token=%q status=%q found=%t calls=%d err=%v", persisted.AccessToken, persisted.Status, found, calls, loadErr)
			}
		})
	}
}

func TestManagedCredentialAcknowledgedLaunchGuardPreventsDispatch(t *testing.T) {
	for _, kind := range []string{"client_credentials", "github_app_installation"} {
		for _, reason := range []string{"canceled", "stale_authority"} {
			t.Run(kind+"/"+reason, func(t *testing.T) {
				harness := effecttest.New()
				cleanup := errors.New("injected launch cleanup failure")
				effects := &postCommitCredentialStore{Store: harness, launchErr: cleanup, staleAfterLaunch: reason == "stale_authority"}
				ctx := harness.Context("credential-blocked-" + kind + "-" + reason)
				if reason == "canceled" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					effects.cancel = cancel
				}
				ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(effects).WithExecutionPosture(executionposture.Live))
				calls := 0
				source := &TokenSource{Store: NewMemoryStore(), HTTPClient: &http.Client{Transport: credentialExchangeRoundTripFunc(func(*http.Request) (*http.Response, error) {
					calls++
					return nil, errors.New("unexpected transport")
				})}}
				var record Record
				var err error
				if kind == "client_credentials" {
					record, err = source.ConnectClientCredentials(ctx, ClientCredentialsRequest{
						Key: "credential", Provider: "test", TokenURL: "http://effect.test/token", ClientID: "client-id",
					})
				} else {
					privateKey, _ := testGitHubAppPrivateKey(t)
					record, err = source.ConnectGitHubAppInstallation(ctx, GitHubAppInstallationRequest{
						Key: "credential", Provider: "github", APIBaseURL: "http://effect.test", ClientID: "github-app-client", InstallationID: "1001", PrivateKey: privateKey,
					})
				}
				if calls != 0 || record.Status != StatusRefreshFailed || !credentialPhaseDiagnostic(err, runtimeeffects.MutationLaunch, singleCredentialAttempt(t, harness)) {
					t.Fatalf("guard = calls:%d status:%q err:%v", calls, record.Status, err)
				}
				if err := harness.RequireState("managed_credential", runtimeeffects.StateTerminalFailure); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestManagedCredentialAcknowledgedRefreshStillChecksNewScopes(t *testing.T) {
	harness := effecttest.New()
	effects := &postCommitCredentialStore{Store: harness, launchErr: errors.New("injected cleanup failure")}
	ctx := harness.Context("credential-refresh-scope")
	ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(effects).WithExecutionPosture(executionposture.Live))
	store := NewMemoryStore()
	if err := store.Put(ctx, Record{
		Key: "credential", Provider: "test", GrantType: GrantClientCredentials, Scopes: []string{"read"},
		TokenURL: "http://effect.test/token", ClientID: "client-id", AccessToken: "old-token",
		Status: StatusConnected, ExpiresAt: time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	source := &TokenSource{Store: store, HTTPClient: &http.Client{Transport: credentialExchangeRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"access_token":"write-token","scope":"write","expires_in":3600}`)), Header: make(http.Header)}, nil
	})}}
	token, record, err := source.AccessToken(ctx, AccessTokenRequest{Key: "credential", Scopes: []string{"read"}})
	if token != "" || record.AccessToken != "write-token" || calls != 1 || !strings.Contains(err.Error(), "scope-insufficient") ||
		!credentialPhaseDiagnostic(err, runtimeeffects.MutationLaunch, singleCredentialAttempt(t, harness)) {
		t.Fatalf("scope gate = token:%q record:%q calls:%d err:%v", token, record.AccessToken, calls, err)
	}
}
