package managedcredentials

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
)

const (
	GrantAuthorizationCode     = "authorization_code"
	GrantAuthorizationCodePKCE = "authorization_code_pkce"
	GrantClientCredentials     = "client_credentials"
	GrantGitHubAppInstallation = "github_app_installation"

	StatusUnconnected       = "unconnected"
	StatusPendingConsent    = "pending_consent"
	StatusConnected         = "connected"
	StatusRefreshFailed     = "refresh_failed"
	StatusScopeInsufficient = "scope_insufficient"
)

const defaultRefreshWindow = 60 * time.Second

var ErrStoreLocked = errors.New("managed credential store is locked")

var ErrStoreLockUnsupported = errors.New("managed credential store locking is unsupported on this platform")

type Store interface {
	Get(ctx context.Context, key string) (Record, bool, error)
	Put(ctx context.Context, record Record) error
	Delete(ctx context.Context, key string) error
	List(ctx context.Context) ([]Descriptor, error)
}

type Record struct {
	Key             string                                     `json:"key"`
	Provider        string                                     `json:"provider"`
	Account         string                                     `json:"account,omitempty"`
	GrantType       string                                     `json:"grant_type"`
	AuthURL         string                                     `json:"auth_url,omitempty"`
	TokenURL        string                                     `json:"token_url"`
	APIBaseURL      string                                     `json:"api_base_url,omitempty"`
	ClientID        string                                     `json:"client_id"`
	ClientSecret    string                                     `json:"client_secret,omitempty"`
	InstallationID  string                                     `json:"installation_id,omitempty"`
	PrivateKey      string                                     `json:"private_key,omitempty"`
	RedirectURL     string                                     `json:"redirect_url,omitempty"`
	Scopes          []string                                   `json:"scopes,omitempty"`
	GrantModel      string                                     `json:"grant_model,omitempty"`
	TokenRequest    managedcredentialmodel.TokenRequestProfile `json:"token_request,omitempty"`
	AccessToken     string                                     `json:"access_token,omitempty"`
	RefreshToken    string                                     `json:"refresh_token,omitempty"`
	TokenType       string                                     `json:"token_type,omitempty"`
	ExpiresAt       time.Time                                  `json:"expires_at,omitempty"`
	Status          string                                     `json:"status"`
	Failure         string                                     `json:"failure,omitempty"`
	PKCEVerifier    string                                     `json:"pkce_verifier,omitempty"`
	PKCEChallenge   string                                     `json:"pkce_challenge,omitempty"`
	OAuthState      string                                     `json:"oauth_state,omitempty"`
	RefreshWindowMS int64                                      `json:"refresh_window_ms,omitempty"`
	UpdatedAt       time.Time                                  `json:"updated_at"`
}

type Descriptor struct {
	Key            string                                     `json:"key"`
	Provider       string                                     `json:"provider,omitempty"`
	Account        string                                     `json:"account,omitempty"`
	GrantType      string                                     `json:"grant_type,omitempty"`
	Scopes         []string                                   `json:"scopes,omitempty"`
	GrantModel     string                                     `json:"grant_model,omitempty"`
	TokenRequest   managedcredentialmodel.TokenRequestProfile `json:"token_request,omitempty"`
	InstallationID string                                     `json:"installation_id,omitempty"`
	APIBaseURL     string                                     `json:"api_base_url,omitempty"`
	Status         string                                     `json:"status"`
	Failure        string                                     `json:"failure,omitempty"`
	ExpiresAt      time.Time                                  `json:"expires_at,omitempty"`
	UpdatedAt      time.Time                                  `json:"updated_at,omitempty"`
}

type BeginAuthCodeRequest struct {
	Key          string
	Provider     string
	AuthURL      string
	TokenURL     string
	ClientID     string
	ClientSecret string
	RedirectURL  string
	Scopes       []string
	GrantModel   string
	TokenRequest managedcredentialmodel.TokenRequestProfile
	Account      string
}

type BeginAuthCodeResult struct {
	Key          string
	State        string
	CodeVerifier string
	AuthorizeURL string
}

type CompleteAuthCodeRequest struct {
	Key   string
	State string
	Code  string
}

type ClientCredentialsRequest struct {
	Key          string
	Provider     string
	TokenURL     string
	ClientID     string
	ClientSecret string
	Scopes       []string
	GrantModel   string
	TokenRequest managedcredentialmodel.TokenRequestProfile
	Account      string
}

type GitHubAppInstallationRequest struct {
	Key            string
	Provider       string
	APIBaseURL     string
	ClientID       string
	InstallationID string
	PrivateKey     string
	Account        string
}

type AccessTokenRequest struct {
	Key            string
	GrantType      string
	Scopes         []string
	GrantModel     string
	TokenRequest   managedcredentialmodel.TokenRequestProfile
	InstallationID string
}

type TokenSource struct {
	Store         Store
	HTTPClient    *http.Client
	Now           func() time.Time
	RefreshWindow time.Duration
}

func DefaultFilePath() (string, error) {
	if raw := strings.TrimSpace(os.Getenv("SWARM_MANAGED_CREDENTIALS_FILE")); raw != "" {
		return raw, nil
	}
	cfg, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(cfg, "swarm", "managed_credentials.json"), nil
}

func NewDefaultFileStore() (*FileStore, error) {
	path, err := DefaultFilePath()
	if err != nil {
		return nil, err
	}
	return NewFileStore(path)
}

func (s *TokenSource) BeginAuthCodePKCE(ctx context.Context, req BeginAuthCodeRequest) (BeginAuthCodeResult, error) {
	return s.beginAuthCode(ctx, req, true)
}

func (s *TokenSource) BeginAuthCode(ctx context.Context, req BeginAuthCodeRequest) (BeginAuthCodeResult, error) {
	return s.beginAuthCode(ctx, req, false)
}

func (s *TokenSource) beginAuthCode(ctx context.Context, req BeginAuthCodeRequest, usePKCE bool) (BeginAuthCodeResult, error) {
	if s == nil || s.Store == nil {
		return BeginAuthCodeResult{}, fmt.Errorf("managed credential store is not configured")
	}
	key := strings.TrimSpace(req.Key)
	if key == "" {
		return BeginAuthCodeResult{}, fmt.Errorf("managed credential key is required")
	}
	authURL := strings.TrimSpace(req.AuthURL)
	tokenURL := strings.TrimSpace(req.TokenURL)
	clientID := strings.TrimSpace(req.ClientID)
	redirectURL := strings.TrimSpace(req.RedirectURL)
	if authURL == "" || tokenURL == "" || clientID == "" || redirectURL == "" {
		return BeginAuthCodeResult{}, fmt.Errorf("auth_url, token_url, client_id, and redirect_url are required for authorization_code grants")
	}
	if err := managedcredentialmodel.ValidateGrantModel(req.GrantModel); err != nil {
		return BeginAuthCodeResult{}, err
	}
	if err := managedcredentialmodel.ValidateTokenRequestProfile(req.TokenRequest); err != nil {
		return BeginAuthCodeResult{}, err
	}
	if err := validateTokenRequestClientAuthConfig(key, clientID, req.ClientSecret, req.TokenRequest); err != nil {
		return BeginAuthCodeResult{}, err
	}
	verifier := ""
	challenge := ""
	var err error
	if usePKCE {
		verifier, err = randomURLToken(48)
		if err != nil {
			return BeginAuthCodeResult{}, err
		}
		challenge = pkceChallenge(verifier)
	}
	state, err := randomURLToken(32)
	if err != nil {
		return BeginAuthCodeResult{}, err
	}
	values := url.Values{}
	values.Set("response_type", "code")
	values.Set("client_id", clientID)
	values.Set("redirect_uri", redirectURL)
	if usePKCE {
		values.Set("code_challenge", challenge)
		values.Set("code_challenge_method", "S256")
	}
	values.Set("state", state)
	if scopes := normalizeStrings(req.Scopes); len(scopes) > 0 {
		values.Set("scope", strings.Join(scopes, " "))
	}
	parsed, err := url.Parse(authURL)
	if err != nil {
		return BeginAuthCodeResult{}, fmt.Errorf("parse auth_url: %w", err)
	}
	query := parsed.Query()
	for key, items := range values {
		for _, item := range items {
			query.Add(key, item)
		}
	}
	parsed.RawQuery = query.Encode()
	grantType := GrantAuthorizationCode
	if usePKCE {
		grantType = GrantAuthorizationCodePKCE
	}
	record := Record{
		Key:           key,
		Provider:      strings.TrimSpace(req.Provider),
		Account:       strings.TrimSpace(req.Account),
		GrantType:     grantType,
		AuthURL:       authURL,
		TokenURL:      tokenURL,
		ClientID:      clientID,
		ClientSecret:  strings.TrimSpace(req.ClientSecret),
		RedirectURL:   redirectURL,
		Scopes:        normalizeStrings(req.Scopes),
		GrantModel:    managedcredentialmodel.NormalizeGrantModel(req.GrantModel),
		TokenRequest:  managedcredentialmodel.NormalizeTokenRequestProfile(req.TokenRequest),
		Status:        StatusPendingConsent,
		PKCEVerifier:  verifier,
		PKCEChallenge: challenge,
		OAuthState:    state,
		UpdatedAt:     s.now(),
	}
	if err := s.Store.Put(ctx, record); err != nil {
		return BeginAuthCodeResult{}, err
	}
	return BeginAuthCodeResult{
		Key:          key,
		State:        state,
		CodeVerifier: verifier,
		AuthorizeURL: parsed.String(),
	}, nil
}

func (s *TokenSource) CompleteAuthCode(ctx context.Context, req CompleteAuthCodeRequest) (Record, error) {
	record, ok, err := s.record(ctx, req.Key)
	if err != nil {
		return Record{}, err
	}
	if !ok {
		return Record{}, fmt.Errorf("missing managed credential %q", strings.TrimSpace(req.Key))
	}
	if record.GrantType != GrantAuthorizationCodePKCE && record.GrantType != GrantAuthorizationCode {
		return Record{}, fmt.Errorf("managed credential %q grant_type is %q, want %s or %s", record.Key, record.GrantType, GrantAuthorizationCodePKCE, GrantAuthorizationCode)
	}
	if strings.TrimSpace(record.OAuthState) == "" || strings.TrimSpace(req.State) != record.OAuthState {
		return Record{}, fmt.Errorf("managed credential %q callback state mismatch", record.Key)
	}
	if strings.TrimSpace(req.Code) == "" {
		return Record{}, fmt.Errorf("authorization code is required")
	}
	values := url.Values{}
	values.Set("grant_type", "authorization_code")
	values.Set("code", strings.TrimSpace(req.Code))
	values.Set("redirect_uri", strings.TrimSpace(record.RedirectURL))
	if record.GrantType == GrantAuthorizationCodePKCE {
		values.Set("code_verifier", strings.TrimSpace(record.PKCEVerifier))
	}
	updated, err := s.exchange(ctx, record, values)
	if err != nil {
		return s.markFailure(ctx, record, err, req.Code)
	}
	updated.Status = StatusConnected
	updated.Failure = ""
	updated.OAuthState = ""
	updated.PKCEVerifier = ""
	updated.PKCEChallenge = ""
	updated.UpdatedAt = s.now()
	if err := s.Store.Put(ctx, updated); err != nil {
		return Record{}, err
	}
	return updated, nil
}

func (s *TokenSource) ConnectClientCredentials(ctx context.Context, req ClientCredentialsRequest) (Record, error) {
	if s == nil || s.Store == nil {
		return Record{}, fmt.Errorf("managed credential store is not configured")
	}
	record := Record{
		Key:          strings.TrimSpace(req.Key),
		Provider:     strings.TrimSpace(req.Provider),
		Account:      strings.TrimSpace(req.Account),
		GrantType:    GrantClientCredentials,
		TokenURL:     strings.TrimSpace(req.TokenURL),
		ClientID:     strings.TrimSpace(req.ClientID),
		ClientSecret: strings.TrimSpace(req.ClientSecret),
		Scopes:       normalizeStrings(req.Scopes),
		GrantModel:   managedcredentialmodel.NormalizeGrantModel(req.GrantModel),
		TokenRequest: managedcredentialmodel.NormalizeTokenRequestProfile(req.TokenRequest),
		Status:       StatusUnconnected,
		UpdatedAt:    s.now(),
	}
	if record.Key == "" || record.TokenURL == "" || record.ClientID == "" {
		return Record{}, fmt.Errorf("key, token_url, and client_id are required for client_credentials")
	}
	if err := managedcredentialmodel.ValidateGrantModel(record.GrantModel); err != nil {
		return Record{}, err
	}
	if err := managedcredentialmodel.ValidateTokenRequestProfile(record.TokenRequest); err != nil {
		return Record{}, err
	}
	if err := validateTokenRequestClientAuthConfig(record.Key, record.ClientID, record.ClientSecret, record.TokenRequest); err != nil {
		return Record{}, err
	}
	values := url.Values{}
	values.Set("grant_type", "client_credentials")
	if len(record.Scopes) > 0 {
		values.Set("scope", strings.Join(record.Scopes, " "))
	}
	updated, err := s.exchange(ctx, record, values)
	if err != nil {
		return s.markFailure(ctx, record, err)
	}
	updated.Status = StatusConnected
	updated.UpdatedAt = s.now()
	if err := s.Store.Put(ctx, updated); err != nil {
		return Record{}, err
	}
	return updated, nil
}

func (s *TokenSource) ConnectGitHubAppInstallation(ctx context.Context, req GitHubAppInstallationRequest) (Record, error) {
	if s == nil || s.Store == nil {
		return Record{}, fmt.Errorf("managed credential store is not configured")
	}
	record := Record{
		Key:            strings.TrimSpace(req.Key),
		Provider:       firstNonEmpty(strings.TrimSpace(req.Provider), "github"),
		Account:        strings.TrimSpace(req.Account),
		GrantType:      GrantGitHubAppInstallation,
		APIBaseURL:     normalizeGitHubAPIBaseURL(req.APIBaseURL),
		ClientID:       strings.TrimSpace(req.ClientID),
		InstallationID: normalizeInstallationID(req.InstallationID),
		PrivateKey:     strings.TrimSpace(req.PrivateKey),
		GrantModel:     managedcredentialmodel.GrantModelInstallation,
		Status:         StatusUnconnected,
		UpdatedAt:      s.now(),
	}
	if record.Key == "" || record.ClientID == "" || record.InstallationID == "" || record.PrivateKey == "" {
		return Record{}, fmt.Errorf("key, client_id, installation_id, and private_key are required for github_app_installation")
	}
	if _, err := parseRSAPrivateKey(record.PrivateKey); err != nil {
		return Record{}, fmt.Errorf("managed credential %q private_key is invalid: %w", record.Key, err)
	}
	updated, err := s.exchangeGitHubAppInstallation(ctx, record, record.InstallationID)
	if err != nil {
		return s.markFailure(ctx, record, err)
	}
	updated.Status = StatusConnected
	updated.Failure = ""
	updated.UpdatedAt = s.now()
	if err := s.Store.Put(ctx, updated); err != nil {
		return Record{}, err
	}
	return updated, nil
}

func (s *TokenSource) AccessToken(ctx context.Context, req AccessTokenRequest) (string, Record, error) {
	record, ok, err := s.record(ctx, req.Key)
	if err != nil {
		return "", Record{}, err
	}
	if !ok {
		return "", Record{}, fmt.Errorf("missing managed credential %q", strings.TrimSpace(req.Key))
	}
	if err := GrantTypeCovers(record.GrantType, req.GrantType); err != nil {
		return "", Record{}, fmt.Errorf("managed credential %q grant-type-insufficient: %w", record.Key, err)
	}
	if err := managedcredentialmodel.GrantModelCovers(record.GrantModel, req.GrantModel); err != nil {
		return "", Record{}, fmt.Errorf("managed credential %q grant-model-insufficient: %w", record.Key, err)
	}
	if err := managedcredentialmodel.TokenRequestProfileCovers(record.TokenRequest, req.TokenRequest); err != nil {
		return "", Record{}, fmt.Errorf("managed credential %q token-request-insufficient: %w", record.Key, err)
	}
	if err := ensureScopes(record.Scopes, req.Scopes); err != nil {
		return "", Record{}, fmt.Errorf("managed credential %q scope-insufficient: %w", record.Key, err)
	}
	if err := ensureInstallationSelection(record, req.InstallationID); err != nil {
		return "", Record{}, err
	}
	if strings.TrimSpace(record.Status) != StatusConnected {
		return "", Record{}, fmt.Errorf("managed credential %q is %s", record.Key, statusOrUnconnected(record.Status))
	}
	if strings.TrimSpace(record.AccessToken) == "" || s.shouldRefresh(record) {
		record, err = s.refresh(ctx, record)
		if err != nil {
			return "", record, err
		}
		if err := GrantTypeCovers(record.GrantType, req.GrantType); err != nil {
			return "", record, fmt.Errorf("managed credential %q grant-type-insufficient: %w", record.Key, err)
		}
		if err := ensureScopes(record.Scopes, req.Scopes); err != nil {
			return "", record, fmt.Errorf("managed credential %q scope-insufficient: %w", record.Key, err)
		}
		if err := managedcredentialmodel.GrantModelCovers(record.GrantModel, req.GrantModel); err != nil {
			return "", record, fmt.Errorf("managed credential %q grant-model-insufficient: %w", record.Key, err)
		}
		if err := managedcredentialmodel.TokenRequestProfileCovers(record.TokenRequest, req.TokenRequest); err != nil {
			return "", record, fmt.Errorf("managed credential %q token-request-insufficient: %w", record.Key, err)
		}
		if err := ensureInstallationSelection(record, req.InstallationID); err != nil {
			return "", record, err
		}
	}
	return record.AccessToken, record, nil
}

func (s *TokenSource) Refresh(ctx context.Context, key string) (string, Record, error) {
	record, ok, err := s.record(ctx, key)
	if err != nil {
		return "", Record{}, err
	}
	if !ok {
		return "", Record{}, fmt.Errorf("missing managed credential %q", strings.TrimSpace(key))
	}
	record, err = s.refresh(ctx, record)
	if err != nil {
		return "", record, err
	}
	return record.AccessToken, record, nil
}

func (s *TokenSource) refresh(ctx context.Context, record Record) (Record, error) {
	values := url.Values{}
	switch record.GrantType {
	case GrantAuthorizationCodePKCE, GrantAuthorizationCode:
		if strings.TrimSpace(record.RefreshToken) == "" {
			err := fmt.Errorf("managed credential %q has no refresh token", record.Key)
			return s.markFailure(ctx, record, err)
		}
		values.Set("grant_type", "refresh_token")
		values.Set("refresh_token", strings.TrimSpace(record.RefreshToken))
	case GrantClientCredentials:
		values.Set("grant_type", "client_credentials")
		if len(record.Scopes) > 0 {
			values.Set("scope", strings.Join(record.Scopes, " "))
		}
	case GrantGitHubAppInstallation:
		updated, err := s.exchangeGitHubAppInstallation(ctx, record, record.InstallationID)
		if err != nil {
			return s.markFailure(ctx, record, err)
		}
		updated.Status = StatusConnected
		updated.Failure = ""
		updated.UpdatedAt = s.now()
		if err := s.Store.Put(ctx, updated); err != nil {
			return Record{}, err
		}
		return updated, nil
	default:
		err := fmt.Errorf("managed credential %q has unsupported grant_type %q", record.Key, record.GrantType)
		return s.markFailure(ctx, record, err)
	}
	updated, err := s.exchange(ctx, record, values)
	if err != nil {
		return s.markFailure(ctx, record, err)
	}
	updated.Status = StatusConnected
	updated.Failure = ""
	updated.UpdatedAt = s.now()
	if err := s.Store.Put(ctx, updated); err != nil {
		return Record{}, err
	}
	return updated, nil
}

func (s *TokenSource) exchange(ctx context.Context, record Record, values url.Values) (Record, error) {
	tokenURL := strings.TrimSpace(record.TokenURL)
	if tokenURL == "" {
		return Record{}, fmt.Errorf("managed credential %q token_url is required", record.Key)
	}
	client := s.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	profile := managedcredentialmodel.NormalizeTokenRequestProfile(record.TokenRequest)
	if err := managedcredentialmodel.ValidateTokenRequestProfile(profile); err != nil {
		return Record{}, fmt.Errorf("managed credential %q has invalid token_request: %w", record.Key, err)
	}
	if err := applyTokenRequestClientAuth(values, record, profile); err != nil {
		return Record{}, err
	}
	requestBody, contentType, err := encodeTokenRequestBody(values, profile)
	if err != nil {
		return Record{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, requestBody)
	if err != nil {
		return Record{}, err
	}
	req.Header.Set("Content-Type", contentType)
	if profile.ClientAuth == managedcredentialmodel.TokenClientAuthBasic {
		req.SetBasicAuth(strings.TrimSpace(record.ClientID), strings.TrimSpace(record.ClientSecret))
	}
	for key, value := range profile.StaticHeaders {
		req.Header.Set(key, value)
	}
	attempt, err := runtimeeffects.Begin(ctx, "managed_credential", []byte(tokenURL+"\x00"+values.Encode()), map[string]string{"credential_key": record.Key})
	if err != nil {
		return Record{}, err
	}
	if err := attempt.MarkLaunched(ctx); err != nil {
		return Record{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return Record{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "managed_credential_request_outcome_unconfirmed", "managed-credentials", "exchange", map[string]any{"credential_key": record.Key, "stage": "transport"}, err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
		TokenType    string `json:"token_type"`
		Scope        string `json:"scope"`
		Error        string `json:"error"`
		Description  string `json:"error_description"`
		Message      string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return Record{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "managed_credential_request_outcome_unconfirmed", "managed-credentials", "exchange", map[string]any{"credential_key": record.Key, "stage": "decode"}, err)
	}
	if resp.StatusCode >= 400 {
		msg := strings.TrimSpace(body.Error)
		if msg == "" {
			msg = fmt.Sprintf("token endpoint returned status %d", resp.StatusCode)
		}
		if desc := strings.TrimSpace(body.Description); desc != "" {
			msg += ": " + desc
		} else if desc := strings.TrimSpace(body.Message); desc != "" {
			msg += ": " + desc
		}
		return Record{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "managed_credential_status_outcome_unconfirmed", "managed-credentials", "exchange", map[string]any{"credential_key": record.Key, "status": resp.StatusCode}, fmt.Errorf("%s", msg))
	}
	access := strings.TrimSpace(body.AccessToken)
	if access == "" {
		err := fmt.Errorf("token endpoint did not return access_token")
		return Record{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "managed_credential_result_outcome_unconfirmed", "managed-credentials", "exchange", map[string]any{"credential_key": record.Key}, err)
	}
	updated := record
	updated.AccessToken = access
	if refresh := strings.TrimSpace(body.RefreshToken); refresh != "" {
		updated.RefreshToken = refresh
	}
	if tokenType := strings.TrimSpace(body.TokenType); tokenType != "" {
		updated.TokenType = tokenType
	} else if updated.TokenType == "" {
		updated.TokenType = "Bearer"
	}
	if scope := normalizeScopeString(body.Scope); len(scope) > 0 {
		updated.Scopes = scope
	}
	if body.ExpiresIn > 0 {
		updated.ExpiresAt = s.now().Add(time.Duration(body.ExpiresIn) * time.Second).UTC()
	}
	if err := attempt.Succeed(ctx, map[string]any{"credential_key": record.Key, "status": resp.StatusCode}); err != nil {
		return Record{}, err
	}
	return updated, nil
}

func (s *TokenSource) exchangeGitHubAppInstallation(ctx context.Context, record Record, installationID string) (Record, error) {
	installationID = normalizeInstallationID(installationID)
	if installationID == "" {
		return Record{}, fmt.Errorf("managed credential %q installation_id is required", record.Key)
	}
	if record.InstallationID != "" && normalizeInstallationID(record.InstallationID) != installationID {
		return Record{}, fmt.Errorf("managed credential %q installation_id mismatch", record.Key)
	}
	if strings.TrimSpace(record.ClientID) == "" {
		return Record{}, fmt.Errorf("managed credential %q client_id is required", record.Key)
	}
	if strings.TrimSpace(record.PrivateKey) == "" {
		return Record{}, fmt.Errorf("managed credential %q private_key is required", record.Key)
	}
	jwt, err := s.githubAppJWT(record)
	if err != nil {
		return Record{}, err
	}
	tokenURL, err := githubInstallationAccessTokenURL(record, installationID)
	if err != nil {
		return Record{}, err
	}
	client := s.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, bytes.NewReader([]byte("{}")))
	if err != nil {
		return Record{}, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	attempt, err := runtimeeffects.Begin(ctx, "managed_credential", []byte(tokenURL+"\x00"+installationID), map[string]string{"credential_key": record.Key})
	if err != nil {
		return Record{}, err
	}
	if err := attempt.MarkLaunched(ctx); err != nil {
		return Record{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		redacted := fmt.Errorf("%s", RedactString(err.Error(), jwt))
		return Record{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "managed_credential_request_outcome_unconfirmed", "managed-credentials", "exchange_github_app", map[string]any{"credential_key": record.Key, "stage": "transport"}, redacted)
	}
	defer resp.Body.Close()
	var body struct {
		Token     string `json:"token"`
		ExpiresAt string `json:"expires_at"`
		Error     string `json:"error"`
		Message   string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return Record{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "managed_credential_request_outcome_unconfirmed", "managed-credentials", "exchange_github_app", map[string]any{"credential_key": record.Key, "stage": "decode"}, err)
	}
	if resp.StatusCode >= 400 {
		msg := strings.TrimSpace(body.Error)
		if msg == "" {
			msg = strings.TrimSpace(body.Message)
		}
		if msg == "" {
			msg = fmt.Sprintf("github installation token endpoint returned status %d", resp.StatusCode)
		}
		return Record{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "managed_credential_status_outcome_unconfirmed", "managed-credentials", "exchange_github_app", map[string]any{"credential_key": record.Key, "status": resp.StatusCode}, fmt.Errorf("%s", RedactString(msg, jwt)))
	}
	token := strings.TrimSpace(body.Token)
	if token == "" {
		err := fmt.Errorf("github installation token endpoint did not return token")
		return Record{}, attempt.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "managed_credential_result_outcome_unconfirmed", "managed-credentials", "exchange_github_app", map[string]any{"credential_key": record.Key}, err)
	}
	updated := record
	updated.AccessToken = token
	updated.TokenType = "Bearer"
	updated.GrantType = GrantGitHubAppInstallation
	updated.GrantModel = managedcredentialmodel.GrantModelInstallation
	updated.InstallationID = installationID
	if expires := strings.TrimSpace(body.ExpiresAt); expires != "" {
		parsed, err := time.Parse(time.RFC3339, expires)
		if err != nil {
			return Record{}, fmt.Errorf("parse github installation token expires_at: %w", err)
		}
		updated.ExpiresAt = parsed.UTC()
	}
	if err := attempt.Succeed(ctx, map[string]any{"credential_key": record.Key, "status": resp.StatusCode}); err != nil {
		return Record{}, err
	}
	return updated, nil
}

func (s *TokenSource) githubAppJWT(record Record) (string, error) {
	key, err := parseRSAPrivateKey(record.PrivateKey)
	if err != nil {
		return "", fmt.Errorf("managed credential %q private_key is invalid: %w", record.Key, err)
	}
	now := s.now()
	header := struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}{Alg: "RS256", Typ: "JWT"}
	claims := struct {
		Iss string `json:"iss"`
		Iat int64  `json:"iat"`
		Exp int64  `json:"exp"`
	}{
		Iss: strings.TrimSpace(record.ClientID),
		Iat: now.Add(-60 * time.Second).Unix(),
		Exp: now.Add(9 * time.Minute).Unix(),
	}
	if claims.Iss == "" {
		return "", fmt.Errorf("managed credential %q client_id is required", record.Key)
	}
	headerRaw, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	claimsRaw, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encoder := base64.RawURLEncoding
	signingInput := encoder.EncodeToString(headerRaw) + "." + encoder.EncodeToString(claimsRaw)
	sum := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		return "", fmt.Errorf("sign github app jwt: %w", err)
	}
	return signingInput + "." + encoder.EncodeToString(sig), nil
}

func parseRSAPrivateKey(raw string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(strings.TrimSpace(raw)))
	if block == nil {
		return nil, fmt.Errorf("PEM block is required")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("private key must be RSA")
	}
	return key, nil
}

func githubInstallationAccessTokenURL(record Record, installationID string) (string, error) {
	base := normalizeGitHubAPIBaseURL(record.APIBaseURL)
	parsed, err := url.Parse(base)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("managed credential %q api_base_url is invalid", record.Key)
	}
	return strings.TrimRight(base, "/") + "/app/installations/" + url.PathEscape(installationID) + "/access_tokens", nil
}

func applyTokenRequestClientAuth(values url.Values, record Record, profile managedcredentialmodel.TokenRequestProfile) error {
	clientID := strings.TrimSpace(record.ClientID)
	clientSecret := strings.TrimSpace(record.ClientSecret)
	if err := validateTokenRequestClientAuthConfig(record.Key, clientID, clientSecret, profile); err != nil {
		return err
	}
	switch managedcredentialmodel.NormalizeTokenRequestProfile(profile).ClientAuth {
	case managedcredentialmodel.TokenClientAuthPost:
		values.Set("client_id", clientID)
		if clientSecret != "" {
			values.Set("client_secret", clientSecret)
		}
	case managedcredentialmodel.TokenClientAuthBasic:
		if clientSecret == "" {
			return fmt.Errorf("managed credential %q client_secret is required for token_request.client_auth basic", record.Key)
		}
	default:
		return fmt.Errorf("managed credential %q has unsupported token_request.client_auth %q", record.Key, profile.ClientAuth)
	}
	return nil
}

func validateTokenRequestClientAuthConfig(key, clientID, clientSecret string, profile managedcredentialmodel.TokenRequestProfile) error {
	key = strings.TrimSpace(key)
	clientID = strings.TrimSpace(clientID)
	clientSecret = strings.TrimSpace(clientSecret)
	if clientID == "" {
		return fmt.Errorf("managed credential %q client_id is required", key)
	}
	switch managedcredentialmodel.NormalizeTokenRequestProfile(profile).ClientAuth {
	case managedcredentialmodel.TokenClientAuthBasic:
		if clientSecret == "" {
			return fmt.Errorf("managed credential %q client_secret is required for token_request.client_auth basic", key)
		}
	case managedcredentialmodel.TokenClientAuthPost:
	default:
		return fmt.Errorf("managed credential %q has unsupported token_request.client_auth %q", key, profile.ClientAuth)
	}
	return nil
}

func encodeTokenRequestBody(values url.Values, profile managedcredentialmodel.TokenRequestProfile) (*bytes.Reader, string, error) {
	switch managedcredentialmodel.NormalizeTokenRequestProfile(profile).Body {
	case managedcredentialmodel.TokenBodyForm:
		return bytes.NewReader([]byte(values.Encode())), "application/x-www-form-urlencoded", nil
	case managedcredentialmodel.TokenBodyJSON:
		body := make(map[string]string, len(values))
		for key, items := range values {
			key = strings.TrimSpace(key)
			if key == "" || len(items) == 0 {
				continue
			}
			body[key] = items[0]
		}
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, "", err
		}
		return bytes.NewReader(raw), "application/json", nil
	default:
		return nil, "", fmt.Errorf("unsupported token_request.body %q", profile.Body)
	}
}

func (s *TokenSource) markFailure(ctx context.Context, record Record, cause error, extraSecrets ...string) (Record, error) {
	secrets := append(record.SecretValues(), normalizeStrings(extraSecrets)...)
	record.Status = StatusRefreshFailed
	if cause != nil {
		record.Failure = RedactString(cause.Error(), secrets...)
	}
	record.UpdatedAt = s.now()
	if s != nil && s.Store != nil {
		if err := s.Store.Put(ctx, record); err != nil {
			if cause == nil {
				cause = fmt.Errorf("managed credential %q refresh failed", record.Key)
			}
			return record, fmt.Errorf(
				"managed credential %q refresh failed: %s; persist refresh_failed state: %s",
				record.Key,
				RedactString(cause.Error(), secrets...),
				RedactString(err.Error(), secrets...),
			)
		}
	}
	if cause == nil {
		cause = fmt.Errorf("managed credential %q refresh failed", record.Key)
	}
	return record, fmt.Errorf("managed credential %q refresh failed: %s", record.Key, RedactString(cause.Error(), secrets...))
}

func (s *TokenSource) shouldRefresh(record Record) bool {
	if strings.TrimSpace(record.AccessToken) == "" {
		return true
	}
	if record.ExpiresAt.IsZero() {
		return false
	}
	return !record.ExpiresAt.After(s.now().Add(s.refreshWindow()))
}

func (s *TokenSource) record(ctx context.Context, key string) (Record, bool, error) {
	if s == nil || s.Store == nil {
		return Record{}, false, fmt.Errorf("managed credential store is not configured")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return Record{}, false, fmt.Errorf("managed credential key is required")
	}
	return s.Store.Get(ctx, key)
}

func (s *TokenSource) now() time.Time {
	if s != nil && s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func (s *TokenSource) refreshWindow() time.Duration {
	if s != nil && s.RefreshWindow > 0 {
		return s.RefreshWindow
	}
	return defaultRefreshWindow
}

func (r Record) Descriptor() Descriptor {
	status := statusOrUnconnected(r.Status)
	return Descriptor{
		Key:            strings.TrimSpace(r.Key),
		Provider:       strings.TrimSpace(r.Provider),
		Account:        strings.TrimSpace(r.Account),
		GrantType:      strings.TrimSpace(r.GrantType),
		Scopes:         append([]string{}, r.Scopes...),
		GrantModel:     managedcredentialmodel.NormalizeGrantModel(r.GrantModel),
		TokenRequest:   managedcredentialmodel.NormalizeTokenRequestProfile(r.TokenRequest),
		InstallationID: normalizeInstallationID(r.InstallationID),
		APIBaseURL:     strings.TrimSpace(r.APIBaseURL),
		Status:         status,
		Failure:        RedactString(strings.TrimSpace(r.Failure), r.SecretValues()...),
		ExpiresAt:      r.ExpiresAt,
		UpdatedAt:      r.UpdatedAt,
	}
}

func (r Record) SecretValues() []string {
	var out []string
	for _, value := range []string{r.AccessToken, r.RefreshToken, r.ClientSecret, r.PrivateKey, r.PKCEVerifier, r.OAuthState} {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

type MemoryStore struct {
	mu      sync.RWMutex
	records map[string]Record
}

func NewMemoryStore(records ...Record) *MemoryStore {
	store := &MemoryStore{records: map[string]Record{}}
	for _, record := range records {
		if key := strings.TrimSpace(record.Key); key != "" {
			record.Key = key
			store.records[key] = normalizeRecord(record)
		}
	}
	return store
}

func (s *MemoryStore) Get(_ context.Context, key string) (Record, bool, error) {
	if s == nil {
		return Record{}, false, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.records[strings.TrimSpace(key)]
	return normalizeRecord(record), ok, nil
}

func (s *MemoryStore) Put(_ context.Context, record Record) error {
	if s == nil {
		return fmt.Errorf("managed credential memory store is nil")
	}
	record = normalizeRecord(record)
	if record.Key == "" {
		return fmt.Errorf("managed credential key is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[record.Key] = record
	return nil
}

func (s *MemoryStore) Delete(_ context.Context, key string) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, strings.TrimSpace(key))
	return nil
}

func (s *MemoryStore) List(_ context.Context) ([]Descriptor, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Descriptor, 0, len(s.records))
	for _, record := range s.records {
		out = append(out, normalizeRecord(record).Descriptor())
	}
	sortDescriptors(out)
	return out, nil
}

type FileStore struct {
	path string
	mu   sync.Mutex
}

func NewFileStore(path string) (*FileStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("managed credential file path is required")
	}
	return &FileStore{path: filepath.Clean(path)}, nil
}

func (s *FileStore) Get(ctx context.Context, key string) (Record, bool, error) {
	records, err := s.read(ctx)
	if err != nil {
		return Record{}, false, err
	}
	record, ok := records[strings.TrimSpace(key)]
	return normalizeRecord(record), ok, nil
}

func (s *FileStore) Put(ctx context.Context, record Record) error {
	record = normalizeRecord(record)
	if record.Key == "" {
		return fmt.Errorf("managed credential key is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.withWriteLockLocked(ctx, func() error {
		records, err := s.readLocked(ctx)
		if err != nil {
			return err
		}
		records[record.Key] = record
		return s.writeLocked(ctx, records)
	})
}

func (s *FileStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.withWriteLockLocked(ctx, func() error {
		records, err := s.readLocked(ctx)
		if err != nil {
			return err
		}
		delete(records, strings.TrimSpace(key))
		return s.writeLocked(ctx, records)
	})
}

func (s *FileStore) List(ctx context.Context) ([]Descriptor, error) {
	records, err := s.read(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Descriptor, 0, len(records))
	for _, record := range records {
		out = append(out, normalizeRecord(record).Descriptor())
	}
	sortDescriptors(out)
	return out, nil
}

func (s *FileStore) read(ctx context.Context) (map[string]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readLocked(ctx)
}

func (s *FileStore) readLocked(ctx context.Context) (map[string]Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return map[string]Record{}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return map[string]Record{}, nil
	}
	var wrapper struct {
		Records map[string]Record `json:"records"`
	}
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		return nil, fmt.Errorf("decode managed credential file: %w", err)
	}
	if wrapper.Records == nil {
		wrapper.Records = map[string]Record{}
	}
	for key, record := range wrapper.Records {
		record.Key = strings.TrimSpace(record.Key)
		if record.Key == "" {
			record.Key = strings.TrimSpace(key)
		}
		wrapper.Records[key] = normalizeRecord(record)
	}
	return wrapper.Records, nil
}

func (s *FileStore) withWriteLockLocked(ctx context.Context, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("create managed credential dir: %w", err)
	}
	unlock, err := lockManagedCredentialFile(s.path + ".lock")
	if err != nil {
		return err
	}
	defer unlock()
	return fn()
}

func (s *FileStore) writeLocked(ctx context.Context, records map[string]Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	clean := map[string]Record{}
	for key, record := range records {
		record = normalizeRecord(record)
		if record.Key == "" {
			record.Key = strings.TrimSpace(key)
		}
		if record.Key == "" {
			continue
		}
		clean[record.Key] = record
	}
	raw, err := json.MarshalIndent(struct {
		Records map[string]Record `json:"records"`
	}{Records: clean}, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".managed-credentials-*.json")
	if err != nil {
		return fmt.Errorf("create temp managed credential file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()
	if _, err := tmp.Write(append(raw, '\n')); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp managed credential file: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp managed credential file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp managed credential file: %w", err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return fmt.Errorf("replace managed credential file: %w", err)
	}
	return nil
}

func normalizeRecord(record Record) Record {
	record.Key = strings.TrimSpace(record.Key)
	record.Provider = strings.TrimSpace(record.Provider)
	record.Account = strings.TrimSpace(record.Account)
	record.GrantType = NormalizeGrantType(record.GrantType)
	record.AuthURL = strings.TrimSpace(record.AuthURL)
	record.TokenURL = strings.TrimSpace(record.TokenURL)
	record.APIBaseURL = strings.TrimSpace(record.APIBaseURL)
	record.ClientID = strings.TrimSpace(record.ClientID)
	record.ClientSecret = strings.TrimSpace(record.ClientSecret)
	record.InstallationID = normalizeInstallationID(record.InstallationID)
	record.PrivateKey = strings.TrimSpace(record.PrivateKey)
	record.RedirectURL = strings.TrimSpace(record.RedirectURL)
	record.Scopes = normalizeStrings(record.Scopes)
	record.GrantModel = managedcredentialmodel.NormalizeGrantModel(record.GrantModel)
	record.TokenRequest = managedcredentialmodel.NormalizeTokenRequestProfile(record.TokenRequest)
	record.AccessToken = strings.TrimSpace(record.AccessToken)
	record.RefreshToken = strings.TrimSpace(record.RefreshToken)
	record.TokenType = strings.TrimSpace(record.TokenType)
	record.Status = statusOrUnconnected(record.Status)
	record.Failure = strings.TrimSpace(record.Failure)
	record.PKCEVerifier = strings.TrimSpace(record.PKCEVerifier)
	record.PKCEChallenge = strings.TrimSpace(record.PKCEChallenge)
	record.OAuthState = strings.TrimSpace(record.OAuthState)
	if !record.ExpiresAt.IsZero() {
		record.ExpiresAt = record.ExpiresAt.UTC()
	}
	if !record.UpdatedAt.IsZero() {
		record.UpdatedAt = record.UpdatedAt.UTC()
	}
	return record
}

func NormalizeGrantType(raw string) string {
	return strings.TrimSpace(strings.ToLower(raw))
}

func ValidateRequiredGrantType(raw string) error {
	normalized := NormalizeGrantType(raw)
	if normalized == "" {
		return nil
	}
	switch normalized {
	case GrantAuthorizationCode, GrantAuthorizationCodePKCE, GrantClientCredentials, GrantGitHubAppInstallation:
		return nil
	default:
		return fmt.Errorf("grant_type %q is not supported", normalized)
	}
}

func GrantTypeCovers(actual, required string) error {
	required = NormalizeGrantType(required)
	if required == "" {
		return nil
	}
	actual = NormalizeGrantType(actual)
	if actual != required {
		return fmt.Errorf("record grant_type %s does not satisfy required grant_type %s", actual, required)
	}
	return nil
}

func ensureInstallationSelection(record Record, requested string) error {
	if NormalizeGrantType(record.GrantType) != GrantGitHubAppInstallation {
		return nil
	}
	requested = normalizeInstallationID(requested)
	if requested == "" {
		return fmt.Errorf("managed credential %q installation_id is required", record.Key)
	}
	recordInstallationID := normalizeInstallationID(record.InstallationID)
	if recordInstallationID == "" {
		return fmt.Errorf("managed credential %q installation_id is not configured", record.Key)
	}
	if requested != recordInstallationID {
		return fmt.Errorf("managed credential %q installation_id mismatch", record.Key)
	}
	return nil
}

func normalizeInstallationID(raw string) string {
	raw = strings.TrimSpace(raw)
	if strings.HasSuffix(raw, ".0") {
		raw = strings.TrimSuffix(raw, ".0")
	}
	return raw
}

func normalizeGitHubAPIBaseURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "https://api.github.com"
	}
	return strings.TrimRight(raw, "/")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func statusOrUnconnected(status string) string {
	status = strings.TrimSpace(status)
	if status == "" {
		return StatusUnconnected
	}
	switch status {
	case StatusUnconnected, StatusPendingConsent, StatusConnected, StatusRefreshFailed, StatusScopeInsufficient:
		return status
	}
	return status
}

func normalizeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func normalizeScopeString(raw string) []string {
	return normalizeStrings(strings.Fields(strings.TrimSpace(raw)))
}

func ensureScopes(actual, required []string) error {
	if len(required) == 0 {
		return nil
	}
	have := map[string]struct{}{}
	for _, scope := range normalizeStrings(actual) {
		have[scope] = struct{}{}
	}
	var missing []string
	for _, scope := range normalizeStrings(required) {
		if _, ok := have[scope]; !ok {
			missing = append(missing, scope)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing scope(s): %s", strings.Join(missing, ", "))
	}
	return nil
}

func sortDescriptors(items []Descriptor) {
	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})
}

func randomURLToken(bytesLen int) (string, error) {
	if bytesLen <= 0 {
		bytesLen = 32
	}
	buf := make([]byte, bytesLen)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func pkceChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func RedactString(raw string, secrets ...string) string {
	out := raw
	for _, secret := range secrets {
		secret = strings.TrimSpace(secret)
		if secret == "" {
			continue
		}
		out = strings.ReplaceAll(out, secret, "[REDACTED]")
	}
	return out
}

func RedactValue(value any, secrets ...string) any {
	switch typed := value.(type) {
	case string:
		return RedactString(typed, secrets...)
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = RedactValue(item, secrets...)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, RedactValue(item, secrets...))
		}
		return out
	default:
		return value
	}
}
