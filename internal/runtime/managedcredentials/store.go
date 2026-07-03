package managedcredentials

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
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
)

const (
	GrantAuthorizationCodePKCE = "authorization_code_pkce"
	GrantClientCredentials     = "client_credentials"

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
	Key             string    `json:"key"`
	Provider        string    `json:"provider"`
	Account         string    `json:"account,omitempty"`
	GrantType       string    `json:"grant_type"`
	AuthURL         string    `json:"auth_url,omitempty"`
	TokenURL        string    `json:"token_url"`
	ClientID        string    `json:"client_id"`
	ClientSecret    string    `json:"client_secret,omitempty"`
	RedirectURL     string    `json:"redirect_url,omitempty"`
	Scopes          []string  `json:"scopes,omitempty"`
	AccessToken     string    `json:"access_token,omitempty"`
	RefreshToken    string    `json:"refresh_token,omitempty"`
	TokenType       string    `json:"token_type,omitempty"`
	ExpiresAt       time.Time `json:"expires_at,omitempty"`
	Status          string    `json:"status"`
	Failure         string    `json:"failure,omitempty"`
	PKCEVerifier    string    `json:"pkce_verifier,omitempty"`
	PKCEChallenge   string    `json:"pkce_challenge,omitempty"`
	OAuthState      string    `json:"oauth_state,omitempty"`
	RefreshWindowMS int64     `json:"refresh_window_ms,omitempty"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type Descriptor struct {
	Key       string    `json:"key"`
	Provider  string    `json:"provider,omitempty"`
	Account   string    `json:"account,omitempty"`
	GrantType string    `json:"grant_type,omitempty"`
	Scopes    []string  `json:"scopes,omitempty"`
	Status    string    `json:"status"`
	Failure   string    `json:"failure,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
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
	Account      string
}

type AccessTokenRequest struct {
	Key    string
	Scopes []string
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
		return BeginAuthCodeResult{}, fmt.Errorf("auth_url, token_url, client_id, and redirect_url are required for authorization_code_pkce")
	}
	verifier, err := randomURLToken(48)
	if err != nil {
		return BeginAuthCodeResult{}, err
	}
	state, err := randomURLToken(32)
	if err != nil {
		return BeginAuthCodeResult{}, err
	}
	challenge := pkceChallenge(verifier)
	values := url.Values{}
	values.Set("response_type", "code")
	values.Set("client_id", clientID)
	values.Set("redirect_uri", redirectURL)
	values.Set("code_challenge", challenge)
	values.Set("code_challenge_method", "S256")
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
	record := Record{
		Key:           key,
		Provider:      strings.TrimSpace(req.Provider),
		Account:       strings.TrimSpace(req.Account),
		GrantType:     GrantAuthorizationCodePKCE,
		AuthURL:       authURL,
		TokenURL:      tokenURL,
		ClientID:      clientID,
		ClientSecret:  strings.TrimSpace(req.ClientSecret),
		RedirectURL:   redirectURL,
		Scopes:        normalizeStrings(req.Scopes),
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
	if record.GrantType != GrantAuthorizationCodePKCE {
		return Record{}, fmt.Errorf("managed credential %q grant_type is %q, want %s", record.Key, record.GrantType, GrantAuthorizationCodePKCE)
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
	values.Set("client_id", strings.TrimSpace(record.ClientID))
	values.Set("code_verifier", strings.TrimSpace(record.PKCEVerifier))
	if strings.TrimSpace(record.ClientSecret) != "" {
		values.Set("client_secret", strings.TrimSpace(record.ClientSecret))
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
		Status:       StatusUnconnected,
		UpdatedAt:    s.now(),
	}
	if record.Key == "" || record.TokenURL == "" || record.ClientID == "" {
		return Record{}, fmt.Errorf("key, token_url, and client_id are required for client_credentials")
	}
	values := url.Values{}
	values.Set("grant_type", "client_credentials")
	values.Set("client_id", record.ClientID)
	if record.ClientSecret != "" {
		values.Set("client_secret", record.ClientSecret)
	}
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

func (s *TokenSource) AccessToken(ctx context.Context, req AccessTokenRequest) (string, Record, error) {
	record, ok, err := s.record(ctx, req.Key)
	if err != nil {
		return "", Record{}, err
	}
	if !ok {
		return "", Record{}, fmt.Errorf("missing managed credential %q", strings.TrimSpace(req.Key))
	}
	if err := ensureScopes(record.Scopes, req.Scopes); err != nil {
		return "", Record{}, fmt.Errorf("managed credential %q scope-insufficient: %w", record.Key, err)
	}
	if strings.TrimSpace(record.Status) != StatusConnected {
		return "", Record{}, fmt.Errorf("managed credential %q is %s", record.Key, statusOrUnconnected(record.Status))
	}
	if strings.TrimSpace(record.AccessToken) == "" || s.shouldRefresh(record) {
		record, err = s.refresh(ctx, record)
		if err != nil {
			return "", record, err
		}
		if err := ensureScopes(record.Scopes, req.Scopes); err != nil {
			return "", record, fmt.Errorf("managed credential %q scope-insufficient: %w", record.Key, err)
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
	case GrantAuthorizationCodePKCE:
		if strings.TrimSpace(record.RefreshToken) == "" {
			err := fmt.Errorf("managed credential %q has no refresh token", record.Key)
			return s.markFailure(ctx, record, err)
		}
		values.Set("grant_type", "refresh_token")
		values.Set("refresh_token", strings.TrimSpace(record.RefreshToken))
		values.Set("client_id", strings.TrimSpace(record.ClientID))
		if strings.TrimSpace(record.ClientSecret) != "" {
			values.Set("client_secret", strings.TrimSpace(record.ClientSecret))
		}
	case GrantClientCredentials:
		values.Set("grant_type", "client_credentials")
		values.Set("client_id", strings.TrimSpace(record.ClientID))
		if strings.TrimSpace(record.ClientSecret) != "" {
			values.Set("client_secret", strings.TrimSpace(record.ClientSecret))
		}
		if len(record.Scopes) > 0 {
			values.Set("scope", strings.Join(record.Scopes, " "))
		}
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(values.Encode()))
	if err != nil {
		return Record{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		return Record{}, err
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
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return Record{}, fmt.Errorf("decode token response: %w", err)
	}
	if resp.StatusCode >= 400 {
		msg := strings.TrimSpace(body.Error)
		if msg == "" {
			msg = fmt.Sprintf("token endpoint returned status %d", resp.StatusCode)
		}
		if desc := strings.TrimSpace(body.Description); desc != "" {
			msg += ": " + desc
		}
		return Record{}, fmt.Errorf("%s", msg)
	}
	access := strings.TrimSpace(body.AccessToken)
	if access == "" {
		return Record{}, fmt.Errorf("token endpoint did not return access_token")
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
	return updated, nil
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
		Key:       strings.TrimSpace(r.Key),
		Provider:  strings.TrimSpace(r.Provider),
		Account:   strings.TrimSpace(r.Account),
		GrantType: strings.TrimSpace(r.GrantType),
		Scopes:    append([]string{}, r.Scopes...),
		Status:    status,
		Failure:   RedactString(strings.TrimSpace(r.Failure), r.SecretValues()...),
		ExpiresAt: r.ExpiresAt,
		UpdatedAt: r.UpdatedAt,
	}
}

func (r Record) SecretValues() []string {
	var out []string
	for _, value := range []string{r.AccessToken, r.RefreshToken, r.ClientSecret, r.PKCEVerifier, r.OAuthState} {
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
	record.GrantType = strings.TrimSpace(record.GrantType)
	record.AuthURL = strings.TrimSpace(record.AuthURL)
	record.TokenURL = strings.TrimSpace(record.TokenURL)
	record.ClientID = strings.TrimSpace(record.ClientID)
	record.ClientSecret = strings.TrimSpace(record.ClientSecret)
	record.RedirectURL = strings.TrimSpace(record.RedirectURL)
	record.Scopes = normalizeStrings(record.Scopes)
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
