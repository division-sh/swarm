package cliapp

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/cli/argcount"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
	"github.com/spf13/cobra"
)

type connectionsConnectOptions struct {
	grant             string
	provider          string
	authURL           string
	tokenURL          string
	apiBaseURL        string
	clientID          string
	clientSecretStdin bool
	privateKeyStdin   bool
	installationID    string
	redirectURL       string
	account           string
	scopes            []string
	grantModel        string
	tokenClientAuth   string
	tokenBody         string
	tokenHeaders      []string
	asJSON            bool
}

type connectionsStatusOptions struct {
	asJSON bool
}

type connectionRecord struct {
	Key            string                                     `json:"key"`
	Provider       string                                     `json:"provider,omitempty"`
	Account        string                                     `json:"account,omitempty"`
	GrantType      string                                     `json:"grant_type,omitempty"`
	InstallationID string                                     `json:"installation_id,omitempty"`
	APIBaseURL     string                                     `json:"api_base_url,omitempty"`
	Scopes         []string                                   `json:"scopes,omitempty"`
	GrantModel     string                                     `json:"grant_model,omitempty"`
	TokenRequest   managedcredentialmodel.TokenRequestProfile `json:"token_request,omitempty"`
	Status         string                                     `json:"status"`
	Failure        string                                     `json:"failure,omitempty"`
	ExpiresAt      string                                     `json:"expires_at,omitempty"`
	UpdatedAt      string                                     `json:"updated_at,omitempty"`
	Present        bool                                       `json:"present"`
}

type connectionsStatusResult struct {
	Connections []connectionRecord `json:"connections"`
}

type connectionsConnectResult struct {
	Connection   connectionRecord `json:"connection"`
	AuthorizeURL string           `json:"authorize_url,omitempty"`
	State        string           `json:"state,omitempty"`
}

func newConnectionsCommand(ctx context.Context, root InvocationRoot) *cobra.Command {
	repo := root.Path()
	cmd := &cobra.Command{
		Use:   "connections",
		Short: "Manage local managed credential connections.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newConnectionsConnectCommand(ctx, repo),
		newConnectionsCallbackCommand(ctx, repo),
		newConnectionsStatusCommand(ctx, repo),
		newConnectionsDisconnectCommand(ctx, repo),
	)
	return cmd
}

func newConnectionsConnectCommand(ctx context.Context, repo string) *cobra.Command {
	opts := connectionsConnectOptions{
		grant:           runtimemanagedcredentials.GrantAuthorizationCodePKCE,
		grantModel:      managedcredentialmodel.GrantModelScope,
		tokenClientAuth: managedcredentialmodel.TokenClientAuthPost,
		tokenBody:       managedcredentialmodel.TokenBodyForm,
	}
	cmd := &cobra.Command{
		Use:   "connect <key>",
		Short: "Start or complete a managed credential grant.",
		Args:  argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			secret, privateKey, err := readConnectionSecrets(cmd.InOrStdin(), opts)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			tokenHeaders, err := parseConnectionTokenHeaders(opts.tokenHeaders)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			tokenProfile := managedcredentialmodel.TokenRequestProfile{
				ClientAuth:    opts.tokenClientAuth,
				Body:          opts.tokenBody,
				StaticHeaders: tokenHeaders,
			}
			if err := managedcredentialmodel.ValidateGrantModel(opts.grantModel); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := managedcredentialmodel.ValidateTokenRequestProfile(tokenProfile); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			store, err := BuildManagedCredentialStore()
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), fmt.Errorf("configure managed credential store: %w", err))
			}
			source := runtimemanagedcredentials.TokenSource{Store: store, DifferentOwner: runtimeeffects.OwnerCredentialLifecycle}
			key := strings.TrimSpace(args[0])
			switch strings.TrimSpace(opts.grant) {
			case runtimemanagedcredentials.GrantAuthorizationCode:
				result, err := source.BeginAuthCode(ctx, runtimemanagedcredentials.BeginAuthCodeRequest{
					Key:          key,
					Provider:     opts.provider,
					AuthURL:      opts.authURL,
					TokenURL:     opts.tokenURL,
					ClientID:     opts.clientID,
					ClientSecret: secret,
					RedirectURL:  opts.redirectURL,
					Scopes:       opts.scopes,
					GrantModel:   opts.grantModel,
					TokenRequest: tokenProfile,
					Account:      opts.account,
				})
				if err != nil {
					return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
				}
				record, ok, err := store.Get(ctx, key)
				if err != nil {
					return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
				}
				output := connectionsConnectResult{
					Connection:   connectionRecordFromDescriptor(record.Descriptor(), ok),
					AuthorizeURL: result.AuthorizeURL,
					State:        result.State,
				}
				if opts.asJSON {
					return encodeSecretsJSON(cmd.OutOrStdout(), output)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "connection pending: key=%s status=%s\n", output.Connection.Key, output.Connection.Status)
				fmt.Fprintf(cmd.OutOrStdout(), "authorize_url: %s\n", output.AuthorizeURL)
				return nil
			case runtimemanagedcredentials.GrantAuthorizationCodePKCE:
				result, err := source.BeginAuthCodePKCE(ctx, runtimemanagedcredentials.BeginAuthCodeRequest{
					Key:          key,
					Provider:     opts.provider,
					AuthURL:      opts.authURL,
					TokenURL:     opts.tokenURL,
					ClientID:     opts.clientID,
					ClientSecret: secret,
					RedirectURL:  opts.redirectURL,
					Scopes:       opts.scopes,
					GrantModel:   opts.grantModel,
					TokenRequest: tokenProfile,
					Account:      opts.account,
				})
				if err != nil {
					return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
				}
				record, ok, err := store.Get(ctx, key)
				if err != nil {
					return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
				}
				output := connectionsConnectResult{
					Connection:   connectionRecordFromDescriptor(record.Descriptor(), ok),
					AuthorizeURL: result.AuthorizeURL,
					State:        result.State,
				}
				if opts.asJSON {
					return encodeSecretsJSON(cmd.OutOrStdout(), output)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "connection pending: key=%s status=%s\n", output.Connection.Key, output.Connection.Status)
				fmt.Fprintf(cmd.OutOrStdout(), "authorize_url: %s\n", output.AuthorizeURL)
				return nil
			case runtimemanagedcredentials.GrantClientCredentials:
				record, err := source.ConnectClientCredentials(ctx, runtimemanagedcredentials.ClientCredentialsRequest{
					Key:          key,
					Provider:     opts.provider,
					TokenURL:     opts.tokenURL,
					ClientID:     opts.clientID,
					ClientSecret: secret,
					Scopes:       opts.scopes,
					GrantModel:   opts.grantModel,
					TokenRequest: tokenProfile,
					Account:      opts.account,
				})
				if err != nil {
					return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
				}
				output := connectionsConnectResult{Connection: connectionRecordFromDescriptor(record.Descriptor(), true)}
				if opts.asJSON {
					return encodeSecretsJSON(cmd.OutOrStdout(), output)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "connection connected: key=%s status=%s\n", output.Connection.Key, output.Connection.Status)
				return nil
			case runtimemanagedcredentials.GrantGitHubAppInstallation:
				record, err := source.ConnectGitHubAppInstallation(ctx, runtimemanagedcredentials.GitHubAppInstallationRequest{
					Key:            key,
					Provider:       firstNonEmpty(opts.provider, "github"),
					APIBaseURL:     opts.apiBaseURL,
					ClientID:       opts.clientID,
					InstallationID: opts.installationID,
					PrivateKey:     privateKey,
					Account:        opts.account,
				})
				if err != nil {
					return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
				}
				output := connectionsConnectResult{Connection: connectionRecordFromDescriptor(record.Descriptor(), true)}
				if opts.asJSON {
					return encodeSecretsJSON(cmd.OutOrStdout(), output)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "connection connected: key=%s status=%s\n", output.Connection.Key, output.Connection.Status)
				return nil
			default:
				return returnCLIValidationError(cmd.ErrOrStderr(), fmt.Errorf("--grant must be %s, %s, %s, or %s", runtimemanagedcredentials.GrantAuthorizationCode, runtimemanagedcredentials.GrantAuthorizationCodePKCE, runtimemanagedcredentials.GrantClientCredentials, runtimemanagedcredentials.GrantGitHubAppInstallation))
			}
		},
	}
	cmd.Flags().StringVar(&opts.grant, "grant", opts.grant, "Grant type: authorization_code, authorization_code_pkce, client_credentials, or github_app_installation")
	cmd.Flags().StringVar(&opts.provider, "provider", "", "Provider label for operator status")
	cmd.Flags().StringVar(&opts.authURL, "auth-url", "", "OAuth authorization URL")
	cmd.Flags().StringVar(&opts.tokenURL, "token-url", "", "OAuth token URL")
	cmd.Flags().StringVar(&opts.apiBaseURL, "api-base-url", "", "Provider API base URL for app-installation grants")
	cmd.Flags().StringVar(&opts.clientID, "client-id", "", "OAuth client ID")
	cmd.Flags().BoolVar(&opts.clientSecretStdin, "client-secret-stdin", false, "Read the OAuth client secret from stdin")
	cmd.Flags().BoolVar(&opts.privateKeyStdin, "private-key-stdin", false, "Read the app private key from stdin")
	cmd.Flags().StringVar(&opts.installationID, "installation-id", "", "Provider app installation id")
	cmd.Flags().StringVar(&opts.redirectURL, "redirect-url", "", "OAuth redirect URL for authorization_code grants")
	cmd.Flags().StringVar(&opts.account, "account", "", "Connected provider account label")
	cmd.Flags().StringSliceVar(&opts.scopes, "scope", nil, "Required OAuth scope; repeat or comma-separate")
	cmd.Flags().StringVar(&opts.grantModel, "grant-model", opts.grantModel, "Grant model: scope_grant or workspace_grant")
	cmd.Flags().StringVar(&opts.tokenClientAuth, "token-client-auth", opts.tokenClientAuth, "Token endpoint client authentication: post or basic")
	cmd.Flags().StringVar(&opts.tokenBody, "token-body", opts.tokenBody, "Token endpoint request body encoding: form or json")
	cmd.Flags().StringArrayVar(&opts.tokenHeaders, "token-header", nil, "Static non-secret token endpoint header as Name=Value; repeatable")
	cmd.Flags().BoolVar(&opts.asJSON, "json", false, "Render successful output as one JSON document")
	return cmd
}

func newConnectionsCallbackCommand(ctx context.Context, repo string) *cobra.Command {
	var state string
	var codeStdin bool
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "callback <key>",
		Short: "Record an OAuth authorization-code callback.",
		Args:  argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			code, err := readConnectionAuthCode(cmd.InOrStdin(), codeStdin)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			store, err := BuildManagedCredentialStore()
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), fmt.Errorf("configure managed credential store: %w", err))
			}
			source := runtimemanagedcredentials.TokenSource{Store: store, DifferentOwner: runtimeeffects.OwnerCredentialLifecycle}
			record, err := source.CompleteAuthCode(ctx, runtimemanagedcredentials.CompleteAuthCodeRequest{
				Key:   strings.TrimSpace(args[0]),
				State: state,
				Code:  code,
			})
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
			}
			output := connectionsConnectResult{Connection: connectionRecordFromDescriptor(record.Descriptor(), true)}
			if asJSON {
				return encodeSecretsJSON(cmd.OutOrStdout(), output)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "connection connected: key=%s status=%s\n", output.Connection.Key, output.Connection.Status)
			return nil
		},
	}
	cmd.Flags().StringVar(&state, "state", "", "OAuth callback state")
	cmd.Flags().BoolVar(&codeStdin, "code-stdin", false, "Read the OAuth authorization code from stdin")
	cmd.Flags().BoolVar(&asJSON, "json", false, "Render successful output as one JSON document")
	return cmd
}

func newConnectionsStatusCommand(ctx context.Context, repo string) *cobra.Command {
	opts := connectionsStatusOptions{}
	cmd := &cobra.Command{
		Use:   "status [key]",
		Short: "Show managed credential connection status.",
		Args:  argcount.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := BuildManagedCredentialStore()
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), fmt.Errorf("configure managed credential store: %w", err))
			}
			records, err := connectionRecords(ctx, store, args)
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
			}
			result := connectionsStatusResult{Connections: records}
			if opts.asJSON {
				return encodeSecretsJSON(cmd.OutOrStdout(), result)
			}
			writeConnectionsTable(cmd.OutOrStdout(), records)
			return nil
		},
	}
	cmd.Flags().BoolVar(&opts.asJSON, "json", false, "Render successful output as one JSON document")
	argcount.SetDiscoveryHint(cmd, "List connection keys with `swarm connections status`.")
	return cmd
}

func newConnectionsDisconnectCommand(ctx context.Context, repo string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "disconnect <key>",
		Short: "Delete a managed credential token record.",
		Args:  argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := BuildManagedCredentialStore()
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), fmt.Errorf("configure managed credential store: %w", err))
			}
			key := strings.TrimSpace(args[0])
			if err := store.Delete(ctx, key); err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "connection disconnected: key=%s\n", key)
			return nil
		},
	}
	argcount.SetDiscoveryHint(cmd, "List connection keys with `swarm connections status`.")
	return cmd
}

func readConnectionSecrets(in io.Reader, opts connectionsConnectOptions) (string, string, error) {
	if opts.clientSecretStdin && opts.privateKeyStdin {
		return "", "", fmt.Errorf("--client-secret-stdin and --private-key-stdin cannot both read from stdin")
	}
	if opts.privateKeyStdin {
		privateKey, err := readConnectionPrivateKey(in)
		return "", privateKey, err
	}
	if !opts.clientSecretStdin {
		return "", "", nil
	}
	raw, err := io.ReadAll(in)
	if err != nil {
		return "", "", err
	}
	return strings.TrimSpace(string(raw)), "", nil
}

func readConnectionPrivateKey(in io.Reader) (string, error) {
	raw, err := io.ReadAll(in)
	if err != nil {
		return "", err
	}
	privateKey := strings.TrimSpace(string(raw))
	if privateKey == "" {
		return "", fmt.Errorf("private key is required")
	}
	return privateKey, nil
}

func readConnectionAuthCode(in io.Reader, codeStdin bool) (string, error) {
	if !codeStdin {
		return "", fmt.Errorf("--code-stdin is required")
	}
	raw, err := io.ReadAll(in)
	if err != nil {
		return "", err
	}
	code := strings.TrimSpace(string(raw))
	if code == "" {
		return "", fmt.Errorf("authorization code is required")
	}
	return code, nil
}

func parseConnectionTokenHeaders(items []string) (map[string]string, error) {
	if len(items) == 0 {
		return nil, nil
	}
	headers := make(map[string]string, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		key, value, ok := strings.Cut(item, "=")
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if !ok || key == "" || value == "" {
			return nil, fmt.Errorf("--token-header must be Name=Value with non-empty name and value")
		}
		headers[key] = value
	}
	if len(headers) == 0 {
		return nil, nil
	}
	return headers, nil
}

func connectionRecords(ctx context.Context, store runtimemanagedcredentials.Store, args []string) ([]connectionRecord, error) {
	if len(args) == 1 {
		key := strings.TrimSpace(args[0])
		record, ok, err := store.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		desc := record.Descriptor()
		if !ok {
			desc = runtimemanagedcredentials.Descriptor{Key: key, Status: runtimemanagedcredentials.StatusUnconnected}
		}
		return []connectionRecord{connectionRecordFromDescriptor(desc, ok)}, nil
	}
	descriptors, err := store.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]connectionRecord, 0, len(descriptors))
	for _, desc := range descriptors {
		out = append(out, connectionRecordFromDescriptor(desc, true))
	}
	return out, nil
}

func connectionRecordFromDescriptor(desc runtimemanagedcredentials.Descriptor, present bool) connectionRecord {
	record := connectionRecord{
		Key:            strings.TrimSpace(desc.Key),
		Provider:       strings.TrimSpace(desc.Provider),
		Account:        strings.TrimSpace(desc.Account),
		GrantType:      strings.TrimSpace(desc.GrantType),
		InstallationID: strings.TrimSpace(desc.InstallationID),
		APIBaseURL:     strings.TrimSpace(desc.APIBaseURL),
		Scopes:         append([]string{}, desc.Scopes...),
		GrantModel:     strings.TrimSpace(desc.GrantModel),
		TokenRequest:   managedcredentialmodel.NormalizeTokenRequestProfile(desc.TokenRequest),
		Status:         strings.TrimSpace(desc.Status),
		Failure:        strings.TrimSpace(desc.Failure),
		Present:        present,
	}
	if !desc.ExpiresAt.IsZero() {
		record.ExpiresAt = desc.ExpiresAt.Format(time.RFC3339)
	}
	if !desc.UpdatedAt.IsZero() {
		record.UpdatedAt = desc.UpdatedAt.Format(time.RFC3339)
	}
	return record
}

func writeConnectionsTable(out io.Writer, records []connectionRecord) {
	if out == nil {
		return
	}
	rows := make([][]string, 0, len(records))
	for _, record := range records {
		rows = append(rows, []string{
			record.Key,
			dash(record.Provider),
			dash(record.Account),
			dash(record.GrantType),
			dash(record.GrantModel),
			dash(managedcredentialmodel.TokenRequestProfileSummary(record.TokenRequest)),
			dash(record.Status),
			dash(record.ExpiresAt),
		})
	}
	writeCLITable(out, cliTable{
		Columns: []cliTableColumn{
			{Header: "KEY", KeyColumn: true},
			{Header: "PROVIDER"},
			{Header: "ACCOUNT"},
			{Header: "GRANT"},
			{Header: "GRANT_MODEL"},
			{Header: "TOKEN_REQUEST"},
			{Header: "STATUS"},
			{Header: "EXPIRES_AT"},
		},
		Rows:         rows,
		EmptyMessage: "No managed connections match the current filters. Add one: swarm connections connect <key>",
	})
}
