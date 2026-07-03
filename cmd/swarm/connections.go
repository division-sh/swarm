package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/spf13/cobra"
)

type connectionsConnectOptions struct {
	grant             string
	provider          string
	authURL           string
	tokenURL          string
	clientID          string
	clientSecretStdin bool
	redirectURL       string
	account           string
	scopes            []string
	asJSON            bool
}

type connectionsStatusOptions struct {
	contractsPath    string
	platformSpecPath string
	asJSON           bool
}

type connectionRecord struct {
	Key        string                  `json:"key"`
	Provider   string                  `json:"provider,omitempty"`
	Account    string                  `json:"account,omitempty"`
	GrantType  string                  `json:"grant_type,omitempty"`
	Scopes     []string                `json:"scopes,omitempty"`
	Status     string                  `json:"status"`
	Failure    string                  `json:"failure,omitempty"`
	ExpiresAt  string                  `json:"expires_at,omitempty"`
	UpdatedAt  string                  `json:"updated_at,omitempty"`
	Present    bool                    `json:"present"`
	RequiredBy []connectionRequirement `json:"required_by,omitempty"`
}

type connectionRequirement struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type connectionsStatusResult struct {
	Connections []connectionRecord `json:"connections"`
}

type connectionsConnectResult struct {
	Connection   connectionRecord `json:"connection"`
	AuthorizeURL string           `json:"authorize_url,omitempty"`
	State        string           `json:"state,omitempty"`
}

func newConnectionsCommand(ctx context.Context, repo string) *cobra.Command {
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
	opts := connectionsConnectOptions{grant: runtimemanagedcredentials.GrantAuthorizationCodePKCE}
	cmd := &cobra.Command{
		Use:   "connect <key>",
		Short: "Start or complete a managed credential grant.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			secret, err := readConnectionClientSecret(cmd.InOrStdin(), opts)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			store, err := buildManagedCredentialStore()
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), fmt.Errorf("configure managed credential store: %w", err))
			}
			source := runtimemanagedcredentials.TokenSource{Store: store}
			key := strings.TrimSpace(args[0])
			switch strings.TrimSpace(opts.grant) {
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
					Connection:   connectionRecordFromDescriptor(record.Descriptor(), ok, nil),
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
					Account:      opts.account,
				})
				if err != nil {
					return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
				}
				output := connectionsConnectResult{Connection: connectionRecordFromDescriptor(record.Descriptor(), true, nil)}
				if opts.asJSON {
					return encodeSecretsJSON(cmd.OutOrStdout(), output)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "connection connected: key=%s status=%s\n", output.Connection.Key, output.Connection.Status)
				return nil
			default:
				return returnCLIValidationError(cmd.ErrOrStderr(), fmt.Errorf("--grant must be %s or %s", runtimemanagedcredentials.GrantAuthorizationCodePKCE, runtimemanagedcredentials.GrantClientCredentials))
			}
		},
	}
	cmd.Flags().StringVar(&opts.grant, "grant", opts.grant, "Grant type: authorization_code_pkce or client_credentials")
	cmd.Flags().StringVar(&opts.provider, "provider", "", "Provider label for operator status")
	cmd.Flags().StringVar(&opts.authURL, "auth-url", "", "OAuth authorization URL")
	cmd.Flags().StringVar(&opts.tokenURL, "token-url", "", "OAuth token URL")
	cmd.Flags().StringVar(&opts.clientID, "client-id", "", "OAuth client ID")
	cmd.Flags().BoolVar(&opts.clientSecretStdin, "client-secret-stdin", false, "Read the OAuth client secret from stdin")
	cmd.Flags().StringVar(&opts.redirectURL, "redirect-url", "", "OAuth redirect URL for authorization_code_pkce")
	cmd.Flags().StringVar(&opts.account, "account", "", "Connected provider account label")
	cmd.Flags().StringSliceVar(&opts.scopes, "scope", nil, "Required OAuth scope; repeat or comma-separate")
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
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			code, err := readConnectionAuthCode(cmd.InOrStdin(), codeStdin)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			store, err := buildManagedCredentialStore()
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), fmt.Errorf("configure managed credential store: %w", err))
			}
			source := runtimemanagedcredentials.TokenSource{Store: store}
			record, err := source.CompleteAuthCode(ctx, runtimemanagedcredentials.CompleteAuthCodeRequest{
				Key:   strings.TrimSpace(args[0]),
				State: state,
				Code:  code,
			})
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
			}
			output := connectionsConnectResult{Connection: connectionRecordFromDescriptor(record.Descriptor(), true, nil)}
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
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := buildManagedCredentialStore()
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), fmt.Errorf("configure managed credential store: %w", err))
			}
			source, err := loadConnectionsSource(cmd, repo, opts.contractsPath, opts.platformSpecPath)
			if err != nil {
				return returnSecretsRuntimeError(cmd.ErrOrStderr(), err)
			}
			records, err := connectionRecords(ctx, store, source, args)
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
	cmd.Flags().StringVar(&opts.contractsPath, "contracts", opts.contractsPath, "Path to Swarm contract bundle root for required_by metadata")
	cmd.Flags().StringVar(&opts.platformSpecPath, "platform-spec", opts.platformSpecPath, "Path to platform spec yaml")
	cmd.Flags().BoolVar(&opts.asJSON, "json", false, "Render successful output as one JSON document")
	return cmd
}

func newConnectionsDisconnectCommand(ctx context.Context, repo string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "disconnect <key>",
		Short: "Delete a managed credential token record.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := buildManagedCredentialStore()
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
	return cmd
}

func readConnectionClientSecret(in io.Reader, opts connectionsConnectOptions) (string, error) {
	if !opts.clientSecretStdin {
		return "", nil
	}
	raw, err := io.ReadAll(in)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(raw)), nil
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

func loadConnectionsSource(cmd *cobra.Command, repo, contractsPath, platformSpecPath string) (semanticview.Source, error) {
	if strings.TrimSpace(contractsPath) == "" {
		return nil, nil
	}
	return loadSecretsSource(cmd, repo, contractsPath, platformSpecPath, true)
}

func connectionRecords(ctx context.Context, store runtimemanagedcredentials.Store, source semanticview.Source, args []string) ([]connectionRecord, error) {
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
		return []connectionRecord{connectionRecordFromDescriptor(desc, ok, nil)}, nil
	}
	descriptors, err := runtimemanagedcredentials.ListRequirementDescriptors(ctx, store, source)
	if err != nil {
		return nil, err
	}
	out := make([]connectionRecord, 0, len(descriptors))
	for _, desc := range descriptors {
		out = append(out, connectionRecordFromDescriptor(desc.Descriptor, desc.Present, desc.RequiredBy))
	}
	return out, nil
}

func connectionRecordFromDescriptor(desc runtimemanagedcredentials.Descriptor, present bool, requiredBy []runtimemanagedcredentials.Requirement) connectionRecord {
	record := connectionRecord{
		Key:       strings.TrimSpace(desc.Key),
		Provider:  strings.TrimSpace(desc.Provider),
		Account:   strings.TrimSpace(desc.Account),
		GrantType: strings.TrimSpace(desc.GrantType),
		Scopes:    append([]string{}, desc.Scopes...),
		Status:    strings.TrimSpace(desc.Status),
		Failure:   strings.TrimSpace(desc.Failure),
		Present:   present,
	}
	if !desc.ExpiresAt.IsZero() {
		record.ExpiresAt = desc.ExpiresAt.Format(time.RFC3339)
	}
	if !desc.UpdatedAt.IsZero() {
		record.UpdatedAt = desc.UpdatedAt.Format(time.RFC3339)
	}
	for _, item := range requiredBy {
		record.RequiredBy = append(record.RequiredBy, connectionRequirement{
			Kind: strings.TrimSpace(item.Kind),
			Name: strings.TrimSpace(item.Name),
		})
	}
	return record
}

func writeConnectionsTable(out io.Writer, records []connectionRecord) {
	if out == nil {
		return
	}
	writer := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "KEY\tPROVIDER\tACCOUNT\tGRANT\tSTATUS\tEXPIRES_AT\tREQUIRED_BY")
	for _, record := range records {
		fmt.Fprintf(writer, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			record.Key,
			dash(record.Provider),
			dash(record.Account),
			dash(record.GrantType),
			dash(record.Status),
			dash(record.ExpiresAt),
			dash(formatConnectionRequirements(record.RequiredBy)),
		)
	}
	_ = writer.Flush()
}

func formatConnectionRequirements(items []connectionRequirement) string {
	if len(items) == 0 {
		return ""
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		item.Kind = strings.TrimSpace(item.Kind)
		item.Name = strings.TrimSpace(item.Name)
		if item.Kind == "" || item.Name == "" {
			continue
		}
		parts = append(parts, item.Kind+":"+item.Name)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
