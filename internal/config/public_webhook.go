package config

import (
	"fmt"
	"net"
	"net/mail"
	"reflect"
	"regexp"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

type ServeConfig struct {
	APIListenAddr string               `yaml:"api_listen_addr"`
	MCPListenAddr string               `yaml:"mcp_listen_addr"`
	APITokenFile  string               `yaml:"api_token_file"`
	PublicWebhook *PublicWebhookConfig `yaml:"public_webhook,omitempty"`
}

type PublicWebhookConfig struct {
	Hostname        string  `yaml:"hostname"`
	CertificateFile *string `yaml:"certificate_file,omitempty"`
	PrivateKeyFile  *string `yaml:"private_key_file,omitempty"`
	Email           string  `yaml:"email,omitempty"`
}

var webhookHostname = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+$`)

func (c *PublicWebhookConfig) UnmarshalYAML(node *yaml.Node) error {
	value := yamlsource.ValueFromNode(node)
	if err := validateConfigSource(value, reflect.TypeFor[PublicWebhookConfig](), "serve.public_webhook"); err != nil {
		return err
	}
	fields, err := value.Mapping()
	if err != nil {
		return err
	}
	for _, field := range fields {
		scalar, err := field.Value.Scalar()
		if err != nil || scalar.Tag != "!!str" {
			return fmt.Errorf("serve.public_webhook.%s must be a string", field.Name)
		}
	}
	type plain PublicWebhookConfig
	if err := node.Decode((*plain)(c)); err != nil {
		return err
	}
	return c.Validate()
}

func (c *PublicWebhookConfig) Validate() error {
	if c == nil {
		return nil
	}
	host := strings.TrimSpace(c.Hostname)
	if len(host) > 253 || net.ParseIP(host) != nil || !webhookHostname.MatchString(host) {
		return fmt.Errorf("serve.public_webhook.hostname must be one DNS hostname without a scheme, port or path")
	}
	if (c.CertificateFile == nil) != (c.PrivateKeyFile == nil) || c.CertificateFile != nil &&
		(strings.TrimSpace(*c.CertificateFile) == "" || strings.TrimSpace(*c.PrivateKeyFile) == "") {
		return fmt.Errorf("serve.public_webhook requires both nonblank certificate_file and private_key_file, or neither")
	}
	if c.CertificateFile != nil {
		for _, reference := range []string{*c.CertificateFile, *c.PrivateKeyFile} {
			if strings.ContainsAny(reference, "\x00\r\n") || strings.Contains(reference, "-----BEGIN") {
				return fmt.Errorf("serve.public_webhook requires file references, not inline certificate or key material")
			}
		}
	}
	if c.Email != "" {
		address, err := mail.ParseAddress(c.Email)
		if err != nil || address.Address != c.Email {
			return fmt.Errorf("serve.public_webhook.email must be an email address without a display name")
		}
	}
	return nil
}
