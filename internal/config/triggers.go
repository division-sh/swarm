package config

import (
	"fmt"
	"io/fs"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

// TriggerConfig is deployment consent for one captured trigger BODY member.
// Its token and transport compatibility are validated by trigger admission.
type TriggerConfig struct {
	Acknowledge string `yaml:"acknowledge"`
}

func validateTriggerConfigSource(source yamlsource.Value) error {
	fields, err := source.Mapping()
	if err != nil {
		return err
	}
	if len(fields) != 1 {
		return fmt.Errorf("a deployment trigger entry requires acknowledge: unsigned_webhook")
	}
	if fields[0].Name != "acknowledge" {
		return nil
	}
	value, err := fields[0].Value.Scalar()
	if err != nil || value.Tag != "!!str" || value.Value == "" {
		return fmt.Errorf("deployment trigger acknowledge must be a nonempty string")
	}
	return nil
}

func (c Config) validateTriggerEntries() error {
	for member := range c.Triggers {
		if err := validateTriggerMember(member); err != nil {
			return err
		}
	}
	return nil
}

func validateTriggerMember(member string) error {
	if !fs.ValidPath(member) || strings.Contains(member, `\`) || !strings.HasSuffix(member, "/trigger.yaml") {
		return fmt.Errorf("deployment trigger entry %q must be an exact selected-root-relative provider/trigger.yaml member path", member)
	}
	return nil
}
