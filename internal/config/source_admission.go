package config

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// DecodeSource admits authored config before projecting into caller-owned defaults.
func DecodeSource(value yamlsource.Value, target *Config) error {
	if target == nil {
		return errors.New("config projection target is required")
	}
	if err := validateConfigSource(value, reflect.TypeFor[Config](), "config"); err != nil {
		return err
	}
	if value.Presence() == yamlsource.PresenceMissing {
		return nil
	}
	return value.Project(target)
}

// CLISourceConfig is the CLI-owned portion of the same swarm.yaml document.
type CLISourceConfig struct {
	Connection struct {
		APIServer    string `yaml:"api_server"`
		APITokenFile string `yaml:"api_token_file"`
	} `yaml:"connection"`
	Serve struct {
		APIListenAddr string `yaml:"api_listen_addr"`
		MCPListenAddr string `yaml:"mcp_listen_addr"`
		APITokenFile  string `yaml:"api_token_file"`
	} `yaml:"serve"`
	Paths struct {
		SwarmDir   string `yaml:"swarm_dir"`
		MonitorDir string `yaml:"monitor_dir"`
	} `yaml:"paths"`
}

func validateConfigSource(value yamlsource.Value, shape reflect.Type, owner string) error {
	if err := value.ValidateExpansion(); err != nil {
		return err
	}
	if err := value.ValidateUniqueMappings(); err != nil {
		return err
	}
	return validateConfigValue(value, shape, owner)
}

func validateConfigValue(value yamlsource.Value, shape reflect.Type, owner string) error {
	if value.Presence() == yamlsource.PresenceMissing {
		return nil
	}
	for shape.Kind() == reflect.Pointer {
		shape = shape.Elem()
	}
	if err := validateConfigContainerShape(value, shape, owner); err != nil {
		return err
	}
	switch shape.Kind() {
	case reflect.Struct:
		declared := map[string]reflect.Type{}
		add := func(model reflect.Type) {
			for i := 0; i < model.NumField(); i++ {
				field := model.Field(i)
				name := strings.Split(field.Tag.Get("yaml"), ",")[0]
				if field.IsExported() && name != "" && name != "-" {
					declared[name] = field.Type
				}
			}
		}
		add(shape)
		if shape == reflect.TypeFor[Config]() {
			add(reflect.TypeFor[ExtensionsConfig]())
			add(reflect.TypeFor[CLISourceConfig]())
		}
		fields, err := value.Mapping()
		if err != nil {
			return err
		}
		options := make(map[string]struct{}, len(declared))
		policy := SourceKeyRules()
		for name := range declared {
			if rule, known := policy[strings.TrimPrefix(owner+"."+name, "config.")]; known && !rule.Supported() {
				continue
			}
			options[name] = struct{}{}
		}
		for _, field := range fields {
			child := declared[field.Name]
			if _, admitted := options[field.Name]; !admitted {
				diagnostic := runtimecontracts.NewUndefinedFieldDiagnostic(owner, field.Name, options, field)
				if owner == "config.paths" && field.Name == "platform_spec_path" {
					diagnostic.Remediation = PlatformSpecPathConfigRemediation
				}
				return diagnostic
			}
			if err := validateConfigValue(field.Value, child, owner+"."+field.Name); err != nil {
				return err
			}
		}
	case reflect.Map:
		if value.Presence() == yamlsource.PresenceMapping || value.Presence() == yamlsource.PresenceEmptyMapping {
			fields, err := value.Mapping()
			if err != nil {
				return err
			}
			for _, field := range fields {
				if err := validateConfigValue(field.Value, shape.Elem(), owner+"."+field.Name); err != nil {
					return err
				}
			}
		}
	case reflect.Slice:
		if value.Presence() == yamlsource.PresenceSequence || value.Presence() == yamlsource.PresenceEmptySequence {
			items, err := value.Sequence()
			if err != nil {
				return err
			}
			for _, item := range items {
				if err := validateConfigValue(item, shape.Elem(), owner); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateConfigContainerShape(value yamlsource.Value, shape reflect.Type, owner string) error {
	expected := ""
	switch shape.Kind() {
	case reflect.Struct, reflect.Map:
		if value.Presence() != yamlsource.PresenceMapping && value.Presence() != yamlsource.PresenceEmptyMapping {
			expected = "mapping"
		}
	case reflect.Slice:
		if value.Presence() != yamlsource.PresenceSequence && value.Presence() != yamlsource.PresenceEmptySequence {
			expected = "sequence"
		}
	}
	if expected == "" {
		return nil
	}
	diagnostic := runtimecontracts.NewExpectedShapeDiagnostic(
		"config.source_shape", owner,
		fmt.Sprintf("%s must be a YAML %s, got %s", owner, expected, value.Presence()),
		"use the declared container shape or omit the key to keep its default; null is not a reset", nil,
	)
	location := value.IntroductionLocation()
	diagnostic.Location.File = location.File
	diagnostic.Location.Line = location.Line
	diagnostic.Location.Column = location.Column
	return diagnostic
}
