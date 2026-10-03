package config

import (
	"errors"
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
		SwarmDir              string `yaml:"swarm_dir"`
		PlatformSpecPath      string `yaml:"platform_spec_path"`
		MonitorDir            string `yaml:"monitor_dir"`
		AgentConfigMapFile    string `yaml:"agent_config_map_file"`
		VerificationGatesFile string `yaml:"verification_gates_file"`
		ToolingLockFile       string `yaml:"tooling_lock_file"`
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
	for shape.Kind() == reflect.Pointer {
		shape = shape.Elem()
	}
	switch shape.Kind() {
	case reflect.Struct:
		if value.Presence() != yamlsource.PresenceMapping && value.Presence() != yamlsource.PresenceEmptyMapping {
			return nil // Scalar, null and wrong-shape semantics remain with typed projection.
		}
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
				return runtimecontracts.NewUndefinedFieldDiagnostic(owner, field.Name, options, field)
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
