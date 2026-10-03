package config

import (
	"reflect"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

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

func validateConfigSource(node *yaml.Node, shape reflect.Type, owner string) error {
	value := yamlsource.ValueFromNode(node)
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
		for name := range declared {
			options[name] = struct{}{}
		}
		for _, field := range fields {
			child, present := declared[field.Name]
			if !present {
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
