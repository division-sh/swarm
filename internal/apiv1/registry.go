package apiv1

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"swarm/internal/apispec"
)

const MethodUnavailableCode = "METHOD_UNAVAILABLE"

type Registry struct {
	api        *apispec.APISpecification
	methods    map[string]apispec.Method
	errorCodes map[string]int
}

func LoadRegistry(platformSpecPath string) (*Registry, error) {
	api, err := apispec.LoadPlatformSpec(strings.TrimSpace(platformSpecPath))
	if err != nil {
		return nil, err
	}
	if _, err := apispec.Validate(api); err != nil {
		return nil, err
	}
	return NewRegistry(api)
}

func NewRegistry(api *apispec.APISpecification) (*Registry, error) {
	if _, err := apispec.Validate(api); err != nil {
		return nil, err
	}
	methods := make(map[string]apispec.Method, len(api.MethodCatalog))
	for name, method := range api.MethodCatalog {
		methods[strings.TrimSpace(name)] = method
	}
	errorCodes := apispec.ApplicationErrorCodes(api.Components.Errors)
	if _, ok := errorCodes[MethodUnavailableCode]; !ok {
		return nil, fmt.Errorf("api specification missing components.errors.%s", MethodUnavailableCode)
	}
	return &Registry{
		api:        api,
		methods:    methods,
		errorCodes: errorCodes,
	}, nil
}

func (r *Registry) Method(name string) (apispec.Method, bool) {
	if r == nil {
		return apispec.Method{}, false
	}
	method, ok := r.methods[strings.TrimSpace(name)]
	return method, ok
}

func (r *Registry) MethodNames() []string {
	if r == nil {
		return nil
	}
	names := make([]string, 0, len(r.methods))
	for name := range r.methods {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r *Registry) ApplicationErrorCode(code string) (int, bool) {
	if r == nil {
		return 0, false
	}
	numeric, ok := r.errorCodes[strings.TrimSpace(code)]
	return numeric, ok
}

func OpenRPCMethodNames(path string) ([]string, error) {
	raw, err := os.ReadFile(strings.TrimSpace(path))
	if err != nil {
		return nil, err
	}
	var doc apispec.OpenRPCDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse openrpc: %w", err)
	}
	names := make([]string, 0, len(doc.Methods))
	for _, method := range doc.Methods {
		names = append(names, strings.TrimSpace(method.Name))
	}
	sort.Strings(names)
	return names, nil
}
