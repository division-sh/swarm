package contracts

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

var (
	toolPathNamePattern   = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	httpMethodNamePattern = regexp.MustCompile(`^[A-Z][A-Z0-9!#$%&'*+.^_` + "`" + `|~-]*$`)
	httpHeaderNamePattern = regexp.MustCompile(`^[!#$%&'*+.^_` + "`" + `|~0-9A-Za-z-]+$`)
)

type toolValuePath struct {
	syntax   string
	segments []toolPathSegment
}

type toolPathSegmentKind uint8

const (
	toolPathProperty toolPathSegmentKind = iota + 1
	toolPathIndex
)

type toolPathSegment struct {
	kind  toolPathSegmentKind
	name  string
	index uint32
}

func compileToolValuePath(raw string, requiredRoot string) (toolValuePath, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return toolValuePath{}, fmt.Errorf("path is required")
	}
	segments := make([]toolPathSegment, 0, 4)
	for cursor := 0; cursor < len(raw); {
		switch raw[cursor] {
		case '.':
			return toolValuePath{}, fmt.Errorf("path %q contains an empty segment", raw)
		case '[':
			end := strings.IndexByte(raw[cursor+1:], ']')
			if end < 0 {
				return toolValuePath{}, fmt.Errorf("path %q contains an unterminated index", raw)
			}
			end += cursor + 1
			index := raw[cursor+1 : end]
			parsed, err := strconv.ParseUint(index, 10, 32)
			if err != nil || strconv.FormatUint(parsed, 10) != index {
				return toolValuePath{}, fmt.Errorf("path %q contains invalid index %q", raw, index)
			}
			segments = append(segments, toolPathSegment{kind: toolPathIndex, index: uint32(parsed)})
			cursor = end + 1
			if cursor < len(raw) && raw[cursor] != '.' && raw[cursor] != '[' {
				return toolValuePath{}, fmt.Errorf("path %q requires a separator after index", raw)
			}
		default:
			end := cursor
			for end < len(raw) && raw[end] != '.' && raw[end] != '[' {
				end++
			}
			segment := raw[cursor:end]
			if !toolPathNamePattern.MatchString(segment) {
				return toolValuePath{}, fmt.Errorf("path %q contains invalid segment %q", raw, segment)
			}
			segments = append(segments, toolPathSegment{kind: toolPathProperty, name: segment})
			cursor = end
		}
		if cursor < len(raw) && raw[cursor] == '.' {
			cursor++
			if cursor == len(raw) {
				return toolValuePath{}, fmt.Errorf("path %q contains an empty segment", raw)
			}
		}
	}
	if len(segments) == 0 || requiredRoot != "" && (segments[0].kind != toolPathProperty || segments[0].name != requiredRoot) {
		return toolValuePath{}, fmt.Errorf("path %q must start with %s.", raw, requiredRoot)
	}
	return toolValuePath{syntax: raw, segments: segments}, nil
}

func (p toolValuePath) lookup(root any) (any, bool) {
	current := root
	for _, segment := range p.segments {
		switch segment.kind {
		case toolPathProperty:
			typed, ok := current.(map[string]any)
			if !ok {
				return nil, false
			}
			next, ok := typed[segment.name]
			if !ok {
				return nil, false
			}
			current = next
		case toolPathIndex:
			typed, ok := current.([]any)
			if !ok || int(segment.index) >= len(typed) {
				return nil, false
			}
			current = typed[segment.index]
		default:
			panic("admitted tool path contains unsupported segment")
		}
	}
	return current, true
}

func (p toolValuePath) set(root map[string]any, value any) error {
	if len(p.segments) == 0 {
		return fmt.Errorf("target path is required")
	}
	current := root
	for _, segment := range p.segments[:len(p.segments)-1] {
		if segment.kind != toolPathProperty {
			return fmt.Errorf("target path %q cannot construct an array index", p.syntax)
		}
		next, exists := current[segment.name]
		if !exists {
			object := map[string]any{}
			current[segment.name] = object
			current = object
			continue
		}
		object, ok := next.(map[string]any)
		if !ok {
			return fmt.Errorf("target path %q overlaps another target", p.syntax)
		}
		current = object
	}
	leaf := p.segments[len(p.segments)-1]
	if leaf.kind != toolPathProperty {
		return fmt.Errorf("target path %q cannot construct an array index", p.syntax)
	}
	if _, exists := current[leaf.name]; exists {
		return fmt.Errorf("target path %q is assigned more than once", p.syntax)
	}
	current[leaf.name] = value
	return nil
}

type toolTemplatePart struct {
	literal string
	path    toolValuePath
	start   int
	end     int
}

type toolTemplate struct {
	syntax string
	parts  []toolTemplatePart
	whole  bool
}

func compileToolTemplate(raw string, allowedRoots ...string) (toolTemplate, error) {
	if !utf8.ValidString(raw) {
		return toolTemplate{}, fmt.Errorf("template is not valid UTF-8")
	}
	parts := make([]toolTemplatePart, 0, 3)
	cursor := 0
	for cursor < len(raw) {
		startOffset := strings.Index(raw[cursor:], "{{")
		endOffset := strings.Index(raw[cursor:], "}}")
		if endOffset >= 0 && (startOffset < 0 || endOffset < startOffset) {
			return toolTemplate{}, fmt.Errorf("template contains an unmatched }}")
		}
		if startOffset < 0 {
			break
		}
		start := cursor + startOffset
		endRelative := strings.Index(raw[start+2:], "}}")
		if endRelative < 0 {
			return toolTemplate{}, fmt.Errorf("unterminated template expression in %q", raw)
		}
		end := start + 2 + endRelative + 2
		expression := strings.TrimSpace(raw[start+2 : end-2])
		if strings.ContainsAny(expression, "{}") {
			return toolTemplate{}, fmt.Errorf("template expression %q contains braces", expression)
		}
		path, err := compileToolValuePath(expression, "")
		if err != nil {
			return toolTemplate{}, fmt.Errorf("template expression %q: %w", expression, err)
		}
		if len(allowedRoots) > 0 {
			allowed := false
			for _, root := range allowedRoots {
				if path.segments[0].kind == toolPathProperty && path.segments[0].name == root {
					allowed = true
					break
				}
			}
			if !allowed {
				return toolTemplate{}, fmt.Errorf("template expression %q must start with %s.", expression, strings.Join(allowedRoots, ". or "))
			}
		}
		parts = append(parts, toolTemplatePart{literal: raw[cursor:start], path: path, start: start, end: end})
		cursor = end
	}
	if strings.Contains(raw[cursor:], "}}") {
		return toolTemplate{}, fmt.Errorf("template contains an unmatched }}")
	}
	if len(parts) == 0 {
		return toolTemplate{syntax: raw}, nil
	}
	parts = append(parts, toolTemplatePart{literal: raw[cursor:]})
	return toolTemplate{
		syntax: raw,
		parts:  parts,
		whole:  len(parts) == 2 && parts[0].literal == "" && parts[1].literal == "",
	}, nil
}

func (t toolTemplate) render(root map[string]any) (any, error) {
	if len(t.parts) == 0 {
		return t.syntax, nil
	}
	if t.whole {
		value, ok := t.parts[0].path.lookup(root)
		if !ok {
			return nil, fmt.Errorf("template variable %q is not available", t.parts[0].path.syntax)
		}
		return value, nil
	}
	var builder strings.Builder
	for _, part := range t.parts {
		builder.WriteString(part.literal)
		if len(part.path.segments) == 0 {
			continue
		}
		value, ok := part.path.lookup(root)
		if !ok {
			return nil, fmt.Errorf("template variable %q is not available", part.path.syntax)
		}
		builder.WriteString(fmt.Sprint(value))
	}
	return builder.String(), nil
}

func (t toolTemplate) renderString(root map[string]any) (string, error) {
	value, err := t.render(root)
	if err != nil {
		return "", err
	}
	return fmt.Sprint(value), nil
}

func (t toolTemplate) renderURL(root map[string]any) (string, error) {
	if len(t.parts) == 0 {
		return t.syntax, nil
	}
	if t.whole {
		return t.renderString(root)
	}
	var builder strings.Builder
	for _, part := range t.parts {
		builder.WriteString(part.literal)
		if len(part.path.segments) == 0 {
			continue
		}
		value, ok := part.path.lookup(root)
		if !ok {
			return "", fmt.Errorf("template variable %q is not available", part.path.syntax)
		}
		builder.WriteString(escapeToolURLTemplateComponent(t.syntax, part.start, part.end, fmt.Sprint(value)))
	}
	return builder.String(), nil
}

func escapeToolURLTemplateComponent(raw string, start, end int, value string) string {
	queryStart := strings.Index(raw, "?")
	fragmentStart := strings.Index(raw, "#")
	if queryStart >= 0 && queryStart <= start && (fragmentStart < 0 || start < fragmentStart) {
		return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
	}
	prefix := raw[:start]
	suffix := raw[end:]
	if strings.HasPrefix(suffix, "://") || strings.HasSuffix(prefix, "://") {
		return value
	}
	if schemeIndex := strings.LastIndex(prefix, "://"); schemeIndex >= 0 {
		if !strings.ContainsAny(prefix[schemeIndex+3:], "/?#") {
			return value
		}
	}
	if start == 0 {
		parsed, err := url.Parse(strings.TrimSpace(value))
		if err == nil && parsed.Scheme != "" && parsed.Host != "" {
			return value
		}
	}
	return url.PathEscape(value)
}

type compiledToolTemplateValue struct {
	scalar   semanticvalue.Value
	template *toolTemplate
	object   map[string]compiledToolTemplateValue
	array    []compiledToolTemplateValue
}

func compileToolTemplateValue(raw any, allowedRoots ...string) (compiledToolTemplateValue, error) {
	value, err := canonicaljson.FromGo(raw)
	if err != nil {
		return compiledToolTemplateValue{}, err
	}
	return compileSemanticToolTemplateValue(value, allowedRoots...)
}

func compileSemanticToolTemplateValue(value semanticvalue.Value, allowedRoots ...string) (compiledToolTemplateValue, error) {
	switch value.Kind() {
	case semanticvalue.KindString:
		raw, _ := value.String()
		template, err := compileToolTemplate(raw, allowedRoots...)
		if err != nil {
			return compiledToolTemplateValue{}, err
		}
		return compiledToolTemplateValue{template: &template}, nil
	case semanticvalue.KindObject:
		members, _ := value.ObjectMap()
		out := make(map[string]compiledToolTemplateValue, len(members))
		for key, member := range members {
			compiled, err := compileSemanticToolTemplateValue(member, allowedRoots...)
			if err != nil {
				return compiledToolTemplateValue{}, fmt.Errorf("%s: %w", key, err)
			}
			out[key] = compiled
		}
		return compiledToolTemplateValue{object: out}, nil
	case semanticvalue.KindArray:
		out := make([]compiledToolTemplateValue, 0, value.Len())
		for index := 0; index < value.Len(); index++ {
			member, _ := value.At(index)
			compiled, err := compileSemanticToolTemplateValue(member, allowedRoots...)
			if err != nil {
				return compiledToolTemplateValue{}, fmt.Errorf("[%d]: %w", index, err)
			}
			out = append(out, compiled)
		}
		return compiledToolTemplateValue{array: out}, nil
	default:
		return compiledToolTemplateValue{scalar: value}, nil
	}
}

func (v compiledToolTemplateValue) render(root map[string]any) (any, error) {
	switch {
	case v.template != nil:
		return v.template.render(root)
	case v.object != nil:
		out := make(map[string]any, len(v.object))
		for key, member := range v.object {
			rendered, err := member.render(root)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", key, err)
			}
			out[key] = rendered
		}
		return out, nil
	case v.array != nil:
		out := make([]any, 0, len(v.array))
		for index, member := range v.array {
			rendered, err := member.render(root)
			if err != nil {
				return nil, fmt.Errorf("[%d]: %w", index, err)
			}
			out = append(out, rendered)
		}
		return out, nil
	default:
		return v.scalar.Interface(), nil
	}
}

func (v compiledToolTemplateValue) syntaxValue() any {
	switch {
	case v.template != nil:
		return v.template.syntax
	case v.object != nil:
		out := make(map[string]any, len(v.object))
		for key, member := range v.object {
			out[key] = member.syntaxValue()
		}
		return out
	case v.array != nil:
		out := make([]any, 0, len(v.array))
		for _, member := range v.array {
			out = append(out, member.syntaxValue())
		}
		return out
	default:
		return v.scalar.Interface()
	}
}

type compiledHTTPToolSpec struct {
	method         string
	url            toolTemplate
	staticHost     string
	headers        map[string]toolTemplate
	body           compiledToolTemplateValue
	hasBody        bool
	timeoutSeconds int
}

// ToolHTTPExecution is the immutable executable HTTP value admitted by a tool
// contract. Authored template strings are not exposed through this surface.
type ToolHTTPExecution struct {
	value *compiledHTTPToolSpec
}

type PreparedToolHTTPRequest struct {
	method  string
	url     string
	headers http.Header
	body    []byte
	timeout time.Duration
}

func AdmitToolHTTPExecution(spec HTTPToolSpec) (ToolHTTPExecution, error) {
	compiled, err := compileHTTPToolSpec(spec)
	if err != nil {
		return ToolHTTPExecution{}, err
	}
	return ToolHTTPExecution{value: &compiled}, nil
}

func (r PreparedToolHTTPRequest) Method() string         { return r.method }
func (r PreparedToolHTTPRequest) URL() string            { return r.url }
func (r PreparedToolHTTPRequest) Headers() http.Header   { return r.headers.Clone() }
func (r PreparedToolHTTPRequest) Body() []byte           { return append([]byte(nil), r.body...) }
func (r PreparedToolHTTPRequest) Timeout() time.Duration { return r.timeout }

func (e ToolHTTPExecution) StaticHost() string {
	if e.value == nil {
		return ""
	}
	return e.value.staticHost
}

func compileHTTPToolSpec(spec HTTPToolSpec) (compiledHTTPToolSpec, error) {
	method := strings.ToUpper(strings.TrimSpace(spec.Method))
	if method == "" {
		return compiledHTTPToolSpec{}, fmt.Errorf("http.method is required")
	}
	if !httpMethodNamePattern.MatchString(method) {
		return compiledHTTPToolSpec{}, fmt.Errorf("http.method %q is invalid", spec.Method)
	}
	rawURL := strings.TrimSpace(spec.URL)
	if rawURL == "" {
		return compiledHTTPToolSpec{}, fmt.Errorf("http.url is required")
	}
	urlTemplate, err := compileToolTemplate(rawURL, "input", "credentials")
	if err != nil {
		return compiledHTTPToolSpec{}, fmt.Errorf("http.url: %w", err)
	}
	if len(urlTemplate.parts) == 0 {
		if _, err := parseToolHTTPURL(rawURL); err != nil {
			return compiledHTTPToolSpec{}, fmt.Errorf("http.url: %w", err)
		}
	} else if !urlTemplate.whole {
		if err := validateTemplatedToolHTTPURLShape(rawURL); err != nil {
			return compiledHTTPToolSpec{}, fmt.Errorf("http.url: %w", err)
		}
	}
	headers := make(map[string]toolTemplate, len(spec.Headers))
	seenHeaders := make(map[string]struct{}, len(spec.Headers))
	for rawName, rawValue := range spec.Headers {
		name := strings.TrimSpace(rawName)
		if !httpHeaderNamePattern.MatchString(name) {
			return compiledHTTPToolSpec{}, fmt.Errorf("http header name %q is invalid", rawName)
		}
		canonical := http.CanonicalHeaderKey(name)
		folded := strings.ToLower(canonical)
		if _, duplicate := seenHeaders[folded]; duplicate {
			return compiledHTTPToolSpec{}, fmt.Errorf("http header %q is duplicated", canonical)
		}
		seenHeaders[folded] = struct{}{}
		template, err := compileToolTemplate(rawValue, "input", "credentials")
		if err != nil {
			return compiledHTTPToolSpec{}, fmt.Errorf("http.headers[%s]: %w", canonical, err)
		}
		headers[canonical] = template
	}
	staticHost := ""
	if parsed, parseErr := url.Parse(rawURL); parseErr == nil {
		switch strings.ToLower(parsed.Scheme) {
		case "http", "https":
			staticHost = strings.TrimSpace(parsed.Host)
		case "":
		default:
			return compiledHTTPToolSpec{}, fmt.Errorf("http.url scheme %q is unsupported", parsed.Scheme)
		}
	}
	compiled := compiledHTTPToolSpec{method: method, url: urlTemplate, staticHost: staticHost, headers: headers, timeoutSeconds: spec.TimeoutSeconds}
	if spec.TimeoutSeconds < 0 {
		return compiledHTTPToolSpec{}, fmt.Errorf("http.timeout_seconds must be non-negative")
	}
	if spec.Body != nil {
		compiled.body, err = compileToolTemplateValue(spec.Body, "input", "credentials")
		if err != nil {
			return compiledHTTPToolSpec{}, fmt.Errorf("http.body: %w", err)
		}
		compiled.hasBody = true
	}
	return compiled, nil
}

func validateTemplatedToolHTTPURLShape(raw string) error {
	lower := strings.ToLower(raw)
	prefixLength := 0
	switch {
	case strings.HasPrefix(lower, "http://"):
		prefixLength = len("http://")
	case strings.HasPrefix(lower, "https://"):
		prefixLength = len("https://")
	default:
		if strings.HasPrefix(raw, "{{") {
			// A leading value may supply the scheme or complete base URL. The
			// prepared request still undergoes absolute HTTP(S) validation.
			return nil
		}
		return fmt.Errorf("templated URL must use a literal http:// or https:// prefix, or start with a template-supplied scheme/base URL")
	}
	authority := raw[prefixLength:]
	if end := strings.IndexAny(authority, "/?#"); end >= 0 {
		authority = authority[:end]
	}
	if authority == "" {
		return fmt.Errorf("absolute URL host is required")
	}
	return nil
}

func (e ToolHTTPExecution) Prepare(input, credentials map[string]any) (PreparedToolHTTPRequest, error) {
	if e.value == nil {
		return PreparedToolHTTPRequest{}, fmt.Errorf("HTTP execution plan is missing")
	}
	env := map[string]any{"input": input, "credentials": credentials}
	resolvedURL, err := e.value.url.renderURL(env)
	if err != nil {
		return PreparedToolHTTPRequest{}, fmt.Errorf("http.url: %w", err)
	}
	resolvedURL = strings.TrimSpace(resolvedURL)
	if resolvedURL == "" {
		return PreparedToolHTTPRequest{}, fmt.Errorf("http.url resolved to an empty value")
	}
	if _, err := parseToolHTTPURL(resolvedURL); err != nil {
		return PreparedToolHTTPRequest{}, fmt.Errorf("http.url resolved to an invalid URL: %w", err)
	}
	headers := make(http.Header, len(e.value.headers))
	names := make([]string, 0, len(e.value.headers))
	for name := range e.value.headers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		rendered, err := e.value.headers[name].renderString(env)
		if err != nil {
			return PreparedToolHTTPRequest{}, fmt.Errorf("http.headers[%s]: %w", name, err)
		}
		headers.Set(name, strings.TrimSpace(rendered))
	}
	var body []byte
	if e.value.hasBody {
		rendered, err := e.value.body.render(env)
		if err != nil {
			return PreparedToolHTTPRequest{}, fmt.Errorf("http.body: %w", err)
		}
		body, err = json.Marshal(rendered)
		if err != nil {
			return PreparedToolHTTPRequest{}, fmt.Errorf("http.body: %w", err)
		}
		if strings.TrimSpace(headers.Get("Content-Type")) == "" {
			headers.Set("Content-Type", "application/json")
		}
	}
	timeout := 30 * time.Second
	if e.value.timeoutSeconds > 0 {
		timeout = time.Duration(e.value.timeoutSeconds) * time.Second
	}
	return PreparedToolHTTPRequest{
		method: e.value.method, url: resolvedURL, headers: headers, body: body, timeout: timeout,
	}, nil
}

func parseToolHTTPURL(raw string) (*url.URL, error) {
	parsed, err := url.ParseRequestURI(strings.TrimSpace(raw))
	if err != nil {
		return nil, err
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("absolute URL host is required")
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https":
		return parsed, nil
	default:
		return nil, fmt.Errorf("URL scheme must be http or https")
	}
}

func (e ToolHTTPExecution) syntax() HTTPToolSpec {
	if e.value == nil {
		return HTTPToolSpec{}
	}
	headers := make(map[string]string, len(e.value.headers))
	for name, template := range e.value.headers {
		headers[name] = template.syntax
	}
	var body any
	if e.value.hasBody {
		body = e.value.body.syntaxValue()
	}
	return HTTPToolSpec{
		Method: e.value.method, URL: e.value.url.syntax, Headers: headers,
		Body: body, TimeoutSeconds: e.value.timeoutSeconds,
	}
}

func (e ToolHTTPExecution) Readback() HTTPToolSpec {
	return e.syntax()
}

// ToolResponseMapping is a compiled response projection owned by the admitted
// tool contract.
type ToolResponseMapping struct {
	value *compiledToolTemplateValue
}

func compileToolResponseMapping(mapping map[string]any) (ToolResponseMapping, error) {
	if mapping == nil {
		return ToolResponseMapping{}, fmt.Errorf("response_mapping must be an object")
	}
	compiled, err := compileToolTemplateValue(mapping, "response")
	if err != nil {
		return ToolResponseMapping{}, fmt.Errorf("response_mapping: %w", err)
	}
	if compiled.object == nil {
		return ToolResponseMapping{}, fmt.Errorf("response_mapping must be an object")
	}
	return ToolResponseMapping{value: &compiled}, nil
}

func (m ToolResponseMapping) validateOutputShape(schema ToolInputSchema) error {
	if m.value == nil {
		return fmt.Errorf("response mapping is missing")
	}
	if schema.IsZero() {
		return nil
	}
	if err := validateCompiledToolTemplateShape("response_mapping", *m.value, schema); err != nil {
		return err
	}
	return nil
}

func validateCompiledToolTemplateShape(path string, value compiledToolTemplateValue, schema ToolInputSchema) error {
	if !value.hasDynamicTemplate() {
		if err := schema.Validate(value.syntaxValue()); err != nil {
			return fmt.Errorf("%s is incompatible with output_schema: %w", path, err)
		}
		return nil
	}
	if value.template != nil {
		return nil
	}
	if schema.Kind() == ToolSchemaAny {
		return nil
	}
	switch {
	case value.object != nil:
		if schema.Kind() != ToolSchemaObject {
			return fmt.Errorf("%s produces object but output_schema requires %s", path, schema.Kind())
		}
		for _, name := range schema.RequiredProperties() {
			if _, ok := value.object[name]; !ok {
				return fmt.Errorf("%s omits required output property %q", path, name)
			}
		}
		for name, member := range value.object {
			property, ok := schema.Property(name)
			if !ok {
				if additional, allowed := schema.AdditionalPropertiesSchema(); allowed {
					property = additional
				} else if allowed, declared := schema.AdditionalPropertiesAllowed(); declared && !allowed {
					return fmt.Errorf("%s.%s is forbidden by output_schema", path, name)
				} else {
					continue
				}
			}
			if err := validateCompiledToolTemplateShape(path+"."+name, member, property); err != nil {
				return err
			}
		}
	case value.array != nil:
		if schema.Kind() != ToolSchemaArray {
			return fmt.Errorf("%s produces array but output_schema requires %s", path, schema.Kind())
		}
		if minimum, ok := schema.MinItems(); ok && len(value.array) < minimum {
			return fmt.Errorf("%s produces %d items but output_schema requires at least %d", path, len(value.array), minimum)
		}
		if maximum, ok := schema.MaxItems(); ok && len(value.array) > maximum {
			return fmt.Errorf("%s produces %d items but output_schema permits at most %d", path, len(value.array), maximum)
		}
		if items, ok := schema.ItemsSchema(); ok {
			for index, member := range value.array {
				if err := validateCompiledToolTemplateShape(fmt.Sprintf("%s[%d]", path, index), member, items); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (v compiledToolTemplateValue) hasDynamicTemplate() bool {
	switch {
	case v.template != nil:
		return true
	case v.object != nil:
		for _, member := range v.object {
			if member.hasDynamicTemplate() {
				return true
			}
		}
	case v.array != nil:
		for _, member := range v.array {
			if member.hasDynamicTemplate() {
				return true
			}
		}
	}
	return false
}

func (m ToolResponseMapping) Render(response map[string]any) (any, error) {
	if m.value == nil {
		return nil, fmt.Errorf("response mapping is missing")
	}
	return m.value.render(response)
}

func (m ToolResponseMapping) syntax() map[string]any {
	if m.value == nil {
		return nil
	}
	out, _ := m.value.syntaxValue().(map[string]any)
	return out
}

func (m ToolResponseMapping) Readback() map[string]any {
	return m.syntax()
}

type toolResponseSuccessKind uint8

const (
	toolResponseSuccessHTTP2xx toolResponseSuccessKind = iota + 1
	toolResponseSuccessJSONFieldEquals
)

type compiledToolResponseSuccess struct {
	kind   toolResponseSuccessKind
	path   toolValuePath
	equals semanticvalue.Value
}

// ToolResponseSuccessPolicy is the exhaustive admitted provider-success
// policy. Path syntax and equality values are compiled before publication.
type ToolResponseSuccessPolicy struct {
	value *compiledToolResponseSuccess
}

func AdmitToolResponseSuccessPolicy(success HTTPResponseSuccess) (ToolResponseSuccessPolicy, error) {
	return compileToolResponseSuccess(success)
}

func compileToolResponseSuccess(success HTTPResponseSuccess) (ToolResponseSuccessPolicy, error) {
	switch kind := strings.ToLower(strings.TrimSpace(success.Kind)); kind {
	case "http_status_2xx":
		if strings.TrimSpace(success.Path) != "" {
			return ToolResponseSuccessPolicy{}, fmt.Errorf("response_success.path is forbidden for kind %s", kind)
		}
		if success.Equals != nil {
			return ToolResponseSuccessPolicy{}, fmt.Errorf("response_success.equals is forbidden for kind %s", kind)
		}
		path, _ := compileToolValuePath("response.status", "response")
		return ToolResponseSuccessPolicy{value: &compiledToolResponseSuccess{kind: toolResponseSuccessHTTP2xx, path: path}}, nil
	case "json_field_equals":
		path, err := compileToolValuePath(success.Path, "response")
		if err != nil {
			return ToolResponseSuccessPolicy{}, fmt.Errorf("response_success.path: %w", err)
		}
		if success.Equals == nil {
			return ToolResponseSuccessPolicy{}, fmt.Errorf("response_success.equals is required for kind %s", kind)
		}
		equals, err := canonicaljson.FromGo(success.Equals)
		if err != nil {
			return ToolResponseSuccessPolicy{}, fmt.Errorf("response_success.equals: %w", err)
		}
		switch equals.Kind() {
		case semanticvalue.KindString, semanticvalue.KindNumber, semanticvalue.KindBool:
		default:
			return ToolResponseSuccessPolicy{}, fmt.Errorf("response_success.equals must be a scalar value")
		}
		return ToolResponseSuccessPolicy{value: &compiledToolResponseSuccess{
			kind: toolResponseSuccessJSONFieldEquals, path: path, equals: equals,
		}}, nil
	case "":
		return ToolResponseSuccessPolicy{}, fmt.Errorf("response_success.kind is required")
	default:
		return ToolResponseSuccessPolicy{}, fmt.Errorf("response_success.kind %q is unsupported", strings.TrimSpace(success.Kind))
	}
}

func (p ToolResponseSuccessPolicy) Evaluate(response map[string]any) error {
	if p.value == nil {
		return nil
	}
	got, ok := p.value.path.lookup(response)
	if !ok {
		return fmt.Errorf("response_success path %q did not resolve", p.value.path.syntax)
	}
	switch p.value.kind {
	case toolResponseSuccessHTTP2xx:
		status, ok := numericToolValue(got)
		if !ok || status < 200 || status >= 300 {
			return fmt.Errorf("response_success failed at %s: want HTTP 2xx", p.value.path.syntax)
		}
		return nil
	case toolResponseSuccessJSONFieldEquals:
		admitted, err := canonicaljson.FromGo(got)
		if err != nil || !admitted.Equal(p.value.equals) {
			return fmt.Errorf("response_success failed at %s: value did not equal the admitted scalar", p.value.path.syntax)
		}
		return nil
	default:
		panic("admitted response-success policy contains unsupported kind")
	}
}

func (p ToolResponseSuccessPolicy) syntax() HTTPResponseSuccess {
	if p.value == nil {
		return HTTPResponseSuccess{}
	}
	switch p.value.kind {
	case toolResponseSuccessHTTP2xx:
		return HTTPResponseSuccess{Kind: "http_status_2xx"}
	case toolResponseSuccessJSONFieldEquals:
		return HTTPResponseSuccess{
			Kind: "json_field_equals", Path: p.value.path.syntax, Equals: p.value.equals.Interface(),
		}
	default:
		panic("admitted response-success policy contains unsupported kind")
	}
}

func (p ToolResponseSuccessPolicy) Readback() HTTPResponseSuccess {
	return p.syntax()
}

func (p ToolResponseSuccessPolicy) Equal(other ToolResponseSuccessPolicy) bool {
	if p.value == nil || other.value == nil {
		return p.value == nil && other.value == nil
	}
	if p.value.kind != other.value.kind || p.value.path.syntax != other.value.path.syntax {
		return false
	}
	if p.value.kind == toolResponseSuccessHTTP2xx {
		return true
	}
	return p.value.equals.Equal(other.value.equals)
}

func numericToolValue(value any) (float64, bool) {
	admitted, err := canonicaljson.FromGo(value)
	if err != nil {
		return 0, false
	}
	return admitted.Number()
}

type compiledToolResultField struct {
	target toolValuePath
	source toolValuePath
}

type compiledResultProjectionValue struct {
	fields       []compiledToolResultField
	outputSchema ToolInputSchema
}

// ToolCompiledResultProjection is the parsed result projection carried by the
// admitted tool owner.
type ToolCompiledResultProjection struct {
	value *compiledResultProjectionValue
}

func compileToolResultProjection(result CompiledResultProjection, sourceSchema ToolInputSchema) (compiledResultProjectionValue, error) {
	if err := sourceSchema.ValidateDefinition(); err != nil {
		return compiledResultProjectionValue{}, fmt.Errorf("compiled result source schema: %w", err)
	}
	if err := result.OutputSchema.ValidateDefinition(); err != nil {
		return compiledResultProjectionValue{}, fmt.Errorf("compiled result output schema: %w", err)
	}
	if result.OutputSchema.Kind() != ToolSchemaObject {
		return compiledResultProjectionValue{}, fmt.Errorf("compiled result output schema must be an object")
	}
	targets := make([]string, 0, len(result.Fields))
	for target := range result.Fields {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	fields := make([]compiledToolResultField, 0, len(targets))
	for _, rawTarget := range targets {
		target, err := compileToolValuePath(rawTarget, "")
		if err != nil {
			return compiledResultProjectionValue{}, fmt.Errorf("compiled result target %q: %w", rawTarget, err)
		}
		if toolPathContainsIndex(target) {
			return compiledResultProjectionValue{}, fmt.Errorf("compiled result target %q cannot construct an array index", target.syntax)
		}
		source, err := compileToolValuePath(result.Fields[rawTarget].From, "result")
		if err != nil {
			return compiledResultProjectionValue{}, fmt.Errorf("compiled result source for %q: %w", rawTarget, err)
		}
		for _, prior := range fields {
			if toolPathsOverlap(prior.target, target) {
				return compiledResultProjectionValue{}, fmt.Errorf("compiled result target %q overlaps %q", target.syntax, prior.target.syntax)
			}
		}
		fields = append(fields, compiledToolResultField{target: target, source: source})
	}
	for _, field := range fields {
		sourceField, ok := guaranteedToolSchemaAtValuePath(sourceSchema, field.source, 1)
		if !ok {
			return compiledResultProjectionValue{}, fmt.Errorf("compiled result source %q is not guaranteed by the admitted source schema", field.source.syntax)
		}
		targetField, ok := toolSchemaAtValuePath(result.OutputSchema, field.target, 0)
		if !ok {
			return compiledResultProjectionValue{}, fmt.Errorf("compiled result target %q is absent from the admitted output schema", field.target.syntax)
		}
		if err := sourceField.ValidateAssignableTo("compiled result "+field.source.syntax+" -> "+field.target.syntax, targetField); err != nil {
			return compiledResultProjectionValue{}, err
		}
	}
	for _, required := range requiredToolSchemaValuePaths(result.OutputSchema, nil) {
		covered := false
		for _, field := range fields {
			if toolPathsOverlap(field.target, required) {
				covered = true
				break
			}
		}
		if !covered {
			return compiledResultProjectionValue{}, fmt.Errorf("compiled result required target %q is not assigned", required.syntax)
		}
	}
	return compiledResultProjectionValue{fields: fields, outputSchema: result.OutputSchema}, nil
}

func toolPathContainsIndex(path toolValuePath) bool {
	for _, segment := range path.segments {
		if segment.kind == toolPathIndex {
			return true
		}
	}
	return false
}

func toolSchemaAtValuePath(schema ToolInputSchema, path toolValuePath, offset int) (ToolInputSchema, bool) {
	if offset < 0 || offset > len(path.segments) {
		return ToolInputSchema{}, false
	}
	current := schema
	for _, segment := range path.segments[offset:] {
		switch segment.kind {
		case toolPathProperty:
			if current.Kind() != ToolSchemaObject {
				return ToolInputSchema{}, false
			}
			property, ok := current.Property(segment.name)
			if !ok {
				property, ok = current.AdditionalPropertiesSchema()
			}
			if !ok {
				return ToolInputSchema{}, false
			}
			current = property
		case toolPathIndex:
			if current.Kind() != ToolSchemaArray {
				return ToolInputSchema{}, false
			}
			items, ok := current.ItemsSchema()
			if !ok {
				return ToolInputSchema{}, false
			}
			current = items
		default:
			panic("admitted tool path contains unsupported segment")
		}
	}
	return current, true
}

func guaranteedToolSchemaAtValuePath(schema ToolInputSchema, path toolValuePath, offset int) (ToolInputSchema, bool) {
	if offset < 0 || offset > len(path.segments) {
		return ToolInputSchema{}, false
	}
	current := schema
	for _, segment := range path.segments[offset:] {
		switch segment.kind {
		case toolPathProperty:
			if current.Kind() != ToolSchemaObject || !current.IsRequired(segment.name) {
				return ToolInputSchema{}, false
			}
			property, ok := current.Property(segment.name)
			if !ok {
				return ToolInputSchema{}, false
			}
			current = property
		case toolPathIndex:
			if current.Kind() != ToolSchemaArray {
				return ToolInputSchema{}, false
			}
			minimum, constrained := current.MinItems()
			if !constrained || minimum <= int(segment.index) {
				return ToolInputSchema{}, false
			}
			items, ok := current.ItemsSchema()
			if !ok {
				return ToolInputSchema{}, false
			}
			current = items
		default:
			panic("admitted tool path contains unsupported segment")
		}
	}
	return current, true
}

func requiredToolSchemaValuePaths(schema ToolInputSchema, prefix []toolPathSegment) []toolValuePath {
	if schema.Kind() != ToolSchemaObject {
		return nil
	}
	var out []toolValuePath
	for _, name := range schema.RequiredProperties() {
		property, ok := schema.Property(name)
		if !ok {
			continue
		}
		segments := append(append([]toolPathSegment(nil), prefix...), toolPathSegment{kind: toolPathProperty, name: name})
		children := requiredToolSchemaValuePaths(property, segments)
		if len(children) == 0 {
			out = append(out, toolValuePath{syntax: toolPathSyntax(segments), segments: segments})
			continue
		}
		out = append(out, children...)
	}
	return out
}

func toolPathSyntax(segments []toolPathSegment) string {
	var builder strings.Builder
	for _, segment := range segments {
		switch segment.kind {
		case toolPathProperty:
			if builder.Len() > 0 {
				builder.WriteByte('.')
			}
			builder.WriteString(segment.name)
		case toolPathIndex:
			builder.WriteByte('[')
			builder.WriteString(strconv.FormatUint(uint64(segment.index), 10))
			builder.WriteByte(']')
		}
	}
	return builder.String()
}

func toolPathsOverlap(left, right toolValuePath) bool {
	shorter := left.segments
	longer := right.segments
	if len(shorter) > len(longer) {
		shorter, longer = longer, shorter
	}
	for index := range shorter {
		if shorter[index] != longer[index] {
			return false
		}
	}
	return true
}

func (p ToolCompiledResultProjection) Project(result any) (map[string]any, error) {
	if p.value == nil {
		return nil, fmt.Errorf("compiled result projection is missing")
	}
	root := map[string]any{"result": result}
	out := map[string]any{}
	for _, field := range p.value.fields {
		value, ok := field.source.lookup(root)
		if !ok {
			return nil, fmt.Errorf("compiled result source %q is missing", field.source.syntax)
		}
		if err := field.target.set(out, value); err != nil {
			return nil, err
		}
	}
	if err := p.value.outputSchema.Validate(out); err != nil {
		return nil, fmt.Errorf("compiled result output: %w", err)
	}
	return out, nil
}

func (p ToolCompiledResultProjection) OutputSchema() ToolInputSchema {
	if p.value == nil {
		return ToolInputSchema{}
	}
	return p.value.outputSchema
}

func (p ToolCompiledResultProjection) syntax() CompiledResultProjection {
	if p.value == nil {
		return CompiledResultProjection{}
	}
	fields := make(map[string]CompiledResultField, len(p.value.fields))
	for _, field := range p.value.fields {
		fields[field.target.syntax] = CompiledResultField{From: field.source.syntax}
	}
	return CompiledResultProjection{Fields: fields, OutputSchema: p.value.outputSchema}
}

func (p ToolCompiledResultProjection) Readback() CompiledResultProjection {
	return p.syntax()
}
