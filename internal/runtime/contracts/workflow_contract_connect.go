package contracts

import (
	"fmt"
	"sort"
	"strings"
)

func (p FlowInputEventPin) PinName() string {
	return strings.TrimSpace(p.Name)
}

func (p FlowInputEventPin) EventType() string {
	if eventType := strings.TrimSpace(p.Event); eventType != "" {
		return eventType
	}
	return strings.TrimSpace(p.Name)
}

func (p FlowInputEventPin) normalized() FlowInputEventPin {
	out := FlowInputEventPin{
		Name:       strings.TrimSpace(p.Name),
		Event:      strings.TrimSpace(p.Event),
		Source:     strings.ToLower(strings.TrimSpace(p.Source)),
		Resolution: p.Resolution.normalized(),
		Carries:    p.Carries.normalized(),
	}
	if out.Event == "" {
		out.Event = out.Name
	}
	if p.Address != nil {
		address := p.Address.normalized()
		out.Address = &address
	}
	return out
}

func (c FlowInputPinCarries) normalized() FlowInputPinCarries {
	if len(c) == 0 {
		return nil
	}
	out := FlowInputPinCarries{}
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		name := strings.TrimSpace(key)
		if name == "" {
			continue
		}
		carry := c[key].normalized()
		if carry.From == "" && carry.Type == "" {
			continue
		}
		out[name] = carry
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (c FlowInputPinCarry) normalized() FlowInputPinCarry {
	return FlowInputPinCarry{
		From: strings.TrimSpace(c.From),
		Type: strings.TrimSpace(c.Type),
	}
}

func (r FlowInputPinResolution) Empty() bool {
	r = r.normalized()
	return r.Mode == "" &&
		r.InstanceKey.Empty() &&
		r.Aggregation == "" &&
		r.Window == "" &&
		len(r.DedupBy) == 0 &&
		r.Singleton == "" &&
		r.RepliesTo == "" &&
		r.CorrelationKey == ""
}

func (r FlowInputPinResolution) normalized() FlowInputPinResolution {
	return FlowInputPinResolution{
		Mode:           strings.ToLower(strings.TrimSpace(r.Mode)),
		InstanceKey:    r.InstanceKey.normalized(),
		Aggregation:    strings.ToLower(strings.TrimSpace(r.Aggregation)),
		Window:         strings.TrimSpace(r.Window),
		DedupBy:        normalizeStringListPreserveOrder(r.DedupBy),
		Singleton:      strings.TrimSpace(r.Singleton),
		RepliesTo:      strings.TrimSpace(r.RepliesTo),
		CorrelationKey: strings.TrimSpace(r.CorrelationKey),
	}
}

func (k FlowInputPinResolutionInstanceKey) Empty() bool {
	k = k.normalized()
	return k.From == "" && k.Mint == "" && k.As == ""
}

func (k FlowInputPinResolutionInstanceKey) normalized() FlowInputPinResolutionInstanceKey {
	return FlowInputPinResolutionInstanceKey{
		From: strings.TrimSpace(k.From),
		Mint: strings.ToLower(strings.TrimSpace(k.Mint)),
		As:   strings.TrimSpace(k.As),
	}
}

func (p FlowOutputEventPin) PinName() string {
	return strings.TrimSpace(p.Name)
}

func (p FlowOutputEventPin) EventType() string {
	if eventType := strings.TrimSpace(p.Event); eventType != "" {
		return eventType
	}
	return strings.TrimSpace(p.Name)
}

func (p FlowOutputEventPin) normalized() FlowOutputEventPin {
	out := FlowOutputEventPin{
		Name:    strings.TrimSpace(p.Name),
		Event:   strings.TrimSpace(p.Event),
		Key:     strings.TrimSpace(p.Key),
		Carries: normalizeOutputPinCarries(p.Carries),
	}
	if out.Event == "" {
		out.Event = out.Name
	}
	return out
}

func (a FlowInputPinAddress) normalized() FlowInputPinAddress {
	return FlowInputPinAddress{
		By:          strings.TrimSpace(a.By),
		Source:      strings.TrimSpace(a.Source),
		Target:      strings.TrimSpace(a.Target),
		Cardinality: strings.TrimSpace(a.Cardinality),
		Mode:        strings.TrimSpace(a.Mode),
	}
}

func (c FlowPackageConnect) FromRef() (FlowPackagePinRef, error) {
	return parseFlowPackagePinRef(c.From)
}

func (c FlowPackageConnect) ToRef() (FlowPackagePinRef, error) {
	return parseFlowPackagePinRef(c.To)
}

func (c FlowPackageConnect) WithPackageKey(packageKey string) FlowPackageConnect {
	out := c.normalized()
	out.PackageKey = strings.TrimSpace(packageKey)
	return out
}

func (c FlowPackageConnect) WithPackageSource(packageKey, sourceFile string) FlowPackageConnect {
	out := c.WithPackageKey(packageKey)
	out.SourceFile = strings.TrimSpace(sourceFile)
	return out
}

func (c FlowPackageConnect) AuthoredLocation() string {
	file := strings.TrimSpace(c.SourceFile)
	if file == "" || c.SourceLine <= 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", file, c.SourceLine)
}

func (c FlowPackageConnect) normalized() FlowPackageConnect {
	return FlowPackageConnect{
		PackageKey: strings.TrimSpace(c.PackageKey),
		SourceFile: strings.TrimSpace(c.SourceFile),
		SourceLine: c.SourceLine,
		From:       strings.TrimSpace(c.From),
		To:         strings.TrimSpace(c.To),
		Adapter:    strings.TrimSpace(c.Adapter),
		Using:      c.Using.normalized(),
		Map:        cloneFlowPackageConnectMap(c.Map),
		Delivery:   strings.TrimSpace(c.Delivery),
		Reply:      normalizeStringMap(c.Reply),
	}
}

func (u FlowPackageConnectUsing) normalized() FlowPackageConnectUsing {
	return FlowPackageConnectUsing{
		Instance: u.Instance.normalized(),
	}
}

func (a FlowPackageConnectInstanceAdapter) normalized() FlowPackageConnectInstanceAdapter {
	return FlowPackageConnectInstanceAdapter{
		Declared: a.Declared,
		Source:   normalizeStringListPreserveOrder(a.Source),
		Target:   normalizeStringListPreserveOrder(a.Target),
	}
}

func normalizeStringListPreserveOrder(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			out = append(out, value)
		}
	}
	return out
}

func parseFlowPackagePinRef(raw string) (FlowPackagePinRef, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return FlowPackagePinRef{}, fmt.Errorf("pin reference is required")
	}
	if strings.HasPrefix(raw, ".") {
		pin := strings.TrimSpace(strings.TrimPrefix(raw, "."))
		if pin == "" {
			return FlowPackagePinRef{}, fmt.Errorf("pin reference %q must use .{root_pin_name} or {flow_id}.{pin_name}", raw)
		}
		return FlowPackagePinRef{
			Root: true,
			Pin:  pin,
		}, nil
	}
	idx := strings.Index(raw, ".")
	if idx <= 0 || idx >= len(raw)-1 {
		return FlowPackagePinRef{}, fmt.Errorf("pin reference %q must use .{root_pin_name} or {flow_id}.{pin_name}", raw)
	}
	ref := FlowPackagePinRef{
		FlowID: strings.TrimSpace(raw[:idx]),
		Pin:    strings.TrimSpace(raw[idx+1:]),
	}
	if ref.FlowID == "" || ref.Pin == "" {
		return FlowPackagePinRef{}, fmt.Errorf("pin reference %q must use .{root_pin_name} or non-empty flow and pin names", raw)
	}
	return ref, nil
}

func inputEventPinsFromEvents(events []string) []FlowInputEventPin {
	out := make([]FlowInputEventPin, 0, len(events))
	for _, eventType := range events {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		out = append(out, FlowInputEventPin{Name: eventType, Event: eventType})
	}
	return out
}

func outputEventPinsFromEvents(events []string) []FlowOutputEventPin {
	out := make([]FlowOutputEventPin, 0, len(events))
	for _, eventType := range events {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		out = append(out, FlowOutputEventPin{Name: eventType, Event: eventType})
	}
	return out
}

func cloneFlowInputEventPins(in []FlowInputEventPin) []FlowInputEventPin {
	out := make([]FlowInputEventPin, 0, len(in))
	for _, pin := range in {
		normalized := pin.normalized()
		if normalized.PinName() == "" {
			continue
		}
		out = append(out, normalized)
	}
	return out
}

func cloneFlowOutputEventPins(in []FlowOutputEventPin) []FlowOutputEventPin {
	out := make([]FlowOutputEventPin, 0, len(in))
	for _, pin := range in {
		normalized := pin.normalized()
		if normalized.PinName() == "" {
			continue
		}
		out = append(out, normalized)
	}
	return out
}

func normalizeOutputPinCarries(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, item := range in {
		out = append(out, strings.TrimSpace(item))
	}
	return out
}

func cloneFlowPackageConnects(in []FlowPackageConnect) []FlowPackageConnect {
	out := make([]FlowPackageConnect, 0, len(in))
	for _, connect := range in {
		normalized := connect.normalized()
		out = append(out, normalized)
	}
	return out
}

func cloneFlowPackageConnectMap(in map[string]FlowPackageConnectMap) map[string]FlowPackageConnectMap {
	if len(in) == 0 {
		return nil
	}
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]FlowPackageConnectMap, len(in))
	for _, key := range keys {
		normalizedKey := strings.TrimSpace(key)
		if normalizedKey == "" {
			continue
		}
		entry := in[key]
		out[normalizedKey] = FlowPackageConnectMap{
			Source: strings.TrimSpace(entry.Source),
			Target: strings.TrimSpace(entry.Target),
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
