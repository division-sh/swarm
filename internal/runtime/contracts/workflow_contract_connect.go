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
		if carry.From == "" && carry.Type == "" && !carry.Optional && carry.Convert == "" {
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
	projection := (FieldProjection{
		From: c.From, Type: c.Type, Optional: c.Optional, Convert: c.Convert,
	}).Normalized()
	return FlowInputPinCarry{
		From: projection.From, Type: projection.Type,
		Optional: projection.Optional, Convert: projection.Convert,
	}
}

func (r FlowInputPinResolution) Empty() bool {
	r = r.normalized()
	return r.Mode == FlowInputResolutionModeNone &&
		r.Aggregation == "" &&
		r.Window == "" &&
		len(r.DedupBy) == 0 &&
		r.Singleton == "" &&
		r.RepliesTo == "" &&
		r.CorrelationKey == ""
}

func (r FlowInputPinResolution) normalized() FlowInputPinResolution {
	return FlowInputPinResolution{
		Mode:           r.Mode,
		Aggregation:    strings.ToLower(strings.TrimSpace(r.Aggregation)),
		Window:         strings.TrimSpace(r.Window),
		DedupBy:        normalizeStringListPreserveOrder(r.DedupBy),
		Singleton:      strings.TrimSpace(r.Singleton),
		RepliesTo:      strings.TrimSpace(r.RepliesTo),
		CorrelationKey: strings.TrimSpace(r.CorrelationKey),
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
		Sink:    p.Sink,
		Key:     strings.TrimSpace(p.Key),
		Carries: normalizeOutputPinCarries(p.Carries),
	}
	if out.Event == "" {
		out.Event = out.Name
	}
	return out
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
		Event:      strings.TrimSpace(c.Event),
		From:       strings.TrimSpace(c.From),
		To:         strings.TrimSpace(c.To),
		Rename:     strings.TrimSpace(c.Rename),
		Adapter:    strings.TrimSpace(c.Adapter),
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
