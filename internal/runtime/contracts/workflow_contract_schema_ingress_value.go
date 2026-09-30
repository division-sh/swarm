package contracts

import (
	"fmt"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func projectSchemaIngressValue(value yamlsource.Value) (*ProjectFlowIngress, error) {
	fields, err := schemaValueFields(value, "ingress", projectFlowIngressFields, nil, true)
	if err != nil {
		return nil, err
	}
	out := &ProjectFlowIngress{}
	if err := schemaValueRequiredTexts(value, fields, map[string]*string{"alias": &out.Alias}); err != nil {
		return nil, err
	}
	providers, present := fields["providers"]
	if !present {
		return nil, nodeValueError(value, fmt.Errorf("ingress providers are required"))
	}
	items, err := schemaValueSequence(providers, true)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, item := range items {
		members, err := schemaValueFields(item, "ingress provider", projectFlowIngressProviderFields, nil, true)
		if err != nil {
			return nil, err
		}
		var row ProjectFlowIngressProvider
		if err := schemaValueRequiredTexts(item, members, map[string]*string{"provider": &row.Provider}); err != nil {
			return nil, err
		}
		if err := schemaValueTexts(members, map[string]*string{"signing_secret": &row.SigningSecret}, true); err != nil {
			return nil, err
		}
		if seen[row.Provider] {
			return nil, nodeValueError(item, fmt.Errorf("provider %q is declared more than once", row.Provider))
		}
		seen[row.Provider] = true
		if admission, present := members["admission"]; present {
			row.Admission, err = projectSchemaIngressAdmissionValue(admission)
			if err != nil {
				return nil, err
			}
		}
		if row.Admission.Authentication != nil && row.Admission.Authentication.Kind == "none" {
			if err := schemaValueForbid(members, "signing_secret"); err != nil {
				return nil, err
			}
		}
		out.Providers = append(out.Providers, row)
	}
	return out, nil
}

// Presence exclusions precede projection; catalog admission still owns the
// authentication plan, installed pack agreement, and required secrets.
func projectSchemaIngressAdmissionValue(value yamlsource.Value) (ProjectFlowIngressAdmission, error) {
	fields, err := schemaValueFields(value, "ingress admission", projectFlowIngressAdmissionFields, nil, true)
	var out ProjectFlowIngressAdmission
	if err != nil {
		return out, err
	}
	if err := schemaValueTexts(fields, map[string]*string{"kind": &out.Kind, "acknowledge": &out.Acknowledge}, true); err != nil {
		return out, err
	}
	switch out.Kind {
	case "", "pack":
		if err := schemaValueForbid(fields, "authentication", "event", "delivery_id", "payload"); err != nil {
			return out, err
		}
		if pack, present := fields["pack"]; present {
			members, err := schemaValueFields(pack, "ingress pack", projectFlowIngressAdmissionPackFields, nil, true)
			if err != nil {
				return out, err
			}
			out.Pack = &ProjectFlowIngressAdmissionPack{}
			if err := schemaValueRequiredTexts(pack, members, map[string]*string{"id": &out.Pack.ID}); err != nil {
				return out, err
			}
		}
	case "raw":
		if err := schemaValueForbid(fields, "pack"); err != nil {
			return out, err
		}
		if err := schemaValueRequiredTexts(value, fields, map[string]*string{"event": &out.Event, "payload": &out.Payload}); err != nil {
			return out, err
		}
		authentication, present := fields["authentication"]
		if !present {
			return out, nodeValueError(value, fmt.Errorf("raw authentication is required"))
		}
		members, err := schemaValueFields(authentication, "raw authentication", projectFlowIngressAuthenticationFields, nil, true)
		if err != nil {
			return out, err
		}
		out.Authentication = &ProjectFlowIngressAuthentication{}
		if err := schemaValueRequiredTexts(authentication, members, map[string]*string{"kind": &out.Authentication.Kind}); err != nil {
			return out, err
		}
		switch out.Authentication.Kind {
		case "none":
			if err := schemaValueForbid(members, "header", "prefix", "encoding"); err != nil {
				return out, err
			}
		case "token", "hmac_sha256":
			if err := schemaValueRequiredTexts(authentication, members, map[string]*string{"header": &out.Authentication.Header}); err != nil {
				return out, err
			}
			if err := schemaValueTexts(members, map[string]*string{"prefix": &out.Authentication.Prefix}, false); err != nil {
				return out, err
			}
			if out.Authentication.Kind == "token" {
				if err := schemaValueForbid(members, "encoding"); err != nil {
					return out, err
				}
			} else if err := schemaValueTexts(members, map[string]*string{"encoding": &out.Authentication.Encoding}, true); err != nil {
				return out, err
			}
		default:
			return out, nodeValueError(authentication, fmt.Errorf("authentication.kind must be none, token or hmac_sha256"))
		}
		delivery, present := fields["delivery_id"]
		if !present {
			return out, nodeValueError(value, fmt.Errorf("raw delivery_id is required"))
		}
		members, err = schemaValueFields(delivery, "raw delivery_id", projectFlowIngressDeliveryIDFields, nil, true)
		if err != nil {
			return out, err
		}
		out.DeliveryID = &ProjectFlowIngressDeliveryID{}
		if err := schemaValueRequiredTexts(delivery, members, map[string]*string{"source": &out.DeliveryID.Source}); err != nil {
			return out, err
		}
		switch out.DeliveryID.Source {
		case "header":
			if err := schemaValueForbid(members, "json_path"); err != nil {
				return out, err
			}
			if err := schemaValueRequiredTexts(delivery, members, map[string]*string{"header": &out.DeliveryID.Header}); err != nil {
				return out, err
			}
		case "json_path":
			if err := schemaValueForbid(members, "header"); err != nil {
				return out, err
			}
			if err := schemaValueRequiredTexts(delivery, members, map[string]*string{"json_path": &out.DeliveryID.JSONPath}); err != nil {
				return out, err
			}
		case "body_sha256":
			if err := schemaValueForbid(members, "header", "json_path"); err != nil {
				return out, err
			}
		default:
			return out, nodeValueError(delivery, fmt.Errorf("delivery_id.source must be header, json_path or body_sha256"))
		}
	default:
		return out, nodeValueError(value, fmt.Errorf("admission.kind must be pack or raw"))
	}
	return out, nil
}

func schemaValueForbid(fields map[string]yamlsource.Value, names ...string) error {
	for _, name := range names {
		if value, present := fields[name]; present {
			return nodeValueError(value, fmt.Errorf("%s is forbidden in this branch, including empty or null presence", name))
		}
	}
	return nil
}
