#!/usr/bin/env python3
"""
Swarm Platform Specification Verifier
Reference implementation for platform boot_verification checks.
Uses canonical check IDs from platform-spec.yaml §engine.boot_verification.

This script implements the subset that can run without the Go runtime
(no CEL parsing, no MCP connectivity, no credential store).

Usage: python3 verify.py [contracts_dir]
"""
import yaml, os, sys, re
from collections import defaultdict

BASE = os.path.dirname(os.path.abspath(__file__))

# Resolve contracts directory
if len(sys.argv) > 1:
    EC = os.path.abspath(sys.argv[1])
else:
    candidates = [
        os.path.join(BASE, 'contracts'),
        os.path.join(BASE, 'empire', 'contracts'),  # legacy fallback
    ]
    EC = next((c for c in candidates if os.path.exists(os.path.join(c, 'package.yaml'))), None)
    if EC is None:
        print("ERROR: No contracts directory found. Pass path as argument: python3 verify.py <contracts_dir>")
        sys.exit(1)

if not os.path.exists(os.path.join(EC, 'package.yaml')):
    print("ERROR: No package.yaml found in %s" % EC)
    sys.exit(1)

# ============================================================
# Findings registry — canonical check IDs from spec
# ============================================================
findings = []

def finding(check_id, severity, message, location=""):
    findings.append({
        'check_id': check_id,
        'severity': severity,
        'message': message,
        'location': location,
    })

def error(check_id, msg, loc=""): finding(check_id, 'error', msg, loc)
def warn(check_id, msg, loc=""): finding(check_id, 'warning', msg, loc)

def load(path):
    with open(path) as f:
        return yaml.safe_load(f) or {}

def load_if_exists(path):
    return load(path) if os.path.exists(path) else {}

# ============================================================
# Load contracts — discover flows from package.yaml
# ============================================================
root_package = load(os.path.join(EC, 'package.yaml'))
FLOWS = [f['flow'] for f in root_package.get('flows', []) if isinstance(f, dict) and 'flow' in f]

root_agents = load_if_exists(os.path.join(EC, 'agents.yaml'))
root_nodes = load_if_exists(os.path.join(EC, 'nodes.yaml'))
root_events = load_if_exists(os.path.join(EC, 'events.yaml'))
root_tools = load_if_exists(os.path.join(EC, 'tools.yaml'))
root_policy = load_if_exists(os.path.join(EC, 'policy.yaml'))

flow_data = {}
for flow in FLOWS:
    fd = os.path.join(EC, 'flows', flow)
    if not os.path.isdir(fd):
        warn("flow_coherence", "Flow '%s' declared in package.yaml but directory not found" % flow, flow)
        continue
    flow_data[flow] = {
        'agents': load_if_exists(os.path.join(fd, 'agents.yaml')),
        'nodes': load_if_exists(os.path.join(fd, 'nodes.yaml')),
        'events': load_if_exists(os.path.join(fd, 'events.yaml')),
        'schema': load_if_exists(os.path.join(fd, 'schema.yaml')),
        'package': load_if_exists(os.path.join(fd, 'package.yaml')),
        'tools': load_if_exists(os.path.join(fd, 'tools.yaml')),
        'policy': load_if_exists(os.path.join(fd, 'policy.yaml')),
    }

# Merge all
all_agents, all_nodes, all_events, all_tools, all_policy = {}, {}, {}, {}, {}
all_agents.update(root_agents)
all_nodes.update(root_nodes)
all_events.update(root_events)
all_tools.update(root_tools)
all_policy.update(root_policy)
for flow in FLOWS:
    if flow not in flow_data: continue
    all_agents.update(flow_data[flow]['agents'])
    all_nodes.update(flow_data[flow]['nodes'])
    all_events.update(flow_data[flow]['events'])
    all_tools.update(flow_data[flow]['tools'])
    all_policy.update(flow_data[flow]['policy'])

events_defined = set(k for k, v in all_events.items() if isinstance(v, dict))

RUNTIME_BUILTIN_TOOLS = {
    'agent_message',
    'schedule',
    'agent_hire',
    'agent_fire',
    'agent_reconfigure',
    'get_entity',
    'save_entity_field',
    'create_entity',
    'query_entities',
    'search_entities',
    'query_metrics',
    'mailbox_send',
    'human_task_request',
    'human_task_decide',
    'read_flow_data',
}

# ============================================================
# Helpers
# ============================================================
def flatten_payload(payload, prefix='', skip_root_swarm=False):
    fields = set()
    if not isinstance(payload, dict): return fields
    for k, v in payload.items():
        if k.startswith('_') or (skip_root_swarm and not prefix and k == 'swarm'): continue
        full = (prefix + '.' + k) if prefix else k
        fields.add(full)
        if isinstance(v, dict) and not any(t in str(v) for t in ['string','integer','number','boolean','array','object','text','timestamp','uuid','numeric']):
            fields.update(flatten_payload(v, full))
    return fields

def event_payload_fields(event_schema):
    if not isinstance(event_schema, dict): return set()
    payload = event_schema.get('payload', {})
    if isinstance(payload, dict) and payload:
        return flatten_payload(payload)
    flat = {}
    metadata_keys = {
        'description', 'emitter', 'emitter_type', 'producer', 'alternate_emitters',
        'consumer', 'consumer_type', 'intercepted', 'passthrough', 'runtime_handling',
        'owning_node', 'delivery_channel', 'required',
    }
    for key, value in event_schema.items():
        if not key or str(key).startswith('_') or key in metadata_keys:
            continue
        flat[key] = value
    return flatten_payload(flat)

def payload_field_exists(fields, ref):
    ref = str(ref or '').strip()
    if not ref: return False
    for candidate in fields:
        if ref == candidate or ref.startswith(candidate + '.') or candidate.startswith(ref + '.'):
            return True
    return False

def declared_mcp_prefixes():
    prefixes = set()
    policy = all_policy.get('mcp_servers', {})
    if not isinstance(policy, dict): return prefixes
    for server in policy.values():
        if not isinstance(server, dict): continue
        prefix = str(server.get('prefix') or '').strip()
        if prefix:
            prefixes.add(prefix)
    return prefixes

def tool_allowed_by_mcp_prefix(tool, prefixes):
    tool = str(tool or '').strip()
    if '.' not in tool: return False
    prefix = tool.split('.', 1)[0].strip()
    return bool(prefix and prefix in prefixes)

def extract_payload_refs(s):
    return re.findall(r'payload\.([a-zA-Z_][a-zA-Z0-9_.]*)', str(s))

def extract_policy_refs(s):
    return re.findall(r'policy\.([a-zA-Z_][a-zA-Z0-9_.]*)', str(s))

def metadata_value_startswith(value, prefixes):
    if isinstance(value, list):
        return any(metadata_value_startswith(item, prefixes) for item in value)
    return str(value).strip().startswith(prefixes)

def metadata_value_present(value):
    if isinstance(value, list):
        return any(metadata_value_present(item) for item in value)
    return str(value or '').strip() != ''

def event_swarm_metadata(ev):
    swarm = ev.get('swarm', {}) if isinstance(ev, dict) else {}
    return swarm if isinstance(swarm, dict) else {}

def event_planned(ev, swarm):
    return swarm.get('status') == 'planned' or ev.get('_status') == 'planned'

def suppresses_event_consumer_warning(ev_name):
    ev = all_events.get(ev_name, {})
    if isinstance(ev, dict):
        swarm = event_swarm_metadata(ev)
        if metadata_value_present(swarm.get('consumer', '')): return True
        if metadata_value_present(ev.get('consumer', '')): return True
        if metadata_value_present(ev.get('_consumer', '')): return True
        if event_planned(ev, swarm): return True
    return False

def suppresses_event_producer_warning(ev_name):
    ev = all_events.get(ev_name, {})
    if isinstance(ev, dict):
        swarm = event_swarm_metadata(ev)
        if metadata_value_startswith(swarm.get('source', ''), ('external', 'platform')): return True
        if metadata_value_startswith(ev.get('_source', ''), ('external', 'platform')): return True
        if metadata_value_present(swarm.get('producer', '')): return True
        if metadata_value_present(ev.get('producer', '')): return True
        if metadata_value_present(ev.get('_producer', '')): return True
        if event_planned(ev, swarm): return True
    return False

def emit_event_name(spec):
    if isinstance(spec, str):
        return spec
    if isinstance(spec, dict):
        ev = spec.get('event')
        if isinstance(ev, str):
            return ev
    return ""

def collect_handler_emits(h):
    """Collect all events emitted by a handler."""
    emitted = set()
    if not isinstance(h, dict): return emitted
    e = emit_event_name(h.get('emit'))
    if e:
        emitted.add(e)
    fo = h.get('fan_out', {})
    if isinstance(fo, dict):
        e = emit_event_name(fo.get('emit'))
        if e:
            emitted.add(e)
    rules = h.get('rules', {})
    if isinstance(rules, dict):
        for r in rules.values():
            if isinstance(r, dict):
                e = emit_event_name(r.get('emit'))
                if e:
                    emitted.add(e)
                fo = r.get('fan_out', {})
                if isinstance(fo, dict):
                    e = emit_event_name(fo.get('emit'))
                    if e:
                        emitted.add(e)
    oc = h.get('on_complete')
    if isinstance(oc, (dict, list)):
        items = oc.values() if isinstance(oc, dict) else oc
        for b in items:
            if isinstance(b, dict):
                e = emit_event_name(b.get('emit'))
                if e:
                    emitted.add(e)
                fo = b.get('fan_out', {})
                if isinstance(fo, dict):
                    e = emit_event_name(fo.get('emit'))
                    if e:
                        emitted.add(e)
    acc = h.get('accumulate', {})
    if isinstance(acc, dict):
        ot = acc.get('on_timeout')
        if isinstance(ot, dict):
            e = emit_event_name(ot.get('emit'))
            if e:
                emitted.add(e)
            fo = ot.get('fan_out', {})
            if isinstance(fo, dict):
                e = emit_event_name(fo.get('emit'))
                if e:
                    emitted.add(e)
        oc = acc.get('on_complete')
        if isinstance(oc, (dict, list)):
            items = oc.values() if isinstance(oc, dict) else oc
            for b in items:
                if isinstance(b, dict):
                    e = emit_event_name(b.get('emit'))
                    if e:
                        emitted.add(e)
                    fo = b.get('fan_out', {})
                    if isinstance(fo, dict):
                        e = emit_event_name(fo.get('emit'))
                        if e:
                            emitted.add(e)
    return emitted

def iter_flows_with_nodes():
    """Yield (flow_name, nodes_dict) for root + all child flows."""
    yield ('root', root_nodes)
    for flow in FLOWS:
        if flow in flow_data:
            yield (flow, flow_data[flow]['nodes'])

def iter_flows_with_agents():
    """Yield (flow_name, agents_dict) for root + all child flows."""
    yield ('root', root_agents)
    for flow in FLOWS:
        if flow in flow_data:
            yield (flow, flow_data[flow]['agents'])

def get_all_handler_conditions(h):
    """Extract all CEL condition strings from a handler."""
    conds = []
    guard = h.get('guard', {})
    if isinstance(guard, dict):
        checks = guard.get('checks', [])
        if not checks and 'check' in guard: checks = [guard]
        conds.extend(c.get('check', '') for c in checks if isinstance(c, dict))
    rules = h.get('rules', {})
    if isinstance(rules, dict):
        conds.extend(r.get('condition', '') for r in rules.values() if isinstance(r, dict))
    oc = h.get('on_complete')
    if isinstance(oc, (dict, list)):
        items = oc.values() if isinstance(oc, dict) else oc
        conds.extend(b.get('condition', '') for b in items if isinstance(b, dict))
    return [c for c in conds if c]

def iter_rule_entries(rules):
    """Yield (rule_name, rule) for supported rule encodings."""
    if isinstance(rules, dict):
        for rule_name, rule in rules.items():
            if isinstance(rule, dict):
                yield rule_name, rule
    elif isinstance(rules, list):
        for idx, rule in enumerate(rules):
            if isinstance(rule, dict):
                rule_name = rule.get('id') or str(idx)
                yield rule_name, rule

def value_present(value):
    if value is None:
        return False
    if isinstance(value, (dict, list)):
        return len(value) > 0
    return str(value).strip() != ''

def normalized_handler_action_id(raw):
    return str(raw or '').strip().lower()

def declared_flow_mode(flow_id):
    for entry in root_package.get('flows', []):
        if not isinstance(entry, dict):
            continue
        if entry.get('flow') == flow_id or entry.get('id') == flow_id:
            return str(entry.get('mode', '')).strip()
    return ""

def flow_is_template(flow_id):
    return declared_flow_mode(flow_id).lower() == 'template'

def handler_action_parts(handler, flow, node_id, event_type):
    action = handler.get('action')
    loc = "%s/%s/%s" % (flow, node_id, event_type)
    if action is None or action == "":
        return "", {}
    if isinstance(action, str):
        return normalized_handler_action_id(action), {}
    if isinstance(action, dict):
        allowed = {'id', 'template', 'instance_id_from', 'config_from', 'mailbox', 'artifact_repo'}
        for field in action.keys():
            if field in {'type', 'flow_template', 'instance_id'}:
                error("handler_field_compliance", "deprecated action field '%s' is not supported" % field, loc)
            elif field not in allowed:
                error("handler_field_compliance", "action field '%s' not in platform spec" % field, loc)
        action_id = normalized_handler_action_id(action.get('id'))
        if not action_id:
            error("handler_field_compliance", "action mapping missing id", loc)
        return action_id, action
    error("handler_field_compliance", "action declaration must be a string or mapping", loc)
    return "", {}

def handler_action_value(handler, action_map, field):
    if isinstance(action_map, dict) and field in action_map:
        return action_map.get(field)
    return handler.get(field)

# ============================================================
# Collect global emitter/subscriber sets
# ============================================================
events_emitted = set()
events_subscribed = set()

for flow in FLOWS:
    if flow not in flow_data: continue
    ae = flow_data[flow]['schema'].get('auto_emit_on_create', {})
    if isinstance(ae, dict) and 'event' in ae:
        events_emitted.add(ae['event'])

for nid, node in all_nodes.items():
    if not isinstance(node, dict): continue
    events_emitted.update(node.get('produces', []))
    for ev, h in node.get('event_handlers', {}).items():
        events_emitted.update(collect_handler_emits(h))

for aid, agent in all_agents.items():
    if not isinstance(agent, dict): continue
    events_emitted.update(agent.get('emit_events', []))

for nid, node in all_nodes.items():
    if not isinstance(node, dict): continue
    for ev in node.get('subscribes_to', []):
        ev_clean = str(ev).split('/')[-1] if '/' in str(ev) else str(ev)
        if '*' not in ev_clean: events_subscribed.add(ev_clean)
    events_subscribed.update(node.get('event_handlers', {}).keys())

for aid, agent in all_agents.items():
    if not isinstance(agent, dict): continue
    for ev in agent.get('subscriptions', []) + agent.get('subscribes_to', []):
        ev_clean = str(ev).split('/')[-1] if '/' in str(ev) else str(ev)
        if '*' not in ev_clean: events_subscribed.add(ev_clean)

fan_out_events = set()
for nid, node in all_nodes.items():
    if not isinstance(node, dict): continue
    for ev, h in node.get('event_handlers', {}).items():
        if not isinstance(h, dict):
            continue
        fo = h.get('fan_out', {})
        if isinstance(fo, dict):
            e = emit_event_name(fo.get('emit'))
            if e:
                fan_out_events.add(e)
        for block_name in ("rules", "on_complete"):
            block = h.get(block_name, {})
            items = block.values() if isinstance(block, dict) else (block if isinstance(block, list) else [])
            for item in items:
                if not isinstance(item, dict):
                    continue
                fo = item.get('fan_out', {})
                if isinstance(fo, dict):
                    e = emit_event_name(fo.get('emit'))
                    if e:
                        fan_out_events.add(e)
        acc = h.get('accumulate', {})
        if isinstance(acc, dict):
            ot = acc.get('on_timeout')
            if isinstance(ot, dict):
                fo = ot.get('fan_out', {})
                if isinstance(fo, dict):
                    e = emit_event_name(fo.get('emit'))
                    if e:
                        fan_out_events.add(e)

# ============================================================
# CHECK: event_chain_integrity [warning, per-flow]
# Events emitted without schema
# ============================================================
for ev in events_emitted:
    if ev and ev not in events_defined and not ev.startswith('timer.') and not ev.startswith('*.'):
        warn("event_chain_integrity", "'%s' emitted but no schema in events.yaml" % ev)

# ============================================================
# CHECK: event_consumer_exists [warning, per-flow]
# ============================================================
for ev in events_emitted:
    if ev and ev in events_defined and ev not in events_subscribed and not ev.startswith('platform.'):
        if not suppresses_event_consumer_warning(ev):
            warn("event_consumer_exists", "'%s' emitted but nobody subscribes" % ev)

# ============================================================
# CHECK: event_producer_exists [warning, per-flow]
# ============================================================
for ev in events_subscribed:
    if ev and ev in events_defined and ev not in events_emitted:
        if not suppresses_event_producer_warning(ev) and ev not in fan_out_events and not ev.startswith('timer.') and not ev.startswith('platform.'):
            warn("event_producer_exists", "'%s' subscribed but nobody emits" % ev)

# ============================================================
# CHECK: payload_field_coverage [error, per-flow]
# ============================================================
for flow, nodes in iter_flows_with_nodes():
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        for ev, h in node.get('event_handlers', {}).items():
            if not isinstance(h, dict): continue
            da = h.get('data_accumulation')
            if not isinstance(da, dict): continue
            source_ev = da.get('source_event', ev)
            if source_ev not in all_events: continue
            event_schema = all_events[source_ev]
            payload_fields = event_payload_fields(event_schema)
            for w in da.get('writes', []):
                if isinstance(w, str):
                    if not payload_field_exists(payload_fields, w):
                        error("payload_field_coverage", "writes '%s' but %s payload has %s" % (w, source_ev, sorted(payload_fields)), "%s/%s/%s" % (flow, nid, ev))
                elif isinstance(w, dict):
                    sf = w.get('source_field', '')
                    if sf and not payload_field_exists(payload_fields, sf):
                        error("payload_field_coverage", "source_field '%s' not in %s payload" % (sf, source_ev), "%s/%s/%s" % (flow, nid, ev))

# ============================================================
# CHECK: condition_payload_alignment [error, per-flow]
# ============================================================
for flow, nodes in iter_flows_with_nodes():
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        for ev, h in node.get('event_handlers', {}).items():
            if not isinstance(h, dict): continue
            if ev not in all_events: continue
            event_payload = all_events[ev]
            payload_fields = event_payload_fields(event_payload)
            loc = "%s/%s/%s" % (flow, nid, ev)

            for rule_name, rule in iter_rule_entries(h.get('rules', {})):
                cond = rule.get('condition', '')
                if cond == 'else': continue
                for ref in extract_payload_refs(cond):
                    if not payload_field_exists(payload_fields, ref):
                        error("condition_payload_alignment", "rule '%s': payload.%s not in event payload" % (rule_name, ref), loc)

            guard = h.get('guard', {})
            if isinstance(guard, dict):
                checks = guard.get('checks', [])
                if not checks and 'check' in guard: checks = [guard]
                for check in checks:
                    if not isinstance(check, dict): continue
                    for ref in extract_payload_refs(check.get('check', '')):
                        if not payload_field_exists(payload_fields, ref):
                            error("condition_payload_alignment", "guard: payload.%s not in event payload" % ref, loc)

            oc = h.get('on_complete')
            if isinstance(oc, (dict, list)):
                items = oc.values() if isinstance(oc, dict) else oc
                for branch in items:
                    if isinstance(branch, dict):
                        for ref in extract_payload_refs(branch.get('condition', '')):
                            if not payload_field_exists(payload_fields, ref):
                                error("condition_payload_alignment", "on_complete: payload.%s not in event payload" % ref, loc)

# ============================================================
# CHECK: condition_policy_alignment [warning, per-flow]
# ============================================================
for flow, nodes in iter_flows_with_nodes():
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        for ev, h in node.get('event_handlers', {}).items():
            if not isinstance(h, dict): continue
            for cond in get_all_handler_conditions(h):
                for ref in extract_policy_refs(cond):
                    if ref not in all_policy:
                        warn("condition_policy_alignment", "policy.%s referenced but not in any policy.yaml" % ref, "%s/%s/%s" % (flow, nid, ev))

# ============================================================
# CHECK: state_machine_coherence [error, per-flow]
# ============================================================
for flow in FLOWS:
    if flow not in flow_data: continue
    schema = flow_data[flow]['schema']
    declared_states = set(schema.get('states', []))
    initial = schema.get('initial_state')
    terminals = set(schema.get('terminal_states', []))

    if initial and initial not in declared_states and declared_states:
        error("state_machine_coherence", "initial_state '%s' not in declared states" % initial, flow)
    for t in terminals:
        if t not in declared_states and declared_states:
            error("state_machine_coherence", "terminal_state '%s' not in declared states" % t, flow)

    nodes = flow_data[flow]['nodes']
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        for ev, h in node.get('event_handlers', {}).items():
            if not isinstance(h, dict): continue
            target = h.get('advances_to')
            if target and declared_states and target not in declared_states:
                error("state_machine_coherence", "advances_to '%s' not in declared states" % target, "%s/%s/%s" % (flow, nid, ev))
            oc = h.get('on_complete')
            if isinstance(oc, (dict, list)):
                items = oc.values() if isinstance(oc, dict) else oc
                for branch in items:
                    if isinstance(branch, dict):
                        bt = branch.get('advances_to')
                        if bt and declared_states and bt not in declared_states:
                            error("state_machine_coherence", "on_complete advances_to '%s' not in declared states" % bt, "%s/%s/%s" % (flow, nid, ev))

# ============================================================
# CHECK: required_agents_match [error, per-flow]
# ============================================================
for flow in FLOWS:
    if flow not in flow_data: continue
    schema = flow_data[flow]['schema']
    agents = flow_data[flow]['agents']
    for ra in schema.get('required_agents', []):
        role = ra.get('role', '')
        agent = agents.get(role)
        if not isinstance(agent, dict):
            error("required_agents_match", "required role '%s' not in agents.yaml" % role, flow)
            continue
        schema_emits = set(ra.get('emits', []))
        agent_emits = set(agent.get('emit_events', []))
        diff = schema_emits - agent_emits
        if diff:
            error("required_agents_match", "role '%s' schema says emits %s but agent doesn't" % (role, diff), flow)

# ============================================================
# CHECK: handler_field_compliance [error, per-node]
# ============================================================
DEFINED_HANDLER_FIELDS = {
    'description', '_note', 'guard', 'accumulate', 'compute', 'completion_rule',
    'policy_ref', 'on_complete', 'advances_to', 'sets_gate',
    'data_accumulation', 'emit', 'rules', 'fan_out', 'query', 'group_by',
    'reduce', 'filter', 'count', 'clear', 'action', 'select_entity',
    'select_or_create_entity',
    'template', 'instance_id_from', 'config_from',
    'clear_gates', 'evidence_target', 'create_entity', 'from', 'branch',
    'dedup_by',
}
DEPRECATED_HANDLER_FIELDS = {
    'condition', 'logic', 'on_below_threshold', 'on_dedup', 'on_pass',
}
RETIRED_HANDLER_FIELDS = {
    'emits': 'use emit: <event> or emit: {event, fields}',
    'payload_transform': 'move payload ownership into emit.fields at the active emit site',
}
SUPPORTED_HANDLER_ACTIONS = {
    'create_flow_instance', 'record_evidence', 'mailbox_write', 'artifact_repo_commit',
}
for flow, nodes in iter_flows_with_nodes():
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        for ev, h in node.get('event_handlers', {}).items():
            if not isinstance(h, dict): continue
            loc = "%s/%s/%s" % (flow, nid, ev)
            for field in h.keys():
                if field in RETIRED_HANDLER_FIELDS:
                    error("handler_field_compliance", "handler field '%s' is retired; %s" % (field, RETIRED_HANDLER_FIELDS[field]), loc)
                elif field in DEPRECATED_HANDLER_FIELDS:
                    error("handler_field_compliance", "handler uses deprecated field '%s'" % field, loc)
                elif field not in DEFINED_HANDLER_FIELDS:
                    error("handler_field_compliance", "handler field '%s' not in platform spec" % field, loc)
            action_id, action_map = handler_action_parts(h, flow, nid, ev)
            if not action_id:
                continue
            if action_id not in SUPPORTED_HANDLER_ACTIONS:
                error("handler_field_compliance", "unsupported handler action '%s'" % action_id, loc)
                continue
            if action_id == 'create_flow_instance':
                template = str(handler_action_value(h, action_map, 'template') or '').strip()
                if not template:
                    error("handler_field_compliance", "create_flow_instance is missing template", loc)
                elif not flow_is_template(template):
                    error("handler_field_compliance", "create_flow_instance template %s is not mode: template" % template, loc)
                if not str(handler_action_value(h, action_map, 'instance_id_from') or '').strip():
                    error("handler_field_compliance", "create_flow_instance is missing instance_id_from", loc)
                config_from = handler_action_value(h, action_map, 'config_from')
                if not isinstance(config_from, dict) or not config_from:
                    error("handler_field_compliance", "create_flow_instance is missing config_from", loc)
            if action_id == 'record_evidence' and not str(h.get('evidence_target') or '').strip():
                error("handler_field_compliance", "record_evidence is missing evidence_target", loc)
            mailbox = action_map.get('mailbox') if isinstance(action_map, dict) else None
            if action_id == 'mailbox_write':
                if not isinstance(mailbox, dict):
                    error("handler_field_compliance", "mailbox_write is missing mailbox", loc)
                else:
                    if not value_present(mailbox.get('item_type')):
                        error("handler_field_compliance", "mailbox_write is missing mailbox.item_type", loc)
                    if not value_present(mailbox.get('summary')):
                        error("handler_field_compliance", "mailbox_write is missing mailbox.summary", loc)
            elif mailbox is not None:
                error("handler_field_compliance", "mailbox declaration requires action mailbox_write", loc)
            artifact_repo = action_map.get('artifact_repo') if isinstance(action_map, dict) else None
            if action_id == 'artifact_repo_commit':
                if not isinstance(artifact_repo, dict):
                    error("handler_field_compliance", "artifact_repo_commit is missing artifact_repo", loc)
                else:
                    if str(artifact_repo.get('provider') or '').strip() != 'local_git':
                        error("handler_field_compliance", "artifact_repo_commit provider %s is unsupported" % str(artifact_repo.get('provider') or '').strip(), loc)
                    for required in ('repo_id', 'request_id'):
                        if not value_present(artifact_repo.get(required)):
                            error("handler_field_compliance", "artifact_repo_commit is missing artifact_repo.%s" % required, loc)
                    if not isinstance(artifact_repo.get('allowed_paths'), list) or not artifact_repo.get('allowed_paths'):
                        error("handler_field_compliance", "artifact_repo_commit requires at least one artifact_repo.allowed_paths entry", loc)
                    if not isinstance(artifact_repo.get('files'), list) or not artifact_repo.get('files'):
                        error("handler_field_compliance", "artifact_repo_commit requires at least one artifact_repo.files entry", loc)
                    output = artifact_repo.get('output')
                    if not isinstance(output, dict):
                        output = {}
                    for required in ('repo_url', 'current_ref', 'file_manifest', 'status', 'failure_reason', 'last_request_id', 'last_source_event_id'):
                        if not str(output.get(required) or '').strip():
                            error("handler_field_compliance", "artifact_repo_commit is missing artifact_repo.output.%s" % required, loc)
            elif artifact_repo is not None:
                error("handler_field_compliance", "artifact_repo declaration requires action artifact_repo_commit", loc)

# ============================================================
# CHECK: tool_resolution [warning, per-agent]
# ============================================================
runtime_tool_names = set(all_tools.keys()) | RUNTIME_BUILTIN_TOOLS
mcp_prefixes = declared_mcp_prefixes()
for flow, agents in iter_flows_with_agents():
    for aid, agent in agents.items():
        if not isinstance(agent, dict): continue
        for tool in agent.get('tools', agent.get('tools_tier2', [])):
            tool_name = str(tool or '').strip()
            if not tool_name: continue
            if tool_name not in runtime_tool_names and not tool_allowed_by_mcp_prefix(tool_name, mcp_prefixes):
                warn("tool_resolution", "tool '%s' not in the structural runtime tool registry subset" % tool_name, "%s/%s" % (flow, aid))

# ============================================================
# CHECK: prompt_exists [warning, per-agent]
# ============================================================
deferred_count = 0
for flow, agents in iter_flows_with_agents():
    prompt_dir = os.path.join(EC, 'prompts') if flow == 'root' else os.path.join(EC, 'flows', flow, 'prompts')
    for aid in agents:
        if not isinstance(agents[aid], dict): continue
        pp = os.path.join(prompt_dir, '%s.md' % aid)
        if not os.path.exists(pp):
            warn("prompt_exists", "no prompt file at prompts/%s.md" % aid, "%s/%s" % (flow, aid))
        else:
            with open(pp) as f:
                content = f.read()
            if '<!-- DEFERRED' in content:
                deferred_count += 1

# ============================================================
# CHECK: produces_drift [warning, per-node]
# ============================================================
for flow, nodes in iter_flows_with_nodes():
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        declared_produces = set(node.get('produces', []))
        actual_emits = set()
        for ev, h in node.get('event_handlers', {}).items():
            actual_emits.update(collect_handler_emits(h))
        drift = actual_emits - declared_produces
        if drift:
            for e in drift:
                warn("produces_drift", "emits '%s' but not in produces list" % e, "%s/%s" % (flow, nid))

# ============================================================
# CHECK: invalid_field_detection [error, per-node]
# ============================================================
DEFINED_NODE_FIELDS = {
    'id', 'execution_type', 'description', 'subscribes_to', 'event_handlers',
    'state_schema', 'state_table', 'timers', 'gate_state', 'permissions',
    'produces', '_produces_note', '_note',
}
for flow, nodes in iter_flows_with_nodes():
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        for field in node.keys():
            if field not in DEFINED_NODE_FIELDS:
                error("invalid_field_detection", "node field '%s' not in platform spec" % field, "%s/%s" % (flow, nid))

# ============================================================
# CHECK: policy_conflict_detection [warning, per-flow]
# ============================================================
for flow in FLOWS:
    if flow not in flow_data: continue
    fp = flow_data[flow]['policy']
    for key, fval in fp.items():
        if key.startswith('_'): continue
        if key in root_policy and not isinstance(fval, dict) and not isinstance(root_policy[key], dict):
            if fval != root_policy[key]:
                warn("policy_conflict_detection", "'%s': root=%s, %s=%s" % (key, root_policy[key], flow, fval), flow)

# ============================================================
# CHECK: event_cycle_detection [error, per-flow]
# ============================================================
node_emit_graph = {}
for flow, nodes in iter_flows_with_nodes():
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        for ev, h in node.get('event_handlers', {}).items():
            emitted = collect_handler_emits(h)
            if emitted:
                node_emit_graph[ev] = node_emit_graph.get(ev, set()) | emitted

def find_cycles(graph):
    cycles = []
    for start in graph:
        visited = set()
        stack = [(start, [start])]
        while stack:
            current, path = stack.pop()
            if current in graph:
                for nxt in graph[current]:
                    if nxt == start and len(path) > 1:
                        cycles.append(path + [nxt])
                    elif nxt not in visited and nxt in graph:
                        visited.add(nxt)
                        stack.append((nxt, path + [nxt]))
    return cycles

for cycle in find_cycles(node_emit_graph):
    error("event_cycle_detection", "handler emit cycle: %s" % " -> ".join(cycle))

# ============================================================
# CHECK: dialect_compliance [error, per-node]
# ============================================================
for flow, nodes in iter_flows_with_nodes():
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        for ev, h in node.get('event_handlers', {}).items():
            if not isinstance(h, dict): continue
            loc = "%s/%s/%s" % (flow, nid, ev)

            g = h.get('guard')
            if isinstance(g, str):
                error("dialect_compliance", "guard is string, must be {id, check}" , loc)
            elif isinstance(g, dict) and 'check' not in g and 'checks' not in g:
                error("dialect_compliance", "guard missing check/checks field", loc)

            if 'on_complete' in h and 'rules' in h:
                error("dialect_compliance", "has both on_complete AND rules (mutually exclusive)", loc)

            if isinstance(h.get('on_complete'), dict):
                error("dialect_compliance", "on_complete is dict (unordered), must be list (ordered)", loc)

            for block_name in ['rules']:
                block = h.get(block_name, {})
                if isinstance(block, dict):
                    for rn, r in block.items():
                        if isinstance(r, dict):
                            c = r.get('condition', '')
                            if c and c != 'else' and not any(c.startswith(p) for p in ['payload.','entity.','policy.','accumulated.','fan_out.']):
                                error("dialect_compliance", "rule '%s' condition '%s' missing context prefix" % (rn, c), loc)

            if isinstance(h.get('advances_to'), list):
                error("dialect_compliance", "advances_to is list, must be string", loc)

            if ev in collect_handler_emits(h):
                error("dialect_compliance", "emits own trigger event '%s' (self-emit)" % ev, loc)

# ============================================================
# CHECK: single_node_per_event [error, global]
# ============================================================
event_node_map = {}
for flow, nodes in iter_flows_with_nodes():
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        for ev in node.get('event_handlers', {}):
            if ev in event_node_map:
                prev_nid, prev_flow = event_node_map[ev]
                error("single_node_per_event", "'%s' handled by both %s/%s and %s/%s" % (ev, prev_flow, prev_nid, flow, nid))
            else:
                event_node_map[ev] = (nid, flow)

# ============================================================
# CHECK: config_from_payload_alignment [error, per-node]
# ============================================================
for flow, nodes in iter_flows_with_nodes():
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        for ev, h in node.get('event_handlers', {}).items():
            if not isinstance(h, dict): continue
            cf = h.get('config_from', {})
            if not isinstance(cf, dict): continue
            event_payload = all_events.get(ev, {})
            payload_fields = set()
            if isinstance(event_payload, dict) and 'payload' in event_payload:
                p = event_payload['payload']
                if isinstance(p, dict): payload_fields = set(p.keys())
            for key, path in cf.items():
                if isinstance(path, str) and path.startswith('payload.'):
                    field = path.split('.', 1)[1]
                    if payload_fields and field not in payload_fields:
                        error("config_from_payload_alignment", "config_from reads payload.%s but event has %s" % (field, sorted(payload_fields)), "%s/%s/%s" % (flow, nid, ev))

# ============================================================
# CHECK: phantom_produces [warning, per-node]
# ============================================================
for flow, nodes in iter_flows_with_nodes():
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        declared_produces = set(node.get('produces', []))
        actual_emits = set()
        for ev, h in node.get('event_handlers', {}).items():
            actual_emits.update(collect_handler_emits(h))
        phantom = declared_produces - actual_emits
        if phantom:
            for p in phantom:
                warn("phantom_produces", "produces '%s' but no handler emits it" % p, "%s/%s" % (flow, nid))

# ============================================================
# CHECK: native_tools_valid [error, per-agent]
# ============================================================
VALID_NATIVE_CAPABILITIES = {'bash', 'web_search', 'file_io'}
for flow, agents in iter_flows_with_agents():
    for aid, agent in agents.items():
        if not isinstance(agent, dict): continue
        nt = agent.get('native_tools', {})
        if not isinstance(nt, dict): continue
        for cap, val in nt.items():
            if cap not in VALID_NATIVE_CAPABILITIES:
                error("native_tools_valid", "unknown native capability '%s'" % cap, "%s/%s" % (flow, aid))
            if not isinstance(val, bool):
                error("native_tools_valid", "native_tools.%s must be boolean, got %s" % (cap, type(val).__name__), "%s/%s" % (flow, aid))

# ============================================================
# CHECK: platform_namespace_violation [error, per-flow]
# ============================================================
for flow in FLOWS:
    if flow not in flow_data: continue
    # Check events.yaml
    for ev_name in flow_data[flow]['events']:
        if isinstance(ev_name, str) and ev_name.startswith('platform.'):
            error("platform_namespace_violation", "product event '%s' uses reserved platform.* prefix" % ev_name, flow)
    # Check agent emit_events
    for aid, agent in flow_data[flow]['agents'].items():
        if not isinstance(agent, dict): continue
        for ev in agent.get('emit_events', []):
            if isinstance(ev, str) and ev.startswith('platform.'):
                error("platform_namespace_violation", "agent '%s' emit_events uses reserved platform.* prefix: '%s'" % (aid, ev), flow)

# Also check root level
for ev_name in root_events:
    if isinstance(ev_name, str) and ev_name.startswith('platform.'):
        error("platform_namespace_violation", "product event '%s' uses reserved platform.* prefix" % ev_name, "root")
for aid, agent in root_agents.items():
    if not isinstance(agent, dict): continue
    for ev in agent.get('emit_events', []):
        if isinstance(ev, str) and ev.startswith('platform.'):
            error("platform_namespace_violation", "agent '%s' emit_events uses reserved platform.* prefix: '%s'" % (aid, ev), "root")

# ============================================================
# CHECK: workspace_class_exists [error, per-agent]
# ============================================================
workspace_classes = set(all_policy.get('workspace_classes', {}).keys()) if isinstance(all_policy.get('workspace_classes'), dict) else set()
if workspace_classes:
    for flow, agents in iter_flows_with_agents():
        for aid, agent in agents.items():
            if not isinstance(agent, dict): continue
            wc = agent.get('workspace_class')
            if wc and wc not in workspace_classes:
                error("workspace_class_exists", "workspace_class '%s' not defined in policy.yaml workspace_classes" % wc, "%s/%s" % (flow, aid))

# ============================================================
# CHECK: credential_key_exists [warning, global]
# Note: Python can only check if keys are referenced, not if they
# exist in the credential store. Logged as info for awareness.
# Full validation requires the Go runtime with credential store access.
# ============================================================
# (Skipped — requires runtime credential store)

# ============================================================
# CHECK: mcp_server_reachable [warning, global]
# (Skipped — requires runtime MCP connectivity)
# ============================================================

# ============================================================
# GATE-INFO: sets_gate targets referenced by guards
# (Not a spec check — informational for contract authors)
# ============================================================
gates_set = set()
gates_read = set()
for flow, nodes in iter_flows_with_nodes():
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        for ev, h in node.get('event_handlers', {}).items():
            if not isinstance(h, dict): continue
            g = h.get('sets_gate')
            if g: gates_set.add(g)
            guard = h.get('guard', {})
            if isinstance(guard, dict):
                for check in guard.get('checks', []) + ([guard] if 'check' in guard else []):
                    if isinstance(check, dict):
                        for gate_ref in re.findall(r'entity\.gates\.(\w+)', check.get('check', '')):
                            gates_read.add(gate_ref)
for g in gates_set:
    if g not in gates_read:
        warn("gate_info", "sets_gate '%s' but no guard reads entity.gates.%s" % (g, g))

# ============================================================
# Report
# ============================================================
print("=" * 70)
print("SWARM PLATFORM VERIFICATION REPORT")
print("=" * 70)

errors_list = [f for f in findings if f['severity'] == 'error']
warnings_list = [f for f in findings if f['severity'] == 'warning']

print("\nERRORS: %d" % len(errors_list))
for f in sorted(errors_list, key=lambda x: (x['check_id'], x['location'])):
    loc = " [%s]" % f['location'] if f['location'] else ""
    print("  [ERROR] %s: %s%s" % (f['check_id'], f['message'], loc))

print("\nWARNINGS: %d" % len(warnings_list))
for f in sorted(warnings_list, key=lambda x: (x['check_id'], x['location'])):
    loc = " [%s]" % f['location'] if f['location'] else ""
    print("  [WARN]  %s: %s%s" % (f['check_id'], f['message'], loc))

h = sum(len(n.get('event_handlers', {})) for n in all_nodes.values() if isinstance(n, dict))
a = sum(1 for v in all_agents.values() if isinstance(v, dict))
e = sum(1 for v in all_events.values() if isinstance(v, dict))
t = sum(1 for v in all_tools.values() if isinstance(v, dict))

if deferred_count:
    print("\n[INFO]  %d prompts marked DEFERRED" % deferred_count)

print("\n[INFO]  structural verifier subset; Go runtime owns CEL, runtime executor resolution, discovered MCP tool inventory, platform_tool_usage_hints, generated_tool_schema_closure, deep artifact result-event typing, credential, and entity-write-target checks")

print("\n" + "=" * 70)
print("SUMMARY: %d errors, %d warnings" % (len(errors_list), len(warnings_list)))
print("COUNTS: %d handlers, %d agents, %d events, %d tools" % (h, a, e, t))
print("=" * 70)

sys.exit(1 if errors_list else 0)
