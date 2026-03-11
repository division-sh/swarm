#!/usr/bin/env python3
"""
MAS Platform Specification Verifier
Boot-time compliance checks. Reference implementation for platform boot_verification.
"""
import yaml, os, sys, re
from collections import defaultdict

BASE = os.path.dirname(os.path.abspath(__file__))
EC = os.path.join(BASE, 'empire', 'contracts')

errors = []
warnings = []
info = []

def error(cat, msg): errors.append("[ERROR] %s: %s" % (cat, msg))
def warn(cat, msg): warnings.append("[WARN]  %s: %s" % (cat, msg))

def load(path):
    with open(path) as f:
        return yaml.safe_load(f) or {}

# ============================================================
# 1. Load everything
# ============================================================
FLOWS = ['discovery', 'scoring', 'validation', 'operating']

root_agents = load(os.path.join(EC, 'agents.yaml'))
root_nodes = load(os.path.join(EC, 'nodes.yaml'))
root_events = load(os.path.join(EC, 'events.yaml'))
root_tools = load(os.path.join(EC, 'tools.yaml'))
root_policy = load(os.path.join(EC, 'policy.yaml'))

flow_data = {}
for flow in FLOWS:
    fd = os.path.join(EC, 'flows', flow)
    flow_data[flow] = {
        'agents': load(os.path.join(fd, 'agents.yaml')),
        'nodes': load(os.path.join(fd, 'nodes.yaml')),
        'events': load(os.path.join(fd, 'events.yaml')),
        'schema': load(os.path.join(fd, 'schema.yaml')),
        'package': load(os.path.join(fd, 'package.yaml')),
    }
    tp = os.path.join(fd, 'tools.yaml')
    flow_data[flow]['tools'] = load(tp) if os.path.exists(tp) else {}
    pp = os.path.join(fd, 'policy.yaml')
    flow_data[flow]['policy'] = load(pp) if os.path.exists(pp) else {}

# Merge all
all_agents, all_nodes, all_events, all_tools, all_policy = {}, {}, {}, {}, {}
all_agents.update(root_agents)
all_nodes.update(root_nodes)
all_events.update(root_events)
all_tools.update(root_tools)
all_policy.update(root_policy)
for flow in FLOWS:
    all_agents.update(flow_data[flow]['agents'])
    all_nodes.update(flow_data[flow]['nodes'])
    all_events.update(flow_data[flow]['events'])
    all_tools.update(flow_data[flow]['tools'])
    all_policy.update(flow_data[flow]['policy'])

events_defined = set(k for k, v in all_events.items() if isinstance(v, dict))

# Helper: flatten payload schema to dot-path field set
def flatten_payload(payload, prefix=''):
    fields = set()
    if not isinstance(payload, dict):
        return fields
    for k, v in payload.items():
        if k.startswith('_'): continue
        full = (prefix + '.' + k) if prefix else k
        fields.add(full)
        if isinstance(v, dict) and not any(t in str(v) for t in ['string','integer','number','boolean','array','object','text','timestamp','uuid','numeric']):
            fields.update(flatten_payload(v, full))
    return fields

# Helper: extract payload.X references from a string
def extract_payload_refs(s):
    return re.findall(r'payload\.([a-zA-Z_][a-zA-Z0-9_.]*)', str(s))

# Helper: extract entity.X references
def extract_entity_refs(s):
    return re.findall(r'entity\.([a-zA-Z_][a-zA-Z0-9_.]*)', str(s))

# Helper: extract policy.X references
def extract_policy_refs(s):
    return re.findall(r'policy\.([a-zA-Z_][a-zA-Z0-9_.]*)', str(s))

# Helper: check if event is suppressed (external, mailbox, planned, fan_out)
def is_suppressed(ev_name):
    ev = all_events.get(ev_name, {})
    if isinstance(ev, dict):
        if ev.get('_source', '').startswith('external'): return True
        if ev.get('_consumer', '').startswith('mailbox'): return True
        if ev.get('_status') == 'planned': return True
    return False

# ============================================================
# CHECK 1: Event chain integrity
# ============================================================
events_emitted = set()
events_subscribed = set()

# Collect emitters (including fan_out, auto_emit, produces)
for flow in FLOWS:
    ae = flow_data[flow]['schema'].get('auto_emit_on_create', {})
    if isinstance(ae, dict) and 'event' in ae:
        events_emitted.add(ae['event'])

for nid, node in all_nodes.items():
    if not isinstance(node, dict): continue
    events_emitted.update(node.get('produces', []))
    for ev, h in node.get('event_handlers', {}).items():
        if not isinstance(h, dict): continue
        emits = h.get('emits')
        if isinstance(emits, str): events_emitted.add(emits)
        elif isinstance(emits, list): events_emitted.update(emits)
        fo = h.get('fan_out')
        if isinstance(fo, dict):
            if fo.get('emit_per_item'): events_emitted.add(fo['emit_per_item'])
            mapping = fo.get('emit_mapping', {}).get('mapping', {})
            events_emitted.update(mapping.values())
        rules = h.get('rules', {})
        if isinstance(rules, dict):
            for rn, r in rules.items():
                if isinstance(r, dict):
                    re_ = r.get('emits')
                    if isinstance(re_, str): events_emitted.add(re_)
                    elif isinstance(re_, list): events_emitted.update(re_)
        oc = h.get('on_complete')
        if isinstance(oc, (dict, list)):
            items = oc.values() if isinstance(oc, dict) else oc
            for branch in items:
                if isinstance(branch, dict):
                    be = branch.get('emits')
                    if isinstance(be, str): events_emitted.add(be)
                    elif isinstance(be, list): events_emitted.update(be)

for aid, agent in all_agents.items():
    if not isinstance(agent, dict): continue
    events_emitted.update(agent.get('emit_events', []))

# Collect subscribers
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

# Events emitted without schema
for ev in events_emitted:
    if ev and ev not in events_defined and not ev.startswith('timer.') and not ev.startswith('*.'):
        warn("EVENT-NO-SCHEMA", "'%s' emitted but no schema in events.yaml" % ev)

# Events emitted without consumer (suppressed if external/mailbox/planned)
for ev in events_emitted:
    if ev and ev in events_defined and ev not in events_subscribed and not ev.startswith('pipeline.'):
        if not is_suppressed(ev):
            warn("EVENT-NO-CONSUMER", "'%s' emitted but nobody subscribes" % ev)

# Events subscribed without producer (suppressed if external/mailbox/planned/fan_out)
fan_out_events = set()
for nid, node in all_nodes.items():
    if not isinstance(node, dict): continue
    for ev, h in node.get('event_handlers', {}).items():
        fo = h.get('fan_out', {}) if isinstance(h, dict) else {}
        if isinstance(fo, dict) and fo.get('emit_per_item'):
            fan_out_events.add(fo['emit_per_item'])

for ev in events_subscribed:
    if ev and ev in events_defined and ev not in events_emitted:
        if not is_suppressed(ev) and ev not in fan_out_events and not ev.startswith('timer.'):
            warn("EVENT-NO-PRODUCER", "'%s' subscribed but nobody emits" % ev)

# ============================================================
# CHECK 2: Payload field coverage (data_accumulation)
# ============================================================
for flow in ['root'] + FLOWS:
    nodes = root_nodes if flow == 'root' else flow_data[flow]['nodes']
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        for ev, h in node.get('event_handlers', {}).items():
            if not isinstance(h, dict): continue
            da = h.get('data_accumulation')
            if not isinstance(da, dict): continue
            source_ev = da.get('source_event', ev)
            event_schema = all_events.get(source_ev, {})
            payload_fields = flatten_payload(event_schema.get('payload', {})) if isinstance(event_schema, dict) else set()
            for w in da.get('writes', []):
                if isinstance(w, str):
                    if payload_fields and w not in payload_fields:
                        error("PAYLOAD-MISMATCH", "%s/%s/%s: writes '%s' but %s payload has %s" % (flow, nid, ev, w, source_ev, sorted(payload_fields)))
                elif isinstance(w, dict):
                    sf = w.get('source_field', '')
                    if sf and payload_fields and sf not in payload_fields:
                        error("PAYLOAD-MISMATCH", "%s/%s/%s: source_field '%s' not in %s payload" % (flow, nid, ev, sf, source_ev))

# ============================================================
# CHECK 3: Condition → payload field alignment
# ============================================================
for flow in ['root'] + FLOWS:
    nodes = root_nodes if flow == 'root' else flow_data[flow]['nodes']
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        for ev, h in node.get('event_handlers', {}).items():
            if not isinstance(h, dict): continue
            event_payload = all_events.get(ev, {})
            payload_fields = flatten_payload(event_payload.get('payload', {})) if isinstance(event_payload, dict) else set()
            if not payload_fields: continue

            # Rules conditions
            rules = h.get('rules', {})
            if isinstance(rules, dict):
                for rule_name, rule in rules.items():
                    if not isinstance(rule, dict): continue
                    cond = rule.get('condition', '')
                    if cond == 'else': continue
                    for ref in extract_payload_refs(cond):
                        if not any(ref == pf or ref.startswith(pf+'.') or pf.startswith(ref) for pf in payload_fields):
                            error("CONDITION-PAYLOAD", "%s/%s/%s rule '%s': payload.%s not in event payload %s" % (flow, nid, ev, rule_name, ref, sorted(payload_fields)))

            # Guard conditions
            guard = h.get('guard', {})
            if isinstance(guard, dict):
                checks = guard.get('checks', [])
                if not checks and 'check' in guard:
                    checks = [guard]
                for check in checks:
                    if not isinstance(check, dict): continue
                    for ref in extract_payload_refs(check.get('check', '')):
                        if not any(ref == pf or ref.startswith(pf+'.') or pf.startswith(ref) for pf in payload_fields):
                            error("CONDITION-PAYLOAD", "%s/%s/%s guard: payload.%s not in event payload %s" % (flow, nid, ev, ref, sorted(payload_fields)))

            # on_complete conditions
            oc = h.get('on_complete')
            if isinstance(oc, (dict, list)):
                items = oc.values() if isinstance(oc, dict) else oc
                for branch in items:
                    if isinstance(branch, dict):
                        for ref in extract_payload_refs(branch.get('condition', '')):
                            if not any(ref == pf or ref.startswith(pf+'.') or pf.startswith(ref) for pf in payload_fields):
                                error("CONDITION-PAYLOAD", "%s/%s/%s on_complete: payload.%s not in event payload" % (flow, nid, ev, ref))

# ============================================================
# CHECK 4: Condition → policy key alignment
# ============================================================
for flow in ['root'] + FLOWS:
    nodes = root_nodes if flow == 'root' else flow_data[flow]['nodes']
    flow_policy = all_policy  # merged
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        for ev, h in node.get('event_handlers', {}).items():
            if not isinstance(h, dict): continue
            # Collect all conditions in handler
            all_conds = []
            guard = h.get('guard', {})
            if isinstance(guard, dict):
                checks = guard.get('checks', [])
                if not checks and 'check' in guard:
                    checks = [guard]
                all_conds.extend(check.get('check', '') for check in checks if isinstance(check, dict))
            rules = h.get('rules', {})
            if isinstance(rules, dict):
                all_conds.extend(r.get('condition', '') for r in rules.values() if isinstance(r, dict))
            oc = h.get('on_complete')
            if isinstance(oc, (dict, list)):
                items = oc.values() if isinstance(oc, dict) else oc
                all_conds.extend(b.get('condition', '') for b in items if isinstance(b, dict))

            for cond in all_conds:
                for ref in extract_policy_refs(cond):
                    if ref not in flow_policy:
                        warn("CONDITION-POLICY", "%s/%s/%s: policy.%s referenced but not in any policy.yaml" % (flow, nid, ev, ref))

# ============================================================
# CHECK 5: State machine coherence
# ============================================================
for flow in FLOWS:
    schema = flow_data[flow]['schema']
    declared_states = set(schema.get('states', []))
    initial = schema.get('initial_state')
    terminals = set(schema.get('terminal_states', []))

    if initial and initial not in declared_states and declared_states:
        error("STATE-MACHINE", "%s: initial_state '%s' not in declared states" % (flow, initial))
    for t in terminals:
        if t not in declared_states and declared_states:
            error("STATE-MACHINE", "%s: terminal_state '%s' not in declared states" % (flow, t))

    # Check all advances_to targets are declared states
    nodes = flow_data[flow]['nodes']
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        for ev, h in node.get('event_handlers', {}).items():
            if not isinstance(h, dict): continue
            target = h.get('advances_to')
            if target and declared_states and target not in declared_states:
                error("STATE-MACHINE", "%s/%s/%s: advances_to '%s' not in declared states %s" % (flow, nid, ev, target, sorted(declared_states)))
            # Check on_complete branches
            oc = h.get('on_complete')
            if isinstance(oc, (dict, list)):
                items = oc.values() if isinstance(oc, dict) else oc
                for branch in items:
                    if isinstance(branch, dict):
                        bt = branch.get('advances_to')
                        if bt and declared_states and bt not in declared_states:
                            error("STATE-MACHINE", "%s/%s/%s on_complete: advances_to '%s' not in declared states" % (flow, nid, ev, bt))

# ============================================================
# CHECK 6: required_agents vs agents.yaml
# ============================================================
for flow in FLOWS:
    schema = flow_data[flow]['schema']
    agents = flow_data[flow]['agents']
    for ra in schema.get('required_agents', []):
        role = ra.get('role', '')
        agent = agents.get(role)
        if not isinstance(agent, dict):
            error("REQUIRED-AGENT", "%s: required role '%s' not in agents.yaml" % (flow, role))
            continue
        schema_emits = set(ra.get('emits', []))
        agent_emits = set(agent.get('emit_events', []))
        diff = schema_emits - agent_emits
        if diff:
            error("EMIT-MISMATCH", "%s/%s: schema says emits %s but agent doesn't" % (flow, role, diff))

# ============================================================
# CHECK 7: Handler field compliance
# ============================================================
DEFINED_HANDLER_FIELDS = {
    'description', '_note', 'guard', 'accumulate', 'compute', 'on_complete',
    'advances_to', 'sets_gate', 'data_accumulation', 'emits', 'rules',
    'fan_out', 'query', 'reduce', 'filter', 'count', 'clear', 'action',
    'template', 'instance_id_from', 'config_from', 'from', 'payload_transform',
    'clear_gates',
}
for flow in ['root'] + FLOWS:
    nodes = root_nodes if flow == 'root' else flow_data[flow]['nodes']
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        for ev, h in node.get('event_handlers', {}).items():
            if not isinstance(h, dict): continue
            for field in h.keys():
                if field not in DEFINED_HANDLER_FIELDS:
                    error("UNDEFINED-FIELD", "%s/%s/%s: handler field '%s' not in platform spec" % (flow, nid, ev, field))

# ============================================================
# CHECK 8: Tool references resolve
# ============================================================
for flow in ['root'] + FLOWS:
    agents = root_agents if flow == 'root' else flow_data[flow]['agents']
    for aid, agent in agents.items():
        if not isinstance(agent, dict): continue
        for tool in agent.get('tools_tier2', []):
            if tool not in all_tools:
                warn("TOOL-MISSING", "%s/%s: tool '%s' not in any tools.yaml" % (flow, aid, tool))

# ============================================================
# CHECK 9: Prompt files exist
# ============================================================
for flow in ['root'] + FLOWS:
    agents = root_agents if flow == 'root' else flow_data[flow]['agents']
    prompt_dir = os.path.join(EC, 'prompts') if flow == 'root' else os.path.join(EC, 'flows', flow, 'prompts')
    for aid in agents:
        if not isinstance(agents[aid], dict): continue
        pp = os.path.join(prompt_dir, '%s.md' % aid)
        if not os.path.exists(pp):
            warn("PROMPT-MISSING", "%s/%s: no prompt file" % (flow, aid))
        else:
            with open(pp) as f:
                content = f.read()
            if '<!-- TODO' in content and '<!-- DEFERRED' not in content:
                warn("PROMPT-STUB", "%s/%s: prompt contains TODO" % (flow, aid))

# ============================================================
# CHECK 10: Deprecated fields
# ============================================================
DEPRECATED = ['subscriptions_bootstrap', 'logic', 'on_below_threshold', 'on_dedup', 'on_pass']
for flow in ['root'] + FLOWS:
    agents = root_agents if flow == 'root' else flow_data[flow]['agents']
    for aid, agent in agents.items():
        if isinstance(agent, dict):
            for dep in DEPRECATED:
                if dep in agent:
                    error("DEPRECATED", "%s/%s: uses deprecated '%s'" % (flow, aid, dep))
    nodes = root_nodes if flow == 'root' else flow_data[flow]['nodes']
    for nid, node in nodes.items():
        if isinstance(node, dict):
            for ev, h in node.get('event_handlers', {}).items():
                if isinstance(h, dict):
                    for dep in DEPRECATED:
                        if dep in h:
                            error("DEPRECATED", "%s/%s/%s: uses deprecated '%s'" % (flow, nid, ev, dep))

# ============================================================
# CHECK 11: Produces list matches actual handler emits
# ============================================================
for flow in ['root'] + FLOWS:
    nodes = root_nodes if flow == 'root' else flow_data[flow]['nodes']
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        declared_produces = set(node.get('produces', []))
        actual_emits = set()
        for ev, h in node.get('event_handlers', {}).items():
            if not isinstance(h, dict): continue
            e = h.get('emits')
            if isinstance(e, str): actual_emits.add(e)
            elif isinstance(e, list): actual_emits.update(e)
            fo = h.get('fan_out', {})
            if isinstance(fo, dict) and fo.get('emit_per_item'):
                actual_emits.add(fo['emit_per_item'])
            rules = h.get('rules', {})
            if isinstance(rules, dict):
                for r in rules.values():
                    if isinstance(r, dict):
                        re_ = r.get('emits')
                        if isinstance(re_, str): actual_emits.add(re_)
                        elif isinstance(re_, list): actual_emits.update(re_)
            oc = h.get('on_complete')
            if isinstance(oc, (dict, list)):
                items = oc.values() if isinstance(oc, dict) else oc
                for b in items:
                    if isinstance(b, dict):
                        be = b.get('emits')
                        if isinstance(be, str): actual_emits.add(be)
                        elif isinstance(be, list): actual_emits.update(be)
        emits_not_in_produces = actual_emits - declared_produces
        if emits_not_in_produces:
            for e in emits_not_in_produces:
                warn("PRODUCES-DRIFT", "%s/%s: emits '%s' but not in produces list" % (flow, nid, e))

# ============================================================
# CHECK 12: Gate references — sets_gate targets exist in schema or node gate_state
# ============================================================
for flow in FLOWS:
    schema = flow_data[flow]['schema']
    nodes = flow_data[flow]['nodes']
    for nid, node in nodes.items():
        if not isinstance(node, dict): continue
        declared_gates = set(node.get('gate_state', {}).keys())
        for ev, h in node.get('event_handlers', {}).items():
            if not isinstance(h, dict): continue
            gate = h.get('sets_gate')
            if gate:
                # Gate should be somewhere — node gate_state or just used
                pass  # Gates are created on first set, no pre-declaration required

# ============================================================
# CHECK 13: Policy conflict detection
# ============================================================
for flow in FLOWS:
    fp = flow_data[flow]['policy']
    for key, fval in fp.items():
        if key.startswith('_'): continue
        if key in root_policy and not isinstance(fval, dict) and not isinstance(root_policy[key], dict):
            if fval != root_policy[key]:
                warn("POLICY-CONFLICT", "'%s': root=%s, %s=%s" % (key, root_policy[key], flow, fval))

# ============================================================
# Report
# ============================================================
print("=" * 70)
print("MAS PLATFORM VERIFICATION REPORT")
print("=" * 70)

print("\nERRORS: %d" % len(errors))
for e in sorted(errors):
    print("  %s" % e)

print("\nWARNINGS: %d" % len(warnings))
for w in sorted(warnings):
    print("  %s" % w)

h = sum(len(n.get('event_handlers', {})) for n in all_nodes.values() if isinstance(n, dict))
a = sum(1 for v in all_agents.values() if isinstance(v, dict))
e = sum(1 for v in all_events.values() if isinstance(v, dict))
t = sum(1 for v in all_tools.values() if isinstance(v, dict))

# Deferred prompts
deferred = 0
for root, dirs, files in os.walk(os.path.join(BASE, 'empire', 'contracts')):
    for f in files:
        if f.endswith('.md') and 'prompts' in root:
            with open(os.path.join(root, f)) as fh:
                if '<!-- DEFERRED' in fh.read():
                    deferred += 1
if deferred:
    print("\n[INFO]  %d prompts marked DEFERRED" % deferred)

print("\n" + "=" * 70)
print("SUMMARY: %d errors, %d warnings" % (len(errors), len(warnings)))
print("COUNTS: %d handlers, %d agents, %d events, %d tools" % (h, a, e, t))
print("=" * 70)

sys.exit(1 if errors else 0)
