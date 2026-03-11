#!/bin/bash
# Regenerate runtime bridge files from source contracts
# Run from empire/contracts/ directory

BASE="$(dirname "$0")/.."
OUT="$(dirname "$0")"

echo "Regenerating runtime bridge from source contracts..."

python3 -c "
import yaml, os, sys

base = '$BASE'
out = '$OUT'

def merge_yaml(paths):
    merged = {}
    for p in paths:
        if os.path.exists(p):
            with open(p) as f:
                d = yaml.safe_load(f) or {}
                merged.update(d)
    return merged

flows = ['discovery', 'scoring', 'validation', 'operating']

for kind in ['nodes', 'agents', 'events']:
    paths = [base + '/%s.yaml' % kind] + [base + '/flows/%s/%s.yaml' % (f, kind) for f in flows]
    merged = merge_yaml(paths)
    with open(out + '/%s.yaml' % kind, 'w') as f:
        f.write('# AUTO-GENERATED — DO NOT EDIT DIRECTLY\n')
        yaml.dump(merged, f, default_flow_style=False, sort_keys=False, allow_unicode=True, width=120)

for kind in ['tools', 'policy']:
    paths = [base + '/%s.yaml' % kind] + [base + '/flows/%s/%s.yaml' % (f, kind) for f in flows if os.path.exists(base + '/flows/%s/%s.yaml' % (f, kind))]
    merged = merge_yaml(paths)
    with open(out + '/%s.yaml' % kind, 'w') as f:
        f.write('# AUTO-GENERATED — DO NOT EDIT DIRECTLY\n')
        yaml.dump(merged, f, default_flow_style=False, sort_keys=False, allow_unicode=True, width=120)

print('Done.')
"
