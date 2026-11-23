#!/usr/bin/env python3
"""
Sets the group prefix and client IDs in the Node-RED flows and in python/config.py.

  python3 configure.py 6            -> prefix upm-dsiot-g6/move, client IDs g6-...
  python3 configure.py 6 gabriel    -> prefix upm-dsiot-g6-gabriel/move (for development,
                                       so team members do not interfere on the public broker)

Can be run any number of times. A backup *.bak is created for each file first.
Back to the state in the repo: git restore flows python
"""
import glob, re, shutil, sys

if len(sys.argv) < 2 or not re.fullmatch(r'\d+', sys.argv[1]):
    sys.exit('Usage: python3 configure.py <group-number> [personal-tag]')
tag = 'g' + sys.argv[1]
if len(sys.argv) > 2:
    suffix = sys.argv[2].lower()
    if not re.fullmatch(r'[a-z0-9]+', suffix):
        sys.exit('Personal tag: lowercase letters and digits only, e.g. gabriel')
    tag += '-' + suffix

files = glob.glob('flows/*.json') + ['python/config.py']
for path in files:
    try:
        s = open(path, encoding='utf-8').read()
    except FileNotFoundError:
        sys.exit('Files not found. Run this in the repository root.')
    m = re.search(r'upm-dsiot-([a-z0-9-]+)/move', s)
    if not m:
        print('skipped (no prefix found):', path)
        continue
    old = m.group(1)
    shutil.copy(path, path + '.bak')
    s = s.replace('upm-dsiot-' + old + '/move', 'upm-dsiot-' + tag + '/move')
    s = re.sub(r'\b' + re.escape(old) + r'-(col-bicimad|col-weather|col-timetable|col-emt|app)\b', tag + r'-\1', s)
    s = re.sub(r"^TAG = '[^']*'", "TAG = '" + tag + "'", s, flags=re.M)
    open(path, 'w', encoding='utf-8').write(s)
    print(f'{path}: {old} -> {tag}')
print('Done. Prefix:', 'upm-dsiot-' + tag + '/move')
