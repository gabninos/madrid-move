#!/usr/bin/env python3
"""
Setzt Gruppen-Präfix und Client-IDs in den Node-RED-Flows und in python/config.py.

  python3 configure.py 6            -> Präfix upm-dsiot-g6/move, Client-IDs g6-...
  python3 configure.py 6 gabriel    -> Präfix upm-dsiot-g6-gabriel/move (zum Entwickeln,
                                       damit ihr euch auf dem öffentlichen Broker nicht stört)

Kann beliebig oft ausgeführt werden. Vorher wird je Datei eine Sicherung *.bak angelegt.
Zurück zum Stand im Repo: git restore flows python
"""
import glob, re, shutil, sys

if len(sys.argv) < 2 or not re.fullmatch(r'\d+', sys.argv[1]):
    sys.exit('Aufruf: python3 configure.py <gruppennummer> [persönliches-kürzel]')
tag = 'g' + sys.argv[1]
if len(sys.argv) > 2:
    suffix = sys.argv[2].lower()
    if not re.fullmatch(r'[a-z0-9]+', suffix):
        sys.exit('Kürzel nur aus Kleinbuchstaben und Ziffern, z. B. gabriel')
    tag += '-' + suffix

files = glob.glob('flows/*.json') + ['python/config.py']
for path in files:
    try:
        s = open(path, encoding='utf-8').read()
    except FileNotFoundError:
        sys.exit('Dateien nicht gefunden. Im Repo-Hauptordner ausführen.')
    m = re.search(r'upm-dsiot-([a-z0-9-]+)/move', s)
    if not m:
        print('übersprungen (kein Präfix gefunden):', path)
        continue
    old = m.group(1)
    shutil.copy(path, path + '.bak')
    s = s.replace('upm-dsiot-' + old + '/move', 'upm-dsiot-' + tag + '/move')
    s = re.sub(r'\b' + re.escape(old) + r'-(col-bicimad|col-weather|app|sim)\b', tag + r'-\1', s)
    s = re.sub(r"^TAG = '[^']*'", "TAG = '" + tag + "'", s, flags=re.M)
    open(path, 'w', encoding='utf-8').write(s)
    print(f'{path}: {old} -> {tag}')
print('Fertig. Präfix:', 'upm-dsiot-' + tag + '/move')
