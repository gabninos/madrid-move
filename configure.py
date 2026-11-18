#!/usr/bin/env python3
"""
Setzt Gruppen-Präfix und Client-IDs in den Flow-Dateien.

  python configure.py 12            -> Präfix upm-dsiot-g12/move, Client-IDs g12-...
  python configure.py 12 tobias     -> Präfix upm-dsiot-g12-tobias/move (zum Entwickeln,
                                       damit ihr euch auf dem öffentlichen Broker nicht stört)

Kann beliebig oft ausgeführt werden. Vorher wird eine Sicherung *.bak angelegt.
"""
import re, sys, glob, shutil

if len(sys.argv) < 2 or not re.fullmatch(r'\d+', sys.argv[1]):
    sys.exit('Aufruf: python configure.py <gruppennummer> [persönliches-kürzel]')
tag = 'g' + sys.argv[1]
if len(sys.argv) > 2:
    suffix = sys.argv[2].lower()
    if not re.fullmatch(r'[a-z0-9]+', suffix):
        sys.exit('Kürzel nur aus Kleinbuchstaben und Ziffern, z. B. tobias')
    tag += '-' + suffix

files = glob.glob('flows/*.json')
if not files:
    sys.exit('Keine Dateien in flows/ gefunden. Im Repo-Hauptordner ausführen.')
for path in files:
    s = open(path, encoding='utf-8').read()
    m = re.search(r'upm-dsiot-([a-z0-9-]+)/move', s)
    if not m:
        print('übersprungen (kein Präfix gefunden):', path)
        continue
    old = m.group(1)
    shutil.copy(path, path + '.bak')
    s = s.replace('upm-dsiot-' + old + '/move', 'upm-dsiot-' + tag + '/move')
    s = re.sub(r'\b' + re.escape(old) + r'-(col-bicimad|col-weather|app|sim)\b', tag + r'-\1', s)
    open(path, 'w', encoding='utf-8').write(s)
    print(f'{path}: {old} -> {tag}')
print('Fertig. Präfix:', 'upm-dsiot-' + tag + '/move')
