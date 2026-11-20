#!/usr/bin/env python3
"""MadridMove Client: die App des Nutzers (Producer und Consumer).

Befehle:
    python3 client.py stationen <suchtext>
        Station suchen (ID für Routen), z. B.  stationen sol
    python3 client.py route-anlegen <nutzer> <routenname> --start <ID> --ziel <ID> --zeit 08:30 [--tage 1-5]
        Route speichern (retained, QoS 1). Tage: 1=Mo ... 7=So, z. B. 1-5 oder 1,3,5
    python3 client.py routen <nutzer>
        Eigene Routen und aktuellen Status anzeigen
    python3 client.py route-loeschen <nutzer> <routen-id>
        Route löschen (leere retained Nachricht)
    python3 client.py einstellungen <nutzer> [--regen kein|leicht|egal] [--wind 25] [--temp 8 30]
        Persönliche Grenzen setzen bzw. anzeigen (ohne Optionen)
    python3 client.py empfangen <nutzer>
        Meldungen empfangen (Vorab-Info, Warnung, Entwarnung). Persistente Session:
        Meldungen, die ankommen, während der Client aus ist, werden beim nächsten Start nachgeliefert.
"""
import argparse
import json
import re
import sys
import threading
import time
import unicodedata
from datetime import datetime

import paho.mqtt.client as mqtt

from config import BROKER, KEEPALIVE, PORT, PREFIX, TAG

DEFAULTS = {'rain': 'kein', 'windMax': 25, 'tempMin': 8, 'tempMax': 30}   # "vorsichtig"


def norm(s):
    s = unicodedata.normalize('NFD', str(s).lower())
    return ''.join(ch for ch in s if unicodedata.category(ch) != 'Mn')


def slug(s):
    return re.sub(r'[^a-z0-9]+', '-', norm(s)).strip('-')[:30]


def connect(client_id=None, clean=True):
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                    client_id=client_id or f'{TAG}-cli-{int(time.time() * 1000) % 10**8}', clean_session=clean)
    c.connect(BROKER, PORT, keepalive=KEEPALIVE)
    return c


def read_retained(topic, wait=3.0):
    """Liest retained Nachrichten zu einem Topic (auch mit Wildcards): {topic: payload}."""
    got, done = {}, threading.Event()
    c = connect()

    def on_message(cl, ud, msg):
        if msg.retain and msg.payload:
            try:
                got[msg.topic] = json.loads(msg.payload)
            except ValueError:
                got[msg.topic] = msg.payload.decode('utf-8', 'replace')
            if '+' not in topic and '#' not in topic:
                done.set()
    c.on_message = on_message
    c.subscribe(topic, qos=1)
    c.loop_start()
    done.wait(wait)
    c.loop_stop()
    c.disconnect()
    return got


def publish(topic, payload, retain=True):
    c = connect()
    c.loop_start()
    data = '' if payload is None else json.dumps(payload, ensure_ascii=False)
    c.publish(topic, data, qos=1, retain=retain).wait_for_publish(5)
    c.loop_stop()
    c.disconnect()


def stations():
    got = read_retained(f'{PREFIX}/static/stations', wait=8)
    lst = next(iter(got.values()), None)
    if not lst:
        sys.exit('Keine Stationsliste auf dem Broker. Läuft der Collector?')
    return {s['id']: s for s in lst}


def parse_days(text):
    days = set()
    for part in text.split(','):
        if '-' in part:
            a, b = part.split('-')
            days.update(range(int(a), int(b) + 1))
        elif part.strip():
            days.add(int(part))
    if not days or not days <= set(range(1, 8)):
        sys.exit('Tage bitte als 1-7 angeben, z. B. 1-5 oder 1,3,5')
    return sorted(days)


# ---------------------------------------------------------------- Befehle

def cmd_stationen(a):
    q = norm(a.suchtext)
    hits = [s for s in stations().values() if q in norm(s['name']) or q == s['id']]
    for s in sorted(hits, key=lambda s: s['name'])[:25]:
        print(f"{s['id']:>6}  {s['name']}  ({s.get('capacity') or '?'} Plätze)")
    if not hits:
        print('Keine Station gefunden.')


def cmd_route_anlegen(a):
    st = stations()
    for sid in (a.start, a.ziel):
        if sid not in st:
            sys.exit(f'Station {sid} unbekannt. Suchen mit: python3 client.py stationen <name>')
    if a.start == a.ziel:
        sys.exit('Start und Ziel sind gleich.')
    if not re.fullmatch(r'\d{1,2}:\d{2}', a.zeit):
        sys.exit('Zeit bitte als HH:MM, z. B. 08:30')
    user, rid = slug(a.nutzer), slug(a.routenname)
    route = {'name': a.routenname,
             'from': {'id': a.start, 'label': st[a.start]['name']},
             'to': {'id': a.ziel, 'label': st[a.ziel]['name']},
             'departure': {'time': a.zeit.zfill(5), 'days': parse_days(a.tage)},
             'createdAt': datetime.now().isoformat(timespec='seconds')}
    publish(f'{PREFIX}/routes/{user}/{rid}', route)
    print(f"Route {user}/{rid} gespeichert: {route['from']['label']} -> {route['to']['label']}, "
          f"{route['departure']['time']} Uhr, Tage {route['departure']['days']}")


def cmd_routen(a):
    user = slug(a.nutzer)
    routes = read_retained(f'{PREFIX}/routes/{user}/+')
    recos = read_retained(f'{PREFIX}/reco/{user}/+')
    if not routes:
        print('Keine Routen gespeichert.')
    for topic, r in sorted(routes.items()):
        rid = topic.split('/')[-1]
        reco = recos.get(f'{PREFIX}/reco/{user}/{rid}') or {}
        print(f"{rid}: {r['from']['label']} -> {r['to']['label']}, {r['departure']['time']} Uhr")
        if reco:
            print(f"    {reco.get('headline', '')}  {reco.get('detail', '')}")


def cmd_route_loeschen(a):
    publish(f'{PREFIX}/routes/{slug(a.nutzer)}/{a.routen_id}', None)
    print('Route gelöscht.')


def cmd_einstellungen(a):
    user = slug(a.nutzer)
    topic = f'{PREFIX}/users/{user}/settings'
    cur = dict(DEFAULTS)
    cur.update(next(iter(read_retained(topic).values()), {}) or {})
    changed = False
    if a.regen:
        cur['rain'], changed = a.regen, True
    if a.wind is not None:
        cur['windMax'], changed = a.wind, True
    if a.temp:
        cur['tempMin'], cur['tempMax'], changed = a.temp[0], a.temp[1], True
    if changed:
        publish(topic, cur)
        print('Gespeichert:', end=' ')
    regen = {'kein': 'kein Regen', 'leicht': 'leichter Regen ok', 'egal': 'Regen egal'}[cur['rain']]
    print(f"{regen}, Wind bis {cur['windMax']} km/h, {cur['tempMin']} bis {cur['tempMax']} °C")


def cmd_empfangen(a):
    user = slug(a.nutzer)
    status = f'{PREFIX}/system/client-{user}'
    # Feste Client-ID + clean_session=False = persistente Session beim Broker
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f'{TAG}-client-{user}', clean_session=False)
    c.will_set(status, 'offline (Absturz)', qos=1, retain=True)

    def on_connect(cl, ud, flags, rc, props):
        resumed = ' (Session fortgesetzt)' if flags.session_present else ''
        print(f'Verbunden mit {BROKER}{resumed}. Warte auf Meldungen für "{user}" ... (Strg+C beendet)')
        cl.publish(status, 'online', qos=1, retain=True)
        cl.subscribe(f'{PREFIX}/alerts/{user}', qos=1)

    def on_message(cl, ud, msg):
        text = msg.payload.decode('utf-8', 'replace')
        print(f"{datetime.now():%H:%M:%S}  {text}", flush=True)
    c.on_connect, c.on_message = on_connect, on_message
    c.connect(BROKER, PORT, keepalive=KEEPALIVE)
    try:
        c.loop_forever()
    except KeyboardInterrupt:
        c.publish(status, 'offline (beendet)', qos=1, retain=True).wait_for_publish(3)
        c.disconnect()
        print('\nBeendet. Neue Meldungen werden bis zum nächsten Start beim Broker gesammelt.')


def main():
    p = argparse.ArgumentParser(description='MadridMove Client')
    sub = p.add_subparsers(dest='cmd', required=True)
    s = sub.add_parser('stationen', help='Station suchen')
    s.add_argument('suchtext')
    s.set_defaults(fn=cmd_stationen)
    s = sub.add_parser('route-anlegen', help='Route speichern')
    s.add_argument('nutzer')
    s.add_argument('routenname')
    s.add_argument('--start', required=True, help='ID der Startstation')
    s.add_argument('--ziel', required=True, help='ID der Zielstation')
    s.add_argument('--zeit', required=True, help='Abfahrt, z. B. 08:30')
    s.add_argument('--tage', default='1-5', help='1=Mo ... 7=So, Standard 1-5')
    s.set_defaults(fn=cmd_route_anlegen)
    s = sub.add_parser('routen', help='Routen und Status anzeigen')
    s.add_argument('nutzer')
    s.set_defaults(fn=cmd_routen)
    s = sub.add_parser('route-loeschen', help='Route löschen')
    s.add_argument('nutzer')
    s.add_argument('routen_id')
    s.set_defaults(fn=cmd_route_loeschen)
    s = sub.add_parser('einstellungen', help='Persönliche Grenzen')
    s.add_argument('nutzer')
    s.add_argument('--regen', choices=['kein', 'leicht', 'egal'])
    s.add_argument('--wind', type=int, help='maximaler Wind in km/h')
    s.add_argument('--temp', type=int, nargs=2, metavar=('MIN', 'MAX'), help='Temperaturbereich in °C')
    s.set_defaults(fn=cmd_einstellungen)
    s = sub.add_parser('empfangen', help='Meldungen empfangen (persistente Session)')
    s.add_argument('nutzer')
    s.set_defaults(fn=cmd_empfangen)
    a = p.parse_args()
    a.fn(a)


if __name__ == '__main__':
    main()
