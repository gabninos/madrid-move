#!/usr/bin/env python3
"""MadridMove client: the user's app (producer and consumer).

Commands:
    python3 client.py stations <search>
        Find a station (ID for routes), e.g.  stations sol
    python3 client.py add-route <user> <route-name> --from <ID> --to <ID> --time 08:30 [--days 1-5]
        Save a route (retained, QoS 1). Days: 1=Mon ... 7=Sun, e.g. 1-5 or 1,3,5
    python3 client.py routes <user>
        Show your routes and their current status
    python3 client.py delete-route <user> <route-id>
        Delete a route (empty retained message)
    python3 client.py settings <user> [--rain none|light|any] [--wind 25] [--temp 8 30]
        Set or show (without options) your personal limits
    python3 client.py listen <user>
        Receive notifications (pre-departure info, warning, all-clear). Persistent session:
        notifications that arrive while the client is off are delivered on the next start.
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

DEFAULTS = {'rain': 'none', 'windMax': 25, 'tempMin': 8, 'tempMax': 30}   # "cautious"
RAIN_OLD = {'kein': 'none', 'leicht': 'light', 'egal': 'any'}            # values saved by older versions
RAIN_TEXT = {'none': 'no rain', 'light': 'light rain ok', 'any': 'rain does not matter'}


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
    """Reads retained messages for a topic (wildcards allowed): {topic: payload}."""
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
    """Publishes with QoS 1 and waits for the broker's acknowledgement. payload None = empty message."""
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
        sys.exit('No station list on the broker. Is the collector running?')
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
        sys.exit('Please give days as 1-7, e.g. 1-5 or 1,3,5')
    return sorted(days)


# ---------------------------------------------------------------- commands

def cmd_stations(a):
    q = norm(a.search)
    hits = [s for s in stations().values() if q in norm(s['name']) or q == s['id']]
    for s in sorted(hits, key=lambda s: s['name'])[:25]:
        print(f"{s['id']:>6}  {s['name']}  ({s.get('capacity') or '?'} docks)")
    if not hits:
        print('No station found.')


def cmd_add_route(a):
    st = stations()
    for sid in (a.start, a.to):
        if sid not in st:
            sys.exit(f'Unknown station {sid}. Search with: python3 client.py stations <name>')
    if a.start == a.to:
        sys.exit('Start and destination are the same.')
    if not re.fullmatch(r'\d{1,2}:\d{2}', a.time):
        sys.exit('Please give the time as HH:MM, e.g. 08:30')
    user, rid = slug(a.user), slug(a.route_name)
    route = {'name': a.route_name,
             'from': {'id': a.start, 'label': st[a.start]['name']},
             'to': {'id': a.to, 'label': st[a.to]['name']},
             'departure': {'time': a.time.zfill(5), 'days': parse_days(a.days)},
             'createdAt': datetime.now().isoformat(timespec='seconds')}
    publish(f'{PREFIX}/routes/{user}/{rid}', route)
    print(f"Route {user}/{rid} saved: {route['from']['label']} -> {route['to']['label']}, "
          f"{route['departure']['time']}, days {route['departure']['days']}")


def cmd_routes(a):
    user = slug(a.user)
    routes = read_retained(f'{PREFIX}/routes/{user}/+')
    recos = read_retained(f'{PREFIX}/reco/{user}/+')
    if not routes:
        print('No routes saved.')
    for topic, r in sorted(routes.items()):
        rid = topic.split('/')[-1]
        reco = recos.get(f'{PREFIX}/reco/{user}/{rid}') or {}
        print(f"{rid}: {r['from']['label']} -> {r['to']['label']}, {r['departure']['time']}")
        if reco:
            print(f"    {reco.get('headline', '')}  {reco.get('detail', '')}")


def cmd_delete_route(a):
    publish(f'{PREFIX}/routes/{slug(a.user)}/{a.route_id}', None)
    print('Route deleted.')


def cmd_settings(a):
    user = slug(a.user)
    topic = f'{PREFIX}/users/{user}/settings'
    cur = dict(DEFAULTS)
    cur.update(next(iter(read_retained(topic).values()), {}) or {})
    cur['rain'] = RAIN_OLD.get(cur['rain'], cur['rain'])
    changed = False
    if a.rain:
        cur['rain'], changed = a.rain, True
    if a.wind is not None:
        cur['windMax'], changed = a.wind, True
    if a.temp:
        cur['tempMin'], cur['tempMax'], changed = a.temp[0], a.temp[1], True
    if changed:
        publish(topic, cur)
        print('Saved:', end=' ')
    print(f"{RAIN_TEXT.get(cur['rain'], cur['rain'])}, wind up to {cur['windMax']} km/h, "
          f"{cur['tempMin']} to {cur['tempMax']} °C")


def cmd_listen(a):
    user = slug(a.user)
    status = f'{PREFIX}/system/client-{user}'
    # Fixed client ID + clean_session=False = persistent session on the broker
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f'{TAG}-client-{user}', clean_session=False)
    c.will_set(status, 'offline (crashed)', qos=1, retain=True)

    def on_connect(cl, ud, flags, rc, props):
        resumed = ' (session resumed)' if flags.session_present else ''
        print(f'Connected to {BROKER}{resumed}. Waiting for notifications for "{user}" ... (Ctrl+C to quit)')
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
        c.publish(status, 'offline (stopped)', qos=1, retain=True).wait_for_publish(3)
        c.disconnect()
        print('\nStopped. New notifications are kept by the broker until the next start.')


def main():
    p = argparse.ArgumentParser(description='MadridMove client')
    sub = p.add_subparsers(dest='cmd', required=True)
    s = sub.add_parser('stations', help='find a station')
    s.add_argument('search')
    s.set_defaults(fn=cmd_stations)
    s = sub.add_parser('add-route', help='save a route')
    s.add_argument('user')
    s.add_argument('route_name', metavar='route-name')
    s.add_argument('--from', dest='start', required=True, help='ID of the start station')
    s.add_argument('--to', required=True, help='ID of the destination station')
    s.add_argument('--time', required=True, help='departure, e.g. 08:30')
    s.add_argument('--days', default='1-5', help='1=Mon ... 7=Sun, default 1-5')
    s.set_defaults(fn=cmd_add_route)
    s = sub.add_parser('routes', help='show routes and status')
    s.add_argument('user')
    s.set_defaults(fn=cmd_routes)
    s = sub.add_parser('delete-route', help='delete a route')
    s.add_argument('user')
    s.add_argument('route_id', metavar='route-id')
    s.set_defaults(fn=cmd_delete_route)
    s = sub.add_parser('settings', help='personal limits')
    s.add_argument('user')
    s.add_argument('--rain', choices=['none', 'light', 'any'])
    s.add_argument('--wind', type=int, help='maximum wind in km/h')
    s.add_argument('--temp', type=int, nargs=2, metavar=('MIN', 'MAX'), help='temperature range in °C')
    s.set_defaults(fn=cmd_settings)
    s = sub.add_parser('listen', help='receive notifications (persistent session)')
    s.add_argument('user')
    s.set_defaults(fn=cmd_listen)
    a = p.parse_args()
    a.fn(a)


if __name__ == '__main__':
    main()
