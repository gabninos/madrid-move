#!/usr/bin/env python3
"""MadridMove collector: BiciMAD (GBFS), weather forecast (Open-Meteo) and EMT bus data -> MQTT.

Producer. One MQTT connection per data source, each with birth, close and Last Will message:
    <prefix>/system/bicimad    "online" / "offline (stopped)" / "offline (crashed)"
    <prefix>/system/weather    same
    <prefix>/system/timetable  same (EMT bus timetable, GTFS)
    <prefix>/system/emt        same (only if EMT credentials are in python/secrets.env)

Publishes (all retained):
    <prefix>/static/stations           station list (QoS 1), hourly
    <prefix>/live/bicimad/<id>         {bikes, docks, renting, returning, ts} (QoS 0),
                                       only on change, at the latest every 10 min ("report by exception")
    <prefix>/live/weather              current weather + 24 h forecast in 15-min steps (QoS 0), every 10 min
    <prefix>/live/emt/<user>/<route>   next buses at the stop near the start of the route (QoS 0), every 90 s
    <prefix>/plan/<user>/<route>       best direct bus connection by timetable for the next departure (QoS 1)

Subscribes to:
    <prefix>/profiles/+/+              (EMT, timetable) start and end points of the routes
    <prefix>/routes/+/+                (timetable) departure times of the routes

Start:  python3 collector.py        (Ctrl+C = clean shutdown -> "offline (stopped)")
"""
import json
import os
import signal
import sys
import threading
import time
import urllib.request

import paho.mqtt.client as mqtt

from config import BROKER, GBFS_URL, KEEPALIVE, METEO_URL, PORT, PREFIX, TAG, USER_AGENT, WEATHER_POINT

STATUS_EVERY_S = 60            # fetch station_status (GBFS ttl is 15 s, 60 s is enough)
STATIONS_EVERY_S = 3600        # the station list rarely changes
WEATHER_EVERY_S = 600          # weather every 10 min
REFRESH_MS = 10 * 60 * 1000    # resend an unchanged station after 10 min at the latest
EMT_EVERY_S = 90               # bus arrivals per route every 90 s (EMT counts requests per day)
EMT_MAX_ROUTES = 10            # query at most this many routes
EMT_RADIUS_M = 400             # stops within this radius of the start point
EMT_MAX_STOPS = 3              # up to 3 stops per route (only if they add more lines)
CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'stations_cache.json')
# retained topics of older versions, cleared on start
LEGACY_TOPICS = ['system/fahrplan', 'system/sim', 'sim/state']

stop = threading.Event()


def log(*args):
    print(time.strftime('%H:%M:%S'), *args, flush=True)


def get_json(url):
    req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode('utf-8'))


class Source:
    """One data source = its own MQTT connection with birth, close and Last Will message."""

    def __init__(self, name, extra_subs=None, on_extra=None):
        self.name = name
        self.extra_subs = extra_subs or []      # additional subscriptions (topic, QoS)
        self.on_extra = on_extra                # handler for these subscriptions
        self.status_topic = f'{PREFIX}/system/{name}'
        self.refresh = threading.Event()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f'{TAG}-col-{name}',
                                  clean_session=True)
        # Last Will: sent by the broker if the connection drops without DISCONNECT
        self.client.will_set(self.status_topic, 'offline (crashed)', qos=1, retain=True)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.reconnect_delay_set(1, 30)

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log(self.name, 'connection refused:', reason_code)
            return
        log(self.name, 'connected to', BROKER)
        client.publish(self.status_topic, 'online', qos=1, retain=True)          # Birth
        if self.extra_subs:
            client.subscribe(self.extra_subs)
        self.refresh.set()      # after (re)connecting, resend everything (in case the broker lost retained messages)

    def _on_message(self, client, userdata, msg):
        if self.on_extra:
            self.on_extra(msg)

    def start(self):
        self.client.connect_async(BROKER, PORT, keepalive=KEEPALIVE)
        self.client.loop_start()

    def publish(self, topic, payload, qos, retain=True):
        data = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False, separators=(',', ':'))
        self.client.publish(topic, data, qos=qos, retain=retain)

    def close(self):
        # close message: a clean shutdown is not a crash
        info = self.client.publish(self.status_topic, 'offline (stopped)', qos=1, retain=True)
        try:
            info.wait_for_publish(3)
        except (RuntimeError, ValueError):
            pass
        self.client.disconnect()
        self.client.loop_stop()


# ---------------------------------------------------------------- BiciMAD

def bicimad_loop(src):
    feeds, last_sent, last_info = None, {}, 0.0
    while not stop.is_set():
        try:
            if feeds is None:
                data = get_json(GBFS_URL)['data']
                lang = 'es' if 'es' in data else ('en' if 'en' in data else next(iter(data)))
                feeds = {f['name']: f['url'] for f in data[lang]['feeds']}
                log('GBFS feeds found (' + lang + ')')
            if time.time() - last_info > STATIONS_EVERY_S:
                publish_stations(src, feeds['station_information'])
                last_info = time.time()
            if src.refresh.is_set():
                src.refresh.clear()
                last_sent = {}
            publish_status(src, feeds['station_status'], last_sent)
        except Exception as e:      # network, JSON, unexpected format
            log('BiciMAD error:', e)
            if last_info == 0 and os.path.exists(CACHE_FILE):
                # no internet: publish the last known station list (demo without internet)
                with open(CACHE_FILE, encoding='utf-8') as f:
                    src.publish(f'{PREFIX}/static/stations', json.load(f), qos=1)
                last_info = time.time() - STATIONS_EVERY_S + 300   # try again in 5 min
                log('station list published from cache')
        src.refresh.wait(STATUS_EVERY_S)


def publish_stations(src, url):
    raw = get_json(url)['data']['stations']
    stations = sorted(({'id': str(s['station_id']), 'name': s['name'], 'lat': s['lat'], 'lon': s['lon'],
                        'capacity': s.get('capacity')} for s in raw), key=lambda s: s['name'])
    src.publish(f'{PREFIX}/static/stations', stations, qos=1)
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(stations, f, ensure_ascii=False)
    log(len(stations), 'stations published')


def publish_status(src, url, last_sent):
    raw = get_json(url)['data']['stations']
    now = int(time.time() * 1000)
    sent = 0
    for s in raw:
        sid = str(s['station_id'])
        v = {'bikes': s.get('num_bikes_available', 0), 'docks': s.get('num_docks_available', 0),
             'renting': s.get('is_renting', 1) not in (0, False),
             'returning': s.get('is_returning', 1) not in (0, False)}
        key = (v['bikes'], v['docks'], v['renting'], v['returning'])
        prev = last_sent.get(sid)
        if prev and prev[0] == key and now - prev[1] < REFRESH_MS:
            continue                                          # Report by Exception
        last_sent[sid] = (key, now)
        v['ts'] = now
        src.publish(f'{PREFIX}/live/bicimad/{sid}', v, qos=0)
        sent += 1
    # heartbeat: corrects an outdated status if an old will message arrives late
    src.publish(src.status_topic, 'online', qos=1)
    log(f'BiciMAD: {sent}/{len(raw)} stations sent')


# ---------------------------------------------------------------- weather

def weather_loop(src):
    lat, lon = WEATHER_POINT
    url = (f'{METEO_URL}?latitude={lat}&longitude={lon}'
           '&current=temperature_2m,precipitation,wind_speed_10m,weather_code'
           '&minutely_15=temperature_2m,precipitation,wind_speed_10m'
           '&forecast_minutely_15=96&timeformat=unixtime&timezone=UTC')
    while not stop.is_set():
        src.refresh.clear()
        try:
            d = get_json(url)
            c = d['current']
            m = d.get('minutely_15') or {}
            forecast = [{'t': t * 1000, 'temp': temp, 'precip': p, 'wind': w}
                        for t, temp, p, w in zip(m.get('time', []), m.get('temperature_2m', []),
                                                 m.get('precipitation', []), m.get('wind_speed_10m', []))]
            w = {'temp': c['temperature_2m'], 'precip': c['precipitation'], 'wind': c['wind_speed_10m'],
                 'code': c.get('weather_code'), 'ts': int(time.time() * 1000),
                 'forecast': forecast, 'source': 'open-meteo'}
            src.publish(f'{PREFIX}/live/weather', w, qos=0)
            src.publish(src.status_topic, 'online', qos=1)
            log(f"Weather: {w['temp']} °C, {w['precip']} mm, {w['wind']} km/h, {len(forecast)} forecast values")
        except Exception as e:
            log('Weather error:', e)
        src.refresh.wait(WEATHER_EVERY_S)


# ---------------------------------------------------------------- EMT (Bus)

class EmtRoutes:
    """Keeps the start points of all routes (from the retained profiles)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.routes = {}            # 'user/route' -> (lat, lon)
        self.removed = set()        # deleted routes: delete their retained bus data too
        self.changed = threading.Event()

    def on_message(self, msg):
        key = '/'.join(msg.topic.split('/')[-2:])
        point = None
        if msg.payload:
            try:
                p = json.loads(msg.payload)
                pt = p.get('fromPoint') or p.get('from') or {}
                if pt.get('lat') is not None and pt.get('lon') is not None:
                    point = (round(float(pt['lat']), 5), round(float(pt['lon']), 5))
            except (ValueError, AttributeError, TypeError):
                pass
        with self.lock:
            if point:
                if self.routes.get(key) != point:
                    self.routes[key] = point
                    self.changed.set()
                self.removed.discard(key)
            elif key in self.routes:
                del self.routes[key]
                self.removed.add(key)
                self.changed.set()


def emt_loop(src, emt, routes):
    nearest = {}                    # (lat, lon) -> stops (cache, does not change)
    warned = set()
    while not stop.is_set():
        routes.changed.clear()
        with routes.lock:
            todo = sorted(routes.routes.items())[:EMT_MAX_ROUTES]
            removed, routes.removed = routes.removed, set()
        for key in removed:
            src.publish(f'{PREFIX}/live/emt/{key}', '', qos=0)
        ok = 0
        for key, (lat, lon) in todo:
            try:
                if (lat, lon) not in nearest:
                    # nearest stops; another one only if it adds more lines
                    chosen, lines = [], set()
                    for s in emt.stops_around(lat, lon, EMT_RADIUS_M):
                        if not chosen or not set(s['lines']) <= lines:
                            chosen.append(s)
                            lines |= set(s['lines'])
                        if len(chosen) >= EMT_MAX_STOPS:
                            break
                    nearest[(lat, lon)] = chosen
                stops = nearest[(lat, lon)]
                if not stops:
                    if key not in warned:
                        log(f'EMT: no bus stop within {EMT_RADIUS_M} m for {key}')
                        warned.add(key)
                    continue
                arr = []
                for s in stops:
                    for a in emt.arrivals(s['id']):
                        arr.append(dict(a, stop=s['name'], distM=s['distM']))
                arr = sorted(arr, key=lambda a: a['min'])[:8]
                if not arr and key not in warned:
                    log(f"EMT: {', '.join(s['name'] for s in stops)} currently report no buses ({key})")
                    warned.add(key)
                elif arr:
                    warned.discard(key)
                s0 = stops[0]
                src.publish(f'{PREFIX}/live/emt/{key}', {
                    'stop': {'id': s0['id'], 'name': s0['name'], 'distM': s0['distM']},
                    'stops': [{'id': s['id'], 'name': s['name'], 'distM': s['distM'], 'lines': s['lines']} for s in stops],
                    'arrivals': arr, 'ts': int(time.time() * 1000)}, qos=0)
                ok += 1
            except Exception as e:      # network, token, unexpected format
                log('EMT error', key + ':', e)
        src.publish(src.status_topic, 'online', qos=1)
        if todo:
            log(f'EMT: buses sent for {ok}/{len(todo)} routes')
        # new route -> query at once, otherwise every EMT_EVERY_S seconds
        routes.changed.wait(EMT_EVERY_S)


# ---------------------------------------------------------------- timetable (GTFS)

class RouteBook:
    """Start/end points (from the profiles) and departure times (from the routes) of all routes."""

    def __init__(self):
        self.lock = threading.Lock()
        self.points, self.deps = {}, {}
        self.removed = set()
        self.changed = threading.Event()

    def on_message(self, msg):
        parts = msg.topic.split('/')
        key = '/'.join(parts[-2:])
        try:
            p = json.loads(msg.payload) if msg.payload else None
        except ValueError:
            p = None
        with self.lock:
            if parts[-3] == 'profiles':
                pt = None
                if isinstance(p, dict):
                    a, b = p.get('fromPoint') or p.get('from') or {}, p.get('toPoint') or p.get('to') or {}
                    try:
                        pt = (float(a['lat']), float(a['lon']), float(b['lat']), float(b['lon']))
                    except (KeyError, TypeError, ValueError):
                        pt = None
                if pt:
                    self.points[key] = tuple(round(x, 5) for x in pt)
                    self.removed.discard(key)
                elif key in self.points:
                    del self.points[key]
                    self.removed.add(key)
            elif parts[-3] == 'routes':
                if isinstance(p, dict) and isinstance(p.get('departure'), dict):
                    self.deps[key] = p['departure']
                else:
                    self.deps.pop(key, None)
        self.changed.set()


def timetable_loop(src, book):
    import gtfs_plan
    tt, loaded_at, last = None, 0.0, {}
    while not stop.is_set():
        book.changed.clear()
        try:
            if tt is None or time.time() - loaded_at > 86400:
                if gtfs_plan.download(log=log):
                    tt = gtfs_plan.Timetable(log=log)
                    loaded_at = time.time()
                    last = {}
                elif tt is None:
                    src.publish(src.status_topic, 'missing: no timetable file (see log)', qos=1)
                    stop.wait(600)
                    continue
            with book.lock:
                points = dict(book.points)
                deps = dict(book.deps)
                removed, book.removed = book.removed, set()
            for key in removed:
                src.publish(f'{PREFIX}/plan/{key}', '', qos=1)
                last.pop(key, None)
            tt.prepare(points)
            for key in points:
                dep = gtfs_plan.next_departure(deps[key]) if key in deps else None
                when = dep or gtfs_plan.now_local().replace(second=0, microsecond=0)
                res = tt.best(key, when)
                res.update({'depAt': int(when.timestamp() * 1000), 'depText': when.strftime('%Y-%m-%d %H:%M'),
                            'forDeparture': dep is not None})
                sig = json.dumps(res, sort_keys=True)
                if last.get(key) != sig:          # send only on change
                    last[key] = sig
                    src.publish(f'{PREFIX}/plan/{key}', dict(res, ts=int(time.time() * 1000)), qos=1)
                    log(f"Timetable {key}: " + (f"line {res['line']} from {res['board']['name']}, "
                        f"{res['totalMin']} min" if res['status'] == 'ok' else res['status']))
            src.publish(src.status_topic, 'online', qos=1)
        except Exception as e:      # unexpected file format etc.
            log('Timetable error:', e)
            src.publish(src.status_topic, f'missing: {e}'[:120], qos=1)
        book.changed.wait(60)


def main():
    bicimad, weather = Source('bicimad'), Source('weather')
    sources = [bicimad, weather]
    emt_on = False
    threads = [threading.Thread(target=bicimad_loop, args=(bicimad,), daemon=True),
               threading.Thread(target=weather_loop, args=(weather,), daemon=True)]
    # timetable (GTFS): no account needed, the file is downloaded automatically
    book = RouteBook()
    plan_src = Source('timetable', [(f'{PREFIX}/profiles/+/+', 1), (f'{PREFIX}/routes/+/+', 1)], book.on_message)
    sources.append(plan_src)
    threads.append(threading.Thread(target=timetable_loop, args=(plan_src, book), daemon=True))
    # EMT only with credentials (python/secrets.env or environment variables)
    try:
        import emt as emt_api
        creds = emt_api.load_credentials()
        if creds:
            client = emt_api.EmtClient(creds, log=log)
            routes = EmtRoutes()
            emt_src = Source('emt', [(f'{PREFIX}/profiles/+/+', 1)], routes.on_message)
            sources.append(emt_src)
            threads.append(threading.Thread(target=emt_loop, args=(emt_src, client, routes), daemon=True))
            emt_on = True
        else:
            log('EMT: no credentials (python/secrets.env) – live bus arrivals off, timetable only')
    except Exception as e:
        log('EMT disabled:', e)
    for s in sources:
        s.start()

    def shutdown(*_):
        stop.set()
        for s in sources:
            s.refresh.set()
        book.changed.set()
        if emt_on:
            routes.changed.set()
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    time.sleep(1)   # let the connections come up
    if not emt_on:
        # without EMT: clear the old retained status (dashboard shows "not set up")
        bicimad.publish(f'{PREFIX}/system/emt', '', qos=1)
    for t in LEGACY_TOPICS:
        bicimad.publish(f'{PREFIX}/{t}', '', qos=1)
    for t in threads:
        t.start()
    log('Collector running (Ctrl+C to quit)')
    while not stop.is_set():
        stop.wait(1)
    log('Shutting down cleanly ...')
    for s in sources:
        s.close()
    sys.exit(0)


if __name__ == '__main__':
    main()
