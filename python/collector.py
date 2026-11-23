#!/usr/bin/env python3
"""MadridMove Collector: BiciMAD (GBFS) und Wettervorhersage (Open-Meteo) -> MQTT.

Producer. Zwei eigene MQTT-Verbindungen, je mit Birth, Close und Last Will:
    <prefix>/system/bicimad   "online" / "offline (beendet)" / "offline (Absturz)"
    <prefix>/system/weather   dito
    <prefix>/system/emt       dito (nur, wenn EMT-Zugangsdaten in python/secrets.env stehen)
    <prefix>/system/fahrplan  dito (Fahrplan der EMT-Busse, GTFS)

Veröffentlicht (alle retained):
    <prefix>/static/stations          Stationsliste (QoS 1), stündlich
    <prefix>/live/bicimad/<id>        {bikes, docks, renting, returning, ts} (QoS 0),
                                      nur bei Änderung, spätestens alle 10 min ("Report by Exception")
    <prefix>/live/weather             aktuelles Wetter + Vorhersage für 24 h in 15-min-Schritten (QoS 0), alle 10 min
    <prefix>/live/emt/<nutzer>/<route> nächste Busse an der Haltestelle am Startpunkt der Route (QoS 0), alle 90 s
    <prefix>/plan/<nutzer>/<route>     beste direkte Busverbindung laut Fahrplan zur nächsten Abfahrt (QoS 1)

Hört auf:
    <prefix>/cmd/bicimad/refresh, <prefix>/cmd/weather/refresh   sofort alles neu senden
    <prefix>/sim/state                während einer Simulation keine Live-Werte senden
    <prefix>/profiles/+/+             (EMT, Fahrplan) Start- und Zielpunkte der Routen
    <prefix>/routes/+/+               (Fahrplan) Abfahrtszeiten der Routen

Start:  python3 collector.py        (Strg+C = sauber beenden -> "offline (beendet)")
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

STATUS_EVERY_S = 60            # station_status abrufen (GBFS-ttl ist 15 s, 60 s reicht)
STATIONS_EVERY_S = 3600        # Stationsliste ändert sich selten
WEATHER_EVERY_S = 600          # Wetter alle 10 min
REFRESH_MS = 10 * 60 * 1000    # unveränderte Station spätestens nach 10 min erneut senden
EMT_EVERY_S = 90               # Busankünfte je Route alle 90 s (EMT zählt Anfragen pro Tag)
EMT_MAX_ROUTES = 10            # höchstens so viele Routen abfragen
EMT_RADIUS_M = 400             # Haltestellen im Umkreis des Startpunkts
EMT_MAX_STOPS = 3              # bis zu 3 Haltestellen je Route (nur wenn sie weitere Linien bringen)
CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'stations_cache.json')

sim_active = threading.Event()   # gesetzt, solange eine Simulation läuft
stop = threading.Event()


def log(*args):
    print(time.strftime('%H:%M:%S'), *args, flush=True)


def get_json(url):
    req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode('utf-8'))


class Source:
    """Eine Datenquelle = eine eigene MQTT-Verbindung mit Birth, Close und Last Will."""

    def __init__(self, name, extra_subs=None, on_extra=None):
        self.name = name
        self.extra_subs = extra_subs or []      # zusätzliche Abos (Topic, QoS)
        self.on_extra = on_extra                # Handler für diese Abos
        self.status_topic = f'{PREFIX}/system/{name}'
        self.refresh = threading.Event()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f'{TAG}-col-{name}',
                                  clean_session=True)
        # Last Will: verschickt der Broker, wenn die Verbindung ohne DISCONNECT abreißt
        self.client.will_set(self.status_topic, 'offline (Absturz)', qos=1, retain=True)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.reconnect_delay_set(1, 30)

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log(self.name, 'Verbindung abgelehnt:', reason_code)
            return
        log(self.name, 'verbunden mit', BROKER)
        client.publish(self.status_topic, 'online', qos=1, retain=True)          # Birth
        client.subscribe([(f'{PREFIX}/cmd/{self.name}/refresh', 1), (f'{PREFIX}/sim/state', 1)] + self.extra_subs)
        self.refresh.set()      # nach (Wieder-)Verbindung alles neu senden (falls der Broker retained verloren hat)

    def _on_message(self, client, userdata, msg):
        if self.on_extra and not msg.topic.endswith('/sim/state') and '/cmd/' not in msg.topic:
            self.on_extra(msg)
        elif msg.topic.endswith('/sim/state'):
            try:
                active = bool(json.loads(msg.payload or b'{}').get('active'))
            except ValueError:
                active = False
            if active and not sim_active.is_set():
                log('Simulation aktiv: Live-Werte pausiert')
            if active:
                sim_active.set()
            else:
                sim_active.clear()
        else:
            log(self.name, 'Kommando: neu senden')
            self.refresh.set()

    def start(self):
        self.client.connect_async(BROKER, PORT, keepalive=KEEPALIVE)
        self.client.loop_start()

    def publish(self, topic, payload, qos, retain=True):
        data = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False, separators=(',', ':'))
        self.client.publish(topic, data, qos=qos, retain=retain)

    def close(self):
        # Close-Nachricht: sauberes Beenden ist kein Absturz
        info = self.client.publish(self.status_topic, 'offline (beendet)', qos=1, retain=True)
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
                log('GBFS-Feeds gefunden (' + lang + ')')
            if time.time() - last_info > STATIONS_EVERY_S:
                publish_stations(src, feeds['station_information'])
                last_info = time.time()
            if src.refresh.is_set():
                src.refresh.clear()
                last_sent = {}
            publish_status(src, feeds['station_status'], last_sent)
        except Exception as e:      # Netzwerk, JSON, unerwartetes Format
            log('BiciMAD-Fehler:', e)
            if last_info == 0 and os.path.exists(CACHE_FILE):
                # Kein Internet: letzte bekannte Stationsliste veröffentlichen (für Simulator/Demo)
                with open(CACHE_FILE, encoding='utf-8') as f:
                    src.publish(f'{PREFIX}/static/stations', json.load(f), qos=1)
                last_info = time.time() - STATIONS_EVERY_S + 300   # in 5 min erneut versuchen
                log('Stationsliste aus Cache veröffentlicht')
        src.refresh.wait(STATUS_EVERY_S)


def publish_stations(src, url):
    raw = get_json(url)['data']['stations']
    stations = sorted(({'id': str(s['station_id']), 'name': s['name'], 'lat': s['lat'], 'lon': s['lon'],
                        'capacity': s.get('capacity')} for s in raw), key=lambda s: s['name'])
    src.publish(f'{PREFIX}/static/stations', stations, qos=1)
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(stations, f, ensure_ascii=False)
    log(len(stations), 'Stationen veröffentlicht')


def publish_status(src, url, last_sent):
    raw = get_json(url)['data']['stations']
    if sim_active.is_set():
        src.publish(src.status_topic, 'online', qos=1)      # nur Heartbeat
        return
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
    # Heartbeat: korrigiert einen veralteten Status, falls eine alte Will-Nachricht nachkommt
    src.publish(src.status_topic, 'online', qos=1)
    log(f'BiciMAD: {sent}/{len(raw)} Stationen gesendet')


# ---------------------------------------------------------------- Wetter

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
            if sim_active.is_set():
                src.publish(src.status_topic, 'online', qos=1)
            else:
                src.publish(f'{PREFIX}/live/weather', w, qos=0)
                src.publish(src.status_topic, 'online', qos=1)
                log(f"Wetter: {w['temp']} °C, {w['precip']} mm, {w['wind']} km/h, {len(forecast)} Vorhersagewerte")
        except Exception as e:
            log('Wetter-Fehler:', e)
        src.refresh.wait(WEATHER_EVERY_S)


# ---------------------------------------------------------------- EMT (Bus)

class EmtRoutes:
    """Merkt sich die Startpunkte aller Routen (aus den retained Profilen)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.routes = {}            # 'nutzer/route' -> (lat, lon)
        self.removed = set()        # gelöschte Routen: retained Busdaten ebenfalls löschen
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
    nearest = {}                    # (lat, lon) -> Haltestellen (Cache, ändert sich nicht)
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
                    # nächste Haltestellen; eine weitere nur, wenn sie zusätzliche Linien bringt
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
                        log(f'EMT: keine Bushaltestelle im Umkreis von {EMT_RADIUS_M} m für {key}')
                        warned.add(key)
                    continue
                arr = []
                for s in stops:
                    for a in emt.arrivals(s['id']):
                        arr.append(dict(a, stop=s['name'], distM=s['distM']))
                arr = sorted(arr, key=lambda a: a['min'])[:8]
                if not arr and key not in warned:
                    log(f"EMT: {', '.join(s['name'] for s in stops)} melden gerade keine Busse ({key})")
                    warned.add(key)
                elif arr:
                    warned.discard(key)
                s0 = stops[0]
                src.publish(f'{PREFIX}/live/emt/{key}', {
                    'stop': {'id': s0['id'], 'name': s0['name'], 'distM': s0['distM']},
                    'stops': [{'id': s['id'], 'name': s['name'], 'distM': s['distM'], 'lines': s['lines']} for s in stops],
                    'arrivals': arr, 'ts': int(time.time() * 1000)}, qos=0)
                ok += 1
            except Exception as e:      # Netzwerk, Token, unerwartetes Format
                log('EMT-Fehler', key + ':', e)
        src.publish(src.status_topic, 'online', qos=1)
        if todo:
            log(f'EMT: Busse für {ok}/{len(todo)} Routen gesendet')
        # neue Route -> sofort abfragen, sonst alle EMT_EVERY_S Sekunden
        routes.changed.wait(EMT_EVERY_S)


# ---------------------------------------------------------------- Fahrplan (GTFS)

class RouteBook:
    """Start-/Zielpunkte (aus den Profilen) und Abfahrtszeiten (aus den Routen) aller Routen."""

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


def fahrplan_loop(src, book):
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
                    src.publish(src.status_topic, 'fehlt: keine Fahrplandatei (siehe Log)', qos=1)
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
                res.update({'depAt': int(when.timestamp() * 1000), 'depText': when.strftime('%d.%m. %H:%M'),
                            'forDeparture': dep is not None})
                sig = json.dumps(res, sort_keys=True)
                if last.get(key) != sig:          # nur bei Änderung senden
                    last[key] = sig
                    src.publish(f'{PREFIX}/plan/{key}', dict(res, ts=int(time.time() * 1000)), qos=1)
                    log(f"Fahrplan {key}: " + (f"Linie {res['line']} ab {res['board']['name']}, "
                        f"{res['totalMin']} min" if res['status'] == 'ok' else res['status']))
            src.publish(src.status_topic, 'online', qos=1)
        except Exception as e:      # unerwartetes Dateiformat usw.
            log('Fahrplan-Fehler:', e)
            src.publish(src.status_topic, f'fehlt: {e}'[:120], qos=1)
        book.changed.wait(60)


def main():
    bicimad, weather = Source('bicimad'), Source('weather')
    sources = [bicimad, weather]
    emt_on = False
    threads = [threading.Thread(target=bicimad_loop, args=(bicimad,), daemon=True),
               threading.Thread(target=weather_loop, args=(weather,), daemon=True)]
    # Fahrplan (GTFS): ohne Konto, Datei wird automatisch geladen
    book = RouteBook()
    plan_src = Source('fahrplan', [(f'{PREFIX}/profiles/+/+', 1), (f'{PREFIX}/routes/+/+', 1)], book.on_message)
    sources.append(plan_src)
    threads.append(threading.Thread(target=fahrplan_loop, args=(plan_src, book), daemon=True))
    # EMT nur mit Zugangsdaten (python/secrets.env oder Umgebungsvariablen)
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
            log('EMT: keine Zugangsdaten (python/secrets.env) – Busankünfte aus, ÖPNV wird geschätzt')
    except Exception as e:
        log('EMT deaktiviert:', e)
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

    time.sleep(1)   # Verbindungen aufbauen lassen
    if not emt_on:
        # ohne EMT: alten retained Status löschen (Dashboard zeigt "nicht eingerichtet")
        bicimad.publish(f'{PREFIX}/system/emt', '', qos=1)
    for t in threads:
        t.start()
    log('Collector läuft (Strg+C zum Beenden)')
    while not stop.is_set():
        stop.wait(1)
    log('Beende sauber ...')
    for s in sources:
        s.close()
    sys.exit(0)


if __name__ == '__main__':
    main()
