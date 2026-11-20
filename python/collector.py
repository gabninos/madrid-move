#!/usr/bin/env python3
"""MadridMove Collector: BiciMAD (GBFS) und Wettervorhersage (Open-Meteo) -> MQTT.

Producer. Zwei eigene MQTT-Verbindungen, je mit Birth, Close und Last Will:
    <prefix>/system/bicimad   "online" / "offline (beendet)" / "offline (Absturz)"
    <prefix>/system/weather   dito

Veröffentlicht (alle retained):
    <prefix>/static/stations          Stationsliste (QoS 1), stündlich
    <prefix>/live/bicimad/<id>        {bikes, docks, renting, returning, ts} (QoS 0),
                                      nur bei Änderung, spätestens alle 10 min ("Report by Exception")
    <prefix>/live/weather             aktuelles Wetter + Vorhersage für 24 h in 15-min-Schritten (QoS 0), alle 10 min

Hört auf:
    <prefix>/cmd/bicimad/refresh, <prefix>/cmd/weather/refresh   sofort alles neu senden
    <prefix>/sim/state                während einer Simulation keine Live-Werte senden

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

    def __init__(self, name):
        self.name = name
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
        client.subscribe([(f'{PREFIX}/cmd/{self.name}/refresh', 1), (f'{PREFIX}/sim/state', 1)])

    def _on_message(self, client, userdata, msg):
        if msg.topic.endswith('/sim/state'):
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


def main():
    bicimad, weather = Source('bicimad'), Source('weather')
    for s in (bicimad, weather):
        s.start()

    def shutdown(*_):
        stop.set()
        bicimad.refresh.set()
        weather.refresh.set()
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    time.sleep(1)   # Verbindungen aufbauen lassen
    threads = [threading.Thread(target=bicimad_loop, args=(bicimad,), daemon=True),
               threading.Thread(target=weather_loop, args=(weather,), daemon=True)]
    for t in threads:
        t.start()
    log('Collector läuft (Strg+C zum Beenden)')
    while not stop.is_set():
        stop.wait(1)
    log('Beende sauber ...')
    for s in (bicimad, weather):
        s.close()
    sys.exit(0)


if __name__ == '__main__':
    main()
