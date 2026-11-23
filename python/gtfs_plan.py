#!/usr/bin/env python3
"""Fahrplan der EMT-Busse (GTFS) -> direkte Busverbindung für eine Route.

GTFS ("General Transit Feed Specification") ist das Standardformat für Fahrpläne: eine ZIP-Datei
mit Textdateien (CSV). Gebraucht werden:
    stops.txt          Haltestellen mit Lage
    routes.txt         Linien (z. B. "27")
    trips.txt          einzelne Fahrten einer Linie (mit Betriebstagen = service_id)
    stop_times.txt     wann eine Fahrt an welcher Haltestelle ist
    frequencies.txt    (optional) Takt statt Uhrzeiten: "7-10 Uhr alle 8 min"
    calendar.txt, calendar_dates.txt   an welchen Tagen eine service_id fährt

Gesucht wird nur die beste DIREKTE Verbindung (ohne Umsteigen):
    Fußweg zu einer Haltestelle nahe dem Start -> Warten -> Busfahrt -> Fußweg zum Ziel.

Die ZIP-Datei wird automatisch geladen (wöchentlich erneuert) nach python/gtfs/transitemt.zip.
Klappt der Download nicht (Firewall, Port 8443), die Datei im Browser laden und dort ablegen:
    https://servicios.emtmadrid.es:8443/gtfs/transitemt.zip

Testen (ohne MQTT):
    python gtfs_plan.py test 40.4169 -3.7035 40.4437 -3.7276 08:30
        Start-Lat Start-Lon Ziel-Lat Ziel-Lon Abfahrt (heute bzw. nächster Tag)
"""
import csv
import io
import math
import os
import sys
import time
import urllib.request
import zipfile
from datetime import date, datetime, timedelta

try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo('Europe/Madrid')       # unter Windows braucht das das Paket "tzdata"
except Exception:                        # noqa: BLE001
    TZ = None

HERE = os.path.dirname(os.path.abspath(__file__))
GTFS_URL = os.environ.get('GTFS_EMT_URL', 'https://servicios.emtmadrid.es:8443/gtfs/transitemt.zip')
GTFS_FILE = os.environ.get('GTFS_EMT_FILE', os.path.join(HERE, 'gtfs', 'transitemt.zip'))
MAX_AGE_S = 7 * 86400          # Fahrplandatei wöchentlich neu laden
RADIUS_M = 600                 # Haltestellen bis so weit (Luftlinie) vom Start bzw. Ziel
DETOUR = 1.3                   # Luftlinie -> Fußweg
WALK_M_PER_MIN = 80            # ca. 4,8 km/h
MAX_WAIT_MIN = 60              # länger warten gilt als "kein Bus zu dieser Zeit"


def now_local():
    return datetime.now(TZ) if TZ else datetime.now()


def dist_m(lat1, lon1, lat2, lon2):
    kx = 111320 * math.cos(math.radians(lat1))
    return math.hypot((lon2 - lon1) * kx, (lat2 - lat1) * 110540)


def walk_min(meters):
    return math.ceil(meters * DETOUR / WALK_M_PER_MIN)


def secs(hms):
    """'25:10:00' -> Sekunden ab Mitternacht des Betriebstags (GTFS erlaubt Zeiten über 24 h)."""
    h, m, s = (hms.strip().split(':') + ['0', '0'])[:3]
    return int(h) * 3600 + int(m) * 60 + int(s)


def hm(total_secs):
    total_secs %= 86400
    return f'{total_secs // 3600:02d}:{(total_secs % 3600) // 60:02d}'


def download(log=print, force=False):
    """Lädt die Fahrplandatei, wenn sie fehlt oder älter als eine Woche ist. True = Datei vorhanden."""
    fresh = os.path.exists(GTFS_FILE) and time.time() - os.path.getmtime(GTFS_FILE) < MAX_AGE_S
    if fresh and not force:
        return True
    os.makedirs(os.path.dirname(GTFS_FILE), exist_ok=True)
    tmp = GTFS_FILE + '.part'
    try:
        log('Fahrplan: lade', GTFS_URL)
        req = urllib.request.Request(GTFS_URL, headers={'User-Agent': 'MadridMove/2.0 (UPM DSIoT student project)'})
        with urllib.request.urlopen(req, timeout=120) as r, open(tmp, 'wb') as f:
            while True:
                chunk = r.read(1 << 20)
                if not chunk:
                    break
                f.write(chunk)
        zipfile.ZipFile(tmp).testzip()
        os.replace(tmp, GTFS_FILE)
        log(f'Fahrplan: {os.path.getsize(GTFS_FILE) // 1024} KB geladen')
        return True
    except Exception as e:                # noqa: BLE001
        if os.path.exists(tmp):
            os.remove(tmp)
        if os.path.exists(GTFS_FILE):
            log('Fahrplan: Download fehlgeschlagen, nutze vorhandene Datei:', e)
            return True
        log('Fahrplan: Download fehlgeschlagen:', e)
        log('  -> Datei im Browser laden und ablegen unter', GTFS_FILE)
        return False


def _rows(zf, name):
    """Liest eine GTFS-Datei zeilenweise als dict (ohne alles in den Speicher zu laden)."""
    match = [n for n in zf.namelist() if n.split('/')[-1] == name]
    if not match:
        return
    with zf.open(match[0]) as raw:
        for row in csv.DictReader(io.TextIOWrapper(raw, encoding='utf-8-sig', newline='')):
            yield {k.strip(): (v or '').strip() for k, v in row.items() if k}


class Timetable:
    """Fahrplan im Speicher: Haltestellen, Linien, Fahrten, Betriebstage. stop_times wird nur
    für die Haltestellen nahe den Routen gelesen (sonst wären es Millionen Zeilen)."""

    def __init__(self, path=GTFS_FILE, log=print):
        self.path, self.log = path, log
        t0 = time.time()
        with zipfile.ZipFile(path) as zf:
            self.stops = {}
            for r in _rows(zf, 'stops.txt'):
                try:
                    self.stops[r['stop_id']] = (r.get('stop_name') or r['stop_id'], float(r['stop_lat']), float(r['stop_lon']))
                except (KeyError, ValueError):
                    continue
            self.routes = {r['route_id']: r.get('route_short_name') or r.get('route_long_name') or r['route_id']
                           for r in _rows(zf, 'routes.txt')}
            self.trips = {r['trip_id']: (r['route_id'], r['service_id'], r.get('trip_headsign', ''))
                          for r in _rows(zf, 'trips.txt')}
            self.freq = {}
            for r in _rows(zf, 'frequencies.txt'):
                self.freq.setdefault(r['trip_id'], []).append(
                    (secs(r['start_time']), secs(r['end_time']), int(r['headway_secs']), r.get('exact_times') == '1'))
            self.calendar = {}
            for r in _rows(zf, 'calendar.txt'):
                days = [r.get(d) == '1' for d in ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday')]
                self.calendar[r['service_id']] = (r['start_date'], r['end_date'], days)
            self.exceptions = {}
            for r in _rows(zf, 'calendar_dates.txt'):
                self.exceptions[(r['service_id'], r['date'])] = r.get('exception_type') == '1'
        self.points_key = None
        self.conn = {}
        self.log(f'Fahrplan: {len(self.stops)} Haltestellen, {len(self.routes)} Linien, {len(self.trips)} Fahrten, '
                 f'{len(self.freq)} Takt-Fahrten ({time.time() - t0:.0f} s)')

    # ---------- Betriebstage ----------
    def runs(self, service_id, day):
        ds = day.strftime('%Y%m%d')
        ex = self.exceptions.get((service_id, ds))
        if ex is not None:
            return ex
        c = self.calendar.get(service_id)
        return bool(c and c[0] <= ds <= c[1] and c[2][day.weekday()])

    def near(self, lat, lon):
        return {sid: dist_m(lat, lon, s[1], s[2]) for sid, s in self.stops.items()
                if dist_m(lat, lon, s[1], s[2]) <= RADIUS_M}

    # ---------- Verbindungen vorbereiten ----------
    def prepare(self, routes):
        """routes: {key: (start_lat, start_lon, ziel_lat, ziel_lon)}. Liest stop_times einmal und merkt sich
        alle Fahrten, die eine Haltestelle nahe dem Start VOR einer Haltestelle nahe dem Ziel anfahren."""
        key = tuple(sorted(routes.items()))
        if key == self.points_key:
            return
        t0 = time.time()
        near = {k: (self.near(p[0], p[1]), self.near(p[2], p[3])) for k, p in routes.items()}
        wanted = set()
        for a, b in near.values():
            wanted |= set(a) | set(b)
        visits, first = {}, {}           # trip -> [(seq, stop, arr, dep)], trip -> (seq, dep) der ersten Haltestelle
        with zipfile.ZipFile(self.path) as zf:
            for r in _rows(zf, 'stop_times.txt'):
                trip = r['trip_id']
                is_freq, is_wanted = trip in self.freq, r['stop_id'] in wanted
                if not (is_freq or is_wanted):
                    continue
                try:
                    seq = int(r['stop_sequence'])
                    arr = secs(r.get('arrival_time') or r['departure_time'])
                    dep = secs(r.get('departure_time') or r['arrival_time'])
                except (KeyError, ValueError):
                    continue             # Haltestelle ohne Zeitangabe (in GTFS erlaubt) -> überspringen
                if is_freq:              # bei Takt-Fahrten sind die Zeiten relativ zur ersten Haltestelle
                    f = first.get(trip)
                    if f is None or seq < f[0]:
                        first[trip] = (seq, dep)
                if is_wanted:
                    visits.setdefault(trip, []).append((seq, r['stop_id'], arr, dep))
        self.conn = {}
        for k, (a, b) in near.items():
            best = {}                    # (trip) -> beste Kombination Ein-/Ausstieg für diese Route
            for trip, vs in visits.items():
                ons = [v for v in vs if v[1] in a]
                offs = [v for v in vs if v[1] in b]
                for on in ons:
                    for off in offs:
                        if off[0] <= on[0]:
                            continue
                        walk_on, walk_off = walk_min(a[on[1]]), walk_min(b[off[1]])
                        ride = (off[2] - on[3]) / 60
                        score = walk_on + ride + walk_off
                        if trip not in best or score < best[trip][0]:
                            best[trip] = (score, on, off, walk_on, walk_off, ride)
            self.conn[k] = [(trip,) + v[1:] for trip, v in best.items()]
        self.first = first
        self.points_key = key
        self.log(f'Fahrplan: Verbindungen für {len(routes)} Routen vorbereitet '
                 f'({sum(len(c) for c in self.conn.values())} Fahrten, {time.time() - t0:.0f} s)')

    # ---------- beste Verbindung zu einem Zeitpunkt ----------
    def best(self, key, dep_dt):
        """Beste direkte Verbindung, wenn man um dep_dt (lokale Zeit) am Startpunkt losgeht."""
        conns = self.conn.get(key)
        if not conns:
            return {'status': 'nodirect'}
        day = dep_dt.date()
        t_leave = dep_dt.hour * 3600 + dep_dt.minute * 60
        cand = None
        for trip, on, off, walk_on, walk_off, ride in conns:
            route_id, service_id, headsign = self.trips.get(trip, (None, None, ''))
            if route_id is None:
                continue
            need = t_leave + walk_on * 60            # so früh kann man an der Haltestelle sein
            # Betriebstag gestern (Zeiten über 24 h = nach Mitternacht) und heute
            for offset_days, shift in ((1, 86400), (0, 0)):
                sday = day - timedelta(days=offset_days)
                if not self.runs(service_id, sday):
                    continue
                if trip in self.freq:
                    f0 = self.first.get(trip, (0, on[3]))[1]
                    rel = on[3] - f0                  # Fahrzeit von der Starthaltestelle der Linie bis hierher
                    for start, end, headway, exact in self.freq[trip]:
                        # Abfahrten an "on": start+rel, start+rel+headway, ... (bis end+rel)
                        lo, hi = start + rel - shift, end + rel - shift
                        if need > hi:
                            continue
                        k = max(0, math.ceil((need - lo) / headway))
                        t_on = lo + k * headway
                        if t_on > hi:
                            continue
                        if exact:                 # Takt mit festen Zeiten
                            wait = (t_on - need) / 60
                        elif need < lo:           # vor Betriebsbeginn: bis zur ersten Fahrt warten
                            wait = (lo - need) / 60
                        else:                     # ungefährer Takt: erwartete Wartezeit = halber Takt
                            wait = headway / 120
                        if wait > MAX_WAIT_MIN:
                            continue
                        total = walk_on + wait + ride + walk_off
                        c = (total, trip, on, off, walk_on, walk_off, ride, wait, None if not exact else t_on, headway)
                        if cand is None or total < cand[0]:
                            cand = c
                else:
                    t_on = on[3] - shift
                    if t_on < need or t_on - need > MAX_WAIT_MIN * 60:
                        continue
                    wait = (t_on - need) / 60
                    total = walk_on + wait + ride + walk_off
                    c = (total, trip, on, off, walk_on, walk_off, ride, wait, t_on, None)
                    if cand is None or total < cand[0]:
                        cand = c
        if cand is None:
            return {'status': 'noservice'}
        total, trip, on, off, walk_on, walk_off, ride, wait, t_on, headway = cand
        route_id, _, headsign = self.trips[trip]
        return {
            'status': 'ok',
            'line': self.routes.get(route_id, route_id), 'headsign': headsign,
            'board': {'id': on[1], 'name': self.stops[on[1]][0], 'walkMin': walk_on},
            'alight': {'id': off[1], 'name': self.stops[off[1]][0], 'walkMin': walk_off},
            'boardAt': hm(t_on) if t_on is not None else None,
            'headwayMin': round(headway / 60) if headway else None,
            'waitMin': round(wait), 'rideMin': round(ride), 'totalMin': round(total)}


def next_departure(dep, now=None):
    """Nächste Abfahrt einer Route ({time:'08:30', days:[1..7]}) als lokale datetime, sonst None."""
    now = now or now_local()
    try:
        h, m = (int(x) for x in dep['time'].split(':'))
        days = [int(d) for d in dep['days']]
    except (KeyError, ValueError, TypeError, AttributeError):
        return None
    for off in range(8):
        d = (now + timedelta(days=off)).replace(hour=h, minute=m, second=0, microsecond=0)
        if d.isoweekday() in days and d >= now.replace(second=0, microsecond=0):
            return d
    return None


def main(argv):
    if len(argv) < 7 or argv[1] != 'test':
        print(__doc__)
        return 1
    if not download():
        return 2
    tt = Timetable()
    s_lat, s_lon, z_lat, z_lon = (float(x) for x in argv[2:6])
    tt.prepare({'test': (s_lat, s_lon, z_lat, z_lon)})
    d = next_departure({'time': argv[6], 'days': [1, 2, 3, 4, 5, 6, 7]})
    print('Abfahrt', d.strftime('%a %d.%m. %H:%M'), '->', tt.best('test', d))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
