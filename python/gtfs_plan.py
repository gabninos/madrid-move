#!/usr/bin/env python3
"""EMT bus timetable (GTFS) -> direct bus connection for a route.

GTFS ("General Transit Feed Specification") is the standard format for timetables: a ZIP file
with text files (CSV). We use:
    stops.txt          stops with location
    routes.txt         lines (e.g. "27")
    trips.txt          individual trips of a line (with service days = service_id)
    stop_times.txt     when a trip is at which stop
    frequencies.txt    (optional) headway instead of times: "7-10 am every 8 min"
    calendar.txt, calendar_dates.txt   on which days a service_id runs

Only the best DIRECT connection (no transfers) is searched:
    walk to a stop near the start -> wait -> bus ride -> walk to the destination.

The ZIP file is downloaded automatically (renewed weekly) to python/gtfs/transitemt.zip.
If the download fails (firewall, port 8443), download the file in a browser and put it there:
    https://servicios.emtmadrid.es:8443/gtfs/transitemt.zip

Testing (without MQTT):
    python gtfs_plan.py test 40.4169 -3.7035 40.4437 -3.7276 08:30
        start-lat start-lon dest-lat dest-lon departure (today or next day)
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
    TZ = ZoneInfo('Europe/Madrid')       # on Windows this needs the package "tzdata"
except Exception:                        # noqa: BLE001
    TZ = None

HERE = os.path.dirname(os.path.abspath(__file__))
GTFS_URL = os.environ.get('GTFS_EMT_URL', 'https://servicios.emtmadrid.es:8443/gtfs/transitemt.zip')
GTFS_FILE = os.environ.get('GTFS_EMT_FILE', os.path.join(HERE, 'gtfs', 'transitemt.zip'))
MAX_AGE_S = 7 * 86400          # reload the timetable file weekly
RADIUS_M = 600                 # stops up to this distance (straight line) from start/destination
DETOUR = 1.3                   # straight line -> walking distance
WALK_M_PER_MIN = 80            # about 4.8 km/h
MAX_WAIT_MIN = 60              # waiting longer counts as "no bus at this time"


def now_local():
    return datetime.now(TZ) if TZ else datetime.now()


def dist_m(lat1, lon1, lat2, lon2):
    kx = 111320 * math.cos(math.radians(lat1))
    return math.hypot((lon2 - lon1) * kx, (lat2 - lat1) * 110540)


def walk_min(meters):
    return math.ceil(meters * DETOUR / WALK_M_PER_MIN)


def secs(hms):
    """'25:10:00' -> seconds after midnight of the service day (GTFS allows times over 24 h)."""
    h, m, s = (hms.strip().split(':') + ['0', '0'])[:3]
    return int(h) * 3600 + int(m) * 60 + int(s)


def hm(total_secs):
    total_secs %= 86400
    return f'{total_secs // 3600:02d}:{(total_secs % 3600) // 60:02d}'


def download(log=print, force=False):
    """Downloads the timetable file if it is missing or older than a week. True = file available."""
    fresh = os.path.exists(GTFS_FILE) and time.time() - os.path.getmtime(GTFS_FILE) < MAX_AGE_S
    if fresh and not force:
        return True
    os.makedirs(os.path.dirname(GTFS_FILE), exist_ok=True)
    tmp = GTFS_FILE + '.part'
    try:
        log('Timetable: downloading', GTFS_URL)
        req = urllib.request.Request(GTFS_URL, headers={'User-Agent': 'MadridMove/2.0 (UPM DSIoT student project)'})
        with urllib.request.urlopen(req, timeout=120) as r, open(tmp, 'wb') as f:
            while True:
                chunk = r.read(1 << 20)
                if not chunk:
                    break
                f.write(chunk)
        zipfile.ZipFile(tmp).testzip()
        os.replace(tmp, GTFS_FILE)
        log(f'Timetable: {os.path.getsize(GTFS_FILE) // 1024} KB downloaded')
        return True
    except Exception as e:                # noqa: BLE001
        if os.path.exists(tmp):
            os.remove(tmp)
        if os.path.exists(GTFS_FILE):
            log('Timetable: download failed, using the existing file:', e)
            return True
        log('Timetable: download failed:', e)
        log('  -> download the file in a browser and save it as', GTFS_FILE)
        return False


def _rows(zf, name):
    """Reads a GTFS file row by row as dicts (without loading everything into memory)."""
    match = [n for n in zf.namelist() if n.split('/')[-1] == name]
    if not match:
        return
    with zf.open(match[0]) as raw:
        for row in csv.DictReader(io.TextIOWrapper(raw, encoding='utf-8-sig', newline='')):
            yield {k.strip(): (v or '').strip() for k, v in row.items() if k}


class Timetable:
    """Timetable in memory: stops, lines, trips, service days. stop_times is only read
    for the stops near the routes (otherwise it would be millions of rows)."""

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
        self.log(f'Timetable: {len(self.stops)} stops, {len(self.routes)} lines, {len(self.trips)} trips, '
                 f'{len(self.freq)} headway-based trips ({time.time() - t0:.0f} s)')

    # ---------- service days ----------
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

    # ---------- prepare connections ----------
    def prepare(self, routes):
        """routes: {key: (start_lat, start_lon, dest_lat, dest_lon)}. Reads stop_times once and keeps
        all trips that serve a stop near the start BEFORE a stop near the destination."""
        key = tuple(sorted(routes.items()))
        if key == self.points_key:
            return
        t0 = time.time()
        near = {k: (self.near(p[0], p[1]), self.near(p[2], p[3])) for k, p in routes.items()}
        wanted = set()
        for a, b in near.values():
            wanted |= set(a) | set(b)
        visits, first = {}, {}           # trip -> [(seq, stop, arr, dep)], trip -> (seq, dep) of the first stop
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
                    continue             # stop without a time (allowed in GTFS) -> skip
                if is_freq:              # for headway-based trips, times are relative to the first stop
                    f = first.get(trip)
                    if f is None or seq < f[0]:
                        first[trip] = (seq, dep)
                if is_wanted:
                    visits.setdefault(trip, []).append((seq, r['stop_id'], arr, dep))
        self.conn = {}
        for k, (a, b) in near.items():
            best = {}                    # (trip) -> best boarding/alighting pair for this route
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
        self.log(f'Timetable: connections prepared for {len(routes)} routes '
                 f'({sum(len(c) for c in self.conn.values())} Fahrten, {time.time() - t0:.0f} s)')

    # ---------- best connection at a given time ----------
    def best(self, key, dep_dt):
        """Best direct connection when leaving the start point at dep_dt (local time)."""
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
            need = t_leave + walk_on * 60            # earliest time at the stop
            # service day yesterday (times over 24 h = after midnight) and today
            for offset_days, shift in ((1, 86400), (0, 0)):
                sday = day - timedelta(days=offset_days)
                if not self.runs(service_id, sday):
                    continue
                if trip in self.freq:
                    f0 = self.first.get(trip, (0, on[3]))[1]
                    rel = on[3] - f0                  # travel time from the line's first stop to here
                    for start, end, headway, exact in self.freq[trip]:
                        # departures at "on": start+rel, start+rel+headway, ... (until end+rel)
                        lo, hi = start + rel - shift, end + rel - shift
                        if need > hi:
                            continue
                        k = max(0, math.ceil((need - lo) / headway))
                        t_on = lo + k * headway
                        if t_on > hi:
                            continue
                        if exact:                 # headway with fixed times
                            wait = (t_on - need) / 60
                        elif need < lo:           # before service starts: wait for the first trip
                            wait = (lo - need) / 60
                        else:                     # approximate headway: expected wait = half the headway
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
    """Next departure of a route ({time:'08:30', days:[1..7]}) as local datetime, otherwise None."""
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
    print('Departure', d.strftime('%a %Y-%m-%d %H:%M'), '->', tt.best('test', d))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
