#!/usr/bin/env python3
"""EMT Madrid (MobilityLabs): login, token handling and bus arrivals.

Credentials (one of the two variants) are stored in python/secrets.env
(template: secrets.env.example; the file is excluded from the repo by .gitignore):
    EMT_EMAIL=...            EMT_PASSWORD=...        (account from mobilitylabs.emtmadrid.es)
    EMT_CLIENT_ID=...        EMT_PASSKEY=...         (alternatively: X-ClientId and passKey of an app)
Environment variables with the same names take precedence over the file.

The token is valid for about 24 h. It is cached in python/.emt_token.json (also git-ignored)
and renewed automatically: 30 min before it expires and at once if EMT rejects it (code 80 / HTTP 401).
The password is never printed or sent via MQTT.

Testing:
    python emt.py diagnose                   check everything at once (login, stops, buses, raw data)
    python emt.py test                       log in, show how long the token is valid
    python emt.py nearby 40.4169 -3.7035     bus stops within 300 m
    python emt.py stop 72                    next buses at stop 72
"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
SECRETS_FILE = os.path.join(HERE, 'secrets.env')
TOKEN_FILE = os.path.join(HERE, '.emt_token.json')
BASE_URL = os.environ.get('EMT_URL', 'https://openapi.emtmadrid.es').rstrip('/')
LOGIN_PATHS = ('/v1/mobilitylabs/user/login/', '/v2/mobilitylabs/user/login/')
RENEW_BEFORE_S = 30 * 60
TIMEOUT_S = 20


class EmtError(Exception):
    pass


def load_credentials():
    """Reads credentials from secrets.env and environment variables. None if there are none."""
    values = {}
    if os.path.exists(SECRETS_FILE):
        with open(SECRETS_FILE, encoding='utf-8-sig') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                values[k.strip()] = v.strip().strip('"').strip("'")
    for k in ('EMT_EMAIL', 'EMT_PASSWORD', 'EMT_CLIENT_ID', 'EMT_PASSKEY'):
        if os.environ.get(k):
            values[k] = os.environ[k]
    if values.get('EMT_EMAIL') and values.get('EMT_PASSWORD'):
        return {'email': values['EMT_EMAIL'], 'password': values['EMT_PASSWORD']}
    if values.get('EMT_CLIENT_ID') and values.get('EMT_PASSKEY'):
        return {'X-ClientId': values['EMT_CLIENT_ID'], 'passKey': values['EMT_PASSKEY']}
    return None


def _http(method, url, headers=None, body=None):
    data = json.dumps(body).encode('utf-8') if body is not None else None
    h = {'Accept': 'application/json', 'User-Agent': 'MadridMove/2.0 (UPM DSIoT student project)'}
    if data is not None:
        h['Content-Type'] = 'application/json'
    h.update(headers or {})
    req = urllib.request.Request(url, data=data, headers=h, method=method)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as r:
            return r.status, json.loads(r.read().decode('utf-8') or '{}')
    except urllib.error.HTTPError as e:
        try:
            payload = json.loads(e.read().decode('utf-8') or '{}')
        except ValueError:
            payload = {}
        return e.code, payload


class EmtClient:
    def __init__(self, credentials=None, log=print):
        self.credentials = credentials or load_credentials()
        if not self.credentials:
            raise EmtError('no credentials: create python/secrets.env (template secrets.env.example)')
        self.log = log
        self.lock = threading.Lock()
        self.token, self.expires = None, 0.0
        self._load_cache()

    # ---------- token ----------
    def _who(self):
        return self.credentials.get('email') or self.credentials.get('X-ClientId')

    def _load_cache(self):
        try:
            with open(TOKEN_FILE, encoding='utf-8') as f:
                c = json.load(f)
            if c.get('who') == self._who() and c.get('expires', 0) - time.time() > RENEW_BEFORE_S:
                self.token, self.expires = c['token'], c['expires']
        except (OSError, ValueError, KeyError):
            pass

    def _save_cache(self):
        try:
            with open(TOKEN_FILE, 'w', encoding='utf-8') as f:
                json.dump({'who': self._who(), 'token': self.token, 'expires': self.expires}, f)
        except OSError as e:
            self.log('EMT: could not save the token:', e)

    def login(self):
        last = None
        for path in LOGIN_PATHS:
            status, res = _http('GET', BASE_URL + path, headers=self.credentials)
            if status == 404:
                last = 'HTTP 404'
                continue
            data = (res.get('data') or [{}])[0] if isinstance(res.get('data'), list) else {}
            if data.get('accessToken'):
                self.token = data['accessToken']
                exp = data.get('tokenDteExpiration')
                if isinstance(exp, dict) and exp.get('$date'):
                    self.expires = exp['$date'] / 1000
                else:
                    self.expires = time.time() + float(data.get('tokenSecExpiration') or 86399)
                self._save_cache()
                self.log('EMT: logged in, token valid until', datetime.fromtimestamp(self.expires).strftime('%Y-%m-%d %H:%M'))
                return self.token
            code = res.get('code')
            reason = {'89': 'wrong password', '92': 'unknown user', '98': 'daily limit reached'}.get(
                str(code), res.get('description') or f'HTTP {status}')
            raise EmtError(f'login failed: {reason} (code {code})')
        raise EmtError(f'login failed: {last}')

    def get_token(self):
        with self.lock:
            if not self.token or self.expires - time.time() < RENEW_BEFORE_S:
                self.login()
            return self.token

    # ---------- requests ----------
    def request(self, method, path, body=None):
        for attempt in (1, 2):
            status, res = _http(method, BASE_URL + path, headers={'accessToken': self.get_token()}, body=body)
            if status == 401 or str(res.get('code')) == '80':      # token expired/invalid
                with self.lock:
                    self.token = None
                if attempt == 1:
                    continue
            if status >= 400:
                raise EmtError(f'{path}: HTTP {status} {res.get("description", "")}'.strip())
            return res
        raise EmtError(f'{path}: token not accepted')

    def stops_around(self, lat, lon, radius=300):
        """Bus stops within the radius, nearest first: [{id, name, distM, lines}]"""
        res = self.request('GET', f'/v2/transport/busemtmad/stops/arroundxy/{lon}/{lat}/{int(radius)}/')
        stops = []
        for s in res.get('data') or []:
            sid = s.get('stopId', s.get('stop'))
            if sid is None:
                continue
            lines = [str(l.get('label') or l.get('line')) for l in (s.get('lines') or []) if isinstance(l, dict)]
            stops.append({'id': str(sid), 'name': s.get('stopName') or s.get('name') or f'Stop {sid}',
                          'distM': int(round(float(s.get('metersToPoint') or s.get('distance') or 0))),
                          'lines': sorted(set(lines))})
        return sorted(stops, key=lambda s: s['distM'])

    def arrivals_raw(self, stop_id):
        """Raw EMT response (request with all fields from the EMT documentation)."""
        return self.request('POST', f'/v2/transport/busemtmad/stops/{stop_id}/arrives/', body={
            'cultureInfo': 'ES',
            'Text_StopRequired_YN': 'Y',
            'Text_EstimationsRequired_YN': 'Y',
            'Text_IncidencesRequired_YN': 'N',
            'DateTime_Referenced_Incidencies_YYYYMMDD': datetime.now().strftime('%Y%m%d')})

    def arrivals(self, stop_id):
        """Next buses at a stop: [{line, destination, min}] (minutes from now)"""
        res = self.arrivals_raw(stop_id)
        code = str(res.get('code', '00'))
        if code == '01':                  # "No estimations found": no bus on the way right now
            return []
        if code not in ('00', '0'):
            raise EmtError(f'Stop {stop_id}: {res.get("description") or "error"} (code {code})')
        out = []
        for a in _find_arrivals(res.get('data')):
            try:
                sec = int(a.get('estimateArrive'))
            except (TypeError, ValueError):
                continue
            if sec >= 999999:             # EMT: no estimate available
                continue
            out.append({'line': str(a.get('line')), 'destination': a.get('destination', ''),
                        'min': sec // 60})
        return sorted(out, key=lambda a: a['min'])


def _find_arrivals(node):
    """Finds the list of arrivals in the response (field "Arrive", however deeply nested)."""
    if isinstance(node, list):
        if node and all(isinstance(x, dict) and 'estimateArrive' in x for x in node):
            return node
        for x in node:
            found = _find_arrivals(x)
            if found:
                return found
    elif isinstance(node, dict):
        for k, v in node.items():
            if k.lower().startswith('arrive') and isinstance(v, list):
                return v
        for v in node.values():
            found = _find_arrivals(v)
            if found:
                return found
    return []


def diagnose(lat=40.4170, lon=-3.7032):
    """Checks step by step and shows the raw EMT responses (without token/password)."""
    print('1) Credentials:', 'found' if load_credentials() else 'MISSING (python/secrets.env)')
    emt = EmtClient()
    emt.get_token()
    print(f'2) Login OK, token valid for {(emt.expires - time.time()) / 3600:.1f} h')
    path = f'/v2/transport/busemtmad/stops/arroundxy/{lon}/{lat}/300/'
    raw = emt.request('GET', path)
    print(f'3) Stops around {lat}, {lon}: code {raw.get("code")}, {len(raw.get("data") or [])} entries')
    print('   Raw data:', json.dumps(raw, ensure_ascii=False)[:700])
    stops = emt.stops_around(lat, lon)
    for s in stops[:5]:
        print(f"   {s['id']:>6}  {s['distM']:>4} m  {s['name']}  (lines {', '.join(s['lines'])})")
    if not stops:
        print('   -> no stop found')
        return
    sid = stops[0]['id']
    raw = emt.arrivals_raw(sid)
    print(f'4) Arrivals at stop {sid}: code {raw.get("code")}')
    print('   Raw data:', json.dumps(raw, ensure_ascii=False)[:900])
    arr = emt.arrivals(sid)
    print(f'   -> {len(arr)} arrivals found:', ', '.join(f"{a['line']} in {a['min']} min" for a in arr[:6]) or '-')


def main(argv):
    if len(argv) < 2 or argv[1] not in ('test', 'nearby', 'stop', 'diagnose'):
        if len(argv) >= 2:
            print(f'Unknown command: {" ".join(argv[1:])}\n')
        print(__doc__)
        return 1
    try:
        emt = EmtClient()
        if argv[1] == 'diagnose':
            diagnose(*(float(x) for x in argv[2:4])) if len(argv) >= 4 else diagnose()
        elif argv[1] == 'test':
            emt.get_token()
            left = (emt.expires - time.time()) / 3600
            print(f'OK: token available, valid for {left:.1f} h (cache: {TOKEN_FILE})')
        elif argv[1] == 'nearby':
            lat, lon = float(argv[2]), float(argv[3])
            for s in emt.stops_around(lat, lon)[:8]:
                print(f"{s['id']:>6}  {s['distM']:>4} m  {s['name']}  (lines {', '.join(s['lines'])})")
        else:
            arr = emt.arrivals(argv[2])
            if not arr:
                print('no arrivals reported')
            for a in arr:
                print(f"Line {a['line']:>4} -> {a['destination']}: {a['min']} min")
    except (EmtError, OSError, IndexError, ValueError) as e:
        print('Error:', e)
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
