#!/usr/bin/env python3
"""EMT Madrid (MobilityLabs): Anmeldung, Token-Verwaltung und Busankünfte.

Zugangsdaten (eine der beiden Varianten) stehen in python/secrets.env
(Vorlage: secrets.env.example; die Datei ist per .gitignore vom Repo ausgeschlossen):
    EMT_EMAIL=...            EMT_PASSWORD=...        (Konto von mobilitylabs.emtmadrid.es)
    EMT_CLIENT_ID=...        EMT_PASSKEY=...         (alternativ: X-ClientId und passKey einer App)
Umgebungsvariablen mit denselben Namen haben Vorrang vor der Datei.

Der Token gilt ca. 24 h. Er wird in python/.emt_token.json zwischengespeichert (auch git-ignoriert)
und automatisch erneuert: 30 min vor Ablauf und sofort, wenn EMT ihn ablehnt (Code 80 / HTTP 401).
Das Passwort wird nie ausgegeben oder per MQTT verschickt.

Testen:
    python emt.py diagnose                   alles auf einmal prüfen (Anmeldung, Haltestellen, Busse, Rohdaten)
    python emt.py test                       Anmelden, Gültigkeit des Tokens anzeigen
    python emt.py umkreis 40.4169 -3.7035    Bushaltestellen im Umkreis von 300 m
    python emt.py haltestelle 72             nächste Busse an Haltestelle 72
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
    """Liest Zugangsdaten aus secrets.env und Umgebungsvariablen. None, wenn keine da sind."""
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
            raise EmtError('keine Zugangsdaten: python/secrets.env anlegen (Vorlage secrets.env.example)')
        self.log = log
        self.lock = threading.Lock()
        self.token, self.expires = None, 0.0
        self._load_cache()

    # ---------- Token ----------
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
            self.log('EMT: Token konnte nicht gespeichert werden:', e)

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
                self.log('EMT: angemeldet, Token gültig bis', datetime.fromtimestamp(self.expires).strftime('%d.%m. %H:%M'))
                return self.token
            code = res.get('code')
            reason = {'89': 'Passwort falsch', '92': 'Benutzer unbekannt', '98': 'Tageslimit erreicht'}.get(
                str(code), res.get('description') or f'HTTP {status}')
            raise EmtError(f'Anmeldung fehlgeschlagen: {reason} (Code {code})')
        raise EmtError(f'Anmeldung fehlgeschlagen: {last}')

    def get_token(self):
        with self.lock:
            if not self.token or self.expires - time.time() < RENEW_BEFORE_S:
                self.login()
            return self.token

    # ---------- Anfragen ----------
    def request(self, method, path, body=None):
        for attempt in (1, 2):
            status, res = _http(method, BASE_URL + path, headers={'accessToken': self.get_token()}, body=body)
            if status == 401 or str(res.get('code')) == '80':      # Token abgelaufen/ungültig
                with self.lock:
                    self.token = None
                if attempt == 1:
                    continue
            if status >= 400:
                raise EmtError(f'{path}: HTTP {status} {res.get("description", "")}'.strip())
            return res
        raise EmtError(f'{path}: Token wird nicht akzeptiert')

    def stops_around(self, lat, lon, radius=300):
        """Bushaltestellen im Umkreis, nächste zuerst: [{id, name, distM, lines}]"""
        res = self.request('GET', f'/v2/transport/busemtmad/stops/arroundxy/{lon}/{lat}/{int(radius)}/')
        stops = []
        for s in res.get('data') or []:
            sid = s.get('stopId', s.get('stop'))
            if sid is None:
                continue
            lines = [str(l.get('label') or l.get('line')) for l in (s.get('lines') or []) if isinstance(l, dict)]
            stops.append({'id': str(sid), 'name': s.get('stopName') or s.get('name') or f'Haltestelle {sid}',
                          'distM': int(round(float(s.get('metersToPoint') or s.get('distance') or 0))),
                          'lines': sorted(set(lines))})
        return sorted(stops, key=lambda s: s['distM'])

    def arrivals_raw(self, stop_id):
        """Rohantwort von EMT (Anfrage mit allen Feldern aus der EMT-Dokumentation)."""
        return self.request('POST', f'/v2/transport/busemtmad/stops/{stop_id}/arrives/', body={
            'cultureInfo': 'ES',
            'Text_StopRequired_YN': 'Y',
            'Text_EstimationsRequired_YN': 'Y',
            'Text_IncidencesRequired_YN': 'N',
            'DateTime_Referenced_Incidencies_YYYYMMDD': datetime.now().strftime('%Y%m%d')})

    def arrivals(self, stop_id):
        """Nächste Busse an einer Haltestelle: [{line, destination, min}] (Minuten ab jetzt)"""
        res = self.arrivals_raw(stop_id)
        code = str(res.get('code', '00'))
        if code == '01':                  # "No estimations found": gerade kein Bus unterwegs
            return []
        if code not in ('00', '0'):
            raise EmtError(f'Haltestelle {stop_id}: {res.get("description") or "Fehler"} (Code {code})')
        out = []
        for a in _find_arrivals(res.get('data')):
            try:
                sec = int(a.get('estimateArrive'))
            except (TypeError, ValueError):
                continue
            if sec >= 999999:             # EMT: keine Schätzung verfügbar
                continue
            out.append({'line': str(a.get('line')), 'destination': a.get('destination', ''),
                        'min': sec // 60})
        return sorted(out, key=lambda a: a['min'])


def _find_arrivals(node):
    """Sucht die Liste der Ankünfte in der Antwort (Feld "Arrive", egal wie tief verschachtelt)."""
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
    """Prüft Schritt für Schritt und zeigt die Rohantworten von EMT (ohne Token/Passwort)."""
    print('1) Zugangsdaten:', 'gefunden' if load_credentials() else 'FEHLEN (python/secrets.env)')
    emt = EmtClient()
    emt.get_token()
    print(f'2) Anmeldung OK, Token noch {(emt.expires - time.time()) / 3600:.1f} h gültig')
    path = f'/v2/transport/busemtmad/stops/arroundxy/{lon}/{lat}/300/'
    raw = emt.request('GET', path)
    print(f'3) Haltestellen um {lat}, {lon}: Code {raw.get("code")}, {len(raw.get("data") or [])} Einträge')
    print('   Rohdaten:', json.dumps(raw, ensure_ascii=False)[:700])
    stops = emt.stops_around(lat, lon)
    for s in stops[:5]:
        print(f"   {s['id']:>6}  {s['distM']:>4} m  {s['name']}  (Linien {', '.join(s['lines'])})")
    if not stops:
        print('   -> keine Haltestelle erkannt')
        return
    sid = stops[0]['id']
    raw = emt.arrivals_raw(sid)
    print(f'4) Ankünfte an Haltestelle {sid}: Code {raw.get("code")}')
    print('   Rohdaten:', json.dumps(raw, ensure_ascii=False)[:900])
    arr = emt.arrivals(sid)
    print(f'   -> {len(arr)} Ankünfte erkannt:', ', '.join(f"{a['line']} in {a['min']} min" for a in arr[:6]) or '-')


def main(argv):
    if len(argv) < 2 or argv[1] not in ('test', 'umkreis', 'haltestelle', 'diagnose'):
        if len(argv) >= 2:
            print(f'Unbekannter Befehl: {" ".join(argv[1:])}\n')
        print(__doc__)
        return 1
    try:
        emt = EmtClient()
        if argv[1] == 'diagnose':
            diagnose(*(float(x) for x in argv[2:4])) if len(argv) >= 4 else diagnose()
        elif argv[1] == 'test':
            emt.get_token()
            left = (emt.expires - time.time()) / 3600
            print(f'OK: Token vorhanden, noch {left:.1f} h gültig (Cache: {TOKEN_FILE})')
        elif argv[1] == 'umkreis':
            lat, lon = float(argv[2]), float(argv[3])
            for s in emt.stops_around(lat, lon)[:8]:
                print(f"{s['id']:>6}  {s['distM']:>4} m  {s['name']}  (Linien {', '.join(s['lines'])})")
        else:
            arr = emt.arrivals(argv[2])
            if not arr:
                print('keine Ankünfte gemeldet')
            for a in arr:
                print(f"Linie {a['line']:>4} -> {a['destination']}: {a['min']} min")
    except (EmtError, OSError, IndexError, ValueError) as e:
        print('Fehler:', e)
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
