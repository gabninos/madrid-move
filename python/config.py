"""Gemeinsame Einstellungen für Collector und Client.

TAG und PREFIX setzt configure.py (nicht von Hand ändern).
Alles andere lässt sich per Umgebungsvariable überschreiben, z. B.
    MM_BROKER=localhost python3 collector.py
"""
import os

TAG = 'g6'
PREFIX = 'upm-dsiot-g6/move'

BROKER = os.environ.get('MM_BROKER', 'test.mosquitto.org')
PORT = int(os.environ.get('MM_PORT', '1883'))
KEEPALIVE = 30   # Sekunden; der Broker erkennt einen Absturz nach ca. 1,5 x KEEPALIVE

# Datenquellen (offen, ohne Schlüssel)
GBFS_URL = os.environ.get('GBFS_URL', 'https://madrid.publicbikesystem.net/customer/gbfs/v2/gbfs.json')
METEO_URL = os.environ.get('METEO_URL', 'https://api.open-meteo.com/v1/forecast')
WEATHER_POINT = (40.4168, -3.7038)   # Puerta del Sol
USER_AGENT = 'MadridMove/2.0 (UPM DSIoT student project)'
