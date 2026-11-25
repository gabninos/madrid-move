# MadridMove

MadridMove tells BiciMAD commuters, for each trip they ride regularly, whether to take the bike today. A trip is saved once (start and destination station, departure time, weekdays, personal weather limits). From 30 minutes before departure the system checks bikes at the start station, the weather forecast for the ride and the bus and walking alternatives, and notifies the rider only when something changes: a warning, a pre-departure info or an all-clear.

Course project *Distributed Systems for IoT* (UPM), group 6: Tobias Kleinkauf, Gabriel Ninos. Python producers and consumers and Node-RED services communicate only over MQTT (broker `test.mosquitto.org`, topic prefix `upm-dsiot-g6/move`). Architecture, topics and delivery semantics are described in the project report and the Node-RED flow documentation.

## Contents

```
python/collector.py   producer: BiciMAD stations and bikes, weather, EMT bus timetable and live arrivals
python/client.py      producer + consumer: save routes and settings, receive notifications
flows/madridmove.json all Node-RED flows: route profile, recommendation, statistics, dashboard
```

## Requirements

- Python 3.9 or newer
- Node-RED 4 with the palette package `@flowfuse/node-red-dashboard` (menu → Manage palette)
- Outgoing port 1883 to the broker (if the network blocks it, use a phone hotspot)

## Start

```bash
# 1. Once, in the repository root
python3 -m venv .venv && source .venv/bin/activate
pip install -r python/requirements.txt

# 2. Terminal 1: Node-RED
node-red
#    open http://localhost:1880 → menu → Import → flows/madridmove.json → "Replace" on conflicts → Deploy

# 3. Terminal 2: collector (keeps running)
cd python && python3 collector.py

# 4. Terminal 3: client, shows the notifications of one user
cd python && python3 client.py listen <user>
```

The dashboard is at http://localhost:1880/dashboard. Routes can be created there (page *Manage routes*) or with the client:

```bash
python3 client.py stations sol                      # find station numbers
python3 client.py add-route <user> "To university" --from 1 --to 6 --time 08:30 --days 1-5
python3 client.py settings <user> --rain light --wind 35 --temp 5 32
python3 client.py routes <user>                     # routes and current status
python3 client.py delete-route <user> to-university
```

## Optional keys

Without keys everything runs; travel times are then estimated and the bus comparison uses the timetable only.

- **Real bike and walking times:** start Node-RED with an openrouteservice key: `export ORS_API_KEY=… && node-red`.
- **EMT live bus arrivals:** copy `python/secrets.env.example` to `python/secrets.env` and fill in `EMT_EMAIL` and `EMT_PASSWORD` (account at mobilitylabs.emtmadrid.es). Check with `cd python && python3 emt.py test`, then restart the collector.

`secrets.env` and the token cache are git-ignored and never leave the machine.

## Data sources

BiciMAD GBFS (EMT Madrid), Open-Meteo (CC BY 4.0), EMT GTFS timetable and EMT MobilityLabs, openrouteservice (© OpenStreetMap contributors, ODbL).
