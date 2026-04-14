#!/usr/bin/env python3
"""Add NHSCC events and fetch their weather data from Open-Meteo ERA5.

Usage:
  Single event:
    python add_events.py --date 2026-05-03 --name "Points Event 3" --drivers 118

  Batch (one event per line: date,name,drivers):
    python add_events.py --batch "2026-05-03,Points Event 3,118
    2026-05-17,Points Event 4,125"
"""

import argparse
import json
import sys
import urllib.request
import time
from pathlib import Path

DATA_FILE = Path(__file__).parent.parent / "data" / "nhscc_events_weather.json"

LAT = 40.587960
LON = -79.993918

WMO_CONDITIONS = {
    0: "☀️ Sunny/Clear",
    1: "☀️ Sunny/Clear",
    2: "☀️ Sunny/Clear",
    3: "☁️ Overcast (Dry)",
    45: "☁️ Overcast (Dry)",
    48: "☁️ Overcast (Dry)",
    51: "🌦️ Drizzle/Trace",
    53: "🌦️ Drizzle/Trace",
    55: "🌦️ Drizzle/Trace",
    56: "🌦️ Drizzle/Trace",
    57: "🌦️ Drizzle/Trace",
    61: "🌧️ Light Rain",
    63: "🌧️ Light Rain",
    65: "⛈️ Heavy Rain",
    66: "🌧️ Light Rain",
    67: "⛈️ Heavy Rain",
    71: "❄️ Snow",
    73: "❄️ Snow",
    75: "❄️ Snow",
    77: "❄️ Snow",
    80: "🌧️ Light Rain",
    81: "🌧️ Light Rain",
    82: "⛈️ Heavy Rain",
    85: "❄️ Snow",
    86: "❄️ Snow",
    95: "⛈️ Heavy Rain",
    96: "⛈️ Heavy Rain",
    99: "⛈️ Heavy Rain",
}


def fetch_weather(dates):
    """Fetch ERA5 weather for a list of dates. Returns dict of date -> weather."""
    if not dates:
        return {}

    dates = sorted(dates)
    start, end = dates[0], dates[-1]

    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={LAT}&longitude={LON}"
        f"&start_date={start}&end_date={end}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,"
        f"wind_speed_10m_max,weather_code"
        f"&temperature_unit=fahrenheit"
        f"&precipitation_unit=inch"
        f"&wind_speed_unit=mph"
    )

    print(f"Fetching weather for {start} to {end}...")
    try:
        with urllib.request.urlopen(url) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(f"ERROR: Open-Meteo returned {e.code}. ERA5 data typically has a ~5 day lag,")
        print(f"so recent dates may not be available yet. Try again in a few days.")
        sys.exit(1)

    daily = data["daily"]
    date_set = set(dates)
    result = {}

    for i, d in enumerate(daily["time"]):
        if d in date_set:
            code = daily["weather_code"][i]
            tmax = round(daily["temperature_2m_max"][i], 1)
            tmin = round(daily["temperature_2m_min"][i], 1)
            result[d] = {
                "tmax": tmax,
                "tmin": tmin,
                "tmid": round((tmax + tmin) / 2, 1),
                "precip": round(daily["precipitation_sum"][i], 3),
                "wind": round(daily["wind_speed_10m_max"][i], 1),
                "code": code,
                "condition": WMO_CONDITIONS.get(code, f"❓ Unknown ({code})"),
            }

    return result


def parse_batch(text):
    """Parse batch input: one event per line as date,name,drivers."""
    events = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",", 2)]
        if len(parts) != 3:
            print(f"Skipping malformed line: {line}")
            continue
        date, name, drivers = parts
        events.append({"date": date, "name": name, "drivers": int(drivers)})
    return events


def main():
    parser = argparse.ArgumentParser(description="Add NHSCC events with weather data")
    parser.add_argument("--date", help="Event date (YYYY-MM-DD)")
    parser.add_argument("--name", help="Event name")
    parser.add_argument("--drivers", type=int, help="Driver count")
    parser.add_argument("--batch", help="Batch input: date,name,drivers per line")
    args = parser.parse_args()

    if args.batch:
        new_events = parse_batch(args.batch)
    elif args.date and args.name and args.drivers is not None:
        new_events = [{"date": args.date, "name": args.name, "drivers": args.drivers}]
    else:
        parser.error("Provide --date/--name/--drivers for a single event, or --batch for multiple")

    if not new_events:
        print("No events to add.")
        sys.exit(1)

    # Load existing data
    with open(DATA_FILE) as f:
        data = json.load(f)

    existing_dates = {e["date"] for e in data["events"]}

    # Check for duplicates
    to_add = []
    for ev in new_events:
        if ev["date"] in existing_dates:
            print(f"Skipping duplicate: {ev['date']} ({ev['name']}) already exists")
        else:
            to_add.append(ev)

    if not to_add:
        print("All events already exist. Nothing to do.")
        sys.exit(0)

    # Fetch weather
    weather = fetch_weather([ev["date"] for ev in to_add])

    # Build full event records
    added = 0
    for ev in to_add:
        w = weather.get(ev["date"])
        if not w:
            print(f"WARNING: No weather data for {ev['date']} — skipping")
            continue

        year = int(ev["date"][:4])
        record = {
            "date": ev["date"],
            "drivers": ev["drivers"],
            "year": year,
            "name": ev["name"],
            "tmax": w["tmax"],
            "tmin": w["tmin"],
            "precip": w["precip"],
            "wind": w["wind"],
            "code": w["code"],
            "tmid": w["tmid"],
            "condition": w["condition"],
        }
        data["events"].append(record)
        added += 1
        print(f"Added: {ev['date']} | {ev['name']} | {ev['drivers']} drivers | {w['tmax']}°F | {w['precip']}\" | {w['condition']}")

    # Sort events by date
    data["events"].sort(key=lambda e: e["date"])

    # Update meta
    data["meta"]["event_count"] = len(data["events"])
    all_years = [e["year"] for e in data["events"]]
    data["meta"]["season_range"] = f"{min(all_years)}-{max(all_years)}"

    # Write back
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\nDone: added {added} event(s). Total: {data['meta']['event_count']} events.")


if __name__ == "__main__":
    main()
