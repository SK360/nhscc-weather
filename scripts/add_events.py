#!/usr/bin/env python3
"""Add NHSCC events and fetch their weather data from Open-Meteo ERA5.

Precipitation is summed for event hours only (8 AM - 5 PM Eastern)
rather than the full UTC day, so overnight rain doesn't inflate totals.

Usage:
  Single event:
    python add_events.py --date 2026-05-03 --name "Points Event 3" --drivers 118

  Batch (one event per line: date,name,drivers):
    python add_events.py --batch "2026-05-03,Points Event 3,118
    2026-05-17,Points Event 4,125"

  Re-fetch weather for all existing events (preserves dates/names/drivers):
    python add_events.py --refetch
"""

import argparse
import json
import sys
import urllib.request
import time
from datetime import date, datetime
from pathlib import Path

DATA_FILE = Path(__file__).parent.parent / "data" / "nhscc_events_weather.json"

LAT = 40.587960
LON = -79.993918

# Event hours: 8 AM - 5 PM Eastern
# EDT (UTC-4): hours 12-20 UTC  |  EST (UTC-5): hours 13-21 UTC
# Hourly precip value at hour H covers H:00 to H+1:00
EDT_HOURS_UTC = set(range(12, 21))  # 12:00-20:00 UTC = 8AM-5PM EDT
EST_HOURS_UTC = set(range(13, 22))  # 13:00-21:00 UTC = 8AM-5PM EST


def is_dst(d):
    """Check if a date falls within US Eastern Daylight Time.

    DST runs from 2nd Sunday of March at 2AM to 1st Sunday of November at 2AM.
    """
    year = d.year if isinstance(d, date) else int(d[:4])
    month = int(d[5:7]) if isinstance(d, str) else d.month
    day = int(d[8:10]) if isinstance(d, str) else d.day

    if month < 3 or month > 11:
        return False
    if month > 3 and month < 11:
        return True

    dt = date(year, month, day)
    if month == 3:
        # 2nd Sunday of March
        first = date(year, 3, 1)
        second_sun = first.day + (6 - first.weekday()) % 7 + 7
        return day >= second_sun
    else:
        # 1st Sunday of November — clocks fall back at 2 AM, so daytime
        # events (8 AM - 5 PM) on that Sunday are still mostly on EDT.
        # Treat the transition day itself as EDT for event-hours purposes.
        first = date(year, 11, 1)
        first_sun = first.day + (6 - first.weekday()) % 7
        return day <= first_sun


def event_hours_utc(event_date):
    """Return the set of UTC hours for 8AM-5PM Eastern on a given date."""
    return EDT_HOURS_UTC if is_dst(event_date) else EST_HOURS_UTC

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
    """Fetch ERA5 weather for a list of dates. Returns dict of date -> weather.

    Uses hourly precipitation data to sum only event hours (8 AM - 5 PM ET).
    Daily values are used for temperature, wind, and weather code.
    """
    if not dates:
        return {}

    result = {}
    dates = sorted(dates)

    # Batch by year to keep API requests reasonable
    years = sorted(set(d[:4] for d in dates))
    for year in years:
        year_dates = sorted(d for d in dates if d.startswith(year))
        start, end = year_dates[0], year_dates[-1]

        url = (
            f"https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={LAT}&longitude={LON}"
            f"&start_date={start}&end_date={end}"
            f"&daily=temperature_2m_max,temperature_2m_min,wind_speed_10m_max,weather_code"
            f"&hourly=precipitation"
            f"&temperature_unit=fahrenheit"
            f"&precipitation_unit=inch"
            f"&wind_speed_unit=mph"
        )

        print(f"  {year}: {start} to {end} ({len(year_dates)} events)...", end=" ", flush=True)
        try:
            with urllib.request.urlopen(url) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            print(f"\nERROR: Open-Meteo returned {e.code}. ERA5 data typically has a ~5 day lag,")
            print(f"so recent dates may not be available yet. Try again in a few days.")
            sys.exit(1)

        daily = data["daily"]
        hourly = data["hourly"]

        # Build hourly precip lookup: date -> {hour: precip}
        hourly_precip = {}
        for i, ts in enumerate(hourly["time"]):
            # ts is like "2021-04-11T13:00"
            d = ts[:10]
            h = int(ts[11:13])
            if d not in hourly_precip:
                hourly_precip[d] = {}
            hourly_precip[d][h] = hourly["precipitation"][i] or 0

        date_set = set(year_dates)
        found = 0
        for i, d in enumerate(daily["time"]):
            if d not in date_set:
                continue
            found += 1

            code = daily["weather_code"][i]
            tmax = round(daily["temperature_2m_max"][i], 1)
            tmin = round(daily["temperature_2m_min"][i], 1)

            # Sum precipitation only during event hours (DST-aware)
            day_hourly = hourly_precip.get(d, {})
            hours = event_hours_utc(d)
            event_precip = sum(day_hourly.get(h, 0) for h in hours)

            result[d] = {
                "tmax": tmax,
                "tmin": tmin,
                "tmid": round((tmax + tmin) / 2, 1),
                "precip": round(event_precip, 3),
                "wind": round(daily["wind_speed_10m_max"][i], 1),
                "code": code,
                "condition": WMO_CONDITIONS.get(code, f"❓ Unknown ({code})"),
            }

        print(f"got {found}/{len(year_dates)}")
        time.sleep(0.3)  # be nice to the API

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


def refetch_all():
    """Re-fetch weather data for all existing events."""
    with open(DATA_FILE) as f:
        data = json.load(f)

    dates = [e["date"] for e in data["events"]]
    print(f"Re-fetching weather for {len(dates)} events (event-hours precip: 8 AM - 5 PM ET)...")

    weather = fetch_weather(dates)

    updated = 0
    for e in data["events"]:
        w = weather.get(e["date"])
        if not w:
            print(f"  WARNING: No weather data for {e['date']} {e['name']}")
            continue
        e["tmax"] = w["tmax"]
        e["tmin"] = w["tmin"]
        e["tmid"] = w["tmid"]
        e["precip"] = w["precip"]
        e["wind"] = w["wind"]
        e["code"] = w["code"]
        e["condition"] = w["condition"]
        updated += 1

    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\nDone: updated {updated} events.")


def main():
    parser = argparse.ArgumentParser(description="Add NHSCC events with weather data")
    parser.add_argument("--date", help="Event date (YYYY-MM-DD)")
    parser.add_argument("--name", help="Event name")
    parser.add_argument("--drivers", type=int, help="Driver count")
    parser.add_argument("--batch", help="Batch input: date,name,drivers per line")
    parser.add_argument("--refetch", action="store_true",
                        help="Re-fetch weather for all existing events")
    args = parser.parse_args()

    if args.refetch:
        refetch_all()
        return

    if args.batch:
        new_events = parse_batch(args.batch)
    elif args.date and args.name and args.drivers is not None:
        new_events = [{"date": args.date, "name": args.name, "drivers": args.drivers}]
    else:
        parser.error("Provide --date/--name/--drivers, --batch, or --refetch")

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
    print(f"Fetching weather (event-hours precip: 8 AM - 5 PM ET)...")
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
