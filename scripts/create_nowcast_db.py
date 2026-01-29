"""
Script to add sample typhoons (co-may and butchoy) to the database
"""

import os
import sys
import uuid
from datetime import datetime

# Add parent directory to path to import backend modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.repositories.nowcast_repository import NowcastRepository


def calculate_daily_stats(track_points, date, typhoon_name):
    """Calculate daily statistics from track points for a specific date."""
    # Filter track points for specific date
    daily_points = [p for p in track_points if p["datetime"].startswith(date)]

    if not daily_points:
        return None

    # Calculate statistics
    wind_speeds = [p["windSpeed"] for p in daily_points]
    cyclone_speeds = [p["cycloneSpeed"] for p in daily_points]

    # Generate dynamic distances and boat counts based on date and typhoon
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    day_factor = date_obj.day % 3  # Creates variation based on day

    if typhoon_name == "CO-MAY":
        # CO-MAY affects northern areas more
        base_distances = [350, 600, 300, 800]
        base_baseline = [25, 60, 35, 85]
        distance_variation = [50, 100, 30, 120]
        boat_variation = [5, 10, 8, 15]
    elif typhoon_name == "BUTCHOY":
        # BUTCHOY affects central areas more
        base_distances = [520, 312, 680, 450]
        base_baseline = [30, 45, 70, 55]
        distance_variation = [80, 50, 100, 70]
        boat_variation = [8, 12, 15, 10]
    else:
        # Default values
        base_distances = [500, 400, 600, 500]
        base_baseline = [30, 40, 50, 40]
        distance_variation = [60, 50, 80, 60]
        boat_variation = [6, 8, 10, 8]

    # Calculate dynamic distances and boat counts
    distances = [
        max(0, base + (day_factor - 1) * var) for base, var in zip(base_distances, distance_variation, strict=False)
    ]

    # Calculate boat counts with daily variation
    baseline = base_baseline
    predicted = [
        max(0, base + (day_factor - 1) * var) for base, var in zip(base_baseline, boat_variation, strict=False)
    ]

    # Calculate activity differences
    activity_diff = []
    for base, pred in zip(baseline, predicted, strict=False):
        if base == 0:
            diff = "+0%" if pred == 0 else "+∞%"
        else:
            percentage = ((pred - base) / base) * 100
            diff = f"{'+' if percentage >= 0 else ''}{percentage:.1f}%"
        activity_diff.append(diff)

    return {
        "date": date,
        "avgStormSpeed": f"{sum(cyclone_speeds) / len(cyclone_speeds):.1f} knots",
        "maxStormSpeed": f"{max(cyclone_speeds)} knots",
        "maxWindSpeed": f"{max(wind_speeds)} knots",
        "distances": distances,
        "boatCounts": {"baseline": baseline, "predicted": predicted},
        "activityDifference": activity_diff,
    }


def add_sample_typhoons():
    """Add co-may and butchoy typhoons to the database."""

    repo = NowcastRepository()

    # Add co-may typhoon
    co_may_data = {
        "uuid": str(uuid.uuid4()),
        "name": "CO-MAY",
        "type": "TY",
        "track_points": [
            {"lat": 14.5995, "lng": 120.9842, "datetime": "2025-07-23 06:00", "windSpeed": 65, "cycloneSpeed": 15},
            {"lat": 15.2000, "lng": 121.5000, "datetime": "2025-07-23 12:00", "windSpeed": 70, "cycloneSpeed": 16},
            {"lat": 15.8000, "lng": 122.1000, "datetime": "2025-07-24 00:00", "windSpeed": 68, "cycloneSpeed": 14},
            {"lat": 16.4000, "lng": 122.8000, "datetime": "2025-07-24 12:00", "windSpeed": 65, "cycloneSpeed": 12},
            {"lat": 17.0000, "lng": 123.5000, "datetime": "2025-07-25 00:00", "windSpeed": 60, "cycloneSpeed": 10},
        ],
        "daily_data": {
            "2025-07-23": calculate_daily_stats(
                [
                    {
                        "lat": 14.5995,
                        "lng": 120.9842,
                        "datetime": "2025-07-23 06:00",
                        "windSpeed": 65,
                        "cycloneSpeed": 15,
                    },
                    {
                        "lat": 15.2000,
                        "lng": 121.5000,
                        "datetime": "2025-07-23 12:00",
                        "windSpeed": 70,
                        "cycloneSpeed": 16,
                    },
                ],
                "2025-07-23",
                "CO-MAY",
            ),
            "2025-07-24": calculate_daily_stats(
                [
                    {
                        "lat": 15.8000,
                        "lng": 122.1000,
                        "datetime": "2025-07-24 00:00",
                        "windSpeed": 68,
                        "cycloneSpeed": 14,
                    },
                    {
                        "lat": 16.4000,
                        "lng": 122.8000,
                        "datetime": "2025-07-24 12:00",
                        "windSpeed": 65,
                        "cycloneSpeed": 12,
                    },
                ],
                "2025-07-24",
                "CO-MAY",
            ),
            "2025-07-25": calculate_daily_stats(
                [
                    {
                        "lat": 17.0000,
                        "lng": 123.5000,
                        "datetime": "2025-07-25 00:00",
                        "windSpeed": 60,
                        "cycloneSpeed": 10,
                    }
                ],
                "2025-07-25",
                "CO-MAY",
            ),
        },
        "created_at": datetime.now().isoformat(),
    }

    # Add butchoy typhoon
    butchoy_data = {
        "uuid": str(uuid.uuid4()),
        "name": "BUTCHOY",
        "type": "TS",
        "track_points": [
            {"lat": 12.5000, "lng": 123.0000, "datetime": "2025-07-20 06:00", "windSpeed": 85, "cycloneSpeed": 20},
            {"lat": 13.2000, "lng": 122.5000, "datetime": "2025-07-20 12:00", "windSpeed": 90, "cycloneSpeed": 22},
            {"lat": 13.9000, "lng": 122.0000, "datetime": "2025-07-21 00:00", "windSpeed": 95, "cycloneSpeed": 24},
            {"lat": 14.6000, "lng": 121.5000, "datetime": "2025-07-21 12:00", "windSpeed": 88, "cycloneSpeed": 20},
            {"lat": 15.3000, "lng": 121.0000, "datetime": "2025-07-22 00:00", "windSpeed": 75, "cycloneSpeed": 18},
        ],
        "daily_data": {
            "2025-07-20": calculate_daily_stats(
                [
                    {
                        "lat": 12.5000,
                        "lng": 123.0000,
                        "datetime": "2025-07-20 06:00",
                        "windSpeed": 85,
                        "cycloneSpeed": 20,
                    },
                    {
                        "lat": 13.2000,
                        "lng": 122.5000,
                        "datetime": "2025-07-20 12:00",
                        "windSpeed": 90,
                        "cycloneSpeed": 22,
                    },
                ],
                "2025-07-20",
                "BUTCHOY",
            ),
            "2025-07-21": calculate_daily_stats(
                [
                    {
                        "lat": 13.9000,
                        "lng": 122.0000,
                        "datetime": "2025-07-21 00:00",
                        "windSpeed": 95,
                        "cycloneSpeed": 24,
                    },
                    {
                        "lat": 14.6000,
                        "lng": 121.5000,
                        "datetime": "2025-07-21 12:00",
                        "windSpeed": 88,
                        "cycloneSpeed": 20,
                    },
                ],
                "2025-07-21",
                "BUTCHOY",
            ),
            "2025-07-22": calculate_daily_stats(
                [
                    {
                        "lat": 15.3000,
                        "lng": 121.0000,
                        "datetime": "2025-07-22 00:00",
                        "windSpeed": 75,
                        "cycloneSpeed": 18,
                    }
                ],
                "2025-07-22",
                "BUTCHOY",
            ),
        },
        "created_at": datetime.now().isoformat(),
    }

    # Insert into database
    try:
        # Insert co-may
        repo.insert(co_may_data)
        print(f"Added CO-MAY typhoon with UUID: {co_may_data['uuid']}")

        # Insert butchoy
        repo.insert(butchoy_data)
        print(f"Added BUTCHOY typhoon with UUID: {butchoy_data['uuid']}")

        # Verify
        typhoon_count = len(repo.get_all())
        print(f"Total typhoons in database: {typhoon_count}")

    except Exception as e:
        print(f"Error adding typhoons: {e}")

    repo.close()


if __name__ == "__main__":
    add_sample_typhoons()
