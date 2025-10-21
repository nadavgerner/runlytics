import json
from pathlib import Path
import pandas as pd

CORRECTION_PATH = Path("data/processed/manual_corrections.json")


def load_corrections():
    """Load existing manual corrections log."""
    if CORRECTION_PATH.exists():
        with open(CORRECTION_PATH, "r") as f:
            return json.load(f)
    return {}


def save_corrections(corrections: dict):
    """Save updated manual corrections log."""
    CORRECTION_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CORRECTION_PATH, "w") as f:
        json.dump(corrections, f, indent=2)


def prompt_correction(row):
    """Ask user whether to correct this indoor run."""
    print()
    print(f"Indoor run: {row.get('name','(Unnamed)')}")
    print(f"Date: {row.get('start_date_local')}")
    print(f"Distance: {row.get('distance',0)/1000:.2f} km | "
          f"Duration: {row.get('elapsed_time',0)/60:.1f} min | "
          f"Avg HR: {row.get('average_heartrate', '—')}")

    choice = input("Keep Strava values? [Enter=keep / m=manual] ").strip().lower()
    if choice != "m":
        return None  # keep Strava values

    mode = input("Simple continuous run or structured intervals? [Enter=simple / i=interval] ").strip().lower()

    if mode != "i":
        # Simple continuous correction
        current_dist = row.get("distance", 0) / 1000
        current_dur = row.get("elapsed_time", 0) / 60
        dist = float(input(f"Actual distance (km) [{current_dist:.2f}]: ") or current_dist)
        dur = float(input(f"Actual duration (min) [{current_dur:.1f}]: ") or current_dur)
        intensity = input("Perceived intensity (1–10) [7]: ") or "7"

        corrected = {
            "distance_km": dist,
            "duration_min": dur,
            "intensity": int(intensity),
            "corrected_on": pd.Timestamp.now().isoformat(),
        }
        print(f"Saved correction: {dist:.2f} km, {dur:.1f} min, intensity {intensity}")
        return corrected

    # Structured interval correction
    try:
        num_intervals = int(input("Number of intervals: "))
        intervals = []
        total_distance = 0.0
        total_minutes = 0.0

        for i in range(1, num_intervals + 1):
            d = float(input(f"Interval {i} distance (km): "))
            s = float(input(f"Interval {i} speed (km/h): "))
            r = float(input(f"Rest after interval (sec) [0]: ") or 0)
            intervals.append({"distance_km": d, "speed_kmh": s, "rest_sec": r})
            total_distance += d
            total_minutes += (d / s) * 60 + (r / 60)

        intensity = input("Overall perceived intensity (1–10) [8]: ") or "8"
        note = input("Optional notes: ").strip()

        corrected = {
            "distance_km": round(total_distance, 2),
            "duration_min": round(total_minutes, 1),
            "intensity": int(intensity),
            "intervals": intervals,
            "notes": note,
            "corrected_on": pd.Timestamp.now().isoformat(),
        }
        print(f"Saved structured session: {total_distance:.2f} km, {total_minutes:.1f} min, intensity {intensity}")
        return corrected
    except Exception as e:
        print("Error during structured entry:", e)
        return None


def handle_indoor_run(df: pd.DataFrame):
    """Prompt for manual correction of indoor runs."""
    df = df.copy()
    corrections = load_corrections()
    updated = False

    for _, row in df.iterrows():
        run_id = str(row["id"])
        if run_id in corrections:
            continue  # already corrected

        correction = prompt_correction(row)
        if correction:
            corrections[run_id] = correction
            updated = True

    if updated:
        save_corrections(corrections)
        print(f"Updated {len(corrections)} total manual corrections.")
    else:
        print("No new corrections added.")

    return corrections
