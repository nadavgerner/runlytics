# src/runlytics/data_ingest/strava_pull.py
import os
import requests
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
from src.runlytics.data_ingest.strava_auth import refresh_access_token, update_env
from src.runlytics.pipelines.indoor_handler import handle_indoor_run

load_dotenv()
STRAVA_BASE = "https://www.strava.com/api/v3"


def get_latest_timestamp(parquet_path: str = "data/raw/strava_activities.parquet") -> float | None:
    """
    Return the Unix timestamp of the most recent activity already stored.

    Returns float | None
        Timestamp (seconds since epoch) of the latest activity, or None if the file does not exist.
    """
    path = Path(parquet_path)
    if path.exists():
        df = pd.read_parquet(path)
        if "start_date" in df.columns:
            latest = pd.to_datetime(df["start_date"]).max()
            print(f"Latest saved activity date: {latest}")
            return latest.timestamp()
    return None


def get_activities(token: str, per_page: int = 200, after_ts: float | None = None):
    """
    Retrieve all Strava activities for the authenticated athlete using full pagination.
    Stops automatically when no further data are returned.
    """
    all_acts = []
    page = 1

    while True:
        params = {"per_page": per_page, "page": page}
        if after_ts:
            params["after"] = int(after_ts)

        resp = requests.get(
            f"{STRAVA_BASE}/athlete/activities",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
        )

        # Handle expired token
        if resp.status_code == 401:
            print("Access token expired. Refreshing...")
            tokens = refresh_access_token()
            update_env(tokens)
            token = tokens["access_token"]
            continue

        resp.raise_for_status()
        data = resp.json()

        if not data:
            print(f"No more data at page {page}.")
            break

        all_acts.extend(data)
        print(f"Fetched page {page} ({len(data)} activities)")

        # Safety stop if something weird (repeating same IDs)
        if page > 1 and len(data) < per_page:
            print("Last partial page fetched, stopping.")
            break

        page += 1

    print(f"Total fetched: {len(all_acts)}")
    return all_acts


def save_or_append_activities(new_data, outpath: str = "data/raw/strava_activities.parquet") -> None:
    """
    Save newly fetched activities to Parquet, appending to existing data if present.

    Duplicate 'id' values are removed, preserving the most recent record.
    """
    df_new = pd.json_normalize(new_data)
    out_path = Path(outpath)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        df_old = pd.read_parquet(out_path)
        combined = pd.concat([df_old, df_new]).drop_duplicates(subset=["id"], keep="last")
    else:
        combined = df_new

    combined.to_parquet(out_path, index=False)
    print(f"Saved {len(combined)} total activities ({len(df_new)} newly fetched).")


if __name__ == "__main__":
    """Main function for Strava ingestion with indoor-run corrections."""
    token = os.getenv("STRAVA_ACCESS_TOKEN")
    if not token:
        raise ValueError("No STRAVA_ACCESS_TOKEN found in .env")

    latest = get_latest_timestamp()
    activities = get_activities(token, per_page=200, after_ts=latest)

    if not activities:
        print("No new activities found.")
    else:
        df_new = pd.json_normalize(activities)
        indoor_mask = df_new.get("type").eq("Run") & (
            df_new.get("trainer", False) | df_new.get("sport_type").eq("VirtualRun")
        )
        indoor_runs = df_new[indoor_mask]
        if not indoor_runs.empty:
            print(f"{len(indoor_runs)} indoor runs found — launching manual handler.")
            handle_indoor_run(indoor_runs)

        save_or_append_activities(activities)
        print("Incremental update complete.")
