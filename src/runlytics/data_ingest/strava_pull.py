import os
import requests
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()  # loads variables from .env

STRAVA_BASE = "https://www.strava.com/api/v3"

def get_activities(token: str, per_page: int = 30):
    """Fetch latest activities from Strava API."""
    resp = requests.get(
        f"{STRAVA_BASE}/athlete/activities",
        headers={"Authorization": f"Bearer {token}"},
        params={"per_page": per_page}
    )
    resp.raise_for_status()
    return resp.json()

def save_activities(data, outpath="data/raw/strava_activities.parquet"):
    """Save Strava activities as Parquet file."""
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    df = pd.json_normalize(data)
    df.to_parquet(outpath, index=False)
    print(f"Saved {len(df)} activities to {outpath}")

if __name__ == "__main__":
    token = os.getenv("STRAVA_ACCESS_TOKEN")
    if not token:
        raise ValueError("No STRAVA_ACCESS_TOKEN in .env")
    activities = get_activities(token, per_page=50)
    save_activities(activities)
