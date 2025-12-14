import os
import requests
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Corrected Import Path
from runlytics.database.models import Run
from runlytics.ingestion.strava_auth import refresh_access_token, update_env

load_dotenv()

# Configuration
STRAVA_BASE = "https://www.strava.com/api/v3"
DATABASE_URL = os.getenv("DATABASE_URL")

# Database Connection
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

def get_latest_db_timestamp(session):
    """
    Queries Supabase to find the timestamp of the last imported Strava run.
    Used for incremental syncing.
    """
    query = text("SELECT MAX(date) FROM runs WHERE source = 'strava'")
    result = session.execute(query).scalar()
    
    if result:
        print(f"Latest Strava run in DB: {result}")
        return result.timestamp()
    
    print("No Strava runs found in DB. Initiating full history fetch...")
    return 0

def fetch_activities(token, after_ts=0):
    """
    Recursively fetches activities from Strava API using pagination.
    """
    activities = []
    page = 1
    
    while True:
        resp = requests.get(
            f"{STRAVA_BASE}/athlete/activities",
            headers={"Authorization": f"Bearer {token}"},
            params={"per_page": 50, "page": page, "after": int(after_ts)}
        )
        
        if resp.status_code == 401:
            print("Access Token Expired. Refreshing...")
            token = refresh_access_token()
            update_env(token)
            continue
            
        if resp.status_code != 200:
            print(f"Error fetching data: {resp.text}")
            break
            
        data = resp.json()
        if not data:
            break
            
        activities.extend(data)
        print(f"Page {page}: Fetched {len(data)} items")
        page += 1
        
    return activities

def upload_to_supabase(activities, session):
    """
    Transforms raw Strava JSON into Run objects and bulk-inserts new ones.
    """
    if not activities:
        return

    # 1. Fetch ALL existing dates from DB at once for fast duplicate checking
    print("Checking for duplicates...")
    existing_dates = set(
        dt[0].replace(tzinfo=None) for dt in session.query(Run.date).all()
    )

    new_runs = []
    
    for act in activities:
        if act.get('type') != 'Run':
            continue

        # Parse date (Strava format: 2024-12-14T10:00:00Z)
        run_date_raw = datetime.strptime(act['start_date_local'], "%Y-%m-%dT%H:%M:%SZ")
        
        # Skip if we already have this date
        if run_date_raw in existing_dates:
            continue

        # Create the Run Object
        run_entry = Run(
            date=run_date_raw,
            distance_km=round(act['distance'] / 1000, 2),
            duration_min=round(act['moving_time'] / 60, 2),
            avg_hr=act.get('average_heartrate'),
            max_hr=act.get('max_heartrate'), # Added per request
            energy_kcal=act.get('kilojoules'), 
            source="strava",
            route_json=act.get('map', {})
        )
        new_runs.append(run_entry)

    # 2. Bulk Save
    if new_runs:
        session.add_all(new_runs)
        session.commit()
        print(f"Successfully uploaded {len(new_runs)} new runs to Supabase.")
    else:
        print("No new runs to upload (all duplicates).")

if __name__ == "__main__":
    session = SessionLocal()
    try:
        # 1. Auth Init
        token = os.getenv("STRAVA_ACCESS_TOKEN")
        if not token:
            token = refresh_access_token()
            update_env(token)

        # 2. Sync Logic
        last_ts = get_latest_db_timestamp(session)
        runs = fetch_activities(token, after_ts=last_ts)

        # 3. Database Write
        if runs:
            upload_to_supabase(runs, session)
        else:
            print("No new runs to sync.")

    except Exception as e:
        print(f"Script execution failed: {e}")
    finally:
        session.close()