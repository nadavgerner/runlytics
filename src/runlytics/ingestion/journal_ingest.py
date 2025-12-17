import os
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Load Environment
load_dotenv()

# --- CONFIG ---
SHEET_URL = "https://docs.google.com/spreadsheets/d/1raS0HJtnGqNn397I1W_AmWEuLjTdKS-K1JprSNwL6IY/edit?resourcekey=&gid=904959031#gid=904959031"

# Database Connection
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is missing from .env")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

def get_google_sheet_data():
    """Authenticates and pulls raw data."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_path = "google_credentials.json"
    
    if not os.path.exists(creds_path):
        raise FileNotFoundError("Missing google_credentials.json")
        
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    # Open by URL is safer than filename
    return client.open_by_url(SHEET_URL).sheet1.get_all_records()

def find_header_map(available_headers):
    """
    Creates a map of 'DatabaseColumn' -> 'SheetColumn' dynamically.
    Returns a dict like: {'mood': 'Mood / Motivation ', 'rpe': 'RPE (Exertion)'}
    """
    # Define what we are looking for (Key = DB field, Value = Keyword to search)
    targets = {
        "date": "Date",
        "rpe": "RPE",
        "mood": "Mood",
        "soreness": "Soreness",
        "knee_pain": "Knee",
        "sleep_quality": "Sleep",
        "notes": "Notes"
    }
    
    header_map = {}
    
    # Efficient O(N*M) search
    for db_field, keyword in targets.items():
        found = False
        for sheet_header in available_headers:
            if keyword.lower() in sheet_header.lower():
                header_map[db_field] = sheet_header
                found = True
                break
        if not found:
            print(f"Warning: Could not find a column matching '{keyword}'")
            
    return header_map

def upload_journal_to_supabase(data, session):
    if not data:
        return

    # 1. Map headers once
    headers = list(data[0].keys())
    col_map = find_header_map(headers)
    
    if "date" not in col_map:
        print("Error: Could not find a Date column. Aborting.")
        return

    # 2. Prepare the Payload (List of Dictionaries)
    bulk_data = []
    
    for row in data:
        raw_date = row.get(col_map["date"])
        if not raw_date: continue
            
        try:
            date_obj = pd.to_datetime(raw_date).date()
            
            # Clean Dictionary Construction
            entry = {
                "date": date_obj,
                "rpe": row.get(col_map.get("rpe")),
                "mood": row.get(col_map.get("mood")),
                "soreness": row.get(col_map.get("soreness")),
                "knee_pain": row.get(col_map.get("knee_pain")),
                "sleep_quality": row.get(col_map.get("sleep_quality")),
                "notes": row.get(col_map.get("notes"))
            }
            
            # Clean empty strings
            entry = {k: (v if v != "" else None) for k, v in entry.items()}
            bulk_data.append(entry)
            
        except Exception as e:
            print(f"Skipping invalid row: {e}")

    # 3. Bulk Insert (One Database Call)
    if bulk_data:
        sql = text("""
            INSERT INTO daily_journal (date, rpe, mood, soreness, knee_pain, sleep_quality, notes)
            VALUES (:date, :rpe, :mood, :soreness, :knee_pain, :sleep_quality, :notes)
            ON CONFLICT (date) 
            DO UPDATE SET 
                rpe = EXCLUDED.rpe,
                mood = EXCLUDED.mood,
                soreness = EXCLUDED.soreness,
                knee_pain = EXCLUDED.knee_pain,
                sleep_quality = EXCLUDED.sleep_quality,
                notes = EXCLUDED.notes;
        """)
        
        session.execute(sql, bulk_data) 
        session.commit()
        print(f"Successfully synced {len(bulk_data)} entries in one transaction.")

def sync_journal_entry_point():
    """
    Wrapper for external calls (like from the webhook).
    Returns a status message.
    """
    session = SessionLocal()
    try:
        print("Starting Journal Sync...")
        data = get_google_sheet_data()
        upload_journal_to_supabase(data, session)
        return "Journal Sync Complete"
    except Exception as e:
        print(f"Journal Sync Failed: {e}")
        raise e
    finally:
        session.close()

if __name__ == "__main__":
    session = SessionLocal()
    try:
        print("Connecting to Google Sheets...")
        raw_data = get_google_sheet_data()
        
        print(f"Fetched {len(raw_data)} rows. Uploading to database...")
        upload_journal_to_supabase(raw_data, session)
            
    except Exception as e:
        print(f"Script failed: {e}")
    finally:
        session.close()