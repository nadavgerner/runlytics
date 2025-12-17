import os
import logging
from fastapi import FastAPI, Request, HTTPException, Security, Header
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert

# --- IMPORTS ---
from runlytics.database.models import Base, Run, Biometric
from runlytics.processing.health_parser import HealthParser
from runlytics.ingestion.journal_ingest import sync_journal_entry_point
from runlytics.ingestion.strava_ingest import sync_strava_entry_point

# --- LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("webhook")

app = FastAPI()

# --- DATABASE SETUP ---
DATABASE_URL = os.getenv("DATABASE_URL")

# Fix for Render/Supabase SSL requirements
if DATABASE_URL and "sslmode" not in DATABASE_URL:
    DATABASE_URL += "?sslmode=require"

engine = None
SessionLocal = None

if DATABASE_URL:
    try:
        # pool_pre_ping=True prevents "Database has gone away" errors
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        
        # Create tables if they don't exist
        Base.metadata.create_all(bind=engine)
        logger.info("Database connection established and tables checked.")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")

# --- SECURITY ---
API_KEY_NAME = "X-API-KEY"
EXPECTED_API_KEY = os.getenv("API_KEY", "default_insecure_key")

async def get_api_key(api_key_header: str = Header(None, alias=API_KEY_NAME)):
    if api_key_header != EXPECTED_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return api_key_header

# --- ROUTES ---

@app.get("/")
def health_check():
    return {"status": "online", "service": "Runlytics V5"}

# 1. APPLE HEALTH TRIGGER
@app.post("/ingest")
async def ingest_data(request: Request, api_key: str = Security(get_api_key)):
    """Receives JSON from Health Auto Export (iOS)."""
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    payload = await request.json()
    parser = HealthParser(payload)
    session = SessionLocal()

    try:
        # A. Process Biometrics
        biometrics_data = parser.parse_biometrics()
        if biometrics_data:
            stmt = insert(Biometric).values(biometrics_data)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=['date', 'type', 'source'] 
            )
            session.execute(stmt)
        
        # B. Process Runs (Apple Watch)
        runs_data = parser.parse_workouts()
        if runs_data:
            for r_data in runs_data:
                local_run = session.merge(Run(**r_data))
                session.add(local_run)

        session.commit()
        
        count_b = len(biometrics_data)
        count_r = len(runs_data)
        logger.info(f"Apple Ingestion Success: {count_b} metrics, {count_r} runs.")
        
        return {"status": "success", "metrics_saved": count_b, "runs_saved": count_r}

    except Exception as e:
        session.rollback()
        logger.error(f"Apple Ingestion Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

# 2. STRAVA TRIGGER
@app.post("/sync/strava")
async def trigger_strava(api_key: str = Security(get_api_key)):
    """Triggers the Strava Python Script."""
    try:
        status_message = sync_strava_entry_point()
        logger.info(f"Strava Trigger: {status_message}")
        return {"status": "success", "message": status_message}
    except Exception as e:
        logger.error(f"Strava Trigger Failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 3. JOURNAL TRIGGER
@app.post("/sync/journal")
async def trigger_journal(api_key: str = Security(get_api_key)):
    """Triggers the Google Sheet Python Script."""
    try:
        status_message = sync_journal_entry_point()
        logger.info(f"Journal Trigger: {status_message}")
        return {"status": "success", "message": status_message}
    except Exception as e:
        logger.error(f"Journal Trigger Failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))