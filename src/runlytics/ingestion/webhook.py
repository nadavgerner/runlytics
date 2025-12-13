import os
import logging
from fastapi import FastAPI, Request, HTTPException, Security, Header
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# --- IMPORTS ---
# Ensure these match your folder structure exactly
from src.runlytics.database.models import Base, Run, Biometric
from src.runlytics.processing.health_parser import HealthParser

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

@app.post("/ingest")
async def ingest_data(request: Request, api_key: str = Security(get_api_key)):
    """Receives JSON from Health Auto Export."""
    if not SessionLocal:
        raise HTTPException(status_code=500, detail="Database not configured")

    payload = await request.json()
    parser = HealthParser(payload)
    session = SessionLocal()

    try:
        # 1. Process Biometrics
        biometrics_data = parser.parse_biometrics()
        if biometrics_data:
            # Using bulk_insert_mappings is much faster for thousands of rows
            session.bulk_insert_mappings(Biometric, biometrics_data)
        
        # 2. Process Runs
        runs_data = parser.parse_workouts()
        if runs_data:
            for r_data in runs_data:
                # Merge handles updates/duplicates (Upsert)
                local_run = session.merge(Run(**r_data))
                session.add(local_run)

        session.commit()
        
        count_b = len(biometrics_data)
        count_r = len(runs_data)
        logger.info(f"Ingestion Success: {count_b} metrics, {count_r} runs.")
        
        return {"status": "success", "metrics_saved": count_b, "runs_saved": count_r}

    except Exception as e:
        session.rollback()
        logger.error(f"Ingestion Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()