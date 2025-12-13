import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def get_db_session():
    """Returns a new DB session based on .env configuration."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL not set in .env")
        
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    return Session()