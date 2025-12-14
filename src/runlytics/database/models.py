from sqlalchemy import Column, Integer, String, Float, DateTime, BigInteger, JSON
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class Run(Base):
    __tablename__ = "runs"

    # Explicitly setting autoincrement=True for BigInteger
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    date = Column(DateTime(timezone=True), unique=True, nullable=False)
    distance_km = Column(Float)
    duration_min = Column(Float)
    avg_hr = Column(Float)
    max_hr = Column(Float)
    energy_kcal = Column(Float)
    source = Column(String)
    route_json = Column(JSON)  # Stores the GPS map data
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class Biometric(Base):
    __tablename__ = 'biometrics'

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, index=True)
    type = Column(String, index=True)
    value = Column(Float)
    unit = Column(String)
    source = Column(String)