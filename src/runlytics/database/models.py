from sqlalchemy import Column, Integer, String, Float, DateTime, JSON
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Run(Base):
    __tablename__ = 'runs'

    id = Column(String, primary_key=True)
    date = Column(DateTime, index=True)
    duration_min = Column(Float)
    distance_km = Column(Float)
    avg_hr = Column(Integer)
    max_hr = Column(Integer)
    energy_kcal = Column(Float)
    source = Column(String)
    route_json = Column(JSON)  # Stores the GPS path

class Biometric(Base):
    __tablename__ = 'biometrics'

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, index=True)
    type = Column(String, index=True)
    value = Column(Float)
    unit = Column(String)
    source = Column(String)