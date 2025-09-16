from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from typing import List, Optional
from datetime import datetime
import os

# Initialize FastAPI application
# To run: activate virtualenv , then use `uvicorn PostgRestAPI:app --reload --port 8000` in API folder
app = FastAPI()

# Allowed origins for CORS (Frontend access from React/Next.js)
origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000"
]

# Middleware to allow CORS (Cross-Origin Resource Sharing)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Function to load database configuration from a properties file
def load_properties(file_path):
    props = {}
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                props[key.strip()] = value.strip()
    return props

# Load configuration file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "..", "resources", "config.properties")
params = load_properties(CONFIG_PATH)

# Read database connection string
DATABASE_URL = params.get("db.rest.url")

# Create database engine and session
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
Base.metadata.create_all(bind=engine)

# ----------------- Pydantic Models (for API response) -----------------

# Response model for alerts
class AlertResponse(BaseModel):
    id: int
    sensor_id: str
    alert_type: str
    value: float
    alert_time: datetime

    class Config:
        orm_mode = True  # Allows returning SQLAlchemy objects as response


# Response model for sensor activity
class SensorActivityResponse(BaseModel):
    id: int
    sensor_id: str
    activity_date: datetime

    class Config:
        orm_mode = True


# Dependency function to get a database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ----------------- SQLAlchemy ORM Models (Database Tables) -----------------

# Sensor Alerts table
class Alert(Base):
    __tablename__ = "sensor_alerts"
    id = Column(Integer, primary_key=True, index=True)
    sensor_id = Column(String(50), nullable=False)
    alert_type = Column(String(20), nullable=False)
    value = Column(Float, nullable=False)
    alert_time = Column(DateTime, nullable=True)

# Sensor Activity table
class SensorActivity(Base):
    __tablename__ = "sensor_activity"
    id = Column(Integer, primary_key=True, index=True)
    sensor_id = Column(String(50), nullable=False)
    activity_date = Column(DateTime, nullable=False)

# ----------------- API Endpoints -----------------

# GET endpoint to read alerts with optional filtering by sensor_id
@app.get("/sensor_alerts/", response_model=List[AlertResponse])
def read_alerts(skip: int = 0, limit: int = 20, sensor_id: Optional[str] = None, db: Session = Depends(get_db)):
    query = db.query(Alert)
    
    if sensor_id:
        query = query.filter(Alert.sensor_id == sensor_id)

    alerts = query.offset(skip).limit(limit).all()
    return alerts

# DELETE endpoint to remove an alert by ID
@app.delete("/sensor_alerts/{alert_id}", response_model=AlertResponse)
def delete_alert(alert_id: int, db: Session = Depends(get_db)):
    alert = db.query(Alert).filter(Alert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")

    db.delete(alert)
    db.commit()
    return alert

# GET endpoint to read all sensor activities
@app.get("/sensor_activity/", response_model=List[SensorActivityResponse])
def read_activity(db: Session = Depends(get_db)):
    activities = db.query(SensorActivity).all()
    return activities
