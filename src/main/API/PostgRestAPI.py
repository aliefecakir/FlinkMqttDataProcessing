from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from typing import List, Optional
from datetime import datetime

app = FastAPI()

def load_properties(file_path):
    props = {}
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                props[key.strip()] = value.strip()
    return props

params = load_properties(r"C:\Users\Ally\IdeaProjects\MqttFlinkPipeline\src\main\resources\config.properties")

DATABASE_URL = params.get("db.rest.url")
print(DATABASE_URL)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
Base.metadata.create_all(bind=engine)

class AlertResponse(BaseModel):
    id: int
    sensor_id: str
    alert_type: str
    value: float
    alert_time: datetime

    class Config:
        orm_mode = True

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class Alert(Base):
    __tablename__ = "sensor_alerts"
    id = Column(Integer, primary_key=True, index=True)
    sensor_id = Column(String(50), nullable=False)
    alert_type = Column(String(20), nullable=False)
    value = Column(Float, nullable=False)
    alert_time = Column(DateTime, nullable=True)

@app.get("/sensor_alerts/", response_model=List[AlertResponse])
def read_alerts(skip: int = 0, limit: int = 20,sensor_id: Optional[str] = None, db: Session = Depends(get_db)):
    query = db.query(Alert)
    
    if sensor_id:
        query = query.filter(Alert.sensor_id == sensor_id)

    alerts = query.offset(skip).limit(limit).all()
    return alerts

@app.delete("/sensor_alerts/{alert_id}", response_model=AlertResponse)
def delete_alert(alert_id: int, db: Session = Depends(get_db)):
    alert = db.query(Alert).filter(Alert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="User not found")

    db.delete(alert)
    db.commit()
    return alert
