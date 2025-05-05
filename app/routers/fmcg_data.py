
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db  # Ensure this is correctly imported
from app.models.datapoints import DataPoint  # Ensure this is correctly imported
from typing import List

router = APIRouter()
@router.get("/fmcg-data/", response_model=List[dict])  # Define a Pydantic model for response if needed
def read_fmcg_data(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    data = db.query(DataPoint).order_by(DataPoint.id).offset(skip).limit(limit).all()
    # Convert SQLAlchemy objects to dictionaries for JSON response
    serialized_data = [
        {
            "id": dp.id,
            "source_url": dp.source_url,
            "insight": dp.insight,
            "year": dp.year,
            "brand": dp.brand,
            "metric": dp.metric,
            "value": dp.value,
            "country": dp.country,
            "summary": dp.summary,
        }
        for dp in data
    ]
    return serialized_data
