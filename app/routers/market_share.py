from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.database import get_db  # Ensure this is correctly imported
from app.models.datapoints import DataPoint, Country, Category, Brand # Import the models
from app.utils import safe_float # Import safe_float
from fastapi import HTTPException

router = APIRouter()

@router.get("/market-share/")
def get_market_share(
    db: Session = Depends(get_db),
    year: str = Query(None, description="Filter by year"),
    country_name: str = Query(None, description="Filter by country name"),
    category_name: str = Query(None, description="Filter by category name"),
    brand_name: str = Query(None, description="Filter by brand name"),
):
    """
    Retrieves market share data, filtered by year, country, category and brand.

    Args:
        db (Session): The database session.
        year (str, optional): The year to filter by. Defaults to None.
        country_name (str, optional): The country name to filter by. Defaults to None.
        category_name (str, optional): The category name to filter by. Defaults to None.
        brand_name (str, optional): The brand name to filter by. Defaults to None.

    Returns:
        list: A list of dictionaries, where each dictionary represents market share data.
              Returns an empty list if no matching data is found.
              Handles the case where the value is None.

    Raises:
        HTTPException: 400 if the year format is invalid.
    """
    query = db.query(DataPoint).filter(DataPoint.metric_category == 'competitive_intelligence').filter(DataPoint.metric.ilike("%market share%"))

    # Join tables to filter by name
    if country_name:
        query = query.join(Country, DataPoint.country == Country.id).filter(Country.name == country_name)
    if category_name:
        query = query.join(Category, DataPoint.category == Category.id).filter(Category.name == category_name)
    if brand_name:
        query = query.join(Brand, DataPoint.brand == Brand.id).filter(Brand.name == brand_name)

    # Apply year filter
    if year:
        try:
            query = query.filter(DataPoint.year == year)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid year format. Must be a valid year.")

    data = query.all()
    # Use safe_float
    return [{"year": item.year, "country": item.country, "metric": item.metric, "value": safe_float(item.value), "brand": item.brand} for item in data]
