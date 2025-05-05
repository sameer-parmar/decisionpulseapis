from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.database import get_db  # Ensure this is correctly imported
from app.models.datapoints import DataPoint, Country, Category, Brand # Import the models
from app.utils import safe_float # Import safe_float
from fastapi import HTTPException

router = APIRouter()

@router.get("/channel-performance/")
def get_channel_performance(
    db: Session = Depends(get_db),
    year: str = Query(None, description="Filter by year"),
    country_name: str = Query(None, description="Filter by country name"),
    category_name: str = Query(None, description="Filter by category name"),
    brand_name: str = Query(None, description="Filter by brand name"),
):
    """
    Retrieves channel performance data, filtered by year, country, category and brand.

    Args:
        db (Session): The database session.
        year (str, optional): The year to filter by. Defaults to None.
        country_name (str, optional): The country name to filter by. Defaults to None.
        category_name (str, optional): The category name to filter by. Defaults to None.
        brand_name (str, optional): The brand name to filter by. Defaults to None.

    Returns:
        list: A list of dictionaries, where each dictionary represents channel performance
              data. Returns an empty list if no matching data is found.
              Handles the case where the value is None.

    Raises:
        HTTPException: 400 if the year format is invalid.
    """
    query = db.query(DataPoint).filter(DataPoint.metric_category == 'ecommerce_digital').filter(DataPoint.metric.ilike("%sales%"))

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
    channel_data = {}
    for item in data:
        channel = "Online"  # Assuming 'ecommerce_digital' category implies online channel
        key = (item.year, item.country, channel)
        if key not in channel_data:
            channel_data[key] = {"year": item.year, "country": item.country, "channel": channel, "total_sales": 0}
        value = safe_float(item.value) # Use safe_float
        if value is not None:
            channel_data[key]["total_sales"] += value
    return list(channel_data.values())

