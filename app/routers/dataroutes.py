from fastapi import APIRouter, Depends
from app.schemas.datapointschema import CategoryWithCountry , CountrySchema, BrandFilterRequest, CompareBrandsRequest ,ChartDataResponse,CompareResponse,AvailableResponse,Filters
from sqlalchemy.orm import Session
from app.database import  get_db
from typing import List,  Dict, Any, Optional, Tuple
from sqlalchemy import Float, cast, distinct, func
from app.schemas.datapointschema import Dataset
from app.models.datapoints import Country
from app.models.datapoints import Brand
from app.models.datapoints import DataPoint

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from typing import List, Optional
from sqlalchemy.orm import Session
from app.database import get_db
from app.models.datapoints import Category
router = APIRouter()


@router.get("/available", response_model=AvailableResponse)
def get_available_filters(db: Session = Depends(get_db)):
    # pull all distincts
    metrics = [m[0] for m in db.query(DataPoint.metric).distinct().all() if m[0]]
    metric_categories = [c[0] for c in db.query(DataPoint.metric_category).distinct().all() if c[0]]
    brands = [b[0] for b in db.query(Brand.name).distinct().all() if b[0]]
    years = [y[0] for y in db.query(DataPoint.year).distinct().all() if y[0]]
    categories = [c[0] for c in db.query(Category.name).distinct().all() if c[0]]
    countries = [c[0] for c in db.query(Country.name).distinct().all() if c[0]]

    return AvailableResponse(filters=Filters(
        metrics=metrics,
        metric_categories=metric_categories,
        brands=brands,
        years=years,
        categories=categories,
        countries=countries
    ))

@router.get("/categories-with-countries/", response_model=List[CategoryWithCountry])
def get_categories_with_countries(db: Session = Depends(get_db)):
    categories = db.query(Category).all()
    results = []

    for cat in categories:
        results.append(CategoryWithCountry(
            category_id=cat.id,
            category_name=cat.name,
            country_id=cat.country,
            country_name=cat.country_rel.name if cat.country_rel else None  # assumes FK relationship is `country_rel`
        ))

    return results

@router.get("/countries/", response_model=List[CountrySchema])  # Use Pydantic schema for response
def get_countries(db: Session = Depends(get_db)):
    countries = db.query(Country).all()  # Query the SQLAlchemy model
    return [CountrySchema(**country.__dict__) for country in countries]  # Convert ORM objects to Pydantic models

# @router.post("/brands/", response_model=List[BrandSchema])
# def get_brands(filter_data: BrandFilter, db: Session = Depends(get_db)):
#     query = db.query(Brand).filter(Brand.category == filter_data.category)

#     if filter_data.country:  # Apply country filter only if provided
#         query = query.filter(Brand.country == filter_data.country)

#     brands = query.all()
#     return brands
@router.get("/datapoints/metrics/", response_model=List[dict])
def get_metrics(db: Session = Depends(get_db)):
    metrics = db.query(DataPoint.metric, DataPoint.metric_category).distinct().all()
    return [{"metric": m[0], "category": m[1]} for m in metrics]


# router = APIRouter(
#     prefix="/api/metrics",
#     tags=["metrics"],
# )

@router.post(
    "/brands",
    response_model=ChartDataResponse,
    summary="POST: Get pivoted chart data for selected brands"
)
def post_metrics_by_brands(
    payload: BrandFilterRequest,
    db: Session = Depends(get_db)
):
    # build base query
    q = (
        db.query(
            Brand.name.label("brand"),
            DataPoint.year,
            DataPoint.value,
            DataPoint.unit,
        )
        .join(Brand, DataPoint.brand == Brand.id)
        .join(Category, DataPoint.category == Category.id, isouter=True)
        .join(Country, DataPoint.country == Country.id, isouter=True)
    )

    # apply filters (brands, metrics, metric_categories, years, categories, countries)
    if payload.brands:
        q = q.filter(func.lower(Brand.name).in_([b.lower() for b in payload.brands]))
    if payload.metrics:
        q = q.filter(func.lower(DataPoint.metric).in_([m.lower() for m in payload.metrics]))
    if payload.metric_categories:
        q = q.filter(func.lower(DataPoint.metric_category).in_([c.lower() for c in payload.metric_categories]))
    if payload.years:
        q = q.filter(DataPoint.year.in_(payload.years))
    if payload.categories:
        q = q.filter(func.lower(Category.name).in_([c.lower() for c in payload.categories]))
    if payload.countries:
        q = q.filter(func.lower(Country.name).in_([c.lower() for c in payload.countries]))

    rows = q.order_by(Brand.name, DataPoint.year).limit(payload.limit).all()

    if not rows:
        return ChartDataResponse(labels=[], datasets=[])

    # pivot into labels & datasets
    years = sorted({ r.year for r in rows })
    brands = sorted({ r.brand for r in rows })
    pivot: Dict[str, Dict[str, Optional[float]]] = { b: {} for b in brands }
    unit_map: Dict[str, Optional[str]] = {}

    for r in rows:
        try:
            val = float(r.value) if r.value is not None else None
        except (ValueError, TypeError):
            val = None
        pivot[r.brand][r.year] = val
        unit_map[r.brand] = r.unit

    datasets = [
        Dataset(
            label=b,
            data=[ pivot[b].get(y) for y in years ],
            unit=unit_map.get(b)
        )
        for b in brands
    ]

    return ChartDataResponse(labels=years, datasets=datasets)


# -------------------------------
# 4) /metrics/compare → POST
# -------------------------------

@router.post(
    "/compare",
    response_model=CompareResponse,
    summary="POST: Compare multiple brands on a single metric"
)
def compare_brands_by_metric(
    payload: CompareBrandsRequest,
    db: Session = Depends(get_db)
):
    # base query filtered by metric
    q = (
        db.query(
            Brand.name.label("brand"),
            DataPoint.year,
            DataPoint.value,
            DataPoint.unit,
        )
        .join(Brand, DataPoint.brand == Brand.id)
        .join(Country, DataPoint.country == Country.id, isouter=True)
        .filter(func.lower(DataPoint.metric) == payload.metric.lower())
    )

    # apply optional filters
    if payload.brands:
        q = q.filter(func.lower(Brand.name).in_([b.lower() for b in payload.brands]))
    if payload.years:
        q = q.filter(DataPoint.year.in_(payload.years))
    if payload.country:
        q = q.filter(func.lower(Country.name) == payload.country.lower())

    rows = q.order_by(DataPoint.year, Brand.name).limit(payload.limit).all()

    if not rows:
        return CompareResponse(metric=payload.metric, labels=[], datasets=[])

    # pivot into labels & datasets
    years = sorted({ r.year for r in rows })
    brands = sorted({ r.brand for r in rows })
    pivot: Dict[str, Dict[str, Optional[float]]] = { b: {} for b in brands }
    unit = rows[0].unit

    for r in rows:
        try:
            val = float(r.value) if r.value is not None else None
        except (ValueError, TypeError):
            val = None
        pivot[r.brand][r.year] = val

    datasets = [
        Dataset(
            label=b,
            data=[ pivot[b].get(y) for y in years ],
            unit=unit
        )
        for b in brands
    ]

    return CompareResponse(
        metric=payload.metric,
        unit=unit,
        labels=years,
        datasets=datasets
    )
@router.get("/category-metrics/{category_name}", response_model=List[Dict[str, Any]])
def get_metrics_by_category(category_name: str, db: Session = Depends(get_db)):
    # Step 1: Join DataPoint with Category and filter by category name
    results = (
        db.query(DataPoint.metric_category, DataPoint.metric)
        .join(Category, DataPoint.category == Category.id)
        .filter(func.lower(Category.name) == category_name.lower())
        .distinct()
        .all()
    )

    # Step 2: Group metrics by metric_category
    grouped = {}
    for mc, m in results:
        if mc:
            grouped.setdefault(mc, []).append(m)

    # Step 3: Return in structured format
    return [{"metric_category": k, "metrics": v} for k, v in grouped.items()]





class ChartSeries(BaseModel):
    data: List[Any]
    categories: List[Any] # Categories can be strings (years) or numbers, use Any for flexibility

class ChartItem(BaseModel):
    """Represents a single chart configuration."""
    title: str # The metric name
    type: str  # e.g., "line", "bar", "metric"
    unit: str
    series: ChartSeries
class MetricSummary(BaseModel):
    """Represents a single metric and all its generated charts."""
    metric_name: str
    unit: str
    charts: List[ChartItem] # List of charts generated for this specific metric

class ResponseMetadata(BaseModel):
    """Metadata about the filters applied to generate this summary."""
    metric_category_path: str
    selected_category_name: str
    brand_name: Optional[str] = None

class MetricCategorySummaryResponse(BaseModel):
    """The overall response structure for the metric category summary."""
    metadata: ResponseMetadata
    metrics: List[MetricSummary] # List of distinct metrics found and their charts
# ... imports and setup ...

class ChartSeries(BaseModel):
    data: List[Any]
    categories: Optional[List[str]] = None

class ChartItem(BaseModel):
    id: str
    title: str
    type: str
    unit: Optional[str] = None
    series: ChartSeries
    height: Optional[str] = None
    className: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None
    options: Optional[Dict[str, Any]] = None
    allSeries: Optional[Dict[str, ChartSeries]] = None

class MetricSummary(BaseModel):
    metric_name: str
    unit: Optional[str] = None
    charts: List[ChartItem]

class ResponseMetadata(BaseModel):
    metric_category_path: str
    selected_category_name: str
    brand_name: Optional[str] = None
    # Add other relevant metadata if needed

class MetricCategorySummaryResponse(BaseModel):
    metadata: ResponseMetadata
    metrics: List[Dict[str, Any]] # Changed to a list of dictionaries

router = APIRouter()
@router.get("/category-metrics/{category_name}", response_model=List[Dict[str, Any]])
def get_metrics_by_category(category_name: str, db: Session = Depends(get_db)):
    """
    Retrieves metric categories for a given category name.

    This endpoint filters DataPoint records based on the provided category name
    (case-insensitive) and returns a list of unique metric categories associated
    with that category.  It no longer returns the individual metrics.

    Args:
        category_name (str): The name of the category to filter by.
        db (Session, optional): The database session. Defaults to Depends(get_db).

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary
            contains a single key-value pair: "metric_category" and its
            corresponding value (the name of the metric category).
            Returns an empty list if no matching categories are found.
    """
    # Step 1: Query for distinct metric categories, filtered by category name.
    results = (
        db.query(distinct(DataPoint.metric_category))
        .join(Category, DataPoint.category == Category.id)
        .filter(func.lower(Category.name) == category_name.lower())
        .all()
    )

    # Step 2:  Convert the result tuples into a list of dictionaries.
    # The query returns a list of tuples, where each tuple contains a single
    # element: the metric category.  We need to convert this into a list of
    # dictionaries as specified by the response_model.
    metric_categories = [{"metric_category": mc[0]} for mc in results]

    return metric_categories



@router.get(
    "/metric-category-summary/{metric_category_path}",
    response_model=MetricCategorySummaryResponse,
    summary="Get a structured summary of charts for a specific metric category with various filtering options."
)
def get_metric_category_summary(
    metric_category_path: str,
    selected_category_name: str = Query(..., alias="category", description="The main category name to filter by (e.g., 'Automobiles')."),
    brand_name: Optional[str] = Query(None, description="Optional brand name to filter by."),
    country_name: Optional[str] = Query(None, description="Optional country name to filter by."),
    year: Optional[str] = Query(None, description="Optional year to filter by (e.g., '2023')."),
    metric_name: Optional[str] = Query(None, description="Optional metric name to filter by (e.g., 'Car sales')."),
    unit: Optional[str] = Query(None, description="Optional unit to filter by (e.g., 'units', '%', 'USD billion')."),
    db: Session = Depends(get_db)
):
    """
    Retrieves a structured summary of data points grouped by metric for a given
    metric category, with flexible filtering options, formatted for dashboard consumption.
    """
    # Create a base query joining all relevant tables
    query_builder = db.query(
        DataPoint.metric,
        DataPoint.unit,
        DataPoint.year,
        DataPoint.value,  # No need for cast since we'll handle conversion later
        Country.name.label("country_name"),
        Brand.name.label("brand_name"),
        Category.name.label("category_name")
    ).join(
        Category, DataPoint.category == Category.id
    ).join(
        Country, DataPoint.country == Country.id, isouter=True
    ).join(
        Brand, DataPoint.brand == Brand.id, isouter=True
    )

    # Apply filters
    query_builder = query_builder.filter(func.lower(Category.name) == selected_category_name.lower())
    query_builder = query_builder.filter(func.lower(DataPoint.metric_category) == metric_category_path.lower())

    if brand_name:
        query_builder = query_builder.filter(func.lower(Brand.name) == brand_name.lower())
    if country_name:
        query_builder = query_builder.filter(func.lower(Country.name) == country_name.lower())
    if year:
        query_builder = query_builder.filter(DataPoint.year.like(f"%{year}%"))
    if metric_name:
        query_builder = query_builder.filter(func.lower(DataPoint.metric).like(f"%{metric_name.lower()}%"))
    if unit:
        query_builder = query_builder.filter(func.lower(DataPoint.unit).like(f"%{unit.lower()}%"))

    raw_data = query_builder.all()

    # Create response metadata
    response_metadata = ResponseMetadata(
        metric_category_path=metric_category_path,
        selected_category_name=selected_category_name,
        brand_name=brand_name
    )

    # Return empty response if no data found
    if not raw_data:
        return MetricCategorySummaryResponse(metadata=response_metadata, metrics=[])

    # Process data and organize by metrics
    metrics_data = {}
    
    for row in raw_data:
        # Ensure metric and unit exist
        metric = row.metric or "Unknown Metric"
        unit = row.unit or "Unknown Unit"
        
        # Create a unique ID for this metric
        metric_id = f"{metric.lower().replace(' ', '-')}"
        
        # Convert value to float for calculations if possible
        try:
            value = float(row.value)
        except (ValueError, TypeError):
            value = 0.0
            
        # Initialize metric if not already in dictionary
        if metric_id not in metrics_data:
            metrics_data[metric_id] = {
                "id": metric_id,
                "title": metric,
                "unit": unit,
                "type": "metric",  # Default type, may be updated
                "series": {"data": [], "categories": []},
                "filters": {},
                "options": {},
                "allSeries": {},
                # Data for chart generation
                "by_year": {},
                "by_country": {},
                "by_brand": {}
            }
        
        # Add the value to the appropriate data structure
        metrics_data[metric_id]["series"]["data"].append(value)
        
        # Group by year (for time series)
        if row.year:
            if row.year not in metrics_data[metric_id]["by_year"]:
                metrics_data[metric_id]["by_year"][row.year] = []
            metrics_data[metric_id]["by_year"][row.year].append(value)
        
        # Group by country
        if row.country_name:
            if row.country_name not in metrics_data[metric_id]["by_country"]:
                metrics_data[metric_id]["by_country"][row.country_name] = []
            metrics_data[metric_id]["by_country"][row.country_name].append(value)
        
        # Group by brand
        if row.brand_name:
            if row.brand_name not in metrics_data[metric_id]["by_brand"]:
                metrics_data[metric_id]["by_brand"][row.brand_name] = []
            metrics_data[metric_id]["by_brand"][row.brand_name].append(value)

    # Process the collected data into final format
    final_metrics = []
    
    for metric_id, metric_data in metrics_data.items():
        # Process time series data (by year)
        if metric_data["by_year"]:
            years = sorted(metric_data["by_year"].keys())
            # Calculate average for each year if there are multiple values
            year_values = [
                sum(metric_data["by_year"][year]) / len(metric_data["by_year"][year])
                for year in years
            ]
            
            # If we have multiple years, create a line chart
            if len(years) > 1:
                metric_data["type"] = "line"
                metric_data["series"]["categories"] = years
                metric_data["series"]["data"] = year_values
                metric_data["options"] = {"xaxis": {"categories": years}}
        
        # Process country data
        if metric_data["by_country"] and len(metric_data["by_country"]) > 1:
            countries = sorted(metric_data["by_country"].keys())
            # Calculate average for each country
            country_values = [
                sum(metric_data["by_country"][country]) / len(metric_data["by_country"][country])
                for country in countries
            ]
            
            # Create country filters
            metric_data["filters"] = {
                "type": "location",
                "options": ["All"] + countries,
                "default": "All"
            }
            
            # Create allSeries structure for country filtering
            metric_data["allSeries"] = {
                "All": {
                    "series": [{"data": country_values}],
                    "options": {"xaxis": {"categories": countries}}
                }
            }
            
            # Add individual country data
            for i, country in enumerate(countries):
                metric_data["allSeries"][country] = {
                    "series": [{"data": [country_values[i]]}],
                    "options": {"xaxis": {"categories": [country]}}
                }
        
        # Process brand data (optional, similar to country)
        if metric_data["by_brand"] and len(metric_data["by_brand"]) > 1 and not metric_data["filters"]:
            brands = sorted(metric_data["by_brand"].keys())
            brand_values = [
                sum(metric_data["by_brand"][brand]) / len(metric_data["by_brand"][brand])
                for brand in brands
            ]
            
            # If we haven't created filters yet, create them for brands
            if not metric_data["filters"]:
                metric_data["filters"] = {
                    "type": "brand",
                    "options": ["All"] + brands,
                    "default": "All"
                }
                
                metric_data["allSeries"] = {
                    "All": {
                        "series": [{"data": brand_values}],
                        "options": {"xaxis": {"categories": brands}}
                    }
                }
                
                for i, brand in enumerate(brands):
                    metric_data["allSeries"][brand] = {
                        "series": [{"data": [brand_values[i]]}],
                        "options": {"xaxis": {"categories": [brand]}}
                    }
        
        # Clean up and remove temporary data structures
        metric_data.pop("by_year", None)
        metric_data.pop("by_country", None)
        metric_data.pop("by_brand", None)
        
        # Add to final metrics list
        final_metrics.append(metric_data)

    return MetricCategorySummaryResponse(metadata=response_metadata, metrics=final_metrics)