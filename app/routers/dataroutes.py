from fastapi import APIRouter, Depends
from app.schemas.datapointschema import CategoryWithCountry , CountrySchema, BrandFilterRequest, CompareBrandsRequest ,ChartDataResponse,CompareResponse,AvailableResponse,Filters
from sqlalchemy.orm import Session
from app.database import  get_db
from typing import List,  Dict, Any, Optional, Tuple
from sqlalchemy import Float, cast, func
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

    Filters can be applied for:
    - Main category (required)
    - Brand (optional)
    - Country (optional)
    - Year (optional)
    - Specific metric (optional)
    - Unit of measurement (optional)

    Generates potential line charts (for time series), bar charts (by country and brand),
    and metric cards for each metric found in the filtered data, structured according to
    the provided sample JSON.
    """
    query_builder = db.query(
        DataPoint.metric,
        DataPoint.unit,
        DataPoint.year,
        cast(DataPoint.value, Float).label("value"),
        Country.name.label("country_name"),
        Brand.name.label("brand_name")
    ).outerjoin(Country, DataPoint.country == Country.id).outerjoin(Brand, DataPoint.brand == Brand.id)

    query_builder = query_builder.join(Category, DataPoint.category == Category.id)
    query_builder = query_builder.filter(func.lower(Category.name) == selected_category_name.lower())
    query_builder = query_builder.filter(func.lower(DataPoint.metric_category) == metric_category_path.lower())

    if brand_name:
        query_builder = query_builder.filter(func.lower(Brand.name) == brand_name.lower())
    if country_name:
        query_builder = query_builder.filter(func.lower(Country.name) == country_name.lower())
    if year:
        if "-" in year:
            query_builder = query_builder.filter(DataPoint.year.like(f"%{year}%"))
        else:
            query_builder = query_builder.filter(DataPoint.year.like(f"%{year}%"))
    if metric_name:
        query_builder = query_builder.filter(func.lower(DataPoint.metric).like(f"%{metric_name.lower()}%"))
    if unit:
        query_builder = query_builder.filter(func.lower(DataPoint.unit).like(f"%{unit.lower()}%"))

    raw_data = query_builder.all()

    response_metadata = ResponseMetadata(
        metric_category_path=metric_category_path,
        selected_category_name=selected_category_name,
        brand_name=brand_name
    )

    if not raw_data:
        return MetricCategorySummaryResponse(metadata=response_metadata, metrics=[])

    grouped_metrics_formatted: Dict[str, Dict[str, Any]] = {}

    for row in raw_data:
        metric_unit_key = f"{row.metric or 'Unknown Metric'} ({row.unit or 'Unknown Unit'})"
        value = row.value if row.value is not None else 0.0

        if metric_unit_key not in grouped_metrics_formatted:
            grouped_metrics_formatted[metric_unit_key] = {
                "id": metric_unit_key.lower().replace(" ", "-"), # Generate a unique ID
                "title": row.metric or "Unknown Metric",
                "unit": row.unit,
                "type": "metric", # Default type, will be updated
                "series": {"data": [], "categories": []},
                "allSeries": {},
                "filters": {},
                "options": {},
                "time_series_data": {}, # {year: value}
                "country_data": {},    # {country: aggregated_value}
                "brand_data": {}      # {brand: aggregated_value}
            }

        metric_data = grouped_metrics_formatted[metric_unit_key]
        metric_data["series"]["data"].append(value) # Add all values initially

        if row.year:
            metric_data["time_series_data"][row.year] = metric_data["time_series_data"].get(row.year, 0.0) + value
        if row.country_name:
            metric_data["country_data"][row.country_name] = metric_data["country_data"].get(row.country_name, 0.0) + value
        if row.brand_name:
            metric_data["brand_data"][row.brand_name] = metric_data["brand_data"].get(row.brand_name, 0.0) + value

    final_metrics_list: List[Dict[str, Any]] = []
    for key, metric_data in grouped_metrics_formatted.items():
        charts: List[Dict[str, Any]] = []

        # --- Time Series Chart ---
        if len(metric_data["time_series_data"]) > 1:
            sorted_years = sorted(metric_data["time_series_data"].keys())
            line_series_data = [metric_data["time_series_data"][year] for year in sorted_years]
            if any(val != 0.0 for val in line_series_data):
                charts.append({
                    "id": f"{metric_data['id']}-time-series",
                    "title": f"{metric_data['title']} over time",
                    "type": "line",
                    "unit": metric_data["unit"],
                    "series": {"data": line_series_data, "categories": sorted_years},
                    "options": {"xaxis": {"categories": sorted_years}}
                })
                metric_data["type"] = "line" # Update type if a line chart is added

        # --- Bar Chart by Country ---
        if len(metric_data["country_data"]) > 1:
            country_names = list(metric_data["country_data"].keys())
            country_values = list(metric_data["country_data"].values())
            if any(val != 0.0 for val in country_values):
                charts.append({
                    "id": f"{metric_data['id']}-country-bar",
                    "title": f"{metric_data['title']} by Country",
                    "type": "bar",
                    "unit": metric_data["unit"],
                    "series": {"data": country_values, "categories": country_names},
                    "options": {"xaxis": {"categories": country_names}}
                })
                if metric_data["type"] == "metric":
                    metric_data["type"] = "bar" # Update type if a bar chart is added

                # Add filters for country if bar chart exists
                metric_data["filters"] = {
                    "type": "location",
                    "options": ["All"] + sorted(country_names),
                    "default": "All"
                }
                metric_data["allSeries"] = {"All": {"series": [{"data": country_values}], "options": {"xaxis": {"categories": country_names}}}}
                for country, value in metric_data["country_data"].items():
                    metric_data["allSeries"][country] = {"series": [{"data": [value]}], "options": {"xaxis": {"categories": [country]}}}

        # --- Bar Chart by Brand ---
        if len(metric_data["brand_data"]) > 1:
            brand_names = list(metric_data["brand_data"].keys())
            brand_values = list(metric_data["brand_data"].values())
            if any(val != 0.0 for val in brand_values):
                charts.append({
                    "id": f"{metric_data['id']}-brand-bar",
                    "title": f"{metric_data['title']} by Brand",
                    "type": "bar",
                    "unit": metric_data["unit"],
                    "series": {"data": brand_values, "categories": brand_names},
                    "options": {"xaxis": {"categories": brand_names}}
                })
                if metric_data["type"] == "metric":
                    metric_data["type"] = "bar" # Update type if a bar chart is added

        # --- Metric Card ---
        # Display a metric card if no other suitable chart type was generated
        if not charts:
            # Use the last value if available, or the sum
            display_value = metric_data["series"]["data"][-1] if metric_data["series"]["data"] else 0
            label = "Value"
            if metric_data["time_series_data"] and len(metric_data["time_series_data"]) == 1:
                label = list(metric_data["time_series_data"].keys())[0]
            elif country_name and len(metric_data["country_data"]) == 1:
                label = country_name
            elif brand_name and len(metric_data["brand_data"]) == 1:
                label = brand_name

            charts.append({
                "id": f"{metric_data['id']}-metric-card",
                "title": metric_data["title"],
                "type": "metric",
                "unit": metric_data["unit"],
                "series": {"data": [display_value], "categories": [label]}
            })
            metric_data["type"] = "metric"

        # Structure the final metric item
        final_metrics_list.append({
            "id": metric_data["id"],
            "title": metric_data["title"],
            "unit": metric_data["unit"],
            "type": metric_data["type"],
            "series": metric_data["series"],
            "filters": metric_data.get("filters"),
            "options": metric_data.get("options"),
            "allSeries": metric_data.get("allSeries")
        })

    return MetricCategorySummaryResponse(metadata=response_metadata, metrics=final_metrics_list)
