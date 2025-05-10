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
    metric category, with flexible filtering options.

    Filters can be applied for:
    - Main category (required)
    - Brand (optional)
    - Country (optional)
    - Year (optional)
    - Specific metric (optional)
    - Unit of measurement (optional)

    Generates potential line charts (for time series), bar charts (by country),
    and metric cards for each metric found in the filtered data.
    """
    # Step 1: Query all relevant rows with filters and joins
    query_builder = db.query(
        DataPoint.metric,
        DataPoint.unit,
        DataPoint.year,
        cast(DataPoint.value, Float).label("value"),
        Country.name.label("country_name"),
        Brand.name.label("brand_name")  # Add brand name to query results
    ).outerjoin(Country, DataPoint.country == Country.id).outerjoin(Brand, DataPoint.brand == Brand.id)        # Join with Brand table

    # Filter by the main selected category name (joining with Category table)
    query_builder = query_builder.join(Category, DataPoint.category == Category.id)
    query_builder = query_builder.filter(func.lower(Category.name) == selected_category_name.lower())

    # Filter by the metric_category from the path
    query_builder = query_builder.filter(func.lower(DataPoint.metric_category) == metric_category_path.lower())

    # Optional filter by brand name
    if brand_name:
        query_builder = query_builder.filter(func.lower(Brand.name) == brand_name.lower())

    # Optional filter by country name
    if country_name:
        query_builder = query_builder.filter(func.lower(Country.name) == country_name.lower())

    # Optional filter by year
    if year:
        # Handle different year formats (e.g., "2023", "FY23", "2022-2023")
        if "-" in year:  # Handle year ranges like "2022-2023"
            query_builder = query_builder.filter(DataPoint.year.like(f"%{year}%"))
        else:  # Handle specific years
            query_builder = query_builder.filter(DataPoint.year.like(f"%{year}%"))

    # Optional filter by metric name
    if metric_name:
        query_builder = query_builder.filter(func.lower(DataPoint.metric).like(f"%{metric_name.lower()}%"))

    # Optional filter by unit
    if unit:
        query_builder = query_builder.filter(func.lower(DataPoint.unit).like(f"%{unit.lower()}%"))

    raw_data = query_builder.all()  # Execute the query

    # Prepare metadata for the response
    response_metadata = ResponseMetadata(
        metric_category_path=metric_category_path,
        selected_category_name=selected_category_name,
        brand_name=brand_name
        # We could extend ResponseMetadata to include all filters, if needed
    )

    if not raw_data:
        # Return the metadata and an empty metrics list if no data points match filters
        return MetricCategorySummaryResponse(metadata=response_metadata, metrics=[])

    # Step 2: Group data by (metric, unit) for processing
    # This structure helps consolidate data points for the same metric+unit combination
    grouped_metrics_raw = {}
    for row in raw_data:
        # Handle potential None values from the query
        value = row.value if row.value is not None else 0.0
        group_key = (row.metric, row.unit)

        if group_key not in grouped_metrics_raw:
            grouped_metrics_raw[group_key] = {
                "metric_name": row.metric,
                "unit_name": row.unit,
                "time_series_points": [],     # Stores (year, value) tuples
                "country_aggregation": {},    # Stores {country_name: aggregated_value}
                "brand_aggregation": {},      # NEW: Add brand aggregation
                "unique_years": set(),        # Tracks unique years for line chart decision
                "total_value": 0.0            # Track total value across all dimensions
            }

        current_group = grouped_metrics_raw[group_key]
        current_group["total_value"] += value

        # Populate time series data if year is available
        if row.year:
            current_group["time_series_points"].append((str(row.year), value))
            current_group["unique_years"].add(str(row.year))

        # Aggregate values by country if country_name is available
        if row.country_name:
            current_group["country_aggregation"][row.country_name] = (
                current_group["country_aggregation"].get(row.country_name, 0.0) + value
            )

        # Aggregate values by brand if brand_name is available
        if row.brand_name:
            current_group["brand_aggregation"][row.brand_name] = (
                current_group["brand_aggregation"].get(row.brand_name, 0.0) + value
            )

    # Step 3: Build MetricSummary objects for each metric group found
    metrics_list: List[MetricSummary] = []

    for (metric, unit), data_points in grouped_metrics_raw.items():
        metric_name = data_points["metric_name"]
        metric_unit = data_points["unit_name"]
        charts_for_metric: List[ChartItem] = []  # List to hold charts for this specific metric

        # --- Generate Line Chart (Time Series) if applicable ---
        if len(data_points["unique_years"]) > 1:
            sorted_years = sorted(list(data_points["unique_years"]))
            # Aggregate values per year if there are multiple data points for the same year
            year_value_map = {year: 0.0 for year in sorted_years}
            for year_str, val in data_points["time_series_points"]:
                year_value_map[year_str] = year_value_map.get(year_str, 0.0) + val
            line_data = [year_value_map[year] for year in sorted_years]

            if any(val != 0.0 for val in line_data):
                charts_for_metric.append(ChartItem(
                    title=f"{metric_name} over time",
                    type="line",
                    unit=metric_unit,
                    series=ChartSeries(data=line_data, categories=sorted_years)
                ))

        # --- Generate Bar Chart (By Country) if applicable ---
        if data_points["country_aggregation"] and len(data_points["country_aggregation"]) > 1:
            country_names = list(data_points["country_aggregation"].keys())
            country_values = list(data_points["country_aggregation"].values())
            
            if any(val != 0.0 for val in country_values):
                charts_for_metric.append(ChartItem(
                    title=f"{metric_name} by Country",
                    type="bar",
                    unit=metric_unit,
                    series=ChartSeries(data=country_values, categories=country_names)
                ))

        # --- Generate Bar Chart (By Brand) if applicable ---
        if data_points["brand_aggregation"] and len(data_points["brand_aggregation"]) > 1:
            brand_names = list(data_points["brand_aggregation"].keys())
            brand_values = list(data_points["brand_aggregation"].values())
            
            if any(val != 0.0 for val in brand_values):
                charts_for_metric.append(ChartItem(
                    title=f"{metric_name} by Brand",
                    type="bar", 
                    unit=metric_unit,
                    series=ChartSeries(data=brand_values, categories=brand_names)
                ))

        # --- Generate Metric Card (Single Value Display) if applicable ---
        # Check if we've already added a line or bar chart
        is_line_chart_added = any(c.type == "line" for c in charts_for_metric)
        is_bar_chart_added = any(c.type == "bar" for c in charts_for_metric)

        # For filtered results (single year, single country, single brand), 
        # we might want to always show a metric card
        is_highly_filtered = all([year, country_name, brand_name])
        
        # Add a metric card when appropriate:
        if is_highly_filtered or (not is_line_chart_added and not is_bar_chart_added):
            # Determine a suitable label for the metric card
            label = "Value"
            
            if len(data_points["unique_years"]) == 1:
                label = list(data_points["unique_years"])[0]  # Use the single year as label
            
            if country_name and len(data_points["country_aggregation"]) == 1:
                label = f"{label} ({country_name})" if label != "Value" else country_name
                
            if brand_name and len(data_points["brand_aggregation"]) == 1:
                label = f"{label} ({brand_name})" if label != "Value" else brand_name
            
            # If we have a non-zero value or it's a specific filter query, add the metric card
            if data_points["total_value"] != 0.0 or is_highly_filtered:
                charts_for_metric.append(ChartItem(
                    title=metric_name,
                    type="metric",
                    unit=metric_unit,
                    series=ChartSeries(data=[data_points["total_value"]], categories=[label])
                ))

        # Add this metric's summary if we generated any charts
        if charts_for_metric:
            metrics_list.append(MetricSummary(
                metric_name=metric_name,
                unit=metric_unit,
                charts=charts_for_metric
            ))

    # Step 4: Return the final structured response
    return MetricCategorySummaryResponse(metadata=response_metadata, metrics=metrics_list)