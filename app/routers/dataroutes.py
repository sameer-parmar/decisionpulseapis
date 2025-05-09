from fastapi import APIRouter, Depends
from app.schemas.datapointschema import CategoryWithCountry , CountrySchema, BrandFilterRequest, CompareBrandsRequest ,ChartDataResponse,CompareResponse,AvailableResponse,Filters
from sqlalchemy.orm import Session
from app.database import  get_db
from typing import List,  Dict, Any, Optional
from sqlalchemy import func
from app.schemas.datapointschema import Dataset
from app.models.datapoints import Country
from app.models.datapoints import Brand
from app.models.datapoints import DataPoint

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