# main.py (or your relevant API router file)
import csv
import io
from fastapi import FastAPI, File, UploadFile, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List
from fastapi import Query
from sqlalchemy import func
# Import your dependencies, models, and session management
from .database import SessionLocal, get_db, engine , Base # Assuming get_db is sync
from app.routers.total_sales_performance import router as total_sales_performance_router
from app.models.datapoints import DataPoint  # Correct import for DataPoint  # Corrected import for Base
from app.utils import safe_float  # Assuming safe_float is a utility function for float conversion
from app.routers.fmcg_data import router   as fmcg_router
from app.routers.upload_fmcg_data import router as upload_fmcg_data_router
Base.metadata.create_all(bind=engine)  # Use only for development/testing if not using Alembic


app = FastAPI()
app.include_router(total_sales_performance_router)
app.include_router(fmcg_router)  # Include your router for FMCG data  
app.include_router(upload_fmcg_data_router)  # Include your router for uploading FMCG data
@app.get("/channel-performance/")
def get_channel_performance(
    db: Session = Depends(get_db),
    year: str = Query(None, description="Filter by year"),
    country: str = Query(None, description="Filter by country"),
):
    query = db.query(DataPoint).filter(DataPoint.metric_category == 'ecommerce_digital').filter(DataPoint.metric.ilike("%sales%")).filter(DataPoint.value.isnot(None))
    if year:
        query = query.filter(DataPoint.year == year)
    if country:
        query = query.filter(DataPoint.country == country)
    data = query.all()
    channel_data = {}
    for item in data:
        channel = "Online" # Assuming 'ecommerce_digital' category implies online channel for now
        key = (item.year, item.country, channel)
        if key not in channel_data:
            channel_data[key] = {"year": item.year, "country": item.country, "channel": channel, "total_sales": 0}
        value = safe_float(item.value)
        if value is not None:
            channel_data[key]["total_sales"] += value
    return list(channel_data.values())

@app.get("/market-share/")
def get_market_share(
    db: Session = Depends(get_db),
    year: str = Query(None, description="Filter by year"),
    country: str = Query(None, description="Filter by country"),
):
    query = db.query(DataPoint).filter(DataPoint.metric_category == 'competitive_intelligence').filter(DataPoint.metric.ilike("%market share%")).filter(DataPoint.value.isnot(None))
    if year:
        query = query.filter(DataPoint.year == year)
    if country:
        query = query.filter(DataPoint.country == country)
    data = query.all()
    return [{"year": item.year, "country": item.country, "metric": item.metric, "value": safe_float(item.value), "brand": item.brand} for item in data]
# --- Background Task ---
# Helper function to safely convert value to float if possible

# --- Background Task ---
# @app.get("/fmcg-data/", response_model=List[dict])  # Define a Pydantic model for response if needed
# def read_fmcg_data(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
#     data = db.query(DataPoint).order_by(DataPoint.id).offset(skip).limit(limit).all()
#     # Convert SQLAlchemy objects to dictionaries for JSON response
#     serialized_data = [
#         {
#             "id": dp.id,
#             "source_url": dp.source_url,
#             "insight": dp.insight,
#             "year": dp.year,
#             "brand": dp.brand,
#             "metric": dp.metric,
#             "value": dp.value,
#             "country": dp.country,
#             "summary": dp.summary,
#         }
#         for dp in data
#     ]
#     return serialized_data


# 1️⃣ Total Sales Performance
# @app.get("/total-sales-performance/")
# def get_total_sales_performance(
#     db: Session = Depends(get_db),
#     year: str = Query(None, description="Filter by year"),
#     country: str = Query(None, description="Filter by country"),
# ):
#     query = db.query(DataPoint).filter(DataPoint.metric_category == 'financial_health').filter(DataPoint.metric.ilike("%sales%")).filter(DataPoint.value.isnot(None))
#     if year:
#         query = query.filter(DataPoint.year == year)
#     if country:
#         query = query.filter(DataPoint.country == country)
#     data = query.all()
#     return [{"year": item.year, "country": item.country, "metric": item.metric, "value": safe_float(item.value)} for item in data]

# 2️⃣ Channel-wise Performance
# # 3️⃣ Discounts & Promotion Impact
# @app.get("/promotion-impact/")
# def get_promotion_impact(
#     db: Session = Depends(get_db),
#     year: str = Query(None, description="Filter by year"),
#     country: str = Query(None, description="Filter by country"),
# ):
#     query = db.query(DataPoint).filter(DataPoint.metric.ilike("%promotion%")).filter(DataPoint.value.isnot(None))
#     if year:
#         query = query.filter(DataPoint.year == year)
#     if country:
#         query = query.filter(DataPoint.country == country)
#     data = query.all()
#     return [{"year": item.year, "country": item.country, "metric": item.metric, "value": safe_float(item.value)} for item in data]

# # 4️⃣ Customer Retention & Churn
# @app.get("/customer-retention/")
# def get_customer_retention(
#     db: Session = Depends(get_db),
#     year: str = Query(None, description="Filter by year"),
#     country: str = Query(None, description="Filter by country"),
# ):
#     query = db.query(DataPoint).filter(DataPoint.metric_category == 'consumer_insights').filter(DataPoint.metric.ilike("%churn%")).filter(DataPoint.value.isnot(None))
#     if year:
#         query = query.filter(DataPoint.year == year)
#     if country:
#         query = query.filter(DataPoint.country == country)
#     data = query.all()
#     return [{"year": item.year, "country": item.country, "metric": item.metric, "value": safe_float(item.value)} for item in data]

# 5️⃣ Market Share Analysis

# 6️⃣ Feature & Innovation Comparison
# @app.get("/feature-comparison/")
# def get_feature_comparison(
#     db: Session = Depends(get_db),
#     year: str = Query(None, description="Filter by year"),
#     country: str = Query(None, description="Filter by country"),
# ):
#     query = db.query(DataPoint).filter(DataPoint.metric.ilike("%feature%")).filter(DataPoint.value.isnot(None))
#     if year:
#         query = query.filter(DataPoint.year == year)
#     if country:
#         query = query.filter(DataPoint.country == country)
#     data = query.all()
#     return [{"year": item.year, "country": item.country, "metric": item.metric, "value": safe_float(item.value), "brand": item.brand} for item in data]

# 7️⃣ Product Demand vs Inventory
# @app.get("/demand-vs-inventory/")
# def get_demand_vs_inventory(
#     db: Session = Depends(get_db),
#     year: str = Query(None, description="Filter by year"),
#     country: str = Query(None, description="Filter by country"),
# ):
#     query = db.query(DataPoint).filter(DataPoint.metric.ilike("%demand%")).filter(DataPoint.metric.ilike("%inventory%")).filter(DataPoint.value.isnot(None))
#     if year:
#         query = query.filter(DataPoint.year == year)
#     if country:
#         query = query.filter(DataPoint.country == country)
#     data = query.all()
#     demand_inventory_data = {}
#     for item in data:
#         key = (item.year, item.country, item.brand)
#         if key not in demand_inventory_data:
#             demand_inventory_data[key] = {"year": item.year, "country": item.country, "brand": item.brand, "demand": None, "inventory": None}
#         if "demand" in item.metric.lower():
#             demand_inventory_data[key]["demand"] = safe_float(item.value)
#         elif "inventory" in item.metric.lower():
#             demand_inventory_data[key]["inventory"] = safe_float(item.value)
#     return list(demand_inventory_data.values())

# # 8️⃣ Cost Optimization
# @app.get("/cost-optimization/")
# def get_cost_optimization(
#     db: Session = Depends(get_db),
#     year: str = Query(None, description="Filter by year"),
#     country: str = Query(None, description="Filter by country"),
# ):
#     query = db.query(DataPoint).filter(DataPoint.metric_category == 'financial_health').filter(DataPoint.metric.ilike("%cost%")).filter(DataPoint.value.isnot(None))
#     if year:
#         query = query.filter(DataPoint.year == year)
#     if country:
#         query = query.filter(DataPoint.country == country)
#     data = query.all()
#     return [{"year": item.year, "country": item.country, "metric": item.metric, "value": safe_float(item.value)} for item in data]

# # 9️⃣ Dealer Inventory & Stock Optimization
# @app.get("/dealer-inventory/")
# def get_dealer_inventory(
#     db: Session = Depends(get_db),
#     year: str = Query(None, description="Filter by year"),
#     country: str = Query(None, description="Filter by country"),
# ):
#     query = db.query(DataPoint).filter(DataPoint.metric.ilike("%inventory%")).filter(DataPoint.metric.ilike("%stock%")).filter(DataPoint.value.isnot(None))
#     if year:
#         query = query.filter(DataPoint.year == year)
#     if country:
#         query = query.filter(DataPoint.country == country)
#     data = query.all()
#     dealer_inventory_data = {}
#     for item in data:
#         key = (item.year, item.country, item.brand) # Assuming brand can act as a dealer identifier for now
#         if key not in dealer_inventory_data:
#             dealer_inventory_data[key] = {"year": item.year, "country": item.country, "brand": item.brand, "inventory": None, "stock_age": None}
#         if "inventory" in item.metric.lower():
#             dealer_inventory_data[key]["inventory"] = safe_float(item.value)
#         elif "stock" in item.metric.lower():
#             dealer_inventory_data[key]["stock_age"] = safe_float(item.value)
#     return list(dealer_inventory_data.values())
