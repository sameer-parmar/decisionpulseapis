from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Depends
from sqlalchemy import create_engine, MetaData, Table, select
import io
import os
import pandas as pd
import re
from sqlalchemy.orm import Session
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Any, Optional

# Assuming these are correctly imported from your project structure
from app.database import get_db
from app.utils.charts import chart_functions # This import is for the descriptive-data-api, not directly used in the new endpoints, but kept for context.
from app.config import settings
from app.models.datapoints import AutoMobileData # Your SQLAlchemy Sale model

router = APIRouter()

UPLOAD_DIR = "uploaded_files"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def process_data_dump(file_bytes: bytes, original_filename: str, db_url: str, table_name: str):
    """
    Processes the uploaded file (CSV or XLSX) and dumps its content into the database.
    This function is intended to be run as a background task.
    """
    print(f"Starting batch‐safe dump for '{original_filename}' → '{table_name}'")
    engine = create_engine(db_url, echo=True)

    # Read file into DataFrame
    ext = original_filename.rsplit('.', 1)[-1].lower()
    if ext == 'csv':
        try:
            content = file_bytes.decode('utf-8')
        except UnicodeDecodeError:
            content = file_bytes.decode('latin-1')
        df = pd.read_csv(io.StringIO(content))
    elif ext == 'xlsx':
        df = pd.read_excel(io.BytesIO(file_bytes))
    else:
        print(f"Unsupported format: {ext}")
        return

    if df.empty:
        print("No data found in file; exiting.")
        return

    # Clean up column names to match database schema
    df.columns = [
        re.sub(r'[^a-z0-9_]', '', col.strip().lower().replace(' ', '_'))
        for col in df.columns
    ]

    # Rename specific columns to match the AutoMobileData model's mapped names
    df.rename(columns={
        'market_share_in_region_': 'market_share_in_region',
        'unit_price_': 'unit_price',
        'discount_offered_': 'discount_offered',
        'final_price_after_discount_': 'final_price_after_discount'
    }, inplace=True)


    num_cols = len(df.columns)
    # Compute max rows per batch so that rows * cols ≤ 2100 (a heuristic for batch size)
    max_rows = max(1, 2100 // num_cols)
    total = len(df)
    print(f"{total} rows; {num_cols} cols → batching {max_rows} rows per chunk")

    # Loop and insert data in chunks
    for idx in range(0, total, max_rows):
        chunk = df.iloc[idx : idx + max_rows]
        print(f"Inserting rows {idx}–{idx + len(chunk) - 1}...")
        chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace' if idx == 0 else 'append', # 'replace' for first chunk, 'append' for subsequent
            index=False,
            method=None   # default, one INSERT per row under the hood
        )

    engine.dispose()
    print(f"Finished dumping '{original_filename}' into '{table_name}'.")


@router.post("/upload-raw-data/")
async def upload_raw_data(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    table_name: str = "auto_mobile_data",
    save_file: Optional[bool] = False
):
    """
    Endpoint to upload raw data files (CSV or XLSX) for processing and database dumping.
    The actual data processing is offloaded to a background task.
    """
    if not file.filename:
        raise HTTPException(400, "No file uploaded.")

    ext = file.filename.rsplit('.', 1)[-1].lower()
    if ext not in ('csv', 'xlsx'):
        raise HTTPException(400, "Only .csv or .xlsx supported.")

    # Clean and validate table name to prevent SQL injection or invalid names
    cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '', table_name.strip().lower())
    if not re.match(r'^[a-zA-Z_]\w*$', cleaned_name):
        raise HTTPException(400, "Invalid table name after cleaning.")

    raw = await file.read()
    if not raw:
        raise HTTPException(400, "Uploaded file is empty.")

    if save_file:
        path = os.path.join(UPLOAD_DIR, file.filename)
        with open(path, 'wb') as f:
            f.write(raw)
        print(f"Saved to {path}")

    print(f"Scheduling batch dump for '{file.filename}' → '{cleaned_name}'")
    background_tasks.add_task(
        process_data_dump,
        raw,
        file.filename,
        settings.sqlalchemy_database_uri,
        cleaned_name
    )

    return {
        "message": f"Received '{file.filename}'. Batch processing started.",
        "table": cleaned_name
    }

# Existing endpoint for descriptive data, kept for context
# @router.get("/descriptive-data-api", response_model=List[Dict[str, Any]])
async def descriptive_data_api(
    db: Session = Depends(get_db),
    country: str = None,
    brand: str = None
):
    """
    Retrieves and aggregates various charts based on the automobile data.
    Allows filtering by country and brand.
    """
    # Reflect the table for dynamic access
    metadata = MetaData()
    auto_table = Table(
        'auto_mobile_data',
        metadata,
        autoload_with=db.bind
    )

    # Fetch every row as a dictionary
    rows: List[Dict[str, Any]] = (
        db.execute(select(auto_table))
          .mappings()
          .all()
    )

    # Filter rows based on provided country and brand
    filtered_rows = []
    for r in rows:
        if country and r.get("country") and r["country"].lower() != country.lower():
            continue
        if brand and r.get("oem_name") and r["oem_name"].lower() != brand.lower():
            continue
        filtered_rows.append(r)
    rows = filtered_rows

    # Dynamically generate charts using predefined chart functions
    charts = []
    for fn in chart_functions:
        try:
            charts.append(fn(rows))
        except Exception as e:
            charts.append({"id": fn.__name__, "error": str(e)})
    return charts

# --- NEW ENDPOINTS ---

# ... (imports and existing upload_raw_data, descriptive_data_api)

# @router.get("/sales-performance-kpis", response_model=Dict[str, Any])
async def get_sales_performance_kpis(
    db: Session = Depends(get_db),
    country: Optional[str] = None,
    region: Optional[str] = None,
    oem_name: Optional[str] = None
):
    """
    Provides key performance indicators for Global & Regional Sales Performance.
    Includes Total Units Sold, Market Share by OEM and Competitor, and Average Selling Price.
    Filters can be applied by country, region, and OEM name.
    """
    # Fetch all data from the AutoMobileData model
    query = db.query(AutoMobileData)

    # Apply filters based on query parameters
    if country:
        query = query.filter(AutoMobileData.country.ilike(f"%{country}%"))
    if region:
        query = query.filter(AutoMobileData.region.ilike(f"%{region}%"))
    if oem_name:
        query = query.filter(AutoMobileData.oem_name.ilike(f"%{oem_name}%"))

    all_data = query.all()

    # FIX: Convert SQLAlchemy objects to dictionaries for easier processing
    rows = []
    for row in all_data:
        # Take everything in __dict__ except SQLAlchemy’s internal state
        row_dict = {
            k: v
            for k, v in row.__dict__.items()
            if not k.startswith("_sa_instance_state")
        }
        rows.append(row_dict)

    total_units_sold = 0
    total_revenue = 0
    oem_units_sold = defaultdict(int)
    competitor_units_sold = defaultdict(int)

    for r in rows:
        units = r.get("units_sold", 0)
        final_price = r.get("final_price_after_discount", 0.0)
        oem = r.get("oem_name")
        competitor = r.get("competitor_oem")

        total_units_sold += units
        total_revenue += (final_price * units) # Calculate total revenue for ASP

        if oem:
            oem_units_sold[oem] += units
        if competitor:
            competitor_units_sold[competitor] += units

    # Calculate Market Share for OEMs
    market_share_by_oem = []
    if total_units_sold > 0:
        for oem, units in oem_units_sold.items():
            market_share_by_oem.append({
                "oem": oem,
                "units_sold": units,
                "market_share_percent": round((units / total_units_sold) * 100, 2)
            })
    # Sort by market share descending
    market_share_by_oem = sorted(market_share_by_oem, key=lambda x: x["market_share_percent"], reverse=True)

    # Calculate Market Share for Competitor OEMs
    total_competitor_units_sold = sum(competitor_units_sold.values())
    market_share_by_competitor_oem = []
    if total_competitor_units_sold > 0:
        for comp, units in competitor_units_sold.items():
            market_share_by_competitor_oem.append({
                "competitor_oem": comp,
                "units_sold": units,
                "market_share_percent": round((units / total_competitor_units_sold) * 100, 2)
            })
    # Sort by market share descending
    market_share_by_competitor_oem = sorted(market_share_by_competitor_oem, key=lambda x: x["market_share_percent"], reverse=True)


    # Calculate Average Selling Price (ASP)
    average_selling_price = total_revenue / total_units_sold if total_units_sold > 0 else 0.0

    return {
        "total_units_sold": {
            "type": "bar",
            "title": "Total Units Sold by OEM",
            "x": "oem",
            "y": "units_sold",
            "data": [{"oem": oem, "units_sold": units} for oem, units in oem_units_sold.items()]
        },
        "average_selling_price": {
            "type": "value",
            "title": "Average Selling Price",
            "value": round(average_selling_price, 2)
        },
        "market_share_by_oem": {
            "type": "bar",
            "title": "Market Share by OEM",
            "x": "oem",
            "y": "market_share_percent",
            "data": market_share_by_oem
        },
        "market_share_by_competitor_oem": {
            "type": "bar",
            "title": "Market Share by Competitor OEM",
            "x": "competitor_oem",
            "y": "market_share_percent",
            "data": market_share_by_competitor_oem
        }
    }

# @router.get("/supply-aftersales-kpis", response_model=Dict[str, Any])
async def get_supply_aftersales_kpis(
    db: Session = Depends(get_db),
    region: Optional[str] = None,
    country: Optional[str] = None,
    dealer_name: Optional[str] = None
):
    """
    Provides key performance indicators for Supply Chain Efficiency and After-Sales & Service Operations.
    Includes Average Delivery Time, Average Delivery Rating, and Complaint Count by Dealer.
    Filters can be applied by region, country, and dealer name.
    """
    query = db.query(AutoMobileData)

    # Apply filters
    if region:
        query = query.filter(AutoMobileData.region.ilike(f"%{region}%"))
    if country:
        query = query.filter(AutoMobileData.country.ilike(f"%{country}%"))
    if dealer_name:
        query = query.filter(AutoMobileData.dealer_name.ilike(f"%{dealer_name}%"))

    all_data = query.all()

    # Convert SQLAlchemy objects to dictionaries for easier processing
    rows = [{c.name: getattr(row, c.name) for c in row.__table__.columns} for row in all_data]

    delivery_delays = defaultdict(list)
    dealer_ratings = defaultdict(list)
    dealer_complaints = defaultdict(int)

    for r in rows:
        booking_date = r.get("booking_date")
        delivery_date = r.get("delivery_date")
        dealer = r.get("dealer_name")
        delivery_rating = r.get("delivery_rating_15")
        complaint_registered = r.get("complaint_registered_yn", "").lower()

        # Calculate average delivery time
        if booking_date and delivery_date and isinstance(booking_date, datetime) and isinstance(delivery_date, datetime):
            delay = (delivery_date - booking_date).days
            # Group by OEM for delivery delay, as per the existing chart logic, but adaptable to region/country
            # For this endpoint, we'll just average across all filtered data, or group by dealer if needed.
            delivery_delays["overall"].append(delay) # Or group by region/country if desired

        # Calculate average delivery rating and complaint count by dealer
        if dealer:
            if delivery_rating is not None:
                dealer_ratings[dealer].append(delivery_rating)
            if complaint_registered == "yes":
                dealer_complaints[dealer] += 1

    avg_delivery_time_days = round(sum(delivery_delays["overall"]) / len(delivery_delays["overall"]), 2) if delivery_delays["overall"] else 0
    avg_delivery_rating_by_dealer = []
    for dealer, ratings in dealer_ratings.items():
        if ratings:
            avg_delivery_rating_by_dealer.append({
                "dealer_name": dealer,
                "avg_rating": round(sum(ratings) / len(ratings), 2)
            })

    complaint_count_by_dealer = []
    for dealer, count in dealer_complaints.items():
        complaint_count_by_dealer.append({
            "dealer_name": dealer,
            "complaint_count": count
        })

    return {
        "average_delivery_time_days": {
            "type": "value",
            "title": "Average Delivery Time (Days)",
            "value": avg_delivery_time_days
        },
        "average_delivery_rating_by_dealer": {
            "type": "bar",
            "title": "Average Delivery Rating by Dealer",
            "x": "dealer_name",
            "y": "avg_rating",
            "data": avg_delivery_rating_by_dealer
        },
        "complaint_count_by_dealer": {
            "type": "bar",
            "title": "Complaint Count by Dealer",
            "x": "dealer_name",
            "y": "complaint_count",
            "data": complaint_count_by_dealer
        }
    }

# @router.get("/customer-sustainability-kpis", response_model=Dict[str, Any])
async def get_customer_sustainability_kpis(
    db: Session = Depends(get_db),
    city: Optional[str] = None,
    customer_type: Optional[str] = None
):
    """
    Provides key performance indicators for Customer & Market Insights and Sustainability & Regulatory Compliance.
    Includes Average NPS by City, Electric Vehicle Share, EV Metrics, and Finance Opted Ratio by Customer Type.
    Filters can be applied by city and customer type.
    """
    query = db.query(AutoMobileData)

    # Apply filters
    if city:
        query = query.filter(AutoMobileData.city.ilike(f"%{city}%"))
    if customer_type:
        query = query.filter(AutoMobileData.customer_type.ilike(f"%{customer_type}%"))

    all_data = query.all()

    # Convert SQLAlchemy objects to dictionaries for easier processing
    rows = [
        {
        k: v
        for k, v in row.__dict__.items()
        if not k.startswith("_sa_instance_state")
        }
        for row in all_data
    ]

    nps_by_city_scores = defaultdict(list)
    electric_vehicle_units = 0
    total_units_overall = 0
    ev_metrics_data = defaultdict(lambda: defaultdict(list))
    finance_opted_counts = defaultdict(lambda: {"yes": 0, "total": 0})

    for r in rows:
        nps = r.get("nps_customer_feedback")
        city_name = r.get("city")
        fuel_type = r.get("fuel_type", "").lower()
        units = r.get("units_sold", 0)
        range_km = r.get("range_km")
        battery_kwh = r.get("battery_capacity_kwh")
        charging_time_hours = r.get("charging_time_hours")
        cust_type = r.get("customer_type")
        finance_opted_yn = r.get("finance_opted_yesno", "").lower()

        # NPS by City
        if city_name and nps is not None:
            nps_by_city_scores[city_name].append(nps)

        # Electric Vehicle Share
        total_units_overall += units
        if "electric" in fuel_type:
            electric_vehicle_units += units
            # EV Metrics
            oem = r.get("oem_name")
            if oem:
                if range_km is not None:
                    ev_metrics_data[oem]["range_km"].append(range_km)
                if battery_kwh is not None:
                    ev_metrics_data[oem]["battery_kwh"].append(battery_kwh)
                if charging_time_hours is not None:
                    ev_metrics_data[oem]["charging_time_hours"].append(charging_time_hours)

        # Finance Opted Ratio by Customer Type
        if cust_type:
            finance_opted_counts[cust_type]["total"] += 1
            if finance_opted_yn == "yes":
                finance_opted_counts[cust_type]["yes"] += 1

    # Calculate Average NPS by City
    avg_nps_by_city = []
    for city_name, scores in nps_by_city_scores.items():
        if scores:
            avg_nps_by_city.append({
                "city": city_name,
                "average_nps": round(sum(scores) / len(scores), 2)
            })

    # Calculate Electric Vehicle Share %
    ev_share_percent = round((electric_vehicle_units / total_units_overall * 100), 2) if total_units_overall > 0 else 0.0

    # Calculate Average EV Metrics
    avg_ev_metrics = []
    for oem, metrics in ev_metrics_data.items():
        avg_ev_metrics.append({
            "oem": oem,
            "avg_range_km": round(sum(metrics["range_km"]) / len(metrics["range_km"]), 2) if metrics["range_km"] else 0,
            "avg_battery_kwh": round(sum(metrics["battery_kwh"]) / len(metrics["battery_kwh"]), 2) if metrics["battery_kwh"] else 0,
            "avg_charging_time_hours": round(sum(metrics["charging_time_hours"]) / len(metrics["charging_time_hours"]), 2) if metrics["charging_time_hours"] else 0
        })

    # Calculate Finance Opted Ratio by Customer Type
    finance_opted_ratio_by_customer_type = []
    for cust_type, counts in finance_opted_counts.items():
        total = counts["total"]
        yes = counts["yes"]
        ratio = round((yes / total * 100), 2) if total > 0 else 0.0
        finance_opted_ratio_by_customer_type.append({
            "customer_type": cust_type,
            "finance_opted_percent": ratio
        })

    return {
        "average_nps_by_city": {
            "type": "bar",
            "title": "Average NPS by City",
            "x": "city",
            "y": "average_nps",
            "data": avg_nps_by_city
        },
        "electric_vehicle_share_percent": {
            "type": "value",
            "title": "Electric Vehicle Share (%)",
            "value": ev_share_percent
        },
        "average_ev_metrics_by_oem": {
            "type": "bar",
            "title": "Average EV Metrics by OEM",
            "x": "oem",
            "y": ["avg_range_km", "avg_battery_kwh", "avg_charging_time_hours"],
            "data": avg_ev_metrics
        },
        "finance_opted_ratio_by_customer_type": {
            "type": "bar",
            "title": "Finance Opted Ratio by Customer Type",
            "x": "customer_type",
            "y": "finance_opted_percent",
            "data": finance_opted_ratio_by_customer_type
        }
    }

@router.get("/dashboard-tabs/")
async def get_dashboard_tabs(dashboard_id: str):
    """
    Returns the list of available tabs for a given dashboard.
    For 'auto_mobile_data', returns ['sales', 'supply', 'customer'].
    """
    if dashboard_id == "auto_mobile":
        return {"tabs": ["sales", "supply", "customer"]}
    # Add more dashboard_id checks as you add more dashboards
    return {"tabs": []}

@router.get("/dashboard-tab-kpis/{dashboard_id}/{tab}")
async def dashboard_tab_kpis_dynamic(
    dashboard_id: str,
    tab: str,
    db: Session = Depends(get_db),
    # Optional filters
    country: Optional[str] = None,
    region: Optional[str] = None,
    oem_name: Optional[str] = None,
    dealer_name: Optional[str] = None,
    city: Optional[str] = None,
    customer_type: Optional[str] = None,
):
    """
    Dynamic endpoint: /dashboard-tab-kpis/{dashboard_id}/{tab}
    Example: /dashboard-tab-kpis/auto_mobile_data/sales
    """
    # Example for auto_mobile_data dashboard
    if dashboard_id == "auto_mobile":
        if tab == "sales":
            return await get_sales_performance_kpis(
                db=db, country=country, region=region, oem_name=oem_name
            )
        elif tab == "supply":
            return await get_supply_aftersales_kpis(
                db=db, region=region, country=country, dealer_name=dealer_name
            )
        elif tab == "customer":
            return await get_customer_sustainability_kpis(
                db=db, city=city, customer_type=customer_type
            )
        elif tab == "descriptive":
            return await descriptive_data_api(
                db=db, country=country, brand=oem_name
            )
        else:
            raise HTTPException(404, "Tab not found for this dashboard.")
    # Add more dashboard_id logic here for other dashboards if needed
    else:
        raise HTTPException(404, "Dashboard not found or not supported.")