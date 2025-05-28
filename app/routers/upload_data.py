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
    save_file: Optional[bool] = False
):
    """
    Endpoint to upload raw data files (CSV or XLSX) for processing and database dumping.
    Automatically generates a safe table name from the uploaded filename.
    """
    if not file.filename:
        raise HTTPException(400, "No file uploaded.")

    ext = file.filename.rsplit('.', 1)[-1].lower()
    if ext not in ('csv', 'xlsx'):
        raise HTTPException(400, "Only .csv or .xlsx supported.")

    # Generate safe table name from filename
    base_name = os.path.splitext(file.filename)[0]  # Remove .csv/.xlsx
    cleaned_base = re.sub(r'[^a-zA-Z0-9_]', '_', base_name.strip().lower())
    table_name = f"table_{cleaned_base}"

    # Ensure valid table name format
    if not re.match(r'^[a-zA-Z_]\w*$', table_name):
        raise HTTPException(400, f"Invalid generated table name: {table_name}")

    raw = await file.read()
    if not raw:
        raise HTTPException(400, "Uploaded file is empty.")

    if save_file:
        path = os.path.join(UPLOAD_DIR, file.filename)
        with open(path, 'wb') as f:
            f.write(raw)
        print(f"Saved to {path}")

    print(f"Scheduling batch dump for '{file.filename}' → '{table_name}'")
    background_tasks.add_task(
        process_data_dump,
        raw,
        file.filename,
        settings.sqlalchemy_database_uri,
        table_name
    )

    return {
        "message": f"Received '{file.filename}'. Batch processing started.",
        "table": table_name
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
    query = db.query(AutoMobileData)
    if country:
        query = query.filter(AutoMobileData.country.ilike(f"%{country}%"))
    if region:
        query = query.filter(AutoMobileData.region.ilike(f"%{region}%"))
    if oem_name:
        query = query.filter(AutoMobileData.oem_name.ilike(f"%{oem_name}%"))
    all_data = query.all()
    rows = [
        {
            k: v
            for k, v in row.__dict__.items()
            if not k.startswith("_sa_instance_state")
        }
        for row in all_data
    ]

    total_units_sold = 0
    total_revenue = 0
    oem_units_sold = defaultdict(int)
    competitor_units_sold = defaultdict(int)
    sales_by_year = defaultdict(int)
    customer_type_sales = defaultdict(int)
    channel_sales = defaultdict(int)
    asp_by_segment = defaultdict(lambda: {"total_price": 0, "units": 0})
    sales_by_month_year = defaultdict(int)  # Initialize here
    sales_by_oem_year_month = defaultdict(lambda: defaultdict(int)) # Initialize here

    for r in rows:
        units = r.get("units_sold", 0)
        final_price_str = r.get("final_price_after_discount")
        try:
            final_price = float(final_price_str) if final_price_str is not None else 0.0
        except (ValueError, TypeError):
            final_price = 0.0
        oem = r.get("oem_name")
        competitor = r.get("competitor_oem")
        sale_date_str = r.get("sale_date")
        vehicle_segment = r.get("vehicle_segment")
        exchange_offered = str(r.get("exchange_vehicle_offered", "")).strip().lower()
        lead_source = str(r.get("lead_source", "")).strip().lower()
        cust_type_raw = str(r.get("customer_type", "")).strip().lower()

        total_units_sold += units
        total_revenue += (final_price * units)

        if oem:
            oem_units_sold[oem] += units
        if competitor:
            competitor_units_sold[competitor] += units

        if sale_date_str:
            try:
                if isinstance(sale_date_str, str):
                    date_obj = datetime.strptime(sale_date_str.split(" ")[0], "%Y-%m-%d")
                elif isinstance(sale_date_str, datetime):
                    date_obj = sale_date_str
                else:
                    date_obj = None  # Handle cases where it's not a string or datetime

                if date_obj:
                    sale_year = date_obj.year
                    sales_by_year[sale_year] += units

                    month_year = date_obj.strftime("%Y-%m")
                    sales_by_month_year[month_year] += units
                    if oem:
                        sales_by_oem_year_month[oem][month_year] += units

            except ValueError:
                pass

        if exchange_offered == "yes":
            customer_type_sales["returning"] += units
        elif exchange_offered == "no":
            customer_type_sales["new"] += units

        if cust_type_raw == "fleet":
            channel_sales["fleet"] += units
        elif lead_source in {"digital", "website", "online"}:
            channel_sales["online"] += units
        else:
            channel_sales["dealership"] += units

        if vehicle_segment:
            asp_by_segment[vehicle_segment]["total_price"] += final_price * units
            asp_by_segment[vehicle_segment]["units"] += units

    market_share_by_oem = []
    if total_units_sold > 0:
        for oem, units in oem_units_sold.items():
            market_share_by_oem.append({
                "oem": oem,
                "units_sold": units,
                "market_share_percent": round((units / total_units_sold) * 100, 2)
            })
    market_share_by_oem = sorted(market_share_by_oem, key=lambda x: x["market_share_percent"], reverse=True)

    total_competitor_units_sold = sum(competitor_units_sold.values())
    market_share_by_competitor_oem = []
    if total_competitor_units_sold > 0:
        for comp, units in competitor_units_sold.items():
            market_share_by_competitor_oem.append({
                "competitor_oem": comp,
                "units_sold": units,
                "market_share_percent": round((units / total_competitor_units_sold) * 100, 2)
            })
    market_share_by_competitor_oem = sorted(market_share_by_competitor_oem, key=lambda x: x["market_share_percent"], reverse=True)

    average_selling_price = total_revenue / total_units_sold if total_units_sold > 0 else 0.0
    asp_by_segment_data = []
    for segment, data in asp_by_segment.items():
        avg_price = data["total_price"] / data["units"] if data["units"] > 0 else 0.0
        asp_by_segment_data.append({"segment": segment, "average_price": round(avg_price, 2)})

    charts = [
        {"id": "total_units_sold", "xKey": "total_units_sold", "x-axis": ["value"], "y-axis": [{"metric": "Total Units Sold", "value": total_units_sold}]},
        {"id": "average_selling_price", "xKey": "average_selling_price", "x-axis": ["value"], "y-axis": [{"metric": "Average Selling Price", "value": round(average_selling_price, 2)}]},
        {"id": "market_share_by_oem", "xKey": "oem", "x-axis": ["market_share_percent"], "y-axis": market_share_by_oem},
        {"id": "market_share_by_competitor_oem", "xKey": "competitor_oem", "x-axis": ["market_share_percent"], "y-axis": market_share_by_competitor_oem},
        {"id": "yoy_sales_units", "xKey": "year", "x-axis": ["units"], "y-axis": [{"year": str(k), "units": v} for k, v in sales_by_year.items()]},
        {"id": "customer_type_sales", "xKey": "customer_type", "x-axis": ["units_sold"], "y-axis": [{"customer_type": k, "units_sold": v} for k, v in customer_type_sales.items()]},
        {"id": "channel_sales", "xKey": "channel", "x-axis": ["units_sold"], "y-axis": [{"channel": k, "units_sold": v} for k, v in channel_sales.items()]},
        {"id": "asp_by_vehicle_segment", "xKey": "segment", "x-axis": ["average_price"], "y-axis": asp_by_segment_data},
    ]

    # 1. YoY Sales Growth % (using sale_date)
    sales_by_month_year_data = defaultdict(int) # Redundant, using the one initialized at the top
    for r in rows:
        if r.get("sale_date"):
            try:
                if isinstance(r["sale_date"], str):
                    date_obj = datetime.strptime(r["sale_date"].split(" ")[0], "%Y-%m-%d")
                elif isinstance(r["sale_date"], datetime):
                    date_obj = r["sale_date"]
                else:
                    date_obj = None

                if date_obj:
                    month_year = date_obj.strftime("%Y-%m")
                    sales_by_month_year[month_year] += r.get("units_sold", 0)
            except ValueError:
                pass

    sorted_month_years = sorted(sales_by_month_year.keys())
    yoy_growth_data = []
    if len(sorted_month_years) > 12: # Need at least two years of data for YoY comparison
        for i in range(12, len(sorted_month_years)):
            prev_year_month = sorted_month_years[i-12]
            current_year_month = sorted_month_years[i]
            prev_sales = sales_by_month_year.get(prev_year_month, 0)
            current_sales = sales_by_month_year.get(current_year_month, 0)
            growth_percent = round(((current_sales - prev_sales) / prev_sales) * 100, 2) if prev_sales else 0.0
            year = current_year_month[:4]
            month = current_year_month[5:]
            yoy_growth_data.append({"period": f"{year}-{month}", "growth_percent": growth_percent})

    if yoy_growth_data:
        charts.append({"id": "yoy_sales_growth_percent", "xKey": "period", "x-axis": ["growth_percent"], "y-axis": yoy_growth_data})

    # 2. Channel-wise Sales Breakdown (using channel_sales)
    if channel_sales:
        charts.append({"id": "channel_contribution", "xKey": "channel", "x-axis": ["units_sold"], "y-axis": [{"channel": k, "units_sold": v} for k, v in channel_sales.items()]})

    # 3. Sales by OEM over time (using sale_date and oem_name)
    sales_by_oem_year_month_data = defaultdict(lambda: defaultdict(int)) # Redundant, using the one initialized at the top
    for r in rows:
        if r.get("sale_date") and r.get("oem_name"):
            try:
                if isinstance(r["sale_date"], str):
                    date_obj = datetime.strptime(r["sale_date"].split(" ")[0], "%Y-%m-%d")
                elif isinstance(r["sale_date"], datetime):
                    date_obj = r["sale_date"]
                else:
                    date_obj = None

                if date_obj:
                    month_year = date_obj.strftime("%Y-%m")
                    sales_by_oem_year_month[r["oem_name"]][month_year] += r.get("units_sold", 0)
            except ValueError:
                pass

    sales_trend_by_oem_data = []
    all_months = sorted(list(set(month for oem_data in sales_by_oem_year_month.values() for month in oem_data.keys())))
    for oem, monthly_sales in sales_by_oem_year_month.items():
        series_data = [{"month": m, "sales": monthly_sales.get(m, 0)} for m in all_months]
        sales_trend_by_oem_data.append({"oem": oem, "series": series_data})

    if sales_trend_by_oem_data:
        charts.append({"id": "sales_trend_by_oem", "xKey": "month", "y-axis": "sales", "seriesKey": "oem", "data": sales_trend_by_oem_data})

    return charts
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

    charts = []

    # Average Delivery Time
    charts.append({
        "id": "average_delivery_time_days",
        "xKey": "average_delivery_time_days",
        "x-axis": ["average_delivery_time_days"],
        "y-axis": [{"average_delivery_time_days": avg_delivery_time_days}]
    })

    # Average Delivery Rating by Dealer
    charts.append({
        "id": "average_delivery_rating_by_dealer",
        "xKey": "dealer_name",
        "x-axis": ["avg_rating"],
        "y-axis": avg_delivery_rating_by_dealer
    })

    # Complaint Count by Dealer
    charts.append({
        "id": "complaint_count_by_dealer",
        "xKey": "dealer_name",
        "x-axis": ["complaint_count"],
        "y-axis": complaint_count_by_dealer
    })

    return charts

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

    charts = []

    # Average NPS by City
    charts.append({
        "id": "average_nps_by_city",
        "xKey": "city",
        "x-axis": ["average_nps"],
        "y-axis": avg_nps_by_city
    })

    # Electric Vehicle Share Percent
    charts.append({
        "id": "electric_vehicle_share_percent",
        "xKey": "electric_vehicle_share_percent",
        "x-axis": ["electric_vehicle_share_percent"],
        "y-axis": [{"electric_vehicle_share_percent": ev_share_percent}]
    })

    # Average EV Metrics by OEM
    charts.append({
        "id": "average_ev_metrics_by_oem",
        "xKey": "oem",
        "x-axis": ["avg_range_km", "avg_battery_kwh", "avg_charging_time_hours"],
        "y-axis": avg_ev_metrics
    })

    # Finance Opted Ratio by Customer Type
    charts.append({
        "id": "finance_opted_ratio_by_customer_type",
        "xKey": "customer_type",
        "x-axis": ["finance_opted_percent"],
        "y-axis": finance_opted_ratio_by_customer_type
    })

    return charts

# The following endpoints have been moved to shared_dashboard.py:
# - /dashboard-tabs/
# - /dashboard-tab-kpis/{dashboard_id}/{tab}