from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from sqlalchemy import create_engine
import io
import os
import pandas as pd
import re

from sqlalchemy.orm import Session
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Any

from app.database import get_db

# IMPORTANT: You'll need to install openpyxl: pip install openpyxl
import openpyxl

from app.config import settings
from typing import Optional
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db  # Your DB session dependency
from app.models.datapoints import AutoMobileData  # Your SQLAlchemy Sale model
from collections import defaultdict
from datetime import datetime
from fastapi import APIRouter, Depends
from sqlalchemy import MetaData, Table, select
from sqlalchemy.orm import Session
from app.database import get_db
from collections import defaultdict
from datetime import datetime
from typing import Dict, Any, List


router = APIRouter()

UPLOAD_DIR = "uploaded_files"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def process_data_dump(file_bytes: bytes, original_filename: str, db_url: str, table_name: str):
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

    # Clean up column names
    df.columns = [
        re.sub(r'[^a-z0-9_]', '', col.strip().lower().replace(' ', '_'))
        for col in df.columns
    ]

    num_cols = len(df.columns)
    # Compute max rows per batch so that rows * cols ≤ 2100
    max_rows = max(1, 2100 // num_cols)
    total = len(df)
    print(f"{total} rows; {num_cols} cols → batching {max_rows} rows per chunk")

    # Loop and insert
    for idx in range(0, total, max_rows):
        chunk = df.iloc[idx : idx + max_rows]
        print(f"Inserting rows {idx}–{idx + len(chunk) - 1}...")
        chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace' if idx == 0 else 'append',
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
    if not file.filename:
        raise HTTPException(400, "No file uploaded.")

    ext = file.filename.rsplit('.', 1)[-1].lower()
    if ext not in ('csv', 'xlsx'):
        raise HTTPException(400, "Only .csv or .xlsx supported.")

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
    
    
    

@router.get("/descriptive-data-api", response_model=List[Dict[str, Any]])
async def descriptive_data_api(
    db: Session = Depends(get_db),
    country: str = None,
    brand: str = None
):
    # 1) Reflect the table
    metadata = MetaData()
    auto_table = Table(
        'auto_mobile_data',
        metadata,
        autoload_with=db.bind
    )

    # 2) Fetch every row as a dict
    rows: List[Dict[str, Any]] = (
        db.execute(select(auto_table))
          .mappings()
          .all()
    )

    # --- FILTERING ---
    filtered_rows = []
    for r in rows:
        if country and r.get("country") and r["country"].lower() != country.lower():
            continue
        if brand and r.get("oem_name") and r["oem_name"].lower() != brand.lower():
            continue
        filtered_rows.append(r)
    rows = filtered_rows

    # --- KPI AGGREGATION ---
    monthly_sales      = defaultdict(lambda: defaultdict(int))
    units_vs_price     = defaultdict(list)
    city_scores        = defaultdict(list)
    fuel_trans         = defaultdict(lambda: defaultdict(int))
    state_acc          = defaultdict(lambda: {"units":0,"mkt_total":0.0,"count":0})
    delays             = defaultdict(list)
    disc_vs_units      = defaultdict(list)
    dr_complaints      = defaultdict(lambda: {"ratings": [], "complaints": 0})
    comp_vs_final      = []
    ev_metrics         = []

    # --- NEW AGGREGATIONS ---
    oem_units          = defaultdict(int)
    competitor_units   = defaultdict(int)
    model_units        = defaultdict(int)
    brand_discount     = defaultdict(list)
    segment_trend      = defaultdict(lambda: defaultdict(int))
    finance_by_cust    = defaultdict(lambda: {"finance_yes": 0, "total": 0})

    for r in rows:
        # parse dates if needed
        sale_date = r.get("sale_date")
        booking   = r.get("booking_date")
        delivery  = r.get("delivery_date")

        # OEM / month KPI
        if sale_date and r.get("oem_name"):
            ym = sale_date.strftime("%Y-%m")
            monthly_sales[ym][r["oem_name"]] += 1

        # Units vs Final Price by region
        region = r.get("region")
        u = r.get("units_sold")
        fp = r.get("final_price_after_discount") or r.get("final_price_after_discount_")
        if region and u is not None and fp is not None:
            units_vs_price[region].append({"units_sold": u, "final_price": fp})

        # NPS by city
        city = r.get("city")
        nps  = r.get("nps_customer_feedback")
        if city and nps is not None:
            city_scores[city].append(nps)

        # Fuel vs transmission
        ft = r.get("fuel_type")
        tt = r.get("transmission_type")
        if ft and tt and u is not None:
            fuel_trans[ft][tt] += u

        # State‐wise market share
        st = r.get("state")
        ms = r.get("market_share_in_region") or r.get("market_share_in_region_")
        if st and u is not None and ms is not None:
            acc = state_acc[st]
            acc["units"]     += u
            acc["mkt_total"] += ms
            acc["count"]     += 1

        # Delivery delay by OEM
        oem = r.get("oem_name")
        if oem and booking and delivery:
            delays[oem].append((delivery - booking).days)

        # Discount vs units by customer
        ct = r.get("customer_type")
        disc = r.get("discount_offered") or r.get("discount_offered_")
        if ct and disc is not None and u is not None:
            disc_vs_units[ct].append({"discount": disc, "units_sold": u})

        # Delivery rating vs complaints by dealer
        dlr = r.get("delivery_rating_15")
        dlr_yes = r.get("complaint_registered_yn", "").lower() == "yes"
        dealer = r.get("dealer_name")
        if dealer and dlr is not None:
            info = dr_complaints[dealer]
            info["ratings"].append(dlr)
            if dlr_yes:
                info["complaints"] += 1

        # Competitor vs final price
        cp = r.get("competitor_price")
        if cp is not None and fp is not None:
            comp_vs_final.append({
                "oem": oem,
                "competitor_price": cp,
                "final_price": fp
            })

        # EV metrics
        if ft and "electric" in ft.lower():
            rng = r.get("range_km")
            bat = r.get("battery_capacity_kwh")
            chg = r.get("charging_time_hours")
            if rng is not None and bat is not None and chg is not None:
                ev_metrics.append({
                    "oem": oem,
                    "range_km": rng,
                    "battery_kwh": bat,
                    "charging_time_hr": chg
                })

        # Market share by OEM and competitor_oem
        oem = r.get("oem_name")
        competitor = r.get("competitor_oem")
        u = r.get("units_sold")
        if oem and u is not None:
            oem_units[oem] += u
        if competitor and u is not None:
            competitor_units[competitor] += u

        # Top selling models
        model = r.get("vehicle_model")
        if model and u is not None:
            model_units[model] += u

        # Average discount by brand
        disc = r.get("discount_offered") or r.get("discount_offered_")
        if oem and disc is not None:
            brand_discount[oem].append(disc)

        # Sales trend by vehicle segment
        segment = r.get("vehicle_segment")
        sale_date = r.get("sale_date")
        if segment and sale_date and u is not None:
            ym = sale_date.strftime("%Y-%m")
            segment_trend[segment][ym] += u

        # Finance opted ratio by customer type
        cust_type = r.get("customer_type")
        finance_yn = r.get("finance_opted_yesno")
        if cust_type:
            finance_by_cust[cust_type]["total"] += 1
            if finance_yn and finance_yn.lower() == "yes":
                finance_by_cust[cust_type]["finance_yes"] += 1

    # 1. Monthly sales by OEM (x: months, y: sales per OEM)
    months = sorted(monthly_sales.keys())
    oems = sorted({oem for v in monthly_sales.values() for oem in v})
    monthly_sales_data = []
    for i, month in enumerate(months):
        row = {"month": month}
        for j, oem in enumerate(oems):
            row[oem] = monthly_sales[month].get(oem, 0)
        monthly_sales_data.append(row)
    chart1 = {
        "id": "monthly_sales_by_oem",
        "xKey": "month",
        "x-axis": oems,
        "y-axis": monthly_sales_data
    }

    # 2. Units vs Final Price by region (x: region, y1: avg units, y2: avg price)
    regions = sorted(units_vs_price.keys())
    units = []
    prices = []
    units_vs_price_data = []
    for region in regions:
        region_data = units_vs_price[region]
        avg_units = sum(d["units_sold"] for d in region_data) / len(region_data) if region_data else 0
        avg_price = sum(d["final_price"] for d in region_data) / len(region_data) if region_data else 0
        units_vs_price_data.append({
            "region": region,
            "avg_units_sold": avg_units,
            "avg_final_price": avg_price
        })
    chart2 = {
        "id": "units_vs_price_by_region",
        "xKey": "region",
        "x-axis": ["avg_units_sold", "avg_final_price"],
        "y-axis": units_vs_price_data
    }

    # 3. NPS by city (x: city, y: avg NPS)
    nps_by_city_data = []
    for city, vals in city_scores.items():
        if len(vals) >= 3:
            nps_by_city_data.append({
                "city": city,
                "avg_nps": sum(vals) / len(vals)
            })
    chart3 = {
        "id": "nps_by_city",
        "xKey": "city",
        "x-axis": ["avg_nps"],
        "y-axis": nps_by_city_data
    }

    # 4. Fuel vs transmission (x: fuel_type, y: units per transmission type)
    fuel_types = sorted(fuel_trans.keys())
    transmissions = sorted({tt for v in fuel_trans.values() for tt in v})
    fuel_vs_trans_data = []
    for ft in fuel_types:
        row = {"fuel_type": ft}
        for tt in transmissions:
            row[tt] = fuel_trans[ft].get(tt, 0)
        fuel_vs_trans_data.append(row)
    chart4 = {
        "id": "fuel_vs_transmission",
        "xKey": "fuel_type",
        "x-axis": transmissions,
        "y-axis": fuel_vs_trans_data
    }

    # 5. Statewise units & avg market share (x: state, y1: units, y2: avg market share)
    statewise_data = []
    for st, d in state_acc.items():
        if d["count"] > 0:
            statewise_data.append({
                "state": st,
                "units_sold": d["units"],
                "avg_market_share": d["mkt_total"] / d["count"]
            })
    chart5 = {
        "id": "statewise_units_market_share",
        "xKey": "state",
        "x-axis": ["units_sold", "avg_market_share"],
        "y-axis": statewise_data
    }

    # 6. Delivery delay by OEM (x: oem, y: avg delay)
    delivery_delay_data = []
    for oem, L in delays.items():
        if L:
            delivery_delay_data.append({
                "oem": oem,
                "avg_delivery_delay_days": sum(L) / len(L)
            })
    chart6 = {
        "id": "delivery_delay_by_oem",
        "xKey": "oem",
        "x-axis": ["avg_delivery_delay_days"],
        "y-axis": delivery_delay_data
    }

    # 7. Discount vs units by customer type (x: customer_type, y1: avg discount, y2: avg units)
    discount_vs_units_data = []
    for ct, vals in disc_vs_units.items():
        if vals:
            discount_vs_units_data.append({
                "customer_type": ct,
                "avg_discount": sum(d["discount"] for d in vals) / len(vals),
                "avg_units_sold": sum(d["units_sold"] for d in vals) / len(vals)
            })
    chart7 = {
        "id": "discount_vs_units_by_customer",
        "xKey": "customer_type",
        "x-axis": ["avg_discount", "avg_units_sold"],
        "y-a": discount_vs_units_data
    }

    # 8. Rating vs complaints by dealer (x: dealer, y1: avg rating, y2: complaint count)
    rating_vs_complaints_data = []
    for dlr, info in dr_complaints.items():
        if info["ratings"]:
            rating_vs_complaints_data.append({
                "dealer": dlr,
                "avg_rating": sum(info["ratings"]) / len(info["ratings"]),
                "complaint_count": info["complaints"]
            })
    chart8 = {
        "id": "rating_vs_complaints_by_dealer",
        "xKey": "dealer",
        "x-axis": ["avg_rating", "complaint_count"],
        "y-axis": rating_vs_complaints_data
    }

    # 9. Competitor vs final price (x: oem, y1: competitor price, y2: final price)
    competitor_vs_final_data = []
    for item in comp_vs_final:
        competitor_vs_final_data.append({
            "oem": item["oem"],
            "competitor_price": item["competitor_price"],
            "final_price": item["final_price"]
        })
    chart9 = {
        "id": "competitor_vs_final_price",
        "xKey": "oem",
        "x-axis": ["competitor_price", "final_price"],
        "y-axis": competitor_vs_final_data
    }

    # 10. EV metrics (x: oem, y1: range, y2: battery, y3: charging time)
    ev_metrics_data = []
    for item in ev_metrics:
        ev_metrics_data.append({
            "oem": item["oem"],
            "range_km": item["range_km"],
            "battery_kwh": item["battery_kwh"],
            "charging_time_hr": item["charging_time_hr"]
        })
    chart10 = {
        "id": "ev_range_vs_battery_vs_charging",
        "xKey": "oem",
        "x-axis": ["range_km", "battery_kwh", "charging_time_hr"],
        "y-axis": ev_metrics_data
    }

    # 11. Market share by OEM (brand)
    total_units = sum(oem_units.values())
    market_share_oem = []
    for oem, units in sorted(oem_units.items(), key=lambda x: x[1], reverse=True):
        market_share_oem.append({
            "oem": oem,
            "units_sold": units,
            "market_share_percent": (units / total_units * 100) if total_units else 0
        })
    chart11 = {
        "id": "market_share_by_oem",
        "xKey": "oem",
        "x-axis": ["units_sold", "market_share_percent"],
        "y-axis": market_share_oem
    }

    # 12. Market share by competitor_oem (company)
    total_comp_units = sum(competitor_units.values())
    market_share_comp = []
    for comp, units in sorted(competitor_units.items(), key=lambda x: x[1], reverse=True):
        market_share_comp.append({
            "competitor_oem": comp,
            "units_sold": units,
            "market_share_percent": (units / total_comp_units * 100) if total_comp_units else 0
        })
    chart12 = {
        "id": "market_share_by_competitor_oem",
        "xKey": "competitor_oem",
        "x-axis": ["units_sold", "market_share_percent"],
        "y-axis": market_share_comp
    }

    # 13. Top selling models
    top_models = sorted(model_units.items(), key=lambda x: x[1], reverse=True)[:10]
    top_models_data = [{"model": m, "units_sold": u} for m, u in top_models]
    chart13 = {
        "id": "top_selling_models",
        "xKey": "model",
        "x-axis": ["units_sold"],
        "y-axis": top_models_data
    }

    # 14. Average discount by brand
    avg_discount_data = []
    for oem, discounts in brand_discount.items():
        if discounts:
            avg_discount_data.append({
                "oem": oem,
                "avg_discount": sum(discounts) / len(discounts)
            })
    chart14 = {
        "id": "avg_discount_by_brand",
        "xKey": "oem",
        "x-axis": ["avg_discount"],
        "y-axis": avg_discount_data
    }

    # 15. Sales trend by vehicle segment
    segment_trend_data = []
    all_months = sorted({m for seg in segment_trend.values() for m in seg})
    for segment, month_units in segment_trend.items():
        row = {"vehicle_segment": segment}
        for m in all_months:
            row[m] = month_units.get(m, 0)
        segment_trend_data.append(row)
    chart15 = {
        "id": "sales_trend_by_vehicle_segment",
        "xKey": "vehicle_segment",
        "x-axis": all_months,
        "y-axis": segment_trend_data
    }

    # 16. Finance opted ratio by customer type
    finance_ratio_data = []
    for cust_type, d in finance_by_cust.items():
        total = d["total"]
        yes = d["finance_yes"]
        ratio = (yes / total * 100) if total else 0
        finance_ratio_data.append({
            "customer_type": cust_type,
            "finance_opted_percent": ratio
        })
    chart16 = {
        "id": "finance_opted_ratio_by_customer_type",
        "xKey": "customer_type",
        "x-axis": ["finance_opted_percent"],
        "y-axis": finance_ratio_data
    }

    return [
        chart1,
        chart2,
        chart3,
        chart4,
        chart5,
        chart6,
        chart7,
        chart8,
        chart9,
        chart10,
        chart11,
        chart12,
        chart13,
        chart14,
        chart15,
        chart16
    ]