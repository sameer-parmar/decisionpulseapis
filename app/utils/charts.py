from collections import defaultdict

chart_functions = []

def chart_function(fn):
    chart_functions.append(fn)
    return fn

@chart_function
def chart_monthly_sales_by_oem(rows):
    monthly_sales = defaultdict(lambda: defaultdict(int))
    for r in rows:
        sale_date = r.get("sale_date")
        oem = r.get("oem_name")
        if sale_date and oem:
            ym = sale_date.strftime("%Y-%m")
            monthly_sales[ym][oem] += 1
    months = sorted(monthly_sales.keys())
    oems = sorted({oem for v in monthly_sales.values() for oem in v})
    monthly_sales_data = []
    for month in months:
        row = {"month": month}
        for oem in oems:
            row[oem] = monthly_sales[month].get(oem, 0)
        monthly_sales_data.append(row)
    return {
        "id": "monthly_sales_by_oem",
        "xKey": "month",
        "x-axis": oems,
        "y-axis": monthly_sales_data
    }

@chart_function
def chart_units_vs_price_by_region(rows):
    units_vs_price = defaultdict(list)
    for r in rows:
        region = r.get("region")
        u = r.get("units_sold")
        fp = r.get("final_price_after_discount") or r.get("final_price_after_discount_")
        if region and u is not None and fp is not None:
            units_vs_price[region].append({"units_sold": u, "final_price": fp})
    regions = sorted(units_vs_price.keys())
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
    return {
        "id": "units_vs_price_by_region",
        "xKey": "region",
        "x-axis": ["avg_units_sold", "avg_final_price"],
        "y-axis": units_vs_price_data
    }

@chart_function
def chart_nps_by_city(rows):
    city_scores = defaultdict(list)
    for r in rows:
        city = r.get("city")
        nps  = r.get("nps_customer_feedback")
        if city and nps is not None:
            city_scores[city].append(nps)
    nps_by_city_data = []
    for city, vals in city_scores.items():
        if len(vals) >= 3:
            nps_by_city_data.append({
                "city": city,
                "avg_nps": sum(vals) / len(vals)
            })
    return {
        "id": "nps_by_city",
        "xKey": "city",
        "x-axis": ["avg_nps"],
        "y-axis": nps_by_city_data
    }

@chart_function
def chart_fuel_vs_transmission(rows):
    fuel_trans = defaultdict(lambda: defaultdict(int))
    for r in rows:
        ft = r.get("fuel_type")
        tt = r.get("transmission_type")
        u = r.get("units_sold")
        if ft and tt and u is not None:
            fuel_trans[ft][tt] += u
    fuel_types = sorted(fuel_trans.keys())
    transmissions = sorted({tt for v in fuel_trans.values() for tt in v})
    fuel_vs_trans_data = []
    for ft in fuel_types:
        row = {"fuel_type": ft}
        for tt in transmissions:
            row[tt] = fuel_trans[ft].get(tt, 0)
        fuel_vs_trans_data.append(row)
    return {
        "id": "fuel_vs_transmission",
        "xKey": "fuel_type",
        "x-axis": transmissions,
        "y-axis": fuel_vs_trans_data
    }

@chart_function
def chart_statewise_units_market_share(rows):
    state_acc = defaultdict(lambda: {"units":0,"mkt_total":0.0,"count":0})
    for r in rows:
        st = r.get("state")
        u = r.get("units_sold")
        ms = r.get("market_share_in_region") or r.get("market_share_in_region_")
        if st and u is not None and ms is not None:
            acc = state_acc[st]
            acc["units"]     += u
            acc["mkt_total"] += ms
            acc["count"]     += 1
    statewise_data = []
    for st, d in state_acc.items():
        if d["count"] > 0:
            statewise_data.append({
                "state": st,
                "units_sold": d["units"],
                "avg_market_share": d["mkt_total"] / d["count"]
            })
    return {
        "id": "statewise_units_market_share",
        "xKey": "state",
        "x-axis": ["units_sold", "avg_market_share"],
        "y-axis": statewise_data
    }

@chart_function
def chart_delivery_delay_by_oem(rows):
    delays = defaultdict(list)
    for r in rows:
        oem = r.get("oem_name")
        booking = r.get("booking_date")
        delivery = r.get("delivery_date")
        if oem and booking and delivery:
            delays[oem].append((delivery - booking).days)
    delivery_delay_data = []
    for oem, L in delays.items():
        if L:
            delivery_delay_data.append({
                "oem": oem,
                "avg_delivery_delay_days": sum(L) / len(L)
            })
    return {
        "id": "delivery_delay_by_oem",
        "xKey": "oem",
        "x-axis": ["avg_delivery_delay_days"],
        "y-axis": delivery_delay_data
    }

@chart_function
def chart_discount_vs_units_by_customer(rows):
    disc_vs_units = defaultdict(list)
    for r in rows:
        ct = r.get("customer_type")
        disc = r.get("discount_offered") or r.get("discount_offered_")
        u = r.get("units_sold")
        if ct and disc is not None and u is not None:
            disc_vs_units[ct].append({"discount": disc, "units_sold": u})
    discount_vs_units_data = []
    for ct, vals in disc_vs_units.items():
        if vals:
            discount_vs_units_data.append({
                "customer_type": ct,
                "avg_discount": sum(d["discount"] for d in vals) / len(vals),
                "avg_units_sold": sum(d["units_sold"] for d in vals) / len(vals)
            })
    return {
        "id": "discount_vs_units_by_customer",
        "xKey": "customer_type",
        "x-axis": ["avg_discount", "avg_units_sold"],
        "y-a": discount_vs_units_data
    }

@chart_function
def chart_rating_vs_complaints_by_dealer(rows):
    dr_complaints = defaultdict(lambda: {"ratings": [], "complaints": 0})
    for r in rows:
        dlr = r.get("delivery_rating_15")
        dlr_yes = r.get("complaint_registered_yn", "").lower() == "yes"
        dealer = r.get("dealer_name")
        if dealer and dlr is not None:
            info = dr_complaints[dealer]
            info["ratings"].append(dlr)
            if dlr_yes:
                info["complaints"] += 1
    rating_vs_complaints_data = []
    for dlr, info in dr_complaints.items():
        if info["ratings"]:
            rating_vs_complaints_data.append({
                "dealer": dlr,
                "avg_rating": sum(info["ratings"]) / len(info["ratings"]),
                "complaint_count": info["complaints"]
            })
    return {
        "id": "rating_vs_complaints_by_dealer",
        "xKey": "dealer",
        "x-axis": ["avg_rating", "complaint_count"],
        "y-axis": rating_vs_complaints_data
    }

@chart_function
def chart_competitor_vs_final_price(rows):
    comp_vs_final = []
    for r in rows:
        cp = r.get("competitor_price")
        fp = r.get("final_price_after_discount") or r.get("final_price_after_discount_")
        oem = r.get("oem_name")
        if cp is not None and fp is not None:
            comp_vs_final.append({
                "oem": oem,
                "competitor_price": cp,
                "final_price": fp
            })
    return {
        "id": "competitor_vs_final_price",
        "xKey": "oem",
        "x-axis": ["competitor_price", "final_price"],
        "y-axis": comp_vs_final
    }

@chart_function
def chart_ev_metrics(rows):
    ev_metrics = []
    for r in rows:
        ft = r.get("fuel_type")
        if ft and "electric" in ft.lower():
            rng = r.get("range_km")
            bat = r.get("battery_capacity_kwh")
            chg = r.get("charging_time_hours")
            if rng is not None and bat is not None and chg is not None:
                ev_metrics.append({
                    "oem": r.get("oem_name"),
                    "range_km": rng,
                    "battery_kwh": bat,
                    "charging_time_hr": chg
                })
    return {
        "id": "ev_range_vs_battery_vs_charging",
        "xKey": "oem",
        "x-axis": ["range_km", "battery_kwh", "charging_time_hr"],
        "y-axis": ev_metrics
    }

@chart_function
def chart_market_share_by_oem(rows):
    oem_units = defaultdict(int)
    total_units = 0
    for r in rows:
        oem = r.get("oem_name")
        u = r.get("units_sold")
        if oem and u is not None:
            oem_units[oem] += u
            total_units += u
    market_share_oem = []
    for oem, units in sorted(oem_units.items(), key=lambda x: x[1], reverse=True):
        market_share_oem.append({
            "oem": oem,
            "units_sold": units,
            "market_share_percent": (units / total_units * 100) if total_units else 0
        })
    return {
        "id": "market_share_by_oem",
        "xKey": "oem",
        "x-axis": ["units_sold", "market_share_percent"],
        "y-axis": market_share_oem
    }

@chart_function
def chart_market_share_by_competitor_oem(rows):
    competitor_units = defaultdict(int)
    total_comp_units = 0
    for r in rows:
        competitor = r.get("competitor_oem")
        u = r.get("units_sold")
        if competitor and u is not None:
            competitor_units[competitor] += u
            total_comp_units += u
    market_share_comp = []
    for comp, units in sorted(competitor_units.items(), key=lambda x: x[1], reverse=True):
        market_share_comp.append({
            "competitor_oem": comp,
            "units_sold": units,
            "market_share_percent": (units / total_comp_units * 100) if total_comp_units else 0
        })
    return {
        "id": "market_share_by_competitor_oem",
        "xKey": "competitor_oem",
        "x-axis": ["units_sold", "market_share_percent"],
        "y-axis": market_share_comp
    }

@chart_function
def chart_top_selling_models(rows):
    model_units = defaultdict(int)
    for r in rows:
        model = r.get("vehicle_model")
        u = r.get("units_sold")
        if model and u is not None:
            model_units[model] += u
    top_models = sorted(model_units.items(), key=lambda x: x[1], reverse=True)[:10]
    top_models_data = [{"model": m, "units_sold": u} for m, u in top_models]
    return {
        "id": "top_selling_models",
        "xKey": "model",
        "x-axis": ["units_sold"],
        "y-axis": top_models_data
    }

@chart_function
def chart_avg_discount_by_brand(rows):
    brand_discount = defaultdict(list)
    for r in rows:
        oem = r.get("oem_name")
        disc = r.get("discount_offered") or r.get("discount_offered_")
        if oem and disc is not None:
            brand_discount[oem].append(disc)
    avg_discount_data = []
    for oem, discounts in brand_discount.items():
        if discounts:
            avg_discount_data.append({
                "oem": oem,
                "avg_discount": sum(discounts) / len(discounts)
            })
    return {
        "id": "avg_discount_by_brand",
        "xKey": "oem",
        "x-axis": ["avg_discount"],
        "y-axis": avg_discount_data
    }

@chart_function
def chart_sales_trend_by_vehicle_segment(rows):
    segment_trend = defaultdict(lambda: defaultdict(int))
    for r in rows:
        segment = r.get("vehicle_segment")
        sale_date = r.get("sale_date")
        u = r.get("units_sold")
        if segment and sale_date and u is not None:
            ym = sale_date.strftime("%Y-%m")
            segment_trend[segment][ym] += u
    segment_trend_data = []
    all_months = sorted({m for seg in segment_trend.values() for m in seg})
    for segment, month_units in segment_trend.items():
        row = {"vehicle_segment": segment}
        for m in all_months:
            row[m] = month_units.get(m, 0)
        segment_trend_data.append(row)
    return {
        "id": "sales_trend_by_vehicle_segment",
        "xKey": "vehicle_segment",
        "x-axis": all_months,
        "y-axis": segment_trend_data
    }

@chart_function
def chart_finance_opted_ratio_by_customer_type(rows):
    finance_by_cust = defaultdict(lambda: {"finance_yes": 0, "total": 0})
    for r in rows:
        cust_type = r.get("customer_type")
        finance_yn = r.get("finance_opted_yesno")
        if cust_type:
            finance_by_cust[cust_type]["total"] += 1
            if finance_yn and finance_yn.lower() == "yes":
                finance_by_cust[cust_type]["finance_yes"] += 1
    finance_ratio_data = []
    for cust_type, d in finance_by_cust.items():
        total = d["total"]
        yes = d["finance_yes"]
        ratio = (yes / total * 100) if total else 0
        finance_ratio_data.append({
            "customer_type": cust_type,
            "finance_opted_percent": ratio
        })
    return {
        "id": "finance_opted_ratio_by_customer_type",
        "xKey": "customer_type",
        "x-axis": ["finance_opted_percent"],
        "y-axis": finance_ratio_data
    }