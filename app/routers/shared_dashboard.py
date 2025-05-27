from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Optional
from app.database import get_db
from app.routers.upload_data import (
    get_sales_performance_kpis,
    get_supply_aftersales_kpis,
    get_customer_sustainability_kpis,
    descriptive_data_api,
)
from app.routers.fmcgrouters import fmcg_dashboard_tab_kpis

router = APIRouter()

@router.get("/dashboard-tabs/")
async def get_dashboard_tabs(dashboard_id: str):
    """
    Returns the list of available tabs for a given dashboard.
    """
    if dashboard_id == "auto_mobile":
        return {"tabs": ["sales", "supply", "customer", "descriptive"]}
    if dashboard_id == "fmcg":
        return {
            "tabs": [
                "global_regional_sales",
                "supply_chain",
                "marketing_brand",
                "financial_profitability",
                "consumer_insights",
                "sustainability_compliance"
            ]
        }
    # Add more dashboard_id checks as you add more dashboards
    return {"tabs": []}

@router.get("/dashboard-tab-kpis/{dashboard_id}/{tab}")
async def dashboard_tab_kpis_dynamic(
    dashboard_id: str,
    tab: str,
    db: Session = Depends(get_db),
    country: Optional[str] = None,
    region: Optional[str] = None,
    oem_name: Optional[str] = None,
    dealer_name: Optional[str] = None,
    city: Optional[str] = None,
    customer_type: Optional[str] = None,
    brand: Optional[str] = None,
    category: Optional[str] = None,
):
    """
    Dynamic endpoint: /dashboard-tab-kpis/{dashboard_id}/{tab}
    Supports both auto_mobile and fmcg dashboards.
    """
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
    elif dashboard_id == "fmcg":
        return await fmcg_dashboard_tab_kpis(
            tab=tab,
            db=db,
            region=region,
            country=country,
            brand=brand,
            category=category
        )
    else:
        raise HTTPException(404, "Dashboard not found or not supported.")
