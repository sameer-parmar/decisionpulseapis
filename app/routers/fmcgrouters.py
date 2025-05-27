from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.orm import Session
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Any, Optional
from app.database import get_db
router = APIRouter()
# app/routers/fmcgrouters.py

async def fmcg_dashboard_tab_kpis(
    tab: str,
    db: Session = Depends(get_db),
    region: Optional[str] = None,
    country: Optional[str] = None,
    brand: Optional[str] = None,
    category: Optional[str] = None
):
    """
    FMCG Dashboard API: /dashboard-tab-kpis/fmcg/{tab}
    Tabs: global_regional_sales, supply_chain, marketing_brand, financial_profitability, consumer_insights, sustainability_compliance
    """
    
    # Reflect the FMCG table
    metadata = MetaData()
    fmcg_table = Table('table_fmcg', metadata, autoload_with=db.bind)
    
    # Base query with filters
    query = select(fmcg_table)
    if region:
        query = query.where(fmcg_table.c.region.ilike(f"%{region}%"))
    if country:
        query = query.where(fmcg_table.c.market.ilike(f"%{country}%"))
    if brand:
        query = query.where(fmcg_table.c.brand.ilike(f"%{brand}%"))
    if category:
        query = query.where(fmcg_table.c.category.ilike(f"%{category}%"))
    
    rows = db.execute(query).mappings().all()
    
    if tab == "global_regional_sales":
        # Units sold by region
        units_by_region = defaultdict(int)
        revenue_by_region = defaultdict(float)
        asp_by_region = defaultdict(lambda: {"sum": 0, "count": 0})
        sales_by_channel = defaultdict(float)
        market_share_by_region = defaultdict(float)
        product_performance = defaultdict(int)
        
        for r in rows:
            region = r.get("region", "Unknown")
            units = r.get("units_sold", 0)
            revenue = r.get("revenue", 0.0)
            selling_price = r.get("selling_price", 0.0)
            channel = r.get("channel", "Unknown")
            product_name = r.get("product_name", "Unknown")
            market_share = r.get("market_share_", 0.0)
            
            units_by_region[region] += units
            revenue_by_region[region] += revenue
            sales_by_channel[channel] += revenue
            product_performance[product_name] += units
            market_share_by_region[region] += market_share
            
            if selling_price > 0:
                asp_by_region[region]["sum"] += selling_price
                asp_by_region[region]["count"] += 1
        
        return [
            {
                "id": "units_sold_by_region",
                "xKey": "region",
                "x-axis": ["units_sold"],
                "y-axis": [{"region": k, "units_sold": v} for k, v in units_by_region.items()]
            },
            {
                "id": "revenue_by_region", 
                "xKey": "region",
                "x-axis": ["revenue"],
                "y-axis": [{"region": k, "revenue": round(v, 2)} for k, v in revenue_by_region.items()]
            },
            {
                "id": "average_selling_price_by_region",
                "xKey": "region",
                "x-axis": ["average_selling_price"],
                "y-axis": [{"region": k, "average_selling_price": round(v["sum"] / v["count"], 2) if v["count"] else 0} for k, v in asp_by_region.items()]
            },
            {
                "id": "sales_by_channel",
                "xKey": "channel",
                "x-axis": ["revenue"],
                "y-axis": [{"channel": k, "revenue": round(v, 2)} for k, v in sales_by_channel.items()]
            },
            {
                "id": "market_share_by_region",
                "xKey": "region",
                "x-axis": ["market_share"],
                "y-axis": [{"region": k, "market_share": round(v, 2)} for k, v in market_share_by_region.items()]
            },
            {
                "id": "product_performance",
                "xKey": "product_name",
                "x-axis": ["units_sold"],
                "y-axis": [{"product_name": k, "units_sold": v} for k, v in sorted(product_performance.items(), key=lambda x: x[1], reverse=True)]
            }
        ]
    
    elif tab == "supply_chain":
        delivery_time_by_region = defaultdict(list)
        stock_levels = defaultdict(int)
        out_of_stock_by_region = defaultdict(lambda: {"total": 0, "oos": 0})
        
        for r in rows:
            region = r.get("region", "Unknown")
            delivery_days = r.get("delivery_time_days", 0)
            stock = r.get("stock_on_hand", 0)
            oos_flag = r.get("out_of_stock_flag", "No")
            
            delivery_time_by_region[region].append(delivery_days)
            stock_levels[region] += stock
            out_of_stock_by_region[region]["total"] += 1
            if oos_flag.lower() == "yes":
                out_of_stock_by_region[region]["oos"] += 1
        
        return [
            {
                "id": "avg_delivery_time_by_region",
                "xKey": "region",
                "x-axis": ["avg_delivery_days"],
                "y-axis": [{"region": k, "avg_delivery_days": round(sum(v)/len(v), 2) if v else 0} for k, v in delivery_time_by_region.items()]
            },
            {
                "id": "stock_levels_by_region",
                "xKey": "region", 
                "x-axis": ["total_stock"],
                "y-axis": [{"region": k, "total_stock": v} for k, v in stock_levels.items()]
            },
            {
                "id": "stockout_rate_by_region",
                "xKey": "region",
                "x-axis": ["stockout_percentage"],
                "y-axis": [{"region": k, "stockout_percentage": round((v["oos"]/v["total"])*100, 2) if v["total"] else 0} for k, v in out_of_stock_by_region.items()]
            }
        ]
    
    elif tab == "marketing_brand":
        brand_penetration_by_region = defaultdict(list)
        promotion_performance = defaultdict(float)
        
        for r in rows:
            region = r.get("region", "Unknown")
            penetration = r.get("brand_penetration_", 0.0)
            promo_type = r.get("promotion_type", "None")
            revenue = r.get("revenue", 0.0)
            
            brand_penetration_by_region[region].append(penetration)
            promotion_performance[promo_type] += revenue
        
        return [
            {
                "id": "brand_penetration_by_region",
                "xKey": "region",
                "x-axis": ["avg_penetration"],
                "y-axis": [{"region": k, "avg_penetration": round(sum(v)/len(v), 2) if v else 0} for k, v in brand_penetration_by_region.items()]
            },
            {
                "id": "promotion_performance",
                "xKey": "promotion_type",
                "x-axis": ["revenue"],
                "y-axis": [{"promotion_type": k, "revenue": round(v, 2)} for k, v in promotion_performance.items()]
            }
        ]
    
    elif tab == "financial_profitability":
        revenue_by_region = defaultdict(float)
        profit_by_region = defaultdict(float)
        cost_by_region = defaultdict(float)
        
        for r in rows:
            region = r.get("region", "Unknown")
            revenue = r.get("revenue", 0.0)
            profit = r.get("profit", 0.0)
            cost = r.get("cost_to_company", 0.0)
            
            revenue_by_region[region] += revenue
            profit_by_region[region] += profit
            cost_by_region[region] += cost
        
        return [
            {
                "id": "revenue_by_region",
                "xKey": "region",
                "x-axis": ["total_revenue"],
                "y-axis": [{"region": k, "total_revenue": round(v, 2)} for k, v in revenue_by_region.items()]
            },
            {
                "id": "profit_margin_by_region",
                "xKey": "region",
                "x-axis": ["profit_margin"],
                "y-axis": [{"region": k, "profit_margin": round((profit_by_region[k]/revenue_by_region[k])*100, 2) if revenue_by_region[k] else 0} for k in revenue_by_region.keys()]
            },
            {
                "id": "cost_breakdown_by_region",
                "xKey": "region",
                "x-axis": ["total_cost"],
                "y-axis": [{"region": k, "total_cost": round(v, 2)} for k, v in cost_by_region.items()]
            }
        ]
    
    elif tab == "consumer_insights":
        feedback_by_region = defaultdict(list)
        customer_type_dist = defaultdict(int)
        return_rate_by_product = defaultdict(lambda: {"returned": 0, "sold": 0})
        
        for r in rows:
            region = r.get("region", "Unknown")
            feedback = r.get("customer_feedback_score", 0)
            customer_type = r.get("customer_type", "Unknown")
            product = r.get("product_name", "Unknown")
            returned = r.get("returned_units", 0)
            sold = r.get("units_sold", 0)
            
            feedback_by_region[region].append(feedback)
            customer_type_dist[customer_type] += 1
            return_rate_by_product[product]["returned"] += returned
            return_rate_by_product[product]["sold"] += sold
        
        return [
            {
                "id": "avg_feedback_by_region",
                "xKey": "region",
                "x-axis": ["avg_feedback_score"],
                "y-axis": [{"region": k, "avg_feedback_score": round(sum(v)/len(v), 2) if v else 0} for k, v in feedback_by_region.items()]
            },
            {
                "id": "customer_type_distribution",
                "xKey": "customer_type",
                "x-axis": ["count"],
                "y-axis": [{"customer_type": k, "count": v} for k, v in customer_type_dist.items()]
            },
            {
                "id": "return_rate_by_product",
                "xKey": "product_name",
                "x-axis": ["return_rate"],
                "y-axis": [{"product_name": k, "return_rate": round((v["returned"]/v["sold"])*100, 2) if v["sold"] else 0} for k, v in return_rate_by_product.items()]
            }
        ]
    
    elif tab == "sustainability_compliance":
        # Mock sustainability data since not in current schema
        return [
            {
                "id": "sustainability_score_by_region",
                "xKey": "region",
                "x-axis": ["sustainability_score"],
                "y-axis": [{"region": "North", "sustainability_score": 85}, {"region": "South", "sustainability_score": 78}, {"region": "East", "sustainability_score": 82}, {"region": "West", "sustainability_score": 79}]
            }
        ]
    
    else:
        raise HTTPException(404, "Tab not found for FMCG dashboard.")
