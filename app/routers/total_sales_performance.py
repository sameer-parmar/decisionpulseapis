from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.models.datapoints import DataPoint, Country, Category, Brand
import pandas as pd
import re

router = APIRouter()

# --- Include the parse_numeric_value helper function here or import it ---
def parse_numeric_value(value):
    # (Same function definition as before)
    if value is None: return pd.NA
    if isinstance(value, (int, float)): return float(value)
    if isinstance(value, str):
        value_str = value.strip().lower()
        multiplier = 1
        if 'billion' in value_str:
            multiplier = 1_000_000_000
            value_str = re.sub(r'billion', '', value_str).strip()
        elif 'million' in value_str:
            multiplier = 1_000_000
            value_str = re.sub(r'million', '', value_str).strip()
        elif 'k' in value_str:
            multiplier = 1_000
            value_str = re.sub(r'k', '', value_str).strip()
        try:
            numeric_part = re.match(r'^[0-9.]+', value_str)
            if numeric_part: return float(numeric_part.group(0)) * multiplier
            else: return pd.NA
        except (ValueError, TypeError): return pd.NA
    return pd.NA
# --- End Helper Function ---


@router.get("/total-sales-performance-dashboard/")
def get_total_sales_performance_dashboard(
    db: Session = Depends(get_db),
    year: str = Query(None, description="Filter by year"),
    country_name: str = Query(None, description="Filter by country name"),
    category_name: str = Query(None, description="Filter by category name"),
    brand_name: str = Query(None, description="Filter by brand name"),
):
    """
    Retrieves and processes sales performance data for dashboard display.
    (Args, Returns, Raises documentation remains the same)
    """
    # --- MODIFIED QUERY ---
    # Select the DataPoint object AND the specific name columns needed
    # Use labels to avoid name conflicts and for clarity
    query = db.query(
        DataPoint,
        Country.name.label("country_name_label"),
        Category.name.label("category_name_label"),
        Brand.name.label("brand_name_label")
    ).select_from(DataPoint) # Explicitly state the 'from' clause

    # Apply joins
    query = query.outerjoin(Country, DataPoint.country == Country.id)\
                 .outerjoin(Category, DataPoint.category == Category.id)\
                 .outerjoin(Brand, DataPoint.brand == Brand.id)

    # Apply base filters
    query = query.filter(
        DataPoint.metric_category == "financial_health",
        DataPoint.metric.ilike("%sales%"),
    )

    # Apply optional name filters on the joined tables
    if country_name:
        query = query.filter(Country.name == country_name)
    if category_name:
        query = query.filter(Category.name == category_name)
    if brand_name:
        query = query.filter(Brand.name == brand_name)

    # Apply year filter
    if year:
        if not year.isdigit() or len(year) != 4:
             raise HTTPException(status_code=400, detail="Invalid year format. Must be a 4-digit year.")
        query = query.filter(DataPoint.year == year)

    # Fetch the data - 'data' will be a list of Row objects (like tuples)
    # Each row: (DataPoint_object, country_name_value, category_name_value, brand_name_value)
    data = query.all()
    # --- END MODIFIED QUERY ---


    if not data:
        # Return default structure if no data found
        return {
            "summary": {"total_sales": 0, "average_sales": 0, "record_count": 0, "numeric_record_count": 0, "years_in_data": []},
            "sales_trend_by_year": [], "notes": ["No data found matching the specified filters."],
            "filters_applied": {"year": year, "country": country_name, "category": category_name, "brand": brand_name}
        }

    # --- MODIFIED RESULTS LIST COMPREHENSION ---
    # Access parts of the Row object correctly
    results = [
        {
            "year": item.DataPoint.year,             # Access DataPoint attributes via item.DataPoint
            "country_id": item.DataPoint.country,
            "country_name": item.country_name_label, # Access the selected label directly
            "category_name": item.category_name_label,# Access the selected label directly
            "brand_name": item.brand_name_label,     # Access the selected label directly
            "metric": item.DataPoint.metric,
            "value": item.DataPoint.value,
        }
        for item in data # Each 'item' is a RowProxy containing elements selected in the query
    ]
    # --- END MODIFIED RESULTS LIST COMPREHENSION ---

    # --- Pandas Processing (remains the same) ---
    df = pd.DataFrame(results)
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df.dropna(subset=['year'], inplace=True)
    df['year'] = df['year'].astype(int)
    df['numeric_value'] = df['value'].apply(parse_numeric_value)

    df_numeric = df.dropna(subset=['numeric_value']).copy()
    df_non_numeric = df[df['numeric_value'].isna()]

    total_sales = 0
    average_sales = 0
    sales_by_year = pd.DataFrame()

    if not df_numeric.empty:
        total_sales = df_numeric['numeric_value'].sum()
        average_sales = df_numeric['numeric_value'].mean()
        sales_by_year = df_numeric.groupby('year')['numeric_value'].sum().reset_index()
        sales_by_year.rename(columns={'numeric_value': 'total_sales'}, inplace=True)
        sales_by_year = sales_by_year.sort_values('year')

    dashboard_data = {
        "summary": {
            "total_sales": total_sales,
            "average_sales": average_sales if pd.notna(average_sales) else 0,
            "record_count": len(df),
            "numeric_record_count": len(df_numeric),
            "years_in_data": sorted(df['year'].unique().tolist()),
        },
        "sales_trend_by_year": sales_by_year.to_dict('records') if not sales_by_year.empty else [],
        "qualitative_data_points": df_non_numeric[['year', 'metric', 'value', 'country_name', 'category_name', 'brand_name']].to_dict('records'),
         "filters_applied": {
            "year": year,
            "country": country_name,
            "category": category_name,
            "brand": brand_name
        }
    }

    return dashboard_data