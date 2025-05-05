# models.py
from sqlalchemy import Column, Integer, String, Text, DateTime, Index, func, ForeignKey
# from sqlalchemy.orm import relationship # If you need relationships later
# from sqlalchemy.orm import declarative_base # Or your existing Base import

from ..database import Base
# If not, uncomment the next line:
# Base = declarative_base()

class Country(Base):
    __tablename__ = "countries"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), nullable=False, unique=True, index=True) # Added index

class Category(Base):
    __tablename__ = "categories"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), nullable=False, unique=True, index=True) # Added index

# --- NEW Brand Model ---
class Brand(Base):
    __tablename__ = "brands"
    id = Column(Integer, primary_key=True, index=True)
    # Increased length slightly, ensure unique and indexed for lookups
    name = Column(String(255), nullable=False, unique=True, index=True)

    def __repr__(self):
        return f"<Brand(id={self.id}, name='{self.name}')>"

class DataPoint(Base):
    __tablename__ = "data_points"

    id = Column(Integer, primary_key=True, index=True)
    # Foreign Keys store IDs
    country = Column(Integer, ForeignKey(Country.id), nullable=True, index=True)
    category = Column(Integer, ForeignKey(Category.id), nullable=True, index=True) # Added index
    brand = Column(Integer, ForeignKey(Brand.id), nullable=True, index=True) # MODIFIED: Now ForeignKey
    # --- End Foreign Keys ---

    source_url = Column(Text, nullable=True)
    insight = Column(Text, nullable=False)
    summary = Column(Text, nullable=True)
    year = Column(String(50), nullable=True)
    metric = Column(String(255), nullable=True)
    metric_category = Column(String(50), nullable=True, default='general', index=True)
    value = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

    # --- assign_category method remains the same ---
    def assign_category(self):
        """
        Attempts to assign a metric category based on metric, insight, or summary.
        Call this method *after* creating the DataPoint instance and *before* saving.
        """
        # ... (assign_category logic remains unchanged) ...
        metric_lower = str(self.metric).lower() if self.metric else ""
        insight_summary_lower = (str(self.insight or '') + " " + str(self.summary or '')).lower().strip()
        assigned_category = 'general' # Default

        if metric_lower:
            if any(kw in metric_lower for kw in ['sku productivity', 'top skus', 'sku proliferation', 'sku rationalization']):
                assigned_category = 'portfolio_strategy';
            elif any(kw in metric_lower for kw in ['margin', 'ebitda', 'working capital', 'revenue per employee', 'turnover', 'profit', 'finance cost', 'investment', 'funding', 'worth', 'expenses']):
                 assigned_category = 'financial_health';
            elif any(kw in metric_lower for kw in ['nps', 'net promoter', 'brand awareness', 'brand recall', 'perceived value', 'purchase intent', 'brand consciousness', 'respondents planning to buy']):
                 assigned_category = 'consumer_insights';
            elif any(kw in metric_lower for kw in ['geography', 'region', 'rural', 'urban', 'penetration by region', 'number of stores', 'number of outlets']):
                 assigned_category = 'regional_performance';
            elif any(kw in metric_lower for kw in ['fill rate', 'lead time', 'otif', 'on-time in-full', 'wastage', 'spoilage', 'capacity utilization', 'distribution network', 'stock turnover']):
                 assigned_category = 'supply_chain';
            elif any(kw in metric_lower for kw in ['share of voice', 'price index', 'competitor', 'market share']):
                 assigned_category = 'competitive_intelligence';
            elif any(kw in metric_lower for kw in ['online sales', 'e-commerce', 'digital', 'conversion rate', 'cart abandonment', 'digital shelf', 'search share', 'devices sold', 'etail share', 'gross merchandise value', 'gmv']):
                 assigned_category = 'ecommerce_digital';
            elif any(kw in metric_lower for kw in ['channel conflict', 'distributor roi', 'partner satisfaction']):
                 assigned_category = 'trade_channel';
            elif any(kw in metric_lower for kw in ['break-even', 'scenario', 'volume vs value', 'elasticity']):
                 assigned_category = 'strategic_levers';
            elif any(kw in metric_lower for kw in ['growth', 'performance', 'sales', 'revenue', 'output']):
                if 'region' in insight_summary_lower: assigned_category = 'regional_performance'
                elif 'e-commerce' in insight_summary_lower or 'online' in insight_summary_lower: assigned_category = 'ecommerce_digital'
                else: assigned_category = 'financial_health'

        elif insight_summary_lower:
             if any(kw in insight_summary_lower for kw in ['sku']): assigned_category = 'portfolio_strategy'
             elif any(kw in insight_summary_lower for kw in ['margin', 'profit', 'turnover']): assigned_category = 'financial_health'
             elif any(kw in insight_summary_lower for kw in ['nps', 'consumer']): assigned_category = 'consumer_insights'
             elif any(kw in insight_summary_lower for kw in ['region', 'store']): assigned_category = 'regional_performance'
             elif any(kw in insight_summary_lower for kw in ['supply', 'distribution', 'logistics']): assigned_category = 'supply_chain'
             elif any(kw in insight_summary_lower for kw in ['competitor', 'market share']): assigned_category = 'competitive_intelligence'
             elif any(kw in insight_summary_lower for kw in ['online', 'e-commerce', 'digital', 'gmv']): assigned_category = 'ecommerce_digital'

        self.metric_category = assigned_category

    @classmethod
    def parse_csv_row(cls, row: dict, expected_headers: set, required_headers: set) -> dict | None:
        """
        Parses and validates data from a CSV row dictionary.

        Returns:
            A dictionary containing the cleaned data if valid, otherwise None.
            Includes 'country_name', 'category_name', and 'brand_name' as strings.
        """
        normalized_row = {str(k).strip().lower(): str(v).strip() for k, v in row.items() if k is not None}
        parsed_data = {}
        missing_required = []
        available_headers = set(normalized_row.keys())
        required_headers_lower = {h.lower() for h in required_headers}

        for header_lower in required_headers_lower:
             csv_header_variants = [h for h in available_headers if h == header_lower]
             if not csv_header_variants:
                 original_case_header = next((h for h in required_headers if h.lower() == header_lower), header_lower)
                 missing_required.append(original_case_header)
                 continue

             value = normalized_row.get(csv_header_variants[0])
             if value is None or value == "":
                 original_case_header = next((h for h in required_headers if h.lower() == header_lower), header_lower)
                 missing_required.append(f"{original_case_header} (empty)")

        if missing_required:
            print(f"Background task Warning: Skipping row due to missing/empty required fields: {missing_required} in row: {row}")
            return None

        # Extract data using lowercase keys, map back to desired keys
        parsed_data['country_name'] = normalized_row.get('country', '')
        parsed_data['category_name'] = normalized_row.get('category', '')
        parsed_data['brand_name'] = normalized_row.get('brand', '') # MODIFIED: Changed key name
        parsed_data['year'] = normalized_row.get('year', '')
        parsed_data['metric'] = normalized_row.get('metric', '')
        parsed_data['value'] = normalized_row.get('value', '')
        parsed_data['source_url'] = normalized_row.get('source url', '') # Handle space
        parsed_data['summary'] = normalized_row.get('summary', None)
        parsed_data['insight'] = normalized_row.get('insight', '')

        # Optional: Add specific validation if needed (e.g., year format)
        # ...

        return parsed_data

    def __repr__(self):
        # MODIFIED: Updated repr to show FK IDs
        return (f"<DataPoint(id={self.id}, metric='{self.metric}', year='{self.year}', "
                f"country_id={self.country}, category_id={self.category}, brand_id={self.brand})>")

# Optional: Add relationships if needed for querying
# Country.data_points = relationship("DataPoint", back_populates="country_rel")
# Category.data_points = relationship("DataPoint", back_populates="category_rel")
# Brand.data_points = relationship("DataPoint", back_populates="brand_rel")
# DataPoint.country_rel = relationship("Country", back_populates="data_points")
# DataPoint.category_rel = relationship("Category", back_populates="data_points")
# DataPoint.brand_rel = relationship("Brand", back_populates="data_points") 