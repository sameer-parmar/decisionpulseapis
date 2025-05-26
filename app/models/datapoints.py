from sqlalchemy import UUID, Column, String, Float, DateTime, BigInteger, Integer, ForeignKey, Text
from app.database import Base
import uuid

class Dashboard(Base):
    __tablename__ = 'dashboards'
    id = Column(String(36), primary_key=True, default=str(uuid.uuid4()))
    name = Column(String(255), unique=True, nullable=False)
    description = Column(String, nullable=True)
# NEW MODEL - Add this to your existing models
class DashboardTab(Base):
    __tablename__ = 'dashboard_tabs'
    
    id = Column(String(36), primary_key=True, default=str(uuid.uuid4()))
    dashboard_id = Column(String(36), ForeignKey('dashboards.id'), nullable=False)
    tab_name = Column(String(255), nullable=False)
    tab_data = Column(Text, nullable=True)  # Will store JSON string
    created_at = Column(DateTime, nullable=True)
class AutoMobileData(Base):
    __tablename__ = 'auto_mobile_data'
    __table_args__ = {"extend_existing": True}

    # use the existing invoice_id as primary key
    invoice_id                   = Column(String,     primary_key=True)
    booking_date                 = Column(DateTime,   nullable=True)
    delivery_date                = Column(DateTime,   nullable=True)
    sale_date                    = Column(DateTime,   nullable=True)
    oem_name                     = Column(String,     nullable=True)
    dealer_name                  = Column(String,     nullable=True)
    region                       = Column(String,     nullable=True)
    country                      = Column(String,     nullable=True)
    state                        = Column(String,     nullable=True)
    city                         = Column(String,     nullable=True)
    vehicle_segment              = Column(String,     nullable=True)
    vehicle_model                = Column(String,     nullable=True)
    variant                      = Column(String,     nullable=True)
    year                         = Column(BigInteger, nullable=True)
    fuel_type                    = Column(String,     nullable=True)
    transmission_type            = Column(String,     nullable=True)
    engine_displacement_cc       = Column(BigInteger, nullable=True)
    color                        = Column(String,     nullable=True)
    type_of_fuel_used_postsale   = Column(String,     nullable=True)
    range_km                     = Column(Float,      nullable=True)
    battery_capacity_kwh         = Column(Float,      nullable=True)
    charging_time_hours          = Column(Float,      nullable=True)
    competitor_model_name        = Column(String,     nullable=True)
    competitor_oem               = Column(String,     nullable=True)
    competitor_price             = Column(BigInteger, nullable=True)

    # map to the actual DB columns that end with an underscore:
    market_share_in_region       = Column( Float,    nullable=True)
    salesperson_name             = Column(String,     nullable=True)
    units_sold                   = Column(BigInteger, nullable=True)
    unit_price                   = Column( BigInteger, nullable=True)
    discount_offered             = Column( BigInteger, nullable=True)
    final_price_after_discount   = Column( Float,   nullable=True)

    customer_type                = Column(String,     nullable=True)
    finance_opted_yesno          = Column(String,     nullable=True)
    financing_partner            = Column(String,     nullable=True)
    exchange_vehicle_offered     = Column(String,     nullable=True)
    lead_source                  = Column(String,     nullable=True)
    promotion_scheme_applied     = Column(String,     nullable=True)
    accessories_bundle           = Column(String,     nullable=True)
    free_services_offered        = Column(BigInteger, nullable=True)
    nps_customer_feedback        = Column(BigInteger, nullable=True)
    complaint_registered_yn      = Column(String,     nullable=True)
    delivery_rating_15           = Column(BigInteger, nullable=True)
    dashboard_id                 = Column(String(36), ForeignKey('dashboards.id'), nullable=True)
