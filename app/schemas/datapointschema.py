from uuid import UUID  # ✅ For SQL Server
from pydantic import BaseModel,Field
from typing import Optional,List, Dict, Any
class DataPoint(BaseModel):
    country: UUID
    category: UUID
    brand: UUID
    year: int
    metric: str
    value: float
    unit: str
    source_url: Optional[str] = None
    summary: Optional[str] = None
    insight: Optional[str] = None

class CategoryCountryLink(BaseModel):
    category_id: UUID
    country_id: UUID


class CategoryCountryList(BaseModel):
    items: List[CategoryCountryLink]
class CategoryWithCountry(BaseModel):
    category_id: UUID
    category_name: str
    country_id: Optional[UUID]
    country_name: Optional[str]

    class Config:
        orm_mode = True
class CountrySchema(BaseModel):
    id: UUID
    name: str

    class Config:
        from_attributes = True 

class DataPointFilter(BaseModel):
    category: Optional[UUID] = None # Mandatory category filter
    country: Optional[UUID] = None  # Optional country filter
    brand: Optional[UUID] = None  # Optional brand filter


class BrandFilterRequest(BaseModel):
    brands: Optional[List[str]] = Field(
        None, description="List of brand names to include"
    )
    metrics: Optional[List[str]] = Field(
        None, description="List of metric names to include"
    )
    metric_categories: Optional[List[str]] = Field(
        None, description="List of metric categories to include"
    )
    years: Optional[List[str]] = Field(
        None, description="List of years (or quarters) to include"
    )
    categories: Optional[List[str]] = Field(
        None, description="List of DataPoint.category names to include"
    )
    countries: Optional[List[str]] = Field(
        None, description="List of country names to include"
    )
    limit: int = Field(100, ge=1, le=1000, description="Max rows to return")


class CompareBrandsRequest(BaseModel):
    metric: str = Field(..., description="Metric to compare")
    brands: Optional[List[str]] = Field(
        None, description="Brands to include in comparison"
    )
    years: Optional[List[str]] = Field(
        None, description="Years to include"
    )
    country: Optional[str] = Field(
        None, description="Country name to filter by"
    )
    limit: int = Field(1000, ge=1, le=5000, description="Max rows to scan")
class Dataset(BaseModel):
    label: str
    data: List[Optional[float]]
    unit: Optional[str]


class ChartDataResponse(BaseModel):
    success: bool = True
    labels: List[str]
    datasets: List[Dataset]


class CompareResponse(BaseModel):
    success: bool = True
    metric: str
    unit: Optional[str]
    labels: List[str]
    datasets: List[Dataset]


class CompareBrandsRequest(BaseModel):
    metric: str
    brands: Optional[List[str]] = None
    years: Optional[List[str]] = None
    country: Optional[str] = None

class CategoryFilterRequest(BaseModel):
    category: Optional[str] = None
class MetricsResponse(BaseModel):
    brand: str
    data: List[Dict[str, Any]]

class BrandMetricsResponse(BaseModel):
    success: bool = True
    data: List[MetricsResponse]       

class Filters(BaseModel):
    metrics:            List[str]
    metric_categories:  List[str]
    brands:             List[str]
    years:              List[str]
    categories:         List[str]
    countries:          List[str]

class AvailableResponse(BaseModel):
    success: bool = True
    filters: Filters    

class ChartSeries(BaseModel):
    data: List[Any]
    categories: List[str]

class ChartItem(BaseModel):
    title: str
    type: str # "line", "bar", "metric"
    unit: Optional[str]
    series: ChartSeries

class MetricSummary(BaseModel):
    metric_name: str
    unit: Optional[str]
    charts: List[ChartItem]

class ResponseMetadata(BaseModel):
    metric_category_path: str
    # Make selected_category_name optional here as it might not be initially provided
    selected_category_name: Optional[str] = None
    brand_name: Optional[str] = None
    country_name: Optional[str] = None
    year: Optional[str] = None
    metric_name: Optional[str] = None
    unit: Optional[str] = None
    # You can add all applied filters here for clarity in the response

# --- NEW Pydantic Models for Filter Options ---

class FilterOptions(BaseModel):
    categories: List[str] = Field(default_factory=list, description="Available main category names.")
    brands: List[str] = Field(default_factory=list, description="Available brand names.")
    countries: List[str] = Field(default_factory=list, description="Available country names.")
    years: List[str] = Field(default_factory=list, description="Available years.")
    metric_names: List[str] = Field(default_factory=list, description="Available metric names.")
    units: List[str] = Field(default_factory=list, description="Available units of measurement.")

class UniversalMetricCategorySummaryResponse(BaseModel):
    metadata: ResponseMetadata
    filters: FilterOptions
    metrics: List[MetricSummary]