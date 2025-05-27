
from fastapi import FastAPI

from .database import  engine , Base # Assuming get_db is sync
from app.routers.upload_data import router as upload_data_router
from app.routers.shared_dashboard import router as shared_dashboard_router
Base.metadata.create_all(bind=engine)  # Use only for development/testing if not using Alembic
from app.routers.fmcgrouters import router as fmcg_router

app = FastAPI()


app.include_router(shared_dashboard_router, prefix="/shared-dashboard", tags=["Shared Dashboard"])
app.include_router(upload_data_router, prefix="/upload-data", tags=["Upload Data"])