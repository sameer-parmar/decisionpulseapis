
from fastapi import FastAPI

from .database import  engine , Base # Assuming get_db is sync
from .routers.dataroutes import router as data_router
from app.routers.upload_data import router as upload_fmcg_data_router
Base.metadata.create_all(bind=engine)  # Use only for development/testing if not using Alembic


app = FastAPI()

app.include_router(upload_fmcg_data_router) 
app.include_router(data_router)