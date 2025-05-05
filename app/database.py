from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.config import settings

engine = create_engine(settings.sqlalchemy_database_uri, echo=True)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Define get_db for dependency injection
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()