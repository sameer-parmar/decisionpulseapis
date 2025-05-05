from pydantic_settings import BaseSettings
from urllib.parse import quote_plus

class Settings(BaseSettings):
    DB_SERVER: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

    class Config:
        env_file = ".env"
        extra = "ignore"  # Ignore extra fields in the .env file

    @property
    def sqlalchemy_database_uri(self):
        encoded_password = quote_plus(self.DB_PASSWORD)
        return (
            f"mssql+pyodbc://{self.DB_USER}:{encoded_password}@{self.DB_SERVER}:{self.DB_PORT}/"
            f"{self.DB_NAME}?driver=ODBC+Driver+18+for+SQL+Server"
            f"&encrypt=yes&trustservercertificate=no&connection+timeout=30"
        )

settings = Settings()
