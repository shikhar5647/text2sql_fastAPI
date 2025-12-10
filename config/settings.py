"""Application configuration settings."""
import os
from pathlib import Path
from dotenv import load_dotenv

# Project Root
PROJECT_ROOT_COMPUTED: Path = Path(__file__).resolve().parent.parent

# Load environment variables from project root .env explicitly
load_dotenv(PROJECT_ROOT_COMPUTED / ".env")

class Settings:
    """Application settings."""
    
    # Gemini Configuration
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY")
    GEMINI_MODEL: str = os.getenv("GEMINI_MODEL")
    
    # Database Configuration (pymssql format)
    DB_SERVER: str = os.getenv("DB_SERVER")
    DB_DATABASE: str = os.getenv("DB_DATABASE")
    DB_USERNAME: str = os.getenv("DB_USERNAME")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD")
    DB_ENCRYPT: bool = os.getenv("DB_ENCRYPT").lower() == "true"
    
    # Application Settings
    LOG_LEVEL: str = os.getenv("LOG_LEVEL")
    CACHE_TTL: int = int(os.getenv("CACHE_TTL"))
    
    # Project Root
    PROJECT_ROOT: Path = PROJECT_ROOT_COMPUTED
    
    @property
    def database_url(self) -> str:
        """Generate database connection string."""
        return (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.DB_SERVER};"
            f"DATABASE={self.DB_DATABASE};"
            f"UID={self.DB_USERNAME};"
            f"PWD={self.DB_PASSWORD};"
            f"Encrypt={'yes' if self.DB_ENCRYPT else 'no'};"
            f"TrustServerCertificate=yes;"
        )

settings = Settings()