"""Secrets management for the application."""
import os
from typing import Optional
from config.settings import settings

class SecretsManager:
    """Manage application secrets securely."""
    
    @staticmethod
    def get_gemini_api_key() -> str:
        """Get Gemini API key from environment."""
        api_key = settings.GEMINI_API_KEY
        if not api_key:
            raise ValueError("GEMINI_API_KEY not found in environment variables")
        return api_key
    
    @staticmethod
    def get_database_credentials() -> dict:
        """Get database credentials."""
        return {
            "server": settings.DB_SERVER,
            "database": settings.DB_DATABASE,
            "username": settings.DB_USERNAME,
            "password": settings.DB_PASSWORD,
            "encrypt": settings.DB_ENCRYPT
        }
    
    @staticmethod
    def validate_secrets() -> bool:
        """Validate all required secrets are present."""
        try:
            SecretsManager.get_gemini_api_key()
            creds = SecretsManager.get_database_credentials()
            return all([
                creds["server"],
                creds["database"],
                creds["username"],
                creds["password"]
            ])
        except Exception:
            return False

secrets_manager = SecretsManager()