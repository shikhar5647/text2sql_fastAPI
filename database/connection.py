"""Database connection management using pymssql."""
import pymssql
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from config.settings import settings
from utils.logger import setup_logger

logger = setup_logger(__name__)

class DatabaseConnection:
    """MS SQL Server connection handler using pymssql."""
    
    def __init__(self):
        self._connection: Optional[pymssql.Connection] = None
    
    def connect(self) -> pymssql.Connection:
        """Establish database connection."""
        try:
            self._connection = pymssql.connect(
                server=settings.DB_SERVER,
                user=settings.DB_USERNAME,
                password=settings.DB_PASSWORD,
                database=settings.DB_DATABASE,
                as_dict=True,
                tds_version='7.4',
                autocommit=True
            )
            logger.info("Database connection established successfully")
            return self._connection
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def disconnect(self):
        """Close database connection."""
        if self._connection:
            self._connection.close()
            logger.info("Database connection closed")
    
    @contextmanager
    def get_cursor(self):
        """Context manager for database cursor."""
        cursor = None
        try:
            if not self._connection:
                self.connect()
            cursor = self._connection.cursor()
            yield cursor
        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute SELECT query and return results."""
        with self.get_cursor() as cursor:
            cursor.execute(query)
            # For as_dict=True, rows are already dicts keyed by column names. Some drivers may deliver
            # unnamed columns; provide a fallback mapping to stable names.
            try:
                results = cursor.fetchall()
                if results and isinstance(results[0], dict):
                    # Ensure all rows share the same keys; if any key is empty, rename it deterministically
                    keys = list(results[0].keys())
                    fixed_keys = []
                    for idx, k in enumerate(keys):
                        name = k if k and str(k).strip() != '' else f'column_{idx}'
                        fixed_keys.append(name)
                    if keys != fixed_keys:
                        normalized = []
                        for row in results:
                            new_row = {}
                            for i, original_key in enumerate(keys):
                                new_row[fixed_keys[i]] = row.get(original_key)
                            normalized.append(new_row)
                        return normalized
                    return results
                # If not dicts, build dicts from cursor.description
                desc = cursor.description or []
                columns = []
                for idx, col in enumerate(desc):
                    name = col[0] if col and col[0] else None
                    if not name or str(name).strip() == '':
                        name = f'column_{idx}'
                    columns.append(str(name))
                return [dict(zip(columns, row)) for row in results]
            except Exception as e:
                logger.error(f"Failed to fetch results: {str(e)}")
                raise
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """Get schema information for a specific table."""
        query = f"""
        SELECT 
            COLUMN_NAME as column_name,
            DATA_TYPE as data_type,
            IS_NULLABLE as is_nullable,
            CHARACTER_MAXIMUM_LENGTH as max_length
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        return self.execute_query(query)
    
    def get_all_tables(self) -> List[str]:
        """Get list of all tables in the database."""
        query = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        results = self.execute_query(query)
        return [row['TABLE_NAME'] for row in results]
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.get_cursor() as cursor:
                # Name the column to avoid drivers complaining about unnamed columns with as_dict=True
                cursor.execute("SELECT 1 AS result")
                return True
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False

# Global database connection instance
db_connection = DatabaseConnection()