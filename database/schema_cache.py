"""Schema caching mechanism."""
import json
import time
from typing import Dict, List, Any, Optional
from pathlib import Path
from config.settings import settings
from database.connection import db_connection
from utils.logger import setup_logger
import pandas as pd

logger = setup_logger(__name__)

class SchemaCache:
    """Cache database schema information."""
    
    def __init__(self, cache_file: str = "schema_cache.json"):
        self.cache_file = settings.PROJECT_ROOT / cache_file
        self.cache: Dict[str, Any] = {}
        self.load_cache()
    
    def load_cache(self):
        """Load cache from file."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r') as f:
                    self.cache = json.load(f)
                logger.info("Schema cache loaded successfully")
            except Exception as e:
                logger.warning(f"Failed to load cache: {str(e)}")
                self.cache = {}
    
    def save_cache(self):
        """Save cache to file."""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, indent=2, default=str)
            logger.info("Schema cache saved successfully")
        except Exception as e:
            logger.error(f"Failed to save cache: {str(e)}")
    
    def is_cache_valid(self) -> bool:
        """Check if cache is still valid."""
        if not self.cache or 'timestamp' not in self.cache:
            return False
        
        cache_age = time.time() - self.cache['timestamp']
        return cache_age < settings.CACHE_TTL
    
    def refresh_schema(self) -> Dict[str, Any]:
        """Refresh schema information from database."""
        logger.info("Refreshing schema from database...")
        
        schema = {
            'timestamp': time.time(),
            'tables': {}
        }
        
        try:
            tables = db_connection.get_all_tables()
            
            for table in tables:
                columns = db_connection.get_table_schema(table)
                schema['tables'][table] = {
                    'columns': columns,
                    'column_names': [col['column_name'] for col in columns]
                }
            
            self.cache = schema
            self.save_cache()
            logger.info(f"Schema refreshed successfully. Found {len(tables)} tables.")
            
        except Exception as e:
            logger.error(f"Failed to refresh schema: {str(e)}")
            raise
        
        return schema

    def load_schema_from_excel(self, excel_path: Optional[Path] = None) -> Dict[str, Any]:
        """Load schema details from an Excel file and store to cache.

        Expected layout: a sheet per table or a single sheet with columns
        [table_name, column_name, data_type, is_nullable].
        """
        path = excel_path or (settings.PROJECT_ROOT / 'trimstone_final.xlsx')
        logger.info(f"Loading schema from Excel: {path}")
        schema = {
            'timestamp': time.time(),
            'tables': {}
        }
        try:
            xls = pd.ExcelFile(path)
            if len(xls.sheet_names) == 1:
                df = pd.read_excel(xls, xls.sheet_names[0])
                required = {"table_name", "column_name", "data_type", "is_nullable"}
                missing = required - {c.lower() for c in df.columns}
                if missing:
                    raise ValueError(f"Excel missing required columns: {missing}")
                # Normalize columns
                df.columns = [c.lower() for c in df.columns]
                for table_name, group in df.groupby('table_name'):
                    columns = []
                    for _, row in group.iterrows():
                        columns.append({
                            'column_name': str(row['column_name']),
                            'data_type': str(row['data_type']),
                            'is_nullable': 'YES' if str(row['is_nullable']).strip().upper() in ['YES', 'Y', 'TRUE', '1'] else 'NO',
                        })
                    schema['tables'][str(table_name)] = {
                        'columns': columns,
                        'column_names': [c['column_name'] for c in columns]
                    }
            else:
                # Sheet per table
                for sheet in xls.sheet_names:
                    df = pd.read_excel(xls, sheet)
                    lower_cols = [c.lower() for c in df.columns]
                    mapping = {name: idx for idx, name in enumerate(lower_cols)}
                    def get(col):
                        return df.iloc[:, mapping[col]] if col in mapping else None
                    col_name_series = get('column_name') or get('column') or get('name')
                    data_type_series = get('data_type') or get('type')
                    is_nullable_series = get('is_nullable') or get('nullable')
                    if col_name_series is None or data_type_series is None:
                        logger.warning(f"Sheet {sheet} missing required columns; skipping")
                        continue
                    columns = []
                    for i in range(len(col_name_series)):
                        is_nullable_val = 'YES'
                        if is_nullable_series is not None:
                            v = str(is_nullable_series.iloc[i]).strip().upper()
                            is_nullable_val = 'YES' if v in ['YES', 'Y', 'TRUE', '1'] else 'NO'
                        columns.append({
                            'column_name': str(col_name_series.iloc[i]),
                            'data_type': str(data_type_series.iloc[i]),
                            'is_nullable': is_nullable_val,
                        })
                    schema['tables'][sheet] = {
                        'columns': columns,
                        'column_names': [c['column_name'] for c in columns]
                    }
            self.cache = schema
            self.save_cache()
            logger.info(f"Loaded schema from Excel. Found {len(schema['tables'])} tables.")
            return schema
        except Exception as e:
            logger.error(f"Failed to load schema from Excel: {str(e)}")
            raise
    
    def get_schema(self, force_refresh: bool = False) -> Dict[str, Any]:
        """Get schema (from cache or refresh)."""
        if force_refresh or not self.is_cache_valid():
            return self.refresh_schema()
        return self.cache
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific table."""
        schema = self.get_schema()
        return schema.get('tables', {}).get(table_name)
    
    def get_schema_as_text(self) -> str:
        """Get schema as formatted text for LLM."""
        schema = self.get_schema()
        text_parts = []
        
        for table_name, table_info in schema.get('tables', {}).items():
            text_parts.append(f"\nTable: {table_name}")
            text_parts.append("Columns:")
            for col in table_info['columns']:
                nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                text_parts.append(
                    f"  - {col['column_name']} ({col['data_type']}) {nullable}"
                )
        
        return "\n".join(text_parts)

    def load_manual_schema(self) -> Dict[str, Any]:
        """Load a predefined manual schema for client, contacts, and project tables."""
        logger.info("Loading manual schema definition")

        # If an on-disk schema cache exists, prefer using it so the manual loader
        # stays in sync with `schema_cache.json`. This allows quick updates by
        # editing the JSON file without changing source code.
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                # basic validation
                if isinstance(loaded, dict) and 'tables' in loaded:
                    self.cache = loaded
                    logger.info("Loaded schema from on-disk cache via load_manual_schema()")
                    return self.cache
        except Exception as e:
            logger.warning(f"Failed to load on-disk schema in load_manual_schema(): {e}")

        # Fallback: embedded manual schema (kept for compatibility when JSON is absent)
        def build_columns(pairs: list[tuple[str, str]]) -> list[dict[str, str]]:
            return [
                {"column_name": name, "data_type": dtype, "is_nullable": "YES"}
                for name, dtype in pairs
            ]

        manual = {
            'timestamp': time.time(),
            'tables': {
                'client': {
                    'columns': build_columns([
                        ("id", "INT"),
                        ("name", "NVARCHAR(255)"),
                        ("slug", "NVARCHAR(255)"),
                        ("email", "NVARCHAR(255)"),
                        ("phone", "NVARCHAR(50)"),
                        ("address1", "NVARCHAR(255)"),
                        ("address2", "NVARCHAR(255)"),
                        ("city", "NVARCHAR(100)"),
                        ("state", "NVARCHAR(100)"),
                        ("country", "NVARCHAR(100)"),
                        ("zipcode", "NVARCHAR(20)"),
                        ("created_at", "DATETIME2"),
                        ("updated_at", "DATETIME2"),
                        ("team_id", "INT"),
                        ("channel_id", "INT"),
                        ("drive_id", "NVARCHAR(255)"),
                        ("delta_token", "NVARCHAR(255)"),
                        ("drive_item_id", "NVARCHAR(255)"),
                        ("hubspot_id", "NVARCHAR(255)"),
                        ("owner_name", "NVARCHAR(255)"),
                        ("status", "NVARCHAR(50)"),
                        ("industry", "NVARCHAR(100)"),
                        ("type", "NVARCHAR(50)"),
                        ("no_employees", "INT"),
                        ("description", "NVARCHAR(MAX)"),
                        ("timezone", "NVARCHAR(100)"),
                        ("created_by", "NVARCHAR(255)"),
                        ("client_number", "NVARCHAR(100)")
                    ]),
                    'column_names': [
                        "id","name","slug","email","phone","address1","address2","city","state","country","zipcode","created_at","updated_at","team_id","channel_id","drive_id","delta_token","drive_item_id","hubspot_id","owner_name","status","industry","type","no_employees","description","timezone","created_by","client_number"
                    ]
                },
                # Keep other embedded tables as before (contacts, project, client_team, etc.)
                'contacts': {
                    'columns': build_columns([
                        ("id", "INT"),
                        ("first_name", "NVARCHAR(100)"),
                        ("last_name", "NVARCHAR(100)"),
                        ("email", "NVARCHAR(255)"),
                        ("owner", "NVARCHAR(255)"),
                        ("phone", "NVARCHAR(50)"),
                        ("mobile", "NVARCHAR(50)"),
                        ("stage", "NVARCHAR(50)"),
                        ("client_id", "INT"),
                        ("client_name", "NVARCHAR(255)"),
                        ("hubspot_id", "NVARCHAR(255)"),
                        ("created_at", "DATETIME2"),
                        ("updated_at", "DATETIME2")
                    ]),
                    'column_names': [
                        "id","first_name","last_name","email","owner","phone","mobile","stage","client_id","client_name","hubspot_id","created_at","updated_at"
                    ]
                },
                'project': {
                    'columns': build_columns([
                        ("id", "INT"),
                        ("name", "NVARCHAR(255)"),
                        ("client_id", "INT"),
                        ("description", "NVARCHAR(MAX)"),
                        ("slug", "NVARCHAR(255)"),
                        ("category", "NVARCHAR(100)"),
                        ("status", "NVARCHAR(50)"),
                        ("priority", "NVARCHAR(50)"),
                        ("start_date", "DATE"),
                        ("end_date", "DATE"),
                        ("currency", "NVARCHAR(10)"),
                        ("budget", "DECIMAL(18,2)"),
                        ("created_by", "NVARCHAR(255)"),
                        ("updated_by", "NVARCHAR(255)"),
                        ("created_at", "DATETIME2"),
                        ("updated_at", "DATETIME2"),
                        ("billing_type", "NVARCHAR(50)"),
                        ("amount_billed", "DECIMAL(18,2)"),
                        ("budget_hours", "DECIMAL(18,2)"),
                        ("team_id", "INT"),
                        ("channel_id", "INT"),
                        ("drive_id", "NVARCHAR(255)"),
                        ("drive_subscription_id", "NVARCHAR(255)"),
                        ("delta_token", "NVARCHAR(255)"),
                        ("drive_item_id", "NVARCHAR(255)"),
                        ("hubspot_id", "NVARCHAR(255)"),
                        ("xero_id", "NVARCHAR(255)"),
                        ("owner_id", "INT"),
                        ("owner_email", "NVARCHAR(255)"),
                        ("last_modified_date", "DATE"),
                        ("project_number", "NVARCHAR(100)")
                    ]),
                    'column_names': [
                        "id","name","client_id","description","slug","category","status","priority","start_date","end_date","currency","budget","created_by","updated_by","created_at","updated_at","billing_type","amount_billed","budget_hours","team_id","channel_id","drive_id","drive_subscription_id","delta_token","drive_item_id","hubspot_id","xero_id","owner_id","owner_email","last_modified_date","project_number"
                    ]
                },
                'client_team': {
                    'columns': build_columns([
                        ("id", "INT"),
                        ("client_id", "INT"),
                        ("user_id", "INT"),
                        ("role", "NVARCHAR(100)"),
                        ("created_at", "DATETIME2"),
                        ("updated_at", "DATETIME2"),
                        ("team_member_id", "INT"),
                        ("type", "NVARCHAR(50)"),
                        ("contracted_hours", "DECIMAL(10,2)"),
                        ("shortcut_id", "NVARCHAR(255)")
                    ]),
                    'column_names': [
                        "id","client_id","user_id","role","created_at","updated_at","team_member_id","type","contracted_hours","shortcut_id"
                    ]
                },
                'project_team': {
                    'columns': build_columns([
                        ("id", "INT"),
                        ("type", "NVARCHAR(50)"),
                        ("ref_id", "INT"),
                        ("title", "NVARCHAR(255)"),
                        ("updated_by", "INT"),
                        ("updated_at", "DATETIME2"),
                        ("description", "NVARCHAR(MAX)"),
                        ("project_id", "INT"),
                        ("file_type", "NVARCHAR(50)"),
                        ("file_url", "NVARCHAR(1024)"),
                        ("change_type", "NVARCHAR(50)")
                    ]),
                    'column_names': [
                        "id","type","ref_id","title","updated_by","updated_at","description","project_id","file_type","file_url","change_type"
                    ]
                },
                'role': {
                    'columns': build_columns([
                        ("id", "INT"),
                        ("name", "NVARCHAR(100)"),
                        ("created_at", "DATETIME2"),
                        ("updated_at", "DATETIME2")
                    ]),
                    'column_names': ["id","name","created_at","updated_at"]
                },
                'project_role': {
                    'columns': build_columns([
                        ("id", "INT"),
                        ("name", "NVARCHAR(255)"),
                        ("created_at", "DATETIME2"),
                        ("updated_at", "DATETIME2")
                    ]),
                    'column_names': ["id", "name", "created_at", "updated_at"]
                },
                'user': {
                    'columns': build_columns([
                        ("id", "INT"),
                        ("first_name", "NVARCHAR(100)"),
                        ("last_name", "NVARCHAR(100)"),
                        ("email", "NVARCHAR(255)"),
                        ("avatar", "NVARCHAR(1024)"),
                        ("azure_ad_id", "NVARCHAR(255)"),
                        ("created_at", "DATETIME2"),
                        ("updated_at", "DATETIME2"),
                        ("groups", "NVARCHAR(MAX)"),
                        ("employment_type", "NVARCHAR(100)"),
                        ("contracted_hour", "DECIMAL(10,2)"),
                        ("role", "NVARCHAR(100)"),
                        ("drive_delta_token", "NVARCHAR(255)")
                    ]),
                    'column_names': [
                        "id","first_name","last_name","email","avatar","azure_ad_id","created_at","updated_at","groups","employment_type","contracted_hour","role","drive_delta_token"
                    ]
                },
                'xero_contacts': {
                    'columns': build_columns([
                        ("name", "NVARCHAR(255)"),
                        ("contact_id", "NVARCHAR(255)"),
                        ("contact_number", "NVARCHAR(255)"),
                        ("account_number", "NVARCHAR(255)"),
                        ("contact_status", "NVARCHAR(50)"),
                        ("first_name", "NVARCHAR(100)"),
                        ("last_name", "NVARCHAR(100)"),
                        ("email_address", "NVARCHAR(255)"),
                        ("skype_user_name", "NVARCHAR(255)"),
                        ("contact_persons", "JSON"),
                        ("bank_account_details", "NVARCHAR(255)"),
                        ("tax_number", "NVARCHAR(255)"),
                        ("accounts_receivable_tax_type", "NVARCHAR(100)"),
                        ("accounts_payable_tax_type", "NVARCHAR(100)"),
                        ("addresses", "JSON"),
                        ("phones", "JSON"),
                        ("is_supplier", "BIT"),
                        ("is_customer", "BIT"),
                        ("default_currency", "NVARCHAR(10)"),
                        ("xero_network_key", "NVARCHAR(255)"),
                        ("sales_default_account_code", "NVARCHAR(100)"),
                        ("purchases_default_account_code", "NVARCHAR(100)"),
                        ("sales_tracking_categories", "JSON"),
                        ("purchases_tracking_categories", "JSON"),
                        ("tracking_category_name", "NVARCHAR(255)"),
                        ("tracking_option_name", "NVARCHAR(255)"),
                        ("payment_terms", "JSON")
                    ]),
                    'column_names': [
                        "name","contact_id","contact_number","account_number","contact_status","first_name","last_name","email_address","skype_user_name","contact_persons","bank_account_details","tax_number","accounts_receivable_tax_type","accounts_payable_tax_type","addresses","phones","is_supplier","is_customer","default_currency","xero_network_key","sales_default_account_code","purchases_default_account_code","sales_tracking_categories","purchases_tracking_categories","tracking_category_name","tracking_option_name","payment_terms"
                    ]
                },
                'xero_projects': {
                    'columns': build_columns([
                        ("contact_id", "NVARCHAR(255)"),
                        ("when_upserted_into_data_store", "DATETIME2"),
                        ("mirror_remote_id", "NVARCHAR(255)"),
                        ("is_deleted", "BIT"),
                        ("when_created", "DATETIME2"),
                        ("when_modified", "DATETIME2"),
                        ("name", "NVARCHAR(255)"),
                        ("project_id", "NVARCHAR(255)"),
                        ("currency_code", "NVARCHAR(10)"),
                        ("minutes_logged", "INT"),
                        ("minutes_to_be_invoiced", "INT"),
                        ("deadline_utc", "DATETIME2"),
                        ("status", "NVARCHAR(50)"),
                        ("total_task_amount_currency", "NVARCHAR(10)"),
                        ("total_task_amount_value", "DECIMAL(18,2)"),
                        ("total_expense_amount_currency", "NVARCHAR(10)"),
                        ("total_expense_amount_value", "DECIMAL(18,2)"),
                        ("task_amount_to_be_invoiced_currency", "NVARCHAR(10)"),
                        ("task_amount_to_be_invoiced_value", "DECIMAL(18,2)"),
                        ("task_amount_invoiced_currency", "NVARCHAR(10)"),
                        ("task_amount_invoiced_value", "DECIMAL(18,2)"),
                        ("expense_amount_to_be_invoiced_currency", "NVARCHAR(10)"),
                        ("expense_amount_to_be_invoiced_value", "DECIMAL(18,2)"),
                        ("expense_amount_invoiced_currency", "NVARCHAR(10)"),
                        ("expense_amount_invoiced_value", "DECIMAL(18,2)"),
                        ("project_amount_invoiced_currency", "NVARCHAR(10)"),
                        ("project_amount_invoiced_value", "DECIMAL(18,2)"),
                        ("total_to_be_invoiced_currency", "NVARCHAR(10)"),
                        ("total_to_be_invoiced_value", "DECIMAL(18,2)"),
                        ("estimate_currency", "NVARCHAR(10)"),
                        ("estimate_value", "DECIMAL(18,2)"),
                        ("id", "NVARCHAR(255)")
                    ]),
                    'column_names': [
                        "contact_id","when_upserted_into_data_store","mirror_remote_id","is_deleted","when_created","when_modified","name","project_id","currency_code","minutes_logged","minutes_to_be_invoiced","deadline_utc","status","total_task_amount_currency","total_task_amount_value","total_expense_amount_currency","total_expense_amount_value","task_amount_to_be_invoiced_currency","task_amount_to_be_invoiced_value","task_amount_invoiced_currency","task_amount_invoiced_value","expense_amount_to_be_invoiced_currency","expense_amount_to_be_invoiced_value","expense_amount_invoiced_currency","expense_amount_invoiced_value","project_amount_invoiced_currency","project_amount_invoiced_value","total_to_be_invoiced_currency","total_to_be_invoiced_value","estimate_currency","estimate_value","id"
                    ]
                }
            }
        }
        self.cache = manual
        self.save_cache()
        logger.info("Manual schema loaded successfully (embedded fallback)")
        return manual

# Global schema cache instance
schema_cache = SchemaCache()