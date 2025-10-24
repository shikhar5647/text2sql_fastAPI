"""Schema Agent for retrieving relevant schema information."""
import re
from typing import Dict, Any, List
from graph.state import GraphState
from database.schema_cache import schema_cache
from utils.logger import setup_logger

logger = setup_logger(__name__)

def _tokenize(text: str) -> set[str]:
    text = (text or "").lower()
    tokens = set(re.findall(r"[a-z0-9_]+", text))
    stopwords = {"show", "list", "get", "find", "all", "the", "me", "for", "with", "in", "of", "and", "top"}
    return tokens - stopwords

class SchemaAgent:
    """Schema Introspection and Retrieval Agent."""
    
    def get_relevant_schema(self, state: GraphState) -> GraphState:
        """Get schema information relevant to the query."""
        logger.info("Retrieving relevant schema information")
        
        try:
            # Get full schema
            schema = schema_cache.get_schema()
            
            # Filter to relevant tables; normalize simple plurals to match cache keys
            relevant_tables = state.get("relevant_tables", [])
            normalized = []
            for t in relevant_tables:
                key = t.strip()
                if key.lower().endswith('s'):
                    key = key[:-1]
                normalized.append(key)
            relevant_tables = normalized or list(schema.get('tables', {}).keys())
            
            if not relevant_tables:
                # If no tables identified, include all
                relevant_tables = list(schema.get('tables', {}).keys())
            
            # Build schema context
            schema_parts = []
            for table_name in relevant_tables:
                table_info = schema.get('tables', {}).get(table_name) or schema.get('tables', {}).get(table_name.lower()) or schema.get('tables', {}).get(table_name.capitalize())
                if table_info:
                    schema_parts.append(f"\n### Table: {table_name}")
                    schema_parts.append("Columns:")
                    for col in table_info['columns']:
                        nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                        schema_parts.append(
                            f"  - {col['column_name']} ({col['data_type']}) {nullable}"
                        )
            
            state["schema_context"] = "\n".join(schema_parts)
            state["step"] = "schema_retrieved"
            
            logger.info(f"Schema context built for tables: {relevant_tables}")
            
        except Exception as e:
            logger.error(f"Schema retrieval error: {str(e)}")
            state["error"] = f"Schema retrieval failed: {str(e)}"
            state["step"] = "error"
        
        return state

schema_agent = SchemaAgent()