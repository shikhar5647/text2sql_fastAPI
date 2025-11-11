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
            
            # --- START OF FIX ---
            
            # 1. Get all *actual* table names from the schema (case-insensitive)
            all_actual_tables = {name.lower(): name for name in schema.get('tables', {}).keys()}

            # 2. Get tables suggested by NLU
            tables_from_nlu = state.get("relevant_tables", [])
            
            # 3. Find which NLU tables *actually exist* in our schema
            matched_tables = []
            if tables_from_nlu:
                for t_nlu in tables_from_nlu:
                    t_lower = t_nlu.lower().strip()

                    # Try to find a match (exact, singular, or plural)
                    if t_lower in all_actual_tables:
                        matched_tables.append(all_actual_tables[t_lower])
                    elif t_lower.endswith('s') and t_lower[:-1] in all_actual_tables:
                        matched_tables.append(all_actual_tables[t_lower[:-1]])
                    elif f"{t_lower}s" in all_actual_tables:
                        matched_tables.append(all_actual_tables[f"{t_lower}s"]) 

            # 4. If NLU did not suggest valid tables (or suggestions were incomplete),
            #    attempt token-based matching against table and column names.
            #    This helps with multi-table queries where the NLU may omit needed tables.
            token_based_tables = []
            try:
                query_text = state.get("user_query", "")
                tokens = _tokenize(query_text)

                # Build quick lookup of table -> set of tokens from its name and columns
                for tbl_name, tbl_info in schema.get('tables', {}).items():
                    tbl_tokens = set(re.findall(r"[a-z0-9_]+", tbl_name.lower()))
                    col_tokens = set()
                    for c in tbl_info.get('columns', []):
                        col_name = c.get('column_name', '') or ''
                        col_tokens.update(re.findall(r"[a-z0-9_]+", col_name.lower()))
                    # If any query token intersects table or column tokens, consider it relevant
                    if tokens & (tbl_tokens | col_tokens):
                        token_based_tables.append(tbl_name)
            except Exception:
                token_based_tables = []

            # 5. Combine matches: prefer NLU-derived matches, but union with token-based matches
            combined = list(set(matched_tables) | set(token_based_tables))
            relevant_tables = sorted(combined)

            # 6. Update the state with the *correct*, *matched* tables.
            state["relevant_tables"] = relevant_tables
            
            # --- END OF FIX ---
            
            # Build schema context (This part is now safe)
            schema_parts = []
            if not relevant_tables:
                # If list is empty, set empty context and stop.
                logger.warning(f"No relevant tables found in schema for NLU tables: {tables_from_nlu}")
                state["schema_context"] = ""
            else:
                for table_name in relevant_tables:
                    table_info = schema.get('tables', {}).get(table_name)
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