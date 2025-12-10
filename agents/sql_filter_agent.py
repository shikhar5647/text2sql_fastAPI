"""SQL Filter Agent for applying user-based access control to generated SQL."""
import re
from typing import Dict, Any, Optional
from graph.state import GraphState
from database.user_auth_service import user_auth_service
from utils.logger import setup_logger

logger = setup_logger(__name__)


class SQLFilterAgent:
    """Agent that applies user-based access control filters to SQL queries."""
    
    def apply_user_filters(self, state: GraphState) -> GraphState:
        """
        Apply user-based project access filters to the generated SQL.
        
        For admin users: No filtering applied
        For non-admin users: Inject project ID filters into WHERE clause
        
        Args:
            state: Current graph state with generated_sql and user_email
            
        Returns:
            Updated state with filtered SQL (or original if admin)
        """
        user_email = state.get("user_email")
        generated_sql = state.get("generated_sql")
        
        if not user_email:
            logger.warning("No user_email in state - skipping access control")
            state["validation_message"] = "User email is required for access control"
            state["is_valid"] = False
            return state
        
        if not generated_sql:
            logger.warning("No generated_sql in state - skipping filter")
            return state
        
        # Get user authorization info
        auth_info = user_auth_service.get_user_authorization(user_email)
        
        if auth_info.get("error"):
            logger.error(f"Authorization error: {auth_info['error']}")
            state["validation_message"] = f"Authorization failed: {auth_info['error']}"
            state["is_valid"] = False
            state["error"] = auth_info["error"]
            return state
        
        # Store auth info in state
        state["user_id"] = auth_info["user_id"]
        state["is_admin"] = auth_info["is_admin"]
        state["accessible_project_ids"] = auth_info["accessible_project_ids"]
        state["original_sql"] = generated_sql  # Store original for debugging
        
        # If admin, no filtering needed
        if auth_info["is_admin"]:
            logger.info(f"User {user_email} is ADMIN - no filtering applied")
            state.setdefault("messages", []).append("Admin user - no project filtering applied")
            return state
        
        # Non-admin: Apply project filters
        accessible_project_ids = auth_info["accessible_project_ids"]
        
        if not accessible_project_ids:
            logger.warning(f"User {user_email} has NO accessible projects")
            state.setdefault("messages", []).append(
                "Warning: You have no project access. Query will return empty results."
            )
            # We'll still execute the query but it will return no results
            # Filter to impossible condition
            filtered_sql = self._inject_impossible_filter(generated_sql)
        else:
            logger.info(f"Applying project filter for {len(accessible_project_ids)} accessible projects")
            filtered_sql = self._inject_project_filter(generated_sql, accessible_project_ids)
        
        state["generated_sql"] = filtered_sql
        state.setdefault("messages", []).append(
            f"Project access filter applied ({len(accessible_project_ids)} projects accessible)"
        )
        
        logger.info(f"SQL filtering complete. Original length: {len(generated_sql)}, "
                   f"Filtered length: {len(filtered_sql)}")
        
        return state
    
    def _inject_project_filter(self, sql: str, project_ids: list) -> str:
        """
        Inject project ID filters into the SQL query.
        
        Strategy:
        1. Detect if 'project' table is in the query
        2. Detect if 'zoho_deals' table is in the query
        3. Add appropriate WHERE clause filters
        4. Use table aliases if present
        
        Args:
            sql: Original SQL query
            project_ids: List of accessible project IDs (strings)
            
        Returns:
            Modified SQL with project filters
        """
        sql_lower = sql.lower()
        
        # Convert project_ids to comma-separated string with quotes (for NVARCHAR IDs)
        # e.g., '4KgNK3orWcqu793jypIiS', 'tbHMQGxmcjOh9pGUTElcy'
        project_ids_str = ",".join(f"'{str(pid)}'" for pid in project_ids)
        
        # Detect table references and their aliases
        project_ref = self._get_table_reference(sql, "project")
        zoho_deals_ref = self._get_table_reference(sql, "zoho_deals")
        
        if not project_ref and not zoho_deals_ref:
            logger.info("Query doesn't reference project or zoho_deals tables - no filtering needed")
            return sql
        
        # Build filter conditions
        filters = []
        
        if project_ref:
            # Use alias if available, otherwise use table name
            # project.id is NVARCHAR, so we use quoted strings
            filters.append(f"{project_ref}.id IN ({project_ids_str})")
        
        if zoho_deals_ref:
            # Filter zoho_deals via project.hubspot_id
            # Use alias if available
            filters.append(
                f"{zoho_deals_ref}.id IN (SELECT hubspot_id FROM project WHERE id IN ({project_ids_str}))"
            )
        
        # Inject filters into WHERE clause
        return self._inject_where_conditions(sql, filters)
    
    def _inject_impossible_filter(self, sql: str) -> str:
        """
        Inject a filter that will return no results (for users with no project access).
        
        Args:
            sql: Original SQL query
            
        Returns:
            Modified SQL with impossible condition
        """
        # Add a WHERE clause that will never be true
        return self._inject_where_conditions(sql, ["1 = 0"])
    
    def _inject_where_conditions(self, sql: str, conditions: list) -> str:
        """
        Inject WHERE conditions into a SQL query.
        
        Handles:
        - Queries with existing WHERE clause (add AND)
        - Queries without WHERE clause (add WHERE)
        - Queries with GROUP BY, ORDER BY, etc.
        - Queries ending with semicolon
        
        Args:
            sql: Original SQL query
            conditions: List of WHERE conditions to add
            
        Returns:
            Modified SQL
        """
        if not conditions:
            return sql
        
        # Remove trailing semicolon and whitespace
        sql = sql.rstrip()
        has_semicolon = sql.endswith(';')
        if has_semicolon:
            sql = sql[:-1].rstrip()
        
        # Combine all conditions with AND
        filter_clause = " AND ".join(f"({cond})" for cond in conditions)
        
        # Find if there's already a WHERE clause
        # Look for WHERE keyword (case-insensitive)
        where_pattern = re.compile(r'\bWHERE\b', re.IGNORECASE)
        where_match = where_pattern.search(sql)
        
        if where_match:
            # Already has WHERE - add AND condition
            # Insert after WHERE keyword
            where_pos = where_match.end()
            modified_sql = (
                sql[:where_pos] + 
                f" ({filter_clause}) AND " + 
                sql[where_pos:]
            )
            logger.info("Injected filter into existing WHERE clause")
        else:
            # No WHERE clause - need to add one
            # Find insertion point (before GROUP BY, ORDER BY, HAVING, etc.)
            insertion_point = self._find_where_insertion_point(sql)
            modified_sql = (
                sql[:insertion_point] + 
                f"\nWHERE {filter_clause}" + 
                sql[insertion_point:]
            )
            logger.info("Added new WHERE clause with filter")
        
        # Add semicolon back if it was there
        if has_semicolon:
            modified_sql += ";"
        
        return modified_sql
    
    def _find_where_insertion_point(self, sql: str) -> int:
        """
        Find the position where WHERE clause should be inserted.
        
        This is before GROUP BY, HAVING, ORDER BY, LIMIT, etc.
        
        Args:
            sql: SQL query
            
        Returns:
            Index position for insertion
        """
        # Look for these keywords (in order of typical appearance)
        keywords = [
            r'\bGROUP\s+BY\b',
            r'\bHAVING\b',
            r'\bORDER\s+BY\b',
            r'\bLIMIT\b',
            r'\bOFFSET\b',
            r'\bFETCH\b',
            r'\bFOR\s+XML\b',
            r'\bFOR\s+JSON\b'
        ]
        
        earliest_pos = len(sql)  # Default to end of query
        
        for keyword_pattern in keywords:
            match = re.search(keyword_pattern, sql, re.IGNORECASE)
            if match and match.start() < earliest_pos:
                earliest_pos = match.start()
        
        return earliest_pos
    
    def _get_table_reference(self, sql: str, table_name: str) -> Optional[str]:
        """
        Get the reference name for a table (alias if present, otherwise table name).
        
        Handles:
        - FROM table_name alias
        - FROM table_name AS alias
        - FROM [table_name] alias
        - JOIN table_name alias
        
        Args:
            sql: SQL query (original case)
            table_name: Table name to search for (lowercase)
            
        Returns:
            Alias or table name to use in WHERE clause, or None if not found
        """
        sql_lower = sql.lower()
        
        # Patterns to match: FROM/JOIN table_name [AS] alias
        patterns = [
            rf'\b(?:FROM|JOIN)\s+{table_name}\s+(?:AS\s+)?(\w+)\b',
            rf'\b(?:FROM|JOIN)\s+\[{table_name}\]\s+(?:AS\s+)?(\w+)\b',
            rf'\b(?:FROM|JOIN)\s+{table_name}\b',
            rf'\b(?:FROM|JOIN)\s+\[{table_name}\]\b',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, sql_lower, re.IGNORECASE)
            if match:
                # If there's a captured group (alias), use it
                if match.lastindex and match.lastindex >= 1:
                    alias = match.group(1)
                    # Make sure alias is not a SQL keyword
                    if alias not in ['where', 'order', 'group', 'having', 'limit', 'union', 'select']:
                        return alias
                # Otherwise, use the table name itself
                return table_name
        
        return None
    
    def _has_table_reference(self, sql_lower: str, table_name: str) -> bool:
        """
        Check if a table is referenced in the SQL query.
        
        Handles:
        - FROM table_name
        - JOIN table_name
        - Table aliases
        
        Args:
            sql_lower: Lowercase SQL query
            table_name: Table name to search for (lowercase)
            
        Returns:
            True if table is referenced
        """
        # Look for table name after FROM or JOIN
        patterns = [
            rf'\bFROM\s+{table_name}\b',
            rf'\bFROM\s+\[{table_name}\]',
            rf'\bJOIN\s+{table_name}\b',
            rf'\bJOIN\s+\[{table_name}\]',
        ]
        
        for pattern in patterns:
            if re.search(pattern, sql_lower, re.IGNORECASE):
                return True
        
        return False


# Global instance
sql_filter_agent = SQLFilterAgent()
