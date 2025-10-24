"""SQL Validator and Safety Agent."""
from typing import Dict, Any
from graph.state import GraphState
from utils.logger import setup_logger
from utils.helpers import is_safe_query, extract_tables_from_query

logger = setup_logger(__name__)

class ValidatorAgent:
    """SQL Validation and Safety Agent."""
    
    def validate_sql(self, state: GraphState) -> GraphState:
        """Validate SQL query for safety and correctness."""
        sql_query = state.get("generated_sql", "")
        
        logger.info("Validating SQL query")
        
        try:
            # Safety check
            is_safe, safety_message = is_safe_query(sql_query)
            
            if not is_safe:
                state["is_valid"] = False
                state["validation_message"] = safety_message
                state["safety_check"] = False
                state["step"] = "validation_failed"
                logger.warning(f"Query failed safety check: {safety_message}")
                return state
            
            # Extract tables used
            tables_used = extract_tables_from_query(sql_query)
            
            # Basic syntax validation
            if not sql_query or len(sql_query) < 10:
                state["is_valid"] = False
                state["validation_message"] = "Query is too short or empty"
                state["safety_check"] = True
                state["step"] = "validation_failed"
                return state
            
            # All checks passed
            state["is_valid"] = True
            state["validation_message"] = f"Query is valid. Tables used: {', '.join(tables_used)}"
            state["safety_check"] = True
            state["step"] = "validated"
            state["requires_human_approval"] = True
            
            logger.info("SQL query validated successfully")
            
        except Exception as e:
            logger.error(f"Validation error: {str(e)}")
            state["is_valid"] = False
            state["validation_message"] = f"Validation error: {str(e)}"
            state["safety_check"] = False
            state["step"] = "validation_failed"
        
        return state

validator_agent = ValidatorAgent()