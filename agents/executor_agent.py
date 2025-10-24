"""SQL Executor Agent."""
from typing import Dict, Any, List
from graph.state import GraphState
from database.connection import db_connection
from utils.logger import setup_logger

logger = setup_logger(__name__)

class ExecutorAgent:
    """SQL Execution Agent."""
    
    def execute_sql(self, state: GraphState) -> GraphState:
        """Execute validated SQL query."""
        sql_query = state.get("generated_sql", "")
        
        # Check if execution is approved
        if not state.get("execution_approved", False):
            state["step"] = "awaiting_approval"
            logger.info("Awaiting human approval for execution")
            return state
        
        logger.info(f"Executing SQL: {sql_query}")
        
        try:
            # Execute query
            results = db_connection.execute_query(sql_query)
            
            state["query_results"] = results
            state["execution_error"] = None
            state["step"] = "executed"
            
            logger.info(f"Query executed successfully. Returned {len(results)} rows")
            
        except Exception as e:
            logger.error(f"Execution error: {str(e)}")
            state["query_results"] = None
            state["execution_error"] = str(e)
            state["step"] = "execution_failed"
        
        return state

executor_agent = ExecutorAgent()