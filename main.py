# /main.py
"""
Main FastAPI application for the Text-to-SQL service.
"""
import uvicorn
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

# Import your project's modules
from graph.workflow import text2sql_workflow
from graph.state import GraphState
from database.connection import db_connection
from database.schema_cache import schema_cache
from config.secrets import secrets_manager
from utils.logger import setup_logger

# --- Import Agents directly ---
# We will call them manually in the execute endpoint
from agents.validator_agent import validator_agent
from agents.executor_agent import executor_agent
from agents.formatter_agent import formatter_agent

# Import Pydantic models
from api_models import (
    QueryRequest, 
    ExecuteRequest,  # <-- Import new model
    GraphStateModel, 
    StatusResponse, 
    SchemaResponse
)

logger = setup_logger(__name__)

# (Startup lifespan function remains the same)
@asynccontextmanager
async def lifespan(app: FastAPI):
    validate_app_secrets()
    logger.info("🚀 FastAPI application startup complete.")
    yield
    db_connection.disconnect()
    logger.info("🛑 FastAPI application shutdown.")

def validate_app_secrets():
    if not secrets_manager.validate_secrets():
        logger.error("❌ Missing required configuration. Please check your .env file.")
        raise RuntimeError("Missing required configuration. Check .env file.")
    logger.info("✅ Application secrets validated.")

# Initialize FastAPI app
app = FastAPI(
    title="Text-to-SQL Assistant API",
    description="Converts natural language to SQL using AI-powered agents.",
    version="1.0.0",
    lifespan=lifespan
)

# --- Workflow Endpoints ---

@app.post("/query/generate", response_model=GraphStateModel, tags=["Workflow"])
async def generate_sql_query(request: QueryRequest) -> GraphStateModel:
    """
    Start a new query. Runs the workflow up to the validation/approval step.
    Returns the generated SQL and the current workflow state.
    """
    logger.info(f"Received new query: {request.user_query}")
    try:
        initial_state: GraphState = {
            "user_query": request.user_query,
            "intent": None,
            "entities": [],
            "relevant_tables": [],
            "schema_context": "",
            "generated_sql": None,
            "is_valid": False,
            "validation_message": "",
            "safety_check": False,
            "execution_approved": False,
            "query_results": None,
            "execution_error": None,
            "formatted_response": None,
            "messages": [], 
            "step": "start",
            "error": None,
            "requires_human_approval": False
        }
        
        result_state = text2sql_workflow.invoke(initial_state)
        
        result_state['messages'] = [str(m) for m in result_state.get('messages', [])]
        
        return GraphStateModel.model_validate(result_state)

    except Exception as e:
        logger.error(f"Error during query generation: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing query: {str(e)}")


# --- MODIFIED EXECUTE ENDPOINT ---

@app.post("/query/execute", response_model=GraphStateModel, tags=["Workflow"])
async def execute_sql_query(request: ExecuteRequest) -> GraphStateModel:
    """
    Execute a query that has been generated and approved.
    This endpoint re-validates the SQL for safety before execution.
    """
    logger.info(f"Received request to execute SQL: {request.generated_sql}")

    if not request.execution_approved:
        raise HTTPException(status_code=400, detail="Query has not been approved for execution.")

    # 1. Create a minimal state for the agents
    state_dict: GraphState = {
        "user_query": request.user_query,
        "generated_sql": request.generated_sql,
        "execution_approved": request.execution_approved,
        "query_results": None,
        "execution_error": None,
        "formatted_response": None,
        "messages": [],
        "step": "execute",
        # Set other keys to defaults
        "intent": None, "entities": [], "relevant_tables": [], "schema_context": "",
        "is_valid": False, "validation_message": "", "safety_check": False,
        "error": None, "requires_human_approval": False
    }

    try:
        # 2. Re-validate the SQL (CRITICAL security step)
        val_state = validator_agent.validate_sql(state_dict)
        
        if not val_state.get("is_valid"):
            logger.warning(f"Execution rejected: SQL failed validation: {val_state['validation_message']}")
            val_state['messages'] = [str(m) for m in val_state.get('messages', [])]
            return GraphStateModel.model_validate(val_state)

        # 3. Execute the validated SQL
        exec_state = executor_agent.execute_sql(val_state)
        
        if exec_state.get("execution_error"):
            logger.warning(f"Execution error: {exec_state['execution_error']}")
            exec_state['messages'] = [str(m) for m in exec_state.get('messages', [])]
            return GraphStateModel.model_validate(exec_state)

        # 4. Format the results
        final_state = formatter_agent.format_results(exec_state)
        
        final_state['messages'] = [str(m) for m in final_state.get('messages', [])]
        return GraphStateModel.model_validate(final_state)

    except Exception as e:
        logger.error(f"Error during query execution: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error executing query: {str(e)}")

# --- Schema and DB Endpoints (Unchanged) ---
# (all other endpoints: /db/test, /schema, etc. remain the same)

@app.get("/db/test", response_model=StatusResponse, tags=["Admin"])
async def test_db_connection():
    if db_connection.test_connection():
        return StatusResponse(status="ok", message="✅ Connected to MS SQL Server")
    else:
        return StatusResponse(status="error", message="❌ Database connection failed.")

@app.get("/schema", response_model=SchemaResponse, tags=["Admin"])
async def get_database_schema():
    try:
        schema = schema_cache.get_schema()
        return SchemaResponse(schema_data=schema)
    except Exception as e:
        logger.error(f"Failed to get schema: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get schema: {str(e)}")

@app.post("/schema/refresh-db", response_model=StatusResponse, tags=["Admin"])
async def refresh_schema_from_db():
    try:
        schema_cache.refresh_schema()
        return StatusResponse(status="ok", message="Schema refreshed from database.")
    except Exception as e:
        logger.error(f"Failed to refresh schema from DB: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to refresh schema: {str(e)}")

@app.post("/schema/load-excel", response_model=StatusResponse, tags=["Admin"])
async def load_schema_from_excel():
    try:
        schema_cache.load_schema_from_excel()
        return StatusResponse(status="ok", message="Schema loaded from Excel.")
    except Exception as e:
        logger.error(f"Failed to load schema from Excel: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to load schema: {str(e)}")

@app.post("/schema/load-manual", response_model=StatusResponse, tags=["Admin"])
async def load_manual_schema():
    try:
        schema_cache.load_manual_schema()
        return StatusResponse(status="ok", message="Manual schema loaded.")
    except Exception as e:
        logger.error(f"Failed to load manual schema: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to load manual schema: {str(e)}")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)