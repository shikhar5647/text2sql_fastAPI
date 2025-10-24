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
from agents.executor_agent import executor_agent
from agents.formatter_agent import formatter_agent
from utils.logger import setup_logger

# Import Pydantic models
from api_models import (
    QueryRequest, 
    GraphStateModel, 
    StatusResponse, 
    SchemaResponse
)

logger = setup_logger(__name__)

# Validate secrets on startup
def validate_app_secrets():
    if not secrets_manager.validate_secrets():
        logger.error("❌ Missing required configuration. Please check your .env file.")
        raise RuntimeError("Missing required configuration. Check .env file.")
    logger.info("✅ Application secrets validated.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Code to run on startup
    validate_app_secrets()
    logger.info("🚀 FastAPI application startup complete.")
    yield
    # Code to run on shutdown
    db_connection.disconnect()
    logger.info("🛑 FastAPI application shutdown.")

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
        # Initialize state
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
            "messages": [], # LangGraph needs this key
            "step": "start",
            "error": None,
            "requires_human_approval": False
        }
        
        # Run workflow up to the first blocking point (human approval)
        result_state = text2sql_workflow.invoke(initial_state)
        
        # Convert the result dict to our Pydantic model for response
        # Note: 'messages' in GraphState is complex; we'll simplify it for the API.
        result_state['messages'] = [str(m) for m in result_state.get('messages', [])]
        
        return GraphStateModel.parse_obj(result_state) # pydantic v1
        # return GraphStateModel.model_validate(result_state) # pydantic v2

    except Exception as e:
        logger.error(f"Error during query generation: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing query: {str(e)}")

@app.post("/query/execute", response_model=GraphStateModel, tags=["Workflow"])
async def execute_sql_query(state: GraphStateModel) -> GraphStateModel:
    """
    Execute a query that has already been generated and approved.
    The client must send back the *entire state* received from /query/generate,
    with "execution_approved" set to true.
    """
    logger.info(f"Received request to execute SQL: {state.generated_sql}")
    
    # Convert Pydantic model back to GraphState dict for the agents
    state_dict: GraphState = state.dict() # pydantic v1
    # state_dict: GraphState = state.model_dump() # pydantic v2

    if not state_dict.get("is_valid"):
        raise HTTPException(status_code=400, detail="Query failed validation and cannot be executed.")
        
    if not state_dict.get("execution_approved"):
        raise HTTPException(status_code=400, detail="Query has not been approved for execution.")

    try:
        # Manually continue the workflow from the executor step
        exec_state = executor_agent.execute_sql(state_dict)
        
        if exec_state.get("execution_error"):
            logger.warning(f"Execution error: {exec_state['execution_error']}")
            exec_state['messages'] = [str(m) for m in exec_state.get('messages', [])]
            return GraphStateModel.parse_obj(exec_state) # pydantic v1
            # return GraphStateModel.model_validate(exec_state) # pydantic v2

        # If execution is successful, format the results
        final_state = formatter_agent.format_results(exec_state)
        
        final_state['messages'] = [str(m) for m in final_state.get('messages', [])]
        return GraphStateModel.parse_obj(final_state) # pydantic v1
        # return GraphStateModel.model_validate(final_state) # pydantic v2

    except Exception as e:
        logger.error(f"Error during query execution: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error executing query: {str(e)}")

# --- Schema and DB Endpoints ---

@app.get("/db/test", response_model=StatusResponse, tags=["Admin"])
async def test_db_connection():
    """Test the connection to the MS SQL Server database."""
    if db_connection.test_connection():
        return StatusResponse(status="ok", message="✅ Connected to MS SQL Server")
    else:
        return StatusResponse(status="error", message="❌ Database connection failed.")

@app.get("/schema", response_model=SchemaResponse, tags=["Admin"])
async def get_database_schema():
    """Get the currently cached database schema."""
    try:
        schema = schema_cache.get_schema()
        return SchemaResponse(schema=schema)
    except Exception as e:
        logger.error(f"Failed to get schema: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get schema: {str(e)}")

@app.post("/schema/refresh-db", response_model=StatusResponse, tags=["Admin"])
async def refresh_schema_from_db():
    """Force-refresh the schema cache from the live database."""
    try:
        schema_cache.refresh_schema()
        return StatusResponse(status="ok", message="Schema refreshed from database.")
    except Exception as e:
        logger.error(f"Failed to refresh schema from DB: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to refresh schema: {str(e)}")

@app.post("/schema/load-excel", response_model=StatusResponse, tags=["Admin"])
async def load_schema_from_excel():
    """Load/overwrite the schema cache from the 'trimstone_final.xlsx' file."""
    try:
        schema_cache.load_schema_from_excel()
        return StatusResponse(status="ok", message="Schema loaded from Excel.")
    except Exception as e:
        logger.error(f"Failed to load schema from Excel: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to load schema: {str(e)}")

@app.post("/schema/load-manual", response_model=StatusResponse, tags=["Admin"])
async def load_manual_schema():
    """Load/overwrite the schema cache with the predefined manual schema."""
    try:
        schema_cache.load_manual_schema()
        return StatusResponse(status="ok", message="Manual schema loaded.")
    except Exception as e:
        logger.error(f"Failed to load manual schema: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to load manual schema: {str(e)}")

if __name__ == "__main__":
    # Run the FastAPI server
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)