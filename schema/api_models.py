# /api_models.py
"""
Pydantic models for the FastAPI application.
"""
from pydantic import BaseModel, ConfigDict
from typing import List, Dict, Any, Optional

class QueryRequest(BaseModel):
    """Request model for starting a new query."""
    user_query: str
    user_email: str

# --- NEW MODEL ---
class ExecuteRequest(BaseModel):
    """
    Request model for executing a query.
    This is much simpler than sending the whole state back.
    """
    user_query: str
    generated_sql: str
    execution_approved: bool

class GraphStateModel(BaseModel):
    """
    Pydantic model representing the full workflow state.
    This is the main response model for BOTH endpoints.
    """
    # User input
    user_query: str
    
    # Intent understanding
    intent: Optional[str] = None
    entities: List[str] = []
    
    # Schema information
    relevant_tables: List[str] = []
    schema_context: Optional[str] = None
    
    # SQL generation
    generated_sql: Optional[str] = None
    
    # Validation
    is_valid: bool = False
    validation_message: Optional[str] = None
    safety_check: bool = False
    
    # Execution
    execution_approved: bool = False
    query_results: Optional[List[Dict[str, Any]]] = None
    execution_error: Optional[str] = None
    
    # Formatting
    formatted_response: Optional[str] = None
    
    # Messages and conversation
    messages: List[str] = []
    
    # Workflow control
    step: Optional[str] = None
    error: Optional[str] = None
    requires_human_approval: bool = False

    # Pydantic v2 config
    model_config = ConfigDict(from_attributes=True)


class StatusResponse(BaseModel):
    """Generic status response model."""
    status: str
    message: str

class SchemaResponse(BaseModel):
    """Response model for returning the database schema."""
    schema_data: Dict[str, Any]


# ======================================================================
#            NEW STREAMING MODELS (Added Without Touching Existing Code)
# ======================================================================

class StreamEvent(BaseModel):
    """
    Model representing a single streaming Server-Sent-Event (SSE).
    Matches the 5-step workflow UI.
    """
    step: str
    message: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class StreamingResponse(BaseModel):
    """
    Wrapper model for streaming responses (if needed).
    Not used by the FastAPI endpoint return directly (SSE),
    but provided for structure and validation.
    """
    status: str
    message: str









# # /api_models.py
# """
# Pydantic models for the FastAPI application.
# """
# from pydantic import BaseModel, ConfigDict
# from typing import List, Dict, Any, Optional

# class QueryRequest(BaseModel):
#     """Request model for starting a new query."""
#     user_query: str

# # --- NEW MODEL ---
# class ExecuteRequest(BaseModel):
#     """
#     Request model for executing a query.
#     This is much simpler than sending the whole state back.
#     """
#     user_query: str
#     generated_sql: str
#     execution_approved: bool

# class GraphStateModel(BaseModel):
#     """
#     Pydantic model representing the full workflow state.
#     This is the main response model for BOTH endpoints.
#     """
#     # User input
#     user_query: str
    
#     # Intent understanding
#     intent: Optional[str] = None
#     entities: List[str] = []
    
#     # Schema information
#     relevant_tables: List[str] = []
#     schema_context: Optional[str] = None
    
#     # SQL generation
#     generated_sql: Optional[str] = None
    
#     # Validation
#     is_valid: bool = False
#     validation_message: Optional[str] = None
#     safety_check: bool = False
    
#     # Execution
#     execution_approved: bool = False
#     query_results: Optional[List[Dict[str, Any]]] = None
#     execution_error: Optional[str] = None
    
#     # Formatting
#     formatted_response: Optional[str] = None
    
#     # Messages and conversation
#     messages: List[str] = []
    
#     # Workflow control
#     step: Optional[str] = None
#     error: Optional[str] = None
#     requires_human_approval: bool = False

#     # Pydantic v2 config
#     model_config = ConfigDict(from_attributes=True)


# class StatusResponse(BaseModel):
#     """Generic status response model."""
#     status: str
#     message: str

# class SchemaResponse(BaseModel):
#     """Response model for returning the database schema."""
#     schema_data: Dict[str, Any]