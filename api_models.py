# /api_models.py
"""
Pydantic models for the FastAPI application, mirroring the GraphState TypedDict.
"""
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class QueryRequest(BaseModel):
    """Request model for starting a new query."""
    user_query: str

class GraphStateModel(BaseModel):
    """
    Pydantic model representing the full workflow state.
    Used for both request and response bodies after the initial query.
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
    
    # Messages and conversation (simplified for API)
    messages: List[str] = []
    
    # Workflow control
    step: Optional[str] = None
    error: Optional[str] = None
    requires_human_approval: bool = False

    # This allows converting the TypedDict from the graph directly to this model
    class Config:
        orm_mode = True 
        # In Pydantic v2, orm_mode is replaced by from_attributes
        # from_attributes = True 

class StatusResponse(BaseModel):
    """Generic status response model."""
    status: str
    message: str

class SchemaResponse(BaseModel):
    """Response model for returning the database schema."""
    schema: Dict[str, Any]