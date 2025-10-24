"""LangGraph state definition."""
from typing import TypedDict, List, Dict, Any, Optional, Annotated
from langgraph.graph import add_messages

class GraphState(TypedDict):
    """State for the Text-to-SQL workflow."""
    
    # User input
    user_query: str
    
    # Intent understanding
    intent: Optional[str]
    entities: List[str]
    
    # Schema information
    relevant_tables: List[str]
    schema_context: str
    
    # SQL generation
    generated_sql: Optional[str]
    
    # Validation
    is_valid: bool
    validation_message: str
    safety_check: bool
    
    # Execution
    execution_approved: bool
    query_results: Optional[List[Dict[str, Any]]]
    execution_error: Optional[str]
    
    # Formatting
    formatted_response: Optional[str]
    
    # Messages and conversation
    messages: Annotated[list, add_messages]
    
    # Workflow control
    step: str
    error: Optional[str]
    requires_human_approval: bool