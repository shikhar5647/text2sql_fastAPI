"""LangGraph workflow definition."""
from typing import Literal
from langgraph.graph import StateGraph, END
from graph.state import GraphState
from agents.nlu_agent import nlu_agent
from agents.schema_agent import schema_agent
from agents.text2sql_agent import text2sql_agent
from agents.validator_agent import validator_agent
from agents.executor_agent import executor_agent
from agents.formatter_agent import formatter_agent
from utils.logger import setup_logger

logger = setup_logger(__name__)

def create_workflow() -> StateGraph:
    """Create the Text-to-SQL LangGraph workflow."""
    
    # Initialize workflow
    workflow = StateGraph(GraphState)
    
    # Add nodes
    workflow.add_node("nlu", nlu_agent.analyze_intent)
    workflow.add_node("schema", schema_agent.get_relevant_schema)
    workflow.add_node("text2sql", text2sql_agent.generate_sql)
    workflow.add_node("validator", validator_agent.validate_sql)
    workflow.add_node("executor", executor_agent.execute_sql)
    workflow.add_node("formatter", formatter_agent.format_results)
    
    # Define edges
    workflow.add_edge("nlu", "schema")
    workflow.add_edge("schema", "text2sql")
    workflow.add_edge("text2sql", "validator")
    
    # Conditional edge from validator
    def should_execute(state: GraphState) -> Literal["executor", "end"]:
        if state.get("is_valid", False) and state.get("execution_approved", False):
            return "executor"
        elif not state.get("is_valid", False):
            return "end"
        else:
            # Awaiting approval
            return "end"
    
    workflow.add_conditional_edges(
        "validator",
        should_execute,
        {
            "executor": "executor",
            "end": END
        }
    )
    
    # Conditional edge from executor
    def should_format(state: GraphState) -> Literal["formatter", "end"]:
        if state.get("query_results") is not None:
            return "formatter"
        return "end"
    
    workflow.add_conditional_edges(
        "executor",
        should_format,
        {
            "formatter": "formatter",
            "end": END
        }
    )
    
    workflow.add_edge("formatter", END)
    
    # Set entry point
    workflow.set_entry_point("nlu")
    
    return workflow.compile()

# Create compiled workflow
text2sql_workflow = create_workflow()