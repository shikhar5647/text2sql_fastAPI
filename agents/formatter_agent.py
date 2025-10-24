"""Result Formatter Agent."""
import google.generativeai as genai
from typing import Dict, Any, List
import pandas as pd
from graph.state import GraphState
from config.secrets import secrets_manager
from config.settings import settings
from utils.logger import setup_logger

logger = setup_logger(__name__)

class FormatterAgent:
    """Result Formatting Agent."""
    
    def __init__(self):
        genai.configure(api_key=secrets_manager.get_gemini_api_key())
        self.model = genai.GenerativeModel(settings.GEMINI_MODEL)
    
    def format_results(self, state: GraphState) -> GraphState:
        """Format query results for user-friendly display."""
        results = state.get("query_results", [])
        user_query = state["user_query"]
        
        logger.info("Formatting query results")
        
        try:
            if not results:
                state["formatted_response"] = "No results found for your query."
                state["step"] = "complete"
                return state
            
            # Create summary with LLM
            df = pd.DataFrame(results)
            summary = f"Found {len(results)} results.\n\n"
            
            # Get column names and sample data
            columns = list(df.columns)
            sample_rows = df.head(3).to_dict('records')
            
            prompt = f"""Provide a brief, natural language summary of these query results for the user.

User's Question: {user_query}

Columns: {', '.join(columns)}
Number of Results: {len(results)}
Sample Data: {sample_rows}

Provide a concise 2-3 sentence summary that:
1. Confirms what data was retrieved
2. Highlights key findings or patterns
3. Is written in natural, user-friendly language and easy to understand

Summary:"""
            
            response = self.model.generate_content(prompt)
            summary += response.text.strip()
            
            state["formatted_response"] = summary
            state["step"] = "complete"
            
            logger.info("Results formatted successfully")
            
        except Exception as e:
            logger.error(f"Formatting error: {str(e)}")
            state["formatted_response"] = f"Results retrieved but formatting failed: {str(e)}"
            state["step"] = "complete"
        
        return state

formatter_agent = FormatterAgent()