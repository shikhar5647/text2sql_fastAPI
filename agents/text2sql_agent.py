"""Text to SQL Agent for generating SQL queries."""
import google.generativeai as genai
from typing import Dict, Any
from config.secrets import secrets_manager
from config.settings import settings
from graph.state import GraphState
from utils.logger import setup_logger
from utils.helpers import sanitize_sql

logger = setup_logger(__name__)

class Text2SQLAgent:
    """SQL Generation Agent."""
    
    def __init__(self):
        genai.configure(api_key=secrets_manager.get_gemini_api_key())
        self.model = genai.GenerativeModel(settings.GEMINI_MODEL)
    
    def generate_sql(self, state: GraphState) -> GraphState:
        """Generate SQL query from natural language."""
        
        # --- START OF FIX ---
        
        # 1. ADDED SAFETY CHECK (from your standalone function)
        # We check for *relevant_tables* (which is now corrected by the new schema_agent)
        if not state.get("relevant_tables"):
            state["generated_sql"] = None
            state["is_valid"] = False
            state["requires_human_approval"] = False
            state["validation_message"] = "No correct schema identified for the question. Cannot generate SQL without risking hallucination."
            state.setdefault("messages", []).append(state["validation_message"])
            logger.info("Text2SQL agent: abstaining from SQL generation because no schema identified.")
            state["step"] = "validation_failed" # Skip to failed validation
            return state

        user_query = state["user_query"]
        schema_context = state.get("schema_context", "")
        
        logger.info(f"Generating SQL for: {user_query}")
        
        # 2. ENHANCED PROMPT (to prevent hallucination)
        # Load few-shot examples from the project file if available and include them in the prompt
        examples_text = ""
        try:
            examples_path = settings.PROJECT_ROOT / "few_shots.txt"
            if examples_path.exists():
                with open(examples_path, "r", encoding="utf-8") as ef:
                    examples_text = ef.read().strip()
        except Exception as e:
            logger.warning(f"Could not load few-shot examples: {e}")
            
            
            
        prompt = (
            "You are a senior data engineer generating safe, production-quality T-SQL for Microsoft SQL Server.\n\n"
            "This system is built for Trimstone and runs against a single Trimstone-owned database. All queries should assume the context is Trimstone’s internal database environment.\n"
            "Never add tenancy filters like tenant_id, org_id, etc. unless they exist in the schema.\n"
            "Never ask the user to disambiguate which database or tenant to use.\n\n"
            
            "You will receive the user request and the available schema. Your job is to write a single, safe SELECT statement.\n\n"
            
            "FEW-SHOT EXAMPLES (use these as style/format guidance; they are authoritative examples of multi-table JOINs and aggregations):\n\n"
            f"{examples_text}\n\n"
            
            "NATURAL LANGUAGE MAPPINGS (apply these simple mappings when interpreting the user request):\n"
            "- Treat words like 'live', 'running', 'ongoing' as meaning status = 'Active' when a status-like column exists (e.g., status, project_status, state).\n"
            "- When user asks for 'budget left', 'remaining budget', 'how much budget is left', compute it as (budget - spent) when appropriate or use an existing column named 'budget_remaining' / 'budget_left' if present. If an aggregate is required (e.g., multiple expense rows), compute budget left as: budget - COALESCE(SUM(spent_column), 0).\n"
            "- Be conservative: only reference columns that appear in the provided SCHEMA. If no suitable columns exist, return NO_SCHEMA_MATCH.\n\n"
            
            "NAME RESOLUTION & USER MATCHING (CRITICAL RULES for PERSON NAME queries):\n"
            "- If the user request mentions a person's name (e.g., 'James', 'Hazal', 'Leila', etc.), treat it as a reference to the `user` table.\n"
            "- Always match name using LIKE for both first and last name, even if the name appears to be complete.\n"
            "- ALWAYS use this exact matching pattern (case insensitive + partial match):\n"
            "    WHERE user.first_name LIKE '%<name>%' OR user.last_name LIKE '%<name>%'\n"
            "-use this kind of query so that we donot miss out anything using both id and email for matching: SELECT p.id AS project_id, p.name AS project_name, u.id AS owner_user_id, u.first_name, u.last_name, u.email AS owner_email FROM project p LEFT JOIN [user] u ON p.owner_id = u.id OR p.owner_email = u.email;\n"
            "- NEVER use '=' for name matching unless explicitly instructed.\n"
            "- Names should always be resolved BEFORE joining to related records such as projects, clients, tasks, etc.\n"
            "- Example: 'Show me all projects James is working on' MUST be translated into a query that joins through the user table and uses LIKE instead of '='.\n"
            "- If multiple users match the name, return all matching rows.\n"
            "- If the schema does not contain the required user table or name columns, return NO_SCHEMA_MATCH.\n\n"
            
            "STRICT RULES:\n"
            "- **CRITICAL:** Use **ONLY** the tables and columns provided in the SCHEMA section.\n"
            "- If the USER REQUEST cannot be answered using the provided SCHEMA, respond with the exact text:\n"
            "  \"NO_SCHEMA_MATCH: Cannot answer query with available schema.\"\n"
            "- Output only the SQL, no commentary or markdown fences.\n"
            "- Only SELECT is allowed. Never use INSERT/UPDATE/DELETE/CREATE/ALTER/DROP/EXEC.\n"
            "- Prefer explicit JOINs with ON clauses over implicit joins.\n"
            "- Donot perform type casting while joining. In most of the cases, keys are of the same type already.\n"
            "- Show names along with the associated id's, whenever possible. Do not return only ids as it's not user friendly and readable. Example: user_id along with user name, client_id along with client name, project name along with project_id.\n\n"
            "- Donot assume join relations between tables. Please check the relations provided.\n"
            
            f"SCHEMA (authoritative):\n{schema_context}\n\n"
            f"USER REQUEST:\n{user_query}\n\n"
            "Return only the final SQL (no code fences, no explanation) OR \"NO_SCHEMA_MATCH:...\"\n"
        )


        # prompt = (
        #     "You are a senior data engineer generating safe, production-quality T-SQL for Microsoft SQL Server.\n\n"
        #     "You will receive the user request and the available schema. Your job is to write a single, safe SELECT statement.\n\n"
        #     "FEW-SHOT EXAMPLES (use these as style/format guidance; they are authoritative examples of multi-table JOINs and aggregations):\n\n"
        #     f"{examples_text}\n\n"
        #     "NATURAL LANGUAGE MAPPINGS (apply these simple mappings when interpreting the user request):\n"
        #     "- Treat words like 'live', 'running', 'ongoing' as meaning status = 'Active' when a status-like column exists (e.g., status, project_status, state).\n"
        #     "- When user asks for 'budget left', 'remaining budget', 'how much budget is left', compute it as (budget - spent) when appropriate or use an existing column named 'budget_remaining' / 'budget_left' if present. If an aggregate is required (e.g., multiple expense rows), compute budget left as: budget - COALESCE(SUM(spent_column), 0).\n"
        #     "- Be conservative: only reference columns that appear in the provided SCHEMA. If no suitable columns exist, return NO_SCHEMA_MATCH.\n\n"
        #     "STRICT RULES:\n"
        #     "- **CRITICAL:** Use **ONLY** the tables and columns provided in the SCHEMA section.\n"
        #     "- If the USER REQUEST cannot be answered using the provided SCHEMA, respond with the exact text:\n"
        #     "  \"NO_SCHEMA_MATCH: Cannot answer query with available schema.\"\n"
        #     "- Output only the SQL, no commentary or markdown fences.\n"
        #     "- Only SELECT is allowed. Never use INSERT/UPDATE/DELETE/CREATE/ALTER/DROP/EXEC.\n"
        #     "- Prefer explicit JOINs with ON clauses over implicit joins.\n"
        #     "- Donot perform type casting while joining.In most of the cases,keys are of the same type already.\n"
        #     "- Show names along with the associated id's, whenever possible, donot return only ids as its not user friendly and readable.Example: user_id along with user name,client_id along with client name. Project name along with project_id.\n"
        #     f"SCHEMA (authoritative):\n{schema_context}\n\n"
        #     f"USER REQUEST:\n{user_query}\n\n"
        #     "Return only the final SQL (no code fences, no explanation) OR \"NO_SCHEMA_MATCH:...\"\n"
        # )
        # --- END OF FIX ---
        
        try:
            response = self.model.generate_content(prompt)
            sql_query = response.text.strip()
            
            # --- START OF FIX ---
            # 3. ADDED SENTINEL CHECK
            if "NO_SCHEMA_MATCH" in sql_query:
                logger.warning(f"Text2SQL agent: LLM returned NO_SCHEMA_MATCH for query: {user_query}")
                state["generated_sql"] = None
                state["is_valid"] = False
                state["validation_message"] = "No correct schema identified to answer the question."
                state["step"] = "validation_failed"
                return state
            # --- END OF FIX ---

            # Extract SQL from markdown code blocks if present
            if "```sql" in sql_query:
                sql_query = sql_query.split("```sql")[1].split("```")[0].strip()
            elif "```" in sql_query:
                sql_query = sql_query.split("```")[1].split("```")[0].strip()
            
            # Sanitize and format
            sql_query = sanitize_sql(sql_query)
            
            state["generated_sql"] = sql_query
            state["step"] = "sql_generated"
            
            logger.info(f"Generated SQL: {sql_query}")
            
        except Exception as e:
            logger.error(f"SQL generation error: {str(e)}")
            state["error"] = f"SQL generation failed: {str(e)}"
            state["step"] = "error"
        
        return state


text2sql_agent = Text2SQLAgent()