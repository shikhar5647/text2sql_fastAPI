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
            "PRIMARY TABLES (use these as your first choice):\n"
            "- `project` is the PRIMARY table for project information (name, status, budget, owner, dates, etc.)\n"
            "- `contacts` is the PRIMARY table for contact information (first_name, last_name, email, phone, client association)\n"
            "- `client` is the PRIMARY table for client/company/account information (name, email, phone, address, industry, etc.)\n"
            "- Zoho tables (`zoho_deals`, `zoho_accounts`, `zoho_contacts`) are SECONDARY and should ONLY be used when the user explicitly mentions 'zoho' in their query\n\n"
            
            "DOMAIN MAPPINGS (apply these when interpreting user queries):\n"
            "- When user asks about 'projects' or 'project' (without mentioning 'zoho'), use the `project` table\n"
            "- When user asks about 'contacts' or 'contact' (without mentioning 'zoho'), use the `contacts` table\n"
            "- When user asks about 'accounts', 'companies', 'company', or 'clients' (without mentioning 'zoho'), use the `client` table\n"
            "- When user explicitly mentions 'zoho projects' or 'projects in zoho', use the `zoho_deals` table\n"
            "- When user explicitly mentions 'zoho contacts' or 'contacts in zoho', use the `zoho_contacts` table\n"
            "- When user explicitly mentions 'zoho accounts' or 'accounts in zoho', use the `zoho_accounts` table\n\n"
            
            "DOMAIN RELATIONSHIPS (use these join rules to connect data across systems):\n"
            "1) `project` table relationships:\n"
            "   - `project.client_id = client.id` to get client/company information for a project\n"
            "   - `project.owner_id = user.id` OR `project.owner_email = user.email` to get project owner information\n"
            "   - `project.hubspot_id = zoho_deals.id` to connect to Zoho deals (only when Zoho data is explicitly requested)\n"
            "   - `project.xero_id = xero_projects.project_id` to get billing/financial information\n"
            "2) `contacts` table relationships:\n"
            "   - `contacts.client_id = client.id` to get the company/client for a contact\n"
            "   - `contacts.hubspot_id = zoho_contacts.id` to connect to Zoho contacts (only when Zoho data is explicitly requested)\n"
            "3) `client` table relationships:\n"
            "   - `client.hubspot_id = zoho_accounts.id` to connect to Zoho accounts (only when Zoho data is explicitly requested)\n"
            "4) Zoho table relationships (only use when 'zoho' is explicitly mentioned):\n"
            "   - `zoho_deals.zoho_account_id = zoho_accounts.id` for Zoho deal-to-account connections\n"
            "   - `zoho_deals.owner_email = user.email` to get Zoho deal owner information\n"
            "- IMPORTANT: Only apply these joins if the referenced columns and tables are present in the SCHEMA provided above. If a required column/table is missing, return NO_SCHEMA_MATCH rather than inventing the join.\n\n"
            
            "NAME RESOLUTION & USER MATCHING (CRITICAL RULES for PERSON NAME queries):\n"
            "- If the user request mentions a person's name (e.g., 'James', 'Hazal', 'Leila', etc.), treat it as a reference to the `user` table.\n"
            "- Always match name using LIKE for both first and last name, even if the name appears to be complete.\n"
            "- ALWAYS use this exact matching pattern (case insensitive + partial match):\n"
            "    WHERE user.first_name LIKE '%<name>%' OR user.last_name LIKE '%<name>%'\n"
            "- Use this kind of query so that we do not miss anything using both id and email for matching: SELECT p.id AS project_id, p.name AS project_name, u.id AS owner_user_id, u.first_name, u.last_name, u.email AS owner_email FROM project p LEFT JOIN [user] u ON p.owner_id = u.id OR p.owner_email = u.email;\n"
            "- NEVER use '=' for name matching unless explicitly instructed.\n"
            "- Names should always be resolved BEFORE joining to related records such as projects, clients, tasks, etc.\n"
            "- Example: 'Show me all projects James is working on' MUST be translated into a query that joins through the user table and uses LIKE instead of '='.\n"
            "- If multiple users match the name, return all matching rows.\n"
            "- If the schema does not contain the required user table or name columns, return NO_SCHEMA_MATCH.\n"
            "- Whenever the user mentions owner/owns this project, include owner-related columns (owner_id, owner_email, created_by) to get owner details.\n\n"
            
            "OWNER QUERIES (CRITICAL for 'who is the owner' or 'who owns' questions):\n"
            "- When user asks 'who is the owner of [project]?' or 'who owns [project]?', you MUST JOIN with the user table to get owner information\n"
            "- The project table has owner-related columns: owner_id (INT) and owner_email (NVARCHAR)\n"
            "- Use this join strategy to find the owner (DO NOT use CAST or TRY_CAST):\n"
            "    LEFT JOIN [user] u ON p.owner_id = u.id OR p.owner_email = u.email\n"
            "- This ensures we match the owner whether they are stored in owner_id or owner_email\n"
            "- Always return owner details: u.first_name, u.last_name, u.email, along with project information\n"
            "- Example query: SELECT p.id, p.name, u.id AS owner_user_id, u.first_name, u.last_name, u.email FROM project p LEFT JOIN [user] u ON p.owner_id = u.id OR p.owner_email = u.email WHERE p.name LIKE '%10103-02 Luna%'\n\n"
            
            
            "CLIENT MATCHING RULES (CRITICAL for client queries):\\n"
            "- The client table has two distinct columns with different purposes:\\n"
            "  * client.name: Contains the FULL client name (e.g., '10102-01 Max Matthiessen') - THIS IS THE PRIMARY IDENTIFIER\\n"
            "  * client.client_number: Contains ONLY the numeric code (e.g., '10102-01') - Use only for explicit 'client number' queries\\n"
            "- When user refers to a client by name (even if it contains numbers), ALWAYS match against client.name using LIKE, NOT client.client_number\\n"
            "- Example CORRECT: WHERE client.name LIKE '%10102-01 Max Matthiessen%' (for query 'projects under client 10102-01 Max Matthiessen')\\n"
            "- Example INCORRECT: WHERE client.client_number = '10102-01' (this would miss the full client name context)\\n"
            "- ONLY use client.client_number when user explicitly asks for 'client number', 'client code', or similar numeric identifier queries\\n"
            "- When joining project to client, use: project.client_id = client.id, then filter on client.name\\n\\n"
            
            "STRICT RULES:\\n"
            "- **NEVER use CAST, TRY_CAST, CONVERT, or any type conversion functions.** Join columns must have compatible types naturally.\n"
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