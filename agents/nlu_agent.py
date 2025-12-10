"""NLU Intent Agent for understanding user queries."""
import google.generativeai as genai
from typing import Dict, Any
from config.secrets import secrets_manager
from config.settings import settings
from graph.state import GraphState
from utils.logger import setup_logger

logger = setup_logger(__name__)

class NLUAgent:
    """Natural Language Understanding Agent."""
    
    def __init__(self):
        genai.configure(api_key=secrets_manager.get_gemini_api_key())
        self.model = genai.GenerativeModel(settings.GEMINI_MODEL)
    
    def analyze_intent(self, state: GraphState) -> GraphState:
        """Analyze user intent and extract entities."""
        user_query = state["user_query"]
        logger.info(f"Analyzing intent for query: {user_query}")
        
        prompt = f"""You are an expert NLU agent helping a Text-to-SQL system.
        Read the user's request and extract:
        1) Intent: high-level goal such as list/filter/aggregate/join/detail/count/top-n.
        2) Entities: important nouns/values (names, companies, cities, dates, ids, statuses, budget thresholds, etc.).
        3) Tables Likely Needed: from the known tables in this database. Use only exact table names you know.
        
        PRIMARY TABLE PRIORITY (CRITICAL - apply these rules first):
        - The PRIMARY tables for this system are: `project`, `contacts`, and `client`
        - For generic project queries (e.g., "what projects do we have?", "show me projects"), use the `project` table
        - For generic contact queries (e.g., "show me contacts", "list all contacts"), use the `contacts` table
        - For queries about accounts or companies (e.g., "what companies do we have?", "show me accounts"), use the `client` table
        - ONLY use Zoho tables (`zoho_deals`, `zoho_accounts`, `zoho_contacts`) when the user EXPLICITLY mentions "zoho" in their query
          Examples of explicit Zoho mentions: "projects in zoho", "zoho deals", "from zoho", "zoho accounts"
        
        DOMAIN MAPPINGS (apply these when identifying tables):
        - "projects" or "project" (without "zoho") → `project` table
        - "contacts" or "contact" (without "zoho") → `contacts` table  
        - "accounts" or "companies" or "company" or "clients" (without "zoho") → `client` table
        - "zoho projects" or "projects in zoho" → `zoho_deals` table
        - "zoho contacts" or "contacts in zoho" → `zoho_contacts` table
        - "zoho accounts" or "accounts in zoho" → `zoho_accounts` table
        
        NAME RESOLUTION & USER MATCHING (CRITICAL RULES for PERSON NAME queries):
        - If the user request mentions a person's name (e.g., 'James', 'Hazal', 'Leila', etc.), treat it as a reference to the `user` table.
        - Always match name using LIKE for both first and last name, even if the name appears to be complete.
        - ALWAYS use this exact matching pattern (case insensitive + partial match):
            WHERE user.first_name LIKE '%<name>%' OR user.last_name LIKE '%<name>%'
        - NEVER use '=' for name matching unless explicitly instructed.
        - Names should always be resolved BEFORE joining to related records such as projects, clients, tasks, etc.
        - Example: 'Show me all projects James is working on' MUST be translated into a query that joins through the user table and uses LIKE instead of '='.
        - If multiple users match the name, return all matching rows.
        - If the schema does not contain the required user table or name columns, return NO_SCHEMA_MATCH.
        - Include 'created_by' or 'owner' columns whenever asked about who is the owner of this project? or who owns this project?
        
        OWNER QUERIES (CRITICAL for project owner questions):
        - When user asks "who is the owner of [project]?" or "who owns [project]?", you MUST include the `user` table
        - Use project.owner_id and project.owner_email to map to the user table
        - The query should JOIN project with user table to get owner details (first_name, last_name, email)
        
        CLIENT COLUMN SEMANTICS (CRITICAL for client queries):
        - client.name: Full client name (e.g., "10102-01 Max Matthiessen") - USE THIS for client identification in queries
        - client.client_number: Short numeric identifier only (e.g., "10102-01") - ONLY use when user explicitly asks for "client number" or "client code"
        - When user mentions a client by name (even if it includes numbers), ALWAYS extract it as targeting the 'name' column, not 'client_number'
        - Example: "client 10102-01 Max Matthiessen" → entity should be client_name:"10102-01 Max Matthiessen"
        - Example: "projects under 10102-01 Max Matthiessen" → entity should be client_name:"10102-01 Max Matthiessen"
        
        Normalization rules (simple mappings you should apply before extracting entities):
        - Treat synonyms for status: 'live', 'running', 'ongoing' -> map to value 'Active' when a status-like column exists.
        - Treat phrases 'budget left', 'remaining budget' or 'how much budget is left' as a request to compute (budget - spent) or to use a column named 'budget_remaining' / 'budget_left' if available. When extracting an entity for this, include a key-value pair like 'budget_left:yes' or 'budget_left:<threshold>' if a number/threshold is present.

        User Query: {user_query}

        Respond exactly in this format (one per line):
        Intent: <intent>
        Entities: <comma-separated values or key:value pairs, e.g. status:Active, client_name:Acme, budget_left:yes>
        Tables Likely Needed: <comma-separated exact table names>
        """
        
        try:
            response = self.model.generate_content(prompt)
            result_text = response.text
            
            # Parse response
            intent = "unknown"
            entities = []
            tables = []
            
            for line in result_text.split('\n'):
                if line.startswith('Intent:'):
                    intent = line.replace('Intent:', '').strip()
                elif line.startswith('Entities:'):
                    entities_str = line.replace('Entities:', '').strip()
                    entities = [e.strip() for e in entities_str.split(',') if e.strip()]
                elif line.startswith('Tables Likely Needed:'):
                    tables_str = line.replace('Tables Likely Needed:', '').strip()
                    tables = [t.strip() for t in tables_str.split(',') if t.strip()]
            
            state["intent"] = intent
            state["entities"] = entities
            state["relevant_tables"] = tables
            state["step"] = "nlu_complete"
            
            logger.info(f"Intent: {intent}, Entities: {entities}, Tables: {tables}")
            
        except Exception as e:
            logger.error(f"NLU error: {str(e)}")
            state["error"] = f"Intent analysis failed: {str(e)}"
            state["step"] = "error"
        
        return state

nlu_agent = NLUAgent()