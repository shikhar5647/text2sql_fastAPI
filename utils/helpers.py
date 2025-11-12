"""Helper utility functions."""
import re
from typing import List, Dict, Any
import sqlparse

def sanitize_sql(sql: str) -> str:
    """Sanitize and format SQL query."""
    # Remove comments
    sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    
    # Format SQL
    formatted = sqlparse.format(
        sql,
        reindent=True,
        keyword_case='upper'
    )
    
    return formatted.strip()

def is_safe_query(sql: str) -> tuple[bool, str]:
    """Check if SQL query is safe (no destructive operations)."""
    sql_upper = sql.upper()
    
    # List of dangerous keywords
    dangerous_keywords = [
        'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 
        'CREATE', 'INSERT', 'UPDATE', 'EXEC',
        'EXECUTE', 'GRANT', 'REVOKE'
    ]
    
    for keyword in dangerous_keywords:
        if re.search(r'\b' + keyword + r'\b', sql_upper):
            return False, f"Query contains dangerous keyword: {keyword}"
    
    # Check for multiple statements
    if ';' in sql.strip().rstrip(';'):
        return False, "Multiple SQL statements not allowed"
    
    # Must be a SELECT statement
    if not sql_upper.strip().startswith('SELECT'):
        return False, "Only SELECT queries are allowed"
    
    return True, "Query is safe"

def extract_tables_from_query(sql: str) -> List[str]:
    """Extract table names from SQL query."""
    # Parse SQL
    parsed = sqlparse.parse(sql)[0]
    tables = []
    
    from_seen = False
    for token in parsed.tokens:
        if from_seen:
            if isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    tables.append(str(identifier.get_name()))
            elif isinstance(token, sqlparse.sql.Identifier):
                tables.append(str(token.get_name()))
            elif token.ttype is None:
                tables.append(str(token).strip())
        
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
            from_seen = True
    
    return [t.strip('[]') for t in tables if t.strip()]

