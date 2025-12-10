"""User Authorization Service for access control."""
from typing import Optional, List, Dict, Any
from database.connection import db_connection
from utils.logger import setup_logger

logger = setup_logger(__name__)


class UserAuthService:
    """Service for managing user permissions and access control."""
    
    ADMIN_GROUP_UUID = "37c57028-06a0-4cfa-b285-55c5b6a0bfec"
    
    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Fetch user details by email (case-insensitive).
        
        Args:
            email: User's email address
            
        Returns:
            User dictionary with id, email, groups, etc. or None if not found
        """
        try:
            # Case-insensitive email comparison using LOWER()
            # %s is a parameterized query placeholder for pymssql (prevents SQL injection)
            query = "SELECT id, email, first_name, last_name, groups FROM [user] WHERE LOWER(email) = LOWER(%s)"
            
            with db_connection.get_cursor() as cursor:
                cursor.execute(query, (email,))
                result = cursor.fetchone()
                
                if not result:
                    logger.warning(f"User not found for email: {email}")
                    return None
                
                # Build dict from cursor.description
                columns = [col[0] for col in cursor.description]
                user_dict = dict(zip(columns, result))
                
                logger.info(f"Found user: {user_dict.get('email')} (ID: {user_dict.get('id')})")
                return user_dict
                
        except Exception as e:
            logger.error(f"Error fetching user by email {email}: {str(e)}")
            return None
    
    def is_admin(self, groups: Optional[str]) -> bool:
        """
        Check if user is an admin based on groups field.
        
        Args:
            groups: The groups field from user table (NVARCHAR(MAX))
            
        Returns:
            True if user has admin group UUID, False otherwise
        """
        if not groups:
            return False
        
        is_admin_user = self.ADMIN_GROUP_UUID in groups
        logger.info(f"Admin check: {is_admin_user} (groups contains admin UUID)")
        return is_admin_user
    
    def get_accessible_project_ids(self, user_id: str) -> List[str]:
        """
        Get list of project IDs that the user has access to.
        
        This includes:
        1. Projects where user is in project_team
        2. Projects where user is the owner
        
        Args:
            user_id: User's ID (UUID/NVARCHAR string)
            
        Returns:
            List of accessible project IDs (as strings)
        """
        try:
            user_id_str = str(user_id)
            
            # DEBUG: Check project_team first
            debug_pt_query = "SELECT project_id FROM project_team WHERE user_id = %s"
            with db_connection.get_cursor() as cursor:
                cursor.execute(debug_pt_query, (user_id_str,))
                pt_results = cursor.fetchall()
                pt_project_ids = [str(row[0]) for row in pt_results if row[0]]
                logger.info(f"DEBUG: project_team returned {len(pt_project_ids)} projects: {pt_project_ids}")
            
            # DEBUG: Check project ownership
            debug_owner_query = "SELECT id FROM project WHERE owner_id = %s"
            with db_connection.get_cursor() as cursor:
                cursor.execute(debug_owner_query, (user_id_str,))
                owner_results = cursor.fetchall()
                owner_project_ids = [str(row[0]) for row in owner_results if row[0]]
                logger.info(f"DEBUG: project ownership returned {len(owner_project_ids)} projects: {owner_project_ids}")
            
            # Main query with UNION
            query = """
            SELECT DISTINCT project_id
            FROM (
                -- Projects from project_team
                SELECT project_id
                FROM project_team
                WHERE user_id = %s
                
                UNION
                
                -- Projects where user is the owner
                SELECT id AS project_id
                FROM project
                WHERE owner_id = %s
            ) AS accessible_projects
            """
            
            with db_connection.get_cursor() as cursor:
                cursor.execute(query, (user_id_str, user_id_str))
                results = cursor.fetchall()
                
                # Extract project IDs from results
                if results and isinstance(results[0], tuple):
                    project_ids = [str(row[0]) for row in results if row[0] is not None]
                else:
                    # Fallback if results are dicts
                    columns = [col[0] for col in cursor.description] if cursor.description else []
                    if columns:
                        key = columns[0]
                        project_ids = [str(row[key]) if isinstance(row, dict) else str(row[0]) 
                                     for row in results if row]
                    else:
                        project_ids = []
                
                logger.info(f"User {user_id} FINAL access to {len(project_ids)} projects: {project_ids}")
                return project_ids
                
        except Exception as e:
            logger.error(f"Error fetching accessible projects for user {user_id}: {str(e)}")
            return []
    
    def get_user_authorization(self, email: str) -> Dict[str, Any]:
        """
        Get complete user authorization info including admin status and accessible projects.
        
        Args:
            email: User's email address
            
        Returns:
            Dictionary with user_id, is_admin, accessible_project_ids
        """
        user = self.get_user_by_email(email)
        
        if not user:
            logger.warning(f"User authorization failed: User not found for email {email}")
            return {
                "user_id": None,
                "is_admin": False,
                "accessible_project_ids": [],
                "error": "User not found"
            }
        
        user_id = user.get("id")
        groups = user.get("groups")
        is_admin_user = self.is_admin(groups)
        
        # If admin, return empty list (means all projects accessible)
        # If non-admin, get specific project IDs
        if is_admin_user:
            accessible_project_ids = []
            logger.info(f"User {email} is ADMIN - has access to ALL projects")
        else:
            accessible_project_ids = self.get_accessible_project_ids(user_id)
            logger.info(f"User {email} is NON-ADMIN - has access to {len(accessible_project_ids)} projects")
        
        return {
            "user_id": user_id,
            "is_admin": is_admin_user,
            "accessible_project_ids": accessible_project_ids,
            "error": None
        }


# Global instance
user_auth_service = UserAuthService()
