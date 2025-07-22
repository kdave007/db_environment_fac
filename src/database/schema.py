"""
Database schema module.
Defines and creates the database tables using provided SQL queries.
"""
import os
import glob
from pathlib import Path
from .connection import DatabaseConnection


class SchemaManager:
    """Manages PostgreSQL database schema creation and updates."""
    
    def __init__(self, db_connection):
        """
        Initialize schema manager.
        
        Args:
            db_connection (DatabaseConnection): Database connection instance
        """
        self.db = db_connection
    
    def create_tables(self, sql_directory=None):
        """
        Create all required tables in the database using SQL files or provided queries.
        
        Args:
            sql_directory (str, optional): Directory containing SQL files with table creation queries
            
        Returns:
            bool: Success status
        """
        success = True
        
        if sql_directory and os.path.exists(sql_directory):
            # Execute SQL files in the specified directory
            success = self.execute_sql_files(sql_directory)
        else:
            # Execute predefined table creation queries
            tables_queries = self.get_table_creation_queries()
            for table_name, query in tables_queries.items():
                print(f"Creating table: {table_name}...")
                if not self.execute_query(query):
                    print(f"Failed to create table: {table_name}")
                    success = False
        
        return success
    
    def execute_sql_files(self, sql_directory):
        """
        Execute all SQL files in the specified directory.
        
        Args:
            sql_directory (str): Directory containing SQL files
            
        Returns:
            bool: Success status
        """
        success = True
        sql_files = glob.glob(os.path.join(sql_directory, "*.sql"))
        
        # Sort files to ensure tables with dependencies are created after their dependencies
        sql_files.sort()
        
        for sql_file in sql_files:
            print(f"Executing SQL file: {os.path.basename(sql_file)}...")
            with open(sql_file, 'r') as f:
                sql_content = f.read()
                
            if not self.execute_query(sql_content):
                print(f"Failed to execute SQL file: {os.path.basename(sql_file)}")
                success = False
        
        return success
    
    def execute_query(self, query):
        """
        Execute a single SQL query.
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            bool: Success status
        """
        cursor = self.db.execute_query(query)
        self.db.commit()
        return cursor is not None
    
    def get_table_creation_queries(self):
        """
        Get predefined table creation queries.
        Replace these with your actual table creation queries.
        
        Returns:
            dict: Dictionary mapping table names to their creation queries
        """
        # This is a placeholder - replace with your actual table creation queries
        return {
            "table1": """
            CREATE TABLE IF NOT EXISTS table1 (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "table2": """
            CREATE TABLE IF NOT EXISTS table2 (
                id SERIAL PRIMARY KEY,
                table1_id INTEGER REFERENCES table1(id),
                value VARCHAR(255) NOT NULL
            )
            """
            # Add more table creation queries as needed
        }
    
    def drop_tables(self, tables=None):
        """
        Drop specified tables from the database.
        Use with caution!
        
        Args:
            tables (list, optional): List of table names to drop.
                                    If None, drops all tables from get_table_creation_queries.
        
        Returns:
            bool: Success status
        """
        if tables is None:
            # Get table names from predefined queries
            tables = list(self.get_table_creation_queries().keys())
        
        # Reverse the order to handle dependencies correctly
        tables.reverse()
        
        success = True
        for table in tables:
            query = f"DROP TABLE IF EXISTS {table} CASCADE"
            print(f"Dropping table: {table}...")
            cursor = self.db.execute_query(query)
            self.db.commit()
            if cursor is None:
                success = False
        
        return success
