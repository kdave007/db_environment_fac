"""
Base class for database table handlers.
"""
import os
import csv


class TableBase:
    """Base class for all table handlers."""
    
    def __init__(self, db_connection, table_name):
        """
        Initialize the table handler.
        
        Args:
            db_connection: Database connection instance
            table_name (str): Name of the table
        """
        self.db = db_connection
        self.table_name = table_name
    
    def create_table(self):
        """
        Create the table using the create query.
        
        Returns:
            bool: Success status
        """
        query = self.get_create_query()
        if not query:
            print(f"No creation query defined for table '{self.table_name}'")
            return False
            
        cursor = self.db.execute_query(query)
        self.db.commit()
        success = cursor is not None
        
        if success:
            print(f"Table '{self.table_name}' created successfully")
        else:
            print(f"Failed to create table '{self.table_name}'")
            
        return success
    
    def get_create_query(self):
        """
        Get the SQL query to create the table.
        Must be implemented by subclasses.
        
        Returns:
            str: SQL query to create the table
        """
        raise NotImplementedError("Subclasses must implement get_create_query()")
    
    def setup(self):
        """
        Set up the table (create and populate if needed).
        Must be implemented by subclasses.
        
        Returns:
            bool: Success status
        """
        raise NotImplementedError("Subclasses must implement setup()")
