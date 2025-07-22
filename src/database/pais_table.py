"""
Implementation of the pais table using the TableSimpleBlueprint class.
"""
from .table_simple_blueprint import TableSimpleBlueprint


class PaisTable(TableSimpleBlueprint):
    """Handler for the 'pais' table."""
    
    def __init__(self, db_connection):
        """Initialize the pais table handler."""
        super().__init__(db_connection, "pais")
    
    def get_create_query(self):
        """
        Get the SQL query to create the pais table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS pais (
            id INT PRIMARY KEY,
            description VARCHAR(50)
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the pais table.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO pais (id, description) 
        VALUES (%s, %s)
        """
    
    def insert_default_data(self):
        """
        Insert the default data into the pais table.
        
        Returns:
            bool: Success status
        """
        data = [
            (2, 'México'),
            (3, 'Nicaragua'),
            (4, 'Guatemala')
        ]
        
        return self.insert_data(data)
    
    def setup(self, use_default_data=True):
        """
        Set up the pais table (create and populate with default data if requested).
        
        Args:
            use_default_data (bool, optional): Whether to use the default data
            
        Returns:
            bool: Success status
        """
        # Create the table
        if not self.create_table():
            return False
        
        # Insert default data if requested
        if use_default_data:
            return self.insert_default_data()
        
        return True
