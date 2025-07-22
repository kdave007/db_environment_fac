"""
Implementation of the errores table using the TableSimpleBlueprint class.
"""
from .table_simple_blueprint import TableSimpleBlueprint


class ErroresTable(TableSimpleBlueprint):
    """Handler for the 'errores' table."""
    
    def __init__(self, db_connection):
        """Initialize the errores table handler."""
        super().__init__(db_connection, "errores")
    
    def get_create_query(self):
        """
        Get the SQL query to create the errores table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS errores (
            id SERIAL,
            fecha TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            descripcion TEXT,
            clase VARCHAR(100)
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the errores table.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO errores (descripcion, clase) 
        VALUES (%s, %s)
        """
    
    def insert_default_data(self):
        """
        Insert default data into the errores table.
        
        Returns:
            bool: True if successful, False otherwise
        """
        # No default data for this table
        print("No default data defined for errores table")
        return True
        
    def setup(self, use_default_data=False):
        """
        Set up the errores table (create only, no default data).
        
        Args:
            use_default_data (bool, optional): Not used for this table
            
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
