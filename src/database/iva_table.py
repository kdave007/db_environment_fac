"""
Implementation of the iva table using the TableSimpleBlueprint class.
"""
from .table_simple_blueprint import TableSimpleBlueprint


class IvaTable(TableSimpleBlueprint):
    """Handler for the 'iva' table."""
    
    def __init__(self, db_connection):
        """Initialize the iva table handler."""
        super().__init__(db_connection, "iva")
    
    def get_create_query(self):
        """
        Get the SQL query to create the iva table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS iva (
            velneo CHAR(1) PRIMARY KEY,
            tipo VARCHAR(20) NOT NULL,
            porcentaje NUMERIC(5,2) NOT NULL,
            pvsi VARCHAR(10)
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the iva table.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO iva (velneo, tipo, porcentaje, pvsi) 
        VALUES (%s, %s, %s, %s)
        """
    
    def insert_default_data(self):
        """
        Insert the default data into the iva table.
        
        Returns:
            bool: Success status
        """
        data = [
            ('G', 'general', 16.00, '7'),
            ('S', 'super_reducido', 4.00, None),
            ('R', 'reducido', 10.00, None),
            ('X', 'exento', 0.00, 'E'),
            ('E', 'especial', 2.00, None)
        ]
        
        return self.insert_data(data)
    
    def setup(self, use_default_data=True):
        """
        Set up the iva table (create and populate with default data if requested).
        
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
