"""
Implementation of the clientes table using the TableSimpleBlueprint class.
"""
from .table_simple_blueprint import TableSimpleBlueprint


class ClientesTable(TableSimpleBlueprint):
    """Handler for the 'clientes' table."""
    
    def __init__(self, db_connection):
        """Initialize the clientes table handler."""
        super().__init__(db_connection, "clientes")
    
    def get_create_query(self):
        """
        Get the SQL query to create the clientes table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS clientes (
            velneo INT ,
            pvsi_clave VARCHAR(10),
            descripcion VARCHAR(100)
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the clientes table.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO clientes (velneo, pvsi_clave, descripcion) 
        VALUES (%s, %s, %s)
        """
    
    def insert_default_data(self):
        """
        Insert the default data into the clientes table.
        
        Returns:
            bool: Success status
        """
        data = [
            (1, 'VTPUB', 'Venta publico')
        ]
        
        return self.insert_data(data)
    
    def setup(self, use_default_data=True):
        """
        Set up the clientes table (create and populate with default data if requested).
        
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
