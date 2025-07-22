"""
Implementation of the proveedor table using the TableSimpleBlueprint class.
"""
from .table_simple_blueprint import TableSimpleBlueprint


class ProveedorTable(TableSimpleBlueprint):
    """Handler for the 'proveedor' table."""
    
    def __init__(self, db_connection):
        """Initialize the proveedor table handler."""
        super().__init__(db_connection, "proveedor")
    
    def get_create_query(self):
        """
        Get the SQL query to create the proveedor table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS proveedor(
            id_velneo VARCHAR(10),
            id_pvsi VARCHAR(10) NOT NULL PRIMARY KEY,
            nombre VARCHAR(50)
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the proveedor table.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO proveedor (id_velneo, id_pvsi, nombre) 
        VALUES (%s, %s, %s)
        """
    
    def insert_default_data(self):
        """
        Insert the default data into the proveedor table.
        
        Returns:
            bool: Success status
        """
        data = [
            ('1', 'DIK', ' Ejemplo')
        ]
        
        return self.insert_data(data)
    
    def setup(self, use_default_data=True):
        """
        Set up the proveedor table (create and populate with default data if requested).
        
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
