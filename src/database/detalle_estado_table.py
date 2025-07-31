"""
Implementation of the detalle_estado table using the TableSimpleBlueprint class.
"""
from .table_simple_blueprint import TableSimpleBlueprint


class DetalleEstadoTable(TableSimpleBlueprint):
    """Handler for the 'detalle_estado' table."""
    
    def __init__(self, db_connection):
        """Initialize the detalle_estado table handler."""
        super().__init__(db_connection, "detalle_estado")
    
    def get_create_query(self):
        """
        Get the SQL query to create the detalle_estado table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS detalle_estado(
            id NUMERIC NOT NULL PRIMARY KEY,
            folio VARCHAR,
            hash_detalle VARCHAR(64),
            fecha DATE,
            estado VARCHAR(50),
            accion VARCHAR(50),
            ref VARCHAR
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the detalle_estado table.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO detalle_estado (id, folio, hash_detalle, fecha, estado, accion, ref) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
    
    def insert_default_data(self):
        """
        Insert default data into the detalle_estado table.
        
        Returns:
            bool: True if successful, False otherwise
        """
        # No default data for this table
        print("No default data defined for detalle_estado table")
        return True
        
    def setup(self, use_default_data=False):
        """
        Set up the detalle_estado table (create only, no default data).
        
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
