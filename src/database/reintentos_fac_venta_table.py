"""
Implementation of the reintentos_fac_venta table using the TableSimpleBlueprint class.
"""
from .table_simple_blueprint import TableSimpleBlueprint


class ReintentosFacturaVentaTable(TableSimpleBlueprint):
    """Handler for the 'reintentos_fac_venta' table."""
    
    def __init__(self, db_connection):
        """Initialize the reintentos_fac_venta table handler."""
        super().__init__(db_connection, "reintentos_fac_venta")
    
    def get_create_query(self):
        """
        Get the SQL query to create the reintentos_fac_venta table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS reintentos_fac_venta (
            folio VARCHAR PRIMARY KEY NOT NULL,
            intentos INTEGER NOT NULL DEFAULT 0,
            fecha_del_registro TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            completado BOOLEAN DEFAULT FALSE
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the reintentos_fac_venta table.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO reintentos_fac_venta (folio, intentos, fecha_del_registro, completado) 
        VALUES (%s, %s, %s, %s)
        """
    
    def insert_default_data(self):
        """
        Insert default data into the reintentos_fac_venta table.
        
        Returns:
            bool: True if successful, False otherwise
        """
        # No default data for this table
        print("No default data defined for reintentos_fac_venta table")
        return True
        
    def setup(self, use_default_data=False):
        """
        Set up the reintentos_fac_venta table (create only, no default data).
        
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
