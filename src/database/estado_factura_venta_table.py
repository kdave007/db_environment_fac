"""
Implementation of the estado_factura_venta table using the TableSimpleBlueprint class.
"""
from .table_simple_blueprint import TableSimpleBlueprint


class EstadoFacturaVentaTable(TableSimpleBlueprint):
    """Handler for the 'estado_factura_venta' table."""
    
    def __init__(self, db_connection):
        """Initialize the estado_factura_venta table handler."""
        super().__init__(db_connection, "estado_factura_venta")
    
    def get_create_query(self):
        """
        Get the SQL query to create the estado_factura_venta table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS estado_factura_venta (
            id NUMERIC PRIMARY KEY NOT NULL,
            folio VARCHAR,
            total_partidas NUMERIC DEFAULT 0,
            hash VARCHAR,
            estado VARCHAR NOT NULL,
            fecha_emision DATE NOT NULL,
            fecha_procesamiento DATE NOT NULL,
            accion VARCHAR
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the estado_factura_venta table.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO estado_factura_venta (id, folio, total_partidas, hash, estado, fecha_emision, fecha_proceso, accion) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
    
    def insert_default_data(self):
        """
        Insert default data into the estado_factura_venta table.
        
        Returns:
            bool: True if successful, False otherwise
        """
        # No default data for this table
        print("No default data defined for estado_factura_venta table")
        return True
        
    def setup(self, use_default_data=False):
        """
        Set up the estado_factura_venta table (create only, no default data).
        
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
