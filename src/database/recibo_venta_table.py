"""
Implementation of the recibo_venta table using the TableSimpleBlueprint class.
"""
from .table_simple_blueprint import TableSimpleBlueprint


class ReciboVentaTable(TableSimpleBlueprint):
    """Handler for the 'recibo_venta' table."""
    
    def __init__(self, db_connection):
        """Initialize the recibo_venta table handler."""
        super().__init__(db_connection, "recibo_venta")
    
    def get_create_query(self):
        """
        Get the SQL query to create the recibo_venta table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS recibo_venta (
            id_sql SERIAL,
            folio VARCHAR,
            num_ref INTEGER,
            respuesta VARCHAR, 
            hash VARCHAR(64),
            estado VARCHAR(20),
            fecha_emision DATE,
            fecha_procesamiento TIMESTAMP,
            indice NUMERIC
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the recibo_venta table.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO recibo_venta (folio, num_ref, dtl_cob_apl_t, dtl_doc_cob_t, cta_cor_t, rbo_cob_t, hash, estado, fecha_emision, fecha_procesamiento) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    
    def insert_default_data(self):
        """
        Insert default data into the recibo_venta table.
        
        Returns:
            bool: True if successful, False otherwise
        """
        # No default data for this table
        print("No default data defined for recibo_venta table")
        return True
        
    def setup(self, use_default_data=False):
        """
        Set up the recibo_venta table (create only, no default data).
        
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
