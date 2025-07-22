"""
Implementation of the tipo_movimiento table using the TableSimpleBlueprint class.
"""
from .table_simple_blueprint import TableSimpleBlueprint


class TipoMovimientoTable(TableSimpleBlueprint):
    """Handler for the 'tipo_movimiento' table."""
    
    def __init__(self, db_connection):
        """Initialize the tipo_movimiento table handler."""
        super().__init__(db_connection, "tipo_movimiento")
    
    def get_create_query(self):
        """
        Get the SQL query to create the tipo_movimiento table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS tipo_movimiento (
            velneo CHAR(1) PRIMARY KEY,
            pvsi VARCHAR(10),
            descripcion VARCHAR(50) NOT NULL
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the tipo_movimiento table.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO tipo_movimiento (velneo, pvsi, descripcion) 
        VALUES (%s, %s, %s)
        """
    
    def insert_default_data(self):
        """
        Insert the default data into the tipo_movimiento table.
        
        Returns:
            bool: Success status
        """
        data = [
            ('C', 'E', 'Entrada'),
            ('V', 'V', 'Salida'),
            ('Y', 'I', 'Inventario'),
            ('Z', 'R', 'Regularización')
        ]
        
        return self.insert_data(data)
    
    def setup(self, use_default_data=True):
        """
        Set up the tipo_movimiento table (create and populate with default data if requested).
        
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
