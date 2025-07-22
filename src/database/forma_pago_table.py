"""
Implementation of the forma_pago table using the TableSimpleBlueprint class.
"""
from .table_simple_blueprint import TableSimpleBlueprint


class FormaPagoTable(TableSimpleBlueprint):
    """Handler for the 'forma_pago' table."""
    
    def __init__(self, db_connection):
        """Initialize the forma_pago table handler."""
        super().__init__(db_connection, "forma_pago")
    
    def get_create_query(self):
        """
        Get the SQL query to create the forma_pago table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS forma_pago (
            id_velneo INT,
            pvsi VARCHAR(20),
            nombre VARCHAR(100)
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the forma_pago table.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO forma_pago (id_velneo, pvsi, nombre) 
        VALUES (%s, %s, %s)
        """
    
    def insert_default_data(self):
        """
        Insert the default data into the forma_pago table.
        
        Returns:
            bool: Success status
        """
        data = [
            (1, 'EF', 'Efectivo'),
            (2, 'CH', 'Cheque'),
            (3, 'TC', 'Tarjeta Credito'),
            (4, 'D', 'Deposito'),
            (5, 'ET', 'Transferencia'),
            (6, 'TD', 'Tarjeta Debito')
        ]
        
        return self.insert_data(data)
    
    def setup(self, use_default_data=True):
        """
        Set up the forma_pago table (create and populate with default data if requested).
        
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
