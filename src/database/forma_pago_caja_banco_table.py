"""
Implementation of the forma_pago_caja_banco table using the TableSimpleBlueprint class.
"""
from .table_simple_blueprint import TableSimpleBlueprint


class FormaPagoCajaBancoTable(TableSimpleBlueprint):
    """Handler for the 'forma_pago_caja_banco' table."""
    
    def __init__(self, db_connection):
        """Initialize the forma_pago_caja_banco table handler."""
        super().__init__(db_connection, "forma_pago_caja_banco")
    
    def get_create_query(self):
        """
        Get the SQL query to create the forma_pago_caja_banco table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS forma_pago_caja_banco (
            caja_banco VARCHAR,
            forma_pago VARCHAR
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the forma_pago_caja_banco table.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO forma_pago_caja_banco (caja_banco, forma_pago) 
        VALUES (%s, %s)
        """
    
    def insert_default_data(self):
        """
        Insert the default data into the forma_pago_caja_banco table.
        
        Returns:
            bool: Success status
        """
        data = [
            ('EF', '1'),
            ('PE', '1'),
            ('F1', '1'),
            ('P2', '1'),
            ('R+', '1'),
            ('R-', '1'),
            ('CI', '1'),
            ('MJ', '1'),
            ('AN', '1'),
            ('CH', '2'),
            ('EB', '4'),
            ('DD', '4'),
            ('SA', '4'),
            ('ET', '5'),
            ('TD', '6'),
            ('PP', '6'),
            ('BD', '6'),
            ('ND', '6'),
            ('MD', '6'),
            ('YD', '6'),
            ('LC', '6'),
            ('PM', '6'),
            ('FD', '6'),
            ('KD', '6'),
            ('default_value','3')
        ]
        
        return self.insert_data(data)
    
    def setup(self, use_default_data=True):
        """
        Set up the forma_pago_caja_banco table (create and populate with default data if requested).
        
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
