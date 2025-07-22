"""
Example table implementation using the TableSimpleBlueprint class.
Copy this file for each new table and customize the queries.
"""
from .table_simple_blueprint import TableSimpleBlueprint


class ExampleSimpleTable(TableSimpleBlueprint):
    """Example table implementation without CSV import functionality."""
    
    def __init__(self, db_connection):
        """Initialize the table handler."""
        super().__init__(db_connection, "example_simple_table")
    
    def get_create_query(self):
        """
        Get the SQL query to create the example table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS example_simple_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            value INTEGER,
            description TEXT
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the example table.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO example_simple_table (name, value, description) 
        VALUES (%s, %s, %s)
        """
    
    def insert_default_data(self):
        """
        Insert default data into the table.
        
        Returns:
            bool: Success status
        """
        data = [
            ('Item 1', 100, 'Description 1'),
            ('Item 2', 200, 'Description 2'),
            ('Item 3', 300, 'Description 3')
        ]
        
        return self.insert_data(data)
    
    def setup(self, use_default_data=True):
        """
        Set up the table (create and populate with default data if requested).
        
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


# Usage example:
"""
# In your main.py or another module:

# Create and set up the table
example_table = ExampleSimpleTable(db_connection)
example_table.setup()  # Creates table and inserts default data

# Or create table without default data
example_table.setup(use_default_data=False)  # Only creates table

# Insert custom data
data = [
    ('Custom Item 1', 150, 'Custom Description 1'),
    ('Custom Item 2', 250, 'Custom Description 2')
]
example_table.insert_data(data)
"""
