"""
Example table implementation using the TableBlueprint class.
Copy this file for each new table and customize the queries.
"""
from .table_blueprint import TableBlueprint


class ExampleTable(TableBlueprint):
    """Example table implementation."""
    
    def __init__(self, db_connection):
        """Initialize the table handler."""
        super().__init__(db_connection, "example_table")
    
    def get_create_query(self):
        """
        Get the SQL query to create the example table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS example_table (
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
        INSERT INTO example_table (name, value, description) 
        VALUES (%s, %s, %s)
        """


# Usage example:
"""
# In your main.py or another module:

# Create and set up the table
example_table = ExampleTable(db_connection)
example_table.setup()

# Import from CSV
example_table.setup(csv_file='path/to/example_data.csv')

# Insert manual data
data = [
    ('Item 1', 100, 'Description 1'),
    ('Item 2', 200, 'Description 2'),
    ('Item 3', 300, 'Description 3')
]
example_table.setup(manual_data=data)
"""
