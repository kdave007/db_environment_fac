"""
Simplified blueprint table class for easy replication.
This version is for tables that don't need CSV import functionality.
Copy this file for each new table and replace the CREATE and INSERT queries.
"""


class TableSimpleBlueprint:
    """
    Simplified blueprint class for database tables without CSV import.
    Copy this class for each new table and replace the queries.
    """
    
    def __init__(self, db_connection, table_name):
        """
        Initialize the table handler.
        
        Args:
            db_connection: Database connection instance
            table_name (str): Name of the table
        """
        self.db = db_connection
        self.table_name = table_name
    
    def get_create_query(self):
        """
        Get the SQL query to create the table.
        REPLACE THIS WITH YOUR ACTUAL CREATE TABLE QUERY.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS table_name (
            id SERIAL PRIMARY KEY,
            column1 VARCHAR(100) NOT NULL,
            column2 INTEGER,
            column3 TEXT
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the table.
        REPLACE THIS WITH YOUR ACTUAL INSERT QUERY.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO table_name (column1, column2, column3) 
        VALUES (%s, %s, %s)
        """
    
    def create_table(self):
        """
        Create the table using the create query.
        
        Returns:
            bool: Success status
        """
        query = self.get_create_query()
        if not query:
            print(f"No creation query defined for table '{self.table_name}'")
            return False
            
        cursor = self.db.execute_query(query)
        self.db.commit()
        success = cursor is not None
        
        if success:
            print(f"Table '{self.table_name}' created successfully")
        else:
            print(f"Failed to create table '{self.table_name}'")
            
        return success
    
    def insert_data(self, data_list):
        """
        Insert data into the table.
        
        Args:
            data_list (list): List of data tuples to insert
            
        Returns:
            bool: Success status
        """
        if not data_list:
            print("No data provided for insertion")
            return False
        
        try:
            query = self.get_insert_query()
            success = self.db.execute_many(query, data_list)
            
            if success:
                self.db.commit()
                print(f"Successfully inserted {len(data_list)} rows into table '{self.table_name}'")
            else:
                print(f"Failed to insert data into table '{self.table_name}'")
                
            return success
            
        except Exception as e:
            print(f"Error inserting data into {self.table_name}: {e}")
            return False
    
    def setup(self, default_data=None):
        """
        Set up the table (create and populate if data provided).
        
        Args:
            default_data (list, optional): List of data tuples to insert
            
        Returns:
            bool: Success status
        """
        # Create the table
        if not self.create_table():
            return False
        
        # Insert default data if provided
        if default_data:
            return self.insert_data(default_data)
        
        return True
