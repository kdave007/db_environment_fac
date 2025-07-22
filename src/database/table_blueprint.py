"""
Blueprint table class for easy replication.
Copy this file for each new table and replace the CREATE and INSERT queries.
"""
import os
import csv


class TableBlueprint:
    """
    Blueprint class for database tables.
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
    
    def import_from_csv(self, csv_file, batch_size=1000, delimiter=','):
        """
        Import data from CSV file into the table.
        
        Args:
            csv_file (str): Path to the CSV file
            batch_size (int, optional): Number of records to insert in each batch
            delimiter (str, optional): CSV delimiter character
            
        Returns:
            tuple: (success, rows_imported)
        """
        if not os.path.exists(csv_file):
            print(f"CSV file not found: {csv_file}")
            return False, 0
        
        try:
            with open(csv_file, 'r', newline='', encoding='utf-8') as f:
                csv_reader = csv.reader(f, delimiter=delimiter)
                
                # Skip header row
                next(csv_reader, None)
                
                # Get the insert query
                query = self.get_insert_query()
                
                # Process data in batches
                batch_data = []
                rows_imported = 0
                
                for row in csv_reader:
                    # CUSTOMIZE THIS SECTION FOR YOUR DATA STRUCTURE
                    # This example assumes 3 columns in the CSV matching the insert query
                    if len(row) >= 3:
                        try:
                            # Process row data as needed (e.g., type conversion)
                            col1 = row[0]
                            col2 = int(row[1]) if row[1] else None
                            col3 = row[2]
                            
                            batch_data.append((col1, col2, col3))
                            
                            # Insert batch when it reaches the specified size
                            if len(batch_data) >= batch_size:
                                success = self.db.execute_many(query, batch_data)
                                if not success:
                                    print(f"Error inserting batch at row {rows_imported + 1}")
                                    return False, rows_imported
                                
                                rows_imported += len(batch_data)
                                print(f"Imported {rows_imported} rows to {self.table_name}...")
                                batch_data = []
                        except (ValueError, IndexError) as e:
                            print(f"Warning: Invalid data in row: {row}. Error: {e}. Skipping.")
                
                # Insert any remaining data
                if batch_data:
                    success = self.db.execute_many(query, batch_data)
                    if not success:
                        print(f"Error inserting final batch")
                        return False, rows_imported
                    
                    rows_imported += len(batch_data)
                
                # Commit the transaction
                self.db.commit()
                print(f"Successfully imported {rows_imported} rows into table '{self.table_name}'")
                return True, rows_imported
                
        except Exception as e:
            print(f"Error importing CSV data to {self.table_name}: {e}")
            return False, 0
    
    def insert_manual_data(self, data_list):
        """
        Insert data manually (not from CSV).
        
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
    
    def setup(self, csv_file=None, manual_data=None):
        """
        Set up the table (create and populate if data provided).
        
        Args:
            csv_file (str, optional): Path to CSV file for data import
            manual_data (list, optional): List of data tuples to insert manually
            
        Returns:
            bool: Success status
        """
        # Create the table
        if not self.create_table():
            return False
        
        success = True
        
        # Import data from CSV if provided
        if csv_file:
            csv_success, _ = self.import_from_csv(csv_file)
            if not csv_success:
                success = False
        
        # Insert manual data if provided
        if manual_data:
            manual_success = self.insert_manual_data(manual_data)
            if not manual_success:
                success = False
        
        return success
