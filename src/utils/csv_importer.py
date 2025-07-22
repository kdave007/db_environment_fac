"""
CSV data importer module.
Handles reading and importing data from CSV files into database tables.
"""
import csv
import os
from pathlib import Path


class CSVImporter:
    """CSV data importer for database tables."""
    
    def __init__(self, db_connection):
        """
        Initialize CSV importer.
        
        Args:
            db_connection: Database connection instance
        """
        self.db = db_connection
    
    def import_csv_to_table(self, csv_file, table_name, columns=None, delimiter=',', 
                           batch_size=1000, skip_header=True):
        """
        Import data from a CSV file into a database table.
        
        Args:
            csv_file (str): Path to the CSV file
            table_name (str): Name of the target database table
            columns (list, optional): List of column names to import. 
                                     If None, all columns from CSV are used.
            delimiter (str, optional): CSV delimiter character
            batch_size (int, optional): Number of records to insert in each batch
            skip_header (bool, optional): Whether to skip the header row
            
        Returns:
            tuple: (success, rows_imported)
        """
        if not os.path.exists(csv_file):
            print(f"CSV file not found: {csv_file}")
            return False, 0
        
        try:
            with open(csv_file, 'r', newline='', encoding='utf-8') as f:
                csv_reader = csv.reader(f, delimiter=delimiter)
                
                # Skip header row if required
                if skip_header:
                    header_row = next(csv_reader)
                    if columns is None:
                        columns = header_row
                
                # Prepare the SQL query for insertion
                placeholders = ', '.join(['%s'] * len(columns))
                column_names = ', '.join([f'"{col}"' for col in columns])
                query = f'INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})'
                
                # Process data in batches
                batch_data = []
                rows_imported = 0
                
                for row in csv_reader:
                    if len(row) >= len(columns):
                        # Extract only the columns we want
                        row_data = row[:len(columns)]
                        batch_data.append(row_data)
                        
                        # Insert batch when it reaches the specified size
                        if len(batch_data) >= batch_size:
                            success = self.db.execute_many(query, batch_data)
                            if not success:
                                print(f"Error inserting batch at row {rows_imported + 1}")
                                return False, rows_imported
                            
                            rows_imported += len(batch_data)
                            print(f"Imported {rows_imported} rows...")
                            batch_data = []
                
                # Insert any remaining data
                if batch_data:
                    success = self.db.execute_many(query, batch_data)
                    if not success:
                        print(f"Error inserting final batch")
                        return False, rows_imported
                    
                    rows_imported += len(batch_data)
                
                # Commit the transaction
                self.db.commit()
                print(f"Successfully imported {rows_imported} rows into table {table_name}")
                return True, rows_imported
                
        except Exception as e:
            print(f"Error importing CSV data: {e}")
            return False, 0
    
    def validate_csv_format(self, csv_file, expected_columns=None, delimiter=','):
        """
        Validate that a CSV file has the expected format.
        
        Args:
            csv_file (str): Path to the CSV file
            expected_columns (list, optional): List of expected column names
            delimiter (str, optional): CSV delimiter character
            
        Returns:
            tuple: (is_valid, actual_columns)
        """
        if not os.path.exists(csv_file):
            print(f"CSV file not found: {csv_file}")
            return False, []
        
        try:
            with open(csv_file, 'r', newline='', encoding='utf-8') as f:
                csv_reader = csv.reader(f, delimiter=delimiter)
                header_row = next(csv_reader)
                
                if expected_columns:
                    # Check if all expected columns are present
                    missing_columns = [col for col in expected_columns if col not in header_row]
                    if missing_columns:
                        print(f"Missing columns in CSV: {missing_columns}")
                        return False, header_row
                
                # Count rows to validate data presence
                row_count = sum(1 for _ in csv_reader)
                if row_count == 0:
                    print("CSV file has no data rows")
                    return False, header_row
                
                return True, header_row
                
        except Exception as e:
            print(f"Error validating CSV file: {e}")
            return False, []
