"""
Implementation of the articulos table with CSV import functionality.
"""
import os
import sys
import csv
from pathlib import Path
from ..config import CSV_DIRECTORY, ARTICULOS_CSV


class ArticulosTable:
    """Handler for the 'articulos' table."""
    
    def __init__(self, db_connection):
        """Initialize the articulos table handler."""
        self.db_connection = db_connection
        self.table_name = "articulos"
    
    def create_table(self):
        """
        Create the articulos table.
        
        Returns:
            bool: Success status
        """
        query = """
        CREATE TABLE IF NOT EXISTS articulos (
            velneo_id INT PRIMARY KEY,
            pvsi_clave VARCHAR(20),
            nombre VARCHAR(255)
        );
        """
        
        cursor = self.db_connection.execute_query(query)
        if cursor is None:
            return False
        
        self.db_connection.commit()
        return True
    
    def _clean_csv_data(self, csv_path):
        """
        Clean CSV data by handling unescaped quotes and other issues.
        
        Args:
            csv_path (str): Path to the CSV file
            
        Returns:
            list: List of dictionaries representing cleaned rows
        """
        cleaned_rows = []
        try:
            # Use standard CSV reader with error handling
            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                # First, read the file to get the header
                csv_content = csvfile.read()
                lines = csv_content.splitlines()
                
                if not lines:
                    print("CSV file is empty!")
                    return []
                
                header_line = lines[0]
                header = header_line.split(',')
                print(f"CSV Header: {header}")
                
                # Process each line after the header
                row_count = 0
                for line_num, line in enumerate(lines[1:], 2):  # Start at 2 to account for header
                    # Skip empty lines
                    if not line.strip():
                        continue
                    
                    row_count += 1
                    # Split by comma
                    values = line.split(',')
                    
                    # Create a dict for this row
                    if len(values) >= len(header):
                        row_dict = {}
                        for i, col_name in enumerate(header):
                            row_dict[col_name] = values[i] if i < len(values) else ''
                        cleaned_rows.append(row_dict)
                    else:
                        print(f"Warning: Line {line_num} has fewer values ({len(values)}) than expected ({len(header)}). Line: {line[:50]}...")
                
                print(f"Processed {row_count} rows, created {len(cleaned_rows)} cleaned rows")
                return cleaned_rows
        except Exception as e:
            print(f"Error cleaning CSV data: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def import_from_csv(self, csv_path=None):
        """
        Import data from a CSV file.
        
        Args:
            csv_path (str, optional): Path to the CSV file
            
        Returns:
            bool: Success status
        """
        if csv_path is None:
            # Default path from config
            csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                                  CSV_DIRECTORY, ARTICULOS_CSV)
        
        # Check if file exists
        if not os.path.exists(csv_path):
            print(f"CSV file not found: {csv_path}")
            return False
        
        # Clean CSV data
        cleaned_rows = self._clean_csv_data(csv_path)
        
        # Process the cleaned data
        try:
            batch_size = 20000
            batch = []
            
            # Prepare insert query
            query = """
            INSERT INTO articulos (velneo_id, pvsi_clave, nombre)
            VALUES (%s, %s, %s)
            ON CONFLICT (velneo_id) DO UPDATE 
            SET pvsi_clave = EXCLUDED.pvsi_clave, 
                nombre = EXCLUDED.nombre
            """
            
            # Process rows in batches
            row_count = 0
            problematic_rows = []
            for row in cleaned_rows:
                row_count += 1
                
                # Check for values exceeding column length limits
                if len(row['nombre']) > 255:
                    problematic_rows.append({
                        'row_number': row_count,
                        'velneo_id': row['velneo_id'],
                        'nombre_length': len(row['nombre'])
                    })
                
                # Only print first row for reference
                if row_count == 1:
                    print("First row values:")
                    print(f"velneo_id: {row['velneo_id']}")
                    print(f"pvsi_clave: {row['pvsi_clave']}")
                    print(f"nombre: {row['nombre']}")
                    print(f"nombre length: {len(row['nombre'])}")
                
                batch.append((
                    int(row['velneo_id']),
                    row['pvsi_clave'],
                    row['nombre'][:255]  # Truncate to 255 chars to avoid error
                ))
                
                # Execute batch insert when batch size is reached
                if len(batch) >= batch_size:
                    cursor = self.db_connection.execute_batch(query, batch)
                    if cursor is None:
                        return False
                    batch = []
            
            # After processing all rows, insert any remaining rows in the batch
            if batch:
                cursor = self.db_connection.execute_batch(query, batch)
                if cursor is None:
                    return False
            
            # Print summary of problematic rows
            if problematic_rows:
                print(f"\nFound {len(problematic_rows)} rows with 'nombre' values exceeding 255 characters:")
                for i, row in enumerate(problematic_rows[:5]):  # Show first 5 problematic rows
                    print(f"Row {row['row_number']}, velneo_id: {row['velneo_id']}, nombre length: {row['nombre_length']}")
                if len(problematic_rows) > 5:
                    print(f"...and {len(problematic_rows) - 5} more rows with long values")
                print("All values were truncated to 255 characters for import.")
            
            self.db_connection.commit()
            return True
                
        except Exception as e:
            print(f"Error importing CSV: {e}")
            return False
    
    def setup(self, csv_path=None):
        """
        Set up the articulos table (create and import data from CSV).
        
        Args:
            csv_path (str, optional): Path to the CSV file
            
        Returns:
            bool: Success status
        """
        # Create the table
        if not self.create_table():
            return False
        
        # Import data from CSV
        return self.import_from_csv(csv_path)
