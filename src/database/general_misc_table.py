"""
Implementation of the general_misc table using the TableSimpleBlueprint class.
"""
import os
import sys
import csv
import json
from pathlib import Path
from ..config import CSV_DIRECTORY
from .table_simple_blueprint import TableSimpleBlueprint


class GeneralMiscTable(TableSimpleBlueprint):
    """Handler for the 'general_misc' table."""
    
    def __init__(self, db_connection):
        """Initialize the general_misc table handler."""
        super().__init__(db_connection, "general_misc")
        # Default CSV filename
        self.csv_filename = "general_misc.csv"
    
    def get_create_query(self):
        """
        Get the SQL query to create the general_misc table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS general_misc(
            id_velneo VARCHAR(10),
            id_pvsi VARCHAR(10),
            title VARCHAR(50),
            plaza VARCHAR(5),
            tienda VARCHAR(5)
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the general_misc table.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO general_misc (id_velneo, id_pvsi, title, plaza, tienda) 
        VALUES (%s, %s, %s, %s, %s)
        """
    
    def insert_default_data(self):
        """
        Insert the default data into the general_misc table.
        
        Returns:
            bool: Success status
        """
        data = [
            ('SIVX3', 'x', 'division', 'XALAP', 'ARAUC'),
            ('SIVX', 'x', 'empresa', 'XALAP', 'ARAUC'),
            ('1', 'x', 'moneda', 'XALAP', 'ARAUC'),
            ('XALAP', 'x', 'plaza', 'XALAP', 'ARAUC'),
            ('9', 'x', 'serie', 'XALAP', 'ARAUC'),
            ('6', 'x', 'serie_compra', 'XALAP', 'ARAUC')
        ]
        
        return self.insert_data(data)
        
    def import_from_json(self, json_path):
        """
        Import profile data from a JSON file and insert into the general_misc table.
        
        The JSON file should have the following structure:
        {
            "profiles": [
                {
                    "plaza": "XALAP",
                    "tienda": "ROTON",
                    "serie": "7",
                    "division": "SIVX2",
                    "empresa": "SIVX",
                    "moneda": 1,
                    "almacen": "XALAPROTON"
                }
            ]
        }
        
        Args:
            json_path (str): Path to the JSON file
            
        Returns:
            bool: Success status
        """
        try:
            # Check if file exists
            if not os.path.exists(json_path):
                print(f"JSON file not found: {json_path}")
                return False
                
            # Read the JSON file
            with open(json_path, 'r', encoding='utf-8') as file:
                json_data = json.load(file)
            
            if 'profiles' not in json_data:
                print("Error: JSON file does not contain 'profiles' key")
                return False
                
            profiles = json_data['profiles']
            if not profiles:
                print("Warning: No profiles found in JSON file")
                return False
                
            # Prepare data for insertion
            data = []
            for profile in profiles:
                # For each profile, create multiple rows (one for each attribute)
                # Each row format: (id_velneo, id_pvsi, title, plaza, tienda)
                
                plaza = profile.get('plaza', '')
                tienda = profile.get('tienda', '')
                
                # Add division entry
                if 'division' in profile:
                    data.append((profile['division'], 'x', 'division', plaza, tienda))
                
                # Add empresa entry
                if 'empresa' in profile:
                    data.append((profile['empresa'], 'x', 'empresa', plaza, tienda))
                
                # Add moneda entry (convert to string if it's a number)
                if 'moneda' in profile:
                    data.append((str(profile['moneda']), 'x', 'moneda', plaza, tienda))
                
                # # Add plaza entry
                # if 'plaza' in profile:
                #     data.append((plaza, 'x', 'plaza', plaza, tienda))
                
                # Add serie entry
                if 'serie' in profile:
                    data.append((profile['serie'], 'x', 'serie', plaza, tienda))
                    
            # Insert the data
            if data:
                return self.insert_data(data)
            else:
                print("No valid data found in profiles")
                return False
                
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            return False
        except Exception as e:
            print(f"Error importing from JSON: {e}")
            import traceback
            traceback.print_exc()
            return False
    
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
                                  CSV_DIRECTORY, self.csv_filename)
        
        # Check if file exists
        if not os.path.exists(csv_path):
            print(f"CSV file not found: {csv_path}")
            return False
        
        # Clean CSV data
        cleaned_rows = self._clean_csv_data(csv_path)
        
        # Process the cleaned data
        try:
            batch_size = 1000
            batch = []
            
            # Prepare insert query
            query = """
            INSERT INTO general_misc (id_velneo, id_pvsi, title, plaza, tienda)
            VALUES (%s, %s, %s, %s, %s)
            """
            
            # Process rows in batches
            row_count = 0
            for row in cleaned_rows:
                row_count += 1
                
                # Only print first row for reference
                if row_count == 1:
                    print("First row values:")
                    print(f"id_velneo: {row['id_velneo']}")
                    print(f"id_pvsi: {row['id_pvsi']}")
                    print(f"title: {row['title']}")
                    print(f"plaza: {row['plaza']}")
                    print(f"tienda: {row['tienda']}")
                
                batch.append((
                    row['id_velneo'],
                    row['id_pvsi'],
                    row['title'],
                    row['plaza'],
                    row['tienda']
                ))
                
                # Execute batch insert when batch size is reached
                if len(batch) >= batch_size:
                    cursor = self.db.execute_batch(query, batch)
                    if cursor is None:
                        return False
                    batch = []
            
            # After processing all rows, insert any remaining rows in the batch
            if batch:
                cursor = self.db.execute_batch(query, batch)
                if cursor is None:
                    return False
            
            self.db.commit()
            return True
                
        except Exception as e:
            print(f"Error importing CSV: {e}")
            return False
    
    def setup(self, use_default_data=True, csv_path=None):
        """
        Set up the general_misc table (create and populate with default data if requested).
        
        Args:
            use_default_data (bool, optional): Whether to use the default data
            csv_path (str, optional): Path to the CSV file to import data from
            
        Returns:
            bool: Success status
        """
        # Create the table
        if not self.create_table():
            return False
        
        # If CSV path is provided, import from CSV
        if csv_path is not None:
            return self.import_from_csv(csv_path)
        
        # Insert default data if requested
        if use_default_data:
            return self.insert_default_data()
        
        return True
