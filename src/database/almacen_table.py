"""
Implementation of the almacen table using the TableSimpleBlueprint class.
"""
import os
from .table_simple_blueprint import TableSimpleBlueprint


class AlmacenTable(TableSimpleBlueprint):
    """Handler for the 'almacen' table."""
    
    def __init__(self, db_connection):
        """Initialize the almacen table handler."""
        super().__init__(db_connection, "almacen")
    
    def get_create_query(self):
        """
        Get the SQL query to create the almacen table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS almacen (
            velneo VARCHAR(255) PRIMARY KEY,
            plaza VARCHAR(255),
            tienda VARCHAR(255)
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the almacen table.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO almacen (velneo, plaza, tienda) 
        VALUES (%s, %s, %s)
        """
    
    def insert_default_data(self):
        """
        Insert the default data into the almacen table.
        Uses environment variables PLAZA and TIENDA to create the velneo value.
        
        Returns:
            bool: Success status
        """
        # Get plaza and tienda from environment variables or use defaults
        plaza = os.getenv('PLAZA', 'XALAP')
        tienda = os.getenv('TIENDA', 'ROTON')
        
        # Create velneo by concatenating plaza and tienda
        velneo = f"{plaza}{tienda}"
        
        data = [
            (velneo, 'x', plaza)
        ]
        
        return self.insert_data(data)
    
    def insert_from_profiles(self, profiles):
        """
        Insert data from profiles into the almacen table.
        
        Args:
            profiles (list): List of profile dictionaries containing plaza and tienda
            
        Returns:
            bool: Success status
        """
        if not profiles:
            return False
            
        data = []
        for profile in profiles:
            plaza = profile.get('plaza', '')
            tienda = profile.get('tienda', '')
            almacen = profile.get('almacen', f"{plaza}{tienda}")
            
            # Only add if both plaza and tienda are present
            if plaza and tienda:
                data.append((almacen, plaza, tienda))
                
        if data:
            return self.insert_data(data)
        return False
    
    def import_from_json(self, json_path):
        """
        Import profile data from a JSON file and insert into the almacen table.
        
        The JSON file should have the following structure:
        {
            "profiles": [
                {
                    "plaza": "XALAP",
                    "tienda": "ROTON",
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
            import json
            import os
            
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
            return self.insert_from_profiles(profiles)
                
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            return False
        except Exception as e:
            print(f"Error importing from JSON: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def setup(self, use_default_data=True, json_path=None):
        """
        Set up the almacen table (create and populate with data).
        
        Args:
            use_default_data (bool, optional): Whether to use the default data
            json_path (str, optional): Path to a JSON file with profile data
            
        Returns:
            bool: Success status
        """
        # Create the table
        if not self.create_table():
            return False
        
        # Import from JSON if path is provided
        if json_path is not None:
            return self.import_from_json(json_path)
        
        # Insert default data if requested
        if use_default_data:
            return self.insert_default_data()
        
        return True
