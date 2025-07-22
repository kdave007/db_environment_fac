"""
Table handlers for specific database tables.
Contains classes for creating and populating specific tables.
"""
import os
import csv
from pathlib import Path


class ArticulosHandler:
    """Handler for the 'articulos' table."""
    
    def __init__(self, db_connection):
        """
        Initialize the handler.
        
        Args:
            db_connection: Database connection instance
        """
        self.db = db_connection
        
    def create_table(self):
        """
        Create the 'articulos' table.
        
        Returns:
            bool: Success status
        """
        query = """
        CREATE TABLE IF NOT EXISTS articulos (
            velneo_id INTEGER PRIMARY KEY NOT NULL,
            pvsi_clave VARCHAR(20) NOT NULL,
            nombre VARCHAR(255) NOT NULL
        );
        """
        
        cursor = self.db.execute_query(query)
        self.db.commit()
        success = cursor is not None
        
        if success:
            print("Table 'articulos' created successfully")
        else:
            print("Failed to create table 'articulos'")
            
        return success
    
    def import_from_csv(self, csv_file, batch_size=1000, delimiter=','):
        """
        Import data from CSV file into the 'articulos' table.
        
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
            # Define column mapping (CSV column index -> database column name)
            columns = ['velneo_id', 'pvsi_clave', 'nombre']
            
            with open(csv_file, 'r', newline='', encoding='utf-8') as f:
                csv_reader = csv.reader(f, delimiter=delimiter)
                
                # Skip header row
                next(csv_reader, None)
                
                # Prepare the SQL query for insertion
                query = """
                INSERT INTO articulos (velneo_id, pvsi_clave, nombre) 
                VALUES (%s, %s, %s)
                """
                
                # Process data in batches
                batch_data = []
                rows_imported = 0
                
                for row in csv_reader:
                    if len(row) >= 3:  # Ensure we have all required columns
                        # Convert velneo_id to integer
                        try:
                            velneo_id = int(row[0])
                            pvsi_clave = row[1]
                            nombre = row[2]
                            
                            batch_data.append((velneo_id, pvsi_clave, nombre))
                            
                            # Insert batch when it reaches the specified size
                            if len(batch_data) >= batch_size:
                                success = self.db.execute_many(query, batch_data)
                                if not success:
                                    print(f"Error inserting batch at row {rows_imported + 1}")
                                    return False, rows_imported
                                
                                rows_imported += len(batch_data)
                                print(f"Imported {rows_imported} rows to articulos...")
                                batch_data = []
                        except ValueError:
                            print(f"Warning: Invalid velneo_id value in row: {row}. Skipping.")
                
                # Insert any remaining data
                if batch_data:
                    success = self.db.execute_many(query, batch_data)
                    if not success:
                        print(f"Error inserting final batch")
                        return False, rows_imported
                    
                    rows_imported += len(batch_data)
                
                # Commit the transaction
                self.db.commit()
                print(f"Successfully imported {rows_imported} rows into table 'articulos'")
                return True, rows_imported
                
        except Exception as e:
            print(f"Error importing CSV data to articulos: {e}")
            return False, 0


class MetodoPagoHandler:
    """Handler for the 'metodo_pago' table."""
    
    def __init__(self, db_connection):
        """
        Initialize the handler.
        
        Args:
            db_connection: Database connection instance
        """
        self.db = db_connection
        
    def create_table(self):
        """
        Create the 'metodo_pago' table.
        
        Returns:
            bool: Success status
        """
        query = """
        CREATE TABLE IF NOT EXISTS metodo_pago (
            velneo INT PRIMARY KEY,
            pvsi VARCHAR(10) NOT NULL,
            descripcion VARCHAR(100) NOT NULL
        );
        """
        
        cursor = self.db.execute_query(query)
        self.db.commit()
        success = cursor is not None
        
        if success:
            print("Table 'metodo_pago' created successfully")
        else:
            print("Failed to create table 'metodo_pago'")
            
        return success
    
    def import_from_csv(self, csv_file, batch_size=1000, delimiter=','):
        """
        Import data from CSV file into the 'metodo_pago' table.
        
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
            # Define column mapping (CSV column index -> database column name)
            columns = ['velneo', 'pvsi', 'descripcion']
            
            with open(csv_file, 'r', newline='', encoding='utf-8') as f:
                csv_reader = csv.reader(f, delimiter=delimiter)
                
                # Skip header row
                next(csv_reader, None)
                
                # Prepare the SQL query for insertion
                query = """
                INSERT INTO metodo_pago (velneo, pvsi, descripcion) 
                VALUES (%s, %s, %s)
                """
                
                # Process data in batches
                batch_data = []
                rows_imported = 0
                
                for row in csv_reader:
                    if len(row) >= 3:  # Ensure we have all required columns
                        # Convert velneo to integer
                        try:
                            velneo = int(row[0])
                            pvsi = row[1]
                            descripcion = row[2]
                            
                            batch_data.append((velneo, pvsi, descripcion))
                            
                            # Insert batch when it reaches the specified size
                            if len(batch_data) >= batch_size:
                                success = self.db.execute_many(query, batch_data)
                                if not success:
                                    print(f"Error inserting batch at row {rows_imported + 1}")
                                    return False, rows_imported
                                
                                rows_imported += len(batch_data)
                                print(f"Imported {rows_imported} rows to metodo_pago...")
                                batch_data = []
                        except ValueError:
                            print(f"Warning: Invalid velneo value in row: {row}. Skipping.")
                
                # Insert any remaining data
                if batch_data:
                    success = self.db.execute_many(query, batch_data)
                    if not success:
                        print(f"Error inserting final batch")
                        return False, rows_imported
                    
                    rows_imported += len(batch_data)
                
                # Commit the transaction
                self.db.commit()
                print(f"Successfully imported {rows_imported} rows into table 'metodo_pago'")
                return True, rows_imported
                
        except Exception as e:
            print(f"Error importing CSV data to metodo_pago: {e}")
            return False, 0
