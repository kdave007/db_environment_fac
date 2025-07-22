"""
Table classes for database tables.
Each class represents a specific table with methods for creation and data import.
"""
import os
import csv
from .table_base import TableBase


class ArticulosTable(TableBase):
    """Handler for the 'articulos' table."""
    
    def __init__(self, db_connection):
        """Initialize the articulos table handler."""
        super().__init__(db_connection, "articulos")
    
    def get_create_query(self):
        """
        Get the SQL query to create the articulos table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS articulos (
            velneo_id INTEGER PRIMARY KEY NOT NULL,
            pvsi_clave VARCHAR(20) NOT NULL,
            nombre VARCHAR(255) NOT NULL
        );
        """
    
    def import_from_csv(self, csv_file, batch_size=1000, delimiter=','):
        """
        Import data from CSV file into the articulos table.
        
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
    
    def setup(self, csv_file=None):
        """
        Set up the articulos table (create and populate if CSV file provided).
        
        Args:
            csv_file (str, optional): Path to CSV file for data import
            
        Returns:
            bool: Success status
        """
        # Create the table
        if not self.create_table():
            return False
        
        # Import data if CSV file provided
        if csv_file:
            success, _ = self.import_from_csv(csv_file)
            return success
        
        return True


class MetodoPagoTable(TableBase):
    """Handler for the 'metodo_pago' table."""
    
    def __init__(self, db_connection):
        """Initialize the metodo_pago table handler."""
        super().__init__(db_connection, "metodo_pago")
    
    def get_create_query(self):
        """
        Get the SQL query to create the metodo_pago table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS metodo_pago (
            velneo INT PRIMARY KEY,
            pvsi VARCHAR(10) NOT NULL,
            descripcion VARCHAR(100) NOT NULL
        );
        """
    
    def import_from_csv(self, csv_file, batch_size=1000, delimiter=','):
        """
        Import data from CSV file into the metodo_pago table.
        
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
    
    def setup(self, csv_file=None):
        """
        Set up the metodo_pago table (create and populate if CSV file provided).
        
        Args:
            csv_file (str, optional): Path to CSV file for data import
            
        Returns:
            bool: Success status
        """
        # Create the table
        if not self.create_table():
            return False
        
        # Import data if CSV file provided
        if csv_file:
            success, _ = self.import_from_csv(csv_file)
            return success
        
        return True
