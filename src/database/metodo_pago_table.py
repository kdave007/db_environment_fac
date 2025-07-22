"""
Implementation of the metodo_pago table with CSV import functionality.
"""
import os
import csv
from pathlib import Path
from ..config import CSV_DIRECTORY, METODO_PAGO_CSV


class MetodoPagoTable:
    """Handler for the 'metodo_pago' table."""
    
    def __init__(self, db_connection):
        """Initialize the metodo_pago table handler."""
        self.db_connection = db_connection
        self.table_name = "metodo_pago"
    
    def create_table(self):
        """
        Create the metodo_pago table.
        
        Returns:
            bool: Success status
        """
        query = """
        CREATE TABLE IF NOT EXISTS metodo_pago (
            velneo INT PRIMARY KEY,
            pvsi VARCHAR(10),
            descripcion VARCHAR(100)
        );
        """
        
        cursor = self.db_connection.execute_query(query)
        if cursor is None:
            return False
        
        self.db_connection.commit()
        return True
    
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
                                  CSV_DIRECTORY, METODO_PAGO_CSV)
        
        # Check if file exists
        if not os.path.exists(csv_path):
            print(f"CSV file not found: {csv_path}")
            return False
        
        # Read CSV file
        try:
            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                batch_size = 1000
                batch = []
                
                # Prepare insert query
                query = """
                INSERT INTO metodo_pago (velneo, pvsi, descripcion)
                VALUES (%s, %s, %s)
                ON CONFLICT (velneo) DO UPDATE 
                SET pvsi = EXCLUDED.pvsi, 
                    descripcion = EXCLUDED.descripcion
                """
                
                # Process rows in batches
                for row in reader:
                    batch.append((
                        int(row['velneo']),
                        row['pvsi'],
                        row['descripcion']
                    ))
                    
                    # Execute batch insert
                    if len(batch) >= batch_size:
                        cursor = self.db_connection.execute_batch(query, batch)
                        if cursor is None:
                            return False
                        batch = []
                
                # Insert remaining rows
                if batch:
                    cursor = self.db_connection.execute_batch(query, batch)
                    if cursor is None:
                        return False
                
                self.db_connection.commit()
                return True
                
        except Exception as e:
            print(f"Error importing CSV: {e}")
            return False
    
    def setup(self, csv_path=None):
        """
        Set up the metodo_pago table (create and import data from CSV).
        
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
