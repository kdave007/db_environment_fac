#!/usr/bin/env python
"""
Debug script for database setup and CSV import.

This script creates the articulos, metodo_pago, and pais tables,
and imports data from CSV files for articulos and metodo_pago.
"""
import os
import sys
import csv
import json
from pathlib import Path
from dotenv import load_dotenv

# Determine the base directory (different for script vs executable)
if getattr(sys, 'frozen', False):
    # Running as a PyInstaller executable
    base_dir = os.path.dirname(sys.executable)
else:
    # Running as a script
    base_dir = os.path.dirname(os.path.abspath(__file__))

# Add project root to path
sys.path.append(base_dir)

# Load environment variables from .env file
env_path = os.path.join(base_dir, '.env')
load_dotenv(dotenv_path=env_path)
print(f"Looking for .env at: {env_path}")

# Import table classes
from src.database.articulos_table import ArticulosTable
from src.database.almacen_table import AlmacenTable
from src.database.caja_banco_table import CajaBancoTable
from src.database.clientes_table import ClientesTable
from src.database.detalle_estado_table import DetalleEstadoTable
from src.database.errores_table import ErroresTable
from src.database.estado_factura_venta_table import EstadoFacturaVentaTable
from src.database.forma_pago_table import FormaPagoTable
from src.database.forma_pago_caja_banco_table import FormaPagoCajaBancoTable
from src.database.general_misc_table import GeneralMiscTable
from src.database.iva_table import IvaTable
from src.database.metodo_pago_table import MetodoPagoTable
from src.database.pais_table import PaisTable
from src.database.proveedor_table import ProveedorTable
from src.database.recibo_venta_table import ReciboVentaTable
from src.database.reintentos_fac_venta_table import ReintentosFacturaVentaTable
from src.database.tipo_movimiento_table import TipoMovimientoTable
from src.database.vendedores_table import VendedoresTable
from src.database.connection import DatabaseConnection
from src.config import ARTICULOS_CSV, METODO_PAGO_CSV, GENERAL_MISC_CSV

def load_table_config():
    """Load table configuration from JSON file."""
    # Determine the base directory (different for script vs executable)
    if getattr(sys, 'frozen', False):
        # Running as a PyInstaller executable
        base_dir = os.path.dirname(sys.executable)
    else:
        # Running as a script
        base_dir = os.path.dirname(os.path.abspath(__file__))
    
    config_path = os.path.join(base_dir, 'table_config.json')
    print(f"Looking for table_config.json at: {config_path}")
    
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as config_file:
                return json.load(config_file)
        else:
            print(f"Warning: Configuration file not found at {config_path}")
            # Return default configuration (all tables enabled)
            return {
                "articulos": True,
                "metodo_pago": True,
                "pais": True,
                "almacen": True
            }
    except Exception as e:
        print(f"Error loading table configuration: {e}")
        # Return default configuration
        return {
            "articulos": True,
            "metodo_pago": True,
            "pais": True,
            "almacen": True
        }

def main():
    """Main function to run the debug script."""
    print("Starting debug script...")
    
    # Load table configuration
    table_config = load_table_config()
    print("Table configuration loaded:")
    for table, enabled in table_config.items():
        print(f"  {table}: {'Enabled' if enabled else 'Disabled'}")
    
    # Print environment variables for debugging
    print(f"\nDB_NAME: {os.getenv('DB_NAME')}")
    print(f"DB_USER: {os.getenv('DB_USER')}")
    print(f"DB_PASSWORD: {os.getenv('DB_PASSWORD')}")
    print(f"DB_HOST: {os.getenv('DB_HOST')}")
    print(f"DB_PORT: {os.getenv('DB_PORT')}")
    print(f"CSV_DIRECTORY: {os.getenv('CSV_DIRECTORY')}")
    
    # Check if tables should be deleted before creation
    delete_tables = os.getenv('DELETE_TABLES', 'false').lower() == 'true'
    if delete_tables:
        print("\nDELETE_TABLES flag is set to true. Tables will be dropped before creation.")
    else:
        print("\nDELETE_TABLES flag is set to false. Tables will not be dropped before creation.")
    
    # Initialize database connection
    db_connection = DatabaseConnection(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
    
    # Connect to database
    if not db_connection.connect():
        print("Failed to connect to database")
        return
    
    try:
        # Delete tables if DELETE_TABLES flag is set to true
        delete_tables = os.getenv('DELETE_TABLES', 'false').lower() == 'true'
        if delete_tables:
            print("\nDeleting tables based on configuration...")
            # Create a list of tables to delete in reverse order (to handle foreign key constraints)
            tables_to_delete = []
            for table_name, enabled in table_config.items():
                if enabled:
                    tables_to_delete.append(table_name)
            
            # Reverse the order to handle foreign key constraints
            tables_to_delete.reverse()
            
            for table_name in tables_to_delete:
                print(f"Dropping table {table_name}...")
                query = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
                cursor = db_connection.execute_query(query)
                if cursor is None:
                    print(f"Failed to drop table {table_name}")
                else:
                    print(f"Table {table_name} dropped successfully")
            
            # Commit the changes
            db_connection.commit()
            print("Table deletion completed")
            print("DELETE_TABLES flag is set to true. Exiting without creating tables.")
            return 0  # Exit with success code

        # Process each table based on configuration
        
        # Process articulos table if enabled
        if table_config.get("articulos", True):
            print("\nCreating articulos table...")
            articulos_table = ArticulosTable(db_connection)
            if articulos_table.create_table():
                print("Articulos table created successfully")
                
                print("\nImporting data from CSV for articulos...")
                if articulos_table.import_from_csv():
                    print("Data imported successfully for articulos")
                else:
                    print("Failed to import data from CSV for articulos")
                    print("Make sure the CSV file exists in the configured directory")
            else:
                print("Failed to create articulos table")
        else:
            print("\nSkipping articulos table (disabled in configuration)")
        
        # Process caja_banco table if enabled
        if table_config.get("caja_banco", True):
            print("\nCreating caja_banco table...")
            caja_banco_table = CajaBancoTable(db_connection)
            if caja_banco_table.create_table():
                print("Caja_banco table created successfully")
                
                print("\nLoading default data for caja_banco...")
                if caja_banco_table.insert_default_data():
                    print("Default data for caja_banco loaded successfully")
                else:
                    print("Failed to load default data for caja_banco")
            else:
                print("Failed to create caja_banco table")
        else:
            print("\nSkipping caja_banco table (disabled in configuration)")
        
        # Process clientes table if enabled
        if table_config.get("clientes", True):
            print("\nCreating clientes table...")
            clientes_table = ClientesTable(db_connection)
            if clientes_table.create_table():
                print("Clientes table created successfully")
                
                print("\nLoading default data for clientes...")
                if clientes_table.insert_default_data():
                    print("Default data for clientes loaded successfully")
                else:
                    print("Failed to load default data for clientes")
            else:
                print("Failed to create clientes table")
        else:
            print("\nSkipping clientes table (disabled in configuration)")
        
        # Process detalle_estado table if enabled
        if table_config.get("detalle_estado", True):
            print("\nCreating detalle_estado table...")
            detalle_estado_table = DetalleEstadoTable(db_connection)
            if detalle_estado_table.create_table():
                print("Detalle_estado table created successfully")
                
                print("\nLoading default data for detalle_estado...")
                if detalle_estado_table.insert_default_data():
                    print("Default data for detalle_estado loaded successfully")
                else:
                    print("Failed to load default data for detalle_estado")
            else:
                print("Failed to create detalle_estado table")
        else:
            print("\nSkipping detalle_estado table (disabled in configuration)")
        
        # Process errores table if enabled
        if table_config.get("errores", True):
            print("\nCreating errores table...")
            errores_table = ErroresTable(db_connection)
            if errores_table.create_table():
                print("Errores table created successfully")
                
                print("\nLoading default data for errores...")
                if errores_table.insert_default_data():
                    print("Default data for errores loaded successfully")
                else:
                    print("Failed to load default data for errores")
            else:
                print("Failed to create errores table")
        else:
            print("\nSkipping errores table (disabled in configuration)")
        
        # Process estado_factura_venta table if enabled
        if table_config.get("estado_factura_venta", True):
            print("\nCreating estado_factura_venta table...")
            estado_factura_venta_table = EstadoFacturaVentaTable(db_connection)
            if estado_factura_venta_table.create_table():
                print("Estado_factura_venta table created successfully")
                
                print("\nLoading default data for estado_factura_venta...")
                if estado_factura_venta_table.insert_default_data():
                    print("Default data for estado_factura_venta loaded successfully")
                else:
                    print("Failed to load default data for estado_factura_venta")
            else:
                print("Failed to create estado_factura_venta table")
        else:
            print("\nSkipping estado_factura_venta table (disabled in configuration)")
        
        # Process forma_pago table if enabled
        if table_config.get("forma_pago", True):
            print("\nCreating forma_pago table...")
            forma_pago_table = FormaPagoTable(db_connection)
            if forma_pago_table.create_table():
                print("Forma_pago table created successfully")
                
                print("\nLoading default data for forma_pago...")
                if forma_pago_table.insert_default_data():
                    print("Default data for forma_pago loaded successfully")
                else:
                    print("Failed to load default data for forma_pago")
            else:
                print("Failed to create forma_pago table")
        else:
            print("\nSkipping forma_pago table (disabled in configuration)")
        
        # Process forma_pago_caja_banco table if enabled
        if table_config.get("forma_pago_caja_banco", True):
            print("\nCreating forma_pago_caja_banco table...")
            forma_pago_caja_banco_table = FormaPagoCajaBancoTable(db_connection)
            if forma_pago_caja_banco_table.create_table():
                print("Forma_pago_caja_banco table created successfully")
                
                print("\nLoading default data for forma_pago_caja_banco...")
                if forma_pago_caja_banco_table.insert_default_data():
                    print("Default data for forma_pago_caja_banco loaded successfully")
                else:
                    print("Failed to load default data for forma_pago_caja_banco")
            else:
                print("Failed to create forma_pago_caja_banco table")
        else:
            print("\nSkipping forma_pago_caja_banco table (disabled in configuration)")
        
        # Process general_misc table if enabled
        if table_config.get("general_misc", True):
            print("\nCreating general_misc table...")
            general_misc_table = GeneralMiscTable(db_connection)
            if general_misc_table.create_table():
                print("General_misc table created successfully")
                
                # Check if JSON import is enabled (default to true)
                use_json = os.getenv('USE_GENERAL_MISC_JSON', 'true').lower() == 'true'
                if use_json:
                    print("\nImporting data from JSON for general_misc...")
                    # Get JSON path from environment variable or use default
                    json_directory = os.getenv('JSON_DIRECTORY', '')
                    json_filename = os.getenv('GENERAL_MISC_JSON', 'tables_setup_values.json')
                    json_path = os.path.join(base_dir, json_directory, json_filename)
                    
                    if os.path.exists(json_path):
                        if general_misc_table.import_from_json(json_path):
                            print("Data imported successfully for general_misc from JSON")
                        else:
                            print("Failed to import data from JSON for general_misc")
                            print("Falling back to default data...")
                            if general_misc_table.insert_default_data():
                                print("Default data for general_misc loaded successfully")
                            else:
                                print("Failed to load default data for general_misc")
                    else:
                        print(f"JSON file not found at {json_path}")
                        print("Falling back to default data...")
                        if general_misc_table.insert_default_data():
                            print("Default data for general_misc loaded successfully")
                        else:
                            print("Failed to load default data for general_misc")
                # else:
                #     # Check if CSV import is enabled as fallback
                #     use_csv = os.getenv('USE_GENERAL_MISC_CSV', 'false').lower() == 'true'
                #     if use_csv:
                #         print("\nImporting data from CSV for general_misc...")
                #         # Get CSV path from environment variable or use default
                #         csv_directory = os.getenv('CSV_DIRECTORY', 'data/csv')
                #         csv_filename = os.getenv('GENERAL_MISC_CSV', GENERAL_MISC_CSV)
                #         csv_path = os.path.join(base_dir, csv_directory, csv_filename)
                        
                #         if general_misc_table.import_from_csv(csv_path):
                #             print("Data imported successfully for general_misc from CSV")
                #         else:
                #             print("Failed to import data from CSV for general_misc")
                #             print("Falling back to default data...")
                #             if general_misc_table.insert_default_data():
                #                 print("Default data for general_misc loaded successfully")
                #             else:
                #                 print("Failed to load default data for general_misc")
                #     else:
                #         print("\nLoading default data for general_misc...")
                #         if general_misc_table.insert_default_data():
                #             print("Default data for general_misc loaded successfully")
                #         else:
                #             print("Failed to load default data for general_misc")
            else:
                print("Failed to create general_misc table")
        else:
            print("\nSkipping general_misc table (disabled in configuration)")
        
        # Process iva table if enabled
        if table_config.get("iva", True):
            print("\nCreating iva table...")
            iva_table = IvaTable(db_connection)
            if iva_table.create_table():
                print("Iva table created successfully")
                
                print("\nLoading default data for iva...")
                if iva_table.insert_default_data():
                    print("Default data for iva loaded successfully")
                else:
                    print("Failed to load default data for iva")
            else:
                print("Failed to create iva table")
        else:
            print("\nSkipping iva table (disabled in configuration)")
        
        # Process metodo_pago table if enabled
        if table_config.get("metodo_pago", True):
            print("\nCreating metodo_pago table...")
            metodo_pago_table = MetodoPagoTable(db_connection)
            if metodo_pago_table.create_table():
                print("Metodo_pago table created successfully")
                
                print("\nImporting data from CSV for metodo_pago...")
                if metodo_pago_table.import_from_csv():
                    print("Data imported successfully for metodo_pago")
                else:
                    print("Failed to import data from CSV for metodo_pago")
                    print("Make sure the CSV file exists in the configured directory")
            else:
                print("Failed to create metodo_pago table")
        else:
            print("\nSkipping metodo_pago table (disabled in configuration)")
        
        # Process pais table if enabled
        if table_config.get("pais", True):
            print("\nCreating pais table...")
            pais_table = PaisTable(db_connection)
            if pais_table.create_table():
                print("Pais table created successfully")
                
                print("\nLoading default data for pais...")
                if pais_table.insert_default_data():
                    print("Default data for pais loaded successfully")
                else:
                    print("Failed to load default data for pais")
            else:
                print("Failed to create pais table")
        else:
            print("\nSkipping pais table (disabled in configuration)")
        
        # Process proveedor table if enabled
        if table_config.get("proveedor", True):
            print("\nCreating proveedor table...")
            proveedor_table = ProveedorTable(db_connection)
            if proveedor_table.create_table():
                print("Proveedor table created successfully")
                
                print("\nLoading default data for proveedor...")
                if proveedor_table.insert_default_data():
                    print("Default data for proveedor loaded successfully")
                else:
                    print("Failed to load default data for proveedor")
            else:
                print("Failed to create proveedor table")
        else:
            print("\nSkipping proveedor table (disabled in configuration)")
        
        # Process recibo_venta table if enabled
        if table_config.get("recibo_venta", True):
            print("\nCreating recibo_venta table...")
            recibo_venta_table = ReciboVentaTable(db_connection)
            if recibo_venta_table.create_table():
                print("Recibo_venta table created successfully")
                
                print("\nLoading default data for recibo_venta...")
                if recibo_venta_table.insert_default_data():
                    print("Default data for recibo_venta loaded successfully")
                else:
                    print("Failed to load default data for recibo_venta")
            else:
                print("Failed to create recibo_venta table")
        else:
            print("\nSkipping recibo_venta table (disabled in configuration)")
        
        # Process reintentos_fac_venta table if enabled
        if table_config.get("reintentos_fac_venta", True):
            print("\nCreating reintentos_fac_venta table...")
            reintentos_fac_venta_table = ReintentosFacturaVentaTable(db_connection)
            if reintentos_fac_venta_table.create_table():
                print("Reintentos_fac_venta table created successfully")
                
                print("\nLoading default data for reintentos_fac_venta...")
                if reintentos_fac_venta_table.insert_default_data():
                    print("Default data for reintentos_fac_venta loaded successfully")
                else:
                    print("Failed to load default data for reintentos_fac_venta")
            else:
                print("Failed to create reintentos_fac_venta table")
        else:
            print("\nSkipping reintentos_fac_venta table (disabled in configuration)")
        
        # Process tipo_movimiento table if enabled
        if table_config.get("tipo_movimiento", True):
            print("\nCreating tipo_movimiento table...")
            tipo_movimiento_table = TipoMovimientoTable(db_connection)
            if tipo_movimiento_table.create_table():
                print("Tipo_movimiento table created successfully")
                
                print("\nLoading default data for tipo_movimiento...")
                if tipo_movimiento_table.insert_default_data():
                    print("Default data for tipo_movimiento loaded successfully")
                else:
                    print("Failed to load default data for tipo_movimiento")
            else:
                print("Failed to create tipo_movimiento table")
        else:
            print("\nSkipping tipo_movimiento table (disabled in configuration)")
        
        # Process vendedores table if enabled
        if table_config.get("vendedores", True):
            print("\nCreating vendedores table...")
            vendedores_table = VendedoresTable(db_connection)
            if vendedores_table.create_table():
                print("Vendedores table created successfully")
                
                print("\nLoading default data for vendedores...")
                if vendedores_table.insert_default_data():
                    print("Default data for vendedores loaded successfully")
                else:
                    print("Failed to load default data for vendedores")
            else:
                print("Failed to create vendedores table")
        else:
            print("\nSkipping vendedores table (disabled in configuration)")
            
        # Process almacen table if enabled
        if table_config.get("almacen", True):
            print("\nCreating almacen table...")
            almacen_table = AlmacenTable(db_connection)
            if almacen_table.create_table():
                print("Almacen table created successfully")
                
                # Check if JSON import is enabled (default to true)
                use_json = os.getenv('USE_ALMACEN_JSON', 'true').lower() == 'true'
                if use_json:
                    print("\nImporting data from JSON for almacen...")
                    # Get JSON path from environment variable or use default
                    json_directory = os.getenv('JSON_DIRECTORY', '')
                    json_filename = os.getenv('ALMACEN_JSON', 'tables_setup_values.json')
                    json_path = os.path.join(base_dir, json_directory, json_filename)
                    
                    if os.path.exists(json_path):
                        if almacen_table.import_from_json(json_path):
                            print("Data imported successfully for almacen from JSON")
                        else:
                            print("Failed to import data from JSON for almacen")
                            print("Falling back to default data...")
                            if almacen_table.insert_default_data():
                                print("Default data for almacen loaded successfully")
                            else:
                                print("Failed to load default data for almacen")
                    else:
                        print(f"JSON file not found at {json_path}")
                        print("Falling back to default data...")
                        if almacen_table.insert_default_data():
                            print("Default data for almacen loaded successfully")
                        else:
                            print("Failed to load default data for almacen")
                else:
                    print("\nLoading default data for almacen...")
                    if almacen_table.insert_default_data():
                        print("Default data for almacen loaded successfully")
                    else:
                        print("Failed to load default data for almacen")
            else:
                print("Failed to create almacen table")
        else:
            print("\nSkipping almacen table (disabled in configuration)")
        
        print("\nDebug script completed successfully")
        
    except Exception as e:
        print(f"Error in debug script: {e}")
    finally:
        # Close database connection
        db_connection.close()


if __name__ == "__main__":
    sys.exit(main())
