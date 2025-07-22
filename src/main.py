#!/usr/bin/env python
"""
Database Installer for VTA

This script creates and populates database tables for a secondary process.
It handles table creation and data import from CSV files.
"""
import os
import sys
import argparse
from pathlib import Path

# Import the fix_imports helper
try:
    from fix_imports import fix_imports
    fix_imports()
    print("Import paths fixed successfully")
except ImportError:
    # If running as a script, add the src directory to the path
    src_dir = os.path.dirname(os.path.abspath(__file__))
    if src_dir not in sys.path:
        sys.path.append(src_dir)
    print(f"Added {src_dir} to sys.path")

from database.db_connection import DatabaseConnection
from config import CSV_DIRECTORY, ensure_csv_directory
from database.tables import ArticulosTable, MetodoPagoTable
from database.caja_banco_table import CajaBancoTable
from database.clientes_table import ClientesTable
from database.detalle_estado_table import DetalleEstadoTable
from database.errores_table import ErroresTable
from database.estado_factura_venta_table import EstadoFacturaVentaTable
from database.tipo_movimiento_table import TipoMovimientoTable
from database.reintentos_fac_venta_table import ReintentosFacturaVentaTable
from database.recibo_venta_table import ReciboVentaTable
from database.proveedor_table import ProveedorTable
from database.pais_table import PaisTable
from database.forma_pago_table import FormaPagoTable
from database.forma_pago_caja_banco_table import FormaPagoCajaBancoTable
from database.general_misc_table import GeneralMiscTable
from database.iva_table import IvaTable


def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Database installer for VTA")
    parser.add_argument("--create-tables", action="store_true", help="Create tables")
    parser.add_argument("--drop-tables", action="store_true", help="Drop tables")
    parser.add_argument("--load-default-data", action="store_true", help="Load default data")
    parser.add_argument("--import-csv", action="store_true", help="Import data from CSV files")
    parser.add_argument("--articulos-csv", type=str, help="Path to articulos CSV file")
    parser.add_argument("--metodo-pago-csv", type=str, help="Path to metodo_pago CSV file")
    parser.add_argument('--dbname', default='postgres', help='Database name')
    parser.add_argument('--user', default='postgres', help='Database user')
    parser.add_argument('--password', default='postgres', help='Database password')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', default='5432', help='Database port')
    
    return parser.parse_args()


def create_tables(db_connection):
    """
    Create database tables.
    
    Args:
        db_connection (DatabaseConnection): Database connection instance
        
    Returns:
        bool: Success status
    """
    success = True
    
    # Create specific tables for articulos and metodo_pago
    articulos_table = ArticulosTable(db_connection)
    if not articulos_table.create_table():
        print("Failed to create articulos table")
        success = False
    
    metodo_pago_table = MetodoPagoTable(db_connection)
    if not metodo_pago_table.create_table():
        print("Failed to create metodo_pago table")
        success = False
    
    # Create caja_banco table
    caja_banco_table = CajaBancoTable(db_connection)
    if not caja_banco_table.create_table():
        print("Failed to create caja_banco table")
        success = False
    
    # Create clientes table
    clientes_table = ClientesTable(db_connection)
    if not clientes_table.create_table():
        print("Failed to create clientes table")
        success = False
    
    # Create detalle_estado table
    detalle_estado_table = DetalleEstadoTable(db_connection)
    if not detalle_estado_table.create_table():
        print("Failed to create detalle_estado table")
        success = False
    
    # Create errores table
    errores_table = ErroresTable(db_connection)
    if not errores_table.create_table():
        print("Failed to create errores table")
        success = False
    
    # Create estado_factura_venta table
    estado_factura_venta_table = EstadoFacturaVentaTable(db_connection)
    if not estado_factura_venta_table.create_table():
        print("Failed to create estado_factura_venta table")
        success = False
    
    # Create tipo_movimiento table
    tipo_movimiento_table = TipoMovimientoTable(db_connection)
    if not tipo_movimiento_table.create_table():
        print("Failed to create tipo_movimiento table")
        success = False
    
    # Create reintentos_fac_venta table
    reintentos_fac_venta_table = ReintentosFacturaVentaTable(db_connection)
    if not reintentos_fac_venta_table.create_table():
        print("Failed to create reintentos_fac_venta table")
        success = False
    
    # Create recibo_venta table
    recibo_venta_table = ReciboVentaTable(db_connection)
    if not recibo_venta_table.create_table():
        print("Failed to create recibo_venta table")
        success = False
    
    # Create proveedor table
    proveedor_table = ProveedorTable(db_connection)
    if not proveedor_table.create_table():
        print("Failed to create proveedor table")
        success = False
    
    # Create pais table
    pais_table = PaisTable(db_connection)
    if not pais_table.create_table():
        print("Failed to create pais table")
        success = False
    
    # Create forma_pago table
    forma_pago_table = FormaPagoTable(db_connection)
    if not forma_pago_table.create_table():
        print("Failed to create forma_pago table")
        success = False
    
    # Create forma_pago_caja_banco table
    forma_pago_caja_banco_table = FormaPagoCajaBancoTable(db_connection)
    if not forma_pago_caja_banco_table.create_table():
        print("Failed to create forma_pago_caja_banco table")
        success = False
    
    # Create general_misc table
    general_misc_table = GeneralMiscTable(db_connection)
    if not general_misc_table.create_table():
        print("Failed to create general_misc table")
        success = False
    
    # Create iva table
    iva_table = IvaTable(db_connection)
    if not iva_table.create_table():
        print("Failed to create iva table")
        success = False
    
    return success


def drop_tables(db_connection):
    """
    Drop database tables.
    
    Args:
        db_connection (DatabaseConnection): Database connection instance
        
    Returns:
        bool: Success status
    """
    # This is a simplified approach - in a real application, you'd need to handle dependencies
    # Drop tables in reverse order of creation to handle foreign key constraints
    success = True
    
    # Drop metodo_pago table
    query = "DROP TABLE IF EXISTS metodo_pago CASCADE;"
    cursor = db_connection.execute_query(query)
    if cursor is None:
        print("Failed to drop metodo_pago table")
        success = False
    
    # Drop articulos table
    query = "DROP TABLE IF EXISTS articulos CASCADE;"
    cursor = db_connection.execute_query(query)
    if cursor is None:
        print("Failed to drop articulos table")
        success = False
    
    # Drop caja_banco table
    query = "DROP TABLE IF EXISTS caja_banco CASCADE;"
    cursor = db_connection.execute_query(query)
    if cursor is None:
        print("Failed to drop caja_banco table")
        success = False
    
    # Drop clientes table
    query = "DROP TABLE IF EXISTS clientes CASCADE;"
    cursor = db_connection.execute_query(query)
    if cursor is None:
        print("Failed to drop clientes table")
        success = False
    
    # Drop detalle_estado table
    query = "DROP TABLE IF EXISTS detalle_estado CASCADE;"
    cursor = db_connection.execute_query(query)
    if cursor is None:
        print("Failed to drop detalle_estado table")
        success = False
    
    # Drop errores table
    query = "DROP TABLE IF EXISTS errores CASCADE;"
    cursor = db_connection.execute_query(query)
    if cursor is None:
        print("Failed to drop errores table")
        success = False
    
    # Drop estado_factura_venta table
    query = "DROP TABLE IF EXISTS estado_factura_venta CASCADE;"
    cursor = db_connection.execute_query(query)
    if cursor is None:
        print("Failed to drop estado_factura_venta table")
        success = False
    
    # Drop tipo_movimiento table
    query = "DROP TABLE IF EXISTS tipo_movimiento CASCADE;"
    cursor = db_connection.execute_query(query)
    if cursor is None:
        print("Failed to drop tipo_movimiento table")
        success = False
    
    # Drop reintentos_fac_venta table
    query = "DROP TABLE IF EXISTS reintentos_fac_venta CASCADE;"
    cursor = db_connection.execute_query(query)
    if cursor is None:
        print("Failed to drop reintentos_fac_venta table")
        success = False
    
    # Drop recibo_venta table
    query = "DROP TABLE IF EXISTS recibo_venta CASCADE;"
    cursor = db_connection.execute_query(query)
    if cursor is None:
        print("Failed to drop recibo_venta table")
        success = False
    
    # Drop proveedor table
    query = "DROP TABLE IF EXISTS proveedor CASCADE;"
    cursor = db_connection.execute_query(query)
    if cursor is None:
        print("Failed to drop proveedor table")
        success = False
    
    # Drop pais table
    query = "DROP TABLE IF EXISTS pais CASCADE;"
    cursor = db_connection.execute_query(query)
    if cursor is None:
        print("Failed to drop pais table")
        success = False
    
    # Drop forma_pago table
    query = "DROP TABLE IF EXISTS forma_pago CASCADE;"
    cursor = db_connection.execute_query(query)
    if cursor is None:
        print("Failed to drop forma_pago table")
        success = False
    
    # Drop forma_pago_caja_banco table
    query = "DROP TABLE IF EXISTS forma_pago_caja_banco CASCADE;"
    cursor = db_connection.execute_query(query)
    if cursor is None:
        print("Failed to drop forma_pago_caja_banco table")
        success = False
    
    # Drop general_misc table
    query = "DROP TABLE IF EXISTS general_misc CASCADE;"
    cursor = db_connection.execute_query(query)
    if cursor is None:
        print("Failed to drop general_misc table")
        success = False
    
    # Drop iva table
    query = "DROP TABLE IF EXISTS iva CASCADE;"
    cursor = db_connection.execute_query(query)
    if cursor is None:
        print("Failed to drop iva table")
        success = False
    
    db_connection.commit()
    return success


def import_csv_data(db_connection, articulos_csv=None, metodo_pago_csv=None):
    """
    Import data from CSV files.
    
    Args:
        db_connection (DatabaseConnection): Database connection
        articulos_csv (str, optional): Path to articulos CSV file
        metodo_pago_csv (str, optional): Path to metodo_pago CSV file
        
    Returns:
        bool: Success status
    """
    success = True
    
    # Ensure CSV directory exists
    csv_dir = ensure_csv_directory()
    
    # Import articulos data
    print("Importing articulos data...")
    articulos_table = ArticulosTable(db_connection)
    if not articulos_table.import_from_csv(articulos_csv):
        print("Failed to import articulos data")
        success = False
    else:
        print("Articulos data imported successfully")
    
    # Import metodo_pago data
    print("Importing metodo_pago data...")
    metodo_pago_table = MetodoPagoTable(db_connection)
    if not metodo_pago_table.import_from_csv(metodo_pago_csv):
        print("Failed to import metodo_pago data")
        success = False
    else:
        print("Metodo_pago data imported successfully")
    
    return success


def load_default_data(db_connection):
    """
    Load default data for tables with predefined data.
    
    Args:
        db_connection (DatabaseConnection): Database connection instance
        
    Returns:
        bool: Success status
    """
    success = True
    
    # Load default data for caja_banco
    print("Loading default data for caja_banco...")
    caja_banco_table = CajaBancoTable(db_connection)
    if not caja_banco_table.setup(use_default_data=True):
        print("Failed to load default data for caja_banco")
        success = False
    
    # Load default data for clientes
    print("Loading default data for clientes...")
    clientes_table = ClientesTable(db_connection)
    if not clientes_table.setup(use_default_data=True):
        print("Failed to load default data for clientes")
        success = False
    
    # Load default data for tipo_movimiento
    print("Loading default data for tipo_movimiento...")
    tipo_movimiento_table = TipoMovimientoTable(db_connection)
    if not tipo_movimiento_table.setup(use_default_data=True):
        print("Failed to load default data for tipo_movimiento")
        success = False
    
    # Load default data for proveedor
    print("Loading default data for proveedor...")
    proveedor_table = ProveedorTable(db_connection)
    if not proveedor_table.setup(use_default_data=True):
        print("Failed to load default data for proveedor")
        success = False
    
    # Load default data for pais
    print("Loading default data for pais...")
    pais_table = PaisTable(db_connection)
    if not pais_table.setup(use_default_data=True):
        print("Failed to load default data for pais")
        success = False
    
    # Load default data for forma_pago
    print("Loading default data for forma_pago...")
    forma_pago_table = FormaPagoTable(db_connection)
    if not forma_pago_table.setup(use_default_data=True):
        print("Failed to load default data for forma_pago")
        success = False
    
    # Load default data for forma_pago_caja_banco
    print("Loading default data for forma_pago_caja_banco...")
    forma_pago_caja_banco_table = FormaPagoCajaBancoTable(db_connection)
    if not forma_pago_caja_banco_table.setup(use_default_data=True):
        print("Failed to load default data for forma_pago_caja_banco")
        success = False
    
    # Load default data for general_misc
    print("Loading default data for general_misc...")
    general_misc_table = GeneralMiscTable(db_connection)
    if not general_misc_table.setup(use_default_data=True):
        print("Failed to load default data for general_misc")
        success = False
    
    # Load default data for iva
    print("Loading default data for iva...")
    iva_table = IvaTable(db_connection)
    if not iva_table.setup(use_default_data=True):
        print("Failed to load default data for iva")
        success = False
    
    return success


def main():
    """
    Main function.
    """
    args = parse_arguments()
    
    # Create database connection
    db = DatabaseConnection(
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port
    )
    
    # Connect to the database
    if not db.connect():
        print("Failed to connect to the database. Exiting.")
        return 1
    
    try:
        # Process actions based on arguments
        if args.drop_tables:
            print("Dropping tables...")
            if not drop_tables(db):
                print("Failed to drop tables")
                return 1
        
        if args.create_tables:
            print("Creating tables...")
            if not create_tables(db):
                print("Failed to create tables")
                return 1
        
        if args.load_default_data:
            print("Loading default data for tables...")
            if not load_default_data(db):
                print("Failed to load default data")
                return 1
        
        if args.import_csv:
            print("Importing data from CSV files...")
            if not import_csv_data(db, args.articulos_csv, args.metodo_pago_csv):
                print("Failed to import data from CSV files")
                return 1
            print("CSV data imported successfully")
        
        print("Database installation completed successfully.")
        return 0
    
    finally:
        # Close the database connection
        db.close()


if __name__ == "__main__":
    sys.exit(main())
