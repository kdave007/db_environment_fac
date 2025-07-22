"""
Configuration module for the database installer.
Loads environment variables from .env file.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Database configuration
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')

# CSV file paths
CSV_DIRECTORY = os.getenv('CSV_DIRECTORY', 'data/csv')
ARTICULOS_CSV = os.getenv('ARTICULOS_CSV', 'articulos.csv')
METODO_PAGO_CSV = os.getenv('METODO_PAGO_CSV', 'metodo_pago.csv')
GENERAL_MISC_CSV = os.getenv('GENERAL_MISC_CSV', 'general_misc.csv')

# Default data loading
LOAD_DEFAULT_DATA = os.getenv('LOAD_DEFAULT_DATA', 'true').lower() == 'true'

# Ensure CSV directory exists
def ensure_csv_directory():
    """
    Ensure the CSV directory exists.
    """
    csv_dir = Path(__file__).parent.parent / CSV_DIRECTORY
    if not csv_dir.exists():
        csv_dir.mkdir(parents=True)
    return csv_dir
