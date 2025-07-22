#!/usr/bin/env python
"""
Test script to verify that the .env and table_config.json files are being read correctly.
"""
import os
import json
from pathlib import Path
from dotenv import load_dotenv

def main():
    """Test file access for .env and table_config.json."""
    print("Testing file access...")
    
    # Get the directory of the executable
    if getattr(sys, 'frozen', False):
        # If the application is run as a bundle, the PyInstaller bootloader
        # extends the sys module by a flag frozen=True and sets the app 
        # path into variable _MEIPASS.
        application_path = os.path.dirname(sys.executable)
    else:
        application_path = os.path.dirname(os.path.abspath(__file__))
    
    print(f"Application path: {application_path}")
    
    # Test .env file access
    env_path = os.path.join(application_path, '.env')
    print(f"Looking for .env at: {env_path}")
    
    if os.path.exists(env_path):
        print(".env file found!")
        load_dotenv(dotenv_path=env_path)
        print(f"DB_NAME from .env: {os.getenv('DB_NAME')}")
        print(f"DB_USER from .env: {os.getenv('DB_USER')}")
    else:
        print("ERROR: .env file not found!")
    
    # Test table_config.json file access
    config_path = os.path.join(application_path, 'table_config.json')
    print(f"\nLooking for table_config.json at: {config_path}")
    
    if os.path.exists(config_path):
        print("table_config.json file found!")
        try:
            with open(config_path, 'r') as config_file:
                config = json.load(config_file)
                print(f"Number of tables in config: {len(config)}")
                print(f"Sample table config - articulos: {config.get('articulos', 'Not found')}")
        except Exception as e:
            print(f"ERROR reading table_config.json: {e}")
    else:
        print("ERROR: table_config.json file not found!")
    
    print("\nTest completed!")

if __name__ == "__main__":
    import sys
    sys.exit(main())
