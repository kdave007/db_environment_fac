#!/usr/bin/env python
"""
Wrapper script for the database installer.
This script ensures proper module imports when running as an executable.
"""
import os
import sys
import importlib.util

def main():
    """Main wrapper function"""
    # Get the directory of the executable or script
    if getattr(sys, 'frozen', False):
        # Running as a PyInstaller executable
        base_dir = os.path.dirname(sys.executable)
    else:
        # Running as a script
        base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Add the base directory and src directory to sys.path
    if base_dir not in sys.path:
        sys.path.insert(0, base_dir)
    
    src_dir = os.path.join(base_dir, 'src')
    if os.path.exists(src_dir) and src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    
    # Print paths for debugging
    print("Python path:")
    for path in sys.path:
        print(f"  {path}")
    
    # Import and run the main module
    try:
        # Try to import the main module
        if getattr(sys, 'frozen', False):
            # When running as executable, import from the bundled modules
            from src import main
            return main.main()
        else:
            # When running as script, import from the src directory
            spec = importlib.util.spec_from_file_location("main", os.path.join(src_dir, "main.py"))
            main_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(main_module)
            return main_module.main()
    except ImportError as e:
        print(f"Error importing main module: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
