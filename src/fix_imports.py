"""
Helper module to fix imports when running as an executable
"""
import os
import sys

def fix_imports():
    """Add necessary paths to sys.path for proper module resolution"""
    # Get the directory of the executable or script
    if getattr(sys, 'frozen', False):
        # Running as a PyInstaller executable
        base_dir = os.path.dirname(sys.executable)
    else:
        # Running as a script
        base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Add the base directory to sys.path
    if base_dir not in sys.path:
        sys.path.insert(0, base_dir)
    
    # Add the src directory to sys.path
    src_dir = os.path.join(base_dir, 'src')
    if os.path.exists(src_dir) and src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    
    # Print paths for debugging
    print("Python path:")
    for path in sys.path:
        print(f"  {path}")
