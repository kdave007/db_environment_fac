"""
Database connection module.
Handles connections to the PostgreSQL database system.
"""
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_batch as pg_execute_batch


class DatabaseConnection:
    """PostgreSQL database connection manager."""
    
    def __init__(self, dbname="postgres", user="postgres", password="postgres", 
                 host="localhost", port="5432"):
        """
        Initialize database connection.
        
        Args:
            dbname (str): Database name
            user (str): Database user
            password (str): Database password
            host (str): Database host
            port (str): Database port
        """
        self.connection_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Establish connection to the database."""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            self.cursor = self.connection.cursor()
            print(f"Connected to PostgreSQL database: {self.connection_params['dbname']} on {self.connection_params['host']}")
            return True
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            return False
    
    def execute_query(self, query, params=None):
        """
        Execute a query on the database.
        
        Args:
            query (str): SQL query to execute
            params (tuple, optional): Parameters for the query
            
        Returns:
            cursor: Query result cursor
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor
        except psycopg2.Error as e:
            print(f"Error executing query: {e}")
            print(f"Query: {query}")
            if params:
                print(f"Parameters: {params}")
            return None
    
    def execute_many(self, query, params_list):
        """
        Execute a query multiple times with different parameters.
        
        Args:
            query (str): SQL query to execute
            params_list (list): List of parameter tuples
            
        Returns:
            bool: Success status
        """
        try:
            self.cursor.executemany(query, params_list)
            return True
        except psycopg2.Error as e:
            print(f"Error executing batch query: {e}")
            print(f"Query: {query}")
            return False
    
    def execute_batch(self, query, params_list):
        """
        Execute a batch of queries using psycopg2.extras.execute_batch.
        This is more efficient than executemany for large datasets.
        
        Args:
            query (str): SQL query to execute
            params_list (list): List of parameter tuples
            
        Returns:
            cursor: Query result cursor or None on failure
        """
        try:
            pg_execute_batch(self.cursor, query, params_list)
            return self.cursor
        except psycopg2.Error as e:
            print(f"Error executing batch: {e}")
            self.connection.rollback()
            return None
    
    def commit(self):
        """Commit changes to the database."""
        if self.connection:
            self.connection.commit()
    
    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.cursor = None
            print("Database connection closed")
