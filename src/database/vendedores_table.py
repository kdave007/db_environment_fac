#!/usr/bin/env python
"""Vendedores table module."""

from src.database.connection import DatabaseConnection


class VendedoresTable:
    """Vendedores table class."""

    def __init__(self, db_connection: DatabaseConnection):
        """Initialize the class with a database connection.

        Args:
            db_connection: Database connection instance
        """
        self.db_connection = db_connection

    def create_table(self):
        """Create the vendedores table.

        Returns:
            bool: True if successful, False otherwise
        """
        query = """
        CREATE TABLE IF NOT EXISTS vendedores (
            velneo INTEGER NOT NULL,
            pvsi_clave VARCHAR(10) NOT NULL,
            nombre VARCHAR(100) NOT NULL
        );
        """
        cursor = self.db_connection.execute_query(query)
        if cursor is None:
            return False
        self.db_connection.commit()
        return True

    def insert_default_data(self):
        """Insert default data into the vendedores table.

        Returns:
            bool: True if successful, False otherwise
        """
        query = """
        INSERT INTO vendedores (velneo, pvsi_clave, nombre)
        VALUES (1, '1', 'Ejemplo')
        ON CONFLICT DO NOTHING;
        """
        cursor = self.db_connection.execute_query(query)
        if cursor is None:
            return False
        self.db_connection.commit()
        return True

    def setup(self):
        """Set up the vendedores table.

        Returns:
            bool: True if successful, False otherwise
        """
        if not self.create_table():
            return False
        if not self.insert_default_data():
            return False
        return True
