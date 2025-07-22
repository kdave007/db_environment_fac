"""
Implementation of the caja_banco table using the TableBlueprint class.
"""
from .table_blueprint import TableBlueprint


class CajaBancoTable(TableBlueprint):
    """Handler for the 'caja_banco' table."""
    
    def __init__(self, db_connection):
        """Initialize the caja_banco table handler."""
        super().__init__(db_connection, "caja_banco")
    
    def get_create_query(self):
        """
        Get the SQL query to create the caja_banco table.
        
        Returns:
            str: SQL query
        """
        return """
        CREATE TABLE IF NOT EXISTS caja_banco(
            velneo INTEGER,
            pvsi TEXT,
            descripcion TEXT
        );
        """
    
    def get_insert_query(self):
        """
        Get the SQL query to insert data into the caja_banco table.
        
        Returns:
            str: SQL query
        """
        return """
        INSERT INTO caja_banco (velneo, pvsi, descripcion) 
        VALUES (%s, %s, %s)
        """
    
    def insert_default_data(self):
        """
        Insert the default data into the caja_banco table.
        
        Returns:
            bool: Success status
        """
        data = [
            (9, 'CH', 'CHEQUE'),
            (10, 'CI', 'CUENTAS INCOBRABLES'),
            (11, 'ET', 'TRANSFERENCIA ELECTRONICA'),
            (12, 'IC', 'INTERCAMBIO COMERCIAL'),
            (13, 'PE', 'PAGO EN ESPECIE'),
            (14, 'S6', 'SANTANDER S6'),
            (15, 'TA', 'AMERICAN EXPRESS TA'),
            (16, 'F1', 'FACTORAJE FINANCIERO'),
            (17, 'F2', 'FACTORAJE FINANCIERO'),
            (18, 'R+', 'AJUSTE DE COBRANZA (MAS)'),
            (19, 'R-', 'AJUSTE DE COBRANZA (MENOS)'),
            (20, 'T9', 'BANAMEX T9'),
            (21, 'TF', 'BANAMEX T16'),
            (22, 'TC', 'BANAMEX TC'),
            (23, 'TD', 'BANAMEX TD'),
            (24, 'PP', 'Pago de puntos Club'),
            (25, 'B3', 'BANCOMER B3'),
            (26, 'B6', 'BANCOMER B6'),
            (27, 'B1', 'BANCOMER 12'),
            (28, 'EF', 'EFECTIVO'),
            (29, 'T1', 'Banamex T12'),
            (30, 'T3', 'BANAMEX T3'),
            (31, 'T6', 'BANAMEX T6'),
            (32, 'TO', 'BANAMEX T18'),
            (33, 'TA', 'AMERICAN EXPRESS TA'),
            (34, 'BC', 'BANCOMER CREDITO'),
            (35, 'BD', 'BANCOMER DEBITO'),
            (36, 'P3', 'NET PAY 3 MESES PROSA'),
            (37, 'MJ', 'MEJORAVIT'),
            (38, 'AN', 'ANTICIPO'),
            (39, 'EB', 'DEP BANCO EFECTIVO'),
            (40, 'DD', 'DEP BANCO EFECTIVO'),
            (41, 'CP', 'COPPEL'),
            (42, 'NC', 'NET PAY CREDITO'),
            (43, 'ND', 'NET PAY DEBITO'),
            (44, 'N3', 'NET PAY 3 MESES'),
            (45, 'N6', 'NET PAY 6 MESES'),
            (46, 'N9', 'NET PAY 9 MESES'),
            (47, 'NA', 'NET PAY AMERICAN EXPRESS'),
            (48, 'P3', 'NET PAY 3 MESES PROSA'),
            (49, 'P6', 'NETPAY  6 MESES PROSA'),
            (50, 'P9', 'NET PAY 9 MESES PROSA'),
            (51, 'CN', 'NET PAY CREDITO'),
            (52, 'MC', 'NETPAY MINI CREDITO'),
            (53, 'MD', 'NETPAY MINI DEBITO'),
            (54, 'M3', 'NETPAY MINI 3 MESES'),
            (55, 'M6', 'NETPAY MINI 6 MESES'),
            (56, 'M9', 'NETPAY MINI 9 MESES'),
            (57, 'M1', 'NETPAY MINI 12 MESES'),
            (58, 'MA', 'NETPAY MINI AMEX'),
            (59, 'A3', 'NETPAY MINI AMEX 3 MESES'),
            (60, 'A6', 'NETPAY MINI  AMEX 6 MESES'),
            (61, 'Y3', 'NETPAY MINI PROSA 3 MESES'),
            (62, 'Y6', 'NETPAY MINI PROSA 6 MESES'),
            (63, 'Y9', 'NETPAY MINI PROSA 9 MESES'),
            (64, 'YD', 'NETPAY MINI PROSA DEBITO'),
            (65, 'YC', 'NETPAYMINI PROSA CREDITO'),
            (66, 'BB', 'TARJETA BIENESTAR'),
            (67, 'LT', 'ECOMMER TRANSFERENCIA'),
            (68, 'LD', 'ECOMMER TARJ DEBITO'),
            (69, 'LC', 'ECOMMER TARJ CREDITO'),
            (70, 'L3', 'ECOMMER 3 MESES'),
            (71, 'L6', 'ECOMMER 6 MESES'),
            (72, 'LA', 'ECOMMER AMEX'),
            (73, 'N1', 'NETPAY AMEX 12 MESES'),
            (74, 'A1', 'NETPAY AMEX 12 MESES'),
            (75, 'IT', 'NETPAY INTERNATIONAL'),
            (76, 'Y6', 'NETPAY MINI PROSA 6 MESES'),
            (77, 'L9', 'ECOMMER 9 MESES'),
            (78, 'L1', 'ECCOMER 12 MESES'),
            (79, 'SV', 'SI VALE'),
            (80, 'PM', 'Puntos Monedero'),
            (81, 'MT', 'AMEX MINI NETPAY 3 MESES'),
            (82, 'MS', 'AMEX MININETPAY 6 MESES'),
            (83, 'I3', 'NETPAY INALAMBRICA 3 MESES'),
            (84, 'I6', 'NETPAY INALAMBRICA 6 MESES'),
            (85, 'I1', 'NETPAY INALAMBRICA 12 MESES'),
            (86, 'ID', 'NETPAY INALAMBRICA DEBITO'),
            (87, 'IR', 'NETPAY INALAMBRICA CREDITO'),
            (88, 'P1', 'NETPAY 12 MESES'),
            (89, 'A9', 'NETPAY MINI  AMEX 9 MESES'),
            (90, 'I9', 'NETPAY INALAMBRICA 9 MESES'),
            (91, 'SA', 'SATISFACCION DEL ACREEDOR'),
            (92, 'FD', 'MIFEL DEBITO'),
            (93, 'FC', 'MIFEL CREDITO'),
            (94, 'Y1', 'NETPAY 12 MESES'),
            (95, 'NO', 'NETPAY 18 MESES'),
            (96, 'PS', 'Pago por subrogación'),
            (97, 'YO', 'NET PAY MINI PROSA 18 MESES'),
            (98, 'PO', 'NETPAY 18 MESES'),
            (99, 'IO', 'INALAMBRICA 18 MESES'),
            (100, 'YD', 'MINI NETPAY DEBITO'),
            (101, 'MO', 'NETPAY MINI 18 MESES'),
            (102, 'Q3', 'Kushki 3 meses  prosa'),
            (103, 'KD', 'KUSHKI DEBITO'),
            (104, 'KC', 'KUSKI CREDITO'),
            (105, 'K3', 'KUSHKI 3 MESES EGLOBAL'),
            (106, 'K6', 'KUSHKI 6 MESES GLOBAL'),
            (107, 'K8', 'KUSHKI 18 MESES EGLOBAL'),
            (108, 'K9', 'KUSHKI 9 MESES EGLOBAL'),
            (109, 'K1', 'KUSHKI 12 MESES EGLOBAL'),
            (110, 'Q6', 'KUSHKI 6 MESES PROSA'),
            (111, 'Q8', 'KUSKI 18 MESES PROSA'),
            (112, 'Q9', 'KUSHKI 9 MESES PROSA'),
            (113, 'KA', 'AMERICAN EXPRESS KUSHKI'),
            (114, 'Q1', 'Kushki 12 meses  prosa'),
            (115, 'KT', 'AMERICAN EXP KUSHKI 3 MESES'),
            (116, 'KS', 'AMERICAN EXP KUSHKI 6 MESES'),
            (117, 'KN', 'AMERICAN EXP KUSHKI 9 MESES'),
            (118, 'KP', 'AMERICAN EXP KUSHKI 12 MESES')
        ]
        
        return self.insert_manual_data(data)
    
    def setup(self, csv_file=None, manual_data=None, use_default_data=True):
        """
        Set up the caja_banco table (create and populate with default or provided data).
        
        Args:
            csv_file (str, optional): Path to CSV file for data import
            manual_data (list, optional): List of data tuples to insert manually
            use_default_data (bool, optional): Whether to use the default data
            
        Returns:
            bool: Success status
        """
        # Create the table
        if not self.create_table():
            return False
        
        success = True
        
        # Import data from CSV if provided
        if csv_file:
            csv_success, _ = self.import_from_csv(csv_file)
            if not csv_success:
                success = False
        
        # Insert manual data if provided
        if manual_data:
            manual_success = self.insert_manual_data(manual_data)
            if not manual_success:
                success = False
        
        # Insert default data if requested and no other data was provided
        if use_default_data and not csv_file and not manual_data:
            default_success = self.insert_default_data()
            if not default_success:
                success = False
        
        return success
