import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from typing import Optional, List
import glob
from dotenv import load_dotenv

load_dotenv()

class QuestDBClient:
    """
    QuestDB client for creating and managing OHLCV1d time-series data
    """
    
    def __init__(self):
        self.host = os.getenv('QUESTDB_HOST', 'localhost')
        self.port = int(os.getenv('QUESTDB_PORT', '8812'))  # PostgreSQL wire protocol port
        self.username = os.getenv('QUESTDB_USERNAME', 'admin')
        self.password = os.getenv('QUESTDB_PASSWORD', 'quest')
        self.database = 'qdb'  # Default QuestDB database name
        
        self.parquet_dir = os.getenv('PARQUET_DIR', './data/parquet')
        
    def get_connection(self):
        """
        Create connection to QuestDB using PostgreSQL wire protocol
        """
        try:
            connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.database
            )
            return connection
        except psycopg2.Error as e:
            print(f"Error connecting to QuestDB: {e}")
            return None
    
    def create_ohlcv_table(self):
        """
        Create OHLCV1d table in QuestDB with optimized schema
        """
        connection = self.get_connection()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            
            # Drop table if exists
            cursor.execute("DROP TABLE IF EXISTS ohlcv1d")
            
            # Create table with designated timestamp and partitioning
            create_table_sql = """
            CREATE TABLE ohlcv1d (
                timestamp TIMESTAMP,
                symbol SYMBOL CAPACITY 128 CACHE,
                etf_name STRING,
                index_tracked SYMBOL CAPACITY 32 CACHE,
                expense_ratio DOUBLE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                adj_close DOUBLE,
                volume LONG,
                daily_return DOUBLE,
                volatility_20d DOUBLE,
                change DOUBLE,
                change_percent DOUBLE,
                vwap DOUBLE
            ) TIMESTAMP(timestamp) PARTITION BY DAY WAL
            """
            
            cursor.execute(create_table_sql)
            connection.commit()
            
            print("Successfully created ohlcv1d table in QuestDB")
            return True
            
        except psycopg2.Error as e:
            print(f"Error creating table: {e}")
            return False
        finally:
            if connection:
                connection.close()
    
    def load_parquet_to_questdb(self, parquet_file: str):
        """
        Load data from parquet file to QuestDB
        """
        if not os.path.exists(parquet_file):
            print(f"Parquet file not found: {parquet_file}")
            return False
        
        try:
            # Read parquet file
            df = pd.read_parquet(parquet_file)
            
            # Prepare data for QuestDB
            df = self.prepare_dataframe_for_questdb(df)
            
            if df.empty:
                print(f"No data to load from {parquet_file}")
                return False
            
            # Insert data into QuestDB
            return self.insert_dataframe_to_questdb(df)
            
        except Exception as e:
            print(f"Error loading parquet file {parquet_file}: {e}")
            return False
    
    def prepare_dataframe_for_questdb(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for QuestDB insertion
        """
        # Create a copy to avoid modifying original
        df_clean = df.copy()
        
        # Rename columns to match QuestDB schema
        column_mapping = {
            'date': 'timestamp',
            'adjClose': 'adj_close'
        }
        
        df_clean = df_clean.rename(columns=column_mapping)
        
        # Ensure timestamp is datetime
        if 'timestamp' in df_clean.columns:
            df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'])
        
        # Select only columns that exist in both DataFrame and our schema
        required_columns = [
            'timestamp', 'symbol', 'etf_name', 'index_tracked', 'expense_ratio',
            'open', 'high', 'low', 'close', 'adj_close', 'volume',
            'daily_return', 'volatility_20d', 'change', 'change_percent', 'vwap'
        ]
        
        # Keep only columns that exist in the DataFrame
        available_columns = [col for col in required_columns if col in df_clean.columns]
        df_clean = df_clean[available_columns]
        
        # Fill NaN values
        numeric_columns = df_clean.select_dtypes(include=['float64', 'int64']).columns
        df_clean[numeric_columns] = df_clean[numeric_columns].fillna(0)
        
        # Fill string NaN values
        string_columns = df_clean.select_dtypes(include=['object']).columns
        df_clean[string_columns] = df_clean[string_columns].fillna('')
        
        return df_clean
    
    def insert_dataframe_to_questdb(self, df: pd.DataFrame) -> bool:
        """
        Insert DataFrame data into QuestDB
        """
        connection = self.get_connection()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            
            # Prepare insert query
            columns = list(df.columns)
            placeholders = ','.join(['%s'] * len(columns))
            insert_sql = f"INSERT INTO ohlcv1d ({','.join(columns)}) VALUES ({placeholders})"
            
            # Convert DataFrame to list of tuples
            data_tuples = [tuple(row) for row in df.to_numpy()]
            
            # Batch insert using execute_values for better performance
            execute_values(
                cursor, 
                insert_sql, 
                data_tuples,
                template=None,
                page_size=1000
            )
            
            connection.commit()
            print(f"Successfully inserted {len(df)} records into QuestDB")
            return True
            
        except psycopg2.Error as e:
            print(f"Error inserting data into QuestDB: {e}")
            connection.rollback()
            return False
        finally:
            if connection:
                connection.close()
    
    def load_all_parquet_files(self):
        """
        Load all parquet files from the parquet directory
        """
        parquet_files = glob.glob(os.path.join(self.parquet_dir, "*.parquet"))
        
        if not parquet_files:
            print("No parquet files found in the directory")
            return False
        
        print(f"Found {len(parquet_files)} parquet files")
        
        success_count = 0
        for parquet_file in parquet_files:
            print(f"Loading {os.path.basename(parquet_file)}...")
            if self.load_parquet_to_questdb(parquet_file):
                success_count += 1
        
        print(f"Successfully loaded {success_count}/{len(parquet_files)} files")
        return success_count > 0
    
    def query_ohlcv_data(self, symbol: str = None, start_date: str = None, end_date: str = None, limit: int = 100):
        """
        Query OHLCV data from QuestDB
        """
        connection = self.get_connection()
        if not connection:
            return None
        
        try:
            cursor = connection.cursor()
            
            # Build query
            query = "SELECT * FROM ohlcv1d"
            conditions = []
            params = []
            
            if symbol:
                conditions.append("symbol = %s")
                params.append(symbol)
            
            if start_date:
                conditions.append("timestamp >= %s")
                params.append(start_date)
            
            if end_date:
                conditions.append("timestamp <= %s")
                params.append(end_date)
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY timestamp DESC"
            
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description]
            
            # Convert to DataFrame
            df = pd.DataFrame(results, columns=column_names)
            
            print(f"Retrieved {len(df)} records from QuestDB")
            return df
            
        except psycopg2.Error as e:
            print(f"Error querying data: {e}")
            return None
        finally:
            if connection:
                connection.close()
    
    def get_table_info(self):
        """
        Get information about the ohlcv1d table
        """
        connection = self.get_connection()
        if not connection:
            return None
        
        try:
            cursor = connection.cursor()
            
            # Get table schema
            cursor.execute("SHOW COLUMNS FROM ohlcv1d")
            columns = cursor.fetchall()
            
            # Get row count
            cursor.execute("SELECT COUNT(*) FROM ohlcv1d")
            row_count = cursor.fetchone()[0]
            
            print("QuestDB Table Information:")
            print("="*50)
            print(f"Table: ohlcv1d")
            print(f"Total rows: {row_count:,}")
            print("\nColumns:")
            for col in columns:
                print(f"  {col[0]:15} | {col[1]}")
            print("="*50)
            
            return {'columns': columns, 'row_count': row_count}
            
        except psycopg2.Error as e:
            print(f"Error getting table info: {e}")
            return None
        finally:
            if connection:
                connection.close()

if __name__ == "__main__":
    client = QuestDBClient()
    
    # Create table
    client.create_ohlcv_table()
    
    # Load all parquet files
    client.load_all_parquet_files()
    
    # Get table information
    client.get_table_info()
    
    # Sample query
    sample_data = client.query_ohlcv_data(symbol='SPY', limit=10)
    if sample_data is not None:
        print("\nSample SPY data:")
        print(sample_data.head())
