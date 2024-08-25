import os
import gdown
import duckdb
import pandas as pd
from internals.file_manager import file_manager
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from sqlalchemy import Connection

def load_data_into_postgres(new_data: DuckDBPyRelation, table: str) -> None:
        
        DB_URL: str = os.getenv("DATABASE_URL")
        
        engine: Connection = create_engine(DB_URL)
        
        df = new_data.df().drop(columns={"idCompra"})
        
        df.columns = df.columns.str.lower()
        
        df.to_sql(table, con=engine, if_exists='append', index=False)
        
        return

if __name__ == "__main__":
    
    load_dotenv()
    
    url_folder: str = os.getenv("URL_DRIVE")
    local_dir: str = './gdown_folder'
    
    manager = file_manager(local_dir, url_folder)
    
    # manager.download_files_from_url()
    
    csv_files: DuckDBPyRelation = manager.csv_files()
    
    table: str = "vendas"
    
    load_data_into_postgres(csv_files, table)  
    