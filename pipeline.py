import os
import gdown
import duckdb
import pandas as pd
from internals.file_manager import file_manager
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

from duckdb import DuckDBPyRelation
from duckdb import DuckDBPyConnection
from sqlalchemy import Connection
    
def load_data_into_postgres(new_data: DuckDBPyRelation, table: str) -> None:
        
        DB_URL: str = os.getenv("DATABASE_URL")
        
        engine: Connection = create_engine(DB_URL)
        
        df = new_data.df().drop(columns={"idCompra"})
        
        df.columns = df.columns.str.lower()
        
        df.to_sql(table, con=engine, if_exists='append', index=False)
        
        return
    
def con_duckdb() -> DuckDBPyConnection:
    """"Conect to db, create if not exist"""
    return duckdb.connect(database="./data/duckdb.db", read_only=False)

if __name__ == "__main__":
    
    load_dotenv()
    
    url_folder: str = os.getenv("URL_DRIVE")
    local_dir: str = './gdown_folder'
    
    con = con_duckdb()
    
    manager = file_manager(local_dir, url_folder, con)
    
    # manager.download_files_from_url()
    
    csv_files: DuckDBPyRelation = manager.csv_files()
    
    try:
    
        table: str = "vendas"
        
        load_data_into_postgres(csv_files, table)
    
        con.close() 
        
    except:
        con.close()
        pass
        
    