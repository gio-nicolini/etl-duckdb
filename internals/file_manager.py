import os
import gdown
import duckdb
from sqlalchemy import create_engine
from datetime import datetime

from duckdb import DuckDBPyRelation
from duckdb import DuckDBPyConnection
from pandas import DataFrame
from sqlalchemy import Connection

class file_manager:
    
    def __init__(self, local_dir: str, url_folder: str, con:DuckDBPyConnection) -> None:
        
        self.__local_dir = local_dir
        self.__url_folder = url_folder
        self.__con = con
        
        pass
    
    def init_table(self) -> None:
        """Create table if not exist"""
        
        path_query: str = '../querys/history_files.sql'
        
        with open(path_query, 'r') as file_query:
            
            query = file_query.read()
            
        file_query.close()
        
        self.__con.execute(query)
        
        return
        
    def file_registry(self, file_name: list[str]) -> None:
        """Add new file into history files on db"""
        
        path_query: str = '../querys/add_files.sql'
        
        with open(path_query, 'r') as file_query:
            
            query = file_query.read()
            
        file_query.close()
        
        
        inserts: list = [(file, datetime.now()) for file in file_name]
        
        if len(inserts) > 1:
            self.__con.executemany(query, inserts)
            
        else:
            self.__con.execute(query, inserts)
        
        return
    
    def download_files_from_url(self) -> None:
        
        os.makedirs(self.__local_dir, exist_ok=True)
        gdown.download_folder(self.__url_folder, output=self.__local_dir)
        
        return 
    
    def csv_files(self) -> list[DuckDBPyRelation]:
        
        # Select files to read
        self.init_table()
        
        # Read query
        path_query = './querys/tracking_files.sql'
        
        with open(path_query, 'r') as file_query:
            
            query = file_query.read()
            
        file_query.close()
        
        # Files already read
        files_in_db: set = set(row[0] for row in self.__con.execute(query).fetchall())
        
        # Files on stage
        files_in_dir: set = set(file for file in os.listdir(self.__local_dir))
        
        files_to_read: list = [os.path.join(self.__local_dir, file) for file in files_in_dir.difference(files_in_db)]
        
        if len(files_to_read) > 1:
            # Read files with duckdb
            sales: DuckDBPyRelation = duckdb.read_csv(files_to_read)
            
            # Add files to read list in db
            self.file_registry(list(files_in_dir))
            
            # Add total sales
            transform_sales: DuckDBPyRelation = duckdb.sql("SELECT *, quantidade * valorUnitario AS totalSales FROM sales")
            
            return sales
            
        else:
            
            pass
        
        return
    
    
        
        
        