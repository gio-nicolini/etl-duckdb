import os
import gdown
import duckdb
from sqlalchemy import create_engine

from duckdb import DuckDBPyRelation
from pandas import DataFrame
from sqlalchemy import Connection

class file_manager:
    
    def __init__(self, local_dir: str, url_folder: str) -> None:
        
        self.__local_dir = local_dir
        self.__url_folder = url_folder
        
        pass
    
    def download_files_from_url(self) -> None:
        
        os.makedirs(self.__local_dir, exist_ok=True)
        gdown.download_folder(self.__url_folder, output=self.__local_dir)
        
        return 
    
    def csv_files(self) -> list[DuckDBPyRelation]:
        
        # 
        sales: DuckDBPyRelation = duckdb.read_csv(f"{self.__local_dir}/*.csv")
        # Add total sales
        transform_sales: DuckDBPyRelation = duckdb.sql("SELECT *, quantidade * valorUnitario AS totalSales FROM sales")
        
        return sales
    
    
        
        
        