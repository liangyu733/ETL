import os
import pandas as pd
import pymysql
import mysql_import as mi
from sqlalchemy import create_engine

MYSQL_USER = 'root'
MYSQL_PWD = 'my_password'
MYSQL_DB = 'my_database'
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306

engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PWD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4")

folder = r'my_file_path'
files = [
    ('table1', 'table1_v1.csv'),
    ('table2', 'table2_v1.csv'),
    ('table1', 'table1_v2.csv')  # demo .csv files
]

pk_map = {
    'table1': 'table1_id',
    'table2': 'table2_id'
}

for table_name, file_name in files:
    file_path = os.path.join(folder, file_name)
    df = pd.read_csv(file_path) # if .xlsx, use pd.read_excel
    primary_key = pk_map.get(table_name, None)
    mi.sql_import(
        df,
        table_name,
        engine,
        primary_key
    )