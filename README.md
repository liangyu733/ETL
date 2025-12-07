# ETL
A simple ETL workflow for data management of a company that used to work through EXCEL only. The raw data would be transformed to .csv and cleaned in Python. After data washing, a local MySQL database has been created as the Data Warehouse.

# `mysql_import` Module

## Overview

`sql_import` is a Python function designed to import a Pandas DataFrame into a MySQL database. It supports automatic table creation, upsert (update or insert), historical record tracking, and dynamic adjustment of string column lengths.

**Key Features:**

1. **Automatic Table Creation**

   * Automatically detects SQL data types based on the DataFrame columns (BIGINT, DOUBLE, DATETIME, VARCHAR, TEXT).
   * Adds an `import_time` TIMESTAMP column automatically.

2. **History Table and Trigger**

   * Creates a `_history` table to store previous versions of updated rows.
   * Automatically sets up a `BEFORE UPDATE` trigger to log old data.

3. **Upsert (Update or Insert)**

   * Compares primary keys:

     * If the primary key exists and other columns change → updates the record and updates `import_time`.
     * If the primary key does not exist → inserts a new record.

4. **Dynamic VARCHAR Length Adjustment**

   * If incoming string data exceeds the current column length, it automatically increases the length or converts the column to TEXT.

## Installation

```bash
pip install pandas numpy pymysql sqlalchemy
```

## Usage

### 1. Import Module and Create MySQL Connection

```python
from sqlalchemy import create_engine
import sql_import as si  # your sql_import.py file

MYSQL_USER = 'root'
MYSQL_PWD  = 'password'
MYSQL_DB   = 'my_database'
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306

engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PWD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
)
```

### 2. Import a DataFrame

```python
import pandas as pd

df = pd.read_csv("table1_v1.csv")  # or pd.read_excel("table1_v1.xlsx")

si.sql_import(
    df,
    table_name="table1",
    engine=engine,
    primary_key="table1_id"  # optional, if not provided the first column is used
)
```

### 3. Import Multiple Files Example

```python
import os

folder = r"my_file_path"
files = [
    ('table1', 'table1_v1.csv'),
    ('table2', 'table2_v1.csv'),
    ('table1', 'table1_v2.csv')
]

pk_map = {
    'table1': 'table1_id',
    'table2': 'table2_id'
}

for table_name, file_name in files:
    file_path = os.path.join(folder, file_name)
    df = pd.read_csv(file_path)
    primary_key = pk_map.get(table_name)
    si.sql_import(df, table_name, engine, primary_key)
```

### 4. History Table Explanation

* Each updated row is logged in the `table_name_history` table.
* `history_id` is an auto-increment primary key.
* `import_time` records the timestamp of the original import.

### 5. Notes

1. Column names are automatically stripped of leading and trailing spaces.
2. Empty strings or strings containing only whitespace are converted to `NULL`.
3. A primary key must exist, otherwise the first column is used by default.
4. Triggers are only generated the first time a table is created.
5. During upsert, `import_time` is automatically updated for modified rows.

## Example Data

| Table  | Columns                      | Type                             | v1 Rows | v2 Rows                  |
| ------ | ---------------------------- | -------------------------------- | ------- | ------------------------ |
| table1 | table1_id, name, age, status | BIGINT, VARCHAR, BIGINT, VARCHAR | 10      | 20 (10 updated + 10 new) |
| table2 | table2_id, category, value   | BIGINT, VARCHAR, DOUBLE          | 10      | N/A                      |

* v2 of `table1` includes 10 new rows (IDs 11-20) and updates 3 randomly selected rows from the original 1-10.

---