import os
import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import text
from datetime import datetime

def sql_import(
        df: pd.DataFrame,
        table_name: str,
        engine,
        primary_key:str = None,
        len_wt: float = 1.2,
        len_lim: int = 1000
):
    df.columns = df.columns.str.strip()
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()
    df = df.replace(r"^\s*$", pd.NA, regex = True)

    col_defs = []
    for col in df.columns:
        tmp = df[col]
        if pd.api.types.is_integer_dtype(tmp):
            sql_type = 'BIGINT'
        elif pd.api.types.is_float_dtype(tmp):
            sql_type = 'DOUBLE'
        elif pd.api.types.is_datetime64_any_dtype(tmp):
            sql_type = 'DATETIME'
        else:
            lens = tmp.dropna().astype(str).map(len)
            if lens.empty:
                max_len = 10
            else:
                max_len = int(np.ceil(lens.max() * len_wt))

            if max_len > len_lim:
                sql_type = 'TEXT'
            else:
                sql_type = f"VARCHAR({max_len})"
        col_defs.append(f'`{col}` {sql_type}')

    if (primary_key is None) or (primary_key.strip() not in df.columns):
        primary_key = df.columns[0]
    else:
        primary_key = primary_key.strip()
    
    col_defs.append('`import_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
    col_defs_history = col_defs.copy()
    col_defs_history.insert(0, '`history_id` BIGINT NOT NULL AUTO_INCREMENT')
    col_defs_history.append('PRIMARY KEY (`history_id`)')
    col_defs.append(f'PRIMARY KEY (`{primary_key}`)')

    with engine.begin() as conn:
        result = conn.execute(text(f"SHOW TABLES LIKE '{table_name}';"))
        exists = result.fetchone() is not None
        if not exists:
            # 1. Create main table
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                {', '.join(col_defs)}
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            conn.execute(text(create_sql))

            # 2. Create history table
            create_sql_history = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}_history` (
                {', '.join(col_defs_history)}
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            conn.execute(text(create_sql_history))

            # 3. Create trigger for history table
            trigger_cols = df.columns.difference([primary_key])
            trigger_sql = f"""
                CREATE TRIGGER `trg_{table_name}_history`
                BEFORE UPDATE ON `{table_name}`
                FOR EACH ROW
                BEGIN
                    IF NOT ({
                        ' AND '.join([
                            f"OLD.`{col}` <=> NEW.`{col}`" 
                            for col in trigger_cols
                            if col != primary_key and col != 'import_time'
                        ])
                    }) THEN
                        INSERT INTO `{table_name}_history` (
                            {', '.join([f'`{col}`' for col in df.columns])},
                            `import_time`
                        ) VALUES (
                            {', '.join([f'OLD.`{col}`' for col in df.columns])},
                            OLD.`import_time`
                        );
                    END IF;
                END;
            """
            conn.execute(text(trigger_sql))
            print(f'Table `{table_name}` created successfully.')
        else:
            # 1. Count existing records
            bcnt = conn.execute(
                text(f"SELECT COUNT(*) FROM `{table_name}`;")
            ).fetchone()[0]
            bcnt_history = conn.execute(
                text(f"SELECT COUNT(*) FROM `{table_name}_history`;")    
            ).fetchone()[0]
            
            # 2. Check table summary info
            summary_sql = f"""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = '{table_name}';
            """
            col_info = {
                row[0]: (row[1], row[2])
                for row in conn.execute(text(summary_sql)).fetchall()
            }

            # 3. Alter len of VARCHAR() columns if needed
            for col in df.columns:
                if col not in col_info:
                    continue
                data_type, current_len = col_info[col]

                if data_type.lower() != 'varchar':
                    continue

                lens = df[col].dropna().astype(str).map(len)
                if lens.empty:
                    continue
                new_len = int(lens.max() * len_wt)
                if new_len > len_lim:  
                    conn.execute(
                        text(f"ALTER TABLE `{table_name}` MODIFY `{col}` TEXT;")
                    )
                    print(f"Converted `{table_name}`.`{col}` → TEXT.")
                    continue

                elif new_len > current_len:
                    conn.execute(
                        text(f"""
                            ALTER TABLE `{table_name}` 
                            MODIFY `{col}` VARCHAR({new_len});
                    """))
                    print(f"Expanded `{table_name}`.`{col}`: {current_len} → {new_len}")
            
            # 4. Create tmp table for updates
            tmp_table_name = f'{table_name}_tmp'
            df.to_sql(
                tmp_table_name,
                con = conn,
                if_exists = 'replace',
                index = False
            )

            # 5. Choose columns for updating existing records from tmp table
            update_cols = df.columns.difference([primary_key])

            # 6. Set conditions if the record has been updated
            cond = [
                f"(NOT (`{table_name}`.`{col}` <=> VALUES(`{col}`)))"
                for col in update_cols
            ]
            cond_sql = ' OR '.join(cond)
            set_sql = [
                f"`{col}` = VALUES(`{col}`)"
                for col in update_cols
            ]
            import_time_sql = f"""
                `import_time` = IF(
                    {cond_sql},
                    CURRENT_TIMESTAMP(),
                    `import_time`
                )
            """
            update_sql = ',\n '.join(set_sql + [import_time_sql])

            # 7. Upsert from tmp table to main table
            upsert_sql = f"""
                INSERT INTO `{table_name}` ({
                    ', '.join([f'`{col}`' for col in df.columns])
                })
                SELECT {', '.join([f'TMP.`{col}`' for col in df.columns])}
                FROM `{tmp_table_name}` AS TMP
                ON DUPLICATE KEY UPDATE
                    {update_sql};
            """
            result = conn.execute(text(upsert_sql))

            # 8. Count upserted records
            acnt = conn.execute(    
                text(f"SELECT COUNT(*) FROM `{table_name}`;")
            ).fetchone()[0]
            acnt_history = conn.execute(    
                text(f"SELECT COUNT(*) FROM `{table_name}_history`;")
            ).fetchone()[0] 
            cnt_inserted = acnt - bcnt
            cnt_updated = acnt_history - bcnt_history
            print(f"{cnt_updated} updated, {cnt_inserted} inserted in `{table_name}`.")

            # 9. Drop tmp table
            conn.execute(text(f"DROP TABLE IF EXISTS `{tmp_table_name}`;"))
