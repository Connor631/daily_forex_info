import pymysql
from loguru import logger
from sqlalchemy import create_engine, text
import pandas as pd
import json
from urllib.parse import quote_plus


class sql_utils():
    def __init__(self, config_path):
        with open(config_path, "r", encoding="utf8") as fp:
            self.config = json.load(fp)
        
    def get_db_connection(self, database):
        return pymysql.connect(
            host=self.config["sql_config"]["host"],
            port=self.config["sql_config"]["port"],
            user=self.config["sql_config"]["account"],
            password=self.config["sql_config"]["pwd"],
            database=database
        )

    def get_sqlalchemy_engine(self, database):
        password = quote_plus(self.config['sql_config']['pwd'])  # 解决密码中特殊符号问题
        acct = self.config['sql_config']['account']
        host = self.config['sql_config']['host']
        port = self.config['sql_config']['port']
        return create_engine(
            f"mysql+pymysql://{acct}:{password}@{host}:{port}/{database}"
        )

    @logger.catch
    def read_sql(self, database, sql, format="dict"):
        connection = self.get_db_connection(database)
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                if format == "df":
                    return pd.DataFrame(result, columns=columns)
                elif format == "dict":
                    result_dict = [dict(zip(columns, row)) for row in result]
                    return result_dict[0] if len(result_dict) == 1 else result_dict
                else:
                    return result
        finally:
            connection.close()
    
    @logger.catch
    def df_write_table(self, df, table_name, database, increm_cols=None, replace=False):
        """
        将 DataFrame 写入数据库表，支持增量写入。
        
        :param df: 待写入的 DataFrame
        :param table_name: 数据库表名
        :param database: 数据库名
        :param increm_cols: 用于增量判断的列名列表 如 ['A', 'B']，为空则直接追加写入
        """
        engine = self.get_sqlalchemy_engine(database)
        # 获取表的列信息
        with engine.connect() as connection:
            table_columns = connection.execute(text(f"SHOW COLUMNS FROM {table_name}")).fetchall()
            table_columns = [col[0] for col in table_columns]
        
        # 需要数据库自动生成默认值的字段
        auto_cols = {"id", "create_date", "mod_date"}
        # 只补齐非自动生成的字段
        missing_cols = [col for col in table_columns if col not in df.columns and col not in auto_cols]
        df[missing_cols] = None
        # 只保留DataFrame已有的列和非自动生成的列
        final_cols = [col for col in table_columns if col not in auto_cols]
        df_clean = df[final_cols]
        try:
            if replace:
                with engine.connect() as connection:
                    connection.execute(text(f"TRUNCATE TABLE {table_name}"))  # 清空表但保留结构
                with engine.connect() as connection:
                    df_clean.to_sql(name=table_name, con=connection, if_exists='append', index=False)
                    logger.info(f"Replacement data successfully written to table {table_name}")
                return
            if increm_cols:
                # 从目标表中读取增量判断列的数据
                with engine.connect() as connection:
                    query = f"SELECT {', '.join(increm_cols)} FROM {table_name}"
                    existing_data = pd.read_sql(query, con=connection)

                # 判断增量数据
                if not existing_data.empty:
                    # 使用 set_index 和 isin 判断增量
                    is_new = ~df_clean.set_index(increm_cols).index.isin(
                        existing_data.set_index(increm_cols).index
                    )
                    df_increm = df_clean.loc[is_new]
                else:
                    df_increm = df_clean
            else:
                # 如果未指定增量列，直接将所有数据作为增量
                df_increm = df_clean
            
            # 写入增量数据
            if not df_increm.empty:
                with engine.connect() as connection:
                    df_increm.to_sql(name=table_name, con=connection, if_exists='append', index=False)
                logger.info(f"Incremental data successfully written to table {table_name}")
            else:
                logger.info(f"No incremental data to write to table {table_name}")
        except Exception as e:
            logger.error(f"Failed to write DataFrame to table {table_name}: {e}")


if __name__ == "__main__":
    pass
