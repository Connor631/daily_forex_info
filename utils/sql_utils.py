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
    def df_write_table(self, df, table_name, database):
        engine = self.get_sqlalchemy_engine(database)
        # 获取表的列信息
        with engine.connect() as connection:
            table_columns = connection.execute(text(f"SHOW COLUMNS FROM {table_name}")).fetchall()
            table_columns = [col[0] for col in table_columns]
        
        # 添加缺失的列
        for col in table_columns:
            if col not in df.columns:
                df[col] = None
        try:
            # 使用 engine.connect() 的上下文管理器来确保连接在使用后关闭
            with engine.connect() as connection:
                df.to_sql(name=table_name, con=connection, if_exists='append', index=False)
            logger.info(f"DataFrame successfully written to table {table_name}")
        except Exception as e:
            logger.error(f"Failed to write DataFrame to table {table_name}: {e}")


if __name__ == "__main__":
    pass
