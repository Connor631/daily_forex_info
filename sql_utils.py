import pymysql
from loguru import logger
from sqlalchemy import create_engine, text
import pandas as pd
import json
import time
from datetime import datetime
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
    # config_path = "config.json"
    # sql_util = sql_utils(config_path)

    # tag = "forex_sina"
    # sql = f"SELECT * FROM t_forex_bat_ctl WHERE uni_tag='{tag}'"
    # dic = sql_util.read_sql(database="forex",sql=sql, format="dict")
    # forex_sina_config = dic[0]

    # conn = sql_util.get_sqlalchemy_engine("forex")
    # logger.info(conn)
    # df = pd.DataFrame({
    #     "bank": ["中国银行", "中国银行", "中国银行"],
    #     "xh_sell_price": [6.5, 6.4, 6.3],
    #     "xh_buy_price": [6.2, 6.1, 6.0]
    # })
    # date_time_obj = pd.to_datetime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    # df["data_dt"] = date_time_obj.date()
    # df["data_tm"] = date_time_obj.time()
    # df["currency"] = "test"
    # logger.info(df)
    # sql_util.df_write_table(df, "t_forex_data_sina", "forex")

    # if forex_sina_config["bat_stat"] == "active":
    #     url = forex_sina_config["url"]
    #     # 获取时间
    #     res1 = int(time.time() * 1000)
    #     url = url.replace("{res1}", str(res1))
    #     logger.info(url)
    # else:
    #     logger.warning(forex_sina_config["uni_tag"] + "该任务已被禁用")
    pass
