from loguru import logger
import pytz
from datetime import datetime
import akshare as ak


# 设置时区为北京时间
tz = pytz.timezone('Asia/Shanghai')

@logger.catch
def get_stock_data(symbol):
    return ak.index_us_stock_sina(symbol)


def stock_data_main(sql_util=None):
    # 获取时间
    tme = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(tme + "任务开始运行")
    # 获取数据
    inx = get_stock_data(".INX")
    inx["index_code"] = "inx"
    inx["index_name"] = "标普500"
    dji = get_stock_data(".DJI")
    dji["index_code"] = "dji"
    dji["index_name"] = "道琼斯"
    ndx = get_stock_data(".NDX")
    ndx["index_code"] = "ndx"
    ndx["index_name"] = "纳斯达克100"
    ixic = get_stock_data(".IXIC")
    ixic["index_code"] = "ixic"
    ixic["index_name"] = "纳斯达克综合"
    # 写入数据
    sql_util.df_write_table(inx, table_name="t_stock_index_data_sina", database="forex", increm_cols=["index_code", "date"])
    sql_util.df_write_table(dji, table_name="t_stock_index_data_sina", database="forex", increm_cols=["index_code", "date"])
    sql_util.df_write_table(ndx, table_name="t_stock_index_data_sina", database="forex", increm_cols=["index_code", "date"])
    sql_util.df_write_table(ixic, table_name="t_stock_index_data_sina", database="forex", increm_cols=["index_code", "date"])
    return None


if __name__ == "__main__":
    pass
