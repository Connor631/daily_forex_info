from loguru import logger
import pytz
from datetime import datetime
import akshare as ak
import time


# 设置时区为北京时间
tz = pytz.timezone('Asia/Shanghai')


@logger.catch
def get_stock_data(symbol):
    time.sleep(10)  # 避免请求过于频繁
    return ak.index_us_stock_sina(symbol)


def get_stock_usa():
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
    return inx, dji, ndx, ixic

def get_stock_global_em(symbol):
    time.sleep(5)  # 避免请求过于频繁
    index_global_hist_em_df = ak.index_global_hist_em(symbol)
    index_global_hist_em_df.rename(
        columns={
            "日期": "date",
            "代码": "index_code",
            "名称": "index_name",
            "最新价": "close",
            "今开": "open",
            "最高": "high",
            "最低": "low",
        }, inplace=True
    )
    return index_global_hist_em_df

def stock_data_main(sql_util=None, lst=[]):
    # 获取时间
    tme = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(tme + "任务开始运行")
    # 获取美股数据
    inx, dji, ndx, ixic = get_stock_usa()
    sql_util.df_write_table(inx, table_name="t_stock_index_data_sina", database="forex", increm_cols=["index_code", "date"])
    sql_util.df_write_table(dji, table_name="t_stock_index_data_sina", database="forex", increm_cols=["index_code", "date"])
    sql_util.df_write_table(ndx, table_name="t_stock_index_data_sina", database="forex", increm_cols=["index_code", "date"])
    sql_util.df_write_table(ixic, table_name="t_stock_index_data_sina", database="forex", increm_cols=["index_code", "date"])
    # 获取全球股指数据
    for i in lst:
        em_data = get_stock_global_em(i)
        sql_util.df_write_table(em_data, table_name="t_stock_index_data_sina", database="forex", increm_cols=["index_code", "date"])
    return None


if __name__ == "__main__":
    pass
