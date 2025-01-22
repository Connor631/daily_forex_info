import requests
from bs4 import BeautifulSoup
from loguru import logger
import re
import pytz
import pandas as pd
from utils.web_utils import crawl_webpage
from utils.sql_utils import sql_utils
from datetime import datetime


# 设置时区为北京时间
tz = pytz.timezone('Asia/Shanghai')


@logger.catch
def get_stock_data(url):
    headers = {
        'Accept': '*/*'
        ,'Accept-Encoding': 'gzip, deflate, br, zstd'
        ,'Accept-Language': 'zh-CN,zh;q=0.9'
        ,'Connection': 'keep-alive'
        ,'Host': 'hq.sinajs.cn'
        ,'Referer': 'https://finance.sina.com.cn/'
        ,'Sec-Fetch-Dest': 'script'
        ,'Sec-Fetch-Mode': 'no-cors'
        ,'Sec-Fetch-Site': 'cross-site'
        ,'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        ,'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"'
        ,'sec-ch-ua-mobile': '?0'
        ,'sec-ch-ua-platform': '"Windows"'
    }
    resp = crawl_webpage(url, headers)
    # 正则表达式提取数据
    reg = r'var hq_str_(\w+)=(".*?");'
    pattern = re.compile(reg)
    matches = pattern.findall(resp.text)
    return matches


def parse_stock(matches):
    parsed_data_cty, parsed_data_usa = {}, {}
    markets_cty = ["znb_UKX","znb_DAX", "znb_INDEXCF", "znb_NKY", "znb_TWJQ", "znb_KOSPI"]
    markets_usa = ["gb_dji", "gb_ixic", "gb_inx"]
    for i in matches:
        index_name = i[0]
        if index_name in markets_cty:
            values = i[1].strip('"').split(',')
            parsed_data_cty[index_name] = {
                "index_name": values[0],
                "close": values[1],
                "change_value": values[2],
                "change_rate": values[3],
                "query_tm": values[4],
                "ctime": values[5],
                "data_dt": values[6],
                "data_tm": values[7],
                "open": values[8],
                "close_lastday": values[9],
                "high": values[10],
                "low": values[11]
            }
        elif index_name in markets_usa:
            values = i[1].strip('"').split(',')
            parsed_data_usa[index_name] = {
                "index_name": values[0],
                "close": values[1],
                "change_percent": values[2],
                "date": values[3],
                "change_value": values[4],
                "open": values[5],
                "high": values[6],
                "low": values[7],
                "week_52_high": values[8],
                "week_52_low": values[9],
                "volume": values[10],
                "market_cap": values[11],
                "close_lastday": values[26]
            }
        else:
            pass
    df_cty = pd.DataFrame(parsed_data_cty)
    df_usa = pd.DataFrame(parsed_data_usa)
    # 转置并保存索引信息
    df_cty_ = df_cty.transpose().reset_index().rename(columns={"index": "index_code"})
    df_usa_ = df_usa.transpose().reset_index().rename(columns={"index": "index_code"})
    # 拆分时间字段为日期和时间
    df_usa_['datetime_column'] = pd.to_datetime(df_usa_['date'])
    df_usa_["data_dt"] = df_usa_["datetime_column"].dt.date
    df_usa_["data_tm"] = df_usa_["datetime_column"].dt.time
    return df_cty_, df_usa_


def stock_data_main(sql_util, stock_sina_config):
    # 获取时间
    tme = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(tme + "任务开始运行")
    # 获取url
    # url = 'https://hq.sinajs.cn/?list=gb_dji,gb_ixic,gb_inx,znb_UKX,znb_DAX,znb_INDEXCF,znb_CAC,znb_SMI,znb_FTSEMIB,znb_MADX,znb_OMX,znb_HEX,znb_OSEAX,znb_ISEQ,znb_AEX,znb_IBEX,znb_SX5E,znb_XU100,znb_NKY,znb_TWJQ,znb_FSSTI,znb_KOSPI,znb_FBMKLCI,znb_SET,znb_JCI,znb_PCOMP,znb_KSE100,znb_SENSEX,znb_VNINDEX,znb_CSEALL,znb_SASEIDX,znb_SPTSX,znb_MEXBOL,znb_IBOV,znb_MERVAL,znb_AS51,znb_NZSE50FG,znb_CASE,znb_JALSH'
    url = stock_sina_config["url"]
    matches = get_stock_data(url)
    # 分类解析数据
    df_cty, df_usa = parse_stock(matches)
    sql_util.df_write_table(df_cty, table_name="t_stock_index_data", database="forex")
    sql_util.df_write_table(df_usa, table_name="t_stock_index_data", database="forex")


if __name__ == "__main__":
    pass
