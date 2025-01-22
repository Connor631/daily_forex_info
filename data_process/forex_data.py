import time
from loguru import logger
import pandas as pd
from utils.web_utils import crawl_webpage
import pytz
from datetime import datetime
import re
import json


# 设置时区为北京时间
tz = pytz.timezone('Asia/Shanghai')

BANK_DIC = {
    "icbc": "中国工商银行"
    ,"boc": "中国银行"
    ,"abchina": "中国农业银行"
    ,"bankcomm": "交通银行"
    ,"ccb": "中国建设银行"
    ,"cmbchina": "招商银行"
    ,"cebbank": "中国光大银行"
    ,"spdb": "上海浦东发展银行"
    ,"cib": "兴业银行"
    ,"ecitic": "中信银行"
}


@logger.catch
def extract_and_parse_json(text, reg=None):
    # 使用正则表达式提取reg中的JSON部分
    match = re.search(reg, text)
    if match:
        json_str = match.group(1)
        # 解析JSON字符串
        data = json.loads(json_str)
        return data
    else:
        logger.error(f'未找到{reg}其中的JSON数据')


@logger.catch
def parse_curr(soup, formatted_time, curr = "USD"):
    # 解析出对应的JSON
    reg = r'getAllBankForex\((.*?)\)'
    result_json_raw = extract_and_parse_json(soup.prettify(), reg)
    result_json = result_json_raw.get('result', {})
    # 转化成df
    df_curr = pd.json_normalize(result_json["data"]["bank"][curr])
    df_curr["bank_chi"] = df_curr["bank"].map(BANK_DIC)
    df_curr["currency"] = curr
    date_time_obj = pd.to_datetime(formatted_time)
    df_curr["data_dt"] = date_time_obj.date()
    df_curr["data_tm"] = date_time_obj.time()
    # 替换所有字符为 "--" 的值为空值
    df_curr.replace("--", pd.NA, inplace=True)
    return df_curr


@logger.catch
def wrap_forex(df, formatted_time, curr = "USD"):
    # 银行现汇卖出越低越好，现汇买入越高越好。
    xh_sell_min = df["xh_sell_price"].min()
    xh_buy_max = df["xh_buy_price"].max()
    best_xh_sell = df[df["xh_sell_price"] == xh_sell_min]
    best_xh_buy = df[df["xh_buy_price"] == xh_buy_max]
    # 避免重复值
    best_xh_sell_bank = best_xh_sell.loc[:,"bank_chi"].to_list()
    best_xh_buy_bank = best_xh_buy.loc[:,"bank_chi"].to_list()
    # 构建输出数据表
    df_out = pd.DataFrame({
        "currency": curr
        ,"best_xh_sell_bank": best_xh_sell_bank[:]
        ,"best_xh_sell": xh_sell_min
        ,"best_xh_buy_bank": best_xh_buy_bank[:]
        ,"best_xh_buy": xh_buy_max
    })
    date_time_obj = pd.to_datetime(formatted_time)
    df_out["data_dt"] = date_time_obj.date()
    df_out["data_tm"] = date_time_obj.time()
    return df_out


@logger.catch
def forex_data_main(sql_util, forex_sina_config):
    # 获取时间
    tme = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(tme + "任务开始运行")
    # url加工
    url_raw = forex_sina_config["url"]
    res1 = int(time.time() * 1000)
    url = url_raw.replace("{res1}", str(res1))
    resp = crawl_webpage(url)
    # 外汇原始报价数据
    forex_raw = parse_curr(resp, tme)
    sql_util.df_write_table(forex_raw, table_name="t_forex_data_sina", database="forex")
    # 外汇报价指标数据
    forex_index = wrap_forex(forex_raw, tme)
    sql_util.df_write_table(forex_index, table_name="t_forex_data_index_sina", database="forex")


if __name__ == "__main__":
    pass
    