import requests
from bs4 import BeautifulSoup
import time
import re
import json
from loguru import logger
import pandas as pd

import pytz
from datetime import datetime


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

def extract_and_parse_json(text):
    # 使用正则表达式提取getAllBankForex函数中的JSON部分
    match = re.search(r'getAllBankForex\((.*?)\)', text)
    if match:
        json_str = match.group(1)
        try:
            # 解析JSON字符串
            data = json.loads(json_str)
            # 获取result对应的JSON
            result = data.get('result', {})
            return result
        except json.JSONDecodeError as e:
            logger.error(f'解析JSON时出错: {e}')
    else:
        logger.error('未找到getAllBankForex函数及其中的JSON数据')


def crawl_webpage(url, headers=None):
    try:
        if headers is None:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        # 发送HTTP请求获取网页内容
        response = requests.get(url, headers=headers)
        # 检查请求是否成功
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            return soup
        else:
            logger.error(f"请求失败，状态码：{response.status_code}")
    except requests.RequestException as e:
        logger.error(f"请求发生异常：{e}")


@logger.catch
def parse_curr(soup, formatted_time, curr = "USD"):
    result_json = extract_and_parse_json(soup.prettify())
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
def forex_data_main(sql_util):
    # 获取时间
    tme = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(tme + "任务开始运行")
    # 读取配置信息
    tag = "forex_sina"
    sql = f"SELECT * FROM t_forex_bat_ctl WHERE uni_tag='{tag}'"
    forex_sina_config = sql_util.read_sql(database="forex",sql=sql, format="dict")
    if forex_sina_config["bat_stat"] == "active":
        # 主项目
        url_raw = forex_sina_config["url"]
        # 获取时间
        res1 = int(time.time() * 1000)
        url = url_raw.replace("{res1}", str(res1))  # 重组url
        resp = crawl_webpage(url)
        # 外汇原始报价数据
        forex_raw = parse_curr(resp, tme)
        sql_util.df_write_table(forex_raw, table_name="t_forex_data_sina", database="forex")
        # 外汇报价指标数据
        forex_index = wrap_forex(forex_raw, tme)
        sql_util.df_write_table(forex_index, table_name="t_forex_data_index_sina", database="forex")
    else:
        logger.warning(forex_sina_config["uni_tag"] + "该任务已被禁用")


if __name__ == "__main__":
    pass
    