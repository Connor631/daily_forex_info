import requests
from bs4 import BeautifulSoup
import time
import re
import json
from loguru import logger
import pandas as pd
from auto_mail import auto_mail
import os
import schedule


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


def crawl_webpage(url):
    try:
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
def parse_usd(soup, formatted_time):
    result_json = extract_and_parse_json(soup.prettify())
    usd = pd.json_normalize(result_json["data"]["bank"]["USD"])
    usd["bank_chi"] = usd["bank"].map(BANK_DIC)
    # 银行现汇卖出越低越好，现汇买入越高越好。
    xh_sell_min = usd["xh_sell_price"].min()
    xh_buy_max = usd["xh_buy_price"].max()
    best_xh_sell = usd[usd["xh_sell_price"] == xh_sell_min]
    best_xh_buy = usd[usd["xh_buy_price"] == xh_buy_max]
    # 避免重复值
    best_xh_sell_bank = best_xh_sell.iloc[:,-1].to_list()
    best_xh_buy_bank = best_xh_buy.iloc[:,-1].to_list()
    out = f"截至{formatted_time},美元主要银行最佳现汇买入银行为{best_xh_buy_bank}，值为{xh_buy_max}；最佳现汇卖出银行为{best_xh_sell_bank}，值为{xh_sell_min}。"
    logger.info(formatted_time + "数据获取完成：" + out)
    return out


def send_msg(obj, msg):
    current_file_path = os.path.abspath(__file__)
    config_path = os.path.join(os.path.dirname(current_file_path), "config.json")
    am = auto_mail(config_path)
    am.send_email_msg(obj, msg)


@logger.catch
def job():
    # 获取时间
    ts = int(time.time() * 1000)
    tme = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    logger.info(tme + "任务开始运行")
    # 主程序
    url = f"https://vip.stock.finance.sina.com.cn/forex/api/openapi.php/ForexService.getBankForex?callback=getAllBankForex&_={ts}"
    ti = crawl_webpage(url)
    res = parse_usd(ti, tme)
    send_msg("外汇每日行情", res)
    logger.info(tme + "任务结束，已发送：" + res)


if __name__ == "__main__":
    logger.add("file_1.log", rotation="50 MB")    # Automatically rotate too big file
    schedule.every().day.at("15:15").do(job)
    job()
    while True:
        schedule.run_pending()
        time.sleep(60)
    