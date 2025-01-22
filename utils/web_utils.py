import requests
from bs4 import BeautifulSoup
from loguru import logger


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
