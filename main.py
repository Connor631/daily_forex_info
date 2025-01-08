import time
from loguru import logger
from auto_mail import auto_mail
from forex_data import forex_data_main
import os
import schedule
import pytz
from datetime import datetime


@logger.catch
def job():
    forex_data_main()
    # 打印下次运行时间
    next_run_time = schedule.next_run().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"本次运行结束，下次运行时间：{next_run_time}")


if __name__ == "__main__":
    logger.add("file_1.log", rotation="50 MB")
    # 每天上午 9:05 和下午 4:05 运行任务
    schedule.every().day.at("09:05").do(job)
    schedule.every().day.at("16:05").do(job)
    job()  # 立刻运行一次
    while True:
        schedule.run_pending()
        time.sleep(60)
    