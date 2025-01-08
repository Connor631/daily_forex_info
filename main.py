import time
from loguru import logger
from forex_data import forex_data_main
import schedule


@logger.catch
def job():
    forex_data_main()
    # 打印下次运行时间
    next_run_time = schedule.next_run().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"本次运行结束，下次运行时间：{next_run_time}")


if __name__ == "__main__":
    logger.add("file_data.log", rotation="50 MB")
    # 每天上午 9:05 和下午 4:05 运行任务
    schedule.every().day.at("09:05").do(job)
    schedule.every().day.at("16:05").do(job)
    schedule.every().day.at("23:05").do(job)
    job()  # 立刻运行一次
    while True:
        schedule.run_pending()
        time.sleep(60)
    