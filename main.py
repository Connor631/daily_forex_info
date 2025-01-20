from loguru import logger
from data_process.forex_data import forex_data_main
from utils.sql_utils import sql_utils
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import os
from datetime import datetime


@logger.catch
def job(sql_util):
    forex_data_main(sql_util)
    # 打印下次运行时间
    job_instance = scheduler.get_job('forex_job')
    if job_instance:
        next_run_time = job_instance.trigger.get_next_fire_time(None, datetime.now()).astimezone().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"本次运行结束，下次运行时间：{next_run_time}")


if __name__ == "__main__":
    # 检查并创建 logs 文件夹
    log_dir = "./logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logger.add(os.path.join(log_dir, "file_data.log"), rotation="50 MB")

    # 后台数据表控制
    config_path = "config.json"
    sql_util = sql_utils(config_path)

    # 使用cron方式启动任务
    scheduler = BlockingScheduler()
    # 定义 cron 表达式
    cron_expression = "5 9,16,23 * * *"  # 每天上午 9:05，下午 4:05 和晚上 11:05
    # 添加任务
    scheduler.add_job(job, CronTrigger.from_crontab(cron_expression), kwargs={'sql_util': sql_util}, id='forex_job')
    # 立刻运行一次任务
    job(sql_util)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    