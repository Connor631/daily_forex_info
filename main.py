from loguru import logger
from data_process.forex_data import forex_data_main
from data_process.stock_data import stock_data_main
from utils.sql_utils import sql_utils
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import os
from datetime import datetime


@logger.catch
def job_forex(sql_util, config):
    if config["bat_stat"] == "active":
        forex_data_main(sql_util, config)
    else:
        logger.info("外汇数据任务被禁用")
    # 打印下次运行时间
    job_instance = scheduler.get_job('forex_job')
    if job_instance:
        next_run_time = job_instance.trigger.get_next_fire_time(None, datetime.now()).astimezone().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"本次运行结束，下次运行时间：{next_run_time}")

@logger.catch
def job_stock(sql_util, config):
    if config["bat_stat"] == "active":
        stock_data_main(sql_util, config)
    else:
        logger.info("股票数据任务被禁用")
    # 打印下次运行时间
    job_instance = scheduler.get_job('stock_job')
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
    # 读取配置
    stock_tag, forex_tag = "stock_sina", "forex_sina"
    stock_sql = f"SELECT * FROM t_task_bat_ctl WHERE uni_tag='{stock_tag}'"
    forex_sql = f"SELECT * FROM t_task_bat_ctl WHERE uni_tag='{forex_tag}'"
    stock_sina_config = sql_util.read_sql(database="forex",sql=stock_sql, format="dict")
    forex_sina_config = sql_util.read_sql(database="forex",sql=forex_sql, format="dict")

    # cron表达式
    stock_cron = stock_sina_config["sched_tm"]
    forex_cron = forex_sina_config["sched_tm"]

    # 使用cron方式启动任务
    scheduler = BlockingScheduler()

    # 添加任务
    scheduler.add_job(job_forex, CronTrigger.from_crontab(forex_cron), kwargs={'sql_util': sql_util, 'config': stock_sina_config}, id='forex_job')
    scheduler.add_job(job_stock, CronTrigger.from_crontab(stock_cron), kwargs={'sql_util': sql_util, 'config': forex_sina_config}, id='stock_job')

    # 立刻运行一次任务
    job_stock(sql_util, stock_sina_config)
    job_forex(sql_util, forex_sina_config)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    