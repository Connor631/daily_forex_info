from loguru import logger
from utils.sql_utils import sql_utils
from utils.mail_utils import auto_mail
from utils.sql_utils import sql_utils
from mail_process.forex_mail import get_forex
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import os
from datetime import datetime
from pathlib import Path
import argparse


def job_forex_mail(sql_util, config):
    sender = config["mail_sender"]
    password = config["mail_sender_pwd"]
    receivers = config["mail_receiver"]
    revs = receivers.split(",")
    if len(revs) > 1:
        am = auto_mail(sender, password, receivers, multiple_receiver=True)
    elif len(revs) == 0:
        logger.error("收件人为空，请检查配置文件")
        return
    else:
        am = auto_mail(sender, password, receivers)
    obj = "daily forex"
    msg = get_forex(sql_util)
    am.send_email_msg(obj, msg)
   # 打印下次运行时间
    job_instance = scheduler.get_job('forex_job')
    if job_instance:
        next_run_time = job_instance.trigger.get_next_fire_time(None, datetime.now()).astimezone().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"本次运行结束，下次运行时间：{next_run_time}")


if __name__ == '__main__':
    logger.info("start.....")
    # 创建解析器对象
    parser = argparse.ArgumentParser(description="邮件服务控制。")
    parser.add_argument('--forex', type=int, default=1, help='外汇邮件任务是否启动')
    parser.add_argument('--stock', type=int, default=1, help='股市邮件任务是否启动')
    args = parser.parse_args()

    # 检查并创建 logs 文件夹
    log_dir = "./logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logger.add(os.path.join(log_dir, "file_mail.log"), rotation="50 MB")

    # 后台数据表控制
    script_path = Path(__file__).resolve().parent
    data_file_path = script_path / 'config.json'
    sql_util = sql_utils(data_file_path)

    # 使用cron方式启动任务
    scheduler = BlockingScheduler()

    if args.forex:
        forex_tag = "forex_sina"
        forex_sql = f"SELECT * FROM t_user_info WHERE uni_tag='{forex_tag}'"
        forex_mail_config = sql_util.read_sql(database="forex",sql=forex_sql, format="dict")
        # cron表达式
        forex_cron = forex_mail_config["sche_receiver"]
        scheduler.add_job(job_forex_mail, CronTrigger.from_crontab(forex_cron), kwargs={'sql_util': sql_util, 'config': forex_mail_config}, id='forex_job', misfire_grace_time=60)
        job_forex_mail(sql_util, forex_mail_config)
    else:
        logger.info("外汇数据任务未启动")
