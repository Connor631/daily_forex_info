from loguru import logger
from utils.sql_utils import sql_utils
from utils.mail_utils import auto_mail
import os
from utils.sql_utils import sql_utils
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import os
from datetime import datetime


def get_forex(sql_util):
    sql = "SELECT * FROM t_forex_data_index_sina ORDER BY data_dt DESC, data_tm DESC LIMIT 1"
    best_res = sql_util.read_sql(database="forex", sql=sql, format='dict')
    dt = best_res["data_dt"]
    tm = best_res["data_tm"]
    best_xh_buy_bank = best_res["best_xh_buy_bank"]
    best_xh_buy = best_res["best_xh_buy"]
    best_xh_sell_bank = best_res["best_xh_sell_bank"]
    best_xh_sell = best_res["best_xh_sell"]
    msg = f"截至{dt}日{tm},美元主要银行最佳现汇买入银行为{best_xh_buy_bank}，值为{best_xh_buy}；最佳现汇卖出银行为{best_xh_sell_bank}，值为{best_xh_sell}。"
    return msg


def job(sql_util):
    sql_config_dict = sql_util.read_sql(database="forex", sql="SELECT * FROM t_forex_bat_ctl WHERE uni_tag='forex_sina'", format="dict")
    sender = sql_config_dict["mail_sender"]
    password = sql_config_dict["mail_pwd"]
    receivers = sql_config_dict["mail_recpt"]
    revs = receivers.split(",")
    if len(revs) > 1:
        am = auto_mail(sender, password, receivers, multiple_receiver=True)
    elif len(revs) == 0:
        logger.error("收件人为空，请检查配置文件")
        return
    else:
        am = auto_mail(sender, password, receivers)
    obj = "daily forex data"
    msg = get_forex(sql_util)
    am.send_email_msg(obj, msg)
   # 打印下次运行时间
    job_instance = scheduler.get_job('forex_job')
    if job_instance:
        next_run_time = job_instance.trigger.get_next_fire_time(None, datetime.now()).astimezone().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"本次运行结束，下次运行时间：{next_run_time}")


if __name__ == '__main__':
    # 检查并创建 logs 文件夹
    log_dir = "./logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    # 添加日志
    logger.add(os.path.join(log_dir, "file_data.log"), rotation="50 MB")

    # 后台数据表控制
    config_path = "config.json"
    sql_util = sql_utils(config_path)

    # 使用cron方式启动任务
    scheduler = BlockingScheduler()
    # 定义 cron 表达式
    cron_expression = "10 16 * * *"
    # 添加任务
    scheduler.add_job(job, CronTrigger.from_crontab(cron_expression), kwargs={'sql_util': sql_util}, id='forex_job')
    # 立刻运行一次任务
    job(sql_util)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
