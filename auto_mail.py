from loguru import logger
import matplotlib.pyplot as plt
from utils.sql_utils import sql_utils
from utils.mail_utils import auto_mail
import os
import io
from utils.sql_utils import sql_utils
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import os
from datetime import datetime


# 设置中文字体为黑体
plt.rcParams['font.sans-serif'] = ['SimHei']
# 解决负号显示问题
plt.rcParams['axes.unicode_minus'] = False


def get_forex(sql_util):
    sql = "SELECT * FROM t_forex_data_index_sina"
    best_res = sql_util.read_sql(database="forex", sql=sql, format='df')
    # 取近30条数据
    df_day = best_res[-30:]
    # 构建html文本
    df_day["dt_formatted"] = df_day["create_date"].dt.strftime('%Y-%m-%d %H:%M:%S')
    dic_m = df_day.iloc[-1].to_dict()
    dt_formatted = dic_m["dt_formatted"]
    best_xh_buy_bank = dic_m["best_xh_buy_bank"]
    best_xh_buy = dic_m["best_xh_buy"]
    best_xh_sell_bank = dic_m["best_xh_sell_bank"]
    best_xh_sell = dic_m["best_xh_sell"]
    html_text = f"""<h3>最新外汇数据</h3>
        截至{dt_formatted}: 
        美元主要银行最佳现汇买入银行为{best_xh_buy_bank}, 值为{best_xh_buy}; 最佳现汇卖出银行为{best_xh_sell_bank}, 值为{best_xh_sell}.
        """
    # 构建图像
    plt.plot(df_day['create_date'], df_day['best_xh_buy'], label='最佳现汇买入')
    plt.plot(df_day['create_date'], df_day['best_xh_sell'], label='最佳现汇卖出')
    # 设置图表标题和坐标轴标签
    plt.title('最佳现汇买入/卖出价')
    plt.xlabel('日期')
    plt.ylabel('汇率')
    plt.xticks(rotation=30)
    # 创建一个BytesIO对象
    imgdata = io.BytesIO()
    # 将图形保存到BytesIO对象中，格式为png
    plt.savefig(imgdata, format='png')
    # 将指针移动到BytesIO对象的起始位置
    imgdata.seek(0)
    # 构建邮件消息
    e_msg = {}
    e_msg["html"] =  html_text
    e_msg["img_bytes"] = imgdata
    return e_msg


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
