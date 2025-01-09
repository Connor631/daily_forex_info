import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
import schedule
import time
from loguru import logger
from sql_utils import sql_utils


class auto_mail(object):
    def __init__(self, config_path):
        with open(config_path, "r", encoding="utf8") as fp:
            config = json.load(fp)
        self.sender = config["mail_config"]["sender"]
        self.receiver = config["mail_config"]["recipient"]
        self.password = config["mail_config"]["password"]

    def send_email_msg(self, subject, msg):
        # 邮件发送者
        sender = self.sender
        # 邮件接收者
        receiver = self.receiver
        # 邮件主题
        subject = subject
        # 邮件内容
        body = msg

        # 创建一个 multipart message
        message = MIMEMultipart()
        message["From"] = sender
        message["To"] = receiver
        message["Subject"] = subject
        # 添加邮件内容
        message.attach(MIMEText(body, "html"))

        # 163邮箱的授权码信息
        username = sender
        password = self.password

        # 通过163邮箱的SMTP服务器发送邮件
        try:
            with smtplib.SMTP_SSL("smtp.163.com", 465, timeout=60) as server:
                server.login(username, password)
                server.sendmail(sender, receiver, message.as_string())
                logger.info("邮件发送成功")
        except smtplib.SMTPException as e:
            logger.error(f"邮件发送失败： {e}")
        except TimeoutError as e:
            logger.error(f"连接超时，{e}")


def get_forex():
    config_path = "config.json"
    sql_util = sql_utils(config_path)
    sql = "SELECT * FROM t_forex_data_index_sina ORDER BY data_dt DESC, data_tm DESC LIMIT 1"
    best_res_dict = sql_util.read_sql(database="forex", sql=sql, format='dict')
    best_res = best_res_dict[0]
    dt = best_res["data_dt"]
    tm = best_res["data_tm"]
    best_xh_buy_bank = best_res["best_xh_buy_bank"]
    best_xh_buy = best_res["best_xh_buy"]
    best_xh_sell_bank = best_res["best_xh_sell_bank"]
    best_xh_sell = best_res["best_xh_sell"]
    msg = f"截至{dt}日{tm},美元主要银行最佳现汇买入银行为{best_xh_buy_bank}，值为{best_xh_buy}；最佳现汇卖出银行为{best_xh_sell_bank}，值为{best_xh_sell}。"
    return msg


def job():
    config_path = "config.json"
    am = auto_mail(config_path)
    obj = "daily forex data"
    msg = get_forex()
    am.send_email_msg(obj, msg)
    # 打印下次运行时间
    next_run_time = schedule.next_run().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"本次运行结束，下次运行时间：{next_run_time}")


if __name__ == '__main__':
    logger.add("file_mail.log", rotation="50 MB")
    schedule.every().day.at("16:05").do(job)
    job()  # 立刻运行一次
    while True:
        schedule.run_pending()
        time.sleep(60)
