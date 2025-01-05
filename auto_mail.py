import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
import os
from loguru import logger


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


if __name__ == '__main__':
    # current_file_path = os.path.abspath(__file__)
    # config_path = os.path.join(os.path.dirname(current_file_path), "config.json")
    # am = auto_mail(config_path)
    # am.send_email_msg("测试邮件", "这是一封测试邮件2")
    pass
