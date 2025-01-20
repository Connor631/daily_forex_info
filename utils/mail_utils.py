import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from loguru import logger


class auto_mail(object):
    def __init__(self, sender, password, receivers, multiple_receiver=False):
        self.sender = sender
        self.receivers = receivers
        self.password = password
        self.multiple_receiver = multiple_receiver

    def send_email_msg(self, subject, msg):
        if self.multiple_receiver:
            self.send_email_msg_multiple(subject, msg)
        else:
            self.send_email_msg_single(subject, msg, self.receivers)

    def send_email_msg_single(self, subject, msg, sing_receiver=None):
        # 邮件发送者
        sender = self.sender
        # 邮件接收者
        receiver = sing_receiver
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

    def send_email_msg_multiple(self, subject, msg):
        for receiver in self.receivers.split(","):
            self.send_email_msg_single(subject, msg, receiver)