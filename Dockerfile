# 使用官方的 Python 基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 复制当前目录内容到工作目录
COPY . /app

# 安装依赖包
RUN pip install --no-cache-dir -r requirements.txt

# 复制配置文件
COPY config.json /app/config.json

# 设置环境变量（如果需要）
ENV PYTHONUNBUFFERED=1

# 运行主程序
CMD ["python", "main.py"]