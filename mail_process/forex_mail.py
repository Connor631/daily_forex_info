import io
from loguru import logger
import matplotlib.pyplot as plt
import warnings


warnings.filterwarnings("ignore")

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
    plt.plot(df_day['create_date'], df_day['best_xh_buy'], label='best_xh_buy')
    plt.plot(df_day['create_date'], df_day['best_xh_sell'], label='best_xh_sell')
    # 设置图表标题和坐标轴标签
    plt.title('Best xh buy and sell')
    plt.xlabel('Date')
    plt.ylabel('Forex')
    plt.xticks(rotation=20)
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
