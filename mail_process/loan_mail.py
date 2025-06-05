import io
from loguru import logger
from utils.sql_utils import sql_utils
from datetime import datetime
import matplotlib.pyplot as plt
import warnings


warnings.filterwarnings("ignore")

def get_loan(sql_util):
    sql_data = "SELECT * FROM t_mortgage_loan_schedule"
    sql_config = "SELECT * FROM t_mortgage_loan_config"
    df = sql_util.read_sql(database="forex", sql=sql_data, format='df')
    df_config = sql_util.read_sql(database="forex", sql=sql_config, format='dict')
    # 构建html文本
    # check if the df is empty
    df_paid = df[df["is_Paid"] == "1"]
    df_not_paid = df[df["is_Paid"] == "0"]
    df_this_year = df[df["is_This_Year"] == True]
    df_next_year = df[df["is_Next_Year"] == True]
    # next payment info
    # get the next payment amount
    next_payment_amount = df_not_paid.iloc[0]["Total Monthly Payment"] if not df_not_paid.empty else 0
    # get the next principal payment
    next_principal_payment = df_not_paid.iloc[0]["Total Principal Payment"] if not df_not_paid.empty else 0
    # get the next interest payment
    next_interest_payment = df_not_paid.iloc[0]["Total Interest Payment"] if not df_not_paid.empty else 0
    
    # paid info
    # calculate the total amount paid
    total_amount_paid = df_paid["Total Monthly Payment"].sum()
    # calculate the total principal paid
    total_principal_paid = df_paid["Total Principal Payment"].sum()

    # unpaid info
    # calculate the total amount unpaid
    total_amount_unpaid = df_not_paid["Total Monthly Payment"].sum()
    # calculate the total principal unpaid
    total_principal_unpaid = df_not_paid["Total Principal Payment"].sum()
    # calculate the total amount in next year
    total_amount_next_year = df_next_year["Total Monthly Payment"].sum()

    # calculate the total amount paid in this year
    total_amount_paid_this_year = df_this_year[df_this_year["is_Paid"] == "1"]["Total Monthly Payment"].sum()
    # calculate the total amount unpaid in this year
    total_amount_unpaid_this_year = df_this_year[df_this_year["is_Paid"] == "0"]["Total Monthly Payment"].sum()
    
    html_report = f"""
    <h3>按揭贷款数据</h3>
    <p>报告日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    <table border="1" cellpadding="5" cellspacing="0">
        <tr><th colspan="2">贷款信息</th></tr>
        <tr><td>公积金贷款金额</td><td>{df_config["gjj_loan"]} 元</td></tr>
        <tr><td>商业贷款金额</td><td>{df_config["bank_loan"]} 元</td></tr>
        <tr><td>贷款总额</td><td>{int(df_config["gjj_loan"]) + int(df_config["bank_loan"])} 元</td></tr>
        <tr><td>公积金贷款利率</td><td>{float(df_config["gjj_interest_rate"]) * 100:.2f}%</td></tr>
        <tr><td>商业贷款利率</td><td>{(float(df_config["LPR"]) - float(df_config["minus_bp"])) * 100:.2f}%</td></tr>
        <tr><th colspan="2">还款计划</th></tr>
        <tr><td>下期还款总额</td><td>{round(next_payment_amount, 2)} 元</td></tr>
        <tr><td>下期还款本金</td><td>{round(next_principal_payment, 2)} 元</td></tr>
        <tr><td>下期还款利息</td><td>{round(next_interest_payment, 2)} 元</td></tr>
        <tr><td>总已还款金额</td><td>{round(total_amount_paid, 2)} 元</td></tr>
        <tr><td>总已还款本金</td><td>{round(total_principal_paid, 2)} 元</td></tr>
        <tr><td>总待还款金额</td><td>{round(total_amount_unpaid, 2)} 元</td></tr>
        <tr><td>总待还款本金</td><td>{round(total_principal_unpaid, 2)} 元</td></tr>
        <tr><td>当年已还款金额</td><td>{round(total_amount_paid_this_year, 2)} 元</td></tr>
        <tr><td>当年待还款金额</td><td>{round(total_amount_unpaid_this_year, 2)} 元</td></tr>
        <tr><td>次年待还款金额</td><td>{round(total_amount_next_year, 2)} 元</td></tr>
        <tr><td>还款进度</td><td>{df_paid["progress in percent"].iloc[-1] if not df_paid.empty else 0.0:.2f}%</td></tr>
    </table>
    """

    # 构建图像
    plt.plot(df['Payment Date'], df['progress in percent'], label='loan_progress')
    # 设置图表标题和坐标轴标签
    plt.title('Loan progress')
    plt.xlabel('Date')
    plt.ylabel('Rate')
    plt.xticks(rotation=20)
    # 创建一个BytesIO对象
    imgdata = io.BytesIO()
    # 将图形保存到BytesIO对象中，格式为png
    plt.savefig(imgdata, format='png')
    # 将指针移动到BytesIO对象的起始位置
    imgdata.seek(0)
    # create a report email
    e_msg = {}
    e_msg["html"] =  html_report
    e_msg["img_bytes"] = imgdata
    return e_msg
