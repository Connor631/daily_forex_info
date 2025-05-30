import numpy as np
import pandas as pd
from datetime import datetime


class MortgageLoan:
    def __init__(self, bank_loan_amount, gjj_loan_amount, loan_begging_date, loan_term_month, initial_gjj_rate, initial_bank_rate):
        self.bank_loan_amount = bank_loan_amount
        self.gjj_loan_amount = gjj_loan_amount
        self.total_loan_amount = bank_loan_amount + gjj_loan_amount
        self.loan_term_month = loan_term_month
        self.gjj_rate = initial_gjj_rate
        self.bank_rate = initial_bank_rate
        # check if the loan_begging_date is in the correct format
        try:
            datetime.strptime(loan_begging_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Loan begging date must be in YYYY-MM-DD format.")
        self.loan_begging_date = loan_begging_date
    

    def deal_with_first_month(self):
        """
        Handle the first month interest calculation.
        The first month interest is calculated based on the total loan amount.
        """
        # calculate the first month interest
        gjj_1st_month_interest = self.gjj_loan_amount * (self.gjj_rate) / 12
        bank_1st_month_interest = self.bank_loan_amount * (self.bank_rate) / 12
        # if the first month is not a full month, we need to adjust the interest
        first_month_date = datetime.strptime(self.loan_begging_date, "%Y-%m-%d")
        if first_month_date.day != 1:
            # calculate the number of days in the first month
            days_in_first_month = (first_month_date.replace(month=first_month_date.month % 12 + 1, day=20) - first_month_date).days
            # adjust the interest based on the number of days in the first month
            gjj_1st_month_interest *= days_in_first_month / 30
            bank_1st_month_interest *= days_in_first_month / 30
        return gjj_1st_month_interest, bank_1st_month_interest


    def calc_schedule(self, gjj_rate=None, bank_rate=None):
        if gjj_rate:
            self.gjj_rate = gjj_rate
        if bank_rate:
            self.bank_rate = bank_rate
        now = datetime.now()
        # init the schedule
        df = pd.DataFrame(range(self.loan_term_month), columns=["order"])
        # calculate the principal monthly payment
        df["Payment Order"] = df["order"] + 1
        df["Bank Principal Payment"] = self.bank_loan_amount / (self.loan_term_month)
        df["GJJ Principal Payment"] = self.gjj_loan_amount / (self.loan_term_month)
        df["Total Principal Payment"] = df["Bank Principal Payment"] + df["GJJ Principal Payment"]
        # calculate the monthly interest
        df["Paid Bank Principal"] = df["Bank Principal Payment"] * df["order"]
        df["Paid GJJ Principal"] = df["GJJ Principal Payment"] * df["order"]
        df["Bank Interest Payment"] = (self.bank_loan_amount - df["Paid Bank Principal"]) * (bank_rate)/12
        df["GJJ Interest Payment"] = (self.gjj_loan_amount - df["Paid GJJ Principal"]) * (gjj_rate)/12
        # adjust the first month payment
        gjj_1st_month_interest, bank_1st_month_interest = self.deal_with_first_month()
        df.loc[0, "GJJ Interest Payment"] = gjj_1st_month_interest
        df.loc[0, "Bank Interest Payment"] = bank_1st_month_interest
        df["Total Interest Payment"] = df["Bank Interest Payment"] + df["GJJ Interest Payment"]
        # calculate the total monthly payment
        df["Total Monthly Payment"] = df["Total Principal Payment"] + df["Total Interest Payment"]
        # calculate the remaining principal
        df["Bank Principal Left"] = self.bank_loan_amount - df["Bank Principal Payment"] * df["Payment Order"]
        df["GJJ Principal Left"] = self.gjj_loan_amount - df["GJJ Principal Payment"] * df["Payment Order"]
        # add date index
        # 计算首期还款日（即loan_begging_date的下月20号）
        first_payment_date = pd.to_datetime(self.loan_begging_date) + pd.offsets.MonthBegin(1)
        df['_Payment Date'] = pd.date_range(start=first_payment_date, periods=len(df), freq='MS')
        df['Payment Date'] = df['_Payment Date'].apply(lambda d: d.replace(day=20))
        df['Payment Year'] = df['Payment Date'].dt.strftime('%Y')
        df['Payment Month'] = df['Payment Date'].dt.strftime('%m')
        df['is_Paid'] = df["Payment Date"].apply(lambda x: "1" if x < now else "0")
        df['is_This_Year'] =  df['Payment Year'] == str(now.year)
        df['is_Next_Year'] =  df['Payment Year'] == str(now.year + 1)
        # add progress in percent
        df["progress in percent"] = df["Total Monthly Payment"].cumsum() / df["Total Monthly Payment"].sum() * 100
        return df.round(2)

    def report_mortgage_loan(self, df=None):
        if df is None:
            df = self.calc_schedule()
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
        
        # create a report email
        e_msg = {}
        html_report = f"""
        <h3>按揭贷款数据</h3>
        <p>报告日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <table border="1" cellpadding="5" cellspacing="0">
            <tr><th colspan="2">贷款信息</th></tr>
            <tr><td>贷款总额</td><td>{self.total_loan_amount} 元</td></tr>
            <tr><td>公积金贷款金额</td><td>{self.gjj_loan_amount} 元</td></tr>
            <tr><td>商业贷款金额</td><td>{self.bank_loan_amount} 元</td></tr>
            <tr><td>公积金贷款利率</td><td>{self.gjj_rate * 100:.2f}%</td></tr>
            <tr><td>商业贷款利率</td><td>{self.bank_rate * 100:.2f}%</td></tr>
            <tr><th colspan="2">还款计划</th></tr>
            <tr><td>下期还款总额</td><td>{round(next_payment_amount, 2)} 元</td></tr>
            <tr><td>下期还款本金</td><td>{round(next_principal_payment, 2)} 元</td></tr>
            <tr><td>下期还款利息</td><td>{round(next_interest_payment, 2)} 元</td></tr>
            <tr><td>总还款金额</td><td>{round(total_amount_paid, 2)} 元</td></tr>
            <tr><td>总还款本金</td><td>{round(total_principal_paid, 2)} 元</td></tr>
            <tr><td>总待还金额</td><td>{round(total_amount_unpaid, 2)} 元</td></tr>
            <tr><td>总待还本金</td><td>{round(total_principal_unpaid, 2)} 元</td></tr>
            <tr><td>下年待还款金额</td><td>{round(total_amount_next_year, 2)} 元</td></tr>
            <tr><td>当年已还款金额</td><td>{round(total_amount_paid_this_year, 2)} 元</td></tr>
            <tr><td>当年待还款金额</td><td>{round(total_amount_unpaid_this_year, 2)} 元</td></tr>
            <tr><td>还款进度</td><td>{df_paid["progress in percent"].iloc[-1] if not df_paid.empty else 0.0:.2f}%</td></tr>
        </table>
        """
        e_msg["html"] =  html_report
        # e_msg["img_bytes"] = imgdata
        return e_msg

def mortgage_data_main(sql_util=None, config=None):
    # Example usage
    gjj_loan = 1097000
    bank_loan = 978000
    gjj_interest_rate = 0.026
    LPR = 0.035
    minus_bp = 0.005
    loan_term_years = 30
    loan_begging_date = "2025-05-26"

    # Create a MortgageLoan object
    mortgage_loan = MortgageLoan(
        bank_loan_amount=bank_loan,
        gjj_loan_amount=gjj_loan,
        loan_begging_date=loan_begging_date,
        loan_term_month=loan_term_years * 12,
        initial_gjj_rate=gjj_interest_rate,
        initial_bank_rate=LPR - minus_bp
    )
    df = mortgage_loan.calc_schedule(
        gjj_rate=gjj_interest_rate,
        bank_rate=LPR - minus_bp,
    )
    sql_util.df_write_table(
        df, table_name="t_mortgage_loan_schedule", database="forex"
    )

if __name__ == "__main__":
    pass
