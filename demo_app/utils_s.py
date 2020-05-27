"""
library functions that run on slave nodes
"""
from pyspark.sql import Row
# https://www.thebalance.com/loan-payment-calculations-315564
def calc_mthly_payment(**kwargs):
    row = kwargs.get("row")
    dict_row = row.asDict()
    num_months = dict_row["term_years"] * 12
    mthly_rate = dict_row["apr"] / 12
    discount_factor = (((1 + mthly_rate) ** num_months) - 1) / (mthly_rate * ((1 + mthly_rate) ** num_months))
    pmt_amt = round(dict_row["loan_amt"] / discount_factor, 2)
    row_res = Row(loan_id=dict_row["loan_id"], monthly_pmt_amt=pmt_amt)
    return row_res

def calc_interest(**kwargs):
    row = kwargs.get("row")
    dict_row = row.asDict()
    int_amt = dict_row["loan_amt"] * dict_row["apr"] * dict_row["term_years"]
    row_res = Row(loan_id=dict_row["loan_id"], interest_amt=int_amt)
    return row_res