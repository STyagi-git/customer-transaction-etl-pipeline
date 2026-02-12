import pandas as pd
import numpy as np
import os


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

df = pd.read_csv(os.path.join(DATA_DIR, 'anz.csv'))

#print(df.columns.to_list())

INPUT_FILE = "input.csv"
CUSTOMERS_OUT = os.path.join(DATA_DIR, 'customers.csv')
TRANSACTIONS_OUT = os.path.join(DATA_DIR, 'transactions.csv')

CUSTOMER_COLS = [
    "customer_id",
    "account",
    "first_name",
    "gender",
    "age",
    "country",
    "long_lat",
    "balance"
]

TRANSACTION_COLS = [
    "transaction_id",
    "customer_id",
    "date",
    "amount",
    "currency",
    "movement",
    "status",
    "card_present_flag",
    "txn_description",
    "extraction",
    "merchant_id",
    "merchant_code",
    "merchant_suburb",
    "merchant_state",
    "merchant_long_lat",
    "bpay_biller_code"
]

customers_df = (
    df[CUSTOMER_COLS]
    .drop_duplicates(subset=["customer_id"])
    .reset_index(drop=True)
)

transactions_df = (
    df[TRANSACTION_COLS]
    .sort_values("date")
    .reset_index(drop=True)
)

customers_df.to_csv(CUSTOMERS_OUT, index=False)
transactions_df.to_csv(TRANSACTIONS_OUT, index=False)

print("Files written:")
print(f" - {CUSTOMERS_OUT} ({len(customers_df)} rows)")
print(f" - {TRANSACTIONS_OUT} ({len(transactions_df)} rows)")


