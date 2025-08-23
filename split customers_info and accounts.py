import pandas as pd

# Läs in originalfilen
df = pd.read_csv("/Users/AliceNguyen/Documents/Data Manager - TUC/(År 1.7) Datakvalitet/sebank_customers_with_accounts.csv")

# 1. Skapa CSV med bara konto (Account och kunder namn)
account_df = df[["BankAccount","Customer"]]
account_df.to_csv("accounts.csv", index=False)

# 2. Skapa CSV med kundinformation utan konto
customer_df = df.drop(columns=["BankAccount"])
customer_df.to_csv("kunder_utan_account.csv", index=False)

print("Två nya CSV-filer skapade: 'accounts.csv' och 'kunder_utan_account.csv'")

