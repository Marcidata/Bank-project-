from validate_transactions import validate_transactions

def main():
    filepath = "/Users/AliceNguyen/Documents/Data Manager - TUC/(År 1.7) Datakvalitet/transactions.csv"
    valid, invalid = validate_transactions(filepath)

    valid.to_csv("valid_transactions.csv", index=False)
    invalid.to_csv("invalid_transactions.csv", index=False)

    print(f"✅ Sparade {len(valid)} giltiga och {len(invalid)} ogiltiga transaktioner.")
    print("Filer: valid_transactions.csv & invalid_transactions.csv")

if __name__ == "__main__":
    main()
