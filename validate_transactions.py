import pandas as pd
import datetime
import great_expectations as gx
import warnings


def validate_transactions(filepath: str):
    warnings.filterwarnings("ignore", message="`result_format` configured at the Validator-level*")

    df = pd.read_csv(filepath, sep=";", encoding='ISO-8859-1')

    currency_pattern = r"^[A-Z]{3}$"
    timestamp_pattern = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
    valid_currencies = ["SEK", "DKK", "USD", "EUR", "NOK", "RMB", "ZAR", "GBP", "ZMW", "JPY"]

    datetime_formats = [
        "%Y%m%d %H:%M:%S", "%y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H.%M", "%Y-%m-%d %H.%M:%S", "%Y-%m-%d %H.%M.%S",
        "%Y-%m-%d %H:%M:%S", "%Y.%m.%d %H.%M.%S"
    ]

    def fix_amount(s):
        if pd.isna(s):
            return s
        s = str(s).replace(" ", "").replace(".", "")
        return s[:-2] + "." + s[-2:] if len(s) > 2 else s

    df["amount"] = df["amount"].apply(fix_amount)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["currency"] = df["currency"].astype(str).str.replace(" ", "")

    def parse_and_format(date_str):
        for fmt in datetime_formats:
            try:
                return datetime.datetime.strptime(date_str, fmt).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
        return None

    df["timestamp"] = df["timestamp"].apply(parse_and_format)

    filtered_df = df[
        (df["transaction_type"] == "outgoing") &
        (~df["sender_account"].astype(str).str.startswith("SE"))
    ]

    # Create the ephemeral GX context
    context = gx.get_context()

    # Add a pandas datasource
    data_source = context.data_sources.add_pandas(name="pandas")

    # Add a dataframe asset
    data_asset = data_source.add_dataframe_asset(name="transactions_data")


    # Define the batch (entire DataFrame)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name="batch_def")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    # Create the expectation suite with a name
    suite = gx.core.expectation_suite.ExpectationSuite(name="transactions_suite")

    # Get the validator using the suite
    validator = context.get_validator(batch=batch, expectation_suite=suite)

    # ✅ Detta fungerar i nya versionen av GX

    validator.expect_column_values_to_match_regex("timestamp", regex=timestamp_pattern)
    validator.expect_column_values_to_not_be_null("transaction_id")
    validator.expect_column_values_to_be_between("amount", min_value=0.01, max_value=100000)
    validator.expect_column_values_to_match_regex("currency", regex=currency_pattern)
    validator.expect_column_values_to_be_in_set("currency", value_set=valid_currencies)

    required_columns = [
        "transaction_id", "sender_account", "receiver_account", "sender_country",
        "sender_municipality", "receiver_country", "transaction_type"
    ]
    for col in required_columns:
        validator.expect_column_values_to_not_be_null(col)

    results = validator.validate()

    unexpected_indexes = set()
    for result in results["results"]:
        unexpected_indexes.update(result["result"].get("unexpected_index_list", []))

    filtered_indexes = set(filtered_df.index)
    total_invalid_indexes = unexpected_indexes.union(filtered_indexes)


    invalid_transactions = df.loc[list(total_invalid_indexes)].copy()
    valid_transactions = df.drop(index=list(total_invalid_indexes)).copy()

    valid_transactions.to_csv("valid_transactions.csv", index=False)
    invalid_transactions.to_csv("invalid_transactions.csv", index=False)

    print(f"Totalt: {len(df)} rader")
    print(f"Giltiga: {len(valid_transactions)}")
    print(f"Ogiltiga: {len(invalid_transactions)}")

    return valid_transactions, invalid_transactions


