import dataiku
import pandas as pd


def main() -> None:
    customers = dataiku.Dataset("customers_raw").get_dataframe()
    reference = dataiku.Dataset("shared_reference_customers").get_dataframe()

    merged = customers.merge(reference, on="customer_id", how="left", suffixes=("", "_ref"))
    merged["risk_score"] = merged.get("risk_score_ref", pd.Series([0] * len(merged))).fillna(0)

    dataiku.Dataset("customers_curated").write_with_schema(merged)


if __name__ == "__main__":
    main()
