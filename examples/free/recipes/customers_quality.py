import dataiku


def main() -> None:
    customers = dataiku.Dataset("customers_curated").get_dataframe()
    qa = customers.assign(
        total_rows=len(customers),
        missing_email=customers["email"].isna().sum(),
        missing_full_name=customers["full_name"].isna().sum(),
    )[["total_rows", "missing_email", "missing_full_name"]].head(1)
    dataiku.Dataset("qa_report").write_with_schema(qa)


if __name__ == "__main__":
    main()
