import dataiku


def main() -> None:
    source = dataiku.Dataset("customers_raw").get_dataframe()
    source["email"] = source["email"].str.lower()
    source["full_name"] = source["first_name"].str.strip() + " " + source["last_name"].str.strip()
    dataiku.Dataset("customers_curated").write_with_schema(source)


if __name__ == "__main__":
    main()
