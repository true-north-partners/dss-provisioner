import dataiku


def run() -> None:
    dataset = dataiku.Dataset("customers_published")
    row_count = dataset.get_dataframe(limit=1).shape[0]
    if row_count == 0:
        raise RuntimeError("Dataset 'customers_published' is empty")


if __name__ == "__main__":
    run()
