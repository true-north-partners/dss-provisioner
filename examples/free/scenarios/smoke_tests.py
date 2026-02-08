import dataiku


def run() -> None:
    dataset = dataiku.Dataset("customers_published")
    row_count = dataset.get_dataframe(limit=1).shape[0]
    if row_count < 0:
        raise RuntimeError("Unexpected negative row count")


if __name__ == "__main__":
    run()
