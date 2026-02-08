import dataiku


def run() -> None:
    dataset = dataiku.Dataset("reporting_customers")
    frame = dataset.get_dataframe(limit=100)
    if frame.empty:
        raise RuntimeError("reporting_customers is empty after build")


if __name__ == "__main__":
    run()
