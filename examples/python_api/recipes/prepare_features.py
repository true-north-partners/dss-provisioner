import dataiku


def main() -> None:
    events = dataiku.Dataset("events_raw").get_dataframe()
    events["event_day"] = events["timestamp"].astype(str).str[:10]
    dataiku.Dataset("events_features").write_with_schema(events)


if __name__ == "__main__":
    main()
