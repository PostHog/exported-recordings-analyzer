import json
from datetime import datetime, timedelta
from decimal import Decimal
from math import trunc
from typing import Literal

import ijson

# Sample input data
snapshots = [
    {
        "windowId": "018e18a7-0faa-7918-92de-8d31b496df41",
        "type": 4,
        "data": {
            "href": "https://posthog.com/tutorials/nextjs-ab-tests",
            "width": 384,
            "height": 726,
        },
        "timestamp": 1709810585828,
    },
    {
        "windowId": "018e18a7-0faa-7918-92de-8d31b496df41",
        "type": 4,
        "data": {
            "href": "https://posthog.com/tutorials/nextjs-ab-tests",
            "width": 384,
            "height": 726,
        },
        "timestamp": 1709810585828,
        "delay": -144426.67749023438,
    },
]


# Function to handle serialization of complex objects
def default_converter(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


# Function to convert timestamp to ISO-8601
def timestamp_to_iso(timestamp):
    return datetime.utcfromtimestamp(trunc(timestamp / 1000)).isoformat() + "Z"


# Calculate and format timedelta in minutes and seconds
def format_timedelta(minutes, seconds):
    return f"{int(minutes)} minutes and {int(seconds)} seconds"


def convert_snapshot_times(file_path: str, source: Literal["s3", "export"]) -> None:
    if source == "s3":
        raise NotImplementedError("S3 source not implemented yet")

    # Processing each item in the snapshots array
    processed_snapshots = []
    with open(file_path, "r") as file:
        for snapshots in ijson.items(file, "data.snapshots"):
            for snapshot in snapshots:
                processed_snapshot = {
                    "type": snapshot["type"],
                    "timestamp": snapshot["timestamp"],
                    "time": timestamp_to_iso(snapshot["timestamp"]),
                }

                # Process delay if it exists
                if "delay" in snapshot:
                    processed_snapshot["delay"] = snapshot["delay"]
                    delay_seconds = (
                        snapshot["delay"] / 1000
                    )  # Assuming delay is in milliseconds
                    delayed_timestamp = snapshot["timestamp"] + snapshot["delay"]
                    processed_snapshot["delayTime"] = timestamp_to_iso(
                        delayed_timestamp
                    )

                    # Calculate timedelta in minutes and seconds
                    delay_in_seconds = float(snapshot["delay"]) / 1000
                    timedelta_obj = timedelta(seconds=delay_in_seconds)
                    minutes, seconds = divmod(abs(timedelta_obj.total_seconds()), 60)
                    processed_snapshot["timedelta"] = format_timedelta(minutes, seconds)

                processed_snapshots.append(processed_snapshot)

    print(json.dumps(processed_snapshots, indent=2, default=default_converter))


if __name__ == "__main__":
    # TODO get the file path from the command line
    convert_snapshot_times(
        "/Users/paul/Downloads/export-018e18a7-0faa-7918-92de-8d3009a7196f.ph-recording.json",
        "export",
    )
    # analyse_recording("/Users/paul/Downloads/another/", "s3")
