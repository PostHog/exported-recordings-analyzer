# Exported Recordings Analyzer

Sometimes a customer reports we can't play their recording.

The browser is frozen, so we can't use the debug tools.

## How to get the data

### wait for the browser to unfreeze

If we're patient we can wait for the browser to unfreeze, and then export the recording as a single JSON file.

this gives you a single file

### download from S3

find the recording in s3 and download the folder

this gives you a directory of files

# What does it do?

It prints out useful information like:

```
message_type_counts
{'Plugin': 3966, 'IncrementalSnapshot': 59529, 'Meta': 13, 'FullSnapshot': 13}
incremental_snapshot_event_source_counts
{'MouseMove': 11350 (3.1MiB), 'Scroll': 9828 (540.2KiB), 'Mutation': 10425 (45.8MiB), 'MouseInteraction': 11149 (709.5KiB), 'Input': 16751 (1.3MiB), 'Selection': 26 (2.4KiB)}
mutation_removal_count: 9520
mutation_addition_counts
{'wat': 28 (19.2KiB), 'Attribute': 200789 (32.0MiB), 'Text': 64359 (6.4MiB)}
```

In this recording, we can see it's almost all attribute additions that are causing the size.

## Usage

get started

```
uv venv --python 3.11
uv venv --python 3.11
uv pip install -r pyproject.toml
```

Edit `main.py` to point at your downloaded recording and run it.

