import dataclasses
import os
from datetime import datetime

from simplejson import JSONDecodeError

import simplejson as json
from typing import Dict, List, Literal, Optional

import ijson

event_types = {
    -1: "Unknown",
    1: "Load",
    2: "FullSnapshot",
    3: "IncrementalSnapshot",
    4: "Meta",
    5: "Custom",
    6: "Plugin",
}

incremental_snapshot_event_source = {
    0: "Mutation",
    1: "MouseMove",
    2: "MouseInteraction",
    3: "Scroll",
    4: "ViewportResize",
    5: "Input",
    6: "TouchMove",
    7: "MediaInteraction",
    8: "StyleSheetRule",
    9: "CanvasMutation",
    10: "Font",
    11: "Log",
    12: "Drag",
    13: "StyleDeclaration",
    14: "Selection",
    15: "AdoptedStyleSheet",
}

node_types = {
    # rrweb source code https://github.com/rrweb-io/rrweb/blob/master/packages/rrdom/src/document.ts#L781C3-L781C14
    # their comment for zero says:
    # // This isn't a node type. Enum type value starts from zero but NodeType value starts from 1.
    # but we see mutations with 0 as note type 🤷️
    0: "PLACEHOLDER",
    1: "Element",
    2: "Attribute",
    3: "Text",
    4: "CDATA",
    5: "EntityReference",
    6: "Entity",
    7: "ProcessingInstruction",
    8: "Comment",
    9: "Document",
    10: "DocumentType",
    11: "DocumentFragment",
}


# https://stackoverflow.com/a/1094933
def sizeof_fmt(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


@dataclasses.dataclass(frozen=True)
class SizedCount:
    count: int
    size: int

    def combine(self, other: "SizedCount") -> "SizedCount":
        return SizedCount(self.count + other.count, self.size + other.size)

    def __add__(self, bytes_to_add: int) -> "SizedCount":
        return SizedCount(self.count + 1, self.size + bytes_to_add)

    def __str__(self) -> str:
        return f"{self.count} ({sizeof_fmt(self.size)})"

    def __repr__(self):
        return str(self)


def _combine_sized_count_dicts(
    left: Dict[str, SizedCount], right: Dict[str, SizedCount]
) -> Dict:
    right_ = {
        k: left.get(k, SizedCount(0, 0)).combine(right.get(k, SizedCount(0, 0)))
        for k in set(left) | set(right)
    }
    return right_


@dataclasses.dataclass(frozen=True)
class UnterminatedLine:
    file_path: str
    line_index: int
    line_tail: str


@dataclasses.dataclass()
class Analysis:
    message_type_counts: Dict[str, int]
    incremental_snapshot_event_source_counts: Dict[str, SizedCount]
    mutation_addition_counts: Dict[str, SizedCount]
    grouped_mutation_attributes_counts: Dict[str, SizedCount]
    individual_mutation_attributes_counts: Dict[str, SizedCount]
    plugin_counts: Dict[str, SizedCount]
    addition_sizes: List[int]
    mutation_removal_count: SizedCount
    text_mutation_count: SizedCount
    unterminated_lines: List[UnterminatedLine]
    first_timestamp: Optional[int]
    last_timestamp: Optional[int]
    full_snapshot_timestamps: List[int]
    isAttachIFrameCount: int

    @staticmethod
    def empty() -> "Analysis":
        return Analysis(
            message_type_counts={},
            incremental_snapshot_event_source_counts={},
            mutation_addition_counts={},
            grouped_mutation_attributes_counts={},
            individual_mutation_attributes_counts={},
            addition_sizes=[],
            mutation_removal_count=SizedCount(0, 0),
            text_mutation_count=SizedCount(0, 0),
            unterminated_lines=[],
            first_timestamp=None,
            last_timestamp=None,
            full_snapshot_timestamps=[],
            isAttachIFrameCount=0,
            plugin_counts={},
        )

    def __add__(self, other: "Analysis") -> "Analysis":
        return Analysis(
            plugin_counts=_combine_sized_count_dicts(
                self.plugin_counts, other.plugin_counts
            ),
            mutation_removal_count=self.mutation_removal_count.combine(
                other.mutation_removal_count
            ),
            addition_sizes=self.addition_sizes + other.addition_sizes,
            grouped_mutation_attributes_counts=_combine_sized_count_dicts(
                self.grouped_mutation_attributes_counts,
                other.grouped_mutation_attributes_counts,
            ),
            individual_mutation_attributes_counts=_combine_sized_count_dicts(
                self.individual_mutation_attributes_counts,
                other.individual_mutation_attributes_counts,
            ),
            mutation_addition_counts=_combine_sized_count_dicts(
                self.mutation_addition_counts,
                other.mutation_addition_counts,
            ),
            incremental_snapshot_event_source_counts=_combine_sized_count_dicts(
                self.incremental_snapshot_event_source_counts,
                other.incremental_snapshot_event_source_counts,
            ),
            message_type_counts={
                k: self.message_type_counts.get(k, 0)
                + other.message_type_counts.get(k, 0)
                for k in set(self.message_type_counts) | set(other.message_type_counts)
            },
            text_mutation_count=self.text_mutation_count.combine(
                other.text_mutation_count
            ),
            unterminated_lines=self.unterminated_lines + other.unterminated_lines,
            first_timestamp=self.min_of_two_timestamps(other),
            last_timestamp=self.max_of_two_timestamps(other),
            full_snapshot_timestamps=self.full_snapshot_timestamps
            + other.full_snapshot_timestamps,
            isAttachIFrameCount=self.isAttachIFrameCount + other.isAttachIFrameCount,
        )

    def max_of_two_timestamps(self, other):
        if self.last_timestamp is None:
            return other.last_timestamp
        if other.last_timestamp is None:
            return self.last_timestamp
        return max(self.last_timestamp, other.last_timestamp)

    def min_of_two_timestamps(self, other):
        if self.first_timestamp is None:
            return other.first_timestamp
        if other.first_timestamp is None:
            return self.first_timestamp
        return min(self.first_timestamp, other.first_timestamp)

    def __str__(self) -> str:
        # splat all the sized dictionaries together
        # sort by size (desc)
        # and then print top 10 items on a separate line
        mutation_overview = "\n".join(
            [
                f"{k}: {v}"
                for k, v in sorted(
                    {
                        **self.mutation_addition_counts,
                        "removal": self.mutation_removal_count,
                        "text": self.text_mutation_count,
                        **self.grouped_mutation_attributes_counts,
                        **self.individual_mutation_attributes_counts,
                    }.items(),
                    key=lambda item: item[1].size,
                    reverse=True,
                )
            ][0:10]
        )

        return f"""
session start: {datetime.fromtimestamp(self.first_timestamp/1000).isoformat()}
session duration: {datetime.fromtimestamp(self.last_timestamp/1000) - datetime.fromtimestamp(self.first_timestamp/1000)}
full snapshot timestamps
{[f"{datetime.fromtimestamp(x/1000).isoformat()} (after {(datetime.fromtimestamp(x/1000) - datetime.fromtimestamp(self.first_timestamp/1000))})" for x in self.full_snapshot_timestamps]}

message_type_counts
{self.message_type_counts}
incremental_snapshot_event_source_counts
{self.incremental_snapshot_event_source_counts}
    mutation_removal_count: {str(self.mutation_removal_count)}
    mutation_addition_counts
    {self.mutation_addition_counts}
    individual_mutation_attributes_counts
    {self.individual_mutation_attributes_counts}
    grouped_mutation_attributes_counts
    (attribute mutations arrive in arrays - this reports the attributes that come together)
    {self.grouped_mutation_attributes_counts}
    text mutations
    {self.text_mutation_count}
unterminated_lines_count
{len(self.unterminated_lines)}
unterminated_lines
{self.unterminated_lines}
iframes?
{self.isAttachIFrameCount}

plugins
{self.plugin_counts}

Top 10 Mutations by size:
{mutation_overview}
"""

    def __repr__(self) -> str:
        return str(self)


def analyse_exported_file(file_path: str) -> Analysis:
    """
    When operating on an "exported recording" then you have one file which has a snapshots key.
    That is an array of JSON objects. each has a window id but is otherwise an rrweb event
    """
    analysis = Analysis.empty()

    with open(file_path, "r") as file:
        for list_of_snapshots in ijson.items(file, "data.snapshots"):
            analysis += analyse_snapshots(list_of_snapshots)

    return analysis


def analyse_s3_file(file_path: str) -> Analysis:
    """
    If operating on an S3 bucket then you can have multiple files.
    Each file is JSONL (regardless of if its extension is .json)
    Each line is a JSON object.
    Each has a window id and an array at the "data" key.
    Each item in that array is an rrweb event
    """
    analysis = Analysis.empty()

    with open(file_path, "r") as file:
        line_index = -1
        for line in file:
            line_index += 1

            if not line:
                continue

            try:
                json_line = json.loads(line)
                line_analysis = analyse_snapshots(json_line.get("data", []))
                analysis += line_analysis
            except JSONDecodeError:
                analysis.unterminated_lines.append(
                    UnterminatedLine(file_path, line_index, line[-20:])
                )

    return analysis


def ensure_all_mutation_types_are_handled(data: Dict) -> None:
    handled_mutations = ["removes", "adds", "attributes", "texts", "isAttachIframe"]
    ignored_keys = ["source"]
    ignore_list = handled_mutations + ignored_keys
    mutations_present = data.keys()
    unhandled_mutations = [
        mutation for mutation in mutations_present if mutation not in ignore_list
    ]

    if unhandled_mutations:
        print(f"Unhandled mutations in {unhandled_mutations} in '{data}'")
        raise ValueError(f"Unhandled mutations: {unhandled_mutations}")


# TODO ijson returns any :'(
def analyse_snapshots(list_of_snapshots: any) -> Analysis:
    analysis = Analysis.empty()

    for snapshot in list_of_snapshots:
        if analysis.first_timestamp is None:
            analysis.first_timestamp = snapshot["timestamp"]
        if analysis.last_timestamp is None:
            analysis.last_timestamp = snapshot["timestamp"]

        analysis.first_timestamp = min(analysis.first_timestamp, snapshot["timestamp"])
        analysis.last_timestamp = max(analysis.last_timestamp, snapshot["timestamp"])

        event_type = event_types[snapshot.get("type", -1)]

        if event_type == "Plugin":
            plugin_name = snapshot["data"]["plugin"]
            if plugin_name not in analysis.plugin_counts:
                analysis.plugin_counts[plugin_name] = SizedCount(0, 0)
            analysis.plugin_counts[plugin_name] += len(
                json.dumps(snapshot["data"], separators=(",", ":"))
            )

        if event_type == "FullSnapshot":
            analysis.full_snapshot_timestamps.append(snapshot["timestamp"])

        if event_type not in analysis.message_type_counts:
            analysis.message_type_counts[event_type] = 0
        analysis.message_type_counts[event_type] += 1

        if event_type == "IncrementalSnapshot":
            source_ = incremental_snapshot_event_source[snapshot["data"]["source"]]
            if source_ not in analysis.incremental_snapshot_event_source_counts:
                analysis.incremental_snapshot_event_source_counts[source_] = SizedCount(
                    0, 0
                )
            analysis.incremental_snapshot_event_source_counts[source_] += len(
                json.dumps(snapshot["data"], separators=(",", ":"))
            )

            if source_ == "Mutation":
                # mutations we handle
                ensure_all_mutation_types_are_handled(snapshot["data"])

                if snapshot["data"].get("isAttachIframe", False):
                    # these have adds, removes, texts, and attributes like other mutations
                    # let's mostly ignore them right now
                    # TODO handle them
                    analysis.isAttachIFrameCount += 1

                for removal in snapshot["data"]["removes"]:
                    analysis.mutation_removal_count += len(
                        json.dumps(removal, separators=(",", ":"))
                    )

                for addition in snapshot["data"]["adds"]:
                    node_type = node_types[addition["node"]["type"]]
                    if node_type not in analysis.mutation_addition_counts:
                        analysis.mutation_addition_counts[node_type] = SizedCount(0, 0)
                    addition_size = len(json.dumps(addition, separators=(",", ":")))
                    analysis.mutation_addition_counts[node_type] += addition_size
                    analysis.addition_sizes.append(addition_size)

                ## attributes individually
                for altered_attribute in snapshot["data"]["attributes"]:
                    # this is an array of dicts. each should have `attributes`
                    # and that is a dict whose key is the attibute
                    changeds = altered_attribute["attributes"].keys()
                    for changed in changeds:
                        if (
                            changed
                            not in analysis.individual_mutation_attributes_counts
                        ):
                            analysis.individual_mutation_attributes_counts[
                                changed
                            ] = SizedCount(0, 0)
                        analysis.individual_mutation_attributes_counts[changed] += len(
                            json.dumps(altered_attribute["attributes"][changed])
                        )

                # attributes grouped
                for mutated_attribute in snapshot["data"]["attributes"]:
                    # attribute mutations come together in a dict
                    # tracking them individually gives confusing counts
                    attribute_fingerprint = "---".join(
                        mutated_attribute["attributes"].keys()
                    )

                    if (
                        attribute_fingerprint
                        not in analysis.grouped_mutation_attributes_counts
                    ):
                        analysis.grouped_mutation_attributes_counts[
                            attribute_fingerprint
                        ] = SizedCount(0, 0)
                    analysis.grouped_mutation_attributes_counts[
                        attribute_fingerprint
                    ] += len(
                        json.dumps(
                            snapshot["data"]["attributes"], separators=(",", ":")
                        )
                    )

                for text in snapshot["data"]["texts"]:
                    analysis.text_mutation_count += len(text)

    return analysis


def analyse_recording(file_path: str, source: Literal["s3", "export"]) -> None:
    analysis = Analysis.empty()

    if source == "export":
        analysis = analyse_exported_file(file_path)
    elif source == "s3":
        # open each file in the provided directory
        sorted_files = sorted(os.listdir(file_path))
        for file_name in sorted_files:
            print(f"processing file: {file_name}")
            analysis += analyse_s3_file(os.path.join(file_path, file_name))
    else:
        raise ValueError(f"Unknown source {source}")

    print(analysis)


if __name__ == "__main__":
    # TODO get the file path from the command line
    # analyse_recording(
    #     "/Users/paul/Downloads/export-018b6681-dbfd-74e3-b1b8-2e3045abb0ed.ph-recording.json",
    #     "export",
    # )
    analyse_recording("/Users/paul/Downloads/many-console-maybe/", "s3")
