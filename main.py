import dataclasses
import os
from simplejson import JSONDecodeError

import simplejson as json
from typing import Dict, List, Literal

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
    0: "wat",
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

    def __add__(self, bytes: int) -> "SizedCount":
        return SizedCount(self.count + 1, self.size + bytes)

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
    message_type_counts: Dict[str, int] = dataclasses.field(default_factory=dict)
    incremental_snapshot_event_source_counts: Dict[str, SizedCount] = dataclasses.field(
        default_factory=dict
    )
    mutation_addition_counts: Dict[str, SizedCount] = dataclasses.field(
        default_factory=dict
    )
    mutation_attributes_counts: Dict[str, SizedCount] = dataclasses.field(
        default_factory=dict
    )
    addition_sizes: List[int] = dataclasses.field(default_factory=list)
    mutation_removal_count: int = 0
    unterminated_lines: List[UnterminatedLine] = dataclasses.field(default_factory=list)

    def __add__(self, other: "Analysis") -> "Analysis":
        return Analysis(
            mutation_removal_count=self.mutation_removal_count
            + other.mutation_removal_count,
            addition_sizes=self.addition_sizes + other.addition_sizes,
            mutation_attributes_counts=_combine_sized_count_dicts(
                self.mutation_attributes_counts,
                other.mutation_attributes_counts,
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
            unterminated_lines=self.unterminated_lines + other.unterminated_lines,
        )

    def __str__(self) -> str:
        return f"""message_type_counts
        {self.message_type_counts}
        incremental_snapshot_event_source_counts
        {self.incremental_snapshot_event_source_counts}
        mutation_removal_count: {str(self.mutation_removal_count)}
        mutation_addition_counts
        {self.mutation_addition_counts}
        mutation_attributes_counts"
        {self.mutation_attributes_counts}
        unterminated_lines
        {self.unterminated_lines}
        """

    def __repr__(self) -> str:
        return str(self)


def analyse_exported_file(file_path: str) -> Analysis:
    analysis = Analysis()

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
    analysis = Analysis()

    with open(file_path, "r") as file:
        line_index = -1
        for line in file:
            line_index += 1

            if not line:
                continue

            try:
                json_line = json.loads(line)
                analysis += analyse_snapshots(json_line.get("data", []))
            except JSONDecodeError:
                analysis.unterminated_lines.append(
                    UnterminatedLine(file_path, line_index, line[-20:])
                )

    return analysis


# TODO ijson returns any :'(
def analyse_snapshots(list_of_snapshots: any) -> Analysis:
    analysis = Analysis()

    for snapshot in list_of_snapshots:
        event_type = event_types[snapshot.get("type", -1)]
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
                json.dumps(snapshot["data"])
            )

            if source_ == "Mutation":
                analysis.mutation_removal_count += len(snapshot["data"]["removes"])
                for addition in snapshot["data"]["adds"]:
                    node_type = node_types[addition["node"]["type"]]
                    if node_type not in analysis.mutation_addition_counts:
                        analysis.mutation_addition_counts[node_type] = SizedCount(0, 0)
                    addition_size = len(json.dumps(addition))
                    analysis.mutation_addition_counts[node_type] += addition_size
                    analysis.addition_sizes.append(addition_size)

                for altered_attribute in snapshot["data"]["attributes"]:
                    # this is an array of dicts. each should have `attributes`
                    # and that is a dict whose key is the attibute
                    changeds = altered_attribute["attributes"].keys()
                    for changed in changeds:
                        if changed not in analysis.mutation_attributes_counts:
                            analysis.mutation_attributes_counts[changed] = SizedCount(
                                0, 0
                            )
                        analysis.mutation_attributes_counts[changed] += len(
                            json.dumps(altered_attribute["attributes"][changed])
                        )

    return analysis


def analyse_recording(file_path: str, source: Literal["s3", "export"]) -> None:
    """
    When operating on an "exported recording" then you have one file which has a snapshots key.
    That is an array of JSON objects. each has a window id but is otherwise an rrweb event


    """

    analysis: Analysis = Analysis()

    if source == "export":
        analysis = analyse_exported_file(file_path)
    elif source == "s3":
        # open each file in the provided directory
        sorted_files = sorted(os.listdir(file_path))
        for file_name in sorted_files:
            analysis = analyse_s3_file(os.path.join(file_path, file_name))
    else:
        raise ValueError(f"Unknown source {source}")

    print(analysis)


if __name__ == "__main__":
    # TODO get the file path from the command line
    analyse_recording(
        "/Users/paul/Downloads/export-018af09a-5995-7334-8970-ac2bf0821e92.ph-recording.json",
        "export",
    )
    # analyse_recording("/Users/paul/Downloads/huge-recording/", "s3")
