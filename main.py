import dataclasses

import simplejson as json
from typing import Dict, List

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

    def __add__(self, bytes: int) -> "SizedCount":
        return SizedCount(self.count + 1, self.size + bytes)

    def __str__(self) -> str:
        return f"{self.count} ({sizeof_fmt(self.size)})"

    def __repr__(self):
        return str(self)


def analyse_exported_recording(file_path: str) -> None:
    message_type_counts = {}
    incremental_snapshot_event_source_counts: Dict[str, SizedCount] = {}
    mutation_addition_counts: Dict[str, SizedCount] = {}
    addition_sizes: List[int] = []
    mutation_removal_count = 0

    with open(file_path, "r") as file:
        for list_of_snapshots in ijson.items(file, "data.snapshots"):
            for snapshot in list_of_snapshots:
                event_type = event_types[snapshot.get("type", -1)]
                if event_type not in message_type_counts:
                    message_type_counts[event_type] = 0
                message_type_counts[event_type] += 1

                if event_type == "IncrementalSnapshot":
                    source_ = incremental_snapshot_event_source[
                        snapshot["data"]["source"]
                    ]
                    if source_ not in incremental_snapshot_event_source_counts:
                        incremental_snapshot_event_source_counts[source_] = SizedCount(
                            0, 0
                        )
                    incremental_snapshot_event_source_counts[source_] += len(
                        json.dumps(snapshot["data"])
                    )

                    if source_ == "Mutation":
                        mutation_removal_count += len(snapshot["data"]["removes"])
                        for addition in snapshot["data"]["adds"]:
                            node_type = node_types[addition["node"]["type"]]
                            if node_type not in mutation_addition_counts:
                                mutation_addition_counts[node_type] = SizedCount(0, 0)
                            addition_size = len(json.dumps(addition))
                            mutation_addition_counts[node_type] += addition_size
                            addition_sizes.append(addition_size)

    print("message_type_counts")
    print(message_type_counts)
    print("incremental_snapshot_event_source_counts")
    print(incremental_snapshot_event_source_counts)
    print("mutation_removal_count: " + str(mutation_removal_count))
    print("mutation_addition_counts")
    print(mutation_addition_counts)


if __name__ == "__main__":
    # TODO get the file path from the command line
    analyse_exported_recording(
        "/Users/paul/Downloads/export-018a8030-524e-7a80-a82c-3e23ba4ebbd2.ph-recording.json"
    )
