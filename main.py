import gzip
import os


from simplejson import JSONDecodeError

import simplejson as json
from typing import Dict, List, Literal, Optional

import ijson

from analysis import Analysis, UnterminatedLine, SizedCount

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
    16: "CustomElement",
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


def analyse_exported_file(
    file_path: str, save_uncompressed: bool = False
) -> tuple[Analysis, List[Dict]]:
    """
    When operating on an "exported recording" then you have one file which has a snapshots key.
    That is an array of JSON objects. each has a window id but is otherwise an rrweb event
    """
    analysis = Analysis.empty()
    uncompressed_data = []

    with open(file_path, "r") as file:
        for list_of_snapshots in ijson.items(file, "data.snapshots"):
            snapshot_analysis, snapshot_data = analyse_snapshots(
                list_of_snapshots, save_uncompressed
            )
            analysis += snapshot_analysis
            uncompressed_data.extend(snapshot_data)

    return analysis, uncompressed_data


def save_uncompressed_data(file_path: str, uncompressed_data: List[Dict]) -> None:
    """Save the uncompressed data to a JSON file"""
    output_path = f"{file_path}_uncompressed.json"
    with open(output_path, "w") as f:
        json.dump(uncompressed_data, f, indent=2)
    print(f"Saved uncompressed data to {output_path}")


def analyse_s3_file(
    file_path: str, save_uncompressed: bool = False
) -> tuple[Analysis, List[Dict]]:
    """
    If operating on an S3 bucket then you can have multiple files.
    Each file is JSONL (regardless of if its extension is .json)
    Each line is a JSON object.
    Each has a window id and an array at the "data" key.
    Each item in that array is an rrweb event
    """
    analysis = Analysis.empty()
    uncompressed_data = []

    with open(file_path, "r") as file:
        line_index = -1
        for line in file:
            line_index += 1

            if not line:
                continue

            try:
                json_line = json.loads(line)
                line_analysis, line_data = analyse_snapshots(
                    json_line.get("data", []), save_uncompressed
                )
                analysis += line_analysis
                uncompressed_data.extend(line_data)
            except JSONDecodeError:
                analysis.unterminated_lines.append(
                    UnterminatedLine(file_path, line_index, line[-20:])
                )

    return analysis, uncompressed_data


def ensure_all_mutation_types_are_handled(data: Dict) -> None:
    handled_mutations = [
        "removes",
        "adds",
        "attributes",
        "texts",
        "isAttachIframe",
        "updates",  # mobile
    ]
    ignored_keys = ["source"]
    ignore_list = handled_mutations + ignored_keys
    mutations_present = data.keys()
    unhandled_mutations = [
        mutation for mutation in mutations_present if mutation not in ignore_list
    ]

    if unhandled_mutations:
        print(f"Unhandled mutations in {unhandled_mutations} in '{data}'")
        raise ValueError(f"Unhandled mutations: {unhandled_mutations}")


def maybe_decompress(x: str | dict | None) -> dict | None:
    if x is None:
        return None
    if isinstance(x, str):
        # Decode the decompressed bytes to a UTF-8 string
        decompressed_str = gzip.decompress(x.encode("latin-1")).decode("utf-8")
        # Parse the JSON string
        return json.loads(decompressed_str)
    return x


# TODO ijson returns any :'(
def analyse_snapshots(
    list_of_snapshots: any, collect_uncompressed: bool = False
) -> tuple[Analysis, List[Dict]]:
    analysis = Analysis.empty()
    uncompressed_data = []

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
            if plugin_name == "rrweb/console@1":
                level = snapshot["data"]["payload"]["level"]
                console_log_line = snapshot["data"]["payload"]["payload"][0]
                fingerprint = level + "---" + console_log_line
                if fingerprint not in analysis.console_log_counts:
                    analysis.console_log_counts[fingerprint] = SizedCount(0, 0)
                analysis.console_log_counts[fingerprint] += len(
                    json.dumps(fingerprint, separators=(",", ":"))
                )

        if event_type == "FullSnapshot":
            analysis.full_snapshot_timestamps.append(snapshot["timestamp"])
            # Decompress the full snapshot data if needed
            if "data" in snapshot:
                snapshot["data"] = maybe_decompress(snapshot["data"])
                snapshot.pop("cv", None)

        if event_type not in analysis.message_type_counts:
            analysis.message_type_counts[event_type] = 0
        analysis.message_type_counts[event_type] += 1

        if event_type == "IncrementalSnapshot":
            data_source_ = snapshot["data"].get("source", None)
            if data_source_ is None:
                print("WoAH unexpected data shape")
                continue

            try:
                source_ = incremental_snapshot_event_source[data_source_]
            except KeyError:
                print(f"Unknown source {data_source_}")
                print(snapshot)
                continue

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

                # Decompress the data in place if needed
                if "removes" in snapshot["data"]:
                    snapshot["data"]["removes"] = maybe_decompress(
                        snapshot["data"]["removes"]
                    )
                if "adds" in snapshot["data"]:
                    snapshot["data"]["adds"] = maybe_decompress(
                        snapshot["data"]["adds"]
                    )
                if "attributes" in snapshot["data"]:
                    snapshot["data"]["attributes"] = maybe_decompress(
                        snapshot["data"]["attributes"]
                    )

                if "texts" in snapshot["data"]:
                    snapshot["data"]["texts"] = maybe_decompress(
                        snapshot["data"]["texts"]
                    )

                snapshot.pop("cv", None)

                for removal in snapshot["data"].get("removes", []):
                    node_id = removal["id"]
                    if node_id not in analysis.removal_by_node_id:
                        analysis.removal_by_node_id[node_id] = SizedCount(0, 0)

                    analysis.removal_by_node_id[node_id] += len(
                        json.dumps(removal, separators=(",", ":"))
                    )
                    analysis.mutation_removal_count += len(
                        json.dumps(removal, separators=(",", ":"))
                    )

                for addition in snapshot["data"].get("adds", []):
                    if "node" in addition:
                        node_id = addition["parentId"]
                        if node_id not in analysis.addition_by_node_id:
                            analysis.addition_by_node_id[node_id] = SizedCount(0, 0)

                        analysis.addition_by_node_id[node_id] += len(
                            json.dumps(addition, separators=(",", ":"))
                        )

                        node_type = node_types[addition["node"]["type"]]
                        if node_type not in analysis.mutation_addition_counts:
                            analysis.mutation_addition_counts[node_type] = SizedCount(
                                0, 0
                            )
                        addition_size = len(json.dumps(addition, separators=(",", ":")))
                        analysis.mutation_addition_counts[node_type] += addition_size
                        analysis.addition_sizes.append(addition_size)

                        # adds changes by value, so we can find particular additions that are adding to size
                        keyable_value = addition["node"].get(
                            "textContent", json.dumps(addition["node"])
                        )[:300]
                        if keyable_value not in analysis.mutation_addition_by_value:
                            analysis.mutation_addition_by_value[keyable_value] = (
                                SizedCount(0, 0)
                            )
                        analysis.mutation_addition_by_value[keyable_value] += (
                            addition_size
                        )
                    else:
                        # print("ooh a mobile recording")
                        # print(json.dumps(addition, separators=(",", ":")))
                        pass

                ## attributes individually
                for altered_attribute in snapshot["data"].get("attributes", []):
                    if "attributes" not in altered_attribute:
                        # print("ooh a mobile recording")
                        # print(json.dumps(altered_attribute, separators=(",", ":")))
                        pass
                    else:
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
                            analysis.individual_mutation_attributes_counts[changed] += (
                                len(
                                    json.dumps(altered_attribute["attributes"][changed])
                                )
                            )

                # attributes grouped
                for mutated_attribute in snapshot["data"].get("attributes", []):
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

                for text in snapshot["data"].get("texts", []):
                    node_id = text["id"]
                    if node_id not in analysis.text_by_node_id:
                        analysis.text_by_node_id[node_id] = SizedCount(0, 0)
                    analysis.text_by_node_id[node_id] += len(text)
                    analysis.text_mutation_count += len(text)

        if collect_uncompressed:
            # Store the snapshot after all decompression has been done
            uncompressed_data.append(snapshot)

    return analysis, uncompressed_data


def analyse_recording(
    file_path: str, source: Literal["s3", "export"], save_uncompressed: bool = False
) -> None:
    analysis = Analysis.empty()
    all_uncompressed_data = []

    if source == "export":
        analysis, uncompressed_data = analyse_exported_file(
            file_path, save_uncompressed
        )
        all_uncompressed_data.extend(uncompressed_data)
    elif source == "s3":
        # open each file in the provided directory
        sorted_files = sorted(os.listdir(file_path))
        for file_name in sorted_files:
            print(f"processing file: {file_name}")
            file_analysis, uncompressed_data = analyse_s3_file(
                os.path.join(file_path, file_name), save_uncompressed
            )
            analysis += file_analysis
            all_uncompressed_data.extend(uncompressed_data)
    else:
        raise ValueError(f"Unknown source {source}")

    print(analysis)

    if save_uncompressed and all_uncompressed_data:
        save_uncompressed_data(file_path, all_uncompressed_data)


if __name__ == "__main__":
    # TODO get the file path from the command line
    # analyse_recording(
    #     "/Users/paul/Downloads/large-sessions/export-019498a4-1d21-7d1b-9b6f-0b704784ee0f.ph-recording.json",
    #     "export",
    # )
    analyse_recording(
        "/Users/pauldambra/Downloads/01959b9e-478d-775a-b341-4a86e04a0e27",
        "s3",
        save_uncompressed=True,
    )
