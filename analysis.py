# https://stackoverflow.com/a/1094933
import dataclasses
from datetime import datetime
from typing import Dict, List, Optional


@dataclasses.dataclass(frozen=True)
class UnterminatedLine:
    file_path: str
    line_index: int
    line_tail: str


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


@dataclasses.dataclass()
class Analysis:
    message_type_counts: Dict[str, int]
    incremental_snapshot_event_source_counts: Dict[str, SizedCount]
    mutation_addition_counts: Dict[str, SizedCount]
    grouped_mutation_attributes_counts: Dict[str, SizedCount]
    individual_mutation_attributes_counts: Dict[str, SizedCount]
    plugin_counts: Dict[str, SizedCount]
    console_log_counts: Dict[str, SizedCount]
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
            console_log_counts={},
        )

    def __add__(self, other: "Analysis") -> "Analysis":
        return Analysis(
            console_log_counts=_combine_sized_count_dicts(
                self.console_log_counts, other.console_log_counts
            ),
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
        mutation_overview = self.top_ten_sized(
            {
                **self.mutation_addition_counts,
                "removal": self.mutation_removal_count,
                "text": self.text_mutation_count,
                **self.grouped_mutation_attributes_counts,
                **self.individual_mutation_attributes_counts,
            }
        )

        console_log_overview = self.top_ten_sized(self.console_log_counts)

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
console logs:
{console_log_overview}

Top 10 Mutations by size:
{mutation_overview}
"""

    @staticmethod
    def top_ten_sized(candidates: Dict[str, SizedCount]) -> str:
        return "\n".join(
            [
                f"{k}: {v}"
                for k, v in sorted(
                    candidates.items(),
                    key=lambda item: item[1].size,
                    reverse=True,
                )
            ][0:10]
        )

    def __repr__(self) -> str:
        return str(self)
