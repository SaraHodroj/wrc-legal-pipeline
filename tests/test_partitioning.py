"""Partitioning is pure logic with fiddly boundaries -- exactly what unit tests
are for. Every bug here is an off-by-one that silently drops or duplicates a
month of legal decisions, which is the kind of error nobody notices for weeks.
"""

from datetime import date

import pytest

from wrc_pipeline.partitioning import Partition, iter_partitions


def test_monthly_partitions_cover_a_full_year():
    partitions = list(iter_partitions(date(2024, 1, 1), date(2025, 1, 1), "monthly"))
    assert len(partitions) == 12
    assert partitions[0] == Partition(date(2024, 1, 1), date(2024, 1, 31))
    assert partitions[-1] == Partition(date(2024, 12, 1), date(2024, 12, 31))


def test_partitions_are_contiguous_and_non_overlapping():
    partitions = list(iter_partitions(date(2023, 1, 1), date(2024, 1, 1), "monthly"))
    for earlier, later in zip(partitions, partitions[1:], strict=False):
        assert (later.start - earlier.end).days == 1, "gap or overlap between partitions"


def test_leap_year_february_is_handled():
    partitions = list(iter_partitions(date(2024, 2, 1), date(2024, 3, 1), "monthly"))
    assert partitions[0].end == date(2024, 2, 29)


def test_month_end_anchor_clamps_to_shorter_months():
    """Starting on the 31st must not overflow into the next month."""
    partitions = list(iter_partitions(date(2024, 1, 31), date(2024, 4, 30), "monthly"))
    assert partitions[0].start == date(2024, 1, 31)
    assert partitions[1].start == date(2024, 2, 29)


def test_final_partition_is_clamped_to_end_date():
    partitions = list(iter_partitions(date(2024, 1, 1), date(2024, 1, 15), "monthly"))
    assert len(partitions) == 1
    assert partitions[0].end == date(2024, 1, 14)


@pytest.mark.parametrize(
    "size,expected",
    [("daily", 31), ("weekly", 5), ("monthly", 1)],
)
def test_partition_sizes(size, expected):
    partitions = list(iter_partitions(date(2024, 1, 1), date(2024, 2, 1), size))
    assert len(partitions) == expected


def test_inverted_window_is_rejected_loudly():
    with pytest.raises(ValueError, match="strictly before"):
        list(iter_partitions(date(2024, 6, 1), date(2024, 1, 1)))


def test_partition_key_is_stable():
    assert Partition(date(2024, 3, 1), date(2024, 3, 31)).key == "2024-03-01"
