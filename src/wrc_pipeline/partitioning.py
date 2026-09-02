"""Date-window partitioning.

The site's search is date-filtered and pages its results. Rather than asking it
for "everything between 2015 and today" (one enormous, un-resumable, deeply
paginated result set), we slice the requested window into fixed periods and
treat each ``(body, partition)`` pair as an independent unit of work.

Why that matters:

* **Resumability** -- a failed partition is retried on its own; we never redo
  a completed one.
* **Bounded result sets** -- deep pagination is where these search UIs get slow
  and flaky. Small windows keep every result set shallow.
* **Parallelism** -- independent partitions can be fanned out across workers.
* **Traceability** -- ``partition_date`` on every record tells us exactly which
  unit of work produced it, which is what makes a targeted re-run possible.
"""

from __future__ import annotations

from calendar import monthrange
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta

from .config import PartitionSize


@dataclass(frozen=True, slots=True)
class Partition:
    """A closed date interval ``[start, end]`` plus its canonical key."""

    start: date
    end: date

    @property
    def key(self) -> str:
        """Stable identifier used in log lines, object keys and Mongo records."""
        return self.start.isoformat()

    def __str__(self) -> str:  # pragma: no cover - cosmetic
        return f"{self.start.isoformat()}..{self.end.isoformat()}"


def _add_months(anchor: date, months: int) -> date:
    """Month arithmetic that clamps to the last valid day of the target month."""
    total = anchor.month - 1 + months
    year = anchor.year + total // 12
    month = total % 12 + 1
    day = min(anchor.day, monthrange(year, month)[1])
    return date(year, month, day)


def iter_partitions(
    start_date: date,
    end_date: date,
    size: PartitionSize = "monthly",
) -> Iterator[Partition]:
    """Yield consecutive, non-overlapping partitions covering ``[start, end)``.

    The end date is treated as *exclusive* so that calling the pipeline for
    ``2024-01-01 -> 2025-01-01`` produces exactly the twelve months of 2024,
    with no off-by-one overlap into January 2025.

    Raises:
        ValueError: if ``start_date`` is not strictly before ``end_date``.
    """
    if start_date >= end_date:
        raise ValueError(
            f"start_date ({start_date}) must be strictly before end_date ({end_date})"
        )

    cursor = start_date
    while cursor < end_date:
        if size == "daily":
            nxt = cursor + timedelta(days=1)
        elif size == "weekly":
            nxt = cursor + timedelta(weeks=1)
        elif size == "monthly":
            nxt = _add_months(cursor, 1)
        elif size == "quarterly":
            nxt = _add_months(cursor, 3)
        elif size == "yearly":
            nxt = _add_months(cursor, 12)
        else:  # pragma: no cover - guarded by the Literal type + pydantic
            raise ValueError(f"Unsupported partition size: {size!r}")

        # Clamp the final partition so we never query past the requested window.
        boundary = min(nxt, end_date)
        yield Partition(start=cursor, end=boundary - timedelta(days=1))
        cursor = nxt
