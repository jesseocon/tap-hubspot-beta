"""A bulk change of a wide object (every record changed, ~1,600 properties) read by list + batch
read stays flat in memory: pages are written and let go as they stream (tests/bulk_change.py).

Measured 2026-11-18 (macOS, 1,600 properties): 150,000 contacts and 150,000 deals in two tap
processes at once peaked at 119 and 106 MB; 20,000 at 123 and 113 MB. The bound is the same for
any count, so a tap that held records would cross it."""

from bulk_change import run

BOUND_MB = 400


def test_a_wide_bulk_change_by_list_stays_under_the_bound():
    out = run({"contacts": 30000, "deals": 30000}, props=1600)
    for obj, result in out.items():
        assert result["exit"] == 0, obj
        assert result["records"] == 30000, obj
        assert result["peak_mb"] < BOUND_MB, (obj, result["peak_mb"])
        # Flat, not growing: the last third of the run is no higher than the first third's peak + 25%.
        trace = [mb for _, mb in result["rss"] if mb > 20]
        third = max(1, len(trace) // 3)
        assert max(trace[-third:]) <= max(trace[:third]) * 1.25, (obj, trace)
