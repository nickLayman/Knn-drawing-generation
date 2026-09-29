"""Small cross-platform process resource measurements."""

from __future__ import annotations

import sys
import time

try:
    import resource
except ImportError:  # Windows
    resource = None


def _rss_kib(value: int | float) -> int:
    # Linux and the BSDs report KiB; macOS reports bytes.
    return int(value / 1024) if sys.platform == "darwin" else int(value)


def process_max_rss_kib() -> int:
    if resource is None:
        return 0
    return _rss_kib(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


def usage_snapshot() -> dict[str, float | int]:
    if resource is None:
        return {
            "self_user_seconds": time.process_time(),
            "self_system_seconds": 0.0,
            "children_user_seconds": 0.0,
            "children_system_seconds": 0.0,
            "self_max_rss_kib": 0,
            "children_max_rss_kib": 0,
        }
    self_usage = resource.getrusage(resource.RUSAGE_SELF)
    child_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return {
        "self_user_seconds": self_usage.ru_utime,
        "self_system_seconds": self_usage.ru_stime,
        "children_user_seconds": child_usage.ru_utime,
        "children_system_seconds": child_usage.ru_stime,
        "self_max_rss_kib": _rss_kib(self_usage.ru_maxrss),
        "children_max_rss_kib": _rss_kib(child_usage.ru_maxrss),
    }
