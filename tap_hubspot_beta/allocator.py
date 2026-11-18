"""glibc allocator settings for a tap process that reads wide objects on a thread pool.

FourthRev, 2026-11-18 (memory_diagnostics, run 2e3983a9): the contacts tap process held 2.3 GB
of RSS while glibc had only 45-119 MB in use; ~2.1 GB was freed memory kept in its arenas.
Batch reads of 100 contacts x 1,897 properties are ~8 MB responses, decoded on 8 worker threads:
each thread gets its own arena (glibc allows 8 per CPU), and after the first big buffer is freed
glibc raises its mmap threshold (up to 32 MB), so the 8 MB bodies and their decoded text land in
those arenas too. Freed between live pages, that memory is neither reused across threads nor
returned to the system, and the process grows to the 4 GiB limit.

tune() (at the start of a sync, before any thread pool): two arenas for the whole process, and a
fixed 1 MiB mmap threshold, so buffers of a megabyte or more are always their own mapping and go
back to the system when freed. trim() hands free pages inside the arenas back (malloc_trim).
Elsewhere than glibc both do nothing.
"""

from __future__ import annotations

import ctypes
import ctypes.util

M_TRIM_THRESHOLD = -1
M_MMAP_THRESHOLD = -3
M_ARENA_MAX = -8
ARENAS = 2
MMAP_THRESHOLD = 1 << 20


def _glibc():
    try:
        libc = ctypes.CDLL("libc.so.6")
        libc.mallopt.argtypes = [ctypes.c_int, ctypes.c_int]
        libc.malloc_trim.argtypes = [ctypes.c_size_t]
        return libc
    except (OSError, AttributeError):
        return None


_LIBC = _glibc()
_tuned = False


def tune() -> bool:
    """Set the arenas and the mmap threshold once per process; True when glibc took them."""
    global _tuned
    if _LIBC is None:
        return False
    if not _tuned:
        _tuned = bool(_LIBC.mallopt(M_ARENA_MAX, ARENAS)) and bool(_LIBC.mallopt(M_MMAP_THRESHOLD, MMAP_THRESHOLD))
    return _tuned


def trim() -> None:
    """Return free memory inside the arenas to the system."""
    if _LIBC is not None:
        _LIBC.malloc_trim(0)
