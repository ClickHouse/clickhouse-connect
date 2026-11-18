# cython: freethreading_compatible = True

import os

from cpython.mem cimport PyMem_Calloc, PyMem_Free, PyMem_Malloc
from cpython.object cimport PyObject
from cpython.unicode cimport (
    Py_UCS1, Py_UCS2, Py_UNICODE_ISALNUM, PyUnicode_DATA,
    PyUnicode_FromKindAndData, PyUnicode_GET_LENGTH, PyUnicode_KIND, PyUnicode_READ,
)
from libc.stdint cimport uint64_t
from libc.string cimport memchr, memcmp, memcpy

from clickhouse_connect.driver.binding import external_bind_re


cdef extern from "Python.h":
    int PyUnicode_READY(object o) except -1


cdef enum:
    QUOTED = 1
    BLOCK_COMMENT = 2
    LINE_COMMENT = 3
    GAVE_UP = 4
    PROBE_FACTOR = 16
    PROBE_SLACK = 1024

cdef uint64_t hash_seed = int.from_bytes(os.urandom(8), "little")


cdef inline bint word_char(Py_UCS4 char) noexcept:
    return char == 95 or 48 <= char <= 57 or 65 <= char <= 90 or 97 <= char <= 122


cdef inline uint64_t hash_step(uint64_t hash, Py_UCS4 char) noexcept:
    return (hash ^ <uint64_t>char) * <uint64_t>1099511628211


cdef inline uint64_t hash_finish(uint64_t hash) noexcept:
    hash = (hash ^ (hash >> 30)) * <uint64_t>0xbf58476d1ce4e5b9
    hash = (hash ^ (hash >> 27)) * <uint64_t>0x94d049bb133111eb
    return hash ^ (hash >> 31)


cdef struct HeredocEntry:
    Py_ssize_t start, length, last
    uint64_t hash


cdef struct Scanner:
    PyObject* sql
    void* data
    int kind
    Py_ssize_t size, index
    HeredocEntry* heredocs
    Py_ssize_t capacity, used, seen, probes, probe_factor, probe_slack, searched
    int shift
    bint heredocs_scanned


cdef int scanner_init(Scanner* s, str sql, Py_ssize_t probe_factor, Py_ssize_t probe_slack) except -1:
    PyUnicode_READY(sql)
    s.sql = <PyObject*>sql
    s.data = PyUnicode_DATA(sql)
    s.kind = PyUnicode_KIND(sql)
    s.size = PyUnicode_GET_LENGTH(sql)
    s.index = 0
    s.heredocs = NULL
    s.capacity = 0
    s.used = 0
    s.seen = 0
    s.probes = 0
    s.searched = 0
    s.probe_factor = probe_factor
    s.probe_slack = probe_slack
    s.shift = 64
    s.heredocs_scanned = False
    return 0


cdef inline Py_UCS4 char_at(Scanner* s, Py_ssize_t index) noexcept:
    return PyUnicode_READ(s.kind, s.data, index)


cdef inline bint over_budget(Scanner* s) noexcept:
    return s.probes > s.probe_factor * s.seen + s.probe_slack


cdef Py_ssize_t tag_end(Scanner* s, Py_ssize_t start, uint64_t* hash) noexcept:
    cdef Py_ssize_t i = start + 1
    cdef Py_UCS4 char
    cdef uint64_t value = <uint64_t>14695981039346656037 ^ hash_seed
    while i < s.size:
        char = char_at(s, i)
        if char == 36:
            hash[0] = hash_finish(value)
            return i + 1
        if not word_char(char):
            break
        value = hash_step(value, char)
        i += 1
    return -1


cdef Py_ssize_t tag_slot(Scanner* s, Py_ssize_t start, Py_ssize_t length, uint64_t hash) noexcept:
    cdef Py_ssize_t mask = s.capacity - 1
    cdef Py_ssize_t slot = <Py_ssize_t>(hash >> s.shift)
    cdef HeredocEntry* entry = &s.heredocs[slot]
    s.probes += 1
    while entry.start != 0 and not (
        entry.hash == hash and entry.length == length and
        memcmp(
            <char*>s.data + (entry.start - 1) * s.kind,
            <char*>s.data + start * s.kind,
            length * s.kind,
        ) == 0
    ):
        slot = (slot + 1) & mask
        entry = &s.heredocs[slot]
        s.probes += 1
    return slot


cdef int grow_tags(Scanner* s) except -1:
    cdef Py_ssize_t old_capacity = s.capacity, i, slot
    cdef Py_ssize_t capacity = old_capacity * 2 if old_capacity else 16
    cdef HeredocEntry* old = s.heredocs
    cdef HeredocEntry* table = <HeredocEntry*>PyMem_Calloc(capacity, sizeof(HeredocEntry))
    if table == NULL:
        raise MemoryError()
    s.heredocs = table
    s.capacity = capacity
    s.shift = 64
    while capacity > 1:
        capacity >>= 1
        s.shift -= 1
    for i in range(old_capacity):
        if old[i].start:
            slot = tag_slot(s, old[i].start - 1, old[i].length, old[i].hash)
            table[slot] = old[i]
    PyMem_Free(old)
    return 0


cdef int index_tags(Scanner* s) except -1:
    """Record the last start of each $tag$. Returns 1 when the probe budget runs out."""
    cdef Py_ssize_t i = 0, close, slot
    cdef uint64_t hash = 0
    s.heredocs_scanned = True
    grow_tags(s)
    i = find_char(s, 0, s.size, 36)
    while i != -1:
        close = tag_end(s, i, &hash)
        if close == -1:
            i = find_char(s, i + 1, s.size, 36)
            continue
        if (s.used + 1) * 2 >= s.capacity:
            grow_tags(s)
        s.seen += 1
        slot = tag_slot(s, i, close - i, hash)
        if over_budget(s):
            return 1
        if s.heredocs[slot].start == 0:
            s.heredocs[slot].start = i + 1
            s.heredocs[slot].length = close - i
            s.heredocs[slot].hash = hash
            s.used += 1
        s.heredocs[slot].last = i
        # The closing dollar can also start an overlapping tag.
        i = close - 1
    return 0


cdef inline Py_ssize_t find_char(Scanner* s, Py_ssize_t i, Py_ssize_t stop, Py_UCS4 char) noexcept:
    cdef char* found
    if s.kind == 1:
        found = <char*>memchr(<char*>s.data + i, char, stop - i)
        return -1 if found == NULL else found - <char*>s.data
    while i < stop:
        if char_at(s, i) == char:
            return i
        i += 1
    return -1


cdef Py_ssize_t close_tag(Scanner* s, Py_ssize_t start, Py_ssize_t end) noexcept:
    """Return the end of the first copy of the tag at start that begins at or after end, or -1."""
    cdef Py_ssize_t length = end - start
    # Find candidates by the first tag character, which is rarer than "$". An empty tag has only "$".
    cdef Py_ssize_t anchor = 1 if length > 2 else 0
    cdef Py_ssize_t i = end + anchor, stop = s.size - length + anchor + 1
    cdef Py_UCS4 char = char_at(s, start + anchor)
    while i < stop:
        i = find_char(s, i, stop, char)
        if i == -1:
            return -1
        if memcmp(
            <char*>s.data + (i - anchor) * s.kind,
            <char*>s.data + start * s.kind,
            length * s.kind,
        ) == 0:
            return i - anchor + length
        i += 1
    return -1


cdef int next_region(Scanner* s, Py_ssize_t* start, Py_ssize_t* end) except -1:
    """Find the next opaque region. Returns 0 at the end or at an unterminated token."""
    cdef Py_ssize_t i = s.index, begin, depth, close, slot, star, found
    cdef Py_UCS4 char, following, closer
    cdef uint64_t hash = 0
    cdef object match
    while i < s.size:
        begin = i
        char = char_at(s, i)
        if char == 39 or char == 34 or char == 96:
            closer = char
            i += 1
            while i < s.size:
                char = char_at(s, i)
                if char == 92:
                    i += 2
                elif char == closer:
                    if i + 1 < s.size and char_at(s, i + 1) == closer:
                        i += 2
                    else:
                        i += 1
                        break
                else:
                    i += 1
            else:
                s.index = s.size
                return 0
            start[0], end[0], s.index = begin, i, i
            return QUOTED

        if char == 0x2018 or char == 0x201c:
            closer = 0x2019 if char == 0x2018 else 0x201d
            i += 1
            while i < s.size and char_at(s, i) != closer:
                i += 1
            if i == s.size:
                s.index = s.size
                return 0
            i += 1
            start[0], end[0], s.index = begin, i, i
            return QUOTED

        following = char_at(s, i + 1) if i + 1 < s.size else 0
        if char == 47 and following == 42:
            depth = 1
            i += 2
            # Every "/*" and "*/" contains a "*", so jump between stars.
            while True:
                star = find_char(s, i, s.size, 42)
                if star == -1:
                    break
                if star > i and char_at(s, star - 1) == 47:
                    depth += 1
                    i = star + 1
                elif star + 1 < s.size and char_at(s, star + 1) == 47:
                    depth -= 1
                    i = star + 2
                    if depth == 0:
                        break
                else:
                    i = star + 1
            if depth:
                s.index = s.size
                return 0
            start[0], end[0], s.index = begin, i, i
            return BLOCK_COMMENT

        if ((char == 45 and following == 45) or
                (char == 47 and following == 47) or
                (char == 35 and (following == 32 or following == 33))):
            i += 2
            while i < s.size and char_at(s, i) != 10:
                i += 1
            if i < s.size:
                i += 1
            start[0], end[0], s.index = begin, i, i
            return LINE_COMMENT

        if char == 123 and (
            word_char(following) or following == 36 or
            (following > 127 and Py_UNICODE_ISALNUM(following))
        ):
            match = external_bind_re.match(<object>s.sql, i)
            if match is not None:
                i = match.end()
                continue

        if char == 36:
            close = tag_end(s, i, &hash)
            if close != -1:
                # Search for the closing tag directly until the searches have read about one string length.
                if not s.heredocs_scanned and s.searched > s.size and index_tags(s):
                    return GAVE_UP
                if s.heredocs_scanned:
                    slot = tag_slot(s, i, close - i, hash)
                    if over_budget(s):
                        return GAVE_UP
                    found = close_tag(s, i, close) if s.heredocs[slot].last >= close else -1
                else:
                    found = close_tag(s, i, close)
                    s.searched += (s.size if found == -1 else found) - close
                if found != -1:
                    start[0], end[0], s.index = begin, found, found
                    return QUOTED
            i += 1
            if i < s.size and word_char(char_at(s, i)):
                i += 1
                while i < s.size and (word_char(char_at(s, i)) or char_at(s, i) == 36):
                    i += 1
            continue

        if 48 <= char <= 57:
            i += 1
            while i < s.size and 48 <= char_at(s, i) <= 57:
                i += 1
            if i < s.size:
                char = char_at(s, i)
                if char == 95 or 65 <= char <= 90 or 97 <= char <= 122:
                    i += 1
                    while i < s.size and word_char(char_at(s, i)):
                        i += 1
            continue
        if char == 95 or 65 <= char <= 90 or 97 <= char <= 122:
            i += 1
            while i < s.size and (word_char(char_at(s, i)) or char_at(s, i) == 36):
                i += 1
            continue
        i += 1
    s.index = s.size
    return 0


def matches_outside_quotes(pattern, str sql not None, Py_ssize_t position):
    """Return whether the pattern matches outside opaque regions, checking from the match at position.

    Returns None when the heredoc probe budget runs out.
    """
    cdef Scanner s
    cdef Py_ssize_t start = 0, end = 0
    cdef int token
    search = pattern.search
    scanner_init(&s, sql, PROBE_FACTOR, PROBE_SLACK)
    try:
        token = next_region(&s, &start, &end)
        while token != GAVE_UP:
            if token and end <= position:
                token = next_region(&s, &start, &end)
                continue
            if token == 0 or position < start:
                return True
            match = search(sql, end)
            if match is None:
                return False
            position = match.start()
        return None
    finally:
        PyMem_Free(s.heredocs)


def remove_sql_comments(str sql not None):
    """Return sql without comments, or None when the heredoc probe budget runs out."""
    cdef Py_ssize_t last_marker = max(
        sql.rfind("--"), sql.rfind("/*"), sql.rfind("//"), sql.rfind("# "), sql.rfind("#!")
    )
    cdef Py_ssize_t start = 0, end = 0, copied = 0, written = 0, count
    cdef int token
    cdef char* output = NULL
    cdef Scanner s
    if last_marker == -1:
        return sql
    scanner_init(&s, sql, PROBE_FACTOR, PROBE_SLACK)
    try:
        while True:
            token = next_region(&s, &start, &end)
            if token == 0:
                break
            if token == GAVE_UP:
                return None
            if token == BLOCK_COMMENT or token == LINE_COMMENT:
                if output == NULL:
                    output = <char*>PyMem_Malloc(s.size * s.kind)
                    if output == NULL:
                        raise MemoryError()
                count = start - copied
                memcpy(output + written * s.kind, <char*>s.data + copied * s.kind, count * s.kind)
                written += count
                if token == BLOCK_COMMENT or char_at(&s, end - 1) == 10:
                    if s.kind == 1:
                        (<Py_UCS1*>output)[written] = 32 if token == BLOCK_COMMENT else 10
                    elif s.kind == 2:
                        (<Py_UCS2*>output)[written] = 32 if token == BLOCK_COMMENT else 10
                    else:
                        (<Py_UCS4*>output)[written] = 32 if token == BLOCK_COMMENT else 10
                    written += 1
                copied = end
            if end > last_marker:
                break
        if output == NULL:
            return sql
        count = s.size - copied
        memcpy(output + written * s.kind, <char*>s.data + copied * s.kind, count * s.kind)
        return PyUnicode_FromKindAndData(s.kind, output, written + count)
    finally:
        PyMem_Free(output)
        PyMem_Free(s.heredocs)


def _opaque_regions(str sql not None, Py_ssize_t probe_factor=PROBE_FACTOR, Py_ssize_t probe_slack=PROBE_SLACK):
    """Test helper: list the opaque regions, or None when the probe budget runs out."""
    cdef Scanner s
    cdef Py_ssize_t start = 0, end = 0
    cdef int token
    regions = []
    scanner_init(&s, sql, probe_factor, probe_slack)
    try:
        token = next_region(&s, &start, &end)
        while token and token != GAVE_UP:
            regions.append((start, end))
            token = next_region(&s, &start, &end)
        return None if token == GAVE_UP else regions
    finally:
        PyMem_Free(s.heredocs)
