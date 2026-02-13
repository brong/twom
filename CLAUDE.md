# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

twom is a skiplist-based key-value store with MVCC support, extracted from Cyrus IMAP. It's a C library (libtwom) with a CLI tool (twomtool) and a comprehensive test suite.

## Build Commands

```bash
make                # build everything: libtwom.a, libtwom.so, twomtool, twomtest
make check          # run the test suite (alias: make test)
./twomtest          # run all tests directly
./twomtest mvcc     # run tests matching a substring filter
make clean          # remove build artifacts
```

Requires: C99 compiler, libuuid (`-luuid`), POSIX (mmap, fcntl locking).

## Source Layout

- `twom.h` — public API (31 functions, opaque types, error codes, flags)
- `twom.c` — full implementation (~3400 lines), organized in labeled sections
- `twomtool.c` — CLI tool for database inspection/manipulation
- `twomtest.c` — test suite (~3600 lines, 42 tests) with custom assertion macros
- `xxhash.h` — embedded xxHash implementation for record checksums

## Architecture

**Skiplist on disk:** The database is a file-backed skiplist with up to 31 levels (2^32 records). Records are mmap'd and accessed via pointer arithmetic macros (`TYPE()`, `KEYLEN()`, `VALLEN()`, `KEYPTR()`, `VALPTR()`, `NEXT0()`, `NEXTN()`).

**Record types:** ADD, FATADD (large), REPLACE, FATREPLACE (large), DELETE, COMMIT, DUMMY (sentinel). "Fat" variants use 64-bit lengths instead of 16-bit.

**MVCC:** Transactions get monotonic counters. Readers see a consistent snapshot; writes are invisible until COMMIT records are written. Recovery replays the log and rolls back uncommitted transactions.

**Locking:** fcntl-based file locking with separate header locks (for metadata/open) and data locks (for writes). Yield points release locks during long iterations.

**Naming conventions:**
- Public API: `twom_db_*`, `twom_txn_*`, `twom_cursor_*`
- Internal structs: `tm_*` prefix (`tm_header`, `tm_file`, `tm_loc`)
- Internal functions: `static` with snake_case
- Macros: UPPERCASE for constants and record field accessors

**Error handling:** All API functions return `enum twom_ret` (TWOM_OK=0, negatives for errors). Output via pointer parameters.

**Sections in twom.c** (marked by `/*** SECTION ***/` comments): TUNING, DATA STRUCTURES, POINTER MANAGEMENT, COMPARATORS, CHECKSUMS, MMAP MANAGEMENT, OBJECT CLEANUP, DATABASE HEADER, RECORD MANAGEMENT, LOCATION MANAGEMENT, DATABASE RECOVERY, FILE LOCKING, OPEN AND CLOSE, UTILITY FUNCTIONS, PUBLIC API.

## Testing

Tests use a custom harness with macros: `ASSERT()`, `ASSERT_EQ()`, `ASSERT_OK()`, `ASSERT_STR_EQ()`, `ASSERT_MEM_EQ()`. Callback variants prefixed with `CB_`. Helper macros like `CANSTORE()`, `CANFETCH()`, `CANDELETE()`, `CANCOMMIT()` wrap common multi-step operations.

Each test gets a fresh temp directory via `setup()`/`teardown()` (uses `TMPDIR` or `/tmp`).

## Versioning

Semver (MAJOR.MINOR.PATCH) defined in the Makefile. Shared library uses soname versioning (`libtwom.so.MAJOR`).
