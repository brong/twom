# twom: Design and Purpose

## What is twom?

twom is a file-backed skiplist key-value store with MVCC (Multi-Version
Concurrency Control) support. It provides ordered key-value storage in a
single file using memory-mapped I/O, fcntl locking, and crash-safe
transactions.

twom was extracted from the Cyrus IMAP server, where it replaced the
earlier "twoskip" format. It is a standalone C library with no
dependencies beyond libuuid and POSIX.

For the author's perspective on the motivation and design, see:
https://www.fastmail.com/blog/introducing-twom/

## Why twom exists

twoskip, its predecessor in Cyrus IMAP, had two key limitations:

1. **Repack held an exclusive lock.** Repacking (compacting dead records)
   blocked all reads and writes for the duration. On large databases this
   could take tens of minutes.

2. **High syscall overhead.** twoskip used `write()` for mutations but
   `mmap` for reads, requiring many syscalls per operation. Each
   additional record in a transaction added ~6 syscalls.

twom solves both problems:

- **MVCC reads during repack.** Readers see a consistent snapshot while
  repack runs. Writers can also continue; their changes are replayed
  into the repacked file before the rename.

- **Minimal syscalls.** twom uses mmap for both reads and writes. A
  single-record transaction takes ~9 syscalls (vs ~19 for twoskip).
  Additional records in the same transaction add zero extra syscalls.

## Core design

### Skiplist on disk

The database file is a sorted linked list with skip pointers for
O(log n) search. Each record has a randomly-chosen level (1 to 31),
and forward pointers at each level link to the next record at that
level or higher. This gives binary-search performance over up to
2^32 records without any tree balancing.

### Dual level-0 pointers

The name "twom" (like "twoskip" before it) refers to the two forward
pointers at level 0. Every record has two level-0 slots. During a
write transaction, one slot points to the committed next record and
the other is used for the in-progress transaction's next record. On
recovery or abort, the uncommitted pointer is zeroed, and the
committed pointer remains valid. See [mvcc.md](mvcc.md) for details.

### Memory-mapped I/O

The entire file is mmap'd. Reads are direct pointer dereferences.
Writes mutate the mapping and msync flushes to disk. The file is
extended by 25% when more space is needed, reducing the frequency of
mmap/munmap cycles.

### Transactions

All mutations happen inside a write transaction. The sequence is:

1. Take an exclusive data lock.
2. Set the DIRTY flag in the header and msync.
3. Append records (ADD, REPLACE, DELETE) to the end of the file,
   updating forward pointers in existing records.
4. Append a COMMIT record.
5. msync all data.
6. Update the header (clear DIRTY, advance current_size) and msync.
7. Release the lock.

If the process crashes between steps 2 and 6, recovery detects the
DIRTY flag and replays the level-0 linked list, zeroing any forward
pointers that reference offsets beyond `current_size`.

### Locking

fcntl byte-range locks on two regions of the file:

- **Header lock** (bytes 0..15): taken briefly during open to serialize
  against concurrent opens. Released before the data lock to prevent
  writer starvation.
- **Data lock** (bytes 96..343, the DUMMY record region): held for the
  duration of a read or write transaction. Shared for readers,
  exclusive for writers.

The two-phase locking prevents a fairness problem in fcntl where
writers could starve readers (or vice versa). After acquiring the
data lock, the header lock is released so other processes can proceed.

### Checksums

Each record has a head checksum (covering the fixed-size header) and
optionally a tail checksum (covering the key and value data). The
default algorithm is xxHash (XXH3_64bits truncated to 32 bits). A
null checksum engine is available for testing, and an external
function pointer can be provided for custom algorithms.

### Repack

When dead records (replaced or deleted values, plus their tombstones)
accumulate, `twom_db_repack()` compacts the file:

1. Open an MVCC read transaction on the existing file.
2. Create a new file (`fname.NEW`).
3. Copy all live records into the new file via `foreach`.
4. Replay any commits that occurred during the copy.
5. Rename the new file over the old one.

This never holds an exclusive lock on the original file for more
than the brief rename operation.

### Record types

| Type       | Code | Purpose                            |
|------------|------|------------------------------------|
| DUMMY      | 1    | Sentinel at file start; has all 31 skip levels |
| ADD        | 2    | Insert a new key-value pair        |
| FATADD     | 3    | ADD with 64-bit length fields      |
| REPLACE    | 4    | Replace value for existing key     |
| FATREPLACE | 5    | REPLACE with 64-bit length fields  |
| DELETE     | 6    | Tombstone for a deleted key        |
| COMMIT     | 7    | Marks end of a committed transaction |

"Fat" variants support keys or values larger than 64KB (key) or 4GB
(value). See [file-format.md](file-format.md) for the exact byte layout.

### Error handling

All public API functions return `enum twom_ret`. Zero means success,
positive means "done iterating", negative means error. Output is
returned through pointer parameters.

## API surface

The public API (`twom.h`) provides 31 functions in three groups:

- **Database**: open, close, fetch, store, foreach, dump, consistency
  check, repack, yield, sync, and metadata accessors.
- **Transaction**: begin, abort, commit, yield, fetch, foreach, store.
- **Cursor**: begin (from db or txn), next, replace, commit, abort, fini.

Non-transactional `twom_db_*` convenience functions create an implicit
transaction for a single operation.
