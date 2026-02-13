# twom: MVCC and Record Lookup

## Overview

twom provides snapshot isolation for readers through two mechanisms:

1. **Dual level-0 pointers** allow a writer to build new linked-list
   structure without corrupting the view seen by concurrent readers.
2. **Ancestor chains** on REPLACE and DELETE records allow an MVCC
   reader to walk back to the version of a record that existed at
   the time its snapshot was taken.

Together these let readers see a consistent point-in-time view while
writers append new records to the file.

## Key concept: the transaction end

Every transaction has an `end` field -- a byte offset into the file.
For a write transaction, `end` advances as records are appended. For
a read transaction, `end` is set to `committed_size` when the
transaction begins and **does not change** if the transaction has the
MVCC flag set. (Non-MVCC readers refresh `end` each time they
re-acquire a lock after yielding.)

Any record at an offset >= `end` is invisible to that transaction.
This single value is the snapshot boundary.

## Dual level-0 pointers

Every record (except DELETE and COMMIT) has two forward pointer slots
at level 0: slot 0 and slot 1. The purpose is crash-safe
transactional updates to the linked list.

### Writing

When `_setloc0()` updates a record's level-0 forward pointer:

1. Read both slots: `val0 = slot[0]`, `val1 = slot[1]`.
2. Determine which slot to write:
   - If `val0 < committed_size` AND (`val1 >= committed_size` OR
     `val0 > val1`): write to slot 1.
   - Otherwise: write to slot 0.

The logic ensures that the slot pointing to committed data is
preserved, while the other slot gets the new (uncommitted) pointer.

### Reading (advance0)

When following level-0 pointers, `advance0()` picks the best of the
two slots:

```
next0 = slot[0]
next1 = slot[1]
if next0 >= end: return next1
if next1 >= end: return next0
if next0 > next1: return next0
return next1
```

A reader with `end = committed_size` will never follow a pointer into
uncommitted space, because any slot pointing past `end` is skipped.
The remaining slot always points to the correct committed successor.

A writer (whose `end` includes uncommitted data) will prefer the
higher-valued pointer, which is the most recently written one.

### Recovery

If a crash occurs mid-transaction, `current_size` in the header still
reflects the last committed state (the header is only updated after
data is synced). Recovery walks the level-0 list and zeroes any
pointer slot whose value >= `current_size`. The surviving slot in
each pair always points to the last committed successor.

## Ancestor chains (MVCC version lookup)

REPLACE and DELETE records contain an `ancestor` field -- a file
offset pointing to the previous version of the same key.

The chain for a key looks like:

```
[newest REPLACE or DELETE]
    -> ancestor: [previous REPLACE or DELETE]
        -> ancestor: [previous REPLACE or DELETE]
            -> ancestor: [original ADD]
                -> ancestor: 0 (end of chain)
```

DELETE records are special: they are inserted into the level-0 linked
list *ahead of* the record they delete. The DELETE's ancestor points
to the ADD/REPLACE that holds the key data.

### MVCC fetch

When `twom_txn_fetch()` finds a record via the skiplist:

1. Start at the current record (or its DELETE if one is chained in
   front of it).
2. If this record's offset >= `txn->end`, it was written after the
   snapshot. Follow the ancestor pointer.
3. Repeat until finding a record with offset < `txn->end`.
4. If that record is a DELETE, the key was deleted in this snapshot
   -- return NOTFOUND.
5. Otherwise, return the key and value from this record.

This is the same logic for both fetch and cursor iteration. The code
from `twom_txn_fetch`:

```c
size_t offset = loc->deleted_offset ? loc->deleted_offset : loc->offset;
const char *ptr = safeptr(loc, offset);
while (offset >= txn->end) {
    offset = ANCESTOR(ptr);
    if (!offset) return TWOM_NOTFOUND;  // didn't exist at snapshot time
    ptr = safeptr(loc, offset);
}
if (TYPE(ptr) == DELETE) return TWOM_NOTFOUND;
// ptr now points to the correct version
```

### Non-MVCC readers

If a transaction is not created with `TWOM_MVCC`, its `end` is
refreshed to the current `written_size` each time a lock is
re-acquired after a yield. This means it sees the latest committed
data, and ancestor chains are never traversed (the newest record is
always within the visible range).

## locate(): skiplist search

`locate()` finds a key (or the position where it would be inserted)
by walking the skiplist from the DUMMY record at the top level down
to level 0.

### Algorithm

```
offset = DUMMY_OFFSET
level = MAXLEVEL - 1

// Phase 1: walk higher levels (MAXLEVEL-1 down to 1)
while level > 0:
    next = NextN(current_record, level)
    if next is valid and next < end:
        compare next's key with search key
        if next < search key:
            advance to next (stay at this level)
            continue
    // next >= search key or no valid pointer: drop down
    backloc[level] = offset
    level--

// Phase 2: walk level 0
while offset:
    next = advance0(current_record, end)
    if next == 0: break  // end of list

    if next is a DELETE:
        record deleted_offset = next
        next = ancestor of DELETE

    compare next's key with search key
    if next > search key: break     // key not found, in gap
    if next == search key: found!   // exact match
    advance to next
```

The result is stored in a `tm_loc` structure:

- `offset`: the matched record (0 if no exact match)
- `deleted_offset`: if the match has a DELETE in front, its offset
- `backloc[0..MAXLEVEL]`: at each level, the last record before the
  search key. These are used when inserting or replacing to rewire
  the skip pointers.

### Optimization: futureoffset

During the higher-level walk, if the pointer at level N and level N-1
point to the same record, the key comparison is skipped (we already
know the result from the level above). This is tracked via the
`futureoffset` variable.

### Optimization: empty key

The empty string sorts before all keys. If `keylen == 0`, all
backloc entries are set to DUMMY_OFFSET immediately without any
comparisons. This is used to initialize a fresh location.

## find_loc(): optimized lookup with caching

`find_loc()` wraps `locate()` with short-circuit paths for common
access patterns. It uses a `tm_loc` that persists across calls,
avoiding a full skiplist traversal when possible.

### Fast paths

1. **File changed or location uninitialized**: if the location's file
   pointer or end offset doesn't match the current transaction, do a
   full `locate()`. This handles both initial setup and file
   replacement after repack.

2. **Same key**: if the location already points to the search key
   (exact match, verified by comparison), return immediately. Cost:
   one key comparison, zero pointer chasing.

3. **Next key**: if the search key sorts after the current position,
   check whether it's the very next record in the level-0 list. If
   so, update the location and return. Cost: one `advance0` + one or
   two key comparisons. This makes sequential access (foreach,
   sorted inserts) nearly free.

4. **Fallback**: if none of the above apply (key moved backwards or
   skipped ahead by more than one), do a full `locate()`.

### Usage pattern

The `find_loc` cache is why twom performs well for sequential
workloads. During `foreach` iteration, each call to `find_loc` hits
fast path 3. During sorted bulk inserts, the same path applies. Only
random access patterns fall through to the full O(log n) skiplist
search.

## advance_loc(): moving to the next record

`advance_loc()` moves a location to the next record in sort order.
Used by `foreach`, cursors, and `TWOM_FETCHNEXT`.

### Steps

1. If the location's file has changed (transaction refreshed after
   yield), re-locate the current key in the new file. This handles
   the case where another process repacked the database.

2. If the location points to an exact match, convert it to a "gap"
   position: update backloc entries to point here, then clear offset.
   This positions us just before the next record.

3. Call `advance0()` to get the next level-0 record.

4. If the next record is a DELETE, follow its ancestor to find the
   actual key (for MVCC version resolution later).

5. Return with `loc->offset` pointing to the next record.

## Locking and yields

Read transactions periodically yield their lock (every 1024 records
by default) so writers are not starved. When a non-MVCC reader
re-acquires the lock, it gets the latest `committed_size` and may see
new records. When an MVCC reader re-acquires, it re-locks the *same
file* (even if the database has been repacked to a new file) and
keeps its original `end`, maintaining its snapshot.

```c
// In cursor_next:
read_lock(db, &txn, txn->mvcc ? txn->file : NULL, TWOM_ONELOCK);
```

Passing `txn->file` forces the lock onto the original file. Passing
NULL lets it discover and switch to a new file if one exists.

## Putting it all together: a read during a write

1. Process A starts a write transaction. `committed_size = 1000`.
2. Process A appends an ADD record at offset 1000. Updates a
   predecessor's level-0 slot 1 to point to 1000. `written_size`
   advances to 1080.
3. Process B starts a read transaction. Takes a shared lock, reads
   the header: `current_size = 1000` (header not yet updated).
   Sets `txn->end = 1000`.
4. Process B searches for a key. `advance0` at the predecessor sees
   slot 1 = 1000 (>= end), so uses slot 0 which still points to the
   old committed chain. The new record is invisible.
5. Process A commits: syncs data, writes COMMIT record, updates
   header to `current_size = 1104`, clears DIRTY, syncs header.
6. Process B yields and re-acquires (non-MVCC). Re-reads header, now
   sees `current_size = 1104`. Sets `end = 1104`. Now the new record
   is visible.
7. If Process B were MVCC, step 6 would keep `end = 1000`, and the
   new record would remain invisible for the lifetime of the
   transaction.
