# twom API Reference

All functions return `int` (specifically `enum twom_ret`) unless noted.
Zero (`TWOM_OK`) means success. Negative values are errors. `TWOM_DONE`
(+1) signals end of iteration.

Keys and values are binary-safe byte strings with explicit lengths --
they are not assumed to be NUL-terminated.

Returned key/value pointers point directly into the mmap'd file. They
are valid until the next operation that could remap the file (any
store, commit, yield, or close). Copy them if you need to keep them.

## Error codes

```c
enum twom_ret {
    TWOM_OK          =  0,   // success
    TWOM_DONE        =  1,   // iteration complete (not an error)
    TWOM_EXISTS      = -1,   // key already exists (with TWOM_IFNOTEXIST)
    TWOM_IOERROR     = -2,   // I/O or mmap failure
    TWOM_INTERNAL    = -3,   // internal consistency error
    TWOM_LOCKED      = -4,   // lock contention (with TWOM_NONBLOCKING), or
                             //   tried to write a read-only db
    TWOM_NOTFOUND    = -5,   // key not found, or file doesn't exist
    TWOM_READONLY    = -6,   // write attempted on a shared (read-only) db
    TWOM_BADFORMAT   = -7,   // file is not a valid twom database
    TWOM_BADUSAGE    = -8,   // invalid arguments or missing callback
    TWOM_BADCHECKSUM = -9,   // record checksum mismatch
};
```

## Types

```c
struct twom_db;       // opaque database handle
struct twom_txn;      // opaque transaction handle
struct twom_cursor;   // opaque cursor handle

// callback for foreach: return 0 to continue, non-zero to stop
typedef int twom_cb(void *rock,
                    const char *key, size_t keylen,
                    const char *data, size_t datalen);

// custom comparison function (memcmp-like semantics)
typedef int twom_compar(const char *s1, size_t l1,
                        const char *s2, size_t l2);

// custom checksum function (returns 32-bit hash)
typedef uint32_t twom_csum(const char *s, size_t l);
```

## Flags

Flags are combined with bitwise OR. Not all flags apply to all
functions; irrelevant flags are silently ignored.

| Flag                 | Value  | Used in          | Effect |
|----------------------|--------|------------------|--------|
| `TWOM_CREATE`        | 1<<0   | open             | Create file if it doesn't exist |
| `TWOM_SHARED`        | 1<<1   | open, begin_txn  | Open read-only / start read transaction |
| `TWOM_NOCSUM`        | 1<<2   | open             | Skip checksum verification on reads |
| `TWOM_NOSYNC`        | 1<<3   | open, begin_txn  | Skip msync/fsync on writes |
| `TWOM_NONBLOCKING`   | 1<<4   | open, begin_txn  | Return TWOM_LOCKED instead of waiting |
| `TWOM_ONELOCK`       | 1<<5   | open, begin_txn  | Skip the header lock (for internal re-locking) |
| `TWOM_ALWAYSYIELD`   | 1<<9   | foreach, cursor  | Yield lock before every callback/next |
| `TWOM_NOYIELD`       | 1<<10  | open, begin_txn  | Never yield read locks automatically |
| `TWOM_IFNOTEXIST`    | 1<<11  | store            | Only store if key doesn't exist |
| `TWOM_IFEXIST`       | 1<<12  | store            | Only store if key already exists |
| `TWOM_FETCHNEXT`     | 1<<13  | fetch            | Return the record after the given key |
| `TWOM_SKIPROOT`      | 1<<14  | foreach, cursor  | Skip the first record if it matches the prefix exactly |
| `TWOM_MVCC`          | 1<<15  | begin_txn, cursor| See a frozen snapshot (serializable isolation) |
| `TWOM_CURSOR_PREFIX` | 1<<16  | cursor           | Restrict cursor to keys matching the prefix |
| `TWOM_CSUM_NULL`     | 1<<27  | open (create)    | Use null checksum (for testing) |
| `TWOM_CSUM_XXH64`    | 1<<28  | open (create)    | Use xxHash (default) |
| `TWOM_CSUM_EXTERNAL` | 1<<29  | open (create)    | Use caller-provided checksum function |
| `TWOM_COMPAR_EXTERNAL`| 1<<30 | open (create)    | Use caller-provided comparison function |

---

## Opening and closing

### twom_db_open

```c
int twom_db_open(const char *fname,
                 struct twom_open_data *setup,
                 struct twom_db **dbptr,
                 struct twom_txn **tidptr);
```

Open (or create) a database. If `tidptr` is non-NULL, a transaction
is started atomically with the open: a write transaction if
`TWOM_SHARED` is not set, otherwise a read transaction.

The same file opened twice in the same process returns the same
`twom_db` handle (reference counted), since fcntl locks are
per-process.

```c
struct twom_db *db = NULL;
struct twom_txn *txn = NULL;
struct twom_open_data setup = TWOM_OPEN_DATA_INITIALIZER;
setup.flags = TWOM_CREATE;

int r = twom_db_open("/tmp/mydb.twom", &setup, &db, &txn);
if (r) {
    fprintf(stderr, "open failed: %s\n", twom_strerror(r));
    return r;
}
// txn is a write transaction, ready for store/commit
```

Open read-only:

```c
struct twom_open_data setup = TWOM_OPEN_DATA_INITIALIZER;
setup.flags = TWOM_SHARED;
int r = twom_db_open("mydb.twom", &setup, &db, NULL);
```

### twom_db_close

```c
int twom_db_close(struct twom_db **dbptr);
```

Close the database. Decrements the reference count; the file is
actually closed and unmapped when the last reference is released.
Sets `*dbptr` to NULL.

```c
twom_db_close(&db);
// db is now NULL
```

---

## Non-transactional convenience functions

These create an implicit transaction for a single operation. Simple
to use, but each call acquires and releases a lock. For multiple
operations, use explicit transactions instead.

### twom_db_store

```c
int twom_db_store(struct twom_db *db,
                  const char *key, size_t keylen,
                  const char *val, size_t vallen,
                  int flags);
```

Store a key-value pair. Creates a write transaction, stores, commits,
and releases the lock. Pass `val = NULL, vallen = 0` to delete.

```c
r = twom_db_store(db, "user:1", 6, "Alice", 5, 0);

// insert only if new
r = twom_db_store(db, "user:1", 6, "Alice", 5, TWOM_IFNOTEXIST);
if (r == TWOM_EXISTS) { /* already there */ }

// delete
r = twom_db_store(db, "user:1", 6, NULL, 0, 0);
```

### twom_db_fetch

```c
int twom_db_fetch(struct twom_db *db,
                  const char *key, size_t keylen,
                  const char **keyp, size_t *keylenp,
                  const char **valp, size_t *vallenp,
                  int flags);
```

Look up a key. Returns pointers to the key and value in the mmap.
Any output pointer may be NULL if you don't need that value.

```c
const char *val;
size_t vallen;
r = twom_db_fetch(db, "user:1", 6, NULL, NULL, &val, &vallen, 0);
if (r == TWOM_NOTFOUND) { /* not in database */ }

// fetch the record AFTER "user:1" in sort order
const char *nextkey;
size_t nextkeylen;
r = twom_db_fetch(db, "user:1", 6,
                  &nextkey, &nextkeylen,
                  &val, &vallen, TWOM_FETCHNEXT);
```

### twom_db_foreach

```c
int twom_db_foreach(struct twom_db *db,
                    const char *prefix, size_t prefixlen,
                    twom_cb *goodp, twom_cb *cb, void *rock,
                    int flags);
```

Iterate over all records whose keys start with `prefix`. For each
record, if `goodp` is non-NULL it is called first as a filter --
the main `cb` is only called if `goodp` returns non-zero. `rock`
is passed through to both callbacks.

Pass `prefix = NULL, prefixlen = 0` to iterate all records.

Return non-zero from `cb` to stop iteration early; that value is
returned from `twom_db_foreach`.

```c
static int print_cb(void *rock, const char *key, size_t keylen,
                    const char *data, size_t datalen)
{
    printf("%.*s = %.*s\n", (int)keylen, key, (int)datalen, data);
    return 0; // continue
}

// all records
twom_db_foreach(db, NULL, 0, NULL, print_cb, NULL, 0);

// only keys starting with "user:"
twom_db_foreach(db, "user:", 5, NULL, print_cb, NULL, 0);
```

---

## Transactions

Transactions group multiple reads and writes into an atomic unit.
Write transactions hold an exclusive lock; read transactions hold a
shared lock.

### twom_db_begin_txn

```c
int twom_db_begin_txn(struct twom_db *db, int flags,
                      struct twom_txn **tidptr);
```

Start a new transaction. Pass `TWOM_SHARED` for a read-only
transaction, or 0 for a read-write transaction. Only one write
transaction per database is allowed at a time.

Can also be called on an existing transaction pointer to re-acquire
a released lock (after yield).

```c
// write transaction
struct twom_txn *txn = NULL;
r = twom_db_begin_txn(db, 0, &txn);

// read-only transaction
struct twom_txn *rtxn = NULL;
r = twom_db_begin_txn(db, TWOM_SHARED, &rtxn);

// MVCC snapshot (reads see a frozen point in time)
struct twom_txn *mvcc_txn = NULL;
r = twom_db_begin_txn(db, TWOM_SHARED | TWOM_MVCC, &mvcc_txn);
```

### twom_txn_commit

```c
int twom_txn_commit(struct twom_txn **txnp);
```

Commit and close a transaction. For write transactions, this appends
a COMMIT record, syncs, updates the header, and releases the lock.
For read transactions, this just releases the lock. Sets `*txnp`
to NULL.

```c
r = twom_txn_store(txn, "a", 1, "1", 1, 0);
r = twom_txn_store(txn, "b", 1, "2", 1, 0);
r = twom_txn_commit(&txn);
// txn is now NULL; both stores are atomically visible
```

### twom_txn_abort

```c
int twom_txn_abort(struct twom_txn **txnp);
```

Abort and close a transaction. For write transactions, runs recovery
to undo any uncommitted pointer changes. Sets `*txnp` to NULL.

```c
r = twom_txn_store(txn, "x", 1, "oops", 4, 0);
r = twom_txn_abort(&txn);
// the store of "x" was rolled back
```

### twom_txn_store

```c
int twom_txn_store(struct twom_txn *txn,
                   const char *key, size_t keylen,
                   const char *val, size_t vallen,
                   int flags);
```

Store a key-value pair within a write transaction. Pass `val = NULL`
to delete. The change is visible within this transaction immediately
but not to other processes until commit.

```c
struct twom_txn *txn = NULL;
r = twom_db_begin_txn(db, 0, &txn);

r = twom_txn_store(txn, "key1", 4, "val1", 4, 0);
r = twom_txn_store(txn, "key2", 4, "val2", 4, 0);

// conditional: fail if key already exists
r = twom_txn_store(txn, "key1", 4, "dup", 3, TWOM_IFNOTEXIST);
// r == TWOM_EXISTS

// delete
r = twom_txn_store(txn, "key1", 4, NULL, 0, 0);

r = twom_txn_commit(&txn);
```

### twom_txn_fetch

```c
int twom_txn_fetch(struct twom_txn *txn,
                   const char *key, size_t keylen,
                   const char **keyp, size_t *keylenp,
                   const char **valp, size_t *vallenp,
                   int flags);
```

Look up a key within a transaction. In an MVCC transaction, this
walks the ancestor chain to find the version visible at snapshot
time.

```c
const char *val;
size_t vallen;
r = twom_txn_fetch(txn, "key1", 4, NULL, NULL, &val, &vallen, 0);

// fetch next key after "key1"
const char *key;
size_t keylen;
r = twom_txn_fetch(txn, "key1", 4, &key, &keylen,
                   &val, &vallen, TWOM_FETCHNEXT);
```

### twom_txn_foreach

```c
int twom_txn_foreach(struct twom_txn *txn,
                     const char *prefix, size_t prefixlen,
                     twom_cb *goodp, twom_cb *cb, void *rock,
                     int flags);
```

Iterate records within a transaction. Same semantics as
`twom_db_foreach` but uses the given transaction's view (including
uncommitted writes in a write transaction, or the MVCC snapshot in
an MVCC read transaction).

```c
struct twom_txn *txn = NULL;
r = twom_db_begin_txn(db, 0, &txn);
r = twom_txn_store(txn, "c", 1, "3", 1, 0);

// this iteration sees the uncommitted "c"
r = twom_txn_foreach(txn, NULL, 0, NULL, print_cb, NULL, 0);

r = twom_txn_commit(&txn);
```

### twom_txn_yield

```c
int twom_txn_yield(struct twom_txn *txn);
```

Release the read lock on a read-only transaction so writers can
proceed. Returns `TWOM_LOCKED` on write transactions. The lock is
automatically re-acquired on the next operation. For MVCC
transactions, re-locking targets the same file to preserve the
snapshot.

```c
struct twom_txn *txn = NULL;
r = twom_db_begin_txn(db, TWOM_SHARED, &txn);

r = twom_txn_fetch(txn, "key", 3, NULL, NULL, &val, &vallen, 0);

// release the lock while doing other work
r = twom_txn_yield(txn);
do_something_slow();

// next fetch re-acquires the lock automatically
r = twom_txn_fetch(txn, "key2", 4, NULL, NULL, &val, &vallen, 0);

r = twom_txn_abort(&txn);
```

---

## Cursors

Cursors provide step-by-step iteration with the ability to replace
values at the current position. There are two ways to create them:
standalone (from a db, managing their own transaction) or within
an existing transaction.

### twom_db_begin_cursor

```c
int twom_db_begin_cursor(struct twom_db *db,
                         const char *key, size_t keylen,
                         struct twom_cursor **curp, int flags);
```

Create a cursor positioned at `key` (or the start of the database if
`key = NULL`). This creates an internal transaction; use
`twom_cursor_commit` or `twom_cursor_abort` to end both the cursor
and its transaction.

```c
struct twom_cursor *cur = NULL;
const char *key, *val;
size_t keylen, vallen;

// iterate all records
r = twom_db_begin_cursor(db, NULL, 0, &cur, 0);
while ((r = twom_cursor_next(cur, &key, &keylen, &val, &vallen)) == 0) {
    printf("%.*s = %.*s\n", (int)keylen, key, (int)vallen, val);
}
// r == TWOM_DONE here
twom_cursor_abort(&cur);
```

Prefix iteration (only keys starting with "c"):

```c
r = twom_db_begin_cursor(db, "c", 1, &cur, TWOM_CURSOR_PREFIX);
while ((r = twom_cursor_next(cur, &key, &keylen, &val, &vallen)) == 0) {
    // key starts with "c"
}
twom_cursor_abort(&cur);
```

Start after a specific key:

```c
r = twom_db_begin_cursor(db, "cherry", 6, &cur, TWOM_SKIPROOT);
// first cursor_next returns the key AFTER "cherry"
```

### twom_txn_begin_cursor

```c
int twom_txn_begin_cursor(struct twom_txn *txn,
                          const char *key, size_t keylen,
                          struct twom_cursor **curp, int flags);
```

Create a cursor within an existing transaction. The cursor sees the
transaction's view (including uncommitted writes). Use
`twom_cursor_fini` to close the cursor without affecting the
transaction.

```c
struct twom_txn *txn = NULL;
r = twom_db_begin_txn(db, 0, &txn);
r = twom_txn_store(txn, "one", 3, "1", 1, 0);
r = twom_txn_store(txn, "two", 3, "2", 1, 0);

// cursor sees uncommitted records
struct twom_cursor *cur = NULL;
r = twom_txn_begin_cursor(txn, NULL, 0, &cur, 0);
int count = 0;
while (twom_cursor_next(cur, &key, &keylen, &val, &vallen) == 0)
    count++;
// count includes "one" and "two"

twom_cursor_fini(&cur); // closes cursor, txn still alive
r = twom_txn_commit(&txn);
```

### twom_cursor_next

```c
int twom_cursor_next(struct twom_cursor *cur,
                     const char **keyp, size_t *keylenp,
                     const char **valp, size_t *vallenp);
```

Advance the cursor to the next record. Returns `TWOM_DONE` when
there are no more records (or no more records within the prefix).

```c
while ((r = twom_cursor_next(cur, &key, &keylen, &val, &vallen)) == 0) {
    // process key/val
}
if (r != TWOM_DONE) {
    // actual error
}
```

### twom_cursor_replace

```c
int twom_cursor_replace(struct twom_cursor *cur,
                        const char *val, size_t vallen,
                        int flags);
```

Replace the value at the cursor's current position. The cursor must
be from a write transaction. Pass `val = NULL` to delete.

```c
r = twom_db_begin_cursor(db, NULL, 0, &cur, 0);
while ((r = twom_cursor_next(cur, &key, &keylen, &val, &vallen)) == 0) {
    if (keylen == 4 && memcmp(key, "beta", 4) == 0) {
        r = twom_cursor_replace(cur, "updated", 7, 0);
    }
}
r = twom_cursor_commit(&cur);
```

### twom_cursor_commit

```c
int twom_cursor_commit(struct twom_cursor **curp);
```

Commit the cursor's transaction and free the cursor. For cursors
created with `twom_db_begin_cursor`. Sets `*curp` to NULL.

```c
r = twom_cursor_commit(&cur);
// all changes made via cursor_replace are now committed
```

### twom_cursor_abort

```c
int twom_cursor_abort(struct twom_cursor **curp);
```

Abort the cursor's transaction and free the cursor. For cursors
created with `twom_db_begin_cursor`. Sets `*curp` to NULL.

```c
r = twom_cursor_abort(&cur);
// any cursor_replace changes are rolled back
```

### twom_cursor_fini

```c
void twom_cursor_fini(struct twom_cursor **curp);
```

Free a cursor without affecting its transaction. For cursors created
with `twom_txn_begin_cursor`. Sets `*curp` to NULL. The transaction
must be committed or aborted separately.

```c
twom_cursor_fini(&cur);
// txn is still alive
r = twom_txn_commit(&txn);
```

---

## Utility functions

### twom_db_dump

```c
int twom_db_dump(struct twom_db *db, int detail);
```

Print the database structure to stdout. `detail > 2` includes
record values. Useful for debugging.

```c
twom_db_dump(db, 1);  // offsets, types, keys
twom_db_dump(db, 3);  // also print values
```

### twom_db_check_consistency

```c
int twom_db_check_consistency(struct twom_db *db);
```

Verify that all skip pointers are correct, keys are in order,
ancestor chains are valid, and record/dirty counts match. Returns
`TWOM_OK` if consistent.

```c
r = twom_db_check_consistency(db);
if (r) {
    fprintf(stderr, "database is corrupt: %s\n", twom_strerror(r));
}
```

### twom_db_repack

```c
int twom_db_repack(struct twom_db *db);
```

Compact the database by copying live records into a new file and
renaming it over the original. Removes dead space from replaced
and deleted records. Uses MVCC internally so readers are not blocked.

Returns `TWOM_LOCKED` if another process is already repacking.

```c
if (twom_db_should_repack(db)) {
    r = twom_db_repack(db);
    if (r == TWOM_LOCKED) { /* another process is repacking */ }
}
```

### twom_db_should_repack

```c
bool twom_db_should_repack(struct twom_db *db);
```

Returns true if dead space exceeds the MINREWRITE threshold and makes
up more than 25% of the file. A hint, not a requirement.

```c
if (twom_db_should_repack(db)) {
    twom_db_repack(db);
}
```

### twom_db_yield

```c
int twom_db_yield(struct twom_db *db);
```

Release all read locks on the database so writers can proceed.
Returns `TWOM_LOCKED` if a write transaction is active or noyield
is set. Locks are re-acquired on the next operation.

```c
// between two unrelated reads, let writers in
twom_db_yield(db);
```

### twom_db_sync

```c
int twom_db_sync(struct twom_db *db);
```

Force an msync of the entire mapped file. Normally not needed since
commits sync automatically unless `TWOM_NOSYNC` is set.

```c
twom_db_sync(db);
```

---

## Metadata accessors

### twom_db_generation

```c
size_t twom_db_generation(struct twom_db *db);
```

Return the generation number. Starts at 1 for a new database,
incremented on each repack.

### twom_db_num_records

```c
size_t twom_db_num_records(struct twom_db *db);
```

Return the number of live (non-deleted) records.

### twom_db_size

```c
size_t twom_db_size(struct twom_db *db);
```

Return `current_size` -- the logical end of committed data in the
file.

### twom_db_fname

```c
const char *twom_db_fname(struct twom_db *db);
```

Return the filename the database was opened with.

### twom_db_uuid

```c
const char *twom_db_uuid(struct twom_db *db);
```

Return the database UUID as a 36-character string
(`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`). The UUID is stable across
repacks and unique per database.

### twom_strerror

```c
const char *twom_strerror(int r);
```

Return a human-readable string for a `twom_ret` error code.

```c
int r = twom_db_open("missing.twom", &setup, &db, NULL);
if (r) printf("error: %s\n", twom_strerror(r));
// prints: "error: Not Found"
```

---

## Setup structure

```c
struct twom_open_data {
    uint32_t flags;
    twom_compar *compar;   // custom key comparison (or NULL)
    twom_csum *csum;       // custom checksum function (or NULL)
    void (*error)(const char *msg, const char *fmt, ...);
};

#define TWOM_OPEN_DATA_INITIALIZER { 0, NULL, NULL, NULL }
```

Always initialize with `TWOM_OPEN_DATA_INITIALIZER` to zero all
fields, then set the ones you need.

Custom comparator example:

```c
static int case_insensitive(const char *a, size_t al,
                            const char *b, size_t bl)
{
    size_t min = al < bl ? al : bl;
    for (size_t i = 0; i < min; i++) {
        int d = tolower((unsigned char)a[i])
              - tolower((unsigned char)b[i]);
        if (d) return d;
    }
    return (al > bl) - (bl > al);
}

struct twom_open_data setup = TWOM_OPEN_DATA_INITIALIZER;
setup.flags = TWOM_CREATE | TWOM_COMPAR_EXTERNAL;
setup.compar = case_insensitive;
twom_db_open("mydb.twom", &setup, &db, NULL);
```

Custom error handler:

```c
static void my_error(const char *msg, const char *fmt, ...)
{
    va_list ap;
    fprintf(stderr, "TWOM ERROR: %s ", msg);
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fprintf(stderr, "\n");
}

struct twom_open_data setup = TWOM_OPEN_DATA_INITIALIZER;
setup.error = my_error;
```

---

## Complete example

```c
#include <stdio.h>
#include <string.h>
#include "twom.h"

static int print_record(void *rock, const char *key, size_t keylen,
                        const char *data, size_t datalen)
{
    (void)rock;
    printf("  %.*s => %.*s\n", (int)keylen, key, (int)datalen, data);
    return 0;
}

int main(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    struct twom_open_data setup = TWOM_OPEN_DATA_INITIALIZER;
    setup.flags = TWOM_CREATE;
    int r;

    r = twom_db_open("/tmp/example.twom", &setup, &db, &txn);
    if (r) return 1;

    /* batch insert */
    twom_txn_store(txn, "alice", 5, "engineer", 8, 0);
    twom_txn_store(txn, "bob", 3, "designer", 8, 0);
    twom_txn_store(txn, "carol", 5, "manager", 7, 0);
    twom_txn_commit(&txn);

    /* point lookup */
    const char *val;
    size_t vallen;
    r = twom_db_fetch(db, "bob", 3, NULL, NULL, &val, &vallen, 0);
    if (r == 0)
        printf("bob => %.*s\n", (int)vallen, val);

    /* iterate all records */
    printf("all records:\n");
    twom_db_foreach(db, NULL, 0, NULL, print_record, NULL, 0);

    /* replace a value */
    twom_db_store(db, "bob", 3, "lead designer", 13, 0);

    /* delete a record */
    twom_db_store(db, "carol", 5, NULL, 0, 0);

    /* repack if worthwhile */
    if (twom_db_should_repack(db))
        twom_db_repack(db);

    printf("records: %zu, generation: %zu\n",
           twom_db_num_records(db), twom_db_generation(db));

    twom_db_close(&db);
    return 0;
}
```
