# twom

A skiplist-based key-value store with MVCC (Multi-Version Concurrency Control) support.

twom provides a file-backed ordered key-value database with:

- Sorted key iteration with prefix matching
- Transactions with commit/abort semantics
- MVCC for serializable isolation
- Cursors for fine-grained iteration control
- Binary keys and values (arbitrary bytes including NUL)
- Checksummed records (xxhash64)
- Crash recovery
- Repack/compaction

## License

CC0 1.0 Universal — public domain. See [LICENSE](LICENSE).

## Dependencies

- C compiler (C99)
- libuuid (`-luuid`)
- POSIX (mmap, file locking)

## Building

```
make            # builds libtwom.a, twomtool, twomtest
make check      # runs the test suite
make clean      # removes build artifacts
```

## Library usage

```c
#include "twom.h"

struct twom_db *db = NULL;
struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
init.flags = TWOM_CREATE;

// open (creates if needed)
twom_db_open("/path/to/db", &init, &db, NULL);

// non-transactional store/fetch
twom_db_store(db, "key", 3, "value", 5, 0);

const char *val;
size_t vallen;
twom_db_fetch(db, "key", 3, NULL, NULL, &val, &vallen, 0);

// transactional
struct twom_txn *txn = NULL;
twom_db_begin_txn(db, 0, &txn);
twom_txn_store(txn, "key", 3, "newval", 6, 0);
twom_txn_commit(&txn);

// iterate
twom_db_foreach(db, "prefix", 6, NULL, my_callback, rock, 0);

// cleanup
twom_db_close(&db);
```

Delete a key by storing NULL with zero length:

```c
twom_db_store(db, "key", 3, NULL, 0, 0);
```

## twomtool

Command-line tool for inspecting and manipulating twom databases.

```
twomtool [options] <db file> <action> [<key>] [<value>]
```

### Options

| Flag | Description |
|------|-------------|
| `-n`, `--create` | Create the database if it doesn't exist |
| `-R`, `--readonly` | Open read-only |
| `-N`, `--no-checksum` | Disable checksums |
| `-S`, `--no-sync` | Don't fsync (dangerous) |
| `-T`, `--use-transaction` | Wrap action in a transaction |
| `-t`, `--no-transaction` | No transaction (default) |

### Actions

| Action | Description |
|--------|-------------|
| `get <key>` | Fetch and print value |
| `set <key> <value>` | Store key/value pair |
| `delete <key>` | Delete a key |
| `show [<prefix>]` | List entries (optionally filtered by prefix) |
| `dump [<level>]` | Internal format dump |
| `consistent` | Check database consistency |
| `repack` | Compact the database |
| `damage` | Write then crash (for recovery testing) |
| `batch` | Batch mode: read commands from stdin |

### Examples

```
twomtool -n /tmp/test.db set hello world
twomtool /tmp/test.db get hello
twomtool /tmp/test.db show
twomtool /tmp/test.db consistent
twomtool /tmp/test.db repack
```

### Batch mode

Commands are read line by line, tab-separated:

```
BEGIN
SET	key1	value1
SET	key2	value2
COMMIT
GET	key1
SHOW
DELETE	key1
```

## Origin

Extracted from [Cyrus IMAP](https://github.com/cyrusimap/cyrus-imapd) where it serves as the `twom` backend for `cyrusdb`.
