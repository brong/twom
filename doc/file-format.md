# twom On-Disk File Format

All multi-byte integers are **little-endian**. All record bodies are
padded to **8-byte alignment**. Offsets in this document are byte
offsets from the start of the file.

## File header (96 bytes)

```
Offset  Size  Field
------  ----  -----
 0      16    Magic: \xA1\x02\x8B\x0Dtwomfile\x00\x00\x00\x00
16      16    UUID (binary, RFC 4122)
32       4    Version (uint32, currently 1)
36       4    Flags (uint32, bitmask)
40       8    Generation (uint64, incremented on repack)
48       8    Num records (uint64, live record count)
56       8    Num commits (uint64, total committed transactions)
64       8    Dirty size (uint64, bytes reclaimable by repack)
72       8    Repack size (uint64, file size at last repack)
80       8    Current size (uint64, logical end of committed data)
88       4    Max level (uint32, highest skip level in use)
92       4    Header checksum (uint32)
```

The header checksum covers bytes 0..91 using the file's checksum engine.

The DIRTY flag (bit 0 of Flags) is set before writing records and
cleared on commit. If set when the file is opened, recovery runs.

Persistent flags stored in the header include the checksum engine
selection (bits 27-29) and external comparator flag (bit 30).

## DUMMY record (offset 96)

The DUMMY is a sentinel record at the start of every file. It has no
key or value and always has level = MAXLEVEL (31). It serves as the
root of all skip pointers.

```
Offset  Size  Field
------  ----  -----
 +0      1    Type = 1 (DUMMY)
 +1      1    Level = 31
 +2      6    (padding, zero)
 +8      8    Next0[0] (uint64, level-0 forward pointer, slot 0)
+16      8    Next0[1] (uint64, level-0 forward pointer, slot 1)
+24    8*30   NextN[1..30] (uint64 each, higher-level forward pointers)
+264     4    Head checksum (uint32, covers bytes +0 to +263)
+268     4    Tail checksum (uint32, always zero -- DUMMY has no tail)
```

HEADLEN = `ptroffset[DUMMY] + 8*(1+Level)` = `8 + 8*32` = **264 bytes**.

DUMMY_SIZE = HEADLEN + 8 (checksums) = **272 bytes**. No tail data.

## Record layout

Every record starts with a fixed-size header, followed by the head
checksum, tail checksum, and then the key+value data (the "tail").

### Common header prefix

```
+0    1    Type (uint8): 1=DUMMY, 2=ADD, 3=FATADD, 4=REPLACE,
                         5=FATREPLACE, 6=DELETE, 7=COMMIT
+1    1    Level (uint8): skip level for this record (1..31)
```

### ADD (type 2) -- regular record insertion

```
+0    1    Type = 2
+1    1    Level
+2    2    Key length (uint16, little-endian)
+4    4    Value length (uint32, little-endian)
+8    8    Next0[0] (level-0 forward pointer, slot 0)
+16   8    Next0[1] (level-0 forward pointer, slot 1)
+24   8*L  NextN[1..L-1] (higher-level forward pointers, L = Level)
```

Head length = `8 + 8*(1+Level)`

```
+HL   4    Head checksum (uint32)
+HL+4 4    Tail checksum (uint32)
+HL+8 KL   Key bytes (not NUL-terminated in the length)
+HL+8+KL   1    NUL separator (0x00)
+HL+9+KL   VL   Value bytes
+HL+9+KL+VL 1   NUL separator (0x00)
... padded to 8-byte boundary
```

Tail length = `PAD8(KL + VL + 2)` (the +2 accounts for both NUL bytes).

**Total record size** = `24 + 8*Level + PAD8(KL + VL + 2)`

### FATADD (type 3) -- large record insertion

For keys > 65535 bytes or values > 4GB. Same structure as ADD but with
64-bit length fields:

```
+0    1    Type = 3
+1    1    Level
+2    6    (padding, zero)
+8    8    Key length (uint64)
+16   8    Value length (uint64)
+24   8    Next0[0]
+32   8    Next0[1]
+40   8*L  NextN[1..L-1]
```

Head length = `24 + 8*(1+Level)`

Tail layout is identical to ADD (key, NUL, value, NUL, padded).

**Total record size** = `40 + 8*Level + PAD8(KL + VL + 2)`

### REPLACE (type 4) -- regular record replacement

Same as ADD, but with an ancestor pointer for MVCC chaining:

```
+0    1    Type = 4
+1    1    Level
+2    2    Key length (uint16)
+4    4    Value length (uint32)
+8    8    Ancestor offset (uint64, points to previous ADD/REPLACE)
+16   8    Next0[0]
+24   8    Next0[1]
+32   8*L  NextN[1..L-1]
```

Head length = `16 + 8*(1+Level)`

Tail layout identical to ADD.

**Total record size** = `32 + 8*Level + PAD8(KL + VL + 2)`

### FATREPLACE (type 5) -- large record replacement

```
+0    1    Type = 5
+1    1    Level
+2    6    (padding, zero)
+8    8    Key length (uint64)
+16   8    Value length (uint64)
+24   8    Ancestor offset (uint64)
+32   8    Next0[0]
+40   8    Next0[1]
+48   8*L  NextN[1..L-1]
```

Head length = `32 + 8*(1+Level)`

**Total record size** = `48 + 8*Level + PAD8(KL + VL + 2)`

### DELETE (type 6) -- tombstone

DELETE records have no key or value data of their own. They point
to their ancestor (the ADD/REPLACE they supersede) via the ancestor
field, and the level-0 linked list threads through them.

```
+0    1    Type = 6
+1    1    Level (always 0 in practice; unused)
+2    6    (padding, zero)
+8    8    Ancestor offset (uint64, the record being deleted)
+16   4    Head checksum (uint32)
+20   4    (padding)
```

Head length = 16 (ptroffset=8, but DELETE has no skip pointers, only
the ancestor; the "level" is not used for forward pointers).

**Total record size** = **24 bytes** (fixed).

Note: DELETE records are inserted into the level-0 linked list *in
front of* the record they delete. The level-0 forward pointer of the
preceding record is updated to point to the DELETE, and the DELETE's
ancestor points to the original ADD/REPLACE. When traversing, the code
sees the DELETE first, follows the ancestor to find the key, then
treats the key as deleted.

### COMMIT (type 7) -- transaction boundary

```
+0    1    Type = 7
+1    1    Level (always 0; unused)
+2    6    (padding, zero)
+8    8    Start offset (uint64, the previous current_size)
+16   4    Head checksum (uint32)
+20   4    (padding)
```

Head length = 16.

**Total record size** = **24 bytes** (fixed).

COMMIT records are appended after all the records in a transaction.
They are not part of the skiplist linked list; they exist only as
markers in the sequential record stream for replay during repack.

## Record size summary

| Type       | Code | Pointer offset | Ancestor? | Has tail? | Fat? | Size formula                         |
|------------|------|----------------|-----------|-----------|------|--------------------------------------|
| DUMMY      | 1    | 8              | No        | No        | No   | 24 + 8*MAXLEVEL (fixed 272)          |
| ADD        | 2    | 8              | No        | Yes       | No   | 24 + 8*L + PAD8(KL+VL+2)            |
| FATADD     | 3    | 24             | No        | Yes       | Yes  | 40 + 8*L + PAD8(KL+VL+2)            |
| REPLACE    | 4    | 16             | Yes (+8)  | Yes       | No   | 32 + 8*L + PAD8(KL+VL+2)            |
| FATREPLACE | 5    | 32             | Yes (+24) | Yes       | Yes  | 48 + 8*L + PAD8(KL+VL+2)            |
| DELETE     | 6    | 8              | Yes (+8)  | No        | No   | 24 (fixed)                           |
| COMMIT     | 7    | 8              | No        | No        | No   | 24 (fixed)                           |

L = Level, KL = key length, VL = value length.
PAD8(n) = (n + 7) & ~7 (round up to next 8-byte boundary).

"Pointer offset" is the byte offset within the record where the
first level-0 forward pointer begins. The head length is always
`pointer_offset + 8*(1+Level)`.

## Checksum coverage

- **Head checksum**: covers bytes from the start of the record through
  the end of the forward pointers (i.e., `HEADLEN` bytes). Stored at
  offset `HEADLEN` within the record.

- **Tail checksum**: covers the key data, NUL separator, value data,
  NUL separator, and padding -- i.e., `PAD8(KL+VL+2)` bytes starting
  from the key. Stored at offset `HEADLEN+4` within the record. Only
  present for record types that have a tail (ADD, FATADD, REPLACE,
  FATREPLACE).

Both are uint32 values produced by the file's checksum engine (default
xxHash via XXH3_64bits, truncated to 32 bits).

## Padding

- All records begin on 8-byte aligned offsets (the header is 96 bytes
  = 8-aligned, DUMMY is 272 bytes = 8-aligned, and all tail lengths
  use PAD8).
- Keys and values are stored contiguously with a NUL byte after each.
  The combined key+NUL+value+NUL is padded to 8 bytes.
- Unused bytes within padding are zero.

## File growth

When a write needs more space than the current mmap, the file is
extended via `ftruncate` to `((current + current/4) + 16383) & ~16383`
-- i.e., 125% of the needed size, rounded up to a 16KB boundary. The
extra space is zero-filled by the filesystem.
