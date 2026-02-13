/* twomtest.c -- standalone test suite for twom database
 *
 * Based on Cyrus IMAP's cunit/aaa-db.testc
 *
 * https://creativecommons.org/publicdomain/zero/1.0/
 */

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "twom.h"

/*
 * ============================================================
 * Test framework
 * ============================================================
 */

static int total_tests = 0;
static int total_passed = 0;
static int total_failed = 0;
static int total_skipped = 0;
static int current_test_failed = 0;

/* assertion macros for test functions (return void) */
#define ASSERT(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "    FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        current_test_failed = 1; \
        return; \
    } \
} while (0)

#define ASSERT_EQ(a, b) do { \
    long long _a = (long long)(a); \
    long long _b = (long long)(b); \
    if (_a != _b) { \
        fprintf(stderr, "    FAIL %s:%d: %s == %lld, expected %s == %lld\n", \
                __FILE__, __LINE__, #a, _a, #b, _b); \
        current_test_failed = 1; \
        return; \
    } \
} while (0)

#define ASSERT_OK(r) ASSERT_EQ(r, TWOM_OK)
#define ASSERT_NULL(p) ASSERT((p) == NULL)
#define ASSERT_NOT_NULL(p) ASSERT((p) != NULL)
#define ASSERT_STR_EQ(a, b) ASSERT(strcmp(a, b) == 0)
#define ASSERT_MEM_EQ(a, b, len) ASSERT(memcmp(a, b, len) == 0)

/* assertion macros for callback functions (return int) */
static int cb_failures = 0;

#define CB_ASSERT(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "    FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        cb_failures++; \
    } \
} while (0)

#define CB_ASSERT_EQ(a, b) do { \
    long long _a = (long long)(a); \
    long long _b = (long long)(b); \
    if (_a != _b) { \
        fprintf(stderr, "    FAIL %s:%d: %s == %lld, expected %s == %lld\n", \
                __FILE__, __LINE__, #a, _a, #b, _b); \
        cb_failures++; \
    } \
} while (0)

#define CB_ASSERT_OK(r) CB_ASSERT_EQ(r, TWOM_OK)
#define CB_ASSERT_STR_EQ(a, b) CB_ASSERT(strcmp(a, b) == 0)

#define SKIP(msg) do { \
    fprintf(stderr, "  SKIP: %s\n", msg); \
    total_skipped++; \
    return; \
} while (0)

/*
 * ============================================================
 * Simple hash table (for test_many)
 * ============================================================
 */

struct ht_entry {
    char *key;
    void *value;
    struct ht_entry *next;
};

struct hash_table {
    struct ht_entry **buckets;
    size_t capacity;
    size_t count;
};

static void ht_init(struct hash_table *ht, size_t cap)
{
    ht->capacity = cap;
    ht->count = 0;
    ht->buckets = calloc(cap, sizeof(struct ht_entry *));
    assert(ht->buckets);
}

static unsigned ht_hash(const char *key, size_t cap)
{
    unsigned h = 5381;
    while (*key) h = h * 33 + (unsigned char)*key++;
    return h % cap;
}

static void ht_insert(struct hash_table *ht, const char *key, void *value)
{
    unsigned idx = ht_hash(key, ht->capacity);
    /* check for existing key */
    for (struct ht_entry *e = ht->buckets[idx]; e; e = e->next) {
        if (strcmp(e->key, key) == 0) {
            e->value = value;
            return;
        }
    }
    struct ht_entry *e = calloc(1, sizeof(*e));
    assert(e);
    e->key = strdup(key);
    assert(e->key);
    e->value = value;
    e->next = ht->buckets[idx];
    ht->buckets[idx] = e;
    ht->count++;
}

static void *ht_lookup(struct hash_table *ht, const char *key)
{
    unsigned idx = ht_hash(key, ht->capacity);
    for (struct ht_entry *e = ht->buckets[idx]; e; e = e->next) {
        if (strcmp(e->key, key) == 0) return e->value;
    }
    return NULL;
}

static void ht_del(struct hash_table *ht, const char *key)
{
    unsigned idx = ht_hash(key, ht->capacity);
    struct ht_entry **pp = &ht->buckets[idx];
    while (*pp) {
        if (strcmp((*pp)->key, key) == 0) {
            struct ht_entry *e = *pp;
            *pp = e->next;
            free(e->key);
            /* value freed by caller */
            free(e);
            ht->count--;
            return;
        }
        pp = &(*pp)->next;
    }
}

static void ht_free(struct hash_table *ht, void (*free_fn)(void *))
{
    for (size_t i = 0; i < ht->capacity; i++) {
        struct ht_entry *e = ht->buckets[i];
        while (e) {
            struct ht_entry *next = e->next;
            free(e->key);
            if (free_fn) free_fn(e->value);
            free(e);
            e = next;
        }
    }
    free(ht->buckets);
    ht->buckets = NULL;
    ht->count = 0;
}

/*
 * ============================================================
 * Multi-process synchronization helpers
 * ============================================================
 */

static void signal_peer(int fd)
{
    char c = 'X';
    ssize_t n = write(fd, &c, 1);
    assert(n == 1);
}

static void wait_for_peer(int fd)
{
    char c;
    ssize_t n = read(fd, &c, 1);
    assert(n == 1);
}

/*
 * ============================================================
 * Test infrastructure
 * ============================================================
 */

static char *basedir = NULL;
static char *filename = NULL;
static char *filename2 = NULL;

static int fexists(const char *fname)
{
    struct stat sb;
    int r = stat(fname, &sb);
    if (r < 0) r = -errno;
    return r;
}

static int setup(void)
{
    char path[PATH_MAX];
    const char *tmpdir = getenv("TMPDIR");
    if (!tmpdir) tmpdir = "/tmp";

    snprintf(path, sizeof(path), "%s/twom-test.%d", tmpdir, getpid());
    if (mkdir(path, 0700) && errno != EEXIST) {
        perror(path);
        return -1;
    }

    basedir = strdup(path);
    assert(basedir);

    snprintf(path, sizeof(path), "%s/stuff", basedir);
    if (mkdir(path, 0700) && errno != EEXIST) {
        perror(path);
        return -1;
    }

    filename = malloc(PATH_MAX);
    assert(filename);
    snprintf(filename, PATH_MAX, "%s/stuff/test.twom", basedir);

    filename2 = malloc(PATH_MAX);
    assert(filename2);
    snprintf(filename2, PATH_MAX, "%s/stuff/testB.twom", basedir);

    return 0;
}

static int teardown(void)
{
    if (basedir) {
        char cmd[PATH_MAX + 20];
        snprintf(cmd, sizeof(cmd), "rm -rf '%s'", basedir);
        int r = system(cmd);
        (void)r;
    }

    free(basedir); basedir = NULL;
    free(filename); filename = NULL;
    free(filename2); filename2 = NULL;

    return 0;
}

/*
 * ============================================================
 * Database test macros
 * ============================================================
 */

#define BADDATA ((const char *)0xdeadbeef)
#define BADLEN  ((size_t)0xcafebabe)

/* auto-begin a write transaction if needed, then store */
#define CANSTORE(k, kl, d, dl) do { \
    if (!txn) { \
        r = twom_db_begin_txn(db, 0, &txn); \
        ASSERT_OK(r); \
        ASSERT_NOT_NULL(txn); \
    } \
    r = twom_txn_store(txn, k, kl, d, dl, 0); \
    ASSERT_OK(r); \
} while (0)

/* auto-begin a write transaction if needed, then delete (force=1, no TWOM_IFEXIST) */
#define CANDELETE(k, kl) do { \
    if (!txn) { \
        r = twom_db_begin_txn(db, 0, &txn); \
        ASSERT_OK(r); \
        ASSERT_NOT_NULL(txn); \
    } \
    r = twom_txn_store(txn, k, kl, NULL, 0, 0); \
    ASSERT_OK(r); \
} while (0)

#define ISCONSISTENT() do { \
    r = twom_db_check_consistency(db); \
    ASSERT_OK(r); \
} while (0)

#define CANFETCH(k, kl, ed, edl) do { \
    const char *_data = BADDATA; \
    size_t _datalen = BADLEN; \
    if (!txn) { \
        r = twom_db_begin_txn(db, 0, &txn); \
        ASSERT_OK(r); \
        ASSERT_NOT_NULL(txn); \
    } \
    r = twom_txn_fetch(txn, k, kl, NULL, NULL, &_data, &_datalen, 0); \
    ASSERT_OK(r); \
    ASSERT_NOT_NULL(_data); \
    ASSERT(_data != BADDATA); \
    ASSERT(_data != (ed)); \
    ASSERT(_datalen != BADLEN); \
    ASSERT_EQ(_datalen, edl); \
    ASSERT(memcmp(_data, ed, _datalen) == 0); \
} while (0)

#define CANFETCH_NOTXN(k, kl, ed, edl) do { \
    const char *_data = BADDATA; \
    size_t _datalen = BADLEN; \
    r = twom_db_fetch(db, k, kl, NULL, NULL, &_data, &_datalen, 0); \
    ASSERT_OK(r); \
    ASSERT_NOT_NULL(_data); \
    ASSERT(_data != BADDATA); \
    ASSERT(_data != (ed)); \
    ASSERT(_datalen != BADLEN); \
    ASSERT_EQ(_datalen, edl); \
    ASSERT(memcmp(_data, ed, _datalen) == 0); \
} while (0)

#define CANNOTFETCH(k, kl, experr) do { \
    const char *_data = BADDATA; \
    size_t _datalen = BADLEN; \
    if (!txn) { \
        r = twom_db_begin_txn(db, 0, &txn); \
        ASSERT_OK(r); \
        ASSERT_NOT_NULL(txn); \
    } \
    r = twom_txn_fetch(txn, k, kl, NULL, NULL, &_data, &_datalen, 0); \
    ASSERT_EQ(r, experr); \
    ASSERT_NULL(_data); \
    ASSERT_EQ(_datalen, 0); \
} while (0)

#define CANNOTFETCH_NOTXN(k, kl, experr) do { \
    const char *_data = BADDATA; \
    size_t _datalen = BADLEN; \
    r = twom_db_fetch(db, k, kl, NULL, NULL, &_data, &_datalen, 0); \
    ASSERT_EQ(r, experr); \
    ASSERT_NULL(_data); \
    ASSERT_EQ(_datalen, 0); \
} while (0)

#define CANFETCHNEXT(k, kl, ek, ekl, ed, edl) do { \
    const char *_data = BADDATA; \
    size_t _datalen = BADLEN; \
    const char *_key = BADDATA; \
    size_t _keylen = BADLEN; \
    if (!txn) { \
        r = twom_db_begin_txn(db, 0, &txn); \
        ASSERT_OK(r); \
    } \
    r = twom_txn_fetch(txn, k, kl, &_key, &_keylen, &_data, &_datalen, TWOM_FETCHNEXT); \
    ASSERT_OK(r); \
    ASSERT_NOT_NULL(_data); \
    ASSERT_NOT_NULL(_key); \
    ASSERT(_data != BADDATA); \
    ASSERT(_key != BADDATA); \
    ASSERT(_datalen != BADLEN); \
    ASSERT(_keylen != BADLEN); \
    ASSERT_EQ(_datalen, edl); \
    ASSERT(memcmp(_data, ed, _datalen) == 0); \
    ASSERT_EQ(_keylen, ekl); \
    ASSERT(memcmp(_key, ek, _keylen) == 0); \
} while (0)

#define CANNOTFETCHNEXT(k, kl, experr) do { \
    if (!txn) { \
        r = twom_db_begin_txn(db, 0, &txn); \
        ASSERT_OK(r); \
    } \
    r = twom_txn_fetch(txn, k, kl, NULL, NULL, NULL, NULL, TWOM_FETCHNEXT); \
    ASSERT_EQ(r, experr); \
} while (0)

#define CANCOMMIT() do { \
    r = twom_txn_commit(&txn); \
    ASSERT_OK(r); \
    txn = NULL; \
} while (0)

#define CANREOPEN() do { \
    r = twom_db_close(&db); \
    db = NULL; \
    ASSERT_OK(r); \
    { \
        struct twom_open_data _init = TWOM_OPEN_DATA_INITIALIZER; \
        r = twom_db_open(filename, &_init, &db, NULL); \
    } \
    ASSERT_OK(r); \
    ASSERT_NOT_NULL(db); \
} while (0)

/*
 * ============================================================
 * Shared data structures and callbacks
 * ============================================================
 */

struct binary_result {
    struct binary_result *next;
    char *key;
    size_t keylen;
    char *data;
    size_t datalen;
};

#define GOTRESULT(ek, ekl, ed, edl) do { \
    struct binary_result *_actual = results; \
    ASSERT_NOT_NULL(results); \
    results = results->next; \
    ASSERT_EQ(_actual->keylen, ekl); \
    ASSERT(memcmp(_actual->key, ek, _actual->keylen) == 0); \
    ASSERT_EQ(_actual->datalen, edl); \
    ASSERT(memcmp(_actual->data, ed, _actual->datalen) == 0); \
    free(_actual->key); \
    free(_actual->data); \
    free(_actual); \
} while (0)

static int foreacher(void *rock,
                     const char *key, size_t keylen,
                     const char *data, size_t datalen)
{
    struct binary_result **head = (struct binary_result **)rock;
    struct binary_result **tail;
    struct binary_result *res;

    CB_ASSERT(key != NULL);
    CB_ASSERT(keylen > 0);
    CB_ASSERT(data != NULL);
    CB_ASSERT(datalen > 0);

    res = calloc(1, sizeof(*res));
    assert(res);
    res->key = malloc(keylen);
    assert(res->key);
    memcpy(res->key, key, keylen);
    res->keylen = keylen;
    res->data = malloc(datalen);
    assert(res->data);
    memcpy(res->data, data, datalen);
    res->datalen = datalen;

    /* append to the list */
    for (tail = head; *tail; tail = &(*tail)->next)
        ;
    *tail = res;

    return 0;
}

struct deleteit {
    struct twom_db *db;
    struct binary_result **results;
};

static int deleter(void *rock,
                   const char *key, size_t keylen,
                   const char *data, size_t datalen)
{
    struct deleteit *dd = rock;
    int r;

    r = foreacher(dd->results, key, keylen, data, datalen);
    if (r) return r;

    /* force=0 (TWOM_IFEXIST): non-transactional delete */
    r = twom_db_store(dd->db, key, keylen, NULL, 0, TWOM_IFEXIST);
    if (r) return r;

    return 0;
}

/*
 * ============================================================
 * Test 1: test_openclose
 * ============================================================
 */

static void test_openclose(void)
{
    struct twom_db *db = NULL;
    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    int r;

    ASSERT_EQ(fexists(filename), -ENOENT);

    /* open() without TWOM_CREATE fails with NOTFOUND */
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT(r == TWOM_NOTFOUND || r == TWOM_IOERROR);
    ASSERT_NULL(db);
    ASSERT_EQ(fexists(filename), -ENOENT);

    /* open() with TWOM_CREATE succeeds */
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);
    ASSERT_EQ(fexists(filename), 0);

    /* closing succeeds */
    r = twom_db_close(&db);
    ASSERT_OK(r);
    ASSERT_EQ(fexists(filename), 0);
}

/*
 * ============================================================
 * Test 3: test_multiopen
 * ============================================================
 */

static void test_multiopen(void)
{
    struct twom_db *db1 = NULL;
    struct twom_txn *txn1 = NULL;
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;
    static const char KEY1[] = "mustache";
    static const char DATA1[] = "blog lomo";
    static const char KEY2[] = "cred";
    static const char DATA2[] = "beard ethical";
    static const char KEY3[] = "leggings";
    static const char DATA3[] = "tumblr salvia";

    ASSERT_EQ(fexists(filename), -ENOENT);

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    /* 1st txn */
    CANSTORE(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    CANCOMMIT();

    /* save db1, open second reference */
    db1 = db;
    txn1 = txn;
    db = NULL;
    txn = NULL;

    init.flags = 0;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    /* 2nd txn on second reference */
    CANSTORE(KEY2, strlen(KEY2), DATA2, strlen(DATA2));
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);

    /* restore first reference */
    db = db1;
    txn = txn1;

    /* 3rd txn on first reference */
    CANSTORE(KEY3, strlen(KEY3), DATA3, strlen(DATA3));
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);

    /* re-open and verify all records */
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    CANFETCH(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    CANFETCH(KEY2, strlen(KEY2), DATA2, strlen(DATA2));
    CANFETCH(KEY3, strlen(KEY3), DATA3, strlen(DATA3));
    CANCOMMIT();

    /* out of TXN works too */
    CANFETCH_NOTXN(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    CANFETCH_NOTXN(KEY2, strlen(KEY2), DATA2, strlen(DATA2));
    CANFETCH_NOTXN(KEY3, strlen(KEY3), DATA3, strlen(DATA3));

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Test 4: test_read_and_delete
 * ============================================================
 */

static void test_read_and_delete(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;
    static const char KEY1[] = "mustache";
    static const char DATA1[] = "blog lomo";
    static const char KEY2[] = "cred";
    static const char DATA2[] = "beard ethical";
    static const char KEY3[] = "leggings";
    static const char DATA3[] = "tumblr salvia";
    static const char KEY3CHILD[] = "leggings.biodiesel";
    static const char KEY4[] = "occupy";
    static const char DATA4[] = "etsy tote bag";

    ASSERT_EQ(fexists(filename), -ENOENT);

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    /* 1st txn */
    CANSTORE(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    CANSTORE(KEY2, strlen(KEY2), DATA2, strlen(DATA2));
    CANSTORE(KEY3, strlen(KEY3), DATA3, strlen(DATA3));
    CANSTORE(KEY4, strlen(KEY4), DATA4, strlen(DATA4));
    CANCOMMIT();
    ISCONSISTENT();

    /* 2nd txn */
    CANNOTFETCH(KEY3CHILD, strlen(KEY3CHILD), TWOM_NOTFOUND);
    CANFETCH(KEY3, strlen(KEY3), DATA3, strlen(DATA3));
    CANDELETE(KEY3, strlen(KEY3));
    CANFETCH(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    CANDELETE(KEY1, strlen(KEY1));
    CANCOMMIT();
    ISCONSISTENT();

    /* what is left? */
    CANNOTFETCH_NOTXN(KEY1, strlen(KEY1), TWOM_NOTFOUND);
    CANFETCH_NOTXN(KEY2, strlen(KEY2), DATA2, strlen(DATA2));
    CANNOTFETCH_NOTXN(KEY3, strlen(KEY3), TWOM_NOTFOUND);
    CANFETCH_NOTXN(KEY4, strlen(KEY4), DATA4, strlen(DATA4));
    ISCONSISTENT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Test 5: test_replace_before_delete
 * ============================================================
 */

static void test_replace_before_delete(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;
    static const char KEY1[] = "alphabet";
    static const char DATA1[] = "blog lomo";
    static const char KEY2[] = "blanket";
    static const char DATA2[] = "beard ethical";
    static const char KEY3[] = "cobra";
    static const char DATA3[] = "prius toke";
    static const char KEY4[] = "dynamo";
    static const char DATA4[] = "etsy tote bag";

    ASSERT_EQ(fexists(filename), -ENOENT);

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    /* 1st txn */
    CANSTORE(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    CANSTORE(KEY3, strlen(KEY3), DATA3, strlen(DATA3));
    CANSTORE(KEY4, strlen(KEY4), DATA4, strlen(DATA4));
    CANCOMMIT();
    ISCONSISTENT();

    /* 2nd txn: delete KEY3 */
    CANDELETE(KEY3, strlen(KEY3));
    CANCOMMIT();
    ISCONSISTENT();

    /* 3rd txn: add KEY2 */
    CANSTORE(KEY2, strlen(KEY2), DATA3, strlen(DATA3));
    CANCOMMIT();
    ISCONSISTENT();

    /* 4th txn: replace KEY2 */
    CANSTORE(KEY2, strlen(KEY2), DATA2, strlen(DATA2));
    CANCOMMIT();
    ISCONSISTENT();

    CANFETCH_NOTXN(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    CANFETCH_NOTXN(KEY2, strlen(KEY2), DATA2, strlen(DATA2));
    CANNOTFETCH_NOTXN(KEY3, strlen(KEY3), TWOM_NOTFOUND);
    CANFETCH_NOTXN(KEY4, strlen(KEY4), DATA4, strlen(DATA4));

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Test 6: test_opentwo
 * ============================================================
 */

static void test_opentwo(void)
{
    struct twom_db *db1 = NULL;
    struct twom_db *db2 = NULL;
    int r;

    ASSERT_EQ(fexists(filename), -ENOENT);
    ASSERT_EQ(fexists(filename2), -ENOENT);

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;

    r = twom_db_open(filename, &init, &db1, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db1);
    ASSERT_EQ(fexists(filename), 0);
    ASSERT_EQ(fexists(filename2), -ENOENT);

    r = twom_db_open(filename2, &init, &db2, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db2);
    ASSERT_EQ(fexists(filename), 0);
    ASSERT_EQ(fexists(filename2), 0);
    ASSERT(db1 != db2);

    r = twom_db_close(&db1);
    ASSERT_OK(r);

    r = twom_db_close(&db2);
    ASSERT_OK(r);

    ASSERT_EQ(fexists(filename), 0);
    ASSERT_EQ(fexists(filename2), 0);
}

/*
 * ============================================================
 * Test 7: test_readwrite
 * ============================================================
 */

static void test_readwrite(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    static const char KEY[] = "skeleton";
    static const char DATA[] = "dem bones dem bones dem thighbones";
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    CANNOTFETCH(KEY, strlen(KEY), TWOM_NOTFOUND);
    CANSTORE(KEY, strlen(KEY), DATA, strlen(DATA));
    CANFETCH(KEY, strlen(KEY), DATA, strlen(DATA));
    CANCOMMIT();

    CANFETCH(KEY, strlen(KEY), DATA, strlen(DATA));
    CANCOMMIT();

    CANREOPEN();

    CANFETCH(KEY, strlen(KEY), DATA, strlen(DATA));
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Test 8: test_multirw
 * ============================================================
 */

static void test_multirw(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    static const char KEY[] = "skeleton";
    static const char DATA1[] = "dem bones";
    static const char DATA2[] = "Dem KneeBones";
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    CANNOTFETCH(KEY, strlen(KEY), TWOM_NOTFOUND);
    CANSTORE(KEY, strlen(KEY), DATA1, strlen(DATA1));
    CANFETCH(KEY, strlen(KEY), DATA1, strlen(DATA1));
    CANSTORE(KEY, strlen(KEY), DATA2, strlen(DATA2));
    CANFETCH(KEY, strlen(KEY), DATA2, strlen(DATA2));
    CANCOMMIT();

    CANFETCH(KEY, strlen(KEY), DATA2, strlen(DATA2));
    CANCOMMIT();

    CANREOPEN();

    CANFETCH(KEY, strlen(KEY), DATA2, strlen(DATA2));
    CANCOMMIT();

    CANFETCH_NOTXN(KEY, strlen(KEY), DATA2, strlen(DATA2));

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Test 9: test_readwrite_zerolen
 * ============================================================
 */

static void test_readwrite_zerolen(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    static const char KEY[] = "keffiyeh";
    static const char DATA[] = "";
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    CANNOTFETCH(KEY, strlen(KEY), TWOM_NOTFOUND);
    CANSTORE(KEY, strlen(KEY), DATA, 0);
    CANFETCH(KEY, strlen(KEY), DATA, 0);
    CANCOMMIT();

    CANFETCH(KEY, strlen(KEY), DATA, 0);
    CANCOMMIT();

    CANREOPEN();

    CANFETCH(KEY, strlen(KEY), DATA, 0);
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Test 10: test_readwrite_null
 * ============================================================
 */

static void test_readwrite_null(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    static const char KEY[] = "skateboard";
    static const char EMPTY[] = "";
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    CANNOTFETCH(KEY, strlen(KEY), TWOM_NOTFOUND);

    /* store NULL data — twom treats it as empty string (matching cyrusdb_twom wrapper) */
    if (!txn) {
        r = twom_db_begin_txn(db, 0, &txn);
        ASSERT_OK(r);
    }
    r = twom_txn_store(txn, KEY, strlen(KEY), "", 0, 0);
    ASSERT_OK(r);

    CANFETCH(KEY, strlen(KEY), EMPTY, 0);
    CANCOMMIT();

    CANFETCH(KEY, strlen(KEY), EMPTY, 0);
    CANCOMMIT();

    CANREOPEN();

    CANFETCH(KEY, strlen(KEY), EMPTY, 0);
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Test 11: test_abort
 * ============================================================
 */

static void test_abort(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    static const char KEY[] = "yale";
    static const char DATA[] = "stanford mit harvard";
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    CANNOTFETCH(KEY, strlen(KEY), TWOM_NOTFOUND);
    CANSTORE(KEY, strlen(KEY), DATA, strlen(DATA));
    CANFETCH(KEY, strlen(KEY), DATA, strlen(DATA));

    /* abort */
    r = twom_txn_abort(&txn);
    ASSERT_OK(r);
    txn = NULL;

    /* data is not present after abort */
    CANNOTFETCH(KEY, strlen(KEY), TWOM_NOTFOUND);
    CANCOMMIT();

    CANREOPEN();

    CANNOTFETCH(KEY, strlen(KEY), TWOM_NOTFOUND);
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Test 12: test_delete
 * ============================================================
 */

static void test_delete(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    static const char KEY1[] = "buzzes";
    static const char DATA1[] = "afro timur funky cents hewitt";
    static const char KEY2[] = "galas";
    static const char DATA2[] = "assad goering flemish brynner heshvan";
    static const char KEY3[] = "bathes";
    static const char DATA3[] = "flax corm naipaul enable herrera fating";
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    CANSTORE(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    CANSTORE(KEY2, strlen(KEY2), DATA2, strlen(DATA2));
    CANSTORE(KEY3, strlen(KEY3), DATA3, strlen(DATA3));
    CANCOMMIT();

    CANFETCH(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    CANFETCH(KEY2, strlen(KEY2), DATA2, strlen(DATA2));
    CANFETCH(KEY3, strlen(KEY3), DATA3, strlen(DATA3));

    /* delete KEY2 (force=1: no TWOM_IFEXIST) */
    r = twom_txn_store(txn, KEY2, strlen(KEY2), NULL, 0, 0);
    ASSERT_OK(r);

    CANNOTFETCH(KEY2, strlen(KEY2), TWOM_NOTFOUND);
    CANFETCH(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    CANFETCH(KEY3, strlen(KEY3), DATA3, strlen(DATA3));
    CANCOMMIT();

    CANNOTFETCH(KEY2, strlen(KEY2), TWOM_NOTFOUND);
    CANFETCH(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    CANFETCH(KEY3, strlen(KEY3), DATA3, strlen(DATA3));
    CANCOMMIT();

    CANREOPEN();

    CANNOTFETCH(KEY2, strlen(KEY2), TWOM_NOTFOUND);
    CANFETCH(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    CANFETCH(KEY3, strlen(KEY3), DATA3, strlen(DATA3));
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Test 13: test_mboxlist
 * ============================================================
 */

static void test_mboxlist(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    struct binary_result *results = NULL;
    static const char KEY1[] = "INBOX.a";
    static const char DATA1[] = "delays maj bullish packard ronald";
    static const char KEY2[] = "INBOX.a b";
    static const char DATA2[] = "bobby tswana cu albumin created";
    static const char KEY3[] = "INBOX.a.b";
    static const char DATA3[] = "aleut stoic muscovy adonis moe docent";
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    CANSTORE(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    CANSTORE(KEY2, strlen(KEY2), DATA2, strlen(DATA2));
    CANSTORE(KEY3, strlen(KEY3), DATA3, strlen(DATA3));
    CANCOMMIT();

    CANFETCH(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    CANFETCH(KEY2, strlen(KEY2), DATA2, strlen(DATA2));
    CANFETCH(KEY3, strlen(KEY3), DATA3, strlen(DATA3));
    CANCOMMIT();

    /* foreach in txn */
    r = twom_db_begin_txn(db, 0, &txn);
    ASSERT_OK(r);
    r = twom_txn_foreach(txn, NULL, 0, NULL, foreacher, &results, 0);
    ASSERT_OK(r);

    /* twom sorts in byte order: INBOX.a, INBOX.a b, INBOX.a.b
     * (space=0x20, dot=0x2e => "INBOX.a " < "INBOX.a.") */
    GOTRESULT(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    GOTRESULT(KEY2, strlen(KEY2), DATA2, strlen(DATA2));
    GOTRESULT(KEY3, strlen(KEY3), DATA3, strlen(DATA3));
    ASSERT_NULL(results);

    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Test 14: test_foreach_nullkey
 * ============================================================
 */

static void test_foreach_nullkey(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    struct binary_result *results = NULL;
    int i;
    static const char *KEY[] = {
        "a\0a", "a\0b", "a\0c", "abc"
    };
    static const char *DATA[] = {
        "delays maj bullish packard ronald",
        "bobby tswana cu albumin created",
        "aleut stoic muscovy adonis moe docent",
        "."
    };
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    /* store records (all keys are 3 bytes) */
    for (i = 0; i < 4; i++)
        CANSTORE(KEY[i], 3, DATA[i], strlen(DATA[i]));
    CANCOMMIT();

    /* all records can be fetched back */
    for (i = 0; i < 4; i++)
        CANFETCH(KEY[i], 3, DATA[i], strlen(DATA[i]));

    /* foreach in txn */
    r = twom_txn_foreach(txn, NULL, 0, NULL, foreacher, &results, 0);
    ASSERT_OK(r);
    for (i = 0; i < 4; i++) {
        GOTRESULT(KEY[i], 3, DATA[i], strlen(DATA[i]));
    }
    ASSERT_NULL(results);
    CANCOMMIT();

    /* foreach no txn, prefix "a\0" len=2 */
    r = twom_db_foreach(db, "a\0", 2, NULL, foreacher, &results, 0);
    ASSERT_OK(r);
    for (i = 0; i < 3; i++) {
        GOTRESULT(KEY[i], 3, DATA[i], strlen(DATA[i]));
    }
    ASSERT_NULL(results);

    /* foreach no txn, prefix "a\0" but len=1 (just "a") */
    r = twom_db_foreach(db, "a\0", 1, NULL, foreacher, &results, 0);
    ASSERT_OK(r);
    for (i = 0; i < 4; i++) {
        GOTRESULT(KEY[i], 3, DATA[i], strlen(DATA[i]));
    }
    ASSERT_NULL(results);

    /* foreach no txn, prefix NULL */
    r = twom_db_foreach(db, NULL, 0, NULL, foreacher, &results, 0);
    ASSERT_OK(r);
    for (i = 0; i < 4; i++) {
        GOTRESULT(KEY[i], 3, DATA[i], strlen(DATA[i]));
    }
    ASSERT_NULL(results);

    /* delete KEY[1] */
    r = twom_db_store(db, KEY[1], 3, NULL, 0, 0);
    ASSERT_OK(r);

    /* foreach no txn, prefix "a\0" len=2 */
    r = twom_db_foreach(db, "a\0", 2, NULL, foreacher, &results, 0);
    ASSERT_OK(r);
    GOTRESULT(KEY[0], 3, DATA[0], strlen(DATA[0]));
    GOTRESULT(KEY[2], 3, DATA[2], strlen(DATA[2]));
    ASSERT_NULL(results);

    /* foreach no txn, prefix "a" len=1 */
    r = twom_db_foreach(db, "a", 1, NULL, foreacher, &results, 0);
    ASSERT_OK(r);
    GOTRESULT(KEY[0], 3, DATA[0], strlen(DATA[0]));
    GOTRESULT(KEY[2], 3, DATA[2], strlen(DATA[2]));
    GOTRESULT(KEY[3], 3, DATA[3], strlen(DATA[3]));
    ASSERT_NULL(results);

    /* foreach no txn, prefix "" len=0 */
    r = twom_db_foreach(db, "", 0, NULL, foreacher, &results, 0);
    ASSERT_OK(r);
    GOTRESULT(KEY[0], 3, DATA[0], strlen(DATA[0]));
    GOTRESULT(KEY[2], 3, DATA[2], strlen(DATA[2]));
    GOTRESULT(KEY[3], 3, DATA[3], strlen(DATA[3]));
    ASSERT_NULL(results);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Test 15: test_foreach
 * ============================================================
 */

static void test_foreach(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    struct binary_result *results = NULL;
    struct deleteit deldata;
    int i;
    static const char *KEY[] = {
        "carib", "cubist", "eulogy", "dressing",
        "inside", "resident", "conflict", "progress"
    };
    static const char *DATA[] = {
        "delays maj bullish packard ronald",
        "bobby tswana cu albumin created",
        "aleut stoic muscovy adonis moe docent",
        ".",
        "0",
        "The mysterious diary records the voice.",
        "the\nquick\tbrown fox",
        "Lets all be unique together",
    };
    /* sorted order: carib(0), conflict(6), cubist(1), dressing(3),
     * eulogy(2), inside(4), progress(7), resident(5) */
    int order[] = { 0, 6, 1, 3, 2, 4, 7, 5 };
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    for (i = 0; i < 8; i++)
        CANSTORE(KEY[i], strlen(KEY[i]), DATA[i], strlen(DATA[i]));
    CANCOMMIT();

    for (i = 0; i < 8; i++)
        CANFETCH(KEY[i], strlen(KEY[i]), DATA[i], strlen(DATA[i]));
    CANCOMMIT();

    /* foreach in txn */
    r = twom_db_begin_txn(db, 0, &txn);
    ASSERT_OK(r);
    r = twom_txn_foreach(txn, NULL, 0, NULL, foreacher, &results, 0);
    ASSERT_OK(r);

    for (i = 0; i < 8; i++) {
        int n = order[i];
        if (n < 0) continue;
        GOTRESULT(KEY[n], strlen(KEY[n]), DATA[n], strlen(DATA[n]));
    }
    ASSERT_NULL(results);

    /* fetchnext iteration */
    {
        const char *prev = NULL;
        size_t prevlen = 0;
        for (i = 0; i < 8; i++) {
            int n = order[i];
            if (n < 0) continue;
            const char *key = KEY[n];
            const char *data = DATA[n];
            size_t keylen = strlen(key);
            size_t datalen = strlen(data);
            CANFETCHNEXT(prev, prevlen, key, keylen, data, datalen);
            prev = key;
            prevlen = keylen;
        }
        CANNOTFETCHNEXT(prev, prevlen, TWOM_NOTFOUND);
    }

    CANCOMMIT();

    /* foreach without txn */
    r = twom_db_foreach(db, NULL, 0, NULL, foreacher, &results, TWOM_ALWAYSYIELD);
    ASSERT_OK(r);
    for (i = 0; i < 8; i++) {
        int n = order[i];
        if (n < 0) continue;
        GOTRESULT(KEY[n], strlen(KEY[n]), DATA[n], strlen(DATA[n]));
    }
    ASSERT_NULL(results);

    /* foreach with prefix "c" */
    r = twom_db_foreach(db, "c", 1, NULL, foreacher, &results, TWOM_ALWAYSYIELD);
    ASSERT_OK(r);
    for (i = 0; i < 3; i++) {
        int n = order[i];
        if (n < 0) continue;
        GOTRESULT(KEY[n], strlen(KEY[n]), DATA[n], strlen(DATA[n]));
    }
    ASSERT_NULL(results);

    /* foreach with non-matching prefix " " */
    r = twom_db_foreach(db, " ", 1, NULL, foreacher, &results, TWOM_ALWAYSYIELD);
    ASSERT_OK(r);
    ASSERT_NULL(results);

    /* foreach with non-matching prefix "z" */
    r = twom_db_foreach(db, "z", 1, NULL, foreacher, &results, TWOM_ALWAYSYIELD);
    ASSERT_OK(r);
    ASSERT_NULL(results);

    /* foreach with prefix "e" - just eulogy */
    r = twom_db_foreach(db, "e", 1, NULL, foreacher, &results, TWOM_ALWAYSYIELD);
    ASSERT_OK(r);
    GOTRESULT(KEY[2], strlen(KEY[2]), DATA[2], strlen(DATA[2]));
    ASSERT_NULL(results);

    /* delete order[7]=resident and order[1]=conflict */
    {
        int n = order[7]; /* resident */
        r = twom_db_store(db, KEY[n], strlen(KEY[n]), NULL, 0, TWOM_IFEXIST);
        ASSERT_OK(r);
        n = order[1]; /* conflict */
        r = twom_db_store(db, KEY[n], strlen(KEY[n]), NULL, 0, TWOM_IFEXIST);
        ASSERT_OK(r);
        order[7] = -1;
        order[1] = -1;
    }

    /* foreach only finds active records */
    r = twom_db_foreach(db, NULL, 0, NULL, foreacher, &results, TWOM_ALWAYSYIELD);
    ASSERT_OK(r);
    for (i = 0; i < 8; i++) {
        int n = order[i];
        if (n < 0) continue;
        GOTRESULT(KEY[n], strlen(KEY[n]), DATA[n], strlen(DATA[n]));
    }
    ASSERT_NULL(results);

    /* foreach with prefix "c" after deletes */
    r = twom_db_foreach(db, "c", 1, NULL, foreacher, &results, TWOM_ALWAYSYIELD);
    ASSERT_OK(r);
    for (i = 0; i < 3; i++) {
        int n = order[i];
        if (n < 0) continue;
        GOTRESULT(KEY[n], strlen(KEY[n]), DATA[n], strlen(DATA[n]));
    }
    ASSERT_NULL(results);

    /* foreach only deleted record prefix "r" */
    r = twom_db_foreach(db, "r", 1, NULL, foreacher, &results, TWOM_ALWAYSYIELD);
    ASSERT_OK(r);
    ASSERT_NULL(results);

    /* fetchnext after deletes */
    {
        const char *prev = NULL;
        size_t prevlen = 0;
        r = twom_db_begin_txn(db, 0, &txn);
        ASSERT_OK(r);
        for (i = 0; i < 8; i++) {
            int n = order[i];
            if (n < 0) continue;
            const char *key = KEY[n];
            const char *data = DATA[n];
            size_t keylen = strlen(key);
            size_t datalen = strlen(data);
            CANFETCHNEXT(prev, prevlen, key, keylen, data, datalen);
            prev = key;
            prevlen = keylen;
        }
        CANNOTFETCHNEXT(prev, prevlen, TWOM_NOTFOUND);
        CANCOMMIT();
    }

    /* delete all records during foreach */
    deldata.db = db;
    deldata.results = &results;
    r = twom_db_foreach(db, NULL, 0, NULL, deleter, &deldata, TWOM_ALWAYSYIELD);
    ASSERT_OK(r);
    for (i = 0; i < 8; i++) {
        int n = order[i];
        if (n < 0) continue;
        GOTRESULT(KEY[n], strlen(KEY[n]), DATA[n], strlen(DATA[n]));
    }
    ASSERT_NULL(results);

    /* nothing left */
    r = twom_db_foreach(db, NULL, 0, NULL, foreacher, &results, TWOM_ALWAYSYIELD);
    ASSERT_OK(r);
    ASSERT_NULL(results);

    /* even with a prefix */
    r = twom_db_foreach(db, " ", 1, NULL, foreacher, &results, TWOM_ALWAYSYIELD);
    ASSERT_OK(r);
    ASSERT_NULL(results);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Test 16: test_foreach_changes
 * ============================================================
 */

#define K0 "affect"
#define K0U "bother"
#define K1 "carib"
#define K2 "cubist"
#define K3 "eulogy"
#define K4 "kidding"
#define K4A "llama"
#define K5 "monkey"
#define K6 "notice"
#define K7 "octopus"
#define K7D "opossum"
#define K7A "possum"
#define K7B "quine"
#define K8 "rooster"

struct ffrock {
    struct twom_db *db;
    struct twom_txn *txn; /* NULL for non-txn case */
    int state;
};

/* helper: store through txn or db */
static int ff_store(struct ffrock *fr, const char *key, size_t keylen,
                    const char *data, size_t datalen, int flags)
{
    if (fr->txn)
        return twom_txn_store(fr->txn, key, keylen, data, datalen, flags);
    return twom_db_store(fr->db, key, keylen, data, datalen, flags);
}

/* helper: fetch through txn or db */
static int ff_fetch(struct ffrock *fr, const char *key, size_t keylen)
{
    if (fr->txn)
        return twom_txn_fetch(fr->txn, key, keylen, NULL, NULL, NULL, NULL, 0);
    return twom_db_fetch(fr->db, key, keylen, NULL, NULL, NULL, NULL, 0);
}

/* helper: delete through txn or db (force=1) */
static int ff_delete(struct ffrock *fr, const char *key, size_t keylen)
{
    if (fr->txn)
        return twom_txn_store(fr->txn, key, keylen, NULL, 0, 0);
    return twom_db_store(fr->db, key, keylen, NULL, 0, 0);
}

static int ffstatemachine(void *rockp,
                          const char *key, size_t keylen,
                          const char *data __attribute__((unused)),
                          size_t datalen __attribute__((unused)))
{
    struct ffrock *fr = (struct ffrock *)rockp;
    char *kcopy = strndup(key, keylen);
    assert(kcopy);
    int r;

    switch (fr->state) {
    case 0:
        CB_ASSERT_STR_EQ(kcopy, K1);
        fr->state = 1;
        break;
    case 1:
        CB_ASSERT_STR_EQ(kcopy, K2);
        /* test prior-location store */
        r = ff_store(fr, K0, strlen(K0), "", 0, 0);
        CB_ASSERT_EQ(r, 0);
        fr->state = 2;
        break;
    case 2:
        CB_ASSERT_STR_EQ(kcopy, K3);
        /* test prior non-existent fetch */
        r = ff_fetch(fr, K0U, strlen(K0U));
        CB_ASSERT_EQ(r, TWOM_NOTFOUND);
        fr->state = 3;
        break;
    case 3:
        CB_ASSERT_STR_EQ(kcopy, K4);
        r = ff_store(fr, K4A, strlen(K4A), "", 0, 0);
        CB_ASSERT_EQ(r, 0);
        fr->state = 4;
        break;
    case 4:
        /* we found the after-added record correctly */
        CB_ASSERT_STR_EQ(kcopy, K4A);
        r = ff_store(fr, K4A, strlen(K4A), "another", 7, 0);
        CB_ASSERT_EQ(r, 0);
        fr->state = 5;
        break;
    case 5:
        /* didn't repeat after replacing */
        CB_ASSERT_STR_EQ(kcopy, K5);
        r = ff_delete(fr, K5, strlen(K5));
        CB_ASSERT_EQ(r, TWOM_OK);
        fr->state = 6;
        break;
    case 6:
        /* moved on after deleting */
        CB_ASSERT_STR_EQ(kcopy, K6);
        fr->state = 7;
        break;
    case 7:
        CB_ASSERT_STR_EQ(kcopy, K7);
        /* replace, add two more, then delete the next */
        r = ff_store(fr, K7, strlen(K7), "newval", 6, 0);
        CB_ASSERT_EQ(r, TWOM_OK);
        r = ff_store(fr, K7D, strlen(K7D), "val", 3, 0);
        CB_ASSERT_EQ(r, TWOM_OK);
        r = ff_store(fr, K7B, strlen(K7B), "bval", 4, 0);
        CB_ASSERT_EQ(r, TWOM_OK);
        r = ff_store(fr, K7A, strlen(K7A), "aval", 4, 0);
        CB_ASSERT_EQ(r, TWOM_OK);
        r = ff_delete(fr, K7D, strlen(K7D));
        CB_ASSERT_EQ(r, TWOM_OK);
        fr->state = 8;
        break;
    case 8:
        CB_ASSERT_STR_EQ(kcopy, K7A);
        fr->state = 9;
        break;
    case 9:
        CB_ASSERT_STR_EQ(kcopy, K7B);
        fr->state = 10;
        break;
    case 10:
        CB_ASSERT_STR_EQ(kcopy, K8);
        fr->state = 11;
        break;
    default:
        CB_ASSERT(0); /* bogus state */
        break;
    }

    free(kcopy);
    return 0;
}

static void test_foreach_changes(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    struct ffrock rock;

    static const char KEY1[] = K1;
    static const char DATA1[] = "delays maj bullish packard ronald";
    static const char KEY2[] = K2;
    static const char DATA2[] = "bobby tswana cu albumin created";
    static const char KEY3[] = K3;
    static const char DATA3[] = "aleut stoic muscovy adonis moe docent";
    static const char KEY4[] = K4;
    static const char DATA4[] = "curry deterrent drove raising hiring";
    static const char KEY5[] = K5;
    static const char DATA5[] = "joining keeper angle burden buffer";
    static const char KEY6[] = K6;
    static const char DATA6[] = "annoying push security plenty ending";
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    CANSTORE(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    CANSTORE(KEY2, strlen(KEY2), DATA2, strlen(DATA2));
    CANSTORE(KEY3, strlen(KEY3), DATA3, strlen(DATA3));
    CANSTORE(KEY4, strlen(KEY4), DATA4, strlen(DATA4));
    CANSTORE(KEY5, strlen(KEY5), DATA5, strlen(DATA5));
    CANSTORE(KEY6, strlen(KEY6), DATA6, strlen(DATA6));
    CANCOMMIT();

    /* with txn */
    cb_failures = 0;
    r = twom_db_begin_txn(db, 0, &txn);
    ASSERT_OK(r);

    rock.db = db;
    rock.txn = txn;
    rock.state = 0;

    r = twom_txn_foreach(txn, NULL, 0, NULL, ffstatemachine, &rock, 0);
    ASSERT_OK(r);
    ASSERT_EQ(rock.state, 7);
    ASSERT_EQ(cb_failures, 0);

    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);

    /* without txn */
    db = NULL;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename2, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    CANSTORE(KEY1, strlen(KEY1), DATA1, strlen(DATA1));
    CANSTORE(KEY2, strlen(KEY2), DATA2, strlen(DATA2));
    CANSTORE(KEY3, strlen(KEY3), DATA3, strlen(DATA3));
    CANSTORE(KEY4, strlen(KEY4), DATA4, strlen(DATA4));
    CANSTORE(KEY5, strlen(KEY5), DATA5, strlen(DATA5));
    CANSTORE(KEY6, strlen(KEY6), DATA6, strlen(DATA6));
    CANCOMMIT();

    cb_failures = 0;
    rock.db = db;
    rock.txn = NULL;
    rock.state = 0;

    r = twom_db_foreach(db, NULL, 0, NULL, ffstatemachine, &rock, TWOM_ALWAYSYIELD);
    ASSERT_OK(r);
    ASSERT_EQ(rock.state, 7);
    ASSERT_EQ(cb_failures, 0);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

#undef K0
#undef K0U
#undef K1
#undef K2
#undef K3
#undef K4
#undef K4A
#undef K5
#undef K6

/*
 * ============================================================
 * Test 17: test_binary_keys
 * ============================================================
 */

static void test_binary_keys(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    static const char KEY1[] = "master\0cleanse";
    static const char DATA1[] = "ethical";
    static const char KEY2[] = "cardigan\tdreamcatcher";
    static const char DATA2[] = "shoreditch";
    static const char KEY3[] = "pitchfork\rcarles";
    static const char DATA3[] = "tumble";
    static const char KEY4[] = "seitan\nraw\ndenim";
    static const char DATA4[] = "fap";
    static const char KEY5[] = { 0x01, 0x02, 0x04, 0x08,
                                 0x10, 0x20, 0x40, 0x80,
                                 0x00/*unused*/ };
    static const char DATA5[] = "farm-to-table";
    static const char KEY6[] = " BLANK\x07\xa0";
    static const char DATA6[] = "magic blank in key!";
    struct binary_result *results = NULL;
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    CANNOTFETCH(KEY1, sizeof(KEY1)-1, TWOM_NOTFOUND);
    CANNOTFETCH(KEY2, sizeof(KEY2)-1, TWOM_NOTFOUND);
    CANNOTFETCH(KEY3, sizeof(KEY3)-1, TWOM_NOTFOUND);
    CANNOTFETCH(KEY4, sizeof(KEY4)-1, TWOM_NOTFOUND);
    CANNOTFETCH(KEY5, sizeof(KEY5)-1, TWOM_NOTFOUND);
    CANNOTFETCH(KEY6, sizeof(KEY6)-1, TWOM_NOTFOUND);

    CANSTORE(KEY1, sizeof(KEY1)-1, DATA1, sizeof(DATA1)-1);
    CANSTORE(KEY2, sizeof(KEY2)-1, DATA2, sizeof(DATA2)-1);
    CANSTORE(KEY3, sizeof(KEY3)-1, DATA3, sizeof(DATA3)-1);
    CANSTORE(KEY4, sizeof(KEY4)-1, DATA4, sizeof(DATA4)-1);
    CANSTORE(KEY5, sizeof(KEY5)-1, DATA5, sizeof(DATA5)-1);
    CANSTORE(KEY6, sizeof(KEY6)-1, DATA6, sizeof(DATA6)-1);

    CANFETCH(KEY1, sizeof(KEY1)-1, DATA1, sizeof(DATA1)-1);
    CANFETCH(KEY2, sizeof(KEY2)-1, DATA2, sizeof(DATA2)-1);
    CANFETCH(KEY3, sizeof(KEY3)-1, DATA3, sizeof(DATA3)-1);
    CANFETCH(KEY4, sizeof(KEY4)-1, DATA4, sizeof(DATA4)-1);
    CANFETCH(KEY5, sizeof(KEY5)-1, DATA5, sizeof(DATA5)-1);
    CANFETCH(KEY6, sizeof(KEY6)-1, DATA6, sizeof(DATA6)-1);
    CANCOMMIT();

    CANFETCH(KEY1, sizeof(KEY1)-1, DATA1, sizeof(DATA1)-1);
    CANFETCH(KEY2, sizeof(KEY2)-1, DATA2, sizeof(DATA2)-1);
    CANFETCH(KEY3, sizeof(KEY3)-1, DATA3, sizeof(DATA3)-1);
    CANFETCH(KEY4, sizeof(KEY4)-1, DATA4, sizeof(DATA4)-1);
    CANFETCH(KEY5, sizeof(KEY5)-1, DATA5, sizeof(DATA5)-1);
    CANFETCH(KEY6, sizeof(KEY6)-1, DATA6, sizeof(DATA6)-1);
    CANCOMMIT();

    /* foreach in txn */
    r = twom_db_begin_txn(db, 0, &txn);
    ASSERT_OK(r);
    r = twom_txn_foreach(txn, NULL, 0, NULL, foreacher, &results, 0);
    ASSERT_OK(r);

    /* byte-sorted order */
    GOTRESULT(KEY5, sizeof(KEY5)-1, DATA5, sizeof(DATA5)-1);
    GOTRESULT(KEY6, sizeof(KEY6)-1, DATA6, sizeof(DATA6)-1);
    GOTRESULT(KEY2, sizeof(KEY2)-1, DATA2, sizeof(DATA2)-1);
    GOTRESULT(KEY1, sizeof(KEY1)-1, DATA1, sizeof(DATA1)-1);
    GOTRESULT(KEY3, sizeof(KEY3)-1, DATA3, sizeof(DATA3)-1);
    GOTRESULT(KEY4, sizeof(KEY4)-1, DATA4, sizeof(DATA4)-1);
    ASSERT_NULL(results);

    CANCOMMIT();

    CANREOPEN();

    CANFETCH(KEY1, sizeof(KEY1)-1, DATA1, sizeof(DATA1)-1);
    CANFETCH(KEY2, sizeof(KEY2)-1, DATA2, sizeof(DATA2)-1);
    CANFETCH(KEY3, sizeof(KEY3)-1, DATA3, sizeof(DATA3)-1);
    CANFETCH(KEY4, sizeof(KEY4)-1, DATA4, sizeof(DATA4)-1);
    CANFETCH(KEY5, sizeof(KEY5)-1, DATA5, sizeof(DATA5)-1);
    CANFETCH(KEY6, sizeof(KEY6)-1, DATA6, sizeof(DATA6)-1);
    CANCOMMIT();

    /* foreach without txn */
    r = twom_db_foreach(db, NULL, 0, NULL, foreacher, &results, TWOM_ALWAYSYIELD);
    ASSERT_OK(r);

    GOTRESULT(KEY5, sizeof(KEY5)-1, DATA5, sizeof(DATA5)-1);
    GOTRESULT(KEY6, sizeof(KEY6)-1, DATA6, sizeof(DATA6)-1);
    GOTRESULT(KEY2, sizeof(KEY2)-1, DATA2, sizeof(DATA2)-1);
    GOTRESULT(KEY1, sizeof(KEY1)-1, DATA1, sizeof(DATA1)-1);
    GOTRESULT(KEY3, sizeof(KEY3)-1, DATA3, sizeof(DATA3)-1);
    GOTRESULT(KEY4, sizeof(KEY4)-1, DATA4, sizeof(DATA4)-1);
    ASSERT_NULL(results);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Test 18: test_binary_data
 * ============================================================
 */

static void test_binary_data(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    static const char KEY1[] = "vinyl";
    static const char DATA1[] = "cosby\0sweater";
    static const char KEY2[] = "blog";
    static const char DATA2[] = "next\tlevel";
    static const char KEY3[] = "chambray";
    static const char DATA3[] = "mcsweeneys\rletterpress";
    static const char KEY4[] = "synth";
    static const char DATA4[] = "readymade\ncliche\nterry\nrichardson";
    static const char KEY5[] = "fixie";
    static const char DATA5[] = { 0x01, 0x02, 0x04, 0x08,
                                  0x10, 0x20, 0x40, 0x80,
                                  0x00/*unused*/ };
    static const char KEY6[] = "magic blank in data!";
    static const char DATA6[] = " BLANK\x07\xa0";
    struct binary_result *results = NULL;
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    CANNOTFETCH(KEY1, sizeof(KEY1)-1, TWOM_NOTFOUND);
    CANNOTFETCH(KEY2, sizeof(KEY2)-1, TWOM_NOTFOUND);
    CANNOTFETCH(KEY3, sizeof(KEY3)-1, TWOM_NOTFOUND);
    CANNOTFETCH(KEY4, sizeof(KEY4)-1, TWOM_NOTFOUND);
    CANNOTFETCH(KEY5, sizeof(KEY5)-1, TWOM_NOTFOUND);
    CANNOTFETCH(KEY6, sizeof(KEY6)-1, TWOM_NOTFOUND);

    CANSTORE(KEY1, sizeof(KEY1)-1, DATA1, sizeof(DATA1)-1);
    CANSTORE(KEY2, sizeof(KEY2)-1, DATA2, sizeof(DATA2)-1);
    CANSTORE(KEY3, sizeof(KEY3)-1, DATA3, sizeof(DATA3)-1);
    CANSTORE(KEY4, sizeof(KEY4)-1, DATA4, sizeof(DATA4)-1);
    CANSTORE(KEY5, sizeof(KEY5)-1, DATA5, sizeof(DATA5)-1);
    CANSTORE(KEY6, sizeof(KEY6)-1, DATA6, sizeof(DATA6)-1);

    CANFETCH(KEY1, sizeof(KEY1)-1, DATA1, sizeof(DATA1)-1);
    CANFETCH(KEY2, sizeof(KEY2)-1, DATA2, sizeof(DATA2)-1);
    CANFETCH(KEY3, sizeof(KEY3)-1, DATA3, sizeof(DATA3)-1);
    CANFETCH(KEY4, sizeof(KEY4)-1, DATA4, sizeof(DATA4)-1);
    CANFETCH(KEY5, sizeof(KEY5)-1, DATA5, sizeof(DATA5)-1);
    CANFETCH(KEY6, sizeof(KEY6)-1, DATA6, sizeof(DATA6)-1);
    CANCOMMIT();

    CANFETCH(KEY1, sizeof(KEY1)-1, DATA1, sizeof(DATA1)-1);
    CANFETCH(KEY2, sizeof(KEY2)-1, DATA2, sizeof(DATA2)-1);
    CANFETCH(KEY3, sizeof(KEY3)-1, DATA3, sizeof(DATA3)-1);
    CANFETCH(KEY4, sizeof(KEY4)-1, DATA4, sizeof(DATA4)-1);
    CANFETCH(KEY5, sizeof(KEY5)-1, DATA5, sizeof(DATA5)-1);
    CANFETCH(KEY6, sizeof(KEY6)-1, DATA6, sizeof(DATA6)-1);
    CANCOMMIT();

    /* foreach in txn */
    r = twom_db_begin_txn(db, 0, &txn);
    ASSERT_OK(r);
    r = twom_txn_foreach(txn, NULL, 0, NULL, foreacher, &results, 0);
    ASSERT_OK(r);

    /* byte-sorted key order */
    GOTRESULT(KEY2, sizeof(KEY2)-1, DATA2, sizeof(DATA2)-1);
    GOTRESULT(KEY3, sizeof(KEY3)-1, DATA3, sizeof(DATA3)-1);
    GOTRESULT(KEY5, sizeof(KEY5)-1, DATA5, sizeof(DATA5)-1);
    GOTRESULT(KEY6, sizeof(KEY6)-1, DATA6, sizeof(DATA6)-1);
    GOTRESULT(KEY4, sizeof(KEY4)-1, DATA4, sizeof(DATA4)-1);
    GOTRESULT(KEY1, sizeof(KEY1)-1, DATA1, sizeof(DATA1)-1);
    ASSERT_NULL(results);

    CANCOMMIT();

    CANREOPEN();

    CANFETCH(KEY1, sizeof(KEY1)-1, DATA1, sizeof(DATA1)-1);
    CANFETCH(KEY2, sizeof(KEY2)-1, DATA2, sizeof(DATA2)-1);
    CANFETCH(KEY3, sizeof(KEY3)-1, DATA3, sizeof(DATA3)-1);
    CANFETCH(KEY4, sizeof(KEY4)-1, DATA4, sizeof(DATA4)-1);
    CANFETCH(KEY5, sizeof(KEY5)-1, DATA5, sizeof(DATA5)-1);
    CANFETCH(KEY6, sizeof(KEY6)-1, DATA6, sizeof(DATA6)-1);

    /* foreach in txn (after reopen) */
    r = twom_txn_foreach(txn, NULL, 0, NULL, foreacher, &results, 0);
    ASSERT_OK(r);

    GOTRESULT(KEY2, sizeof(KEY2)-1, DATA2, sizeof(DATA2)-1);
    GOTRESULT(KEY3, sizeof(KEY3)-1, DATA3, sizeof(DATA3)-1);
    GOTRESULT(KEY5, sizeof(KEY5)-1, DATA5, sizeof(DATA5)-1);
    GOTRESULT(KEY6, sizeof(KEY6)-1, DATA6, sizeof(DATA6)-1);
    GOTRESULT(KEY4, sizeof(KEY4)-1, DATA4, sizeof(DATA4)-1);
    GOTRESULT(KEY1, sizeof(KEY1)-1, DATA1, sizeof(DATA1)-1);
    ASSERT_NULL(results);

    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Test 19: test_many
 * ============================================================
 */

#define MAXN 4095

static const char *nth_compound(unsigned int n,
                                const char * const *words,
                                const char *sep,
                                char *buf, size_t bufsz)
{
    buf[0] = '\0';
    size_t pos = 0;

    if ((n / 1000) % 10) {
        pos += (size_t)snprintf(buf + pos, bufsz - pos, "%s", words[28 + (n / 1000) % 10]);
    }
    if ((n / 100) % 10) {
        if (pos && sep)
            pos += (size_t)snprintf(buf + pos, bufsz - pos, "%s", sep);
        pos += (size_t)snprintf(buf + pos, bufsz - pos, "%s", words[19 + (n / 100) % 10]);
    }
    if ((n / 10) % 10) {
        if (pos && sep)
            pos += (size_t)snprintf(buf + pos, bufsz - pos, "%s", sep);
        pos += (size_t)snprintf(buf + pos, bufsz - pos, "%s", words[10 + (n / 10) % 10]);
    }
    if (pos && sep)
        pos += (size_t)snprintf(buf + pos, bufsz - pos, "%s", sep);
    snprintf(buf + pos, bufsz - pos, "%s", words[n % 10]);

    return buf;
}

static const char *nth_key(unsigned int n)
{
    static const char * const words[37] = {
        "dray", "bite", "cue", "ado", "felt",
        "firm", "sal", "ahab", "cab", "lord",
        "blob", "be", "coil", "hay",
        "bled", "got", "leta", "sept", "deft",
        "ibm", "kama", "bean", "ado",
        "cord", "firm", "ben", "fore", "huck",
        "haas", "jack", "aden", "nerf",
        "gash", "stu", "nona", "gel", "ale"
    };
    static char buf[256];
    return nth_compound(n, words, ".", buf, sizeof(buf));
}

static const char *nth_data(unsigned int n)
{
    static const char * const words[37] = {
        "abettor", "afresh", "aisling", "arthur", "ascots",
        "belled", "berserk", "border", "bourbon", "brawny",
        "carpels", "cavils", "coating", "cologne",
        "concern", "consul", "crater", "crocks", "deirdre",
        "dewier", "disdain", "dowdier", "duncan",
        "eighth", "enigma", "evelyn", "fennel", "flowery",
        "flukier", "forums", "gametes", "gamins",
        "gavels", "gibbers", "gulags", "gunther", "gunwale"
    };
    static char buf[256];
    return nth_compound(n, words, " ", buf, sizeof(buf));
}

static int finder(void *rock,
                  const char *key, size_t keylen,
                  const char *data, size_t datalen)
{
    struct hash_table *exphash = (struct hash_table *)rock;
    char kbuf[256];

    CB_ASSERT(key != NULL);
    CB_ASSERT(keylen > 0);
    CB_ASSERT(data != NULL);
    CB_ASSERT(datalen > 0);

    if (keylen >= sizeof(kbuf)) keylen = sizeof(kbuf) - 1;
    memcpy(kbuf, key, keylen);
    kbuf[keylen] = '\0';

    char *expected = ht_lookup(exphash, kbuf);
    CB_ASSERT(expected != NULL);
    if (expected) {
        CB_ASSERT_EQ(datalen, strlen(expected));
        CB_ASSERT(memcmp(data, expected, datalen) == 0);
        ht_del(exphash, kbuf);
        free(expected);
    }

    return 0;
}

static void test_many(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    struct hash_table exphash;
    unsigned int n;
    int r;

    ht_init(&exphash, (MAXN + 1) * 4);

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    /* store records */
    for (n = 0; n <= MAXN; n++) {
        const char *key = nth_key(n);
        const char *data = nth_data(n);
        CANSTORE(key, strlen(key), data, strlen(data));
    }
    CANCOMMIT();

    /* check all records */
    for (n = 0; n <= MAXN; n++) {
        const char *key = nth_key(n);
        const char *data = nth_data(n);
        CANFETCH(key, strlen(key), data, strlen(data));
    }

    /* prefix=NULL: iterate all records */
    cb_failures = 0;
    for (n = 0; n <= MAXN; n++) {
        const char *key = nth_key(n);
        const char *data = nth_data(n);
        ht_insert(&exphash, key, strdup(data));
    }
    ASSERT_EQ(exphash.count, MAXN + 1);
    r = twom_txn_foreach(txn, NULL, 0, NULL, finder, &exphash, 0);
    ASSERT_OK(r);
    ASSERT_EQ(exphash.count, 0);
    ASSERT_EQ(cb_failures, 0);

    /* prefix="": iterate all records */
    cb_failures = 0;
    for (n = 0; n <= MAXN; n++) {
        const char *key = nth_key(n);
        const char *data = nth_data(n);
        ht_insert(&exphash, key, strdup(data));
    }
    r = twom_txn_foreach(txn, "", 0, NULL, finder, &exphash, 0);
    ASSERT_OK(r);
    ASSERT_EQ(exphash.count, 0);
    ASSERT_EQ(cb_failures, 0);

    /* prefix="jack.": iterate n/1000==1 (1000 records) */
    cb_failures = 0;
    {
        unsigned int nsubset = 0;
        for (n = 0; n <= MAXN; n++) {
            if (n / 1000 == 1) {
                const char *key = nth_key(n);
                const char *data = nth_data(n);
                ht_insert(&exphash, key, strdup(data));
                nsubset++;
            }
        }
        ASSERT_EQ(nsubset, 1000);
    }
    r = twom_txn_foreach(txn, "jack.", 5, NULL, finder, &exphash, 0);
    ASSERT_OK(r);
    ASSERT_EQ(exphash.count, 0);
    ASSERT_EQ(cb_failures, 0);

    /* delete records one by one */
    for (n = 0; n <= MAXN; n++) {
        const char *key = nth_key(n);
        r = twom_txn_store(txn, key, strlen(key), NULL, 0, 0);
        ASSERT_OK(r);

        if (n && n % 301 == 0) {
            /* check remaining records */
            unsigned int remain = MAXN - n;
            cb_failures = 0;
            unsigned int i;
            for (i = 0; i <= MAXN; i++) {
                if (i > n) {
                    const char *k = nth_key(i);
                    const char *d = nth_data(i);
                    ht_insert(&exphash, k, strdup(d));
                }
            }
            ASSERT_EQ(exphash.count, remain);
            r = twom_txn_foreach(txn, NULL, 0, NULL, finder, &exphash, 0);
            ASSERT_OK(r);
            ASSERT_EQ(exphash.count, 0);
            ASSERT_EQ(cb_failures, 0);
        }
    }

    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);

    ht_free(&exphash, free);
}

#undef MAXN

/*
 * ============================================================
 * Test 20: test_foreach_replace
 * ============================================================
 */

struct replace_data {
    struct twom_txn *txn;
};

static int replacer(void *rock,
                    const char *key, size_t keylen,
                    const char *data, size_t datalen)
{
    struct replace_data *rd = rock;

    CB_ASSERT(data != NULL);
    CB_ASSERT(datalen > 0);

    int r = twom_txn_store(rd->txn, key, keylen, "bogus", 5, 0);
    CB_ASSERT_OK(r);

    return 0;
}

static void test_foreach_replace(void)
{
    int r;
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    struct replace_data data;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);

    CANSTORE("01", 2, "one", 3);
    CANSTORE("02", 2, "two", 3);
    CANSTORE("03", 2, "thr", 3);
    CANCOMMIT();

    CANREOPEN();

    CANFETCH("01", 2, "one", 3);
    CANFETCH("02", 2, "two", 3);
    CANFETCH("03", 2, "thr", 3);

    /* replace all values during foreach */
    cb_failures = 0;
    data.txn = txn;

    r = twom_txn_foreach(txn, NULL, 0, NULL, replacer, &data, 0);
    ASSERT_OK(r);
    ASSERT_EQ(cb_failures, 0);

    CANCOMMIT();
    CANREOPEN();

    CANFETCH("01", 2, "bogus", 5);
    CANFETCH("02", 2, "bogus", 5);
    CANFETCH("03", 2, "bogus", 5);
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Cursor tests
 * ============================================================
 */

static void test_cursor_basic(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    struct twom_cursor *cur = NULL;
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    /* store 5 sorted records */
    CANSTORE("apple", 5, "val_a", 5);
    CANSTORE("banana", 6, "val_b", 5);
    CANSTORE("cherry", 6, "val_c", 5);
    CANSTORE("cranberry", 9, "val_cr", 6);
    CANSTORE("date", 4, "val_d", 5);
    CANCOMMIT();
    CANREOPEN();

    /* full iteration */
    r = twom_db_begin_cursor(db, NULL, 0, &cur, 0);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(cur);

    const char *key, *val;
    size_t keylen, vallen;

    r = twom_cursor_next(cur, &key, &keylen, &val, &vallen);
    ASSERT_OK(r);
    ASSERT_EQ(keylen, 5); ASSERT(memcmp(key, "apple", 5) == 0);
    ASSERT_EQ(vallen, 5); ASSERT(memcmp(val, "val_a", 5) == 0);

    r = twom_cursor_next(cur, &key, &keylen, &val, &vallen);
    ASSERT_OK(r);
    ASSERT_EQ(keylen, 6); ASSERT(memcmp(key, "banana", 6) == 0);
    ASSERT_EQ(vallen, 5); ASSERT(memcmp(val, "val_b", 5) == 0);

    r = twom_cursor_next(cur, &key, &keylen, &val, &vallen);
    ASSERT_OK(r);
    ASSERT_EQ(keylen, 6); ASSERT(memcmp(key, "cherry", 6) == 0);
    ASSERT_EQ(vallen, 5); ASSERT(memcmp(val, "val_c", 5) == 0);

    r = twom_cursor_next(cur, &key, &keylen, &val, &vallen);
    ASSERT_OK(r);
    ASSERT_EQ(keylen, 9); ASSERT(memcmp(key, "cranberry", 9) == 0);
    ASSERT_EQ(vallen, 6); ASSERT(memcmp(val, "val_cr", 6) == 0);

    r = twom_cursor_next(cur, &key, &keylen, &val, &vallen);
    ASSERT_OK(r);
    ASSERT_EQ(keylen, 4); ASSERT(memcmp(key, "date", 4) == 0);
    ASSERT_EQ(vallen, 5); ASSERT(memcmp(val, "val_d", 5) == 0);

    r = twom_cursor_next(cur, &key, &keylen, &val, &vallen);
    ASSERT_EQ(r, TWOM_DONE);

    r = twom_cursor_abort(&cur);
    ASSERT_OK(r);

    /* TWOM_CURSOR_PREFIX: only keys starting with "c" */
    r = twom_db_begin_cursor(db, "c", 1, &cur, TWOM_CURSOR_PREFIX);
    ASSERT_OK(r);

    r = twom_cursor_next(cur, &key, &keylen, &val, &vallen);
    ASSERT_OK(r);
    ASSERT_EQ(keylen, 6); ASSERT(memcmp(key, "cherry", 6) == 0);

    r = twom_cursor_next(cur, &key, &keylen, &val, &vallen);
    ASSERT_OK(r);
    ASSERT_EQ(keylen, 9); ASSERT(memcmp(key, "cranberry", 9) == 0);

    r = twom_cursor_next(cur, &key, &keylen, &val, &vallen);
    ASSERT_EQ(r, TWOM_DONE);

    r = twom_cursor_abort(&cur);
    ASSERT_OK(r);

    /* TWOM_SKIPROOT: start at "cherry" but skip it */
    r = twom_db_begin_cursor(db, "cherry", 6, &cur, TWOM_SKIPROOT);
    ASSERT_OK(r);

    r = twom_cursor_next(cur, &key, &keylen, &val, &vallen);
    ASSERT_OK(r);
    ASSERT_EQ(keylen, 9); ASSERT(memcmp(key, "cranberry", 9) == 0);

    r = twom_cursor_abort(&cur);
    ASSERT_OK(r);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static void test_cursor_replace(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    struct twom_cursor *cur = NULL;
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    /* store 3 records */
    CANSTORE("alpha", 5, "old_a", 5);
    CANSTORE("beta", 4, "old_b", 5);
    CANSTORE("gamma", 5, "old_g", 5);
    CANCOMMIT();
    CANREOPEN();

    /* open write cursor (no TWOM_SHARED) */
    r = twom_db_begin_cursor(db, NULL, 0, &cur, 0);
    ASSERT_OK(r);

    const char *key, *val;
    size_t keylen, vallen;

    /* first record: alpha */
    r = twom_cursor_next(cur, &key, &keylen, &val, &vallen);
    ASSERT_OK(r);
    ASSERT_EQ(keylen, 5); ASSERT(memcmp(key, "alpha", 5) == 0);

    /* second record: beta - replace it */
    r = twom_cursor_next(cur, &key, &keylen, &val, &vallen);
    ASSERT_OK(r);
    ASSERT_EQ(keylen, 4); ASSERT(memcmp(key, "beta", 4) == 0);
    ASSERT_EQ(vallen, 5); ASSERT(memcmp(val, "old_b", 5) == 0);

    r = twom_cursor_replace(cur, "new_b", 5, 0);
    ASSERT_OK(r);

    /* third record: gamma - unchanged */
    r = twom_cursor_next(cur, &key, &keylen, &val, &vallen);
    ASSERT_OK(r);
    ASSERT_EQ(keylen, 5); ASSERT(memcmp(key, "gamma", 5) == 0);
    ASSERT_EQ(vallen, 5); ASSERT(memcmp(val, "old_g", 5) == 0);

    r = twom_cursor_next(cur, &key, &keylen, &val, &vallen);
    ASSERT_EQ(r, TWOM_DONE);

    /* commit the cursor */
    r = twom_cursor_commit(&cur);
    ASSERT_OK(r);

    /* reopen and verify */
    CANREOPEN();

    CANFETCH("alpha", 5, "old_a", 5);
    CANFETCH("beta", 4, "new_b", 5);
    CANFETCH("gamma", 5, "old_g", 5);
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static void test_cursor_txn(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    struct twom_cursor *cur = NULL;
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    /* begin write txn and store records */
    r = twom_db_begin_txn(db, 0, &txn);
    ASSERT_OK(r);

    r = twom_txn_store(txn, "one", 3, "val_1", 5, 0);
    ASSERT_OK(r);
    r = twom_txn_store(txn, "two", 3, "val_2", 5, 0);
    ASSERT_OK(r);
    r = twom_txn_store(txn, "three", 5, "val_3", 5, 0);
    ASSERT_OK(r);

    /* cursor inside the transaction sees uncommitted data */
    r = twom_txn_begin_cursor(txn, NULL, 0, &cur, 0);
    ASSERT_OK(r);

    const char *key, *val;
    size_t keylen, vallen;
    int count = 0;

    while ((r = twom_cursor_next(cur, &key, &keylen, &val, &vallen)) == TWOM_OK) {
        count++;
    }
    ASSERT_EQ(r, TWOM_DONE);
    ASSERT_EQ(count, 3);

    /* fini cursor only, txn still alive */
    twom_cursor_fini(&cur);
    ASSERT_NULL(cur);

    /* commit the txn */
    r = twom_txn_commit(&txn);
    ASSERT_OK(r);

    /* reopen and verify records persisted */
    CANREOPEN();

    CANFETCH("one", 3, "val_1", 5);
    CANFETCH("two", 3, "val_2", 5);
    CANFETCH("three", 5, "val_3", 5);
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * MVCC tests (multi-process)
 * ============================================================
 */

static void test_mvcc_write_while_reading(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    struct twom_cursor *cur = NULL;
    int r;

    /* populate database */
    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    CANSTORE("apple", 5, "old_a", 5);
    CANSTORE("banana", 6, "old_b", 5);
    CANSTORE("cherry", 6, "old_c", 5);
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
    db = NULL;

    /* set up pipes for synchronization */
    int p2c[2], c2p[2];
    r = pipe(p2c);
    ASSERT_EQ(r, 0);
    r = pipe(c2p);
    ASSERT_EQ(r, 0);

    pid_t pid = fork();
    ASSERT(pid >= 0);

    if (pid == 0) {
        /* === CHILD === */
        close(p2c[1]); /* close write end of parent-to-child */
        close(c2p[0]); /* close read end of child-to-parent */

        /* wait for parent to open MVCC cursor */
        wait_for_peer(p2c[0]);

        /* open db and write a new value for banana */
        struct twom_open_data cinit = TWOM_OPEN_DATA_INITIALIZER;
        struct twom_db *cdb = NULL;
        struct twom_txn *ctxn = NULL;

        int cr = twom_db_open(filename, &cinit, &cdb, NULL);
        assert(cr == TWOM_OK);

        cr = twom_db_begin_txn(cdb, 0, &ctxn);
        assert(cr == TWOM_OK);

        cr = twom_txn_store(ctxn, "banana", 6, "new_b", 5, 0);
        assert(cr == TWOM_OK);

        cr = twom_txn_commit(&ctxn);
        assert(cr == TWOM_OK);

        cr = twom_db_close(&cdb);
        assert(cr == TWOM_OK);

        /* signal parent that write is done */
        signal_peer(c2p[1]);

        /* wait for parent to finish reading */
        wait_for_peer(p2c[0]);

        close(p2c[0]);
        close(c2p[1]);
        _exit(0);
    }

    /* === PARENT === */
    close(p2c[0]); /* close read end of parent-to-child */
    close(c2p[1]); /* close write end of child-to-parent */

    /* open db and begin MVCC cursor (shared so child can write) */
    init.flags = 0;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    r = twom_db_begin_cursor(db, NULL, 0, &cur, TWOM_SHARED | TWOM_MVCC);
    ASSERT_OK(r);

    /* yield lock so child can acquire write lock */
    r = twom_db_yield(db);
    ASSERT_OK(r);

    /* signal child to do its write */
    signal_peer(p2c[1]);

    /* wait for child to finish writing */
    wait_for_peer(c2p[0]);

    /* iterate cursor - must see the OLD value of banana */
    const char *key, *val;
    size_t keylen, vallen;
    int saw_banana = 0;

    while ((r = twom_cursor_next(cur, &key, &keylen, &val, &vallen)) == TWOM_OK) {
        if (keylen == 6 && memcmp(key, "banana", 6) == 0) {
            saw_banana = 1;
            ASSERT_EQ(vallen, 5);
            ASSERT(memcmp(val, "old_b", 5) == 0);
        }
    }
    ASSERT_EQ(r, TWOM_DONE);
    ASSERT(saw_banana);

    r = twom_cursor_abort(&cur);
    ASSERT_OK(r);

    r = twom_db_close(&db);
    ASSERT_OK(r);

    /* signal child it can exit */
    signal_peer(p2c[1]);

    /* wait for child and check exit status */
    int status;
    waitpid(pid, &status, 0);
    ASSERT(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);

    close(p2c[1]);
    close(c2p[0]);

    /* verify write actually happened by reopening without MVCC */
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    const char *data;
    size_t datalen;
    r = twom_db_fetch(db, "banana", 6, NULL, NULL, &data, &datalen, 0);
    ASSERT_OK(r);
    ASSERT_EQ(datalen, 5);
    ASSERT(memcmp(data, "new_b", 5) == 0);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static void test_mvcc_delete_while_reading(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    struct twom_cursor *cur = NULL;
    int r;

    /* populate database */
    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    CANSTORE("apple", 5, "val_a", 5);
    CANSTORE("banana", 6, "val_b", 5);
    CANSTORE("cherry", 6, "val_c", 5);
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
    db = NULL;

    /* set up pipes */
    int p2c[2], c2p[2];
    r = pipe(p2c);
    ASSERT_EQ(r, 0);
    r = pipe(c2p);
    ASSERT_EQ(r, 0);

    pid_t pid = fork();
    ASSERT(pid >= 0);

    if (pid == 0) {
        /* === CHILD === */
        close(p2c[1]);
        close(c2p[0]);

        wait_for_peer(p2c[0]);

        /* delete banana */
        struct twom_open_data cinit = TWOM_OPEN_DATA_INITIALIZER;
        struct twom_db *cdb = NULL;
        struct twom_txn *ctxn = NULL;

        int cr = twom_db_open(filename, &cinit, &cdb, NULL);
        assert(cr == TWOM_OK);

        cr = twom_db_begin_txn(cdb, 0, &ctxn);
        assert(cr == TWOM_OK);

        cr = twom_txn_store(ctxn, "banana", 6, NULL, 0, 0);
        assert(cr == TWOM_OK);

        cr = twom_txn_commit(&ctxn);
        assert(cr == TWOM_OK);

        cr = twom_db_close(&cdb);
        assert(cr == TWOM_OK);

        signal_peer(c2p[1]);
        wait_for_peer(p2c[0]);

        close(p2c[0]);
        close(c2p[1]);
        _exit(0);
    }

    /* === PARENT === */
    close(p2c[0]);
    close(c2p[1]);

    /* open db and begin MVCC cursor (shared so child can write) */
    init.flags = 0;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    r = twom_db_begin_cursor(db, NULL, 0, &cur, TWOM_SHARED | TWOM_MVCC);
    ASSERT_OK(r);

    /* yield lock so child can acquire write lock */
    r = twom_db_yield(db);
    ASSERT_OK(r);

    /* signal child to delete banana */
    signal_peer(p2c[1]);
    wait_for_peer(c2p[0]);

    /* iterate cursor - must STILL see banana (snapshot isolation) */
    const char *key, *val;
    size_t keylen, vallen;
    int saw_banana = 0;
    int count = 0;

    while ((r = twom_cursor_next(cur, &key, &keylen, &val, &vallen)) == TWOM_OK) {
        count++;
        if (keylen == 6 && memcmp(key, "banana", 6) == 0) {
            saw_banana = 1;
            ASSERT_EQ(vallen, 5);
            ASSERT(memcmp(val, "val_b", 5) == 0);
        }
    }
    ASSERT_EQ(r, TWOM_DONE);
    ASSERT_EQ(count, 3);
    ASSERT(saw_banana);

    r = twom_cursor_abort(&cur);
    ASSERT_OK(r);

    r = twom_db_close(&db);
    ASSERT_OK(r);

    signal_peer(p2c[1]);

    int status;
    waitpid(pid, &status, 0);
    ASSERT(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);

    close(p2c[1]);
    close(c2p[0]);

    /* verify delete actually happened */
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    const char *data;
    size_t datalen;
    r = twom_db_fetch(db, "banana", 6, NULL, NULL, &data, &datalen, 0);
    ASSERT_EQ(r, TWOM_NOTFOUND);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static void test_mvcc_create_delete_invisible(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    struct twom_cursor *cur = NULL;
    int r;

    /* populate database with apple and cherry only (no banana) */
    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    CANSTORE("apple", 5, "val_a", 5);
    CANSTORE("cherry", 6, "val_c", 5);
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
    db = NULL;

    /* set up pipes */
    int p2c[2], c2p[2];
    r = pipe(p2c);
    ASSERT_EQ(r, 0);
    r = pipe(c2p);
    ASSERT_EQ(r, 0);

    pid_t pid = fork();
    ASSERT(pid >= 0);

    if (pid == 0) {
        /* === CHILD === */
        close(p2c[1]);
        close(c2p[0]);

        wait_for_peer(p2c[0]);

        /* create banana, then delete it */
        struct twom_open_data cinit = TWOM_OPEN_DATA_INITIALIZER;
        struct twom_db *cdb = NULL;
        struct twom_txn *ctxn = NULL;

        int cr = twom_db_open(filename, &cinit, &cdb, NULL);
        assert(cr == TWOM_OK);

        /* first txn: create banana */
        cr = twom_db_begin_txn(cdb, 0, &ctxn);
        assert(cr == TWOM_OK);
        cr = twom_txn_store(ctxn, "banana", 6, "val_b", 5, 0);
        assert(cr == TWOM_OK);
        cr = twom_txn_commit(&ctxn);
        assert(cr == TWOM_OK);

        /* second txn: delete banana */
        cr = twom_db_begin_txn(cdb, 0, &ctxn);
        assert(cr == TWOM_OK);
        cr = twom_txn_store(ctxn, "banana", 6, NULL, 0, 0);
        assert(cr == TWOM_OK);
        cr = twom_txn_commit(&ctxn);
        assert(cr == TWOM_OK);

        cr = twom_db_close(&cdb);
        assert(cr == TWOM_OK);

        signal_peer(c2p[1]);
        wait_for_peer(p2c[0]);

        close(p2c[0]);
        close(c2p[1]);
        _exit(0);
    }

    /* === PARENT === */
    close(p2c[0]);
    close(c2p[1]);

    /* open db and begin MVCC cursor (shared so child can write) */
    init.flags = 0;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    r = twom_db_begin_cursor(db, NULL, 0, &cur, TWOM_SHARED | TWOM_MVCC);
    ASSERT_OK(r);

    /* yield lock so child can acquire write lock */
    r = twom_db_yield(db);
    ASSERT_OK(r);

    /* signal child to create+delete banana */
    signal_peer(p2c[1]);
    wait_for_peer(c2p[0]);

    /* iterate cursor - must NOT see banana */
    const char *key, *val;
    size_t keylen, vallen;
    int saw_banana = 0;
    int count = 0;

    while ((r = twom_cursor_next(cur, &key, &keylen, &val, &vallen)) == TWOM_OK) {
        count++;
        if (keylen == 6 && memcmp(key, "banana", 6) == 0) {
            saw_banana = 1;
        }
    }
    ASSERT_EQ(r, TWOM_DONE);
    ASSERT_EQ(count, 2);  /* only apple and cherry */
    ASSERT(!saw_banana);

    r = twom_cursor_abort(&cur);
    ASSERT_OK(r);

    r = twom_db_close(&db);
    ASSERT_OK(r);

    signal_peer(p2c[1]);

    int status;
    waitpid(pid, &status, 0);
    ASSERT(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);

    close(p2c[1]);
    close(c2p[0]);

    /* also verify banana is gone via fetch */
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    const char *data;
    size_t datalen;
    r = twom_db_fetch(db, "banana", 6, NULL, NULL, &data, &datalen, 0);
    ASSERT_EQ(r, TWOM_NOTFOUND);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Repack, metadata, readonly, conditional store, and misc tests
 * ============================================================
 */

static void test_repack(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    /* store some records */
    CANSTORE("apple", 5, "val_a", 5);
    CANSTORE("banana", 6, "val_b", 5);
    CANSTORE("cherry", 6, "val_c", 5);
    CANCOMMIT();

    /* delete one to create dirty space */
    CANDELETE("banana", 6);
    CANCOMMIT();

    /* overwrite another to create more dirty space */
    CANSTORE("apple", 5, "new_a", 5);
    CANCOMMIT();

    size_t size_before = twom_db_size(db);
    size_t gen_before = twom_db_generation(db);

    /* repack */
    r = twom_db_repack(db);
    ASSERT_OK(r);

    /* generation should increase after repack */
    size_t gen_after = twom_db_generation(db);
    ASSERT(gen_after > gen_before);

    /* repacked file should be smaller (removed dirty space) */
    size_t size_after = twom_db_size(db);
    ASSERT(size_after < size_before);

    /* check consistency */
    ISCONSISTENT();

    /* verify surviving records */
    CANFETCH("apple", 5, "new_a", 5);
    CANNOTFETCH("banana", 6, TWOM_NOTFOUND);
    CANFETCH("cherry", 6, "val_c", 5);
    CANCOMMIT();

    /* verify num_records reflects actual count */
    ASSERT_EQ(twom_db_num_records(db), 2);

    r = twom_db_close(&db);
    ASSERT_OK(r);

    /* reopen and verify data survived */
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    CANFETCH("apple", 5, "new_a", 5);
    CANNOTFETCH("banana", 6, TWOM_NOTFOUND);
    CANFETCH("cherry", 6, "val_c", 5);
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static void test_metadata(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    /* fname should match what we opened */
    const char *fname = twom_db_fname(db);
    ASSERT_NOT_NULL(fname);
    ASSERT_STR_EQ(fname, filename);

    /* uuid should be a 36-char string (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx) */
    const char *uuid = twom_db_uuid(db);
    ASSERT_NOT_NULL(uuid);
    ASSERT_EQ(strlen(uuid), 36);
    ASSERT_EQ(uuid[8], '-');
    ASSERT_EQ(uuid[13], '-');

    /* empty db should have 0 records */
    ASSERT_EQ(twom_db_num_records(db), 0);

    /* size should be positive (at least header + dummy) */
    size_t initial_size = twom_db_size(db);
    ASSERT(initial_size > 0);

    /* generation starts at 1 for a new db */
    size_t gen = twom_db_generation(db);
    ASSERT_EQ(gen, 1);

    /* store some records and check counts */
    CANSTORE("one", 3, "val1", 4);
    CANSTORE("two", 3, "val2", 4);
    CANSTORE("three", 5, "val3", 4);
    CANCOMMIT();

    ASSERT_EQ(twom_db_num_records(db), 3);
    ASSERT(twom_db_size(db) > initial_size);

    /* delete one */
    CANDELETE("two", 3);
    CANCOMMIT();

    ASSERT_EQ(twom_db_num_records(db), 2);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static void test_readonly(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;

    /* first create a database with some data */
    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    CANSTORE("key1", 4, "val1", 4);
    CANSTORE("key2", 4, "val2", 4);
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);

    /* reopen read-only */
    init.flags = TWOM_SHARED;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    /* reads should work */
    CANFETCH_NOTXN("key1", 4, "val1", 4);
    CANFETCH_NOTXN("key2", 4, "val2", 4);

    /* write transaction should fail */
    r = twom_db_begin_txn(db, 0, &txn);
    ASSERT_EQ(r, TWOM_LOCKED);

    /* non-transactional store should also fail */
    r = twom_db_store(db, "key3", 4, "val3", 4, 0);
    ASSERT(r != TWOM_OK);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static void test_conditional_store(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    /* TWOM_IFNOTEXIST: store only if key doesn't exist */
    r = twom_db_begin_txn(db, 0, &txn);
    ASSERT_OK(r);

    r = twom_txn_store(txn, "alpha", 5, "first", 5, TWOM_IFNOTEXIST);
    ASSERT_OK(r);

    /* second store with IFNOTEXIST should fail with EXISTS */
    r = twom_txn_store(txn, "alpha", 5, "second", 6, TWOM_IFNOTEXIST);
    ASSERT_EQ(r, TWOM_EXISTS);

    CANCOMMIT();

    /* verify original value stuck */
    CANFETCH("alpha", 5, "first", 5);
    CANCOMMIT();

    /* TWOM_IFEXIST: store only if key exists */
    r = twom_db_begin_txn(db, 0, &txn);
    ASSERT_OK(r);

    /* update existing key - should succeed */
    r = twom_txn_store(txn, "alpha", 5, "updated", 7, TWOM_IFEXIST);
    ASSERT_OK(r);

    /* update non-existing key - should fail with NOTFOUND */
    r = twom_txn_store(txn, "beta", 4, "value", 5, TWOM_IFEXIST);
    ASSERT_EQ(r, TWOM_NOTFOUND);

    CANCOMMIT();

    /* verify update applied */
    CANFETCH("alpha", 5, "updated", 7);
    CANNOTFETCH("beta", 4, TWOM_NOTFOUND);
    CANCOMMIT();

    /* TWOM_IFEXIST for delete: only delete if exists */
    r = twom_db_begin_txn(db, 0, &txn);
    ASSERT_OK(r);

    /* delete existing key with IFEXIST */
    r = twom_txn_store(txn, "alpha", 5, NULL, 0, TWOM_IFEXIST);
    ASSERT_OK(r);

    /* delete non-existing key with IFEXIST - should fail */
    r = twom_txn_store(txn, "gamma", 5, NULL, 0, TWOM_IFEXIST);
    ASSERT_EQ(r, TWOM_NOTFOUND);

    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static void test_nosync(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;

    /* open with NOSYNC - operations should work, just skip fsync */
    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE | TWOM_NOSYNC;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    CANSTORE("key", 3, "value", 5);
    CANCOMMIT();

    CANREOPEN();

    CANFETCH("key", 3, "value", 5);
    CANCOMMIT();

    ISCONSISTENT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static void test_nocheck(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;

    /* create a normal database first */
    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    CANSTORE("key", 3, "value", 5);
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);

    /* reopen with NOCSUM - should skip checksum verification */
    init.flags = TWOM_NOCSUM;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    CANFETCH_NOTXN("key", 3, "value", 5);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static void test_sync(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    CANSTORE("key", 3, "value", 5);
    CANCOMMIT();

    /* explicit sync should succeed */
    r = twom_db_sync(db);
    ASSERT_OK(r);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static void test_dump(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    CANSTORE("key1", 4, "val1", 4);
    CANSTORE("key2", 4, "val2", 4);
    CANCOMMIT();

    /* redirect stdout to /dev/null so dump output doesn't pollute test output */
    fflush(stdout);
    int saved_stdout = dup(STDOUT_FILENO);
    int devnull = open("/dev/null", O_WRONLY);
    dup2(devnull, STDOUT_FILENO);
    close(devnull);

    /* dump at detail level 0 (summary) */
    r = twom_db_dump(db, 0);
    ASSERT_OK(r);

    /* dump at detail level 1 (verbose) */
    r = twom_db_dump(db, 1);
    ASSERT_OK(r);

    /* restore stdout */
    fflush(stdout);
    dup2(saved_stdout, STDOUT_FILENO);
    close(saved_stdout);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static void test_txn_yield(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    CANSTORE("key", 3, "value", 5);
    CANCOMMIT();

    /* begin a shared (read) transaction */
    r = twom_db_begin_txn(db, TWOM_SHARED, &txn);
    ASSERT_OK(r);

    /* fetch before yield */
    const char *data;
    size_t datalen;
    r = twom_txn_fetch(txn, "key", 3, NULL, NULL, &data, &datalen, 0);
    ASSERT_OK(r);
    ASSERT_EQ(datalen, 5);
    ASSERT(memcmp(data, "value", 5) == 0);

    /* yield should succeed on a read txn */
    r = twom_txn_yield(txn);
    ASSERT_OK(r);

    /* yield on a write txn should fail */
    struct twom_txn *wtxn = NULL;
    r = twom_db_begin_txn(db, 0, &wtxn);
    ASSERT_OK(r);
    r = twom_txn_yield(wtxn);
    ASSERT_EQ(r, TWOM_LOCKED);
    r = twom_txn_abort(&wtxn);
    ASSERT_OK(r);

    /* fetch should still work after yield (re-acquires lock) */
    r = twom_db_begin_txn(db, TWOM_SHARED, &txn);
    ASSERT_OK(r);
    r = twom_txn_fetch(txn, "key", 3, NULL, NULL, &data, &datalen, 0);
    ASSERT_OK(r);
    ASSERT_EQ(datalen, 5);
    ASSERT(memcmp(data, "value", 5) == 0);

    r = twom_txn_abort(&txn);
    ASSERT_OK(r);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static void test_strerror(void)
{
    /* verify all error codes return non-NULL distinct strings */
    const char *s;

    s = twom_strerror(TWOM_OK);
    ASSERT_NOT_NULL(s);
    ASSERT_STR_EQ(s, "OK");

    s = twom_strerror(TWOM_DONE);
    ASSERT_NOT_NULL(s);
    ASSERT_STR_EQ(s, "Done");

    s = twom_strerror(TWOM_IOERROR);
    ASSERT_NOT_NULL(s);

    s = twom_strerror(TWOM_EXISTS);
    ASSERT_NOT_NULL(s);

    s = twom_strerror(TWOM_INTERNAL);
    ASSERT_NOT_NULL(s);

    s = twom_strerror(TWOM_NOTFOUND);
    ASSERT_NOT_NULL(s);

    s = twom_strerror(TWOM_LOCKED);
    ASSERT_NOT_NULL(s);

    s = twom_strerror(TWOM_READONLY);
    ASSERT_NOT_NULL(s);

    /* unknown code should still return something */
    s = twom_strerror(-999);
    ASSERT_NOT_NULL(s);
}

/*
 * ============================================================
 * Additional coverage tests
 * ============================================================
 */

static void test_should_repack(void)
{
    struct twom_db *db = NULL;
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    /* empty db should not need repack */
    ASSERT(!twom_db_should_repack(db));

    /* store enough data to exceed MINREWRITE (16834 bytes) of dirty space */
    char key[32];
    char val[256];
    memset(val, 'x', sizeof(val));

    for (int i = 0; i < 200; i++) {
        int klen = snprintf(key, sizeof(key), "key-%04d", i);
        r = twom_db_store(db, key, klen, val, sizeof(val), 0);
        ASSERT_OK(r);
    }

    /* still shouldn't need repack - no dirty space yet */
    ASSERT(!twom_db_should_repack(db));

    /* now delete all records to create dirty space */
    for (int i = 0; i < 200; i++) {
        int klen = snprintf(key, sizeof(key), "key-%04d", i);
        r = twom_db_store(db, key, klen, NULL, 0, 0);
        ASSERT_OK(r);
    }

    /* should now recommend repack (dirty_size > MINREWRITE and
     * current_size < 4 * dirty_size) */
    ASSERT(twom_db_should_repack(db));

    /* repack should clear the dirty space */
    r = twom_db_repack(db);
    ASSERT_OK(r);

    ASSERT(!twom_db_should_repack(db));

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static void test_nonblocking(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;

    /* create a database */
    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    CANSTORE("key", 3, "value", 5);
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
    db = NULL;

    /* set up pipes */
    int p2c[2], c2p[2];
    r = pipe(p2c);
    ASSERT_EQ(r, 0);
    r = pipe(c2p);
    ASSERT_EQ(r, 0);

    pid_t pid = fork();
    ASSERT(pid >= 0);

    if (pid == 0) {
        /* === CHILD === */
        close(p2c[1]);
        close(c2p[0]);

        wait_for_peer(p2c[0]);

        /* try to open with NONBLOCKING - the open itself takes a read lock,
         * which conflicts with the parent's write lock. With NONBLOCKING
         * this should fail immediately with TWOM_LOCKED */
        struct twom_open_data cinit = TWOM_OPEN_DATA_INITIALIZER;
        cinit.flags = TWOM_NONBLOCKING;
        struct twom_db *cdb = NULL;

        int cr = twom_db_open(filename, &cinit, &cdb, NULL);
        int got_locked = (cr == TWOM_LOCKED);

        if (cdb) {
            cr = twom_db_close(&cdb);
            assert(cr == TWOM_OK);
        }

        /* send result: 'Y' = got locked as expected, 'N' = didn't */
        char result = got_locked ? 'Y' : 'N';
        ssize_t n = write(c2p[1], &result, 1);
        assert(n == 1);

        wait_for_peer(p2c[0]);

        close(p2c[0]);
        close(c2p[1]);
        _exit(0);
    }

    /* === PARENT === */
    close(p2c[0]);
    close(c2p[1]);

    /* open db and hold a write lock */
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    r = twom_db_begin_txn(db, 0, &txn);
    ASSERT_OK(r);

    /* signal child to try its nonblocking lock, then read result */
    signal_peer(p2c[1]);

    char result;
    ssize_t n = read(c2p[0], &result, 1);
    ASSERT_EQ(n, 1);
    ASSERT_EQ(result, 'Y');

    /* clean up */
    r = twom_txn_abort(&txn);
    ASSERT_OK(r);

    r = twom_db_close(&db);
    ASSERT_OK(r);

    signal_peer(p2c[1]);

    int status;
    waitpid(pid, &status, 0);
    ASSERT(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);

    close(p2c[1]);
    close(c2p[0]);
}

static int alwaysyield_cb(void *rock, const char *key, size_t keylen,
                           const char *data, size_t datalen)
{
    (void)key; (void)keylen; (void)data; (void)datalen;
    int *count = (int *)rock;
    (*count)++;
    return 0;
}

static void test_alwaysyield(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    /* store several records */
    CANSTORE("a", 1, "1", 1);
    CANSTORE("b", 1, "2", 1);
    CANSTORE("c", 1, "3", 1);
    CANSTORE("d", 1, "4", 1);
    CANSTORE("e", 1, "5", 1);
    CANCOMMIT();

    /* iterate with ALWAYSYIELD - should still visit all records */
    int count = 0;
    r = twom_db_foreach(db, NULL, 0, NULL, alwaysyield_cb, &count,
                        TWOM_ALWAYSYIELD);
    ASSERT_OK(r);
    ASSERT_EQ(count, 5);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static void test_open_with_txn(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;

    /* open with tidptr to get a write transaction immediately */
    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, &txn);
    ASSERT_OK(r);
    ASSERT_NOT_NULL(db);
    ASSERT_NOT_NULL(txn);

    /* use the returned txn directly */
    r = twom_txn_store(txn, "key1", 4, "val1", 4, 0);
    ASSERT_OK(r);
    r = twom_txn_store(txn, "key2", 4, "val2", 4, 0);
    ASSERT_OK(r);

    r = twom_txn_commit(&txn);
    ASSERT_OK(r);

    /* verify data */
    CANFETCH("key1", 4, "val1", 4);
    CANFETCH("key2", 4, "val2", 4);
    CANCOMMIT();

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static int goodp_filter(void *rock, const char *key, size_t keylen,
                         const char *data, size_t datalen)
{
    (void)rock; (void)data; (void)datalen;
    /* only accept keys starting with 'b' */
    return (keylen > 0 && key[0] == 'b');
}

struct foreach_result {
    int count;
    char keys[10][32];
};

static int collect_cb(void *rock, const char *key, size_t keylen,
                       const char *data, size_t datalen)
{
    (void)data; (void)datalen;
    struct foreach_result *res = (struct foreach_result *)rock;
    if (res->count < 10 && keylen < 32) {
        memcpy(res->keys[res->count], key, keylen);
        res->keys[res->count][keylen] = '\0';
    }
    res->count++;
    return 0;
}

static void test_foreach_goodp(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;

    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    CANSTORE("apple", 5, "1", 1);
    CANSTORE("banana", 6, "2", 1);
    CANSTORE("blueberry", 9, "3", 1);
    CANSTORE("cherry", 6, "4", 1);
    CANSTORE("boysenberry", 11, "5", 1);
    CANCOMMIT();

    /* foreach with goodp filter - only keys starting with 'b' */
    struct foreach_result res = { 0, {{0}} };
    r = twom_db_foreach(db, NULL, 0, goodp_filter, collect_cb, &res, 0);
    ASSERT_OK(r);
    ASSERT_EQ(res.count, 3);
    ASSERT_STR_EQ(res.keys[0], "banana");
    ASSERT_STR_EQ(res.keys[1], "blueberry");
    ASSERT_STR_EQ(res.keys[2], "boysenberry");

    /* without filter - should get all 5 */
    memset(&res, 0, sizeof(res));
    r = twom_db_foreach(db, NULL, 0, NULL, collect_cb, &res, 0);
    ASSERT_OK(r);
    ASSERT_EQ(res.count, 5);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

static void test_error_cases(void)
{
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;
    int r;

    /* open nonexistent file without CREATE should fail */
    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_EQ(r, TWOM_NOTFOUND);
    ASSERT_NULL(db);

    /* create the db for remaining tests */
    init.flags = TWOM_CREATE;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    CANSTORE("key", 3, "val", 3);
    CANCOMMIT();

    /* close and close again (double-close should be safe) */
    r = twom_db_close(&db);
    ASSERT_OK(r);
    ASSERT_NULL(db);

    r = twom_db_close(&db);
    ASSERT_OK(r);

    /* fetch from non-existent key */
    init.flags = 0;
    r = twom_db_open(filename, &init, &db, NULL);
    ASSERT_OK(r);

    CANNOTFETCH_NOTXN("nokey", 5, TWOM_NOTFOUND);

    /* FETCHNEXT past last key */
    CANNOTFETCHNEXT("key", 3, TWOM_NOTFOUND);

    /* abort the txn that CANNOTFETCHNEXT auto-began */
    r = twom_txn_abort(&txn);
    ASSERT_OK(r);

    r = twom_db_close(&db);
    ASSERT_OK(r);
}

/*
 * ============================================================
 * Test runner
 * ============================================================
 */

struct test_entry {
    const char *name;
    void (*func)(void);
};

static struct test_entry tests[] = {
    { "test_openclose",          test_openclose },
    { "test_multiopen",          test_multiopen },
    { "test_read_and_delete",    test_read_and_delete },
    { "test_replace_before_delete", test_replace_before_delete },
    { "test_opentwo",            test_opentwo },
    { "test_readwrite",          test_readwrite },
    { "test_multirw",            test_multirw },
    { "test_readwrite_zerolen",  test_readwrite_zerolen },
    { "test_readwrite_null",     test_readwrite_null },
    { "test_abort",              test_abort },
    { "test_delete",             test_delete },
    { "test_mboxlist",           test_mboxlist },
    { "test_foreach_nullkey",    test_foreach_nullkey },
    { "test_foreach",            test_foreach },
    { "test_foreach_changes",    test_foreach_changes },
    { "test_binary_keys",        test_binary_keys },
    { "test_binary_data",        test_binary_data },
    { "test_many",               test_many },
    { "test_foreach_replace",    test_foreach_replace },
    { "test_cursor_basic",       test_cursor_basic },
    { "test_cursor_replace",     test_cursor_replace },
    { "test_cursor_txn",         test_cursor_txn },
    { "test_mvcc_write_while_reading",      test_mvcc_write_while_reading },
    { "test_mvcc_delete_while_reading",     test_mvcc_delete_while_reading },
    { "test_mvcc_create_delete_invisible",  test_mvcc_create_delete_invisible },
    { "test_repack",             test_repack },
    { "test_metadata",           test_metadata },
    { "test_readonly",           test_readonly },
    { "test_conditional_store",  test_conditional_store },
    { "test_nosync",             test_nosync },
    { "test_nocheck",            test_nocheck },
    { "test_sync",               test_sync },
    { "test_dump",               test_dump },
    { "test_txn_yield",          test_txn_yield },
    { "test_strerror",           test_strerror },
    { "test_should_repack",      test_should_repack },
    { "test_nonblocking",        test_nonblocking },
    { "test_alwaysyield",        test_alwaysyield },
    { "test_open_with_txn",      test_open_with_txn },
    { "test_foreach_goodp",      test_foreach_goodp },
    { "test_error_cases",        test_error_cases },
    { NULL, NULL }
};

int main(int argc, char **argv)
{
    const char *filter = NULL;
    if (argc > 1) filter = argv[1];

    for (struct test_entry *t = tests; t->name; t++) {
        if (filter && !strstr(t->name, filter)) continue;

        total_tests++;
        current_test_failed = 0;
        cb_failures = 0;

        if (setup() != 0) {
            fprintf(stderr, "  FAIL: setup failed for %s\n", t->name);
            total_failed++;
            teardown();
            continue;
        }

        fprintf(stderr, "  %-30s ", t->name);
        t->func();

        if (current_test_failed || cb_failures) {
            fprintf(stderr, "FAIL\n");
            total_failed++;
        } else {
            fprintf(stderr, "ok\n");
            total_passed++;
        }

        teardown();
    }

    fprintf(stderr, "\n%d tests: %d passed, %d failed, %d skipped\n",
            total_tests, total_passed, total_failed, total_skipped);

    return total_failed ? 1 : 0;
}
