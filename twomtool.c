/* twomtool.c -- standalone CLI tool for twom databases
 *
 * Available under any of: CC0-1.0, 0BSD, or MIT-0
 * See LICENSE-CC0, LICENSE-0BSD, or LICENSE-MIT-0 for details.
 */

#include <errno.h>
#include <getopt.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "twom.h"

#define STACKSIZE 64000
static char stack[STACKSIZE + 1];

static int read_key_value(char **keyptr, size_t *keylen,
                          char **valptr, size_t *vallen)
{
    int c, res, inkey;
    res = 0;
    inkey = 1;
    *keyptr = stack;
    *keylen = 0;
    *vallen = 0;
    while ((c = getchar()) != EOF) {
        if (c == '\n') break;
        if ((c == '\t') && inkey) {
            inkey = 0;
            *valptr = stack + *keylen + 1;
        } else {
            if (inkey) {
                (*keyptr)[(*keylen)++] = c;
                res = 1;
            } else {
                (*valptr)[(*vallen)++] = c;
            }
        }
        if (*keylen + *vallen >= STACKSIZE - 1) {
            fprintf(stderr, "Error, stack overflow\n");
            exit(1);
        }
    }
    (*keyptr)[*keylen] = '\0';
    if (inkey) {
        *valptr = *keyptr + *keylen;
    } else {
        (*valptr)[*vallen] = '\0';
    }
    return res;
}

static int printer_cb(void *rock,
                      const char *key, size_t keylen,
                      const char *data, size_t datalen)
{
    (void)rock;
    fwrite(key, 1, keylen, stdout);
    fputc('\t', stdout);
    fwrite(data, 1, datalen, stdout);
    fputc('\n', stdout);
    return 0;
}

/* recursive mkdir -p for the parent directory of a path */
static int mkdir_p(const char *path)
{
    char buf[PATH_MAX];
    char *slash;

    snprintf(buf, sizeof(buf), "%s", path);

    /* find the last slash to get the parent directory */
    slash = strrchr(buf, '/');
    if (!slash || slash == buf) return 0;
    *slash = '\0';

    /* try to create, and if it fails because parent doesn't exist, recurse */
    if (mkdir(buf, 0755) == 0) return 0;
    if (errno == EEXIST) return 0;
    if (errno != ENOENT) return -1;

    /* parent doesn't exist, recurse */
    if (mkdir_p(buf) != 0) return -1;
    return mkdir(buf, 0755);
}

static void batch_commands(struct twom_db *db)
{
    char line[STACKSIZE + 1];
    struct twom_txn *txn = NULL;
    int lineno = 0;

    while (fgets(line, sizeof(line), stdin)) {
        lineno++;
        /* strip trailing newline */
        size_t len = strlen(line);
        if (len > 0 && line[len - 1] == '\n') line[--len] = '\0';
        if (len > 0 && line[len - 1] == '\r') line[--len] = '\0';

        if (len == 0) continue;

        /* parse command\tkey\tvalue */
        char *cmd = line;
        char *key = NULL;
        char *val = NULL;
        size_t keylen = 0, vallen = 0;

        char *tab1 = strchr(cmd, '\t');
        if (tab1) {
            *tab1 = '\0';
            key = tab1 + 1;
            char *tab2 = strchr(key, '\t');
            if (tab2) {
                *tab2 = '\0';
                val = tab2 + 1;
                vallen = strlen(val);
            }
            keylen = strlen(key);
        }

        int r = 0;

        if (!strcmp(cmd, "BEGIN")) {
            if (txn) {
                fprintf(stderr, "ERROR: line %d: already in transaction\n", lineno);
                goto fail;
            }
            r = twom_db_begin_txn(db, 0, &txn);
            if (r) {
                fprintf(stderr, "ERROR: line %d: BEGIN: %s\n", lineno, twom_strerror(r));
                goto fail;
            }
        }
        else if (!strcmp(cmd, "COMMIT")) {
            if (!txn) {
                fprintf(stderr, "ERROR: line %d: not in transaction\n", lineno);
                goto fail;
            }
            r = twom_txn_commit(&txn);
            if (r) {
                fprintf(stderr, "ERROR: line %d: COMMIT: %s\n", lineno, twom_strerror(r));
                goto fail;
            }
        }
        else if (!strcmp(cmd, "ABORT")) {
            if (!txn) {
                fprintf(stderr, "ERROR: line %d: not in transaction\n", lineno);
                goto fail;
            }
            r = twom_txn_abort(&txn);
            if (r) {
                fprintf(stderr, "ERROR: line %d: ABORT: %s\n", lineno, twom_strerror(r));
                goto fail;
            }
        }
        else if (!strcmp(cmd, "GET")) {
            if (!key) {
                fprintf(stderr, "ERROR: line %d: GET requires a key\n", lineno);
                goto fail;
            }
            const char *data = NULL;
            size_t datalen = 0;
            if (txn) {
                r = twom_txn_fetch(txn, key, keylen, NULL, NULL,
                                   &data, &datalen, 0);
            } else {
                r = twom_db_fetch(db, key, keylen, NULL, NULL,
                                  &data, &datalen, 0);
            }
            if (r == TWOM_NOTFOUND) {
                /* silently skip, matching cyr_dbtool behavior */
            } else if (r) {
                fprintf(stderr, "ERROR: line %d: GET: %s\n", lineno, twom_strerror(r));
                goto fail;
            } else {
                fwrite(key, 1, keylen, stdout);
                fputc('\t', stdout);
                fwrite(data, 1, datalen, stdout);
                fputc('\n', stdout);
                fflush(stdout);
            }
        }
        else if (!strcmp(cmd, "SET")) {
            if (!key || !val) {
                fprintf(stderr, "ERROR: line %d: SET requires key and value\n", lineno);
                goto fail;
            }
            if (txn) {
                r = twom_txn_store(txn, key, keylen, val, vallen, 0);
            } else {
                r = twom_db_store(db, key, keylen, val, vallen, 0);
            }
            if (r) {
                fprintf(stderr, "ERROR: line %d: SET: %s\n", lineno, twom_strerror(r));
                goto fail;
            }
        }
        else if (!strcmp(cmd, "DELETE")) {
            if (!key) {
                fprintf(stderr, "ERROR: line %d: DELETE requires a key\n", lineno);
                goto fail;
            }
            if (txn) {
                r = twom_txn_store(txn, key, keylen, NULL, 0, 0);
            } else {
                r = twom_db_store(db, key, keylen, NULL, 0, 0);
            }
            if (r) {
                fprintf(stderr, "ERROR: line %d: DELETE: %s\n", lineno, twom_strerror(r));
                goto fail;
            }
        }
        else if (!strcmp(cmd, "SHOW")) {
            /* key is optional prefix */
            const char *prefix = key ? key : "";
            size_t prefixlen = key ? keylen : 0;
            if (txn) {
                r = twom_txn_foreach(txn, prefix, prefixlen,
                                     NULL, printer_cb, NULL, 0);
            } else {
                r = twom_db_foreach(db, prefix, prefixlen,
                                    NULL, printer_cb, NULL, 0);
            }
            if (r) {
                fprintf(stderr, "ERROR: line %d: SHOW: %s\n", lineno, twom_strerror(r));
                goto fail;
            }
            fflush(stdout);
        }
        else {
            fprintf(stderr, "ERROR: line %d: unknown command '%s'\n", lineno, cmd);
            goto fail;
        }
    }

    if (txn) {
        int r = twom_txn_commit(&txn);
        if (r) {
            fprintf(stderr, "ERROR: final COMMIT: %s\n", twom_strerror(r));
        }
    }
    return;

fail:
    if (txn) twom_txn_abort(&txn);
}

static void usage(const char *progname)
{
    fprintf(stderr, "Usage: %s [options] <db file> <action> [<key>] [<value>]\n", progname);
    fprintf(stderr, "\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  -n, --create          create the database if it doesn't exist\n");
    fprintf(stderr, "  -R, --readonly        open the database readonly\n");
    fprintf(stderr, "  -N, --no-checksum     disable checksums\n");
    fprintf(stderr, "  -S, --no-sync         don't fsync writes (dangerous)\n");
    fprintf(stderr, "  -T, --use-transaction use a single transaction for the action\n");
    fprintf(stderr, "  -t, --no-transaction  don't use a transaction (default)\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "Actions:\n");
    fprintf(stderr, "  show [<prefix>]   list all entries (or those matching prefix)\n");
    fprintf(stderr, "  get <key>         fetch and print value\n");
    fprintf(stderr, "  set <key> <value> store key/value pair\n");
    fprintf(stderr, "  delete <key>      delete key\n");
    fprintf(stderr, "  dump [<level>]    internal format dump\n");
    fprintf(stderr, "  consistent        check database consistency\n");
    fprintf(stderr, "  repack            repack/compact the database\n");
    fprintf(stderr, "  damage            write then crash (recovery testing)\n");
    fprintf(stderr, "  batch             batch mode from stdin\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "Keys/values can be provided on stdin (key<tab>value per line)\n");
}

int main(int argc, char *argv[])
{
    const char *fname;
    const char *action;
    char *key = NULL;
    char *value = NULL;
    int r;
    size_t keylen = 0, vallen = 0;
    int loop;
    int use_txn = 0;
    uint32_t open_flags = 0;
    struct twom_db *db = NULL;
    struct twom_txn *txn = NULL;

    static const char short_options[] = "NRSTnt";

    static const struct option long_options[] = {
        { "no-checksum",     no_argument, NULL, 'N' },
        { "create",          no_argument, NULL, 'n' },
        { "readonly",        no_argument, NULL, 'R' },
        { "no-sync",         no_argument, NULL, 'S' },
        { "use-transaction", no_argument, NULL, 'T' },
        { "no-transaction",  no_argument, NULL, 't' },
        { 0, 0, 0, 0 },
    };

    int opt;
    while (-1 != (opt = getopt_long(argc, argv, short_options,
                                    long_options, NULL)))
    {
        switch (opt) {
        case 'N':
            open_flags |= TWOM_NOCSUM | TWOM_CSUM_NULL;
            break;
        case 'n':
            open_flags |= TWOM_CREATE;
            break;
        case 'R':
            open_flags |= TWOM_SHARED;
            break;
        case 'S':
            open_flags |= TWOM_NOSYNC;
            break;
        case 'T':
            use_txn = 1;
            break;
        case 't':
            use_txn = 0;
            break;
        default:
            usage(argv[0]);
            return 1;
        }
    }

    if ((argc - optind) < 2) {
        usage(argv[0]);
        return 1;
    }

    fname = argv[optind];
    action = argv[optind + 1];

    if (fname[0] != '/') {
        fprintf(stderr, "\nPlease use absolute pathnames.\n\n");
        return 1;
    }

    /* open the database */
    struct twom_open_data init = TWOM_OPEN_DATA_INITIALIZER;
    init.flags = open_flags;

    r = twom_db_open(fname, &init, &db, use_txn ? &txn : NULL);
    if (r == TWOM_NOTFOUND && (open_flags & TWOM_CREATE)) {
        /* try creating parent directories */
        if (mkdir_p(fname) == 0) {
            r = twom_db_open(fname, &init, &db, use_txn ? &txn : NULL);
        }
    }
    if (r) {
        fprintf(stderr, "can't open database %s: %s\n", fname, twom_strerror(r));
        return 1;
    }

    int is_get = !strcmp(action, "get");
    int is_set = !strcmp(action, "set");
    int is_delete = !strcmp(action, "delete");

    if (is_get || is_set || is_delete) {
        int use_stdin = ((argc - optind) < 3);
        if (use_stdin) {
            loop = read_key_value(&key, &keylen, &value, &vallen);
        } else {
            key = argv[optind + 2];
            keylen = strlen(key);
            if (is_set) {
                if ((argc - optind) < 4) {
                    fprintf(stderr, "set requires a value\n");
                    twom_db_close(&db);
                    return 1;
                }
                value = argv[optind + 3];
                vallen = strlen(value);
            }
            loop = 1;
        }
        while (loop) {
            if (is_get) {
                const char *res = NULL;
                size_t reslen = 0;
                if (txn) {
                    r = twom_txn_fetch(txn, key, keylen, NULL, NULL,
                                       &res, &reslen, 0);
                } else {
                    r = twom_db_fetch(db, key, keylen, NULL, NULL,
                                      &res, &reslen, 0);
                }
                if (r) break;
                fwrite(key, 1, keylen, stdout);
                fputc('\t', stdout);
                fwrite(res, 1, reslen, stdout);
                fputc('\n', stdout);
            } else if (is_set) {
                if (txn) {
                    r = twom_txn_store(txn, key, keylen, value, vallen, 0);
                } else {
                    r = twom_db_store(db, key, keylen, value, vallen, 0);
                }
                if (r) break;
            } else if (is_delete) {
                /* force=1: silent if not found */
                if (txn) {
                    r = twom_txn_store(txn, key, keylen, NULL, 0, 0);
                } else {
                    r = twom_db_store(db, key, keylen, NULL, 0, 0);
                }
                if (r) break;
            }

            loop = 0;
            if (use_stdin) {
                loop = read_key_value(&key, &keylen, &value, &vallen);
            }
        }
    } else if (!strcmp(action, "batch")) {
        batch_commands(db);
    } else if (!strcmp(action, "show")) {
        const char *prefix = "";
        size_t prefixlen = 0;
        if ((argc - optind) >= 3) {
            prefix = argv[optind + 2];
            prefixlen = strlen(prefix);
        }
        if (txn) {
            r = twom_txn_foreach(txn, prefix, prefixlen,
                                 NULL, printer_cb, NULL, 0);
        } else {
            r = twom_db_foreach(db, prefix, prefixlen,
                                NULL, printer_cb, NULL, 0);
        }
    } else if (!strcmp(action, "dump")) {
        int level = 1;
        if ((argc - optind) > 2)
            level = atoi(argv[optind + 2]);
        r = twom_db_dump(db, level);
    } else if (!strcmp(action, "consistent")) {
        r = twom_db_check_consistency(db);
        if (r) {
            printf("No, not consistent\n");
        } else {
            printf("Yes, consistent\n");
        }
    } else if (!strcmp(action, "repack")) {
        r = twom_db_repack(db);
    } else if (!strcmp(action, "damage")) {
        if (!txn) {
            r = twom_db_begin_txn(db, 0, &txn);
            if (r) {
                fprintf(stderr, "begin txn failed: %s\n", twom_strerror(r));
                twom_db_close(&db);
                return 1;
            }
        }
        twom_txn_store(txn, "INVALID", 7, "CRASHME", 7, 0);
        /* don't commit - just exit to simulate crash */
        _exit(0);
    } else {
        fprintf(stderr, "Unknown action: %s\n", action);
        r = 1;
    }

    if (txn) {
        if (r) {
            fprintf(stderr, "ABORTING: %s\n", twom_strerror(r));
            int r2 = twom_txn_abort(&txn);
            if (r2)
                fprintf(stderr, "ERROR ON ABORT: %s\n", twom_strerror(r2));
        } else {
            int r2 = twom_txn_commit(&txn);
            if (r2)
                fprintf(stderr, "ERROR ON COMMIT: %s\n", twom_strerror(r2));
        }
    }

    twom_db_close(&db);

    return r ? 1 : 0;
}
