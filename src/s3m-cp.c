/*
 * s3m-cp — parallel server-side copy
 *
 * Part of s3m: Parallel S3 Object Manager
 *
 * Copies objects between buckets/prefixes entirely server-side
 * (CopyObject, multipart UploadPartCopy above 5 GiB) with the s3m
 * safety-first design: dry run by default, existing destinations
 * skipped unless --overwrite, and --move for copy-then-delete renames.
 *
 * Three phases, like s3m-sync: index the destination (skipped with
 * --overwrite), list the sources building the plan, execute the plan
 * across the worker pool.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "s3mcore.h"

#include <errno.h>
#include <getopt.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define S3M_CP_VERSION "1.0.0"

struct target {
    char  bucket[256];
    char *key;                /* prefix (ends '/' or "") or exact key   */
    bool  is_prefix;
    char *dst_exact;          /* exact sources: the resolved dest key   */
};

static struct target targets[64];
static int           ntargets;

static char  dst_bucket[256];
static char *dst_prefix;      /* "" or ends with '/'                    */
static bool  dst_is_key;      /* rename-style: single source -> one key */

static struct {
    bool        apply;
    bool        overwrite;
    bool        move;
    int         nthreads;
    int         shard_depth;
    const char *outpath;
    bool        quiet;
    bool        suppress;
    bool        progress;
} g = { .nthreads = 16, .shard_depth = 2 };

static _Atomic uint64_t n_indexed;
static _Atomic uint64_t n_scanned;
static _Atomic uint64_t n_exists;     /* skipped: destination exists    */
static _Atomic uint64_t n_tocopy;
static _Atomic uint64_t n_copied, n_deleted;
static _Atomic uint64_t bytes_tocopy, bytes_done;
static _Atomic int      cur_phase;    /* 1 index · 2 plan · 3 copy      */

static s3m_stack stk;
static s3m_sink  sink;

/* ------------------------------------------------------------------ */
/* destination key set                                                  */
/* ------------------------------------------------------------------ */

struct dkey {
    char        *key;
    struct dkey *next;
};

#define IDX_BUCKETS 65536
static struct dkey    *idx[IDX_BUCKETS];
static pthread_mutex_t idx_mu = PTHREAD_MUTEX_INITIALIZER;

static uint64_t str_hash(const char *s)
{
    uint64_t h = 1469598103934665603ULL;
    for (; *s; s++) {
        h ^= (unsigned char)*s;
        h *= 1099511628211ULL;
    }
    return h;
}

/* returns true if the key was newly added (false = already present) */
static bool set_add(struct dkey **set, pthread_mutex_t *mu,
                    const char *key)
{
    uint64_t h = str_hash(key) & (IDX_BUCKETS - 1);
    pthread_mutex_lock(mu);
    for (struct dkey *d = set[h]; d; d = d->next) {
        if (!strcmp(d->key, key)) {
            pthread_mutex_unlock(mu);
            return false;
        }
    }
    struct dkey *d = malloc(sizeof *d);
    if (d) {
        d->key = strdup(key);
        if (!d->key) {
            free(d);
            d = NULL;
        }
    }
    if (d) {
        d->next = set[h];
        set[h] = d;
    }
    pthread_mutex_unlock(mu);
    return d != NULL;
}

static bool set_has(struct dkey **set, const char *key)
{
    for (struct dkey *d = set[str_hash(key) & (IDX_BUCKETS - 1)]; d;
         d = d->next)
        if (!strcmp(d->key, key))
            return true;
    return false;
}

/* planned destination keys, to de-duplicate overlapping sources */
static struct dkey    *planned[IDX_BUCKETS];
static pthread_mutex_t planned_mu = PTHREAD_MUTEX_INITIALIZER;

/* ------------------------------------------------------------------ */
/* action plan                                                          */
/* ------------------------------------------------------------------ */

struct action {
    int       tgt;            /* source target index                    */
    char     *skey;           /* full source key                        */
    char     *dkey;           /* full destination key                   */
    uint64_t  size;
};

static struct action  *acts;
static size_t          nacts, cacts;
static pthread_mutex_t acts_mu = PTHREAD_MUTEX_INITIALIZER;
static _Atomic size_t  next_act;

static void act_add(int tgt, const char *skey, const char *dkey,
                    uint64_t size)
{
    pthread_mutex_lock(&acts_mu);
    if (nacts == cacts) {
        size_t nc = cacts ? cacts * 2 : 1024;
        struct action *na = realloc(acts, nc * sizeof *na);
        if (!na) {
            pthread_mutex_unlock(&acts_mu);
            s3m_note_error(skey, "plan", "out of memory");
            return;
        }
        acts = na;
        cacts = nc;
    }
    struct action *a = &acts[nacts];
    a->tgt = tgt;
    a->skey = strdup(skey);
    a->dkey = strdup(dkey);
    a->size = size;
    if (!a->skey || !a->dkey) {
        free(a->skey);
        free(a->dkey);
        pthread_mutex_unlock(&acts_mu);
        s3m_note_error(skey, "plan", "out of memory");
        return;
    }
    nacts++;
    pthread_mutex_unlock(&acts_mu);

    atomic_fetch_add_explicit(&n_tocopy, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&bytes_tocopy, size, memory_order_relaxed);
}

/* ------------------------------------------------------------------ */
/* CSV rows                                                             */
/* ------------------------------------------------------------------ */

static void emit_row(s3m_outbuf *ob, const char *skey, const char *dkey,
                     uint64_t size, const char *result)
{
    if (g.suppress)
        return;
    if (!s3m_ob_room(ob, 2 * (strlen(skey) + strlen(dkey) +
                              strlen(result)) + 256)) {
        s3m_note_error(skey, "emit", "row too long");
        return;
    }
    s3m_ob_csv(ob, skey);
    s3m_ob_putc(ob, ',');
    s3m_ob_csv(ob, dkey);
    s3m_ob_fmt(ob, ",%llu,", (unsigned long long)size);
    s3m_ob_csv(ob, result);
    s3m_ob_putc(ob, '\n');
}

/* ------------------------------------------------------------------ */
/* phase 1: index destination keys                                      */
/* ------------------------------------------------------------------ */

static void on_index_obj(void *ctx, const s3m_obj *o)
{
    (void)ctx;
    s3m_set_current(o->key);
    if (set_add(idx, &idx_mu, o->key))
        atomic_fetch_add_explicit(&n_indexed, 1, memory_order_relaxed);
}

/* ------------------------------------------------------------------ */
/* phase 2: plan                                                        */
/* ------------------------------------------------------------------ */

struct wctx {
    s3m_http    *h;
    s3m_outbuf   ob;
    s3m_delbatch batch;
    int          tag;
};

static void on_src_obj(void *ctx, const s3m_obj *o)
{
    struct wctx *w = ctx;
    struct target *t = &targets[w->tag];

    atomic_fetch_add_explicit(&n_scanned, 1, memory_order_relaxed);
    s3m_set_current(o->key);

    char dkey[2200];
    if (t->is_prefix) {
        if (strncmp(o->key, t->key, strlen(t->key)) != 0)
            return;
        const char *rel = o->key + strlen(t->key);
        if (!rel[0] || rel[strlen(rel) - 1] == '/')
            return;                   /* skip placeholder objects */
        snprintf(dkey, sizeof dkey, "%s%s", dst_prefix, rel);
    } else {
        if (strcmp(o->key, t->key) != 0)
            return;
        snprintf(dkey, sizeof dkey, "%s", t->dst_exact);
    }

    /* skip existing destinations unless --overwrite */
    if (!g.overwrite && set_has(idx, dkey)) {
        atomic_fetch_add_explicit(&n_exists, 1, memory_order_relaxed);
        emit_row(&w->ob, o->key, dkey, o->size, "exists");
        return;
    }
    /* de-duplicate: overlapping sources can map the same destination */
    if (!set_add(planned, &planned_mu, dkey))
        return;

    act_add(w->tag, o->key, dkey, o->size);
}

static void *plan_worker(void *arg)
{
    bool indexing = arg != NULL;
    struct wctx w;
    w.h = s3m_http_new();
    if (!w.h || s3m_ob_init(&w.ob, &sink) != 0) {
        s3m_note_error("worker", "startup", "out of memory");
        s3m_http_free(w.h);
        char *j;
        while ((j = s3m_stack_pop(&stk)) != NULL)
            free(j);
        return NULL;
    }
    char *job;
    while ((job = s3m_stack_pop(&stk)) != NULL) {
        char *bucket, *prefix;
        int tag;
        int depth = s3m_job_parse(job, &bucket, &prefix, &tag);
        if (depth >= 0) {
            w.tag = tag;
            s3m_list_job(w.h, &stk, bucket, prefix, depth, g.shard_depth,
                         tag, false, false,
                         indexing ? on_index_obj : on_src_obj, &w);
        }
        free(job);
    }
    s3m_ob_flush(&w.ob);
    s3m_ob_free(&w.ob);
    s3m_http_free(w.h);
    return NULL;
}

static int run_pool(void *(*fn)(void *), void *arg)
{
    pthread_t tids[256];
    int started = 0;
    for (int i = 0; i < g.nthreads; i++) {
        if (pthread_create(&tids[i], NULL, fn, arg) != 0)
            break;
        started++;
    }
    if (started == 0)
        return -1;
    if (started < g.nthreads)
        s3m_stack_set_threads(&stk, started);
    for (int i = 0; i < started; i++)
        pthread_join(tids[i], NULL);
    return 0;
}

/* ------------------------------------------------------------------ */
/* phase 3: execute                                                     */
/* ------------------------------------------------------------------ */

static void on_deleted(void *ctx, const char *key, const char *vid,
                       bool ok, const char *code, const char *msg)
{
    struct wctx *w = ctx;
    (void)vid;
    if (ok) {
        atomic_fetch_add_explicit(&n_deleted, 1, memory_order_relaxed);
    } else {
        char res[400];
        snprintf(res, sizeof res, "%s: %s", code, msg);
        s3m_note_error(key, "move-delete", res);
        emit_row(&w->ob, key, "", 0, "move-delete failed");
    }
}

static void *exec_worker(void *arg)
{
    (void)arg;
    struct wctx w;
    w.h = s3m_http_new();
    if (!w.h || s3m_ob_init(&w.ob, &sink) != 0) {
        s3m_note_error("worker", "startup", "out of memory");
        s3m_http_free(w.h);
        return NULL;
    }
    int batch_tgt = -1;

    for (;;) {
        size_t i = atomic_fetch_add(&next_act, 1);
        if (i >= nacts)
            break;
        struct action *a = &acts[i];
        struct target *t = &targets[a->tgt];
        s3m_set_current(a->skey);

        char err[320] = "";
        if (s3m_copy_object(w.h, t->bucket, a->skey, dst_bucket, a->dkey,
                            a->size, &bytes_done, err,
                            sizeof err) == 0) {
            atomic_fetch_add_explicit(&n_copied, 1, memory_order_relaxed);
            emit_row(&w.ob, a->skey, a->dkey, a->size,
                     g.move ? "moved" : "copied");
            if (g.move) {
                /* the delete batch is per source bucket */
                if (batch_tgt >= 0 &&
                    strcmp(targets[batch_tgt].bucket, t->bucket) != 0)
                    s3m_delbatch_flush(&w.batch, w.h);
                if (batch_tgt < 0 ||
                    strcmp(targets[batch_tgt].bucket, t->bucket) != 0)
                    s3m_delbatch_init(&w.batch, t->bucket, on_deleted,
                                      &w);
                batch_tgt = a->tgt;
                s3m_delbatch_add(&w.batch, w.h, a->skey, NULL);
            }
        } else {
            char res[400];
            snprintf(res, sizeof res, "failed: %s", err);
            s3m_note_error(a->skey, "copy", err);
            emit_row(&w.ob, a->skey, a->dkey, a->size, res);
        }
    }
    if (g.move && batch_tgt >= 0)
        s3m_delbatch_flush(&w.batch, w.h);
    s3m_ob_flush(&w.ob);
    s3m_ob_free(&w.ob);
    s3m_http_free(w.h);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* progress display                                                     */
/* ------------------------------------------------------------------ */

#define PROG_LINES 7

static double t_start;

static uint64_t prog_items(void)
{
    return atomic_load_explicit(&bytes_done, memory_order_relaxed);
}

static void prog_draw(double rate, int frame)
{
    static const char *phases[] = { "", "index destination", "plan",
                                    "copy" };
    int ph = atomic_load_explicit(&cur_phase, memory_order_relaxed);
    uint64_t nidx = atomic_load_explicit(&n_indexed, memory_order_relaxed);
    uint64_t nsc  = atomic_load_explicit(&n_scanned, memory_order_relaxed);
    uint64_t ncp  = atomic_load_explicit(&n_copied, memory_order_relaxed);
    uint64_t tcp  = atomic_load_explicit(&n_tocopy, memory_order_relaxed);
    uint64_t nex  = atomic_load_explicit(&n_exists, memory_order_relaxed);
    uint64_t bd   = atomic_load_explicit(&bytes_done, memory_order_relaxed);
    uint64_t bt   = atomic_load_explicit(&bytes_tocopy, memory_order_relaxed);
    uint64_t errs = atomic_load_explicit(&s3m_nerrors, memory_order_relaxed);

    char cur[2048];
    s3m_get_current(cur, sizeof cur);
    char ptr[512];
    int pmax = s3m_term_width() - 13;
    if (pmax > 500)
        pmax = 500;
    if (pmax < 20)
        pmax = 20;
    s3m_trunc_left(cur, (size_t)pmax, ptr, sizeof ptr);

    char iv[32], sv[32], cv[32], tv[32], xv[32], ev[32], bdv[32],
         btv[32], rv[32], el[32];
    s3m_fmt_u64(nidx, iv);
    s3m_fmt_u64(nsc, sv);
    s3m_fmt_u64(ncp, cv);
    s3m_fmt_u64(tcp, tv);
    s3m_fmt_u64(nex, xv);
    s3m_fmt_u64(errs, ev);
    s3m_fmt_size(bd, bdv);
    s3m_fmt_size(bt, btv);
    s3m_fmt_size((uint64_t)(rate + 0.5), rv);
    s3m_fmt_elapsed(s3m_mono_now() - t_start, el);

    char copystr[80], bytestr[80], ratestr[48];
    snprintf(copystr, sizeof copystr, "%s / %s", cv, tv);
    snprintf(bytestr, sizeof bytestr, "%s / %s", bdv, btv);
    snprintf(ratestr, sizeof ratestr, "%s/s", rv);

    char buf[4096];
    size_t off = 0;
#define ADD(...) off += (size_t)snprintf(buf + off, sizeof buf - off, __VA_ARGS__)
    ADD("\x1b[K%s%s%s %ss3m-cp%s %s— parallel server-side %s (%s)%s\n",
        C_CYAN, s3m_spinner[frame % 10], C_RESET, C_BOLD, C_RESET,
        C_DIM, g.move ? "move" : "copy",
        g.apply ? "apply" : "dry run", C_RESET);
    ADD("\x1b[K  %s%-9s%s %s\n", C_DIM, "key", C_RESET, ptr);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %d\n",
        C_DIM, "phase", C_RESET, phases[ph],
        C_DIM, "threads", C_RESET, g.nthreads);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s\n",
        C_DIM, "indexed", C_RESET, iv, C_DIM, "scanned", C_RESET, sv);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s\n",
        C_DIM, "copied", C_RESET, copystr, C_DIM, "exists", C_RESET, xv);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s%s%s\n",
        C_DIM, "bytes", C_RESET, bytestr, C_DIM, "errors", C_RESET,
        errs ? C_RED : "", ev, errs ? C_RESET : "");
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s\n",
        C_DIM, "rate", C_RESET, ratestr, C_DIM, "elapsed", C_RESET, el);
#undef ADD
    fwrite(buf, 1, off, stderr);
}

/* ------------------------------------------------------------------ */
/* summary                                                              */
/* ------------------------------------------------------------------ */

static void print_summary(double elapsed)
{
    uint64_t nsc  = atomic_load(&n_scanned);
    uint64_t nex  = atomic_load(&n_exists);
    uint64_t ncp  = atomic_load(&n_copied);
    uint64_t tcp  = atomic_load(&n_tocopy);
    uint64_t ndl  = atomic_load(&n_deleted);
    uint64_t bt   = atomic_load(&bytes_tocopy);
    uint64_t bd   = atomic_load(&bytes_done);
    uint64_t errs = atomic_load(&s3m_nerrors);

    char sv[32], xv[32], cv[32], tv[32], dv[32], ev[32], bv[32],
         el[32], rv[32];
    s3m_fmt_u64(nsc, sv);
    s3m_fmt_u64(nex, xv);
    s3m_fmt_u64(ncp, cv);
    s3m_fmt_u64(tcp, tv);
    s3m_fmt_u64(ndl, dv);
    s3m_fmt_u64(errs, ev);
    s3m_fmt_size(g.apply ? bd : bt, bv);
    s3m_fmt_elapsed(elapsed, el);
    s3m_fmt_size(elapsed > 0 ? (uint64_t)((double)bd / elapsed) : 0, rv);

    fprintf(stderr,
            "%s✓%s %ss3m-cp%s complete — %s %s · %s scanned",
            C_GREEN, C_RESET, C_BOLD, C_RESET,
            g.move ? "move" : "copy", g.apply ? "apply" : "dry run", sv);
    if (g.apply) {
        fprintf(stderr, " · %s", cv);
        if (ncp != tcp)
            fprintf(stderr, " of %s", tv);
        fprintf(stderr, " copied");
        if (g.move)
            fprintf(stderr, " · %s source%s deleted", dv,
                    ndl == 1 ? "" : "s");
    } else {
        fprintf(stderr, " · %s to copy", tv);
    }
    if (nex)
        fprintf(stderr, " · %s skipped (exists)", xv);
    fprintf(stderr, " · %s%s error%s%s\n", errs ? C_RED : "", ev,
            errs == 1 ? "" : "s", errs ? C_RESET : "");
    fprintf(stderr, "  %s in %s", bv, el);
    if (g.apply && bd)
        fprintf(stderr, " (%s/s server-side)", rv);
    if (g.outpath)
        fprintf(stderr, " → %s", g.outpath);
    fputc('\n', stderr);
    if (!g.apply && tcp)
        fprintf(stderr, "  %sdry run — nothing was copied; add --apply "
                "to %s%s\n", C_BOLD, g.move ? "move" : "copy", C_RESET);
    if (nex && !g.overwrite)
        fprintf(stderr, "  existing destinations were skipped — pass "
                "--overwrite to replace them\n");
}

/* ------------------------------------------------------------------ */
/* argument parsing / main                                              */
/* ------------------------------------------------------------------ */

static void usage(FILE *to)
{
    fputs(
"Usage: s3m-cp [OPTIONS] s3://SRC/KEY|PREFIX/... s3://DST[/PREFIX[/]]\n"
"\n"
"Copy objects between buckets/prefixes entirely server-side — no data\n"
"flows through the client. By default this is a DRY RUN listing what\n"
"would be copied; add --apply to copy.\n"
"\n"
"Source and destination rules (aws-cli-style):\n"
"  prefix source `a/b/`  -> its contents land under DST/PREFIX/\n"
"  exact key + DST `.../`-> DST/PREFIX/<basename of the key>\n"
"  exact key + DST key   -> exactly that key (rename-style copy;\n"
"                           only with a single source)\n"
"\n"
"Options:\n"
"      --apply           actually copy (per s3m convention there is no\n"
"                        --dry-run flag: that is the default state)\n"
"      --overwrite       replace existing destination objects; without\n"
"                        it they are skipped and reported ('exists')\n"
"      --move            delete each source object after its successful\n"
"                        copy (parallel rename); batched deletes\n"
"  -j, --threads N       worker threads, 1-256 (default: 16)\n"
"      --shard-depth N   prefix levels to expand for parallelism, 0-9\n"
"                        (default: 2)\n"
"  -o, --output FILE     write the CSV plan/report to FILE; show progress\n"
"  -q, --quiet           suppress the console listing (progress and the\n"
"                        summary are still shown)\n"
"  -h, --help            show this help and exit\n"
"  -V, --version         show version and exit\n",
    to);
    fputs(s3m_common_usage, to);
    fputs(
"\n"
"Objects over 5 GiB are copied with multipart UploadPartCopy. Both\n"
"buckets must be on the same endpoint and credentials. Copying a\n"
"prefix into itself is refused.\n",
    to);
}

int main(int argc, char **argv)
{
    static const struct option lopts[] = {
        { "apply",       no_argument,       NULL, 1001 },
        { "overwrite",   no_argument,       NULL, 1002 },
        { "move",        no_argument,       NULL, 1003 },
        { "threads",     required_argument, NULL, 'j' },
        { "shard-depth", required_argument, NULL, 1004 },
        { "output",      required_argument, NULL, 'o' },
        { "quiet",       no_argument,       NULL, 'q' },
        { "help",        no_argument,       NULL, 'h' },
        { "version",     no_argument,       NULL, 'V' },
        S3M_COMMON_LOPTS,
        { 0, 0, 0, 0 }
    };

    int c;
    while ((c = getopt_long(argc, argv, "j:o:qhV", lopts, NULL)) != -1) {
        if (s3m_common_opt(c, optarg))
            continue;
        switch (c) {
        case 1001:
            g.apply = true;
            break;
        case 1002:
            g.overwrite = true;
            break;
        case 1003:
            g.move = true;
            break;
        case 'j': {
            char *end;
            long v = strtol(optarg, &end, 10);
            if (*end || v < 1 || v > 256) {
                fprintf(stderr, "s3m-cp: invalid thread count '%s' "
                        "(expected 1-256)\n", optarg);
                return 2;
            }
            g.nthreads = (int)v;
            break;
        }
        case 1004: {
            char *end;
            long v = strtol(optarg, &end, 10);
            if (*end || v < 0 || v > 9) {
                fprintf(stderr, "s3m-cp: invalid shard depth '%s' "
                        "(expected 0-9)\n", optarg);
                return 2;
            }
            g.shard_depth = (int)v;
            break;
        }
        case 'o':
            g.outpath = optarg;
            break;
        case 'q':
            g.quiet = true;
            break;
        case 'h':
            usage(stdout);
            return 0;
        case 'V':
            printf("s3m-cp %s (s3m: Parallel S3 Object Manager)\n",
                   S3M_CP_VERSION);
            return 0;
        default:
            usage(stderr);
            return 2;
        }
    }

    int nargs = argc - optind;
    if (nargs < 2) {
        fprintf(stderr, "s3m-cp: expected SOURCE... DEST\n");
        usage(stderr);
        return 2;
    }
    if (nargs - 1 > 64) {
        fprintf(stderr, "s3m-cp: too many sources (max 64)\n");
        return 2;
    }

    /* ---- destination ---- */
    char *dkey;
    if (s3m_uri_parse("s3m-cp", argv[argc - 1], dst_bucket, &dkey) != 0)
        return 2;
    size_t dl = strlen(dkey);
    dst_is_key = dl > 0 && dkey[dl - 1] != '/';
    if (dst_is_key) {
        dst_prefix = dkey;                    /* resolved per source */
    } else {
        dst_prefix = dkey;                    /* "" or ends with '/' */
    }

    /* ---- sources ---- */
    for (int i = optind; i < argc - 1; i++) {
        struct target *t = &targets[ntargets];
        char *key;
        if (s3m_uri_parse("s3m-cp", argv[i], t->bucket, &key) != 0)
            return 2;
        size_t kl = strlen(key);
        t->key = key;
        t->is_prefix = (kl == 0 || key[kl - 1] == '/');

        if (t->is_prefix && dst_is_key) {
            fprintf(stderr, "s3m-cp: prefix source '%s' needs a prefix "
                    "destination (end it with '/')\n", argv[i]);
            return 2;
        }
        if (!t->is_prefix) {
            if (dst_is_key) {
                if (nargs - 1 > 1) {
                    fprintf(stderr, "s3m-cp: multiple sources require "
                            "the destination to be a prefix "
                            "(end it with '/')\n");
                    return 2;
                }
                t->dst_exact = strdup(dst_prefix);
            } else {
                const char *base = strrchr(key, '/');
                base = base ? base + 1 : key;
                t->dst_exact = s3m_strdupf("%s%s", dst_prefix, base);
            }
            if (!t->dst_exact) {
                fprintf(stderr, "s3m-cp: out of memory\n");
                return 2;
            }
        }

        /* self-copy guards */
        if (!strcmp(t->bucket, dst_bucket)) {
            if (t->is_prefix && !dst_is_key &&
                !strncmp(dst_prefix, t->key, strlen(t->key))) {
                fprintf(stderr, "s3m-cp: refusing to copy '%s' into "
                        "itself\n", argv[i]);
                return 2;
            }
            if (!t->is_prefix && !strcmp(t->key, t->dst_exact)) {
                fprintf(stderr, "s3m-cp: '%s' and destination are the "
                        "same object\n", argv[i]);
                return 2;
            }
        }
        ntargets++;
    }

    s3m_color = isatty(STDERR_FILENO);
    g.suppress = g.quiet && !g.outpath;
    g.progress = (g.outpath || g.quiet) && isatty(STDERR_FILENO);

    if (s3m_config_finalize("s3m-cp") != 0)
        return 2;
    if (s3m_http_global_init() != 0) {
        fprintf(stderr, "s3m-cp: could not initialise libcurl\n");
        return 2;
    }

    FILE *out;
    if (g.outpath) {
        out = fopen(g.outpath, "w");
        if (!out) {
            fprintf(stderr, "s3m-cp: cannot open '%s': %s\n",
                    g.outpath, strerror(errno));
            return 2;
        }
    } else {
        out = stdout;
    }
    static char outvbuf[1 << 20];
    setvbuf(out, outvbuf, _IOFBF, sizeof outvbuf);
    s3m_sink_init(&sink, out);

    if (!g.suppress)
        fputs("src_key,dst_key,size,result\n", out);

    if (g.progress)
        s3m_set_current("…");

    t_start = s3m_mono_now();

    if (g.progress) {
        s3m_progress_cfg cfg = {
            .draw = prog_draw, .items = prog_items, .lines = PROG_LINES
        };
        if (s3m_progress_start(&cfg) != 0)
            g.progress = false;
    }

    /* ---- phase 1: index existing destination keys ---- */
    atomic_store(&cur_phase, 1);
    if (!g.overwrite) {
        s3m_stack_init(&stk, g.nthreads);
        if (dst_is_key)
            s3m_push_job(&stk, dst_bucket, targets[0].dst_exact, 0, 0);
        else
            s3m_push_job(&stk, dst_bucket, dst_prefix, 0, 0);
        if (run_pool(plan_worker, (void *)1) != 0) {
            fprintf(stderr, "s3m-cp: could not start worker threads\n");
            return 2;
        }
        s3m_stack_destroy(&stk);
    }

    /* ---- phase 2: list the sources, building the plan ---- */
    atomic_store(&cur_phase, 2);
    s3m_stack_init(&stk, g.nthreads);
    for (int i = 0; i < ntargets; i++)
        s3m_push_job(&stk, targets[i].bucket, targets[i].key, 0, i);
    if (run_pool(plan_worker, NULL) != 0) {
        fprintf(stderr, "s3m-cp: could not start worker threads\n");
        return 2;
    }
    s3m_stack_destroy(&stk);

    /* ---- phase 3: report (dry run) or execute ---- */
    atomic_store(&cur_phase, 3);
    if (!g.apply) {
        s3m_outbuf ob;
        if (s3m_ob_init(&ob, &sink) == 0) {
            for (size_t i = 0; i < nacts; i++)
                emit_row(&ob, acts[i].skey, acts[i].dkey, acts[i].size,
                         "pending");
            s3m_ob_flush(&ob);
            s3m_ob_free(&ob);
        }
    } else if (nacts > 0) {
        pthread_t tids[256];
        int started = 0;
        for (int i = 0; i < g.nthreads; i++) {
            if (pthread_create(&tids[i], NULL, exec_worker, NULL) != 0)
                break;
            started++;
        }
        if (started == 0) {
            fprintf(stderr, "s3m-cp: could not start worker threads\n");
            return 2;
        }
        for (int i = 0; i < started; i++)
            pthread_join(tids[i], NULL);
    }

    double elapsed = s3m_mono_now() - t_start;

    if (g.progress)
        s3m_progress_stop();

    if (fflush(out) != 0 || ferror(out))
        atomic_store(&sink.failed, true);
    if (g.outpath && fclose(out) != 0)
        atomic_store(&sink.failed, true);

    if (atomic_load(&sink.failed)) {
        fprintf(stderr, "s3m-cp: %swrite error%s on %s — output is "
                "incomplete\n", C_RED, C_RESET,
                g.outpath ? g.outpath : "stdout");
        return 1;
    }

    print_summary(elapsed);
    s3m_print_errors();

    return atomic_load(&s3m_nerrors) ? 1 : 0;
}
