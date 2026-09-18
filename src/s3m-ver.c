/*
 * s3m-ver — version & delete-marker manager
 *
 * Part of s3m: Parallel S3 Object Manager
 *
 * Lists every stored version and delete marker in a bucket or prefix
 * (ListObjectVersions), and prunes them: keep the newest N non-current
 * versions per key (--keep), drop versions older than an age
 * (--older-than), and clean up delete markers (--markers,
 * --only-markers). Pruning is a dry run unless --apply is given.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "s3mcore.h"

#include <errno.h>
#include <getopt.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define S3M_VER_VERSION "1.0.0"

static struct {
    long        keep;            /* non-current versions to keep; -1 off */
    int64_t     older;           /* age threshold in seconds; -1 off     */
    bool        markers;         /* clean up orphaned delete markers     */
    bool        only_markers;    /* delete only delete markers           */
    bool        latest;          /* --older-than may take the current    */
    bool        apply;
    bool        prune;           /* any pruning criteria given           */
    int         nthreads;
    int         shard_depth;
    const char *outpath;
    bool        quiet;
    bool        suppress;
    bool        progress;
    bool        rrdns;
} g = { .keep = -1, .older = -1, .nthreads = 16, .shard_depth = 2 };

static s3m_endpoint_pool endpoints;

static _Atomic uint64_t n_keys;
static _Atomic uint64_t n_versions;   /* real versions scanned          */
static _Atomic uint64_t n_markers;    /* delete markers scanned         */
static _Atomic uint64_t n_prune;      /* candidates for deletion        */
static _Atomic uint64_t n_deleted;    /* apply mode: confirmed deleted  */
static _Atomic uint64_t n_bytes;      /* reclaimable bytes              */

static time_t now;

static s3m_stack stk;
static s3m_sink  sink;

/* ------------------------------------------------------------------ */
/* per-key version groups                                               */
/* ------------------------------------------------------------------ */

struct ventry {
    char     *vid;
    uint64_t  size;
    time_t    mtime;
    bool      latest, marker;
};

struct wctx {
    s3m_http     *h;
    s3m_outbuf    ob;
    s3m_delbatch  batch;
    const char   *bucket;
    char          curkey[1100];
    struct ventry *ents;         /* newest first, as listed             */
    size_t         n, cap;
};

static void emit_row(s3m_outbuf *ob, const char *key,
                     const struct ventry *e, const char *result)
{
    if (!s3m_ob_room(ob, 2 * strlen(key) + strlen(e->vid) + 320)) {
        s3m_note_error(key, "emit", "row too long");
        return;
    }
    char mt[32] = "";
    if (e->mtime != (time_t)-1)
        s3m_fmt_time(e->mtime, mt);
    s3m_ob_csv(ob, key);
    s3m_ob_putc(ob, ',');
    s3m_ob_csv(ob, e->vid);
    s3m_ob_fmt(ob, ",%c,%c,%llu,%s,", e->latest ? 'y' : 'n',
               e->marker ? 'y' : 'n', (unsigned long long)e->size, mt);
    s3m_ob_csv(ob, result);
    s3m_ob_putc(ob, '\n');
}

static void on_deleted(void *ctx, const char *key, const char *vid,
                       bool ok, const char *code, const char *msg)
{
    struct wctx *w = ctx;
    struct ventry e = { .vid = (char *)vid, .mtime = (time_t)-1 };
    if (ok) {
        atomic_fetch_add_explicit(&n_deleted, 1, memory_order_relaxed);
        if (!g.suppress)
            emit_row(&w->ob, key, &e, "deleted");
    } else {
        char res[512];
        snprintf(res, sizeof res, "failed: %s: %s", code, msg);
        s3m_note_error(key, "delete", res + 8);
        if (!g.suppress)
            emit_row(&w->ob, key, &e, res);
    }
}

/* decide the fate of one key's version stack and act on it */
static void process_group(struct wctx *w)
{
    if (w->n == 0)
        return;
    atomic_fetch_add_explicit(&n_keys, 1, memory_order_relaxed);

    size_t nvers = 0;
    for (size_t i = 0; i < w->n; i++)
        if (!w->ents[i].marker)
            nvers++;

    /* pass 1: which real versions go? (newest-first rank over the
     * non-current versions; the current version only goes when
     * --latest and --older-than both say so) */
    bool *del = calloc(w->n, sizeof *del);
    if (!del)
        goto out;
    size_t rank = 0, ndelvers = 0;
    for (size_t i = 0; i < w->n; i++) {
        struct ventry *e = &w->ents[i];
        if (e->marker)
            continue;
        bool eligible;
        int64_t age = (e->mtime == (time_t)-1) ? -1
                    : (int64_t)(now - e->mtime);
        if (e->latest) {
            eligible = g.latest && g.older >= 0 && age > g.older;
        } else {
            bool ok_keep = (g.keep < 0) || (rank >= (size_t)g.keep);
            bool ok_old  = (g.older < 0) || (age > g.older);
            /* with no version criteria at all (marker-only runs),
             * real versions are never eligible */
            eligible = ok_keep && ok_old &&
                       (g.keep >= 0 || g.older >= 0);
            rank++;
        }
        if (g.only_markers)
            eligible = false;
        if (eligible) {
            del[i] = true;
            ndelvers++;
        }
    }

    /* pass 2: markers — --only-markers takes them all; --markers takes
     * them when the key ends up with no versions at all */
    if (g.only_markers || (g.markers && ndelvers == nvers)) {
        for (size_t i = 0; i < w->n; i++)
            if (w->ents[i].marker)
                del[i] = true;
    }

    for (size_t i = 0; i < w->n; i++) {
        if (!del[i])
            continue;
        struct ventry *e = &w->ents[i];
        atomic_fetch_add_explicit(&n_prune, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&n_bytes, e->size, memory_order_relaxed);
        if (!g.apply) {
            if (!g.suppress)
                emit_row(&w->ob, w->curkey, e, "pending");
        } else {
            s3m_delbatch_add(&w->batch, w->h, w->curkey, e->vid);
        }
    }
    free(del);
out:
    for (size_t i = 0; i < w->n; i++)
        free(w->ents[i].vid);
    w->n = 0;
}

static void on_obj(void *ctx, const s3m_obj *o)
{
    struct wctx *w = ctx;

    if (o->is_marker)
        atomic_fetch_add_explicit(&n_markers, 1, memory_order_relaxed);
    else {
        atomic_fetch_add_explicit(&n_versions, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&n_bytes, g.prune ? 0 : o->size,
                                  memory_order_relaxed);
    }
    s3m_set_current(o->key);

    if (!g.prune) {
        /* listing mode: one row per version, no grouping needed */
        if (g.suppress)
            return;
        s3m_outbuf *ob = &w->ob;
        if (!s3m_ob_room(ob, 2 * strlen(o->key) + 512)) {
            s3m_note_error(o->key, "emit", "row too long");
            return;
        }
        char mt[32] = "";
        if (o->mtime != (time_t)-1)
            s3m_fmt_time(o->mtime, mt);
        s3m_ob_csv(ob, o->key);
        s3m_ob_putc(ob, ',');
        s3m_ob_csv(ob, o->version_id);
        s3m_ob_fmt(ob, ",%c,%c,%llu,%s,", o->is_latest ? 'y' : 'n',
                   o->is_marker ? 'y' : 'n',
                   (unsigned long long)o->size, mt);
        s3m_ob_csv(ob, o->etag);
        s3m_ob_putc(ob, ',');
        s3m_ob_puts(ob, o->storclass[0] ? o->storclass : "STANDARD");
        s3m_ob_putc(ob, '\n');
        return;
    }

    /* prune mode: gather this key's stack, flushing the previous one */
    if (strcmp(w->curkey, o->key) != 0) {
        process_group(w);
        snprintf(w->curkey, sizeof w->curkey, "%s", o->key);
    }
    if (w->n == w->cap) {
        size_t nc = w->cap ? w->cap * 2 : 64;
        struct ventry *ne = realloc(w->ents, nc * sizeof *ne);
        if (!ne) {
            s3m_note_error(o->key, "group", "out of memory");
            return;
        }
        w->ents = ne;
        w->cap = nc;
    }
    struct ventry *e = &w->ents[w->n];
    e->vid    = strdup(o->version_id);
    e->size   = o->size;
    e->mtime  = o->mtime;
    e->latest = o->is_latest;
    e->marker = o->is_marker;
    if (!e->vid) {
        s3m_note_error(o->key, "group", "out of memory");
        return;
    }
    w->n++;
}

/* ------------------------------------------------------------------ */
/* workers                                                              */
/* ------------------------------------------------------------------ */

static void *worker(void *arg)
{
    int idx = (int)(intptr_t)arg;
    struct wctx w;
    memset(&w, 0, sizeof w);
    w.h = s3m_http_new();
    if (!w.h || s3m_ob_init(&w.ob, &sink) != 0) {
        s3m_note_error("worker", "startup", "out of memory");
        s3m_http_free(w.h);
        char *j;
        while ((j = s3m_stack_pop(&stk)) != NULL)
            free(j);
        return NULL;
    }
    if (g.rrdns)
        s3m_http_pin_endpoint(w.h, &endpoints, (size_t)idx);

    char *job;
    while ((job = s3m_stack_pop(&stk)) != NULL) {
        char *bucket, *prefix;
        int depth = s3m_job_parse(job, &bucket, &prefix, NULL);
        if (depth >= 0) {
            w.bucket = bucket;
            w.curkey[0] = '\0';
            s3m_delbatch_init(&w.batch, bucket, on_deleted, &w);
            s3m_list_job(w.h, &stk, bucket, prefix, depth, g.shard_depth,
                         0, true, false, on_obj, &w);
            process_group(&w);            /* flush the final key group */
            s3m_delbatch_flush(&w.batch, w.h);
        }
        free(job);
        if (g.rrdns && s3m_http_transport_failed(w.h))
            s3m_http_rotate_endpoint(w.h, &endpoints);
    }
    s3m_ob_flush(&w.ob);
    s3m_ob_free(&w.ob);
    free(w.ents);
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
    return atomic_load_explicit(&n_versions, memory_order_relaxed) +
           atomic_load_explicit(&n_markers, memory_order_relaxed);
}

static void prog_draw(double rate, int frame)
{
    uint64_t vers = atomic_load_explicit(&n_versions, memory_order_relaxed);
    uint64_t mks  = atomic_load_explicit(&n_markers, memory_order_relaxed);
    uint64_t pru  = atomic_load_explicit(&n_prune, memory_order_relaxed);
    uint64_t del  = atomic_load_explicit(&n_deleted, memory_order_relaxed);
    uint64_t errs = atomic_load_explicit(&s3m_nerrors, memory_order_relaxed);
    uint64_t byt  = atomic_load_explicit(&n_bytes, memory_order_relaxed);

    char cur[2048];
    s3m_get_current(cur, sizeof cur);
    char ptr[512];
    int pmax = s3m_term_width() - 13;
    if (pmax > 500)
        pmax = 500;
    if (pmax < 20)
        pmax = 20;
    s3m_trunc_left(cur, (size_t)pmax, ptr, sizeof ptr);

    char vv[32], mv[32], pv[32], dv[32], ev[32], rv[32], bv[32], el[32];
    s3m_fmt_u64(vers, vv);
    s3m_fmt_u64(mks, mv);
    s3m_fmt_u64(pru, pv);
    s3m_fmt_u64(del, dv);
    s3m_fmt_u64(errs, ev);
    s3m_fmt_u64((uint64_t)(rate + 0.5), rv);
    s3m_fmt_size(byt, bv);
    s3m_fmt_elapsed(s3m_mono_now() - t_start, el);

    char ratestr[48];
    snprintf(ratestr, sizeof ratestr, "%s ver/s", rv);

    const char *action = !g.prune ? "list"
                       : g.apply ? "apply" : "dry run";

    char buf[4096];
    size_t off = 0;
#define ADD(...) off += (size_t)snprintf(buf + off, sizeof buf - off, __VA_ARGS__)
    ADD("\x1b[K%s%s%s %ss3m-ver%s %s— version manager (%s)%s\n",
        C_CYAN, s3m_spinner[frame % 10], C_RESET, C_BOLD, C_RESET,
        C_DIM, action, C_RESET);
    ADD("\x1b[K  %s%-9s%s %s\n", C_DIM, "key", C_RESET, ptr);
    ADD("\x1b[K  %s%-9s%s %s\n", C_DIM, "output", C_RESET,
        g.outpath ? g.outpath : "none (-q)");
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s\n",
        C_DIM, "versions", C_RESET, vv, C_DIM, "markers", C_RESET, mv);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s\n",
        C_DIM, g.prune ? "to delete" : "rate", C_RESET,
        g.prune ? pv : ratestr,
        C_DIM, g.apply ? "deleted" : "errors", C_RESET,
        g.apply ? dv : ev);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s%s%s\n",
        C_DIM, g.prune ? "reclaim" : "size", C_RESET, bv,
        C_DIM, "errors", C_RESET,
        errs ? C_RED : "", ev, errs ? C_RESET : "");
    ADD("\x1b[K  %s%-9s%s %-14d %s%-8s%s %s\n",
        C_DIM, "threads", C_RESET, g.nthreads,
        C_DIM, "elapsed", C_RESET, el);
#undef ADD
    fwrite(buf, 1, off, stderr);
}

/* ------------------------------------------------------------------ */
/* summary                                                              */
/* ------------------------------------------------------------------ */

static void print_summary(double elapsed)
{
    uint64_t keys = atomic_load(&n_keys);
    uint64_t vers = atomic_load(&n_versions);
    uint64_t mks  = atomic_load(&n_markers);
    uint64_t pru  = atomic_load(&n_prune);
    uint64_t del  = atomic_load(&n_deleted);
    uint64_t errs = atomic_load(&s3m_nerrors);
    uint64_t byt  = atomic_load(&n_bytes);

    char kv[32], vv[32], mv[32], pv[32], dv[32], ev[32], bv[32], el[32];
    s3m_fmt_u64(keys, kv);
    s3m_fmt_u64(vers, vv);
    s3m_fmt_u64(mks, mv);
    s3m_fmt_u64(pru, pv);
    s3m_fmt_u64(del, dv);
    s3m_fmt_u64(errs, ev);
    s3m_fmt_size(byt, bv);
    s3m_fmt_elapsed(elapsed, el);

    if (!g.prune) {
        fprintf(stderr,
                "%s✓%s %ss3m-ver%s complete — %s version%s · %s marker%s · "
                "%s%s error%s%s · %s\n  in %s",
                C_GREEN, C_RESET, C_BOLD, C_RESET, vv,
                vers == 1 ? "" : "s", mv, mks == 1 ? "" : "s",
                errs ? C_RED : "", ev, errs == 1 ? "" : "s",
                errs ? C_RESET : "", bv, el);
    } else {
        fprintf(stderr,
                "%s✓%s %ss3m-ver%s complete — %s · %s keys · %s versions · "
                "%s markers\n  %s %s",
                C_GREEN, C_RESET, C_BOLD, C_RESET,
                g.apply ? "apply" : "dry run", kv, vv, mv,
                g.apply ? dv : pv,
                g.apply ? "deleted" : "to delete");
        if (g.apply && del != pru)
            fprintf(stderr, " (of %s)", pv);
        fprintf(stderr, " · %s reclaim%s · %s%s error%s%s · in %s",
                bv, g.apply ? "ed" : "able",
                errs ? C_RED : "", ev, errs == 1 ? "" : "s",
                errs ? C_RESET : "", el);
    }
    if (g.outpath)
        fprintf(stderr, " → %s", g.outpath);
    fputc('\n', stderr);
    if (g.prune && !g.apply && pru)
        fprintf(stderr, "  %sdry run — nothing was removed; add --apply "
                "to delete%s\n", C_BOLD, C_RESET);
}

/* ------------------------------------------------------------------ */
/* argument parsing / main                                              */
/* ------------------------------------------------------------------ */

static void usage(FILE *to)
{
    fputs(
"Usage: s3m-ver [OPTIONS] s3://BUCKET[/PREFIX]...\n"
"\n"
"List and manage object versions and delete markers. With no pruning\n"
"options, lists every stored version as CSV. With pruning options this\n"
"is a DRY RUN listing what would be deleted; add --apply to delete.\n"
"\n"
"Pruning options:\n"
"      --keep N          per key, keep the newest N non-current versions\n"
"                        (in addition to the current version) and delete\n"
"                        the rest; --keep 0 keeps only the current version\n"
"      --older-than DUR  only delete versions older than DUR (30d, 12h,\n"
"                        45m, 4w); combined with --keep, a version must\n"
"                        fail BOTH tests to be deleted\n"
"      --markers         also delete a key's delete markers when the key\n"
"                        is left with no versions at all (tombstone\n"
"                        cleanup)\n"
"      --only-markers    delete nothing but delete markers — removing a\n"
"                        latest marker undeletes the object\n"
"      --latest          allow --older-than to delete the current version\n"
"                        too (the previous version becomes current)\n"
"      --apply           actually delete (otherwise: dry run)\n"
"\n"
"Other options:\n"
"  -j, --threads N       worker threads, 1-256 (default: 16)\n"
"      --shard-depth N   prefix levels to expand for parallelism, 0-9\n"
"                        (default: 2)\n"
"      --rrdns           resolve the endpoint hostname to every A/AAAA\n"
"                        address it has and spread worker threads across\n"
"                        them (round robin), moving a thread to the next\n"
"                        address if its current one starts failing\n"
"  -o, --output FILE     write the CSV to FILE; show live progress\n"
"  -q, --quiet           suppress the console listing\n"
"  -h, --help            show this help and exit\n"
"  -V, --version         show version and exit\n",
    to);
    fputs(s3m_common_usage, to);
}

int main(int argc, char **argv)
{
    static const struct option lopts[] = {
        { "keep",         required_argument, NULL, 1001 },
        { "older-than",   required_argument, NULL, 1002 },
        { "markers",      no_argument,       NULL, 1003 },
        { "only-markers", no_argument,       NULL, 1004 },
        { "latest",       no_argument,       NULL, 1005 },
        { "apply",        no_argument,       NULL, 1006 },
        { "threads",      required_argument, NULL, 'j' },
        { "shard-depth",  required_argument, NULL, 1007 },
        { "rrdns",        no_argument,       NULL, 1008 },
        { "output",       required_argument, NULL, 'o' },
        { "quiet",        no_argument,       NULL, 'q' },
        { "help",         no_argument,       NULL, 'h' },
        { "version",      no_argument,       NULL, 'V' },
        S3M_COMMON_LOPTS,
        { 0, 0, 0, 0 }
    };

    bool latest_flag = false;
    int c;
    while ((c = getopt_long(argc, argv, "j:o:qhV", lopts, NULL)) != -1) {
        if (s3m_common_opt(c, optarg))
            continue;
        switch (c) {
        case 1001: {
            char *end;
            long v = strtol(optarg, &end, 10);
            if (*end || v < 0 || v > 1000000) {
                fprintf(stderr, "s3m-ver: invalid --keep '%s'\n", optarg);
                return 2;
            }
            g.keep = v;
            break;
        }
        case 1002:
            if (s3m_parse_dur(optarg, &g.older) != 0) {
                fprintf(stderr, "s3m-ver: invalid duration '%s' "
                        "(expected e.g. 30d, 12h, 45m, 4w)\n", optarg);
                return 2;
            }
            break;
        case 1003:
            g.markers = true;
            break;
        case 1004:
            g.only_markers = true;
            break;
        case 1005:
            latest_flag = true;
            break;
        case 1006:
            g.apply = true;
            break;
        case 'j': {
            char *end;
            long v = strtol(optarg, &end, 10);
            if (*end || v < 1 || v > 256) {
                fprintf(stderr, "s3m-ver: invalid thread count '%s' "
                        "(expected 1-256)\n", optarg);
                return 2;
            }
            g.nthreads = (int)v;
            break;
        }
        case 1007: {
            char *end;
            long v = strtol(optarg, &end, 10);
            if (*end || v < 0 || v > 9) {
                fprintf(stderr, "s3m-ver: invalid shard depth '%s' "
                        "(expected 0-9)\n", optarg);
                return 2;
            }
            g.shard_depth = (int)v;
            break;
        }
        case 1008:
            g.rrdns = true;
            break;
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
            printf("s3m-ver %s (s3m: Parallel S3 Object Manager)\n",
                   S3M_VER_VERSION);
            return 0;
        default:
            usage(stderr);
            return 2;
        }
    }

    g.latest = latest_flag;
    g.prune = (g.keep >= 0) || (g.older >= 0) || g.markers ||
              g.only_markers;

    if (optind >= argc) {
        fprintf(stderr, "s3m-ver: no s3:// URI given\n");
        usage(stderr);
        return 2;
    }
    if (!g.prune && g.apply) {
        fprintf(stderr, "s3m-ver: --apply given without any pruning "
                "option (--keep, --older-than, --markers, "
                "--only-markers)\n");
        return 2;
    }
    if (g.latest && g.older < 0) {
        fprintf(stderr, "s3m-ver: --latest only makes sense together "
                "with --older-than\n");
        return 2;
    }
    if (g.only_markers && (g.keep >= 0 || g.older >= 0)) {
        fprintf(stderr, "s3m-ver: --only-markers cannot be combined with "
                "--keep/--older-than (use --markers)\n");
        return 2;
    }

    s3m_color = isatty(STDERR_FILENO);
    g.suppress = g.quiet && !g.outpath;
    g.progress = (g.outpath || g.quiet) && isatty(STDERR_FILENO);
    now = time(NULL);

    if (s3m_config_finalize("s3m-ver") != 0)
        return 2;
    if (g.rrdns && s3m_endpoint_pool_init(&endpoints, "s3m-ver") != 0)
        return 2;
    if (s3m_http_global_init() != 0) {
        fprintf(stderr, "s3m-ver: could not initialise libcurl\n");
        return 2;
    }

    FILE *out;
    if (g.outpath) {
        out = fopen(g.outpath, "w");
        if (!out) {
            fprintf(stderr, "s3m-ver: cannot open '%s': %s\n",
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
        fputs(g.prune
              ? "key,version_id,latest,marker,size,mtime,result\n"
              : "key,version_id,latest,marker,size,mtime,etag,class\n",
              out);

    /* seed the queue, de-duplicating overlapping prefixes so no key's
     * version stack is ever processed twice */
    s3m_stack_init(&stk, g.nthreads);
    char *ukeys[64];
    char  ubuckets[64][256];
    int   nuris = 0;
    for (int i = optind; i < argc && nuris < 64; i++) {
        if (s3m_uri_parse("s3m-ver", argv[i], ubuckets[nuris],
                          &ukeys[nuris]) != 0)
            return 2;
        bool covered = false;
        for (int j = 0; j < nuris && !covered; j++)
            covered = !strcmp(ubuckets[j], ubuckets[nuris]) &&
                      !strncmp(ukeys[j], ukeys[nuris], strlen(ukeys[j]));
        if (!covered)
            s3m_push_job(&stk, ubuckets[nuris], ukeys[nuris], 0, 0);
        nuris++;
    }
    for (int i = 0; i < nuris; i++)
        free(ukeys[i]);

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

    pthread_t *tids = calloc((size_t)g.nthreads, sizeof *tids);
    if (!tids) {
        fprintf(stderr, "s3m-ver: out of memory\n");
        return 2;
    }
    int started = 0;
    for (int i = 0; i < g.nthreads; i++) {
        if (pthread_create(&tids[i], NULL, worker,
                           (void *)(intptr_t)i) != 0)
            break;
        started++;
    }
    if (started < g.nthreads) {
        if (started == 0) {
            fprintf(stderr, "s3m-ver: could not create any worker "
                    "threads\n");
            return 2;
        }
        s3m_stack_set_threads(&stk, started);
    }
    for (int i = 0; i < started; i++)
        pthread_join(tids[i], NULL);
    free(tids);

    double elapsed = s3m_mono_now() - t_start;

    if (g.progress)
        s3m_progress_stop();

    if (fflush(out) != 0 || ferror(out))
        atomic_store(&sink.failed, true);
    if (g.outpath && fclose(out) != 0)
        atomic_store(&sink.failed, true);

    if (atomic_load(&sink.failed)) {
        fprintf(stderr, "s3m-ver: %swrite error%s on %s — output is "
                "incomplete\n", C_RED, C_RESET,
                g.outpath ? g.outpath : "stdout");
        return 1;
    }

    if (g.prune || g.outpath || g.quiet)
        print_summary(elapsed);
    s3m_print_errors();
    s3m_stack_destroy(&stk);
    if (g.rrdns)
        s3m_endpoint_pool_destroy(&endpoints);

    return atomic_load(&s3m_nerrors) ? 1 : 0;
}
