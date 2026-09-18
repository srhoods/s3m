/*
 * s3m-du — parallel object storage usage reporter
 *
 * Part of s3m: Parallel S3 Object Manager
 *
 * Aggregates object sizes and counts by prefix, du-style, using the
 * parallel sharded listing engine. Optionally includes non-current
 * versions and delete markers (--versions) and breaks totals down by
 * storage class (--by-class).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "s3mcore.h"

#include <errno.h>
#include <getopt.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define S3M_DU_VERSION "1.0.0"

static struct {
    int         nthreads;
    int         shard_depth;
    int         max_depth;       /* prefix levels to print; -1 = all   */
    bool        total;           /* -c                                 */
    bool        human;           /* -h (1024)                          */
    bool        si;              /* --si (1000)                        */
    bool        by_class;
    bool        versions;
    const char *outpath;
    bool        quiet;
    bool        progress;
    bool        rrdns;
} g = { .nthreads = 16, .shard_depth = 2, .max_depth = -1 };

static s3m_endpoint_pool endpoints;

static _Atomic uint64_t n_objs;
static _Atomic uint64_t n_bytes;
static _Atomic uint64_t n_markers;

static s3m_stack stk;

/* ------------------------------------------------------------------ */
/* per-worker prefix aggregation map                                    */
/* ------------------------------------------------------------------ */

struct agg {
    char       *name;            /* prefix, or prefix "\x01" class     */
    uint64_t    bytes, count;
    struct agg *next;
};

#define AGG_BUCKETS 4096

struct aggmap {
    struct agg *b[AGG_BUCKETS];
};

static uint64_t str_hash(const char *s)
{
    uint64_t h = 1469598103934665603ULL;
    for (; *s; s++) {
        h ^= (unsigned char)*s;
        h *= 1099511628211ULL;
    }
    return h;
}

static void agg_add(struct aggmap *m, const char *name,
                    uint64_t bytes, uint64_t count)
{
    struct agg **slot = &m->b[str_hash(name) & (AGG_BUCKETS - 1)];
    for (struct agg *a = *slot; a; a = a->next) {
        if (!strcmp(a->name, name)) {
            a->bytes += bytes;
            a->count += count;
            return;
        }
    }
    struct agg *a = malloc(sizeof *a);
    if (!a)
        return;
    a->name = strdup(name);
    if (!a->name) {
        free(a);
        return;
    }
    a->bytes = bytes;
    a->count = count;
    a->next = *slot;
    *slot = a;
}

static void agg_merge(struct aggmap *dst, struct aggmap *src)
{
    for (int i = 0; i < AGG_BUCKETS; i++) {
        for (struct agg *a = src->b[i]; a; ) {
            struct agg *nx = a->next;
            agg_add(dst, a->name, a->bytes, a->count);
            free(a->name);
            free(a);
            a = nx;
        }
        src->b[i] = NULL;
    }
}

/* ------------------------------------------------------------------ */
/* object accounting                                                    */
/* ------------------------------------------------------------------ */

struct wctx {
    struct aggmap map;
    const char   *bucket;
    const char   *root;          /* "bucket/keyprefix" this job is under */
    size_t        root_keylen;   /* length of the key part of the root   */
};

/* credit `o` to its root and every ancestor prefix within max_depth */
static void on_obj(void *ctx, const s3m_obj *o)
{
    struct wctx *w = ctx;

    atomic_fetch_add_explicit(&n_objs, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&n_bytes, o->size, memory_order_relaxed);
    if (o->is_marker)
        atomic_fetch_add_explicit(&n_markers, 1, memory_order_relaxed);
    s3m_set_current(o->key);

    const char *cls = o->storclass[0] ? o->storclass : "STANDARD";
    char name[2200];

    /* the root itself (depth 0) */
    if (g.by_class)
        snprintf(name, sizeof name, "%s\x01%s", w->root, cls);
    else
        snprintf(name, sizeof name, "%s", w->root);
    agg_add(&w->map, name, o->size, 1);

    if (g.max_depth == 0)
        return;

    /* ancestor prefixes below the root: first k path segments */
    const char *rel = o->key + w->root_keylen;
    int depth = 0;
    for (const char *p = rel; (p = strchr(p, '/')) != NULL; p++) {
        depth++;
        if (g.max_depth >= 0 && depth > g.max_depth)
            break;
        size_t plen = (size_t)(p - o->key) + 1;   /* include the '/' */
        if (plen > 2048)
            break;
        if (g.by_class)
            snprintf(name, sizeof name, "%s/%.*s\x01%s", w->bucket,
                     (int)plen, o->key, cls);
        else
            snprintf(name, sizeof name, "%s/%.*s", w->bucket,
                     (int)plen, o->key);
        agg_add(&w->map, name, o->size, 1);
    }
}

/* ------------------------------------------------------------------ */
/* workers                                                              */
/* ------------------------------------------------------------------ */

static struct aggmap    final_map;
static pthread_mutex_t  final_mu = PTHREAD_MUTEX_INITIALIZER;

/* roots are tracked so each object can be credited to the URI argument
 * it was found under; jobs carry the root via a parallel registry */
static char  *roots[64];
static size_t rootlens[64];
static size_t root_keylens[64];
static int    nroots;

static int root_for(const char *bucket, const char *prefix)
{
    /* find the registered root this job's prefix descends from */
    char full[2200];
    snprintf(full, sizeof full, "%s/%s", bucket, prefix);
    for (int i = 0; i < nroots; i++) {
        if (!strncmp(full, roots[i], rootlens[i]))
            return i;
    }
    return 0;
}

static void *worker(void *arg)
{
    int idx = (int)(intptr_t)arg;
    s3m_http *h = s3m_http_new();
    if (!h) {
        s3m_note_error("worker", "startup", "out of memory");
        char *j;
        while ((j = s3m_stack_pop(&stk)) != NULL)
            free(j);
        return NULL;
    }
    if (g.rrdns)
        s3m_http_pin_endpoint(h, &endpoints, (size_t)idx);
    struct wctx w;
    memset(&w.map, 0, sizeof w.map);

    char *job;
    while ((job = s3m_stack_pop(&stk)) != NULL) {
        char *bucket, *prefix;
        int depth = s3m_job_parse(job, &bucket, &prefix, NULL);
        if (depth >= 0) {
            int ri = root_for(bucket, prefix);
            w.bucket = bucket;
            w.root = roots[ri];
            w.root_keylen = root_keylens[ri];
            s3m_list_job(h, &stk, bucket, prefix, depth, g.shard_depth,
                         0, g.versions, false, on_obj, &w);
        }
        free(job);
        if (g.rrdns && s3m_http_transport_failed(h))
            s3m_http_rotate_endpoint(h, &endpoints);
    }
    pthread_mutex_lock(&final_mu);
    agg_merge(&final_map, &w.map);
    pthread_mutex_unlock(&final_mu);
    s3m_http_free(h);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* output                                                               */
/* ------------------------------------------------------------------ */

static void fmt_du_size(uint64_t b, char out[32])
{
    if (!g.human && !g.si) {
        snprintf(out, 32, "%llu", (unsigned long long)b);
        return;
    }
    uint64_t base = g.si ? 1000 : 1024;
    static const char *u1024[] = { "", "K", "M", "G", "T", "P" };
    static const char *u1000[] = { "", "k", "M", "G", "T", "P" };
    const char **u = g.si ? u1000 : u1024;
    double v = (double)b;
    int i = 0;
    while (v >= (double)base && i < 5) {
        v /= (double)base;
        i++;
    }
    if (i == 0)
        snprintf(out, 32, "%llu", (unsigned long long)b);
    else if (v < 10)
        snprintf(out, 32, "%.1f%s", v, u[i]);
    else
        snprintf(out, 32, "%.0f%s", v, u[i]);
}

static int agg_cmp(const void *a, const void *b)
{
    return strcmp((*(const struct agg **)a)->name,
                  (*(const struct agg **)b)->name);
}

static int print_report(FILE *out)
{
    /* flatten + sort */
    size_t n = 0;
    for (int i = 0; i < AGG_BUCKETS; i++)
        for (struct agg *a = final_map.b[i]; a; a = a->next)
            n++;
    struct agg **v = malloc(n * sizeof *v);
    if (!v && n) {
        fprintf(stderr, "s3m-du: out of memory\n");
        return -1;
    }
    size_t k = 0;
    for (int i = 0; i < AGG_BUCKETS; i++)
        for (struct agg *a = final_map.b[i]; a; a = a->next)
            v[k++] = a;
    qsort(v, n, sizeof *v, agg_cmp);

    char sz[32];
    for (size_t i = 0; i < n; i++) {
        char *cls = strchr(v[i]->name, '\x01');
        fmt_du_size(v[i]->bytes, sz);
        if (cls)
            fprintf(out, "%12s\ts3://%.*s\t%s\n", sz,
                    (int)(cls - v[i]->name), v[i]->name, cls + 1);
        else
            fprintf(out, "%12s\ts3://%s\n", sz, v[i]->name);
    }
    if (g.total) {
        fmt_du_size(atomic_load(&n_bytes), sz);
        fprintf(out, "%12s\ttotal\n", sz);
    }
    free(v);
    return 0;
}

/* ------------------------------------------------------------------ */
/* progress display                                                     */
/* ------------------------------------------------------------------ */

#define PROG_LINES 6

static double t_start;

static uint64_t prog_items(void)
{
    return atomic_load_explicit(&n_objs, memory_order_relaxed);
}

static void prog_draw(double rate, int frame)
{
    uint64_t objs  = atomic_load_explicit(&n_objs, memory_order_relaxed);
    uint64_t errs  = atomic_load_explicit(&s3m_nerrors, memory_order_relaxed);
    uint64_t bytes = atomic_load_explicit(&n_bytes, memory_order_relaxed);
    uint64_t reqs  = atomic_load_explicit(&s3m_nrequests, memory_order_relaxed);

    char cur[2048];
    s3m_get_current(cur, sizeof cur);
    char ptr[512];
    int pmax = s3m_term_width() - 13;
    if (pmax > 500)
        pmax = 500;
    if (pmax < 20)
        pmax = 20;
    s3m_trunc_left(cur, (size_t)pmax, ptr, sizeof ptr);

    char ov[32], ev[32], rv[32], sv[32], el[32], qv[32];
    s3m_fmt_u64(objs, ov);
    s3m_fmt_u64(errs, ev);
    s3m_fmt_u64((uint64_t)(rate + 0.5), rv);
    s3m_fmt_size(bytes, sv);
    s3m_fmt_elapsed(s3m_mono_now() - t_start, el);
    s3m_fmt_u64(reqs, qv);

    char ratestr[48];
    snprintf(ratestr, sizeof ratestr, "%s obj/s", rv);

    char buf[4096];
    size_t off = 0;
#define ADD(...) off += (size_t)snprintf(buf + off, sizeof buf - off, __VA_ARGS__)
    ADD("\x1b[K%s%s%s %ss3m-du%s %s— parallel usage scan%s\n",
        C_CYAN, s3m_spinner[frame % 10], C_RESET, C_BOLD, C_RESET,
        C_DIM, C_RESET);
    ADD("\x1b[K  %s%-9s%s %s\n", C_DIM, "key", C_RESET, ptr);
    ADD("\x1b[K  %s%-9s%s %-14d %s%-8s%s %s\n",
        C_DIM, "threads", C_RESET, g.nthreads,
        C_DIM, "listing", C_RESET, g.versions ? "versions" : "objects");
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s\n",
        C_DIM, "objects", C_RESET, ov, C_DIM, "requests", C_RESET, qv);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s%s%s\n",
        C_DIM, "rate", C_RESET, ratestr, C_DIM, "errors", C_RESET,
        errs ? C_RED : "", ev, errs ? C_RESET : "");
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s\n",
        C_DIM, "size", C_RESET, sv, C_DIM, "elapsed", C_RESET, el);
#undef ADD
    fwrite(buf, 1, off, stderr);
}

static void print_summary(double elapsed)
{
    uint64_t objs  = atomic_load(&n_objs);
    uint64_t errs  = atomic_load(&s3m_nerrors);
    uint64_t bytes = atomic_load(&n_bytes);
    uint64_t mks   = atomic_load(&n_markers);

    char ov[32], ev[32], sv[32], el[32], mv[32];
    s3m_fmt_u64(objs, ov);
    s3m_fmt_u64(errs, ev);
    s3m_fmt_size(bytes, sv);
    s3m_fmt_elapsed(elapsed, el);
    s3m_fmt_u64(mks, mv);

    fprintf(stderr,
            "%s✓%s %ss3m-du%s complete — %s object%s · %s%s error%s%s · %s",
            C_GREEN, C_RESET, C_BOLD, C_RESET, ov, objs == 1 ? "" : "s",
            errs ? C_RED : "", ev, errs == 1 ? "" : "s",
            errs ? C_RESET : "", sv);
    if (g.versions)
        fprintf(stderr, " · %s delete marker%s", mv, mks == 1 ? "" : "s");
    fprintf(stderr, "\n  in %s", el);
    if (g.outpath)
        fprintf(stderr, " → %s", g.outpath);
    fputc('\n', stderr);
}

/* ------------------------------------------------------------------ */
/* argument parsing / main                                              */
/* ------------------------------------------------------------------ */

static void usage(FILE *to)
{
    fputs(
"Usage: s3m-du [OPTIONS] s3://BUCKET[/PREFIX]...\n"
"\n"
"Summarise object storage usage by prefix, in parallel. Output is\n"
"du-style: size, a tab, then the prefix (and the storage class with\n"
"--by-class).\n"
"\n"
"Options:\n"
"  -s, --summarize      only print a total for each URI argument\n"
"  -d, --max-depth N    print prefixes at most N levels below each URI\n"
"  -c, --total          also print a grand total\n"
"  -h, --human-readable sizes in powers of 1024 (e.g. 5.1M)\n"
"      --si             sizes in powers of 1000\n"
"      --by-class       break every line down by storage class\n"
"      --versions       include non-current versions and delete markers\n"
"  -j, --threads N      worker threads, 1-256 (default: 16)\n"
"      --shard-depth N  prefix levels to expand for parallelism, 0-9\n"
"                       (default: 2; 0 = one flat serial listing)\n"
"      --rrdns          resolve the endpoint hostname to every A/AAAA\n"
"                       address it has and spread worker threads across\n"
"                       them (round robin), moving a thread to the next\n"
"                       address if its current one starts failing\n"
"  -o, --output FILE    write the report to FILE; show live progress\n"
"  -q, --quiet          suppress the report on stdout (summary still shown)\n"
"      --help           show this help and exit\n"
"  -V, --version        show version and exit\n",
    to);
    fputs(s3m_common_usage, to);
}

int main(int argc, char **argv)
{
    static const struct option lopts[] = {
        { "summarize",      no_argument,       NULL, 's' },
        { "max-depth",      required_argument, NULL, 'd' },
        { "total",          no_argument,       NULL, 'c' },
        { "human-readable", no_argument,       NULL, 'h' },
        { "si",             no_argument,       NULL, 1001 },
        { "by-class",       no_argument,       NULL, 1002 },
        { "versions",       no_argument,       NULL, 1003 },
        { "threads",        required_argument, NULL, 'j' },
        { "shard-depth",    required_argument, NULL, 1004 },
        { "rrdns",          no_argument,       NULL, 1005 },
        { "output",         required_argument, NULL, 'o' },
        { "quiet",          no_argument,       NULL, 'q' },
        { "help",           no_argument,       NULL, 1000 },
        { "version",        no_argument,       NULL, 'V' },
        S3M_COMMON_LOPTS,
        { 0, 0, 0, 0 }
    };

    int c;
    while ((c = getopt_long(argc, argv, "sd:chj:o:qV", lopts, NULL)) != -1) {
        if (s3m_common_opt(c, optarg))
            continue;
        switch (c) {
        case 's':
            g.max_depth = 0;
            break;
        case 'd': {
            char *end;
            long v = strtol(optarg, &end, 10);
            if (*end || v < 0 || v > 999) {
                fprintf(stderr, "s3m-du: invalid depth '%s'\n", optarg);
                return 2;
            }
            g.max_depth = (int)v;
            break;
        }
        case 'c':
            g.total = true;
            break;
        case 'h':
            g.human = true;
            break;
        case 1001:
            g.si = true;
            break;
        case 1002:
            g.by_class = true;
            break;
        case 1003:
            g.versions = true;
            break;
        case 'j': {
            char *end;
            long v = strtol(optarg, &end, 10);
            if (*end || v < 1 || v > 256) {
                fprintf(stderr, "s3m-du: invalid thread count '%s' "
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
                fprintf(stderr, "s3m-du: invalid shard depth '%s' "
                        "(expected 0-9)\n", optarg);
                return 2;
            }
            g.shard_depth = (int)v;
            break;
        }
        case 1005:
            g.rrdns = true;
            break;
        case 'o':
            g.outpath = optarg;
            break;
        case 'q':
            g.quiet = true;
            break;
        case 1000:
            usage(stdout);
            return 0;
        case 'V':
            printf("s3m-du %s (s3m: Parallel S3 Object Manager)\n",
                   S3M_DU_VERSION);
            return 0;
        default:
            usage(stderr);
            return 2;
        }
    }

    if (optind >= argc) {
        fprintf(stderr, "s3m-du: no s3:// URI given\n");
        usage(stderr);
        return 2;
    }
    if (argc - optind > 64) {
        fprintf(stderr, "s3m-du: too many URIs (max 64)\n");
        return 2;
    }

    /* the report only prints after the scan, so live progress can never
     * interleave with it — show it whenever stderr is a terminal */
    s3m_color = isatty(STDERR_FILENO);
    g.progress = isatty(STDERR_FILENO);

    if (s3m_config_finalize("s3m-du") != 0)
        return 2;
    if (g.rrdns && s3m_endpoint_pool_init(&endpoints, "s3m-du") != 0)
        return 2;
    if (s3m_http_global_init() != 0) {
        fprintf(stderr, "s3m-du: could not initialise libcurl\n");
        return 2;
    }

    FILE *out;
    if (g.outpath) {
        out = fopen(g.outpath, "w");
        if (!out) {
            fprintf(stderr, "s3m-du: cannot open '%s': %s\n",
                    g.outpath, strerror(errno));
            return 2;
        }
    } else {
        out = stdout;
    }

    /* seed the work queue and register the roots */
    s3m_stack_init(&stk, g.nthreads);
    for (int i = optind; i < argc; i++) {
        char bucket[256];
        char *key;
        if (s3m_uri_parse("s3m-du", argv[i], bucket, &key) != 0)
            return 2;
        roots[nroots] = s3m_strdupf("%s/%s", bucket, key);
        if (!roots[nroots]) {
            fprintf(stderr, "s3m-du: out of memory\n");
            return 2;
        }
        rootlens[nroots] = strlen(roots[nroots]);
        root_keylens[nroots] = strlen(key);
        nroots++;
        s3m_push_job(&stk, bucket, key, 0, 0);
        free(key);
    }

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
        fprintf(stderr, "s3m-du: out of memory\n");
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
            fprintf(stderr, "s3m-du: could not create any worker threads\n");
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

    int prc = 0;
    if (!(g.quiet && !g.outpath))
        prc = print_report(out);

    bool wfail = false;
    if (fflush(out) != 0 || ferror(out))
        wfail = true;
    if (g.outpath && fclose(out) != 0)
        wfail = true;
    if (wfail || prc != 0) {
        fprintf(stderr, "s3m-du: %swrite error%s on %s — output is "
                "incomplete\n", C_RED, C_RESET,
                g.outpath ? g.outpath : "stdout");
        return 1;
    }

    if (g.progress || g.outpath || g.quiet)
        print_summary(elapsed);
    s3m_print_errors();
    s3m_stack_destroy(&stk);
    if (g.rrdns)
        s3m_endpoint_pool_destroy(&endpoints);

    return atomic_load(&s3m_nerrors) ? 1 : 0;
}
