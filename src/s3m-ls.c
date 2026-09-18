/*
 * s3m-ls — parallel bucket lister
 *
 * Part of s3m: Parallel S3 Object Manager
 *
 * Lists one or more buckets or prefixes with a pool of worker threads
 * and emits every object found as CSV, at three levels of detail.
 * Parallelism comes from prefix sharding: delimiter listings discover
 * the "directory" structure of the key space and each discovered prefix
 * is listed concurrently.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "s3mcore.h"

#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define S3M_LS_VERSION "1.0.0"

enum detail_mode { MODE_BASIC, MODE_STANDARD, MODE_FULL };

static const char *mode_names[] = { "basic", "standard", "full" };

static struct {
    enum detail_mode mode;
    int              nthreads;
    int              shard_depth;
    const char      *outpath;    /* NULL => stdout */
    const char      *classes;    /* storage-class filter, comma list */
    bool             quiet;
    bool             suppress;   /* quiet without -o: emit nothing at all */
    bool             progress;
    bool             rrdns;
} g = { .mode = MODE_BASIC, .nthreads = 16, .shard_depth = 2 };

static s3m_endpoint_pool endpoints;

static _Atomic uint64_t n_objs;    /* objects listed                     */
static _Atomic uint64_t n_bytes;   /* aggregate object size              */

static s3m_stack stk;
static s3m_sink  sink;

/* ------------------------------------------------------------------ */
/* CSV row emission                                                     */
/* ------------------------------------------------------------------ */

static const char *csv_header(void)
{
    switch (g.mode) {
    case MODE_STANDARD:
        return "key,size,mtime,etag,class\n";
    case MODE_FULL:
        return "key,size,mtime,etag,class,owner,content_type,"
               "version_id,sse\n";
    default:
        return "key\n";
    }
}

static bool class_match(const char *cls)
{
    if (!g.classes)
        return true;
    /* an empty class in a list response means STANDARD */
    const char *c = cls[0] ? cls : "STANDARD";
    size_t cl = strlen(c);
    for (const char *p = g.classes; *p; ) {
        size_t n = strcspn(p, ",");
        if (n == cl && !strncasecmp(p, c, n))
            return true;
        p += n;
        if (*p == ',')
            p++;
    }
    return false;
}

struct wctx {
    s3m_http  *h;
    s3m_outbuf ob;
    const char *bucket;
};

static void emit_obj(struct wctx *w, const s3m_obj *o)
{
    s3m_outbuf *ob = &w->ob;
    if (!s3m_ob_room(ob, 2 * strlen(o->key) + 1024)) {
        s3m_note_error(o->key, "emit", "row too long");
        return;
    }

    s3m_ob_csv(ob, o->key);
    if (g.mode == MODE_BASIC) {
        s3m_ob_putc(ob, '\n');
        return;
    }

    char mt[32] = "";
    if (o->mtime != (time_t)-1)
        s3m_fmt_time(o->mtime, mt);
    s3m_ob_fmt(ob, ",%llu,%s,", (unsigned long long)o->size, mt);
    s3m_ob_csv(ob, o->etag);
    s3m_ob_putc(ob, ',');
    s3m_ob_puts(ob, o->storclass[0] ? o->storclass : "STANDARD");

    if (g.mode == MODE_STANDARD) {
        s3m_ob_putc(ob, '\n');
        return;
    }

    /* full: one HEAD per object for the fields listings don't carry */
    char ctype[160] = "", vid[288] = "", sse[64] = "";
    s3m_resp r;
    if (s3m_req(w->h, "HEAD", w->bucket, o->key, NULL, NULL, 0, NULL,
                false, NULL, 0, &r) == 0 && r.status == 200) {
        s3m_resp_header(&r, "Content-Type", ctype, sizeof ctype);
        s3m_resp_header(&r, "x-amz-version-id", vid, sizeof vid);
        s3m_resp_header(&r, "x-amz-server-side-encryption", sse,
                        sizeof sse);
    } else {
        char es[256];
        s3m_resp_errstr(&r, es, sizeof es);
        s3m_note_error(o->key, "head", es);
    }
    s3m_resp_free(&r);

    s3m_ob_putc(ob, ',');
    s3m_ob_csv(ob, o->owner);
    s3m_ob_putc(ob, ',');
    s3m_ob_csv(ob, ctype);
    s3m_ob_putc(ob, ',');
    s3m_ob_csv(ob, vid);
    s3m_ob_putc(ob, ',');
    s3m_ob_csv(ob, sse);
    s3m_ob_putc(ob, '\n');
}

static void on_obj(void *ctx, const s3m_obj *o)
{
    struct wctx *w = ctx;

    atomic_fetch_add_explicit(&n_objs, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&n_bytes, o->size, memory_order_relaxed);
    s3m_set_current(o->key);

    if (g.suppress || !class_match(o->storclass))
        return;
    emit_obj(w, o);
}

/* ------------------------------------------------------------------ */
/* workers                                                              */
/* ------------------------------------------------------------------ */

static void *worker(void *arg)
{
    int idx = (int)(intptr_t)arg;
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
    if (g.rrdns)
        s3m_http_pin_endpoint(w.h, &endpoints, (size_t)idx);
    char *job;
    while ((job = s3m_stack_pop(&stk)) != NULL) {
        if (atomic_load_explicit(&sink.failed, memory_order_relaxed)) {
            free(job);
            continue;              /* abort: let the queue drain */
        }
        char *bucket, *prefix;
        int depth = s3m_job_parse(job, &bucket, &prefix, NULL);
        if (depth >= 0) {
            w.bucket = bucket;
            s3m_list_job(w.h, &stk, bucket, prefix, depth, g.shard_depth,
                         0, false, g.mode == MODE_FULL, on_obj, &w);
        }
        free(job);
        if (g.rrdns && s3m_http_transport_failed(w.h))
            s3m_http_rotate_endpoint(w.h, &endpoints);
    }
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
    return atomic_load_explicit(&n_objs, memory_order_relaxed);
}

static void prog_draw(double rate, int frame)
{
    uint64_t objs  = atomic_load_explicit(&n_objs, memory_order_relaxed);
    uint64_t errs  = atomic_load_explicit(&s3m_nerrors, memory_order_relaxed);
    uint64_t bytes = atomic_load_explicit(&n_bytes, memory_order_relaxed);
    uint64_t reqs  = atomic_load_explicit(&s3m_nrequests, memory_order_relaxed);
    uint64_t rtry  = atomic_load_explicit(&s3m_nretries, memory_order_relaxed);

    char cur[2048];
    s3m_get_current(cur, sizeof cur);
    char ptr[512];
    int pmax = s3m_term_width() - 13;
    if (pmax > 500)
        pmax = 500;
    if (pmax < 20)
        pmax = 20;
    s3m_trunc_left(cur, (size_t)pmax, ptr, sizeof ptr);

    char ov[32], ev[32], rv[32], sv[32], el[32], qv[32], yv[32], pv[32];
    s3m_fmt_u64(objs, ov);
    s3m_fmt_u64(errs, ev);
    s3m_fmt_u64((uint64_t)(rate + 0.5), rv);
    s3m_fmt_size(bytes, sv);
    s3m_fmt_elapsed(s3m_mono_now() - t_start, el);
    s3m_fmt_u64(reqs, qv);
    s3m_fmt_u64(rtry, yv);
    s3m_fmt_u64((uint64_t)s3m_stack_pending(&stk), pv);

    char ratestr[48], reqstr[80];
    snprintf(ratestr, sizeof ratestr, "%s obj/s", rv);
    snprintf(reqstr, sizeof reqstr, "%s (%s retried)", qv, yv);

    char buf[4096];
    size_t off = 0;
#define ADD(...) off += (size_t)snprintf(buf + off, sizeof buf - off, __VA_ARGS__)
    ADD("\x1b[K%s%s%s %ss3m-ls%s %s— parallel bucket listing%s\n",
        C_CYAN, s3m_spinner[frame % 10], C_RESET, C_BOLD, C_RESET,
        C_DIM, C_RESET);
    ADD("\x1b[K  %s%-9s%s %s\n", C_DIM, "key", C_RESET, ptr);
    ADD("\x1b[K  %s%-9s%s %s\n", C_DIM, "output", C_RESET,
        g.outpath ? g.outpath : "none (-q)");
    ADD("\x1b[K  %s%-9s%s %-14d %s%-8s%s %s\n",
        C_DIM, "threads", C_RESET, g.nthreads,
        C_DIM, "mode", C_RESET, mode_names[g.mode]);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s pending\n",
        C_DIM, "objects", C_RESET, ov, C_DIM, "prefixes", C_RESET, pv);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s%s%s\n",
        C_DIM, "rate", C_RESET, ratestr, C_DIM, "errors", C_RESET,
        errs ? C_RED : "", ev, errs ? C_RESET : "");
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s %s(req %s)%s\n",
        C_DIM, "size", C_RESET, sv, C_DIM, "elapsed", C_RESET, el,
        C_DIM, reqstr, C_RESET);
#undef ADD
    fwrite(buf, 1, off, stderr);
}

/* ------------------------------------------------------------------ */
/* summary                                                              */
/* ------------------------------------------------------------------ */

static void print_summary(double elapsed)
{
    uint64_t objs  = atomic_load(&n_objs);
    uint64_t errs  = atomic_load(&s3m_nerrors);
    uint64_t bytes = atomic_load(&n_bytes);
    uint64_t reqs  = atomic_load(&s3m_nrequests);

    char ov[32], ev[32], rv[32], sv[32], el[32], qv[32];
    s3m_fmt_u64(objs, ov);
    s3m_fmt_u64(errs, ev);
    s3m_fmt_u64(elapsed > 0 ? (uint64_t)((double)objs / elapsed) : 0, rv);
    s3m_fmt_size(bytes, sv);
    s3m_fmt_elapsed(elapsed, el);
    s3m_fmt_u64(reqs, qv);

    fprintf(stderr,
            "%s✓%s %ss3m-ls%s complete — %s object%s · %s%s error%s%s · %s\n",
            C_GREEN, C_RESET, C_BOLD, C_RESET, ov, objs == 1 ? "" : "s",
            errs ? C_RED : "", ev, errs == 1 ? "" : "s",
            errs ? C_RESET : "", sv);
    fprintf(stderr, "  %s in %s (%s obj/s, %s requests)",
            mode_names[g.mode], el, rv, qv);
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
"Usage: s3m-ls [OPTIONS] s3://BUCKET[/PREFIX]...\n"
"\n"
"List objects in one or more buckets (or prefixes) in parallel,\n"
"emitting CSV.\n"
"\n"
"Options:\n"
"  -m, --mode LEVEL     detail level: basic (default), standard, full\n"
"                         basic     key only\n"
"                         standard  key, size, mtime, etag, storage class\n"
"                         full      + owner, content type, version id, sse\n"
"                                   (one extra HEAD request per object)\n"
"  -j, --threads N      worker threads, 1-256 (default: 16)\n"
"      --class CLASSES  only output objects of these storage classes,\n"
"                       comma-separated, e.g. --class STANDARD,GLACIER\n"
"      --shard-depth N  prefix levels to expand for parallelism, 0-9\n"
"                       (default: 2; 0 = one flat serial listing)\n"
"      --rrdns          resolve the endpoint hostname to every A/AAAA\n"
"                       address it has and spread worker threads across\n"
"                       them (round robin), moving a thread to the next\n"
"                       address if its current one starts failing\n"
"  -o, --output FILE    write CSV to FILE; a live progress display is shown\n"
"  -q, --quiet          suppress the console listing (progress and the\n"
"                       summary are still shown; a -o file is still written)\n"
"  -h, --help           show this help and exit\n"
"  -V, --version        show version and exit\n",
    to);
    fputs(s3m_common_usage, to);
    fputs(
"\n"
"Output order is non-deterministic (parallel listing); pipe through\n"
"sort(1) if a stable order is required.\n",
    to);
}

int main(int argc, char **argv)
{
    static const struct option lopts[] = {
        { "mode",        required_argument, NULL, 'm' },
        { "threads",     required_argument, NULL, 'j' },
        { "class",       required_argument, NULL, 1001 },
        { "shard-depth", required_argument, NULL, 1002 },
        { "rrdns",       no_argument,       NULL, 1003 },
        { "output",      required_argument, NULL, 'o' },
        { "quiet",       no_argument,       NULL, 'q' },
        { "help",        no_argument,       NULL, 'h' },
        { "version",     no_argument,       NULL, 'V' },
        S3M_COMMON_LOPTS,
        { 0, 0, 0, 0 }
    };

    int c;
    while ((c = getopt_long(argc, argv, "m:j:o:qhV", lopts, NULL)) != -1) {
        if (s3m_common_opt(c, optarg))
            continue;
        switch (c) {
        case 'm':
            if      (!strcmp(optarg, "basic")    || !strcmp(optarg, "b"))
                g.mode = MODE_BASIC;
            else if (!strcmp(optarg, "standard") || !strcmp(optarg, "s"))
                g.mode = MODE_STANDARD;
            else if (!strcmp(optarg, "full")     || !strcmp(optarg, "f"))
                g.mode = MODE_FULL;
            else {
                fprintf(stderr, "s3m-ls: invalid mode '%s' "
                        "(expected basic, standard or full)\n", optarg);
                return 2;
            }
            break;
        case 'j': {
            char *end;
            long v = strtol(optarg, &end, 10);
            if (*end || v < 1 || v > 256) {
                fprintf(stderr, "s3m-ls: invalid thread count '%s' "
                        "(expected 1-256)\n", optarg);
                return 2;
            }
            g.nthreads = (int)v;
            break;
        }
        case 1001:
            g.classes = optarg;
            break;
        case 1002: {
            char *end;
            long v = strtol(optarg, &end, 10);
            if (*end || v < 0 || v > 9) {
                fprintf(stderr, "s3m-ls: invalid shard depth '%s' "
                        "(expected 0-9)\n", optarg);
                return 2;
            }
            g.shard_depth = (int)v;
            break;
        }
        case 1003:
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
            printf("s3m-ls %s (s3m: Parallel S3 Object Manager)\n",
                   S3M_LS_VERSION);
            return 0;
        default:
            usage(stderr);
            return 2;
        }
    }

    if (optind >= argc) {
        fprintf(stderr, "s3m-ls: no s3:// URI given\n");
        usage(stderr);
        return 2;
    }

    s3m_color = isatty(STDERR_FILENO);
    g.suppress = g.quiet && !g.outpath;
    g.progress = (g.outpath || g.quiet) && isatty(STDERR_FILENO);

    if (s3m_config_finalize("s3m-ls") != 0)
        return 2;
    if (g.rrdns && s3m_endpoint_pool_init(&endpoints, "s3m-ls") != 0)
        return 2;
    if (s3m_http_global_init() != 0) {
        fprintf(stderr, "s3m-ls: could not initialise libcurl\n");
        return 2;
    }

    FILE *out;
    if (g.outpath) {
        out = fopen(g.outpath, "w");
        if (!out) {
            fprintf(stderr, "s3m-ls: cannot open '%s': %s\n",
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
        fputs(csv_header(), out);

    /* seed the work queue with the root URIs */
    s3m_stack_init(&stk, g.nthreads);
    for (int i = optind; i < argc; i++) {
        char bucket[256];
        char *key;
        if (s3m_uri_parse("s3m-ls", argv[i], bucket, &key) != 0)
            return 2;
        s3m_push_job(&stk, bucket, key, 0, 0);
        free(key);
    }

    if (g.progress)
        s3m_set_current("…");

    /* launch */
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
        fprintf(stderr, "s3m-ls: out of memory\n");
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
            fprintf(stderr, "s3m-ls: could not create any worker threads\n");
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

    /* finish the output stream */
    if (fflush(out) != 0 || ferror(out))
        atomic_store(&sink.failed, true);
    if (g.outpath && fclose(out) != 0)
        atomic_store(&sink.failed, true);

    if (atomic_load(&sink.failed)) {
        fprintf(stderr, "s3m-ls: %swrite error%s on %s — output is "
                "incomplete\n", C_RED, C_RESET,
                g.outpath ? g.outpath : "stdout");
        return 1;
    }

    if (g.outpath || g.quiet)
        print_summary(elapsed);
    s3m_print_errors();
    s3m_stack_destroy(&stk);
    if (g.rrdns)
        s3m_endpoint_pool_destroy(&endpoints);

    return atomic_load(&s3m_nerrors) ? 1 : 0;
}
