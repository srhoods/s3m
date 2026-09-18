/*
 * s3m-rm — parallel object remover
 *
 * Part of s3m: Parallel S3 Object Manager
 *
 * Removes objects by exact key, prefix or glob mask using batched
 * DeleteObjects requests fed by the parallel listing engine — with a
 * safety-first design. By default nothing is removed: the tool
 * performs a dry run listing everything that would be deleted; add
 * --apply to delete.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "s3mcore.h"

#include <errno.h>
#include <fnmatch.h>
#include <getopt.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define S3M_RM_VERSION "1.0.0"

enum tkind { T_EXACT, T_PREFIX, T_MASK };

struct target {
    enum tkind  kind;
    char        bucket[256];
    char       *arg;             /* key, prefix or pattern              */
    char       *root;            /* listing prefix (literal part)       */
};

static struct target targets[64];
static int           ntargets;

static struct {
    bool        apply;
    bool        permanent;       /* delete versions + markers           */
    bool        entire_bucket;
    int         nthreads;
    int         shard_depth;
    const char *outpath;
    bool        quiet;
    bool        suppress;
    bool        progress;
    bool        rrdns;
} g = { .nthreads = 16, .shard_depth = 2 };

static s3m_endpoint_pool endpoints;

static _Atomic uint64_t n_scanned;   /* objects/versions listed         */
static _Atomic uint64_t n_matched;   /* matched a target                */
static _Atomic uint64_t n_deleted;   /* apply mode: confirmed deleted   */
static _Atomic uint64_t n_bytes;     /* aggregate size of matches       */

static s3m_stack stk;
static s3m_sink  sink;

/* ------------------------------------------------------------------ */
/* matching                                                             */
/* ------------------------------------------------------------------ */

static bool target_match(const struct target *t, const char *bucket,
                         const char *key)
{
    if (strcmp(t->bucket, bucket) != 0)
        return false;
    switch (t->kind) {
    case T_EXACT:
        return strcmp(t->arg, key) == 0;
    case T_PREFIX:
        return strncmp(t->arg, key, strlen(t->arg)) == 0;
    case T_MASK:
        return fnmatch(t->arg, key, 0) == 0;
    }
    return false;
}

static bool any_match(const char *bucket, const char *key)
{
    for (int i = 0; i < ntargets; i++)
        if (target_match(&targets[i], bucket, key))
            return true;
    return false;
}

/* ------------------------------------------------------------------ */
/* row emission + delete pump                                           */
/* ------------------------------------------------------------------ */

struct wctx {
    s3m_http    *h;
    s3m_outbuf   ob;
    s3m_delbatch batch;
    const char  *bucket;
};

static void emit_row(s3m_outbuf *ob, const char *key, const char *vid,
                     uint64_t size, bool have_size, const char *result)
{
    if (!s3m_ob_room(ob, 2 * (strlen(key) + strlen(result)) +
                         strlen(vid) + 256)) {
        s3m_note_error(key, "emit", "row too long");
        return;
    }
    s3m_ob_csv(ob, key);
    s3m_ob_putc(ob, ',');
    s3m_ob_csv(ob, vid);
    if (have_size)
        s3m_ob_fmt(ob, ",%llu,", (unsigned long long)size);
    else
        s3m_ob_puts(ob, ",,");
    s3m_ob_csv(ob, result);
    s3m_ob_putc(ob, '\n');
}

static void on_deleted(void *ctx, const char *key, const char *vid,
                       bool ok, const char *code, const char *msg)
{
    struct wctx *w = ctx;
    if (ok) {
        atomic_fetch_add_explicit(&n_deleted, 1, memory_order_relaxed);
        if (!g.suppress)
            emit_row(&w->ob, key, vid, 0, false, "deleted");
    } else {
        char res[512];
        snprintf(res, sizeof res, "failed: %s: %s", code, msg);
        s3m_note_error(key, "delete", res + 8);
        if (!g.suppress)
            emit_row(&w->ob, key, vid, 0, false, res);
    }
}

static void on_obj(void *ctx, const s3m_obj *o)
{
    struct wctx *w = ctx;

    atomic_fetch_add_explicit(&n_scanned, 1, memory_order_relaxed);
    s3m_set_current(o->key);

    if (!any_match(w->bucket, o->key))
        return;

    atomic_fetch_add_explicit(&n_matched, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&n_bytes, o->size, memory_order_relaxed);

    const char *vid = g.permanent ? o->version_id : "";

    if (!g.apply) {
        if (!g.suppress)
            emit_row(&w->ob, o->key, vid, o->size, true, "pending");
        return;
    }
    s3m_delbatch_add(&w->batch, w->h, o->key, vid);
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
        char *bucket, *prefix;
        int depth = s3m_job_parse(job, &bucket, &prefix, NULL);
        if (depth >= 0) {
            w.bucket = bucket;
            s3m_delbatch_init(&w.batch, bucket, on_deleted, &w);
            s3m_list_job(w.h, &stk, bucket, prefix, depth, g.shard_depth,
                         0, g.permanent, false, on_obj, &w);
            s3m_delbatch_flush(&w.batch, w.h);
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
    return atomic_load_explicit(&n_scanned, memory_order_relaxed);
}

static void prog_draw(double rate, int frame)
{
    uint64_t scan = atomic_load_explicit(&n_scanned, memory_order_relaxed);
    uint64_t mat  = atomic_load_explicit(&n_matched, memory_order_relaxed);
    uint64_t del  = atomic_load_explicit(&n_deleted, memory_order_relaxed);
    uint64_t errs = atomic_load_explicit(&s3m_nerrors, memory_order_relaxed);
    uint64_t reqs = atomic_load_explicit(&s3m_nrequests, memory_order_relaxed);

    char cur[2048];
    s3m_get_current(cur, sizeof cur);
    char ptr[512];
    int pmax = s3m_term_width() - 13;
    if (pmax > 500)
        pmax = 500;
    if (pmax < 20)
        pmax = 20;
    s3m_trunc_left(cur, (size_t)pmax, ptr, sizeof ptr);

    char sv[32], mv[32], dv[32], ev[32], rv[32], el[32], qv[32];
    s3m_fmt_u64(scan, sv);
    s3m_fmt_u64(mat, mv);
    s3m_fmt_u64(del, dv);
    s3m_fmt_u64(errs, ev);
    s3m_fmt_u64((uint64_t)(rate + 0.5), rv);
    s3m_fmt_elapsed(s3m_mono_now() - t_start, el);
    s3m_fmt_u64(reqs, qv);

    char ratestr[48];
    snprintf(ratestr, sizeof ratestr, "%s obj/s", rv);

    char buf[4096];
    size_t off = 0;
#define ADD(...) off += (size_t)snprintf(buf + off, sizeof buf - off, __VA_ARGS__)
    ADD("\x1b[K%s%s%s %ss3m-rm%s %s— parallel remove (%s)%s\n",
        C_CYAN, s3m_spinner[frame % 10], C_RESET, C_BOLD, C_RESET,
        C_DIM, g.apply ? "apply" : "dry run", C_RESET);
    ADD("\x1b[K  %s%-9s%s %s\n", C_DIM, "key", C_RESET, ptr);
    ADD("\x1b[K  %s%-9s%s %s\n", C_DIM, "output", C_RESET,
        g.outpath ? g.outpath : "none (-q)");
    ADD("\x1b[K  %s%-9s%s %-14d %s%-8s%s %s\n",
        C_DIM, "threads", C_RESET, g.nthreads,
        C_DIM, "action", C_RESET,
        g.apply ? (g.permanent ? "apply (permanent)" : "apply")
                : "dry run");
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s\n",
        C_DIM, "scanned", C_RESET, sv, C_DIM, "matched", C_RESET, mv);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s%s%s\n",
        C_DIM, "deleted", C_RESET, dv, C_DIM, "errors", C_RESET,
        errs ? C_RED : "", ev, errs ? C_RESET : "");
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s %s(req %s)%s\n",
        C_DIM, "rate", C_RESET, ratestr, C_DIM, "elapsed", C_RESET, el,
        C_DIM, qv, C_RESET);
#undef ADD
    fwrite(buf, 1, off, stderr);
}

/* ------------------------------------------------------------------ */
/* summary                                                              */
/* ------------------------------------------------------------------ */

static void print_summary(double elapsed)
{
    uint64_t scan  = atomic_load(&n_scanned);
    uint64_t mat   = atomic_load(&n_matched);
    uint64_t del   = atomic_load(&n_deleted);
    uint64_t errs  = atomic_load(&s3m_nerrors);
    uint64_t bytes = atomic_load(&n_bytes);

    char sv[32], mv[32], dv[32], ev[32], bv[32], el[32];
    s3m_fmt_u64(scan, sv);
    s3m_fmt_u64(mat, mv);
    s3m_fmt_u64(del, dv);
    s3m_fmt_u64(errs, ev);
    s3m_fmt_size(bytes, bv);
    s3m_fmt_elapsed(elapsed, el);

    fprintf(stderr,
            "%s✓%s %ss3m-rm%s complete — %s · %s scanned · %s matched",
            C_GREEN, C_RESET, C_BOLD, C_RESET,
            g.apply ? "apply" : "dry run", sv, mv);
    if (g.apply)
        fprintf(stderr, " · %s deleted", dv);
    fprintf(stderr, " · %s%s error%s%s · %s\n",
            errs ? C_RED : "", ev, errs == 1 ? "" : "s",
            errs ? C_RESET : "", bv);
    fprintf(stderr, "  in %s", el);
    if (g.outpath)
        fprintf(stderr, " → %s", g.outpath);
    fputc('\n', stderr);
    if (!g.apply && mat)
        fprintf(stderr, "  %sdry run — nothing was removed; add --apply "
                "to delete%s\n", C_BOLD, C_RESET);
}

/* ------------------------------------------------------------------ */
/* argument parsing / main                                              */
/* ------------------------------------------------------------------ */

static void usage(FILE *to)
{
    fputs(
"Usage: s3m-rm [OPTIONS] s3://BUCKET/KEY|PREFIX/|MASK...\n"
"\n"
"Remove objects in parallel using batched DeleteObjects requests.\n"
"By default this is a DRY RUN: everything that would be removed is\n"
"listed as CSV and nothing is deleted. Add --apply to delete.\n"
"\n"
"Target forms:\n"
"  s3://bucket/some/key       exactly that key\n"
"  s3://bucket/some/prefix/   everything under the prefix (note the /)\n"
"  s3://bucket/logs/2024-*    glob mask (*, ?, […]; * also spans /)\n"
"                             — quote masks so the shell ignores them\n"
"\n"
"Options:\n"
"      --apply           actually remove (per s3m convention there is no\n"
"                        --dry-run flag: that is the default state)\n"
"      --permanent       on versioned buckets, delete every stored version\n"
"                        and delete marker of the matched keys instead of\n"
"                        just writing a delete marker — unrecoverable\n"
"      --entire-bucket   allow a target that spans a whole bucket\n"
"                        (refused otherwise; there is no short form)\n"
"  -j, --threads N       worker threads, 1-256 (default: 16)\n"
"      --shard-depth N   prefix levels to expand for parallelism, 0-9\n"
"                        (default: 2)\n"
"      --rrdns           resolve the endpoint hostname to every A/AAAA\n"
"                        address it has and spread worker threads across\n"
"                        them (round robin), moving a thread to the next\n"
"                        address if its current one starts failing\n"
"  -o, --output FILE     write the CSV listing to FILE; show progress\n"
"  -q, --quiet           suppress the console listing (progress and the\n"
"                        summary are still shown)\n"
"  -h, --help            show this help and exit\n"
"  -V, --version         show version and exit\n",
    to);
    fputs(s3m_common_usage, to);
}

int main(int argc, char **argv)
{
    static const struct option lopts[] = {
        { "apply",         no_argument,       NULL, 1001 },
        { "permanent",     no_argument,       NULL, 1002 },
        { "entire-bucket", no_argument,       NULL, 1003 },
        { "threads",       required_argument, NULL, 'j' },
        { "shard-depth",   required_argument, NULL, 1004 },
        { "rrdns",         no_argument,       NULL, 1005 },
        { "output",        required_argument, NULL, 'o' },
        { "quiet",         no_argument,       NULL, 'q' },
        { "help",          no_argument,       NULL, 'h' },
        { "version",       no_argument,       NULL, 'V' },
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
            g.permanent = true;
            break;
        case 1003:
            g.entire_bucket = true;
            break;
        case 'j': {
            char *end;
            long v = strtol(optarg, &end, 10);
            if (*end || v < 1 || v > 256) {
                fprintf(stderr, "s3m-rm: invalid thread count '%s' "
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
                fprintf(stderr, "s3m-rm: invalid shard depth '%s' "
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
        case 'h':
            usage(stdout);
            return 0;
        case 'V':
            printf("s3m-rm %s (s3m: Parallel S3 Object Manager)\n",
                   S3M_RM_VERSION);
            return 0;
        default:
            usage(stderr);
            return 2;
        }
    }

    if (optind >= argc) {
        fprintf(stderr, "s3m-rm: no target given\n");
        usage(stderr);
        return 2;
    }
    if (argc - optind > 64) {
        fprintf(stderr, "s3m-rm: too many targets (max 64)\n");
        return 2;
    }

    /* ---- build and validate targets (all guards pass before any
     *      network traffic; a refused target means nothing happens) */
    for (int i = optind; i < argc; i++) {
        struct target *t = &targets[ntargets];
        char *key;
        if (s3m_uri_parse("s3m-rm", argv[i], t->bucket, &key) != 0)
            return 2;

        size_t globpos = strcspn(key, "*?[");
        if (key[globpos] != '\0') {
            t->kind = T_MASK;
            t->arg = key;
            t->root = strndup(key, globpos);
        } else if (key[0] == '\0' || key[strlen(key) - 1] == '/') {
            t->kind = T_PREFIX;
            t->arg = key;
            t->root = strdup(key);
        } else {
            t->kind = T_EXACT;
            t->arg = key;
            t->root = strdup(key);
        }
        if (!t->root) {
            fprintf(stderr, "s3m-rm: out of memory\n");
            return 2;
        }

        /* hard guard: a target whose listing root is the whole bucket
         * is refused without the explicit long flag */
        if (t->root[0] == '\0' && t->kind != T_EXACT &&
            !g.entire_bucket) {
            fprintf(stderr,
"s3m-rm: refusing '%s' — it spans the entire bucket\n"
"s3m-rm: pass --entire-bucket if that is really what you want\n",
                    argv[i]);
            return 2;
        }
        ntargets++;
    }

    s3m_color = isatty(STDERR_FILENO);
    g.suppress = g.quiet && !g.outpath;
    g.progress = (g.outpath || g.quiet) && isatty(STDERR_FILENO);

    if (s3m_config_finalize("s3m-rm") != 0)
        return 2;
    if (g.rrdns && s3m_endpoint_pool_init(&endpoints, "s3m-rm") != 0)
        return 2;
    if (s3m_http_global_init() != 0) {
        fprintf(stderr, "s3m-rm: could not initialise libcurl\n");
        return 2;
    }

    FILE *out;
    if (g.outpath) {
        out = fopen(g.outpath, "w");
        if (!out) {
            fprintf(stderr, "s3m-rm: cannot open '%s': %s\n",
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
        fputs("key,version_id,size,result\n", out);

    /* seed one listing job per target, skipping targets whose listing
     * root is already covered by an earlier target's root — this keeps
     * the listings disjoint, so no key is ever seen (or deleted) twice
     * even when targets overlap */
    s3m_stack_init(&stk, g.nthreads);
    for (int i = 0; i < ntargets; i++) {
        bool covered = false;
        for (int j = 0; j < i && !covered; j++) {
            covered = !strcmp(targets[j].bucket, targets[i].bucket) &&
                      !strncmp(targets[j].root, targets[i].root,
                               strlen(targets[j].root));
        }
        if (!covered)
            s3m_push_job(&stk, targets[i].bucket, targets[i].root, 0, 0);
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
        fprintf(stderr, "s3m-rm: out of memory\n");
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
            fprintf(stderr, "s3m-rm: could not create any worker threads\n");
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
        fprintf(stderr, "s3m-rm: %swrite error%s on %s — output is "
                "incomplete\n", C_RED, C_RESET,
                g.outpath ? g.outpath : "stdout");
        return 1;
    }

    print_summary(elapsed);
    s3m_print_errors();
    s3m_stack_destroy(&stk);
    if (g.rrdns)
        s3m_endpoint_pool_destroy(&endpoints);

    return atomic_load(&s3m_nerrors) ? 1 : 0;
}
