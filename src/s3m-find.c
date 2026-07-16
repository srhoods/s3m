/*
 * s3m-find — parallel find for object storage
 *
 * Part of s3m: Parallel S3 Object Manager
 *
 * Evaluates a find(1)-style expression — the classic operator grammar
 * with tests adapted to objects — against every object in one or more
 * buckets/prefixes, in parallel. Everything a test needs comes free in
 * the listing, so no per-object requests are ever made.
 *
 * There is deliberately no -delete and no -exec: s3m-find only reads.
 * Pipe matches to xargs -0 (via -print0) or feed them to s3m-rm, which
 * has its own dry-run safety net.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "s3mcore.h"

#include <errno.h>
#include <fnmatch.h>
#include <getopt.h>
#include <regex.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define S3M_FIND_VERSION "1.0.0"

static struct {
    int         nthreads;
    int         shard_depth;
    bool        versions;
    bool        print0;
    const char *outpath;
    bool        quiet;
    bool        suppress;
    bool        progress;
} g = { .nthreads = 16, .shard_depth = 2 };

static _Atomic uint64_t n_scanned;
static _Atomic uint64_t n_matched;

static time_t now;

static s3m_stack stk;
static s3m_sink  sink;

/* ------------------------------------------------------------------ */
/* expression AST                                                       */
/* ------------------------------------------------------------------ */

enum ntype { N_AND, N_OR, N_NOT, N_TEST };

enum ttype {
    T_NAME, T_INAME, T_KEY, T_IKEY, T_REGEX, T_IREGEX,
    T_SIZE, T_MTIME, T_MMIN, T_CLASS, T_ETAG, T_EMPTY,
    T_LATEST, T_MARKER, T_TRUE, T_FALSE
};

struct node {
    enum ntype   type;
    struct node *a, *b;
    enum ttype   test;
    char        *str;
    regex_t      re;
    char         sign;         /* '+', '-' or 0 (exact)                 */
    uint64_t     val;
    uint64_t     unit;         /* -size unit in bytes                   */
};

static char **toks;
static int    ntoks, tpos;

static void parse_fail(const char *msg, const char *tok)
{
    fprintf(stderr, "s3m-find: %s%s%s\n", msg, tok ? ": " : "",
            tok ? tok : "");
    exit(2);
}

static const char *peek(void)
{
    return tpos < ntoks ? toks[tpos] : NULL;
}

static const char *next_tok(const char *what)
{
    if (tpos >= ntoks)
        parse_fail("missing argument for", what);
    return toks[tpos++];
}

static struct node *new_node(enum ntype t)
{
    struct node *n = calloc(1, sizeof *n);
    if (!n) {
        fprintf(stderr, "s3m-find: out of memory\n");
        exit(2);
    }
    n->type = t;
    return n;
}

static struct node *parse_or(void);

static void parse_numeric(const char *what, const char *s,
                          struct node *n)
{
    n->sign = (*s == '+' || *s == '-') ? *s : 0;
    if (n->sign)
        s++;
    char *end;
    unsigned long long v = strtoull(s, &end, 10);
    if (end == s)
        parse_fail("invalid number for", what);
    n->val = v;
    n->unit = 1;
    if (*end) {
        switch (*end) {
        case 'c': n->unit = 1;                       break;
        case 'k': n->unit = (uint64_t)1 << 10;       break;
        case 'M': n->unit = (uint64_t)1 << 20;       break;
        case 'G': n->unit = (uint64_t)1 << 30;       break;
        case 'T': n->unit = (uint64_t)1 << 40;       break;
        default:  parse_fail("invalid unit in", toks[tpos - 1]);
        }
        if (end[1])
            parse_fail("trailing junk in", toks[tpos - 1]);
    }
}

static struct node *make_test(enum ttype t)
{
    struct node *n = new_node(N_TEST);
    n->test = t;
    return n;
}

static struct node *make_regex(enum ttype t, const char *what)
{
    struct node *n = make_test(t);
    const char *pat = next_tok(what);
    char *anch = s3m_strdupf("^(%s)$", pat);
    if (!anch)
        parse_fail("out of memory", NULL);
    int flags = REG_EXTENDED | REG_NOSUB |
                (t == T_IREGEX ? REG_ICASE : 0);
    int rc = regcomp(&n->re, anch, flags);
    free(anch);
    if (rc != 0) {
        char eb[256];
        regerror(rc, &n->re, eb, sizeof eb);
        parse_fail(eb, pat);
    }
    return n;
}

static struct node *parse_unary(void)
{
    const char *t = peek();
    if (!t)
        parse_fail("expression expected", NULL);
    tpos++;

    if (!strcmp(t, "(")) {
        struct node *n = parse_or();
        const char *close = peek();
        if (!close || strcmp(close, ")"))
            parse_fail("missing )", NULL);
        tpos++;
        return n;
    }
    if (!strcmp(t, "!") || !strcmp(t, "-not")) {
        struct node *n = new_node(N_NOT);
        n->a = parse_unary();
        return n;
    }

    struct node *n;
    if      (!strcmp(t, "-name"))   { n = make_test(T_NAME);   n->str = strdup(next_tok(t)); }
    else if (!strcmp(t, "-iname"))  { n = make_test(T_INAME);  n->str = strdup(next_tok(t)); }
    else if (!strcmp(t, "-key") ||
             !strcmp(t, "-path"))   { n = make_test(T_KEY);    n->str = strdup(next_tok(t)); }
    else if (!strcmp(t, "-ikey") ||
             !strcmp(t, "-ipath"))  { n = make_test(T_IKEY);   n->str = strdup(next_tok(t)); }
    else if (!strcmp(t, "-regex"))  { n = make_regex(T_REGEX, t); }
    else if (!strcmp(t, "-iregex")) { n = make_regex(T_IREGEX, t); }
    else if (!strcmp(t, "-class"))  { n = make_test(T_CLASS);  n->str = strdup(next_tok(t)); }
    else if (!strcmp(t, "-etag"))   {
        n = make_test(T_ETAG);
        const char *e = next_tok(t);
        size_t el = strlen(e);
        if (el >= 2 && e[0] == '"' && e[el - 1] == '"')
            n->str = strndup(e + 1, el - 2);
        else
            n->str = strdup(e);
    }
    else if (!strcmp(t, "-size"))   { n = make_test(T_SIZE);  parse_numeric(t, next_tok(t), n); }
    else if (!strcmp(t, "-mtime"))  { n = make_test(T_MTIME); parse_numeric(t, next_tok(t), n); }
    else if (!strcmp(t, "-mmin"))   { n = make_test(T_MMIN);  parse_numeric(t, next_tok(t), n); }
    else if (!strcmp(t, "-empty"))  { n = make_test(T_EMPTY); }
    else if (!strcmp(t, "-latest")) { n = make_test(T_LATEST); }
    else if (!strcmp(t, "-marker")) { n = make_test(T_MARKER); }
    else if (!strcmp(t, "-true"))   { n = make_test(T_TRUE); }
    else if (!strcmp(t, "-false"))  { n = make_test(T_FALSE); }
    else if (!strcmp(t, "-print"))  { n = make_test(T_TRUE); }
    else if (!strcmp(t, "-print0")) { g.print0 = true; n = make_test(T_TRUE); }
    else {
        parse_fail("unknown test", t);
        return NULL;               /* unreachable */
    }

    if ((n->test == T_LATEST || n->test == T_MARKER) && !g.versions)
        parse_fail("test requires --versions", t);
    if ((n->test == T_NAME || n->test == T_INAME || n->test == T_KEY ||
         n->test == T_IKEY || n->test == T_CLASS ||
         n->test == T_ETAG) && !n->str)
        parse_fail("out of memory", NULL);
    return n;
}

static bool starts_unary(const char *t)
{
    return t && (t[0] == '-' || !strcmp(t, "(") || !strcmp(t, "!"));
}

static struct node *parse_and(void)
{
    struct node *left = parse_unary();
    for (;;) {
        const char *t = peek();
        if (!t || !strcmp(t, ")") || !strcmp(t, "-o") ||
            !strcmp(t, "-or"))
            return left;
        if (!strcmp(t, "-a") || !strcmp(t, "-and")) {
            tpos++;
            t = peek();
        }
        if (!starts_unary(t))
            parse_fail("unexpected token", t);
        struct node *n = new_node(N_AND);
        n->a = left;
        n->b = parse_unary();
        left = n;
    }
}

static struct node *parse_or(void)
{
    struct node *left = parse_and();
    for (;;) {
        const char *t = peek();
        if (!t || (strcmp(t, "-o") && strcmp(t, "-or")))
            return left;
        tpos++;
        struct node *n = new_node(N_OR);
        n->a = left;
        n->b = parse_and();
        left = n;
    }
}

/* ------------------------------------------------------------------ */
/* evaluation                                                           */
/* ------------------------------------------------------------------ */

/* find-style rounded-up comparison: +N greater, -N less, N exact */
static bool num_cmp(uint64_t units, char sign, uint64_t n)
{
    if (sign == '+')
        return units > n;
    if (sign == '-')
        return units < n;
    return units == n;
}

static bool eval(const struct node *n, const s3m_obj *o,
                 const char *base)
{
    switch (n->type) {
    case N_AND: return eval(n->a, o, base) && eval(n->b, o, base);
    case N_OR:  return eval(n->a, o, base) || eval(n->b, o, base);
    case N_NOT: return !eval(n->a, o, base);
    case N_TEST:
        break;
    }
    switch (n->test) {
    case T_NAME:   return fnmatch(n->str, base, 0) == 0;
    case T_INAME:  return fnmatch(n->str, base, FNM_CASEFOLD) == 0;
    case T_KEY:    return fnmatch(n->str, o->key, 0) == 0;
    case T_IKEY:   return fnmatch(n->str, o->key, FNM_CASEFOLD) == 0;
    case T_REGEX:
    case T_IREGEX: return regexec(&n->re, o->key, 0, NULL, 0) == 0;
    case T_SIZE: {
        uint64_t units = (o->size + n->unit - 1) / n->unit;
        return num_cmp(units, n->sign, n->val);
    }
    case T_MTIME: {
        if (o->mtime == (time_t)-1)
            return false;
        int64_t age = now - o->mtime;
        if (age < 0)
            age = 0;
        return num_cmp((uint64_t)(age / 86400), n->sign, n->val);
    }
    case T_MMIN: {
        if (o->mtime == (time_t)-1)
            return false;
        int64_t age = now - o->mtime;
        if (age < 0)
            age = 0;
        return num_cmp((uint64_t)(age / 60), n->sign, n->val);
    }
    case T_CLASS:
        return strcasecmp(n->str,
                          o->storclass[0] ? o->storclass
                                          : "STANDARD") == 0;
    case T_ETAG:   return strcasecmp(n->str, o->etag) == 0;
    case T_EMPTY:  return o->size == 0 && !o->is_marker;
    case T_LATEST: return o->is_latest;
    case T_MARKER: return o->is_marker;
    case T_TRUE:   return true;
    case T_FALSE:  return false;
    }
    return false;
}

static struct node *expr;          /* NULL = match everything */

/* ------------------------------------------------------------------ */
/* workers                                                              */
/* ------------------------------------------------------------------ */

struct wctx {
    s3m_http  *h;
    s3m_outbuf ob;
};

static void on_obj(void *ctx, const s3m_obj *o)
{
    struct wctx *w = ctx;

    atomic_fetch_add_explicit(&n_scanned, 1, memory_order_relaxed);
    s3m_set_current(o->key);

    const char *base = strrchr(o->key, '/');
    base = base ? base + 1 : o->key;

    if (expr && !eval(expr, o, base))
        return;
    atomic_fetch_add_explicit(&n_matched, 1, memory_order_relaxed);

    if (g.suppress)
        return;
    s3m_outbuf *ob = &w->ob;
    if (!s3m_ob_room(ob, 2 * strlen(o->key) + 320)) {
        s3m_note_error(o->key, "emit", "row too long");
        return;
    }
    if (g.print0) {
        s3m_ob_puts(ob, o->key);
        s3m_ob_putc(ob, '\0');
        return;
    }
    s3m_ob_csv(ob, o->key);
    if (g.versions) {
        s3m_ob_putc(ob, ',');
        s3m_ob_csv(ob, o->version_id);
    }
    s3m_ob_putc(ob, '\n');
}

static void *worker(void *arg)
{
    (void)arg;
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
        int depth = s3m_job_parse(job, &bucket, &prefix, NULL);
        if (depth >= 0)
            s3m_list_job(w.h, &stk, bucket, prefix, depth, g.shard_depth,
                         0, g.versions, false, on_obj, &w);
        free(job);
    }
    s3m_ob_flush(&w.ob);
    s3m_ob_free(&w.ob);
    s3m_http_free(w.h);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* progress display                                                     */
/* ------------------------------------------------------------------ */

#define PROG_LINES 6

static double t_start;

static uint64_t prog_items(void)
{
    return atomic_load_explicit(&n_scanned, memory_order_relaxed);
}

static void prog_draw(double rate, int frame)
{
    uint64_t scan = atomic_load_explicit(&n_scanned, memory_order_relaxed);
    uint64_t mat  = atomic_load_explicit(&n_matched, memory_order_relaxed);
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

    char sv[32], mv[32], ev[32], rv[32], el[32], qv[32];
    s3m_fmt_u64(scan, sv);
    s3m_fmt_u64(mat, mv);
    s3m_fmt_u64(errs, ev);
    s3m_fmt_u64((uint64_t)(rate + 0.5), rv);
    s3m_fmt_elapsed(s3m_mono_now() - t_start, el);
    s3m_fmt_u64(reqs, qv);

    char ratestr[48];
    snprintf(ratestr, sizeof ratestr, "%s obj/s", rv);

    char buf[4096];
    size_t off = 0;
#define ADD(...) off += (size_t)snprintf(buf + off, sizeof buf - off, __VA_ARGS__)
    ADD("\x1b[K%s%s%s %ss3m-find%s %s— parallel object search%s\n",
        C_CYAN, s3m_spinner[frame % 10], C_RESET, C_BOLD, C_RESET,
        C_DIM, C_RESET);
    ADD("\x1b[K  %s%-9s%s %s\n", C_DIM, "key", C_RESET, ptr);
    ADD("\x1b[K  %s%-9s%s %s\n", C_DIM, "output", C_RESET,
        g.outpath ? g.outpath : "none (-q)");
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s\n",
        C_DIM, "scanned", C_RESET, sv, C_DIM, "matched", C_RESET, mv);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s%s%s\n",
        C_DIM, "rate", C_RESET, ratestr, C_DIM, "errors", C_RESET,
        errs ? C_RED : "", ev, errs ? C_RESET : "");
    ADD("\x1b[K  %s%-9s%s %-14d %s%-8s%s %s %s(req %s)%s\n",
        C_DIM, "threads", C_RESET, g.nthreads,
        C_DIM, "elapsed", C_RESET, el, C_DIM, qv, C_RESET);
#undef ADD
    fwrite(buf, 1, off, stderr);
}

static void print_summary(double elapsed)
{
    uint64_t scan = atomic_load(&n_scanned);
    uint64_t mat  = atomic_load(&n_matched);
    uint64_t errs = atomic_load(&s3m_nerrors);

    char sv[32], mv[32], ev[32], rv[32], el[32];
    s3m_fmt_u64(scan, sv);
    s3m_fmt_u64(mat, mv);
    s3m_fmt_u64(errs, ev);
    s3m_fmt_u64(elapsed > 0 ? (uint64_t)((double)scan / elapsed) : 0, rv);
    s3m_fmt_elapsed(elapsed, el);

    fprintf(stderr,
            "%s✓%s %ss3m-find%s complete — %s matched · %s %s scanned · "
            "%s%s error%s%s\n  %s (%s obj/s)",
            C_GREEN, C_RESET, C_BOLD, C_RESET, mv, sv,
            g.versions ? "versions" : "objects",
            errs ? C_RED : "", ev, errs == 1 ? "" : "s",
            errs ? C_RESET : "", el, rv);
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
"Usage: s3m-find [S3M-OPTIONS] s3://BUCKET[/PREFIX]... [EXPRESSION]\n"
"\n"
"Search buckets in parallel, evaluating a find(1)-style expression\n"
"against every object. s3m options come first, then URIs, then the\n"
"expression. With no expression, everything matches. There is\n"
"deliberately no -delete and no -exec — pipe matches to s3m-rm.\n"
"\n"
"s3m options:\n"
"  -j, --threads N      worker threads, 1-256 (default: 16)\n"
"      --shard-depth N  prefix levels to expand for parallelism, 0-9\n"
"      --versions       search every stored version and delete marker\n"
"                       (output gains a version_id column; enables\n"
"                       -latest and -marker)\n"
"  -o, --output FILE    write matches to FILE; show live progress\n"
"  -q, --quiet          no listing; the summary still shows the count\n"
"  -h, --help           show this help and exit\n"
"  -V, --version        show version and exit\n",
    to);
    fputs(s3m_common_usage, to);
    fputs(
"\n"
"Tests:\n"
"  -name G / -iname G     glob on the last path segment of the key\n"
"  -key G  / -ikey G      glob on the whole key (* also spans /)\n"
"  -regex R / -iregex R   POSIX extended regex, anchored to the whole key\n"
"  -size [+-]N[ckMGT]     size (rounded up to the unit; default c=bytes)\n"
"  -mtime [+-]N           age in days     (find rounding rules)\n"
"  -mmin [+-]N            age in minutes\n"
"  -class C               storage class (case-insensitive)\n"
"  -etag E                exact etag (quotes optional)\n"
"  -empty                 zero-byte object\n"
"  -latest / -marker      current version / delete marker (--versions)\n"
"  -true / -false         constants\n"
"\n"
"Operators (highest precedence first, as in find):\n"
"  ( EXPR )   ! EXPR, -not EXPR   EXPR EXPR, EXPR -a EXPR   EXPR -o EXPR\n"
"\n"
"Output: CSV with a key header (key,version_id with --versions); with\n"
"-print0, NUL-separated raw keys for xargs -0.\n",
    to);
}

int main(int argc, char **argv)
{
    static const struct option lopts[] = {
        { "threads",     required_argument, NULL, 'j' },
        { "shard-depth", required_argument, NULL, 1001 },
        { "versions",    no_argument,       NULL, 1002 },
        { "output",      required_argument, NULL, 'o' },
        { "quiet",       no_argument,       NULL, 'q' },
        { "help",        no_argument,       NULL, 'h' },
        { "version",     no_argument,       NULL, 'V' },
        S3M_COMMON_LOPTS,
        { 0, 0, 0, 0 }
    };

    /* "+" stops getopt at the first non-option (the first URI), so the
     * expression's -name/-size/… are never seen by getopt */
    int c;
    while ((c = getopt_long(argc, argv, "+j:o:qhV", lopts, NULL)) != -1) {
        if (s3m_common_opt(c, optarg))
            continue;
        switch (c) {
        case 'j': {
            char *end;
            long v = strtol(optarg, &end, 10);
            if (*end || v < 1 || v > 256) {
                fprintf(stderr, "s3m-find: invalid thread count '%s' "
                        "(expected 1-256)\n", optarg);
                return 2;
            }
            g.nthreads = (int)v;
            break;
        }
        case 1001: {
            char *end;
            long v = strtol(optarg, &end, 10);
            if (*end || v < 0 || v > 9) {
                fprintf(stderr, "s3m-find: invalid shard depth '%s' "
                        "(expected 0-9)\n", optarg);
                return 2;
            }
            g.shard_depth = (int)v;
            break;
        }
        case 1002:
            g.versions = true;
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
            printf("s3m-find %s (s3m: Parallel S3 Object Manager)\n",
                   S3M_FIND_VERSION);
            return 0;
        default:
            usage(stderr);
            return 2;
        }
    }

    /* URIs, then the expression */
    int uri_start = optind, uri_end = optind;
    while (uri_end < argc && !strncasecmp(argv[uri_end], "s3://", 5))
        uri_end++;
    if (uri_end == uri_start) {
        fprintf(stderr, "s3m-find: no s3:// URI given\n");
        usage(stderr);
        return 2;
    }

    now = time(NULL);
    if (uri_end < argc) {
        toks = &argv[uri_end];
        ntoks = argc - uri_end;
        expr = parse_or();
        if (tpos < ntoks)
            parse_fail("unexpected token", toks[tpos]);
    }

    s3m_color = isatty(STDERR_FILENO);
    g.suppress = g.quiet && !g.outpath;
    g.progress = (g.outpath || g.quiet) && isatty(STDERR_FILENO);

    if (s3m_config_finalize("s3m-find") != 0)
        return 2;
    if (s3m_http_global_init() != 0) {
        fprintf(stderr, "s3m-find: could not initialise libcurl\n");
        return 2;
    }

    FILE *out;
    if (g.outpath) {
        out = fopen(g.outpath, "w");
        if (!out) {
            fprintf(stderr, "s3m-find: cannot open '%s': %s\n",
                    g.outpath, strerror(errno));
            return 2;
        }
    } else {
        out = stdout;
    }
    static char outvbuf[1 << 20];
    setvbuf(out, outvbuf, _IOFBF, sizeof outvbuf);
    s3m_sink_init(&sink, out);

    if (!g.suppress && !g.print0)
        fputs(g.versions ? "key,version_id\n" : "key\n", out);

    s3m_stack_init(&stk, g.nthreads);
    for (int i = uri_start; i < uri_end; i++) {
        char bucket[256];
        char *key;
        if (s3m_uri_parse("s3m-find", argv[i], bucket, &key) != 0)
            return 2;
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
        fprintf(stderr, "s3m-find: out of memory\n");
        return 2;
    }
    int started = 0;
    for (int i = 0; i < g.nthreads; i++) {
        if (pthread_create(&tids[i], NULL, worker, NULL) != 0)
            break;
        started++;
    }
    if (started < g.nthreads) {
        if (started == 0) {
            fprintf(stderr, "s3m-find: could not create any worker "
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
        fprintf(stderr, "s3m-find: %swrite error%s on %s — output is "
                "incomplete\n", C_RED, C_RESET,
                g.outpath ? g.outpath : "stdout");
        return 1;
    }

    if (g.outpath || g.quiet)
        print_summary(elapsed);
    s3m_print_errors();
    s3m_stack_destroy(&stk);

    return atomic_load(&s3m_nerrors) ? 1 : 0;
}
