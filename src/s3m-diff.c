/*
 * s3m-diff — parallel bucket/prefix comparison
 *
 * Part of s3m: Parallel S3 Object Manager
 *
 * Compares two buckets/prefixes, or a local directory against a
 * bucket/prefix, and reports the likelihood that their contents are
 * the same. By default it compares key presence, sizes, and etags
 * where etags are conclusive (single-part uploads are content MD5s);
 * with -c it additionally verifies contents — a local MD5 against a
 * conclusive etag where possible (no network I/O), ranged GETs
 * otherwise, stopping at the first differing byte. Read-only: nothing
 * on either side is ever modified.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "s3mcore.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define S3M_DIFF_VERSION "1.1.0"

#define CHUNK ((uint64_t)8 << 20)      /* content compare chunk */

struct side {
    bool  is_s3;
    char  bucket[256];
    char *prefix;              /* s3: "" or ends with '/' */
    char *dir;                 /* local: no trailing '/'  */
};

static struct side L, R;

static struct {
    bool        checksum;
    int         nthreads;
    int         shard_depth;
    const char *outpath;
    bool        quiet;
    bool        suppress;
    bool        progress;
    bool        rrdns;
} g = { .nthreads = 16, .shard_depth = 2 };

static s3m_endpoint_pool endpoints;

static _Atomic uint64_t n_left;        /* left objects indexed        */
static _Atomic uint64_t n_right;       /* right objects scanned       */
static _Atomic uint64_t d_onlyl, d_onlyr, d_size, d_etag, d_content;
static _Atomic uint64_t n_unverified;  /* inconclusive, not checked   */
static _Atomic uint64_t bytes_checked; /* content bytes compared (×2) */
static _Atomic int      cur_phase;     /* 1 index · 2 scan · 3 verify */

static s3m_stack stk;
static s3m_sink  sink;

static uint64_t total_diffs(void)
{
    return atomic_load(&d_onlyl) + atomic_load(&d_onlyr) +
           atomic_load(&d_size) + atomic_load(&d_etag) +
           atomic_load(&d_content);
}

/* ------------------------------------------------------------------ */
/* left index                                                           */
/* ------------------------------------------------------------------ */

struct dent {
    char        *rel;
    uint64_t     size;
    char         etag[68];
    bool         seen;
    struct dent *next;
};

#define IDX_BUCKETS 65536
static struct dent    *idx[IDX_BUCKETS];
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

static void idx_add(const char *rel, uint64_t size, const char *etag)
{
    struct dent *d = malloc(sizeof *d);
    if (!d)
        return;
    d->rel = strdup(rel);
    if (!d->rel) {
        free(d);
        return;
    }
    d->size = size;
    snprintf(d->etag, sizeof d->etag, "%s", etag);
    d->seen = false;
    uint64_t h = str_hash(rel) & (IDX_BUCKETS - 1);
    pthread_mutex_lock(&idx_mu);
    d->next = idx[h];
    idx[h] = d;
    pthread_mutex_unlock(&idx_mu);
    atomic_fetch_add_explicit(&n_left, 1, memory_order_relaxed);
}

static struct dent *idx_find(const char *rel)
{
    for (struct dent *d = idx[str_hash(rel) & (IDX_BUCKETS - 1)]; d;
         d = d->next)
        if (!strcmp(d->rel, rel))
            return d;
    return NULL;
}

/* ------------------------------------------------------------------ */
/* difference rows                                                      */
/* ------------------------------------------------------------------ */

static void emit_diff(s3m_outbuf *ob, const char *rel, const char *what,
                      const char *left, const char *right)
{
    if (g.suppress)
        return;
    if (!s3m_ob_room(ob, 2 * strlen(rel) + strlen(left) +
                         strlen(right) + 256)) {
        s3m_note_error(rel, "emit", "row too long");
        return;
    }
    s3m_ob_csv(ob, rel);
    s3m_ob_fmt(ob, ",%s,", what);
    s3m_ob_csv(ob, left);
    s3m_ob_putc(ob, ',');
    s3m_ob_csv(ob, right);
    s3m_ob_putc(ob, '\n');
}

/* ------------------------------------------------------------------ */
/* content-check candidates                                             */
/* ------------------------------------------------------------------ */

struct cand {
    char    *rel;
    uint64_t size;
    char     etag[68];         /* the S3 side's etag (mixed runs) */
};

static struct cand    *cands;
static size_t          ncands, ccands;
static pthread_mutex_t cands_mu = PTHREAD_MUTEX_INITIALIZER;
static _Atomic size_t  next_cand;

static void cand_add(const char *rel, uint64_t size, const char *etag)
{
    pthread_mutex_lock(&cands_mu);
    if (ncands == ccands) {
        size_t nc = ccands ? ccands * 2 : 1024;
        struct cand *na = realloc(cands, nc * sizeof *na);
        if (!na) {
            pthread_mutex_unlock(&cands_mu);
            s3m_note_error(rel, "plan", "out of memory");
            return;
        }
        cands = na;
        ccands = nc;
    }
    cands[ncands].rel = strdup(rel);
    cands[ncands].size = size;
    snprintf(cands[ncands].etag, sizeof cands[ncands].etag, "%s",
             etag ? etag : "");
    if (cands[ncands].rel)
        ncands++;
    pthread_mutex_unlock(&cands_mu);
}

/* ------------------------------------------------------------------ */
/* listing callbacks                                                    */
/* ------------------------------------------------------------------ */

struct wctx {
    s3m_http  *h;
    s3m_outbuf ob;
};

static bool etag_conclusive(const char *e)
{
    return e[0] && !strchr(e, '-');
}

static void on_left_obj(void *ctx, const s3m_obj *o)
{
    (void)ctx;
    size_t plen = strlen(L.prefix);
    if (strncmp(o->key, L.prefix, plen) != 0)
        return;
    const char *rel = o->key + plen;
    if (!rel[0] || rel[strlen(rel) - 1] == '/')
        return;
    s3m_set_current(o->key);
    idx_add(rel, o->size, o->etag);
}

/* compare one right-side entry against the left index; etag is ""
 * for local entries */
static void compare_entry(struct wctx *w, const char *rel,
                          uint64_t size, const char *etag)
{
    atomic_fetch_add_explicit(&n_right, 1, memory_order_relaxed);
    s3m_set_current(rel);

    struct dent *d = idx_find(rel);
    if (!d) {
        char rs[32];
        snprintf(rs, sizeof rs, "%llu", (unsigned long long)size);
        atomic_fetch_add_explicit(&d_onlyr, 1, memory_order_relaxed);
        emit_diff(&w->ob, rel, "only-right", "", rs);
        return;
    }
    d->seen = true;                    /* keys are unique per side */

    if (d->size != size) {
        char ls[32], rs[32];
        snprintf(ls, sizeof ls, "%llu", (unsigned long long)d->size);
        snprintf(rs, sizeof rs, "%llu", (unsigned long long)size);
        atomic_fetch_add_explicit(&d_size, 1, memory_order_relaxed);
        emit_diff(&w->ob, rel, "size", ls, rs);
        return;
    }
    if (etag_conclusive(d->etag) && etag_conclusive(etag)) {
        if (strcasecmp(d->etag, etag) != 0) {
            atomic_fetch_add_explicit(&d_etag, 1, memory_order_relaxed);
            emit_diff(&w->ob, rel, "etag", d->etag, etag);
        }
        return;                        /* conclusive either way */
    }
    /* sizes equal, comparison inconclusive so far */
    if (g.checksum && size > 0)
        cand_add(rel, size, etag[0] ? etag : d->etag);
    else if (size > 0)
        atomic_fetch_add_explicit(&n_unverified, 1, memory_order_relaxed);
}

static void on_right_obj(void *ctx, const s3m_obj *o)
{
    struct wctx *w = ctx;
    size_t plen = strlen(R.prefix);
    if (strncmp(o->key, R.prefix, plen) != 0)
        return;
    const char *rel = o->key + plen;
    if (!rel[0] || rel[strlen(rel) - 1] == '/')
        return;
    compare_entry(w, rel, o->size, o->etag);
}

/* recursive local walk (regular files only, symlinks never followed);
 * indexes the left side or compares as the right side */
static void walk_local(struct wctx *w, const char *root, size_t rootlen,
                       bool indexing)
{
    DIR *d = opendir(root);
    if (!d) {
        s3m_note_error(root, "opendir", strerror(errno));
        return;
    }
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        const char *nm = de->d_name;
        if (nm[0] == '.' &&
            (nm[1] == '\0' || (nm[1] == '.' && nm[2] == '\0')))
            continue;
        char *fp = s3m_strdupf("%s/%s", root, nm);
        if (!fp)
            continue;
        struct stat st;
        if (lstat(fp, &st) != 0) {
            s3m_note_error(fp, "stat", strerror(errno));
        } else if (S_ISDIR(st.st_mode)) {
            walk_local(w, fp, rootlen, indexing);
        } else if (S_ISREG(st.st_mode)) {
            const char *rel = fp + rootlen + 1;
            if (indexing) {
                s3m_set_current(rel);
                idx_add(rel, (uint64_t)st.st_size, "");
            } else {
                compare_entry(w, rel, (uint64_t)st.st_size, "");
            }
        }
        free(fp);
    }
    closedir(d);
}

struct list_arg {
    bool left;
    int  idx;               /* worker index: initial endpoint pick   */
};

static void *list_worker(void *arg)
{
    struct list_arg *la = arg;
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
        s3m_http_pin_endpoint(w.h, &endpoints, (size_t)la->idx);
    char *job;
    while ((job = s3m_stack_pop(&stk)) != NULL) {
        char *bucket, *prefix;
        int depth = s3m_job_parse(job, &bucket, &prefix, NULL);
        if (depth >= 0)
            s3m_list_job(w.h, &stk, bucket, prefix, depth, g.shard_depth,
                         0, false, false,
                         la->left ? on_left_obj : on_right_obj, &w);
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
/* content verification                                                 */
/* ------------------------------------------------------------------ */

/* ranged GET into memory; returns 0 with body in r */
static int get_range(s3m_http *h, const char *bucket, const char *key,
                     uint64_t off, uint64_t len, s3m_resp *r)
{
    char range[80];
    snprintf(range, sizeof range, "range:bytes=%llu-%llu",
             (unsigned long long)off,
             (unsigned long long)(off + len - 1));
    const char *xh[1] = { range };
    if (s3m_req(h, "GET", bucket, key, NULL, NULL, 0, NULL, false,
                xh, 1, r) != 0 || (r->status != 206 && r->status != 200))
        return -1;
    return 0;
}

/* fetch one chunk of a side: ranged GET (body borrowed from *r) or
 * pread into buf; returns false and notes the error on failure */
static bool side_chunk(s3m_http *h, const struct side *s,
                       const char *rel, int fd, char *buf,
                       uint64_t off, uint64_t len, s3m_resp *r,
                       const char **out, size_t *outn,
                       const char *what)
{
    if (s->is_s3) {
        char key[2200];
        snprintf(key, sizeof key, "%s%s", s->prefix, rel);
        if (get_range(h, s->bucket, key, off, len, r) != 0) {
            char es[256];
            s3m_resp_errstr(r, es, sizeof es);
            s3m_note_error(key, what, es);
            return false;
        }
        *out = r->body;
        *outn = r->body_len;
        return true;
    }
    ssize_t n = pread(fd, buf, (size_t)len, (off_t)off);
    if (n < 0) {
        s3m_note_error(rel, what, strerror(errno));
        return false;
    }
    *out = buf;
    *outn = (size_t)n;
    return true;
}

static void *verify_worker(void *arg)
{
    int idx = (int)(intptr_t)arg;
    struct wctx w;
    w.h = s3m_http_new();
    char *lbuf = L.is_s3 ? NULL : malloc(CHUNK);
    char *rbuf = R.is_s3 ? NULL : malloc(CHUNK);
    if (!w.h || s3m_ob_init(&w.ob, &sink) != 0 ||
        (!L.is_s3 && !lbuf) || (!R.is_s3 && !rbuf)) {
        s3m_note_error("worker", "startup", "out of memory");
        s3m_http_free(w.h);
        free(lbuf);
        free(rbuf);
        return NULL;
    }
    if (g.rrdns)
        s3m_http_pin_endpoint(w.h, &endpoints, (size_t)idx);
    for (;;) {
        size_t i = atomic_fetch_add(&next_cand, 1);
        if (i >= ncands)
            break;
        struct cand *cd = &cands[i];
        s3m_set_current(cd->rel);

        /* mixed run with a conclusive S3 etag: one local MD5 pass is
         * full content verification with no network I/O at all */
        if (L.is_s3 != R.is_s3 && etag_conclusive(cd->etag)) {
            const struct side *ls = L.is_s3 ? &R : &L;
            char *path = s3m_strdupf("%s/%s", ls->dir, cd->rel);
            char md5[33];
            if (!path || s3m_file_md5(path, md5) != 0) {
                s3m_note_error(cd->rel, "read-local",
                               path ? strerror(errno) : "out of memory");
            } else {
                atomic_fetch_add_explicit(&bytes_checked, cd->size,
                                          memory_order_relaxed);
                if (strcasecmp(md5, cd->etag) != 0) {
                    atomic_fetch_add_explicit(&d_content, 1,
                                              memory_order_relaxed);
                    emit_diff(&w.ob, cd->rel, "content",
                              "md5 differs from etag", "");
                }
            }
            free(path);
            continue;
        }

        /* chunked compare; each side is a ranged GET or a pread */
        int lfd = -1, rfd = -1;
        bool failed = false;
        if (!L.is_s3) {
            char *p = s3m_strdupf("%s/%s", L.dir, cd->rel);
            if (!p || (lfd = open(p, O_RDONLY | O_CLOEXEC)) < 0) {
                s3m_note_error(cd->rel, "read-left",
                               p ? strerror(errno) : "out of memory");
                failed = true;
            }
            free(p);
        }
        if (!R.is_s3 && !failed) {
            char *p = s3m_strdupf("%s/%s", R.dir, cd->rel);
            if (!p || (rfd = open(p, O_RDONLY | O_CLOEXEC)) < 0) {
                s3m_note_error(cd->rel, "read-right",
                               p ? strerror(errno) : "out of memory");
                failed = true;
            }
            free(p);
        }

        bool differ = false;
        uint64_t diff_at = 0;
        for (uint64_t off = 0; off < cd->size && !differ && !failed;
             off += CHUNK) {
            uint64_t len = (off + CHUNK <= cd->size) ? CHUNK
                                                     : cd->size - off;
            s3m_resp lr = { 0 }, rr = { 0 };
            const char *lb = NULL, *rb = NULL;
            size_t ln = 0, rn = 0;
            if (!side_chunk(w.h, &L, cd->rel, lfd, lbuf, off, len, &lr,
                            &lb, &ln, "read-left") ||
                !side_chunk(w.h, &R, cd->rel, rfd, rbuf, off, len, &rr,
                            &rb, &rn, "read-right")) {
                failed = true;
            } else {
                size_t n = ln < rn ? ln : rn;
                if (ln != rn) {
                    /* short read on one side: it changed mid-run */
                    differ = true;
                    diff_at = off + n;
                }
                for (size_t b = 0; b < n; b++) {
                    if (lb[b] != rb[b]) {
                        differ = true;
                        diff_at = off + b;
                        break;
                    }
                }
                atomic_fetch_add_explicit(&bytes_checked, ln + rn,
                                          memory_order_relaxed);
            }
            s3m_resp_free(&lr);
            s3m_resp_free(&rr);
        }
        if (lfd >= 0)
            close(lfd);
        if (rfd >= 0)
            close(rfd);
        if (differ) {
            char at[48];
            snprintf(at, sizeof at, "differ at byte %llu",
                     (unsigned long long)diff_at);
            atomic_fetch_add_explicit(&d_content, 1,
                                      memory_order_relaxed);
            emit_diff(&w.ob, cd->rel, "content", at, "");
        }
        if (g.rrdns && s3m_http_transport_failed(w.h))
            s3m_http_rotate_endpoint(w.h, &endpoints);
    }
    free(lbuf);
    free(rbuf);
    s3m_ob_flush(&w.ob);
    s3m_ob_free(&w.ob);
    s3m_http_free(w.h);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* shared pool runner                                                   */
/* ------------------------------------------------------------------ */

static int run_pool(bool left)
{
    pthread_t tids[256];
    struct list_arg args[256];
    int started = 0;
    for (int i = 0; i < g.nthreads; i++) {
        args[i].left = left;
        args[i].idx = i;
        if (pthread_create(&tids[i], NULL, list_worker, &args[i]) != 0)
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
/* progress display                                                     */
/* ------------------------------------------------------------------ */

#define PROG_LINES 6

static double t_start;

static uint64_t prog_items(void)
{
    return atomic_load_explicit(&n_left, memory_order_relaxed) +
           atomic_load_explicit(&n_right, memory_order_relaxed) +
           (atomic_load_explicit(&bytes_checked,
                                 memory_order_relaxed) >> 20);
}

static void prog_draw(double rate, int frame)
{
    static const char *phases[] = { "", "index left", "scan right",
                                    "verify contents" };
    int ph = atomic_load_explicit(&cur_phase, memory_order_relaxed);
    uint64_t nl   = atomic_load_explicit(&n_left, memory_order_relaxed);
    uint64_t nr   = atomic_load_explicit(&n_right, memory_order_relaxed);
    uint64_t bc   = atomic_load_explicit(&bytes_checked, memory_order_relaxed);
    uint64_t errs = atomic_load_explicit(&s3m_nerrors, memory_order_relaxed);
    uint64_t dfs  = total_diffs();

    char cur[2048];
    s3m_get_current(cur, sizeof cur);
    char ptr[512];
    int pmax = s3m_term_width() - 13;
    if (pmax > 500)
        pmax = 500;
    if (pmax < 20)
        pmax = 20;
    s3m_trunc_left(cur, (size_t)pmax, ptr, sizeof ptr);

    char lv[32], rv[32], dv[32], ev[32], bv[32], el[32], iv[32];
    s3m_fmt_u64(nl, lv);
    s3m_fmt_u64(nr, rv);
    s3m_fmt_u64(dfs, dv);
    s3m_fmt_u64(errs, ev);
    s3m_fmt_size(bc, bv);
    s3m_fmt_elapsed(s3m_mono_now() - t_start, el);
    s3m_fmt_u64((uint64_t)(rate + 0.5), iv);

    char buf[4096];
    size_t off = 0;
#define ADD(...) off += (size_t)snprintf(buf + off, sizeof buf - off, __VA_ARGS__)
    ADD("\x1b[K%s%s%s %ss3m-diff%s %s— parallel compare (%s)%s\n",
        C_CYAN, s3m_spinner[frame % 10], C_RESET, C_BOLD, C_RESET,
        C_DIM, g.checksum ? "keys+size+etag+content" : "keys+size+etag",
        C_RESET);
    ADD("\x1b[K  %s%-9s%s %s\n", C_DIM, "key", C_RESET, ptr);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %d\n",
        C_DIM, "phase", C_RESET, phases[ph],
        C_DIM, "threads", C_RESET, g.nthreads);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s\n",
        C_DIM, "left", C_RESET, lv, C_DIM, "right", C_RESET, rv);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s%s%s\n",
        C_DIM, "diffs", C_RESET, dv, C_DIM, "errors", C_RESET,
        errs ? C_RED : "", ev, errs ? C_RESET : "");
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s\n",
        C_DIM, "checked", C_RESET, bv, C_DIM, "elapsed", C_RESET, el);
#undef ADD
    fwrite(buf, 1, off, stderr);
}

/* ------------------------------------------------------------------ */
/* summary and verdict                                                  */
/* ------------------------------------------------------------------ */

static void print_summary(double elapsed)
{
    uint64_t nl  = atomic_load(&n_left);
    uint64_t nr  = atomic_load(&n_right);
    uint64_t ol  = atomic_load(&d_onlyl);
    uint64_t or_ = atomic_load(&d_onlyr);
    uint64_t ds  = atomic_load(&d_size);
    uint64_t de  = atomic_load(&d_etag);
    uint64_t dc  = atomic_load(&d_content);
    uint64_t uv  = atomic_load(&n_unverified);
    uint64_t bc  = atomic_load(&bytes_checked);
    uint64_t errs = atomic_load(&s3m_nerrors);
    uint64_t dfs = total_diffs();

    char lv[32], rv[32], dv[32], ev[32], el[32], bv[32];
    s3m_fmt_u64(nl, lv);
    s3m_fmt_u64(nr, rv);
    s3m_fmt_u64(dfs, dv);
    s3m_fmt_u64(errs, ev);
    s3m_fmt_elapsed(elapsed, el);
    s3m_fmt_size(bc, bv);

    fprintf(stderr,
            "%s%s%s %ss3m-diff%s complete — %s left · %s right · "
            "%s%s difference%s%s · %s%s error%s%s\n",
            dfs ? C_RED : C_GREEN, dfs ? "✗" : "✓", C_RESET,
            C_BOLD, C_RESET, lv, rv,
            dfs ? C_RED : "", dv, dfs == 1 ? "" : "s",
            dfs ? C_RESET : "",
            errs ? C_RED : "", ev, errs == 1 ? "" : "s",
            errs ? C_RESET : "");
    if (dfs) {
        char a[32], b[32], c2[32], d2[32], e2[32];
        s3m_fmt_u64(ol, a);
        s3m_fmt_u64(or_, b);
        s3m_fmt_u64(ds, c2);
        s3m_fmt_u64(de, d2);
        s3m_fmt_u64(dc, e2);
        fprintf(stderr, "  %s only in left · %s only in right · %s size "
                "· %s etag · %s content\n", a, b, c2, d2, e2);
    }
    fprintf(stderr, "  %s", el);
    if (bc)
        fprintf(stderr, " · %s of content compared", bv);
    if (g.outpath)
        fprintf(stderr, " → %s", g.outpath);
    fputc('\n', stderr);

    /* the verdict */
    if (errs) {
        fprintf(stderr, "  %sverdict withheld%s — the comparison hit "
                "errors, so coverage was incomplete\n", C_RED, C_RESET);
        return;
    }
    if (dfs) {
        fprintf(stderr, "  sides differ — %s difference%s listed "
                "above\n", dv, dfs == 1 ? " is" : "s are");
        return;
    }
    if (g.checksum || uv == 0) {
        fprintf(stderr, "  sides are identical — keys, sizes and "
                "contents all match%s\n",
                g.checksum ? " (contents verified)"
                           : " (every etag was conclusive)");
    } else {
        char u[32];
        s3m_fmt_u64(uv, u);
        fprintf(stderr, "  sides are very likely identical — keys, "
                "sizes and comparable etags all match (%s pair%s not "
                "content-verified — add -c)\n", u, uv == 1 ? "" : "s");
    }
}

/* ------------------------------------------------------------------ */
/* argument parsing / main                                              */
/* ------------------------------------------------------------------ */

static void usage(FILE *to)
{
    fputs(
"Usage: s3m-diff [OPTIONS] LEFT RIGHT\n"
"\n"
"Compare two sides in parallel and report every difference as CSV,\n"
"ending with a summary and a plain-language verdict. Each side is an\n"
"s3://BUCKET[/PREFIX] URI or a local directory (both local: use\n"
"p3m-diff). Read-only. Exit status is diff-like: 0 no differences,\n"
"1 differences found, 2 usage error or the comparison hit errors\n"
"(the verdict is withheld — coverage was incomplete).\n"
"\n"
"By default keys, sizes, and conclusive etags (single-part uploads\n"
"are content MD5s) are compared; local files have no etag, so a\n"
"local-vs-S3 run compares keys and sizes. -c verifies contents.\n"
"\n"
"Options:\n"
"  -c, --checksum       verify contents where the cheap checks are\n"
"                       inconclusive: local MD5 against a conclusive\n"
"                       etag where possible (no network I/O), else\n"
"                       chunked reads/ranged GETs stopping at the\n"
"                       first differing byte. Sizes gate everything:\n"
"                       size-mismatched objects are never read.\n"
"  -j, --threads N      worker threads, 1-256 (default: 16)\n"
"      --shard-depth N  prefix levels to expand for parallelism, 0-9\n"
"                       (default: 2)\n"
"      --rrdns          resolve the endpoint hostname to every A/AAAA\n"
"                       address it has and spread worker threads across\n"
"                       them (round robin), moving a thread to the next\n"
"                       address if its current one starts failing\n"
"  -o, --output FILE    write the CSV to FILE; show live progress\n"
"  -q, --quiet          suppress the listing; the summary and verdict\n"
"                       still print — a fast \"are these the same?\"\n"
"  -h, --help           show this help and exit\n"
"  -V, --version        show version and exit\n",
    to);
    fputs(s3m_common_usage, to);
}

int main(int argc, char **argv)
{
    static const struct option lopts[] = {
        { "checksum",    no_argument,       NULL, 'c' },
        { "threads",     required_argument, NULL, 'j' },
        { "shard-depth", required_argument, NULL, 1001 },
        { "rrdns",       no_argument,       NULL, 1002 },
        { "output",      required_argument, NULL, 'o' },
        { "quiet",       no_argument,       NULL, 'q' },
        { "help",        no_argument,       NULL, 'h' },
        { "version",     no_argument,       NULL, 'V' },
        S3M_COMMON_LOPTS,
        { 0, 0, 0, 0 }
    };

    int c;
    while ((c = getopt_long(argc, argv, "cj:o:qhV", lopts, NULL)) != -1) {
        if (s3m_common_opt(c, optarg))
            continue;
        switch (c) {
        case 'c':
            g.checksum = true;
            break;
        case 'j': {
            char *end;
            long v = strtol(optarg, &end, 10);
            if (*end || v < 1 || v > 256) {
                fprintf(stderr, "s3m-diff: invalid thread count '%s' "
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
                fprintf(stderr, "s3m-diff: invalid shard depth '%s' "
                        "(expected 0-9)\n", optarg);
                return 2;
            }
            g.shard_depth = (int)v;
            break;
        }
        case 1002:
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
            printf("s3m-diff %s (s3m: Parallel S3 Object Manager)\n",
                   S3M_DIFF_VERSION);
            return 0;
        default:
            usage(stderr);
            return 2;
        }
    }

    if (argc - optind != 2) {
        fprintf(stderr, "s3m-diff: expected exactly LEFT and RIGHT\n");
        usage(stderr);
        return 2;
    }

    for (int i = 0; i < 2; i++) {
        struct side *s = i ? &R : &L;
        const char *arg = argv[optind + i];
        s->is_s3 = !strncasecmp(arg, "s3://", 5);
        if (s->is_s3) {
            char *k;
            if (s3m_uri_parse("s3m-diff", arg, s->bucket, &k) != 0)
                return 2;
            if (k[0] && k[strlen(k) - 1] != '/') {
                char *n = s3m_strdupf("%s/", k);  /* directory-like */
                free(k);
                k = n;
            }
            if (!k) {
                fprintf(stderr, "s3m-diff: out of memory\n");
                return 2;
            }
            s->prefix = k;
        } else {
            char *d = strdup(arg);
            if (!d) {
                fprintf(stderr, "s3m-diff: out of memory\n");
                return 2;
            }
            size_t l = strlen(d);
            while (l > 1 && d[l - 1] == '/')
                d[--l] = '\0';
            struct stat st;
            if (stat(d, &st) != 0 || !S_ISDIR(st.st_mode)) {
                fprintf(stderr, "s3m-diff: '%s' is not a directory\n",
                        arg);
                return 2;
            }
            s->dir = d;
        }
    }
    if (!L.is_s3 && !R.is_s3) {
        fprintf(stderr, "s3m-diff: both sides are local — use p3m-diff "
                "for local trees\n");
        return 2;
    }

    s3m_color = isatty(STDERR_FILENO);
    g.suppress = g.quiet && !g.outpath;
    g.progress = (g.outpath || g.quiet) && isatty(STDERR_FILENO);

    if (s3m_config_finalize("s3m-diff") != 0)
        return 2;
    if (g.rrdns && s3m_endpoint_pool_init(&endpoints, "s3m-diff") != 0)
        return 2;
    if (s3m_http_global_init() != 0) {
        fprintf(stderr, "s3m-diff: could not initialise libcurl\n");
        return 2;
    }

    FILE *out;
    if (g.outpath) {
        out = fopen(g.outpath, "w");
        if (!out) {
            fprintf(stderr, "s3m-diff: cannot open '%s': %s\n",
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
        fputs("key,difference,left,right\n", out);

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

    /* local sides are walked on the main thread */
    struct wctx mw;
    mw.h = NULL;
    if (s3m_ob_init(&mw.ob, &sink) != 0) {
        fprintf(stderr, "s3m-diff: out of memory\n");
        return 2;
    }

    /* ---- phase 1: index left ---- */
    atomic_store(&cur_phase, 1);
    if (L.is_s3) {
        s3m_stack_init(&stk, g.nthreads);
        s3m_push_job(&stk, L.bucket, L.prefix, 0, 0);
        if (run_pool(true) != 0) {
            fprintf(stderr, "s3m-diff: could not start worker threads\n");
            return 2;
        }
        s3m_stack_destroy(&stk);
    } else {
        walk_local(&mw, L.dir, strlen(L.dir), true);
    }

    /* ---- phase 2: scan right, comparing ---- */
    atomic_store(&cur_phase, 2);
    if (R.is_s3) {
        s3m_stack_init(&stk, g.nthreads);
        s3m_push_job(&stk, R.bucket, R.prefix, 0, 0);
        if (run_pool(false) != 0) {
            fprintf(stderr, "s3m-diff: could not start worker threads\n");
            return 2;
        }
        s3m_stack_destroy(&stk);
    } else {
        walk_local(&mw, R.dir, strlen(R.dir), false);
    }
    s3m_ob_flush(&mw.ob);
    s3m_ob_free(&mw.ob);

    /* left entries the right side never matched */
    {
        s3m_outbuf ob;
        if (s3m_ob_init(&ob, &sink) == 0) {
            for (int i = 0; i < IDX_BUCKETS; i++) {
                for (struct dent *d = idx[i]; d; d = d->next) {
                    if (d->seen)
                        continue;
                    char ls[32];
                    snprintf(ls, sizeof ls, "%llu",
                             (unsigned long long)d->size);
                    atomic_fetch_add_explicit(&d_onlyl, 1,
                                              memory_order_relaxed);
                    emit_diff(&ob, d->rel, "only-left", ls, "");
                }
            }
            s3m_ob_flush(&ob);
            s3m_ob_free(&ob);
        }
    }

    /* ---- phase 3: content verification (-c) ---- */
    atomic_store(&cur_phase, 3);
    if (g.checksum && ncands > 0) {
        pthread_t tids[256];
        int started = 0;
        for (int i = 0; i < g.nthreads; i++) {
            if (pthread_create(&tids[i], NULL, verify_worker,
                               (void *)(intptr_t)i) != 0)
                break;
            started++;
        }
        if (started == 0) {
            fprintf(stderr, "s3m-diff: could not start worker threads\n");
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
        fprintf(stderr, "s3m-diff: %swrite error%s on %s — output is "
                "incomplete\n", C_RED, C_RESET,
                g.outpath ? g.outpath : "stdout");
        return 2;
    }

    print_summary(elapsed);
    s3m_print_errors();
    if (g.rrdns)
        s3m_endpoint_pool_destroy(&endpoints);

    if (atomic_load(&s3m_nerrors))
        return 2;
    return total_diffs() ? 1 : 0;
}
