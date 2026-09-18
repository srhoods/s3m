/*
 * s3m-sync — parallel synchroniser
 *
 * Part of s3m: Parallel S3 Object Manager
 *
 * Synchronises a local directory with a bucket/prefix, two
 * buckets/prefixes, or two local directories: local→S3 (upload),
 * S3→local (download), S3→S3 (server-side copy) and local→local
 * (parallel mirror). Copies only what is new or changed; --delete
 * removes destination entries absent from the source. Dry run by
 * default: --apply is the only way to change anything.
 *
 * Three phases: index the destination (parallel listing / local walk),
 * diff the source against the index to build the action plan, then
 * execute the plan across the worker pool.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "s3mcore.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>


#define S3M_SYNC_VERSION "1.2.0"

enum smode { M_UPLOAD, M_DOWNLOAD, M_REMOTE, M_LOCAL };

struct side {
    bool  is_s3;
    char  bucket[256];
    char *prefix;             /* s3: "" or ends with '/'                */
    char *dir;                /* local: no trailing '/'                 */
};

static struct side src, dst;

static struct {
    enum smode  mode;
    bool        apply;
    bool        del;          /* --delete                               */
    bool        size_only;
    bool        checksum;
    int         nthreads;
    int         shard_depth;
    const char *outpath;
    bool        quiet;
    bool        suppress;
    bool        progress;
    bool        rrdns;        /* --rrdns: spread threads across every    */
} g = { .nthreads = 16, .shard_depth = 2 };

static s3m_endpoint_pool endpoints;

/* phase counters */
static _Atomic uint64_t n_indexed;    /* destination entries indexed    */
static _Atomic uint64_t n_scanned;    /* source entries examined        */
static _Atomic uint64_t n_skipped;    /* source entries already in sync */
static _Atomic uint64_t n_tocopy, n_todel;
static _Atomic uint64_t n_copied, n_deleted;
static _Atomic uint64_t bytes_tocopy, bytes_done;
static _Atomic int      cur_phase;    /* 1 index · 2 diff · 3 transfer  */

static s3m_stack stk;                 /* phase 1/2 S3 listing jobs      */
static s3m_sink  sink;

/* ------------------------------------------------------------------ */
/* destination index                                                    */
/* ------------------------------------------------------------------ */

struct dent {
    char        *rel;
    uint64_t     size;
    time_t       mtime;
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

static void idx_add(const char *rel, uint64_t size, time_t mtime,
                    const char *etag)
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
    d->mtime = mtime;
    snprintf(d->etag, sizeof d->etag, "%s", etag ? etag : "");
    d->seen = false;
    uint64_t h = str_hash(rel) & (IDX_BUCKETS - 1);
    pthread_mutex_lock(&idx_mu);
    d->next = idx[h];
    idx[h] = d;
    pthread_mutex_unlock(&idx_mu);
    atomic_fetch_add_explicit(&n_indexed, 1, memory_order_relaxed);
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
/* action plan                                                          */
/* ------------------------------------------------------------------ */

enum akind { A_COPY, A_DELETE };

struct action {
    enum akind kind;
    char      *rel;
    uint64_t   size;
    time_t     mtime;         /* source mtime (downloads restore it)    */
};

static struct action  *acts;
static size_t          nacts, cacts;
static pthread_mutex_t acts_mu = PTHREAD_MUTEX_INITIALIZER;
static _Atomic size_t  next_act;

static void act_add(enum akind kind, const char *rel, uint64_t size,
                    time_t mtime)
{
    pthread_mutex_lock(&acts_mu);
    if (nacts == cacts) {
        size_t nc = cacts ? cacts * 2 : 1024;
        struct action *na = realloc(acts, nc * sizeof *na);
        if (!na) {
            pthread_mutex_unlock(&acts_mu);
            s3m_note_error(rel, "plan", "out of memory");
            return;
        }
        acts = na;
        cacts = nc;
    }
    struct action *a = &acts[nacts];
    a->kind = kind;
    a->rel = strdup(rel);
    a->size = size;
    a->mtime = mtime;
    if (!a->rel) {
        pthread_mutex_unlock(&acts_mu);
        s3m_note_error(rel, "plan", "out of memory");
        return;
    }
    nacts++;
    pthread_mutex_unlock(&acts_mu);

    if (kind == A_COPY) {
        atomic_fetch_add_explicit(&n_tocopy, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&bytes_tocopy, size,
                                  memory_order_relaxed);
    } else {
        atomic_fetch_add_explicit(&n_todel, 1, memory_order_relaxed);
    }
}

/* ------------------------------------------------------------------ */
/* CSV rows                                                             */
/* ------------------------------------------------------------------ */

static const char *action_name(enum akind k)
{
    if (k == A_DELETE)
        return (g.mode == M_DOWNLOAD || g.mode == M_LOCAL)
               ? "delete-local" : "delete";
    switch (g.mode) {
    case M_UPLOAD:   return "upload";
    case M_DOWNLOAD: return "download";
    default:         return "copy";
    }
}

static void emit_row(s3m_outbuf *ob, const char *rel, const char *action,
                     uint64_t size, const char *result)
{
    if (g.suppress)
        return;
    if (!s3m_ob_room(ob, 2 * (strlen(rel) + strlen(result)) + 256)) {
        s3m_note_error(rel, "emit", "row too long");
        return;
    }
    s3m_ob_csv(ob, rel);
    s3m_ob_fmt(ob, ",%s,%llu,", action, (unsigned long long)size);
    s3m_ob_csv(ob, result);
    s3m_ob_putc(ob, '\n');
}

/* ------------------------------------------------------------------ */
/* diff: does this source entry need copying?                           */
/* ------------------------------------------------------------------ */

static void diff_entry(const char *rel, uint64_t size, time_t mtime,
                       const char *etag, const char *local_path)
{
    atomic_fetch_add_explicit(&n_scanned, 1, memory_order_relaxed);
    s3m_set_current(rel);

    struct dent *d = idx_find(rel);
    if (d)
        d->seen = true;                /* single diff thread per entry:
                                          the source never lists a key
                                          twice, so no lock is needed */
    bool copy;
    if (!d) {
        copy = true;
    } else if (size != d->size) {
        copy = true;
    } else if (g.checksum) {
        /* etag-based comparison where conclusive, mtime otherwise.
         * In download mode the local file is the destination side. */
        const char *src_etag = etag;
        char md5[33];
        char *dlpath = (g.mode == M_DOWNLOAD)
                     ? s3m_strdupf("%s/%s", dst.dir, rel) : NULL;
        if (g.mode == M_UPLOAD && !strchr(d->etag, '-') && d->etag[0] &&
            s3m_file_md5(local_path, md5) == 0)
            copy = strcasecmp(md5, d->etag) != 0;
        else if (g.mode == M_DOWNLOAD && src_etag && src_etag[0] &&
                 !strchr(src_etag, '-') && dlpath &&
                 s3m_file_md5(dlpath, md5) == 0)
            copy = strcasecmp(md5, src_etag) != 0;
        else if (g.mode == M_REMOTE && src_etag && src_etag[0] &&
                 d->etag[0])
            copy = strcasecmp(src_etag, d->etag) != 0;
        else if (g.mode == M_LOCAL) {
            /* both sides are files: hash them both */
            char smd5[33], dmd5[33];
            char *dp = s3m_strdupf("%s/%s", dst.dir, rel);
            if (dp && s3m_file_md5(local_path, smd5) == 0 &&
                s3m_file_md5(dp, dmd5) == 0)
                copy = strcmp(smd5, dmd5) != 0;
            else
                copy = mtime > d->mtime;
            free(dp);
        }
        else
            copy = mtime > d->mtime;
        free(dlpath);
    } else if (g.size_only) {
        copy = false;
    } else {
        copy = mtime > d->mtime;
    }

    if (copy)
        act_add(A_COPY, rel, size, mtime);
    else
        atomic_fetch_add_explicit(&n_skipped, 1, memory_order_relaxed);
}

/* ------------------------------------------------------------------ */
/* S3 listing callbacks (phases 1 and 2)                                */
/* ------------------------------------------------------------------ */

static void on_index_obj(void *ctx, const s3m_obj *o)
{
    (void)ctx;
    size_t plen = strlen(dst.prefix);
    if (strncmp(o->key, dst.prefix, plen) != 0)
        return;
    const char *rel = o->key + plen;
    if (!rel[0] || rel[strlen(rel) - 1] == '/')
        return;                        /* skip "directory" placeholders */
    s3m_set_current(o->key);
    idx_add(rel, o->size, o->mtime, o->etag);
}

static void on_src_obj(void *ctx, const s3m_obj *o)
{
    (void)ctx;
    size_t plen = strlen(src.prefix);
    if (strncmp(o->key, src.prefix, plen) != 0)
        return;
    const char *rel = o->key + plen;
    if (!rel[0] || rel[strlen(rel) - 1] == '/')
        return;
    diff_entry(rel, o->size, o->mtime, o->etag, NULL);
}

struct list_arg {
    s3m_obj_cb cb;
    int        idx;            /* worker index: initial endpoint pick   */
};

static void *list_worker(void *arg)
{
    struct list_arg *la = arg;
    s3m_http *h = s3m_http_new();
    if (!h) {
        s3m_note_error("worker", "startup", "out of memory");
        char *j;
        while ((j = s3m_stack_pop(&stk)) != NULL)
            free(j);
        return NULL;
    }
    if (g.rrdns)
        s3m_http_pin_endpoint(h, &endpoints, (size_t)la->idx);
    char *job;
    while ((job = s3m_stack_pop(&stk)) != NULL) {
        char *bucket, *prefix;
        int depth = s3m_job_parse(job, &bucket, &prefix, NULL);
        if (depth >= 0)
            s3m_list_job(h, &stk, bucket, prefix, depth, g.shard_depth,
                         0, false, false, la->cb, NULL);
        free(job);
        if (g.rrdns && s3m_http_transport_failed(h))
            s3m_http_rotate_endpoint(h, &endpoints);
    }
    s3m_http_free(h);
    return NULL;
}

/* run a parallel listing of `side` through callback `cb` */
static int run_listing(struct side *s, s3m_obj_cb cb)
{
    s3m_stack_init(&stk, g.nthreads);
    s3m_push_job(&stk, s->bucket, s->prefix, 0, 0);
    pthread_t tids[256];
    struct list_arg args[256];
    int started = 0;
    for (int i = 0; i < g.nthreads; i++) {
        args[i].cb = cb;
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
    s3m_stack_destroy(&stk);
    return 0;
}

/* ------------------------------------------------------------------ */
/* local walks (index and diff sides)                                   */
/* ------------------------------------------------------------------ */

static void walk_local(const char *root, size_t rootlen, bool indexing)
{
    DIR *d = opendir(root);
    if (!d) {
        if (indexing && errno == ENOENT)
            return;                    /* missing dst dir = empty index */
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
            free(fp);
            continue;
        }
        if (S_ISDIR(st.st_mode)) {
            walk_local(fp, rootlen, indexing);
        } else if (S_ISREG(st.st_mode)) {
            const char *rel = fp + rootlen + 1;
            if (indexing) {
                s3m_set_current(rel);
                idx_add(rel, (uint64_t)st.st_size, st.st_mtime, NULL);
            } else {
                diff_entry(rel, (uint64_t)st.st_size, st.st_mtime,
                           NULL, fp);
            }
        }
        /* symlinks and special files are never followed or synced */
        free(fp);
    }
    closedir(d);
}

/* ------------------------------------------------------------------ */
/* transfers (the heavy lifting lives in s3mcore)                       */
/* ------------------------------------------------------------------ */

/* local→local: atomic copy via a temp file in the target directory,
 * copy_file_range with a read/write fallback, source mtime preserved
 * so re-runs converge */
static int local_copy_file(const char *spath, const char *dpath,
                           time_t mtime, char *err, size_t errsz)
{
    if (s3m_mkdirs_for(dpath) != 0) {
        snprintf(err, errsz, "mkdir: %s", strerror(errno));
        return -1;
    }
    int sfd = open(spath, O_RDONLY | O_CLOEXEC);
    if (sfd < 0) {
        snprintf(err, errsz, "open: %s", strerror(errno));
        return -1;
    }
    char *tmp = s3m_strdupf("%s.s3m-tmp-XXXXXX", dpath);
    int dfd = tmp ? mkstemp(tmp) : -1;
    if (dfd < 0) {
        snprintf(err, errsz, "mkstemp: %s", strerror(errno));
        close(sfd);
        free(tmp);
        return -1;
    }

    int rc = 0;
    static __thread char *cpbuf;          /* 1 MiB fallback buffer */
    for (;;) {
        ssize_t r = copy_file_range(sfd, NULL, dfd, NULL,
                                    (size_t)4 << 20, 0);
        if (r > 0) {
            atomic_fetch_add_explicit(&bytes_done, (uint64_t)r,
                                      memory_order_relaxed);
            continue;
        }
        if (r == 0)
            break;
        if (errno != EINVAL && errno != EXDEV &&
            errno != ENOSYS && errno != EOPNOTSUPP) {
            snprintf(err, errsz, "copy: %s", strerror(errno));
            rc = -1;
            break;
        }
        /* fall back to read/write (cross-device etc.) */
        if (!cpbuf && !(cpbuf = malloc(1 << 20))) {
            snprintf(err, errsz, "out of memory");
            rc = -1;
            break;
        }
        for (;;) {
            ssize_t n = read(sfd, cpbuf, 1 << 20);
            if (n == 0)
                break;
            if (n < 0) {
                snprintf(err, errsz, "read: %s", strerror(errno));
                rc = -1;
                break;
            }
            char *p = cpbuf;
            while (n > 0) {
                ssize_t w = write(dfd, p, (size_t)n);
                if (w < 0) {
                    snprintf(err, errsz, "write: %s", strerror(errno));
                    rc = -1;
                    break;
                }
                p += w;
                n -= w;
            }
            if (rc != 0)
                break;
            atomic_fetch_add_explicit(&bytes_done,
                                      (uint64_t)(p - cpbuf),
                                      memory_order_relaxed);
        }
        break;
    }
    close(sfd);
    if (rc == 0) {
        if (mtime != (time_t)-1) {
            struct timespec ts[2] = { { mtime, 0 }, { mtime, 0 } };
            futimens(dfd, ts);
        }
        if (close(dfd) != 0 || rename(tmp, dpath) != 0) {
            snprintf(err, errsz, "rename: %s", strerror(errno));
            rc = -1;
        }
    } else {
        close(dfd);
    }
    if (rc != 0)
        unlink(tmp);
    free(tmp);
    return rc;
}

/* execute one copy action; returns 0, or -1 with a message in err */
static int do_copy(s3m_http *h, const struct action *a,
                   char *err, size_t errsz)
{
    int rc = -1;

    if (g.mode == M_UPLOAD) {
        char *path = s3m_strdupf("%s/%s", src.dir, a->rel);
        char *key  = s3m_strdupf("%s%s", dst.prefix, a->rel);
        int fd = path ? open(path, O_RDONLY | O_CLOEXEC) : -1;
        if (fd < 0) {
            snprintf(err, errsz, "open: %s", strerror(errno));
        } else {
            rc = s3m_upload_file(h, dst.bucket, key, fd, a->size,
                                 s3m_mime_type(a->rel), &bytes_done,
                                 err, errsz);
            close(fd);
        }
        free(path);
        free(key);
    } else if (g.mode == M_DOWNLOAD) {
        char *path = s3m_strdupf("%s/%s", dst.dir, a->rel);
        char *key  = s3m_strdupf("%s%s", src.prefix, a->rel);
        if (path && key)
            rc = s3m_download_file(h, src.bucket, key, path, a->mtime,
                                   &bytes_done, err, errsz);
        else
            snprintf(err, errsz, "out of memory");
        free(path);
        free(key);
    } else if (g.mode == M_REMOTE) {  /* server-side copy */
        char *skey = s3m_strdupf("%s%s", src.prefix, a->rel);
        char *dkey = s3m_strdupf("%s%s", dst.prefix, a->rel);
        if (skey && dkey)
            rc = s3m_copy_object(h, src.bucket, skey, dst.bucket, dkey,
                                 a->size, &bytes_done, err, errsz);
        free(skey);
        free(dkey);
    } else {                          /* M_LOCAL */
        char *spath = s3m_strdupf("%s/%s", src.dir, a->rel);
        char *dpath = s3m_strdupf("%s/%s", dst.dir, a->rel);
        if (spath && dpath)
            rc = local_copy_file(spath, dpath, a->mtime, err, errsz);
        else
            snprintf(err, errsz, "out of memory");
        free(spath);
        free(dpath);
    }
    return rc;
}

/* ------------------------------------------------------------------ */
/* phase 3 workers                                                      */
/* ------------------------------------------------------------------ */

struct wctx {
    s3m_http    *h;
    s3m_outbuf   ob;
    s3m_delbatch batch;
};

static void on_deleted(void *ctx, const char *key, const char *vid,
                       bool ok, const char *code, const char *msg)
{
    struct wctx *w = ctx;
    (void)vid;
    /* strip the destination prefix back off for the report */
    const char *rel = key;
    size_t plen = strlen(dst.prefix);
    if (!strncmp(key, dst.prefix, plen))
        rel = key + plen;
    if (ok) {
        atomic_fetch_add_explicit(&n_deleted, 1, memory_order_relaxed);
        emit_row(&w->ob, rel, "delete", 0, "done");
    } else {
        char res[512];
        snprintf(res, sizeof res, "failed: %s: %s", code, msg);
        s3m_note_error(key, "delete", res + 8);
        emit_row(&w->ob, rel, "delete", 0, res);
    }
}

static void *exec_worker(void *arg)
{
    int idx = (int)(intptr_t)arg;
    struct wctx w;
    w.h = s3m_http_new();
    if (!w.h || s3m_ob_init(&w.ob, &sink) != 0) {
        s3m_note_error("worker", "startup", "out of memory");
        s3m_http_free(w.h);
        return NULL;
    }
    if (g.rrdns)
        s3m_http_pin_endpoint(w.h, &endpoints, (size_t)idx);
    s3m_delbatch_init(&w.batch, dst.bucket, on_deleted, &w);

    for (;;) {
        size_t i = atomic_fetch_add(&next_act, 1);
        if (i >= nacts)
            break;
        struct action *a = &acts[i];
        s3m_set_current(a->rel);

        if (a->kind == A_DELETE) {
            if (g.mode == M_DOWNLOAD || g.mode == M_LOCAL) {
                char *path = s3m_strdupf("%s/%s", dst.dir, a->rel);
                if (path && (unlink(path) == 0 || errno == ENOENT)) {
                    atomic_fetch_add_explicit(&n_deleted, 1,
                                              memory_order_relaxed);
                    emit_row(&w.ob, a->rel, "delete-local", 0, "done");
                } else {
                    char res[256];
                    snprintf(res, sizeof res, "failed: unlink: %s",
                             strerror(errno));
                    s3m_note_error(a->rel, "delete", res + 8);
                    emit_row(&w.ob, a->rel, "delete-local", 0, res);
                }
                free(path);
            } else {
                char *key = s3m_strdupf("%s%s", dst.prefix, a->rel);
                if (key)
                    s3m_delbatch_add(&w.batch, w.h, key, NULL);
                free(key);
                if (g.rrdns && s3m_http_transport_failed(w.h))
                    s3m_http_rotate_endpoint(w.h, &endpoints);
            }
            continue;
        }

        char err[320] = "";
        if (do_copy(w.h, a, err, sizeof err) == 0) {
            atomic_fetch_add_explicit(&n_copied, 1, memory_order_relaxed);
            emit_row(&w.ob, a->rel, action_name(A_COPY), a->size, "done");
        } else {
            char res[400];
            snprintf(res, sizeof res, "failed: %s", err);
            s3m_note_error(a->rel, action_name(A_COPY), err);
            emit_row(&w.ob, a->rel, action_name(A_COPY), a->size, res);
        }
        if (g.rrdns && s3m_http_transport_failed(w.h))
            s3m_http_rotate_endpoint(w.h, &endpoints);
    }
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
    static const char *phases[] = { "", "index destination",
                                    "diff source", "transfer" };
    int  ph    = atomic_load_explicit(&cur_phase, memory_order_relaxed);
    uint64_t nidx = atomic_load_explicit(&n_indexed, memory_order_relaxed);
    uint64_t nsc  = atomic_load_explicit(&n_scanned, memory_order_relaxed);
    uint64_t ncp  = atomic_load_explicit(&n_copied, memory_order_relaxed);
    uint64_t tcp  = atomic_load_explicit(&n_tocopy, memory_order_relaxed);
    uint64_t ndl  = atomic_load_explicit(&n_deleted, memory_order_relaxed);
    uint64_t tdl  = atomic_load_explicit(&n_todel, memory_order_relaxed);
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

    char iv[32], sv[32], cv[32], tv[32], dv[32], tdv[32], ev[32],
         bdv[32], btv[32], rv[32], el[32];
    s3m_fmt_u64(nidx, iv);
    s3m_fmt_u64(nsc, sv);
    s3m_fmt_u64(ncp, cv);
    s3m_fmt_u64(tcp, tv);
    s3m_fmt_u64(ndl, dv);
    s3m_fmt_u64(tdl, tdv);
    s3m_fmt_u64(errs, ev);
    s3m_fmt_size(bd, bdv);
    s3m_fmt_size(bt, btv);
    s3m_fmt_size((uint64_t)(rate + 0.5), rv);
    s3m_fmt_elapsed(s3m_mono_now() - t_start, el);

    char copystr[80], delstr[80], bytestr[80], ratestr[48];
    snprintf(copystr, sizeof copystr, "%s / %s", cv, tv);
    snprintf(delstr, sizeof delstr, "%s / %s", dv, tdv);
    snprintf(bytestr, sizeof bytestr, "%s / %s", bdv, btv);
    snprintf(ratestr, sizeof ratestr, "%s/s", rv);

    static const char *modes[] = { "upload", "download", "copy", "local" };

    char buf[4096];
    size_t off = 0;
#define ADD(...) off += (size_t)snprintf(buf + off, sizeof buf - off, __VA_ARGS__)
    ADD("\x1b[K%s%s%s %ss3m-sync%s %s— parallel synchronise (%s, %s)%s\n",
        C_CYAN, s3m_spinner[frame % 10], C_RESET, C_BOLD, C_RESET,
        C_DIM, modes[g.mode], g.apply ? "apply" : "dry run", C_RESET);
    ADD("\x1b[K  %s%-9s%s %s\n", C_DIM, "key", C_RESET, ptr);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %d\n",
        C_DIM, "phase", C_RESET, phases[ph],
        C_DIM, "threads", C_RESET, g.nthreads);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s\n",
        C_DIM, "indexed", C_RESET, iv, C_DIM, "scanned", C_RESET, sv);
    ADD("\x1b[K  %s%-9s%s %-14s %s%-8s%s %s\n",
        C_DIM, "copied", C_RESET, copystr, C_DIM, "deleted", C_RESET,
        delstr);
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
    uint64_t nsk  = atomic_load(&n_skipped);
    uint64_t ncp  = atomic_load(&n_copied);
    uint64_t tcp  = atomic_load(&n_tocopy);
    uint64_t ndl  = atomic_load(&n_deleted);
    uint64_t tdl  = atomic_load(&n_todel);
    uint64_t bt   = atomic_load(&bytes_tocopy);
    uint64_t bd   = atomic_load(&bytes_done);
    uint64_t errs = atomic_load(&s3m_nerrors);

    static const char *modes[] = { "upload", "download", "copy", "local" };

    char sv[32], kv[32], cv[32], tv[32], dv[32], tdv[32], ev[32],
         bv[32], el[32], rv[32];
    s3m_fmt_u64(nsc, sv);
    s3m_fmt_u64(nsk, kv);
    s3m_fmt_u64(ncp, cv);
    s3m_fmt_u64(tcp, tv);
    s3m_fmt_u64(ndl, dv);
    s3m_fmt_u64(tdl, tdv);
    s3m_fmt_u64(errs, ev);
    s3m_fmt_size(g.apply ? bd : bt, bv);
    s3m_fmt_elapsed(elapsed, el);
    s3m_fmt_size(elapsed > 0 ? (uint64_t)((double)bd / elapsed) : 0, rv);

    fprintf(stderr,
            "%s✓%s %ss3m-sync%s complete — %s %s · %s scanned · "
            "%s in sync",
            C_GREEN, C_RESET, C_BOLD, C_RESET, modes[g.mode],
            g.apply ? "apply" : "dry run", sv, kv);
    if (g.apply) {
        fprintf(stderr, " · %s", cv);
        if (ncp != tcp)
            fprintf(stderr, " of %s", tv);
        fprintf(stderr, " copied");
        if (g.del) {
            fprintf(stderr, " · %s", dv);
            if (ndl != tdl)
                fprintf(stderr, " of %s", tdv);
            fprintf(stderr, " deleted");
        }
    } else {
        fprintf(stderr, " · %s to copy", tv);
        if (g.del)
            fprintf(stderr, " · %s to delete", tdv);
    }
    fprintf(stderr, " · %s%s error%s%s\n", errs ? C_RED : "", ev,
            errs == 1 ? "" : "s", errs ? C_RESET : "");
    fprintf(stderr, "  %s in %s", bv, el);
    if (g.apply && bd)
        fprintf(stderr, " (%s/s)", rv);
    if (g.outpath)
        fprintf(stderr, " → %s", g.outpath);
    fputc('\n', stderr);
    if (!g.apply && (tcp || tdl))
        fprintf(stderr, "  %sdry run — nothing was changed; add --apply "
                "to synchronise%s\n", C_BOLD, C_RESET);
}

/* ------------------------------------------------------------------ */
/* argument parsing / main                                              */
/* ------------------------------------------------------------------ */

static void usage(FILE *to)
{
    fputs(
"Usage: s3m-sync [OPTIONS] SRC DST\n"
"\n"
"Synchronise SRC to DST, copying only what is new or changed. SRC and\n"
"DST are a local directory or an s3://BUCKET[/PREFIX] URI:\n"
"  local dir  -> s3://…      upload\n"
"  s3://…     -> local dir   download\n"
"  s3://…     -> s3://…      server-side copy (same endpoint)\n"
"  local dir  -> local dir   parallel local mirror (atomic copies,\n"
"                            source mtimes preserved)\n"
"\n"
"By default this is a DRY RUN: the plan is printed as CSV and nothing\n"
"is changed. Add --apply to synchronise.\n"
"\n"
"Options:\n"
"      --apply           actually copy/delete (otherwise: dry run)\n"
"      --delete          also remove destination entries that do not\n"
"                        exist in the source\n"
"      --size-only       compare by size alone (ignore timestamps)\n"
"      --checksum        compare by MD5/etag where conclusive (single-\n"
"                        part etags); falls back to size+mtime\n"
"  -j, --threads N       worker threads, 1-256 (default: 16)\n"
"      --shard-depth N   prefix levels to expand for parallelism, 0-9\n"
"                        (default: 2)\n"
"      --rrdns           resolve the endpoint hostname to every A/AAAA\n"
"                        address it has and spread worker threads across\n"
"                        them (round robin), moving a thread to the next\n"
"                        address if its current one starts failing\n"
"  -o, --output FILE     write the CSV plan/report to FILE; show progress\n"
"  -q, --quiet           suppress the console listing (progress and the\n"
"                        summary are still shown)\n"
"  -h, --help            show this help and exit\n"
"  -V, --version         show version and exit\n",
    to);
    fputs(s3m_common_usage, to);
    fputs(
"\n"
"Comparison: an entry is copied when it is missing from the destination,\n"
"its size differs, or the source is newer (unless --size-only). Uploads\n"
"larger than 128 MiB use multipart; downloads write to a temp file and\n"
"are renamed into place with mtime set to the object's LastModified.\n"
"Symbolic links are never followed or synced.\n",
    to);
}

static int parse_side(const char *tool, const char *arg, struct side *s)
{
    if (!strncasecmp(arg, "s3://", 5)) {
        s->is_s3 = true;
        char *key;
        if (s3m_uri_parse(tool, arg, s->bucket, &key) != 0)
            return -1;
        size_t kl = strlen(key);
        if (kl && key[kl - 1] != '/') {
            char *nk = s3m_strdupf("%s/", key);
            free(key);
            if (!nk)
                return -1;
            key = nk;
        }
        s->prefix = key;
    } else {
        s->is_s3 = false;
        char *d = strdup(arg);
        if (!d)
            return -1;
        size_t l = strlen(d);
        while (l > 1 && d[l - 1] == '/')
            d[--l] = '\0';
        s->dir = d;
    }
    return 0;
}

int main(int argc, char **argv)
{
    static const struct option lopts[] = {
        { "apply",       no_argument,       NULL, 1001 },
        { "delete",      no_argument,       NULL, 1002 },
        { "size-only",   no_argument,       NULL, 1003 },
        { "checksum",    no_argument,       NULL, 1004 },
        { "threads",     required_argument, NULL, 'j' },
        { "shard-depth", required_argument, NULL, 1005 },
        { "rrdns",       no_argument,       NULL, 1006 },
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
            g.del = true;
            break;
        case 1003:
            g.size_only = true;
            break;
        case 1004:
            g.checksum = true;
            break;
        case 'j': {
            char *end;
            long v = strtol(optarg, &end, 10);
            if (*end || v < 1 || v > 256) {
                fprintf(stderr, "s3m-sync: invalid thread count '%s' "
                        "(expected 1-256)\n", optarg);
                return 2;
            }
            g.nthreads = (int)v;
            break;
        }
        case 1005: {
            char *end;
            long v = strtol(optarg, &end, 10);
            if (*end || v < 0 || v > 9) {
                fprintf(stderr, "s3m-sync: invalid shard depth '%s' "
                        "(expected 0-9)\n", optarg);
                return 2;
            }
            g.shard_depth = (int)v;
            break;
        }
        case 1006:
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
            printf("s3m-sync %s (s3m: Parallel S3 Object Manager)\n",
                   S3M_SYNC_VERSION);
            return 0;
        default:
            usage(stderr);
            return 2;
        }
    }

    if (argc - optind != 2) {
        fprintf(stderr, "s3m-sync: expected exactly SRC and DST\n");
        usage(stderr);
        return 2;
    }
    if (parse_side("s3m-sync", argv[optind], &src) != 0 ||
        parse_side("s3m-sync", argv[optind + 1], &dst) != 0)
        return 2;

    if (src.is_s3 && dst.is_s3)
        g.mode = M_REMOTE;
    else if (!src.is_s3 && dst.is_s3)
        g.mode = M_UPLOAD;
    else if (src.is_s3 && !dst.is_s3)
        g.mode = M_DOWNLOAD;
    else
        g.mode = M_LOCAL;
    if (g.size_only && g.checksum) {
        fprintf(stderr, "s3m-sync: --size-only and --checksum are "
                "mutually exclusive\n");
        return 2;
    }
    if (g.mode == M_UPLOAD || g.mode == M_LOCAL) {
        struct stat st;
        if (stat(src.dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
            fprintf(stderr, "s3m-sync: '%s' is not a directory\n",
                    src.dir);
            return 2;
        }
    }
    if (g.mode == M_LOCAL) {
        /* refuse syncing a directory into itself or its own subtree —
         * resolved before any work starts, symlinks and all. A
         * destination that does not exist yet is judged by its deepest
         * existing ancestor (it would be created inside that). */
        char rs[PATH_MAX], rd[PATH_MAX] = "";
        if (realpath(src.dir, rs)) {
            char probe[PATH_MAX];
            snprintf(probe, sizeof probe, "%s", dst.dir);
            while (!realpath(probe, rd)) {
                rd[0] = '\0';
                char *sl = strrchr(probe, '/');
                if (!sl) {
                    if (!realpath(".", rd))
                        rd[0] = '\0';
                    break;
                }
                *sl = '\0';
                if (!probe[0]) {
                    snprintf(probe, sizeof probe, "/");
                    if (!realpath(probe, rd))
                        rd[0] = '\0';
                    break;
                }
            }
            size_t rl = strlen(rs);
            if (rd[0] && (!strcmp(rs, rd) ||
                          (!strncmp(rd, rs, rl) && rd[rl] == '/'))) {
                fprintf(stderr, "s3m-sync: refusing to sync '%s' into "
                        "itself\n", src.dir);
                return 2;
            }
        }
    }

    if (g.rrdns && g.mode == M_LOCAL) {
        fprintf(stderr,
                "s3m-sync: --rrdns has no effect on a local->local "
                "sync (no S3 endpoint involved)\n");
        return 2;
    }

    s3m_color = isatty(STDERR_FILENO);
    g.suppress = g.quiet && !g.outpath;
    g.progress = (g.outpath || g.quiet) && isatty(STDERR_FILENO);

    if (s3m_config_finalize("s3m-sync") != 0)
        return 2;
    if (g.rrdns && s3m_endpoint_pool_init(&endpoints, "s3m-sync") != 0)
        return 2;
    if (s3m_http_global_init() != 0) {
        fprintf(stderr, "s3m-sync: could not initialise libcurl\n");
        return 2;
    }

    FILE *out;
    if (g.outpath) {
        out = fopen(g.outpath, "w");
        if (!out) {
            fprintf(stderr, "s3m-sync: cannot open '%s': %s\n",
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
        fputs("key,action,size,result\n", out);

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

    /* ---- phase 1: index the destination ---- */
    atomic_store(&cur_phase, 1);
    if (dst.is_s3) {
        if (run_listing(&dst, on_index_obj) != 0) {
            fprintf(stderr, "s3m-sync: could not start worker threads\n");
            return 2;
        }
    } else {
        walk_local(dst.dir, strlen(dst.dir), true);
    }
    uint64_t idx_errors = atomic_load(&s3m_nerrors);

    /* ---- phase 2: diff the source, building the plan ---- */
    atomic_store(&cur_phase, 2);
    if (src.is_s3) {
        if (run_listing(&src, on_src_obj) != 0) {
            fprintf(stderr, "s3m-sync: could not start worker threads\n");
            return 2;
        }
    } else {
        walk_local(src.dir, strlen(src.dir), false);
    }

    /* --delete: destination entries the source never touched.
     * If indexing hit errors the index may be incomplete — deleting
     * from it would be guesswork, so refuse that part of the job. */
    if (g.del && idx_errors) {
        fprintf(stderr, "s3m-sync: %s--delete skipped%s — the "
                "destination index is incomplete (%llu error%s)\n",
                C_RED, C_RESET, (unsigned long long)idx_errors,
                idx_errors == 1 ? "" : "s");
    } else if (g.del) {
        for (int i = 0; i < IDX_BUCKETS; i++)
            for (struct dent *d = idx[i]; d; d = d->next)
                if (!d->seen)
                    act_add(A_DELETE, d->rel, d->size, d->mtime);
    }

    /* ---- phase 3: dry-run report, or execute the plan ---- */
    atomic_store(&cur_phase, 3);
    int rc2 = 0;
    if (!g.apply) {
        s3m_outbuf ob;
        if (s3m_ob_init(&ob, &sink) == 0) {
            for (size_t i = 0; i < nacts; i++)
                emit_row(&ob, acts[i].rel, action_name(acts[i].kind),
                         acts[i].size, "pending");
            s3m_ob_flush(&ob);
            s3m_ob_free(&ob);
        }
    } else if (nacts > 0) {
        pthread_t tids[256];
        int started = 0;
        for (int i = 0; i < g.nthreads; i++) {
            if (pthread_create(&tids[i], NULL, exec_worker,
                               (void *)(intptr_t)i) != 0)
                break;
            started++;
        }
        if (started == 0) {
            fprintf(stderr, "s3m-sync: could not start worker threads\n");
            rc2 = 2;
        }
        for (int i = 0; i < started; i++)
            pthread_join(tids[i], NULL);
    }

    double elapsed = s3m_mono_now() - t_start;

    if (g.progress)
        s3m_progress_stop();
    if (rc2)
        return rc2;

    if (fflush(out) != 0 || ferror(out))
        atomic_store(&sink.failed, true);
    if (g.outpath && fclose(out) != 0)
        atomic_store(&sink.failed, true);

    if (atomic_load(&sink.failed)) {
        fprintf(stderr, "s3m-sync: %swrite error%s on %s — output is "
                "incomplete\n", C_RED, C_RESET,
                g.outpath ? g.outpath : "stdout");
        return 1;
    }

    print_summary(elapsed);
    s3m_print_errors();

    if (g.rrdns)
        s3m_endpoint_pool_destroy(&endpoints);

    return atomic_load(&s3m_nerrors) ? 1 : 0;
}
