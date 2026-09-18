/*
 * s3mcore — shared engine for the s3m tool suite
 *
 * S3 REST transport (libcurl, one persistent handle per worker thread),
 * SigV4 request signing (OpenSSL), retrying with jittered backoff, a
 * minimal XML reader for S3 response documents, endpoint/credential
 * configuration, the parallel prefix-sharded listing engine, the batched
 * DeleteObjects pump, and the buffered CSV output, error accounting and
 * live progress scaffolding shared with the p3m suite.
 */
#ifndef S3MCORE_H
#define S3MCORE_H

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

/* ---- colours (tools set s3m_color once at startup) ------------------ */

extern bool s3m_color;
#define C_RESET (s3m_color ? "\x1b[0m"  : "")
#define C_BOLD  (s3m_color ? "\x1b[1m"  : "")
#define C_DIM   (s3m_color ? "\x1b[2m"  : "")
#define C_RED   (s3m_color ? "\x1b[31m" : "")
#define C_GREEN (s3m_color ? "\x1b[32m" : "")
#define C_CYAN  (s3m_color ? "\x1b[36m" : "")

/* ---- formatting helpers --------------------------------------------- */

double s3m_mono_now(void);
char  *s3m_fmt_u64(uint64_t v, char out[32]);      /* 1234567 -> 1,234,567 */
char  *s3m_fmt_size(uint64_t b, char out[32]);     /* 1234567 -> 1.18 MiB  */
char  *s3m_fmt_elapsed(double s, char out[32]);
void   s3m_fmt_time(time_t t, char out[32]);       /* ISO 8601 UTC         */
int    s3m_term_width(void);
void   s3m_trunc_left(const char *s, size_t max, char *out, size_t outsz);
char  *s3m_strdupf(const char *fmt, ...)
    __attribute__((format(printf, 1, 2)));

/* "30d", "12h", "45m", "10s", "4w" -> seconds; 0 ok, -1 bad */
int    s3m_parse_dur(const char *s, int64_t *secs);
/* "2026-07-16T02:03:04.000Z" -> time_t; (time_t)-1 on error */
time_t s3m_parse_iso8601(const char *s);
/* percent-encode for URLs/SigV4; keep_slash leaves '/' literal */
void   s3m_urlenc(const char *s, bool keep_slash, char *out, size_t outsz);
void   s3m_urldec(char *s);                        /* %XX decode in place  */

/* ---- error accounting ----------------------------------------------- */

extern _Atomic uint64_t s3m_nerrors;
void s3m_note_error(const char *subject, const char *what, const char *msg);
void s3m_print_errors(void);                       /* report on stderr */

/* ---- "current key" shown by the progress display --------------------- */

void s3m_set_current(const char *p);
void s3m_get_current(char *dst, size_t n);

/* ---- parallel work stack of prefix jobs ------------------------------ */

typedef struct {
    char           **items;
    size_t           len, cap;
    int              idle, nthreads;
    bool             done;
    pthread_mutex_t  mu;
    pthread_cond_t   cv;
} s3m_stack;

void  s3m_stack_init(s3m_stack *s, int nthreads);
void  s3m_stack_set_threads(s3m_stack *s, int nthreads);
/* takes ownership of the strings (malloc'd); frees them on queue failure */
void  s3m_stack_push_batch(s3m_stack *s, char **jobs, size_t n);
/* returns a job (caller frees), or NULL when the walk is done */
char *s3m_stack_pop(s3m_stack *s);
size_t s3m_stack_pending(s3m_stack *s);
void  s3m_stack_destroy(s3m_stack *s);

/* listing jobs are "<depth digit><tag byte><bucket>\x01<prefix>"
 * strings; the tag (0-63) is an arbitrary tool-defined label that
 * sub-jobs inherit (e.g. which command-line target spawned the walk) */
char *s3m_job_make(const char *bucket, const char *prefix, int depth,
                   int tag);
void  s3m_push_job(s3m_stack *s, const char *bucket, const char *prefix,
                   int depth, int tag);
/* splits a popped job in place; returns the depth, or -1 if malformed */
int   s3m_job_parse(char *job, char **bucket, char **prefix, int *tag);

/* ---- buffered, thread-safe output ------------------------------------ */

#define S3M_OB_CAP ((size_t)1 << 20)

typedef struct {
    FILE           *f;
    pthread_mutex_t mu;
    atomic_bool     failed;
} s3m_sink;

typedef struct {
    char     *buf;
    size_t    len;
    s3m_sink *sink;
} s3m_outbuf;

void s3m_sink_init(s3m_sink *k, FILE *f);
int  s3m_ob_init(s3m_outbuf *ob, s3m_sink *k);     /* 0 ok, -1 ENOMEM */
void s3m_ob_free(s3m_outbuf *ob);
void s3m_ob_flush(s3m_outbuf *ob);
/* ensure `need` bytes are free (flushing if necessary); false = too big */
bool s3m_ob_room(s3m_outbuf *ob, size_t need);
void s3m_ob_puts(s3m_outbuf *ob, const char *s);
void s3m_ob_putc(s3m_outbuf *ob, char c);
void s3m_ob_fmt(s3m_outbuf *ob, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
void s3m_ob_csv(s3m_outbuf *ob, const char *s);    /* RFC 4180 quoting */

/* ---- live progress display scaffolding ------------------------------- */

typedef struct {
    void     (*draw)(double rate, int frame);
    uint64_t (*items)(void);       /* monotonic counter used for the rate */
    int        lines;
} s3m_progress_cfg;

int  s3m_progress_start(const s3m_progress_cfg *cfg);
void s3m_progress_stop(void);      /* joins, clears block, restores cursor */

extern const char *s3m_spinner[10];

/* ---- endpoint / credential configuration ----------------------------- */

typedef struct {
    char endpoint[512];        /* scheme://host[:port], no trailing slash */
    char region[64];
    char profile[64];
    bool path_style;
    bool no_sign;              /* anonymous access */
    bool insecure;             /* skip TLS verification */
    char ca_bundle[512];
    char access_key[128];
    char secret_key[128];
    char session_token[2600];
    /* derived at finalise time */
    char scheme[8];
    char hostport[280];        /* host[:port] as it appears in Host: */
} s3m_config;

extern s3m_config s3m_cfg;

/*
 * Common connection options, shared by every tool.  Add S3M_COMMON_LOPTS
 * to the getopt_long table and give unmatched option codes to
 * s3m_common_opt(); it returns true when it consumed the option.
 */
#define S3M_COMMON_LOPTS \
    { "endpoint",   required_argument, NULL, 2001 }, \
    { "region",     required_argument, NULL, 2002 }, \
    { "profile",    required_argument, NULL, 2003 }, \
    { "path-style", no_argument,       NULL, 2004 }, \
    { "no-sign",    no_argument,       NULL, 2005 }, \
    { "insecure",   no_argument,       NULL, 2006 }, \
    { "ca-bundle",  required_argument, NULL, 2007 }

bool s3m_common_opt(int c, const char *arg);
extern const char *s3m_common_usage;   /* help text block for the above */

/* resolve endpoint/region/credentials from flags, env and ~/.aws files;
 * prints its own message and returns -1 on failure */
int s3m_config_finalize(const char *tool);

/* "s3://bucket/key-or-prefix" -> bucket + malloc'd key ("" if none);
 * 0 ok, -1 bad (message printed with `tool` prefix) */
int s3m_uri_parse(const char *tool, const char *arg,
                  char bucket[256], char **key);

/* ---- HTTP transport --------------------------------------------------- */

extern _Atomic uint64_t s3m_nrequests;   /* HTTP requests issued  */
extern _Atomic uint64_t s3m_nretries;    /* of which were retries */

typedef struct s3m_http s3m_http;        /* one per worker thread */

int       s3m_http_global_init(void);    /* once, before threads  */
s3m_http *s3m_http_new(void);
void      s3m_http_free(s3m_http *h);

/* ---- DNS round-robin endpoint pool ------------------------------------
 *
 * Resolves the configured endpoint's hostname to every A/AAAA address it
 * has, so worker threads can be spread across them instead of each
 * making its own independent DNS pick (which typically collapses onto
 * one address via OS/resolver caching). The Host header, TLS SNI and
 * SigV4 signature always stay on the configured hostname — only the TCP
 * connection target changes — so this is transparent to certificates
 * and to S3-compatible services that validate the Host header.
 */

typedef struct {
    char   **addrs;             /* resolved addresses, textual form */
    size_t   n;
    _Atomic size_t cursor;       /* round-robin assignment counter */
} s3m_endpoint_pool;

/* resolve the finalized endpoint's hostname; 0 ok (pool->n >= 1),
 * -1 on DNS failure (message printed with `tool` prefix) */
int  s3m_endpoint_pool_init(s3m_endpoint_pool *pool, const char *tool);
void s3m_endpoint_pool_destroy(s3m_endpoint_pool *pool);

/* pins h's connections to pool member `i % pool->n` */
void s3m_http_pin_endpoint(s3m_http *h, s3m_endpoint_pool *pool, size_t i);
/* moves h to the next pool member (round-robin), e.g. after a transport
 * failure on the current one */
void s3m_http_rotate_endpoint(s3m_http *h, s3m_endpoint_pool *pool);
/* true when h's most recent request failed at the transport level
 * (connection error, timeout, ... — retries against it exhausted) */
bool s3m_http_transport_failed(const s3m_http *h);

typedef struct {
    long   status;         /* HTTP status; 0 = transport failure */
    char  *body;           /* malloc'd, NUL-terminated (may be empty) */
    size_t body_len;
    char  *hdrs;           /* malloc'd raw response header block */
    char   code[64];       /* S3 <Error><Code> when status >= 400 */
    char   msg[192];       /* S3 <Error><Message> */
} s3m_resp;

void s3m_resp_free(s3m_resp *r);
/* case-insensitive response header lookup; true if found */
bool s3m_resp_header(const s3m_resp *r, const char *name,
                     char *out, size_t outsz);
/* "<Code>: <Message> (HTTP <status>)" for error reporting */
void s3m_resp_errstr(const s3m_resp *r, char *out, size_t outsz);

/*
 * Perform a signed S3 request with retries (exponential backoff with
 * jitter on transport errors, 429 and 5xx).  `key` is the raw object key
 * (not URL-encoded), "" or NULL for bucket-level requests.  `query` must
 * already be in canonical form: parameters sorted by name, values
 * percent-encoded.  Extra headers are "name:value" strings, lowercase
 * names; x-amz-* headers are signed.  Returns 0 when an HTTP response
 * was obtained (check r->status), -1 on transport failure.
 */
int s3m_req(s3m_http *h, const char *method, const char *bucket,
            const char *key, const char *query,
            const void *body, size_t body_len, const char *content_type,
            bool md5_body, const char **xhdrs, size_t nxhdrs,
            s3m_resp *r);

/* PUT `len` bytes read from fd at `off` (UNSIGNED-PAYLOAD streaming) */
int s3m_req_upload(s3m_http *h, const char *bucket, const char *key,
                   const char *query, int fd, uint64_t off, uint64_t len,
                   const char *content_type, const char **xhdrs,
                   size_t nxhdrs, _Atomic uint64_t *ctr, s3m_resp *r);

/* GET an object body straight to fd (error bodies land in r->body) */
int s3m_req_download(s3m_http *h, const char *bucket, const char *key,
                     const char *query, int fd, _Atomic uint64_t *ctr,
                     s3m_resp *r);

/* server-side copy: plain CopyObject up to 5 GiB, multipart
 * UploadPartCopy above (aborted server-side on failure).  Adds bytes
 * to *ctr as parts complete.  0 ok, -1 failed with a message in err. */
int s3m_copy_object(s3m_http *h, const char *src_bucket,
                    const char *src_key, const char *dst_bucket,
                    const char *dst_key, uint64_t size,
                    _Atomic uint64_t *ctr, char *err, size_t errsz);

/* ---- local-file transfer helpers -------------------------------------- */

/* Content-Type guess from the file extension */
const char *s3m_mime_type(const char *name);
/* hex MD5 of a local file (for etag comparison); 0 ok, -1 I/O error */
int s3m_file_md5(const char *path, char out[33]);
/* create every missing parent directory of path; 0 ok */
int s3m_mkdirs_for(const char *path);
/* PUT a local file, streaming; multipart above 128 MiB (aborted
 * server-side on failure); 0 ok, -1 with a message in err */
int s3m_upload_file(s3m_http *h, const char *bucket, const char *key,
                    int fd, uint64_t size, const char *content_type,
                    _Atomic uint64_t *ctr, char *err, size_t errsz);
/* GET an object to path atomically (temp file + rename), creating
 * parent directories; mtime >= 0 is applied to the file */
int s3m_download_file(s3m_http *h, const char *bucket, const char *key,
                      const char *path, time_t mtime,
                      _Atomic uint64_t *ctr, char *err, size_t errsz);

/* ---- minimal XML reader for S3 response documents --------------------- */

enum { S3M_XML_OPEN, S3M_XML_ELEM, S3M_XML_CLOSE };

typedef struct {
    const char *p, *end;
    int         kind;
    char        tag[80];
    char       *text;          /* decoded text for S3M_XML_ELEM */
    size_t      text_cap;
} s3m_xml;

void s3m_xml_init(s3m_xml *x, const char *doc, size_t len);
bool s3m_xml_next(s3m_xml *x);
void s3m_xml_free(s3m_xml *x);

/* ---- listing engine ---------------------------------------------------- */

typedef struct {
    const char *key;           /* valid for the duration of the callback */
    uint64_t    size;
    time_t      mtime;
    char        etag[68];      /* quotes stripped */
    char        storclass[40];
    char        owner[128];
    char        version_id[288];
    bool        is_latest;
    bool        is_marker;     /* delete marker (versions mode only) */
} s3m_obj;

typedef void (*s3m_obj_cb)(void *ctx, const s3m_obj *o);

/*
 * Process one listing job: below `shard_depth`, list `prefix` with
 * delimiter "/" (emitting this level's objects, pushing each common
 * prefix onto the stack as a deeper job); at or beyond it, list the
 * whole prefix flat with serial pagination.  In `versions` mode the
 * callback also receives delete markers, and a key's versions arrive
 * contiguously, newest first.  Errors are noted; returns 0 or -1.
 */
int s3m_list_job(s3m_http *h, s3m_stack *stk, const char *bucket,
                 const char *prefix, int depth, int shard_depth,
                 int tag, bool versions, bool fetch_owner,
                 s3m_obj_cb cb, void *ctx);

/* ---- batched DeleteObjects pump ---------------------------------------- */

#define S3M_DEL_MAX 1000

typedef void (*s3m_del_cb)(void *ctx, const char *key, const char *vid,
                           bool ok, const char *code, const char *msg);

typedef struct {
    const char *bucket;
    s3m_del_cb  done;
    void       *ctx;
    int         n;
    char       *keys[S3M_DEL_MAX];
    char       *vids[S3M_DEL_MAX];   /* NULL = unversioned delete */
} s3m_delbatch;

void s3m_delbatch_init(s3m_delbatch *b, const char *bucket,
                       s3m_del_cb done, void *ctx);
/* queues one delete; flushes automatically at S3M_DEL_MAX; 0 ok, -1 err */
int  s3m_delbatch_add(s3m_delbatch *b, s3m_http *h,
                      const char *key, const char *vid);
int  s3m_delbatch_flush(s3m_delbatch *b, s3m_http *h);

#endif /* S3MCORE_H */
