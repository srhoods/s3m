/* s3mcore — shared engine for the s3m tool suite */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "s3mcore.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <curl/curl.h>
#include <openssl/evp.h>

bool s3m_color;

/* ------------------------------------------------------------------ */
/* formatting helpers                                                   */
/* ------------------------------------------------------------------ */

double s3m_mono_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

char *s3m_fmt_u64(uint64_t v, char out[32])
{
    char tmp[24];
    int n = snprintf(tmp, sizeof tmp, "%llu", (unsigned long long)v);
    int m = n + (n - 1) / 3;
    out[m] = '\0';
    for (int ti = n - 1, oi = m - 1, cnt = 0; ti >= 0; ) {
        out[oi--] = tmp[ti--];
        if (++cnt == 3 && ti >= 0) {
            out[oi--] = ',';
            cnt = 0;
        }
    }
    return out;
}

char *s3m_fmt_size(uint64_t b, char out[32])
{
    static const char *unit[] = { "B", "KiB", "MiB", "GiB", "TiB", "PiB" };
    double v = (double)b;
    int i = 0;
    while (v >= 1024.0 && i < 5) {
        v /= 1024.0;
        i++;
    }
    if (i == 0)
        snprintf(out, 32, "%llu B", (unsigned long long)b);
    else
        snprintf(out, 32, "%.2f %s", v, unit[i]);
    return out;
}

char *s3m_fmt_elapsed(double s, char out[32])
{
    if (s < 60.0)
        snprintf(out, 32, "%.1fs", s);
    else
        snprintf(out, 32, "%dm %02ds", (int)(s / 60.0), (int)s % 60);
    return out;
}

void s3m_fmt_time(time_t t, char out[32])
{
    struct tm tm;
    gmtime_r(&t, &tm);
    strftime(out, 32, "%Y-%m-%dT%H:%M:%SZ", &tm);
}

int s3m_term_width(void)
{
    struct winsize ws;
    if (ioctl(STDERR_FILENO, TIOCGWINSZ, &ws) == 0 && ws.ws_col > 0)
        return ws.ws_col;
    return 80;
}

void s3m_trunc_left(const char *s, size_t max, char *out, size_t outsz)
{
    size_t len = strlen(s);
    if (max >= outsz)
        max = outsz - 1;
    if (len <= max) {
        memcpy(out, s, len + 1);
        return;
    }
    /* keep the tail, prefix a single-column UTF-8 ellipsis (3 bytes) */
    size_t keep = (max > 4) ? max - 1 : 3;
    if (keep + 4 > outsz)
        keep = outsz - 4;
    memcpy(out, "…", 3);
    memcpy(out + 3, s + (len - keep), keep + 1);
}

char *s3m_strdupf(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (n < 0)
        return NULL;
    char *p = malloc((size_t)n + 1);
    if (!p)
        return NULL;
    va_start(ap, fmt);
    vsnprintf(p, (size_t)n + 1, fmt, ap);
    va_end(ap);
    return p;
}

int s3m_parse_dur(const char *s, int64_t *secs)
{
    char *end;
    long long v = strtoll(s, &end, 10);
    if (end == s || v < 0 || end[0] == '\0' || end[1] != '\0')
        return -1;
    switch (*end) {
    case 's': *secs = v;                  return 0;
    case 'm': *secs = v * 60;             return 0;
    case 'h': *secs = v * 3600;           return 0;
    case 'd': *secs = v * 86400;          return 0;
    case 'w': *secs = v * 7 * 86400;      return 0;
    default:  return -1;
    }
}

time_t s3m_parse_iso8601(const char *s)
{
    struct tm tm = { 0 };
    int y, mo, d, h, mi, se;
    if (sscanf(s, "%4d-%2d-%2dT%2d:%2d:%2d", &y, &mo, &d, &h, &mi, &se) != 6)
        return (time_t)-1;
    tm.tm_year = y - 1900;
    tm.tm_mon  = mo - 1;
    tm.tm_mday = d;
    tm.tm_hour = h;
    tm.tm_min  = mi;
    tm.tm_sec  = se;
    return timegm(&tm);
}

static bool unreserved(unsigned char c)
{
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
           (c >= '0' && c <= '9') || c == '-' || c == '.' ||
           c == '_' || c == '~';
}

void s3m_urlenc(const char *s, bool keep_slash, char *out, size_t outsz)
{
    static const char hex[] = "0123456789ABCDEF";
    size_t o = 0;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        if (unreserved(*p) || (keep_slash && *p == '/')) {
            if (o + 2 > outsz)
                break;
            out[o++] = (char)*p;
        } else {
            if (o + 4 > outsz)
                break;
            out[o++] = '%';
            out[o++] = hex[*p >> 4];
            out[o++] = hex[*p & 15];
        }
    }
    out[o] = '\0';
}

static int hexval(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

void s3m_urldec(char *s)
{
    char *o = s;
    for (char *p = s; *p; ) {
        if (p[0] == '%' && hexval(p[1]) >= 0 && hexval(p[2]) >= 0) {
            *o++ = (char)(hexval(p[1]) * 16 + hexval(p[2]));
            p += 3;
        } else {
            *o++ = *p++;
        }
    }
    *o = '\0';
}

/* ------------------------------------------------------------------ */
/* error accounting                                                     */
/* ------------------------------------------------------------------ */

_Atomic uint64_t s3m_nerrors;

#define ERRLOG_MAX 24
static char           *errlog[ERRLOG_MAX];
static int             errlog_n;
static pthread_mutex_t errlog_mu = PTHREAD_MUTEX_INITIALIZER;

void s3m_note_error(const char *subject, const char *what, const char *msg)
{
    atomic_fetch_add_explicit(&s3m_nerrors, 1, memory_order_relaxed);
    pthread_mutex_lock(&errlog_mu);
    if (errlog_n < ERRLOG_MAX) {
        char buf[2048];
        snprintf(buf, sizeof buf, "%s: %s: %s", what, subject, msg);
        errlog[errlog_n] = strdup(buf);
        if (errlog[errlog_n])
            errlog_n++;
    }
    pthread_mutex_unlock(&errlog_mu);
}

void s3m_print_errors(void)
{
    uint64_t errs = atomic_load(&s3m_nerrors);
    if (!errs)
        return;
    fprintf(stderr, "%s%llu error%s encountered:%s\n", C_RED,
            (unsigned long long)errs, errs == 1 ? "" : "s", C_RESET);
    for (int i = 0; i < errlog_n; i++)
        fprintf(stderr, "  %s\n", errlog[i]);
    if (errs > (uint64_t)errlog_n)
        fprintf(stderr, "  … and %llu more\n",
                (unsigned long long)(errs - (uint64_t)errlog_n));
}

/* ------------------------------------------------------------------ */
/* "current key"                                                        */
/* ------------------------------------------------------------------ */

static char            cur_key[2048];
static pthread_mutex_t cur_mu = PTHREAD_MUTEX_INITIALIZER;

void s3m_set_current(const char *p)
{
    pthread_mutex_lock(&cur_mu);
    snprintf(cur_key, sizeof cur_key, "%s", p);
    pthread_mutex_unlock(&cur_mu);
}

void s3m_get_current(char *dst, size_t n)
{
    pthread_mutex_lock(&cur_mu);
    snprintf(dst, n, "%s", cur_key);
    pthread_mutex_unlock(&cur_mu);
}

/* ------------------------------------------------------------------ */
/* work stack                                                           */
/* ------------------------------------------------------------------ */

void s3m_stack_init(s3m_stack *s, int nthreads)
{
    memset(s, 0, sizeof *s);
    pthread_mutex_init(&s->mu, NULL);
    pthread_cond_init(&s->cv, NULL);
    s->nthreads = nthreads;
}

void s3m_stack_set_threads(s3m_stack *s, int nthreads)
{
    pthread_mutex_lock(&s->mu);
    s->nthreads = nthreads;
    pthread_cond_broadcast(&s->cv);
    pthread_mutex_unlock(&s->mu);
}

void s3m_stack_push_batch(s3m_stack *s, char **jobs, size_t n)
{
    if (!n)
        return;
    pthread_mutex_lock(&s->mu);
    if (s->len + n > s->cap) {
        size_t nc = s->cap ? s->cap : 256;
        while (nc < s->len + n)
            nc *= 2;
        char **ni = realloc(s->items, nc * sizeof *ni);
        if (!ni) {
            pthread_mutex_unlock(&s->mu);
            for (size_t i = 0; i < n; i++) {
                s3m_note_error(jobs[i], "queue", "out of memory");
                free(jobs[i]);
            }
            return;
        }
        s->items = ni;
        s->cap = nc;
    }
    memcpy(s->items + s->len, jobs, n * sizeof *jobs);
    s->len += n;
    if (n == 1)
        pthread_cond_signal(&s->cv);
    else
        pthread_cond_broadcast(&s->cv);
    pthread_mutex_unlock(&s->mu);
}

char *s3m_stack_pop(s3m_stack *s)
{
    pthread_mutex_lock(&s->mu);
    while (s->len == 0 && !s->done) {
        s->idle++;
        if (s->idle >= s->nthreads) {
            s->done = true;
            pthread_cond_broadcast(&s->cv);
        } else {
            pthread_cond_wait(&s->cv, &s->mu);
        }
        s->idle--;
    }
    char *p = NULL;
    if (s->len > 0)
        p = s->items[--s->len];
    pthread_mutex_unlock(&s->mu);
    return p;
}

size_t s3m_stack_pending(s3m_stack *s)
{
    pthread_mutex_lock(&s->mu);
    size_t n = s->len;
    pthread_mutex_unlock(&s->mu);
    return n;
}

void s3m_stack_destroy(s3m_stack *s)
{
    free(s->items);
    pthread_mutex_destroy(&s->mu);
    pthread_cond_destroy(&s->cv);
}

char *s3m_job_make(const char *bucket, const char *prefix, int depth)
{
    if (depth > 9)
        depth = 9;
    return s3m_strdupf("%c%s\x01%s", '0' + depth, bucket, prefix);
}

void s3m_push_job(s3m_stack *s, const char *bucket, const char *prefix,
                  int depth)
{
    char *j = s3m_job_make(bucket, prefix, depth);
    if (!j) {
        s3m_note_error(prefix, "queue", "out of memory");
        return;
    }
    s3m_stack_push_batch(s, &j, 1);
}

int s3m_job_parse(char *job, char **bucket, char **prefix)
{
    if (job[0] < '0' || job[0] > '9')
        return -1;
    char *sep = strchr(job + 1, '\x01');
    if (!sep)
        return -1;
    *sep = '\0';
    *bucket = job + 1;
    *prefix = sep + 1;
    return job[0] - '0';
}

/* ------------------------------------------------------------------ */
/* buffered output                                                      */
/* ------------------------------------------------------------------ */

void s3m_sink_init(s3m_sink *k, FILE *f)
{
    k->f = f;
    pthread_mutex_init(&k->mu, NULL);
    atomic_store(&k->failed, false);
}

int s3m_ob_init(s3m_outbuf *ob, s3m_sink *k)
{
    ob->buf = malloc(S3M_OB_CAP);
    ob->len = 0;
    ob->sink = k;
    return ob->buf ? 0 : -1;
}

void s3m_ob_free(s3m_outbuf *ob)
{
    free(ob->buf);
    ob->buf = NULL;
}

void s3m_ob_flush(s3m_outbuf *ob)
{
    if (!ob->len)
        return;
    s3m_sink *k = ob->sink;
    pthread_mutex_lock(&k->mu);
    size_t w = fwrite(ob->buf, 1, ob->len, k->f);
    pthread_mutex_unlock(&k->mu);
    if (w != ob->len)
        atomic_store(&k->failed, true);
    ob->len = 0;
}

bool s3m_ob_room(s3m_outbuf *ob, size_t need)
{
    if (need > S3M_OB_CAP)
        return false;
    if (S3M_OB_CAP - ob->len < need)
        s3m_ob_flush(ob);
    return true;
}

void s3m_ob_puts(s3m_outbuf *ob, const char *s)
{
    size_t n = strlen(s);
    memcpy(ob->buf + ob->len, s, n);
    ob->len += n;
}

void s3m_ob_putc(s3m_outbuf *ob, char c)
{
    ob->buf[ob->len++] = c;
}

void s3m_ob_fmt(s3m_outbuf *ob, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(ob->buf + ob->len, S3M_OB_CAP - ob->len, fmt, ap);
    va_end(ap);
    if (n > 0)
        ob->len += (size_t)n;
}

void s3m_ob_csv(s3m_outbuf *ob, const char *s)
{
    if (!s[strcspn(s, ",\"\n\r")]) {
        s3m_ob_puts(ob, s);
        return;
    }
    s3m_ob_putc(ob, '"');
    for (const char *p = s; *p; p++) {
        if (*p == '"')
            s3m_ob_putc(ob, '"');
        s3m_ob_putc(ob, *p);
    }
    s3m_ob_putc(ob, '"');
}

/* ------------------------------------------------------------------ */
/* progress display                                                     */
/* ------------------------------------------------------------------ */

const char *s3m_spinner[10] = { "⠋", "⠙", "⠹", "⠸", "⠼",
                                "⠴", "⠦", "⠧", "⠇", "⠏" };

static struct {
    s3m_progress_cfg cfg;
    pthread_t        tid;
    atomic_bool      running;
    bool             active;
} prog;

static void prog_signal(int sig)
{
    /* restore the cursor before dying mid-display */
    ssize_t r = write(STDERR_FILENO, "\x1b[?25h\n", 7);
    (void)r;
    signal(sig, SIG_DFL);
    raise(sig);
}

static void *prog_fn(void *arg)
{
    fputs("\x1b[?25l", stderr);       /* hide cursor */
    bool first = true;
    int frame = 0;
    double prev_t = s3m_mono_now(), rate = 0.0;
    uint64_t prev_items = 0;

    while (atomic_load(&prog.running)) {
        double t = s3m_mono_now();
        uint64_t items = prog.cfg.items();
        double dt = t - prev_t;
        if (dt > 1e-4) {
            double inst = (double)(items - prev_items) / dt;
            rate = first ? inst : rate * 0.7 + inst * 0.3;
        }
        prev_t = t;
        prev_items = items;
        if (!first)
            fprintf(stderr, "\x1b[%dA", prog.cfg.lines);
        prog.cfg.draw(rate, frame++);
        fflush(stderr);
        first = false;
        struct timespec ts = { 0, 125 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    /* clear the live block; the tool prints its final summary after */
    if (!first)
        fprintf(stderr, "\x1b[%dA\x1b[J", prog.cfg.lines);
    fputs("\x1b[?25h", stderr);       /* show cursor */
    fflush(stderr);
    return arg;
}

int s3m_progress_start(const s3m_progress_cfg *cfg)
{
    prog.cfg = *cfg;
    atomic_store(&prog.running, true);
    signal(SIGINT, prog_signal);
    signal(SIGTERM, prog_signal);
    if (pthread_create(&prog.tid, NULL, prog_fn, NULL) != 0)
        return -1;
    prog.active = true;
    return 0;
}

void s3m_progress_stop(void)
{
    if (!prog.active)
        return;
    atomic_store(&prog.running, false);
    pthread_join(prog.tid, NULL);
    prog.active = false;
}

/* ------------------------------------------------------------------ */
/* configuration                                                        */
/* ------------------------------------------------------------------ */

s3m_config s3m_cfg;

const char *s3m_common_usage =
"\n"
"Connection options (all s3m tools):\n"
"      --endpoint URL    S3 endpoint (default: AWS; env S3M_ENDPOINT,\n"
"                        AWS_ENDPOINT_URL)\n"
"      --region R        signing region (default: profile/env, else us-east-1)\n"
"      --profile NAME    profile in ~/.aws/credentials (env AWS_PROFILE)\n"
"      --path-style      path-style addressing; auto-enabled for IP,\n"
"                        localhost and dotless endpoint hosts\n"
"      --no-sign         anonymous access (public buckets)\n"
"      --insecure        skip TLS certificate verification\n"
"      --ca-bundle FILE  CA bundle for private CAs\n";

bool s3m_common_opt(int c, const char *arg)
{
    switch (c) {
    case 2001:
        snprintf(s3m_cfg.endpoint, sizeof s3m_cfg.endpoint, "%s", arg);
        return true;
    case 2002:
        snprintf(s3m_cfg.region, sizeof s3m_cfg.region, "%s", arg);
        return true;
    case 2003:
        snprintf(s3m_cfg.profile, sizeof s3m_cfg.profile, "%s", arg);
        return true;
    case 2004:
        s3m_cfg.path_style = true;
        return true;
    case 2005:
        s3m_cfg.no_sign = true;
        return true;
    case 2006:
        s3m_cfg.insecure = true;
        return true;
    case 2007:
        snprintf(s3m_cfg.ca_bundle, sizeof s3m_cfg.ca_bundle, "%s", arg);
        return true;
    default:
        return false;
    }
}

/* tiny INI scanner: fills empty dsts from keys found in section sec1
 * (or sec2, if non-NULL); silently ignores a missing file */
struct ini_kv {
    const char *name;
    char       *dst;
    size_t      sz;
};

static void ini_scan(const char *path, const char *sec1, const char *sec2,
                     struct ini_kv *kvs, int nkv)
{
    FILE *f = fopen(path, "r");
    if (!f)
        return;
    char line[4096];
    bool in = false;
    while (fgets(line, sizeof line, f)) {
        char *p = line;
        if (*p == '[') {
            char *e = strchr(p, ']');
            if (!e)
                continue;
            *e = '\0';
            p++;
            in = !strcasecmp(p, sec1) || (sec2 && !strcasecmp(p, sec2));
            continue;
        }
        /* sub-blocks (indented lines) and comments are skipped */
        if (!in || *p == ' ' || *p == '\t' || *p == '#' || *p == ';')
            continue;
        char *eq = strchr(p, '=');
        if (!eq)
            continue;
        *eq = '\0';
        char *key = p, *val = eq + 1;
        while (*key && isspace((unsigned char)key[strlen(key) - 1]))
            key[strlen(key) - 1] = '\0';
        while (isspace((unsigned char)*val))
            val++;
        size_t vl = strlen(val);
        while (vl && isspace((unsigned char)val[vl - 1]))
            val[--vl] = '\0';
        for (int i = 0; i < nkv; i++) {
            if (!kvs[i].dst[0] && !strcasecmp(key, kvs[i].name))
                snprintf(kvs[i].dst, kvs[i].sz, "%s", val);
        }
    }
    fclose(f);
}

static const char *env_nonempty(const char *name)
{
    const char *v = getenv(name);
    return (v && *v) ? v : NULL;
}

int s3m_config_finalize(const char *tool)
{
    s3m_config *c = &s3m_cfg;
    const char *e;

    if (!c->profile[0] && (e = env_nonempty("AWS_PROFILE")))
        snprintf(c->profile, sizeof c->profile, "%s", e);
    if (!c->profile[0])
        snprintf(c->profile, sizeof c->profile, "default");

    if (!c->no_sign && !c->access_key[0] &&
        (e = env_nonempty("AWS_ACCESS_KEY_ID"))) {
        snprintf(c->access_key, sizeof c->access_key, "%s", e);
        if ((e = env_nonempty("AWS_SECRET_ACCESS_KEY")))
            snprintf(c->secret_key, sizeof c->secret_key, "%s", e);
        if ((e = env_nonempty("AWS_SESSION_TOKEN")))
            snprintf(c->session_token, sizeof c->session_token, "%s", e);
    }

    char credpath[600], confpath[600];
    const char *home = env_nonempty("HOME");
    if ((e = env_nonempty("AWS_SHARED_CREDENTIALS_FILE")))
        snprintf(credpath, sizeof credpath, "%s", e);
    else
        snprintf(credpath, sizeof credpath, "%s/.aws/credentials",
                 home ? home : "");
    if ((e = env_nonempty("AWS_CONFIG_FILE")))
        snprintf(confpath, sizeof confpath, "%s", e);
    else
        snprintf(confpath, sizeof confpath, "%s/.aws/config",
                 home ? home : "");

    char confsec[80];  /* the config file spells it "[profile name]" */
    snprintf(confsec, sizeof confsec, "profile %.63s", c->profile);

    if (!c->no_sign && !c->access_key[0]) {
        struct ini_kv kv[] = {
            { "aws_access_key_id",     c->access_key,    sizeof c->access_key },
            { "aws_secret_access_key", c->secret_key,    sizeof c->secret_key },
            { "aws_session_token",     c->session_token, sizeof c->session_token },
        };
        ini_scan(credpath, c->profile, NULL, kv, 3);
        if (!c->access_key[0])
            ini_scan(confpath, confsec, c->profile, kv, 3);
    }
    if (!c->no_sign && (!c->access_key[0] || !c->secret_key[0])) {
        fprintf(stderr,
"%s: no credentials found for profile '%s'\n"
"%s: set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, add the profile to\n"
"%s: ~/.aws/credentials, or pass --no-sign for anonymous access\n",
                tool, c->profile, tool, tool);
        return -1;
    }

    if (!c->region[0] && (e = env_nonempty("AWS_REGION")))
        snprintf(c->region, sizeof c->region, "%s", e);
    if (!c->region[0] && (e = env_nonempty("AWS_DEFAULT_REGION")))
        snprintf(c->region, sizeof c->region, "%s", e);
    if (!c->region[0] || !c->endpoint[0]) {
        struct ini_kv kv[] = {
            { "region",       c->region,   sizeof c->region },
            { "endpoint_url", c->endpoint, sizeof c->endpoint },
        };
        ini_scan(credpath, c->profile, NULL, kv, 2);
        ini_scan(confpath, confsec, c->profile, kv, 2);
    }
    if (!c->region[0])
        snprintf(c->region, sizeof c->region, "us-east-1");

    if (!c->endpoint[0] && (e = env_nonempty("S3M_ENDPOINT")))
        snprintf(c->endpoint, sizeof c->endpoint, "%s", e);
    if (!c->endpoint[0] && (e = env_nonempty("AWS_ENDPOINT_URL_S3")))
        snprintf(c->endpoint, sizeof c->endpoint, "%s", e);
    if (!c->endpoint[0] && (e = env_nonempty("AWS_ENDPOINT_URL")))
        snprintf(c->endpoint, sizeof c->endpoint, "%s", e);
    if (!c->endpoint[0]) {
        char def[512];
        snprintf(def, sizeof def, "https://s3.%.63s.amazonaws.com",
                 c->region);
        memcpy(c->endpoint, def, sizeof def);
    }

    /* split the endpoint into scheme + host[:port] */
    const char *rest;
    if (!strncasecmp(c->endpoint, "https://", 8)) {
        snprintf(c->scheme, sizeof c->scheme, "https");
        rest = c->endpoint + 8;
    } else if (!strncasecmp(c->endpoint, "http://", 7)) {
        snprintf(c->scheme, sizeof c->scheme, "http");
        rest = c->endpoint + 7;
    } else {
        snprintf(c->scheme, sizeof c->scheme, "https");
        rest = c->endpoint;
    }
    size_t hl = strcspn(rest, "/");
    if (hl == 0 || hl >= sizeof c->hostport) {
        fprintf(stderr, "%s: invalid endpoint '%s'\n", tool, c->endpoint);
        return -1;
    }
    if (rest[hl] != '\0' && strcmp(rest + hl, "/") != 0) {
        fprintf(stderr, "%s: path-prefixed endpoints are not supported "
                "('%s')\n", tool, c->endpoint);
        return -1;
    }
    memcpy(c->hostport, rest, hl);
    c->hostport[hl] = '\0';

    /* virtual-hosted addressing cannot work for IPs, localhost or
     * single-label hosts — fall back to path-style automatically */
    if (!c->path_style) {
        char host[280];
        snprintf(host, sizeof host, "%s", c->hostport);
        char *colon = strrchr(host, ':');
        if (colon && strspn(colon + 1, "0123456789") == strlen(colon + 1))
            *colon = '\0';
        struct in_addr a4;
        struct in6_addr a6;
        if (inet_pton(AF_INET, host, &a4) == 1 ||
            inet_pton(AF_INET6, host, &a6) == 1 ||
            !strcasecmp(host, "localhost") || !strchr(host, '.'))
            c->path_style = true;
    }
    return 0;
}

int s3m_uri_parse(const char *tool, const char *arg,
                  char bucket[256], char **key)
{
    if (strncasecmp(arg, "s3://", 5) != 0) {
        fprintf(stderr, "%s: '%s' is not an s3:// URI\n", tool, arg);
        return -1;
    }
    const char *b = arg + 5;
    size_t bl = strcspn(b, "/");
    if (bl == 0 || bl > 255) {
        fprintf(stderr, "%s: invalid bucket name in '%s'\n", tool, arg);
        return -1;
    }
    memcpy(bucket, b, bl);
    bucket[bl] = '\0';
    *key = strdup(b[bl] == '/' ? b + bl + 1 : "");
    if (!*key) {
        fprintf(stderr, "%s: out of memory\n", tool);
        return -1;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* crypto helpers (OpenSSL EVP only — no deprecated interfaces)         */
/* ------------------------------------------------------------------ */

static void dg_sha256(const void *d, size_t n, unsigned char out[32])
{
    unsigned int l = 32;
    EVP_Digest(d, n, out, &l, EVP_sha256(), NULL);
}

static void sha256hex(const void *d, size_t n, char out[65])
{
    static const char hx[] = "0123456789abcdef";
    unsigned char md[32];
    dg_sha256(d, n, md);
    for (int i = 0; i < 32; i++) {
        out[i * 2]     = hx[md[i] >> 4];
        out[i * 2 + 1] = hx[md[i] & 15];
    }
    out[64] = '\0';
}

static void hmac256(const void *key, size_t klen,
                    const void *d, size_t dlen, unsigned char out[32])
{
    unsigned char kb[64] = { 0 }, pad[64], inner[32];
    if (klen > 64)
        dg_sha256(key, klen, kb);
    else
        memcpy(kb, key, klen);

    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    for (int i = 0; i < 64; i++)
        pad[i] = kb[i] ^ 0x36;
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, pad, 64);
    EVP_DigestUpdate(ctx, d, dlen);
    unsigned int l = 32;
    EVP_DigestFinal_ex(ctx, inner, &l);

    for (int i = 0; i < 64; i++)
        pad[i] = kb[i] ^ 0x5c;
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, pad, 64);
    EVP_DigestUpdate(ctx, inner, 32);
    l = 32;
    EVP_DigestFinal_ex(ctx, out, &l);
    EVP_MD_CTX_free(ctx);
}

static void md5_b64(const void *d, size_t n, char out[32])
{
    unsigned char md[16];
    unsigned int l = 16;
    EVP_Digest(d, n, md, &l, EVP_md5(), NULL);
    EVP_EncodeBlock((unsigned char *)out, md, 16);   /* 24 chars + NUL */
}

/* ------------------------------------------------------------------ */
/* HTTP transport                                                       */
/* ------------------------------------------------------------------ */

_Atomic uint64_t s3m_nrequests;
_Atomic uint64_t s3m_nretries;

struct s3m_http {
    CURL        *curl;
    unsigned int seed;                 /* retry jitter */
    char         errbuf[CURL_ERROR_SIZE];
};

int s3m_http_global_init(void)
{
    signal(SIGPIPE, SIG_IGN);
    return curl_global_init(CURL_GLOBAL_ALL) == 0 ? 0 : -1;
}

s3m_http *s3m_http_new(void)
{
    s3m_http *h = calloc(1, sizeof *h);
    if (!h)
        return NULL;
    h->curl = curl_easy_init();
    if (!h->curl) {
        free(h);
        return NULL;
    }
    h->seed = (unsigned int)(uintptr_t)h ^ (unsigned int)time(NULL);
    return h;
}

void s3m_http_free(s3m_http *h)
{
    if (!h)
        return;
    curl_easy_cleanup(h->curl);
    free(h);
}

/* growable response buffer */
struct rbuf {
    char  *p;
    size_t len, cap;
};

static bool rbuf_add(struct rbuf *b, const void *d, size_t n)
{
    if (b->len + n + 1 > b->cap) {
        size_t nc = b->cap ? b->cap : 4096;
        while (nc < b->len + n + 1)
            nc *= 2;
        char *np = realloc(b->p, nc);
        if (!np)
            return false;
        b->p = np;
        b->cap = nc;
    }
    memcpy(b->p + b->len, d, n);
    b->len += n;
    b->p[b->len] = '\0';
    return true;
}

struct reqctx {
    struct rbuf      body, hdrs;
    long             code;            /* from the last HTTP status line */
    bool             oom, local_err;
    int              download_fd;
    int              upload_fd;
    uint64_t         up_off, up_len, up_pos;
    _Atomic uint64_t *ctr;
};

static size_t hdr_cb(char *buf, size_t sz, size_t n, void *ud)
{
    struct reqctx *cx = ud;
    size_t len = sz * n;
    if (len > 5 && !strncasecmp(buf, "HTTP/", 5)) {
        /* new status line: parse the code, restart the header block */
        const char *sp = memchr(buf, ' ', len);
        cx->code = sp ? atol(sp + 1) : 0;
        cx->hdrs.len = 0;
        if (cx->hdrs.p)
            cx->hdrs.p[0] = '\0';
    }
    if (!rbuf_add(&cx->hdrs, buf, len)) {
        cx->oom = true;
        return 0;
    }
    return len;
}

static size_t write_cb(char *buf, size_t sz, size_t n, void *ud)
{
    struct reqctx *cx = ud;
    size_t len = sz * n;
    if (cx->download_fd >= 0 && cx->code == 200) {
        const char *p = buf;
        size_t left = len;
        while (left > 0) {
            ssize_t w = write(cx->download_fd, p, left);
            if (w < 0) {
                cx->local_err = true;
                return 0;
            }
            p += w;
            left -= (size_t)w;
        }
        if (cx->ctr)
            atomic_fetch_add_explicit(cx->ctr, len, memory_order_relaxed);
        return len;
    }
    if (!rbuf_add(&cx->body, buf, len)) {
        cx->oom = true;
        return 0;
    }
    return len;
}

static size_t read_cb(char *buf, size_t sz, size_t n, void *ud)
{
    struct reqctx *cx = ud;
    uint64_t left = cx->up_len - cx->up_pos;
    size_t want = sz * n;
    if (want > left)
        want = (size_t)left;
    if (want == 0)
        return 0;
    ssize_t r = pread(cx->upload_fd, buf, want,
                      (off_t)(cx->up_off + cx->up_pos));
    if (r < 0) {
        cx->local_err = true;
        return CURL_READFUNC_ABORT;
    }
    cx->up_pos += (uint64_t)r;
    if (cx->ctr)
        atomic_fetch_add_explicit(cx->ctr, (uint64_t)r, memory_order_relaxed);
    return (size_t)r;
}

/* signed headers, kept sorted by name */
struct shdr {
    const char *name;
    const char *value;
};

static int shdr_cmp(const void *a, const void *b)
{
    return strcmp(((const struct shdr *)a)->name,
                  ((const struct shdr *)b)->name);
}

static void sign_request(const char *method, const char *path,
                         const char *query, struct shdr *sh, size_t nsh,
                         const char *payhash, const char *amzdate,
                         char *auth, size_t authsz)
{
    qsort(sh, nsh, sizeof *sh, shdr_cmp);

    char canon[8192];
    size_t o = 0;
    o += (size_t)snprintf(canon + o, sizeof canon - o, "%s\n%s\n%s\n",
                          method, path, query ? query : "");
    for (size_t i = 0; i < nsh && o < sizeof canon; i++)
        o += (size_t)snprintf(canon + o, sizeof canon - o, "%s:%s\n",
                              sh[i].name, sh[i].value);
    o += (size_t)snprintf(canon + o, sizeof canon - o, "\n");
    char signed_list[512];
    size_t so = 0;
    for (size_t i = 0; i < nsh; i++)
        so += (size_t)snprintf(signed_list + so, sizeof signed_list - so,
                               "%s%s", i ? ";" : "", sh[i].name);
    o += (size_t)snprintf(canon + o, sizeof canon - o, "%s\n%s",
                          signed_list, payhash);

    char creq_hash[65];
    sha256hex(canon, o, creq_hash);

    char date[9];
    memcpy(date, amzdate, 8);
    date[8] = '\0';
    char scope[128];
    snprintf(scope, sizeof scope, "%s/%s/s3/aws4_request",
             date, s3m_cfg.region);
    char sts[512];
    int stsn = snprintf(sts, sizeof sts, "AWS4-HMAC-SHA256\n%s\n%s\n%s",
                        amzdate, scope, creq_hash);

    unsigned char k[32];
    char k0[160];
    snprintf(k0, sizeof k0, "AWS4%s", s3m_cfg.secret_key);
    hmac256(k0, strlen(k0), date, 8, k);
    hmac256(k, 32, s3m_cfg.region, strlen(s3m_cfg.region), k);
    hmac256(k, 32, "s3", 2, k);
    hmac256(k, 32, "aws4_request", 12, k);
    unsigned char sig[32];
    hmac256(k, 32, sts, (size_t)stsn, sig);

    static const char hx[] = "0123456789abcdef";
    char sighex[65];
    for (int i = 0; i < 32; i++) {
        sighex[i * 2]     = hx[sig[i] >> 4];
        sighex[i * 2 + 1] = hx[sig[i] & 15];
    }
    sighex[64] = '\0';

    snprintf(auth, authsz,
             "Authorization: AWS4-HMAC-SHA256 Credential=%s/%s, "
             "SignedHeaders=%s, Signature=%s",
             s3m_cfg.access_key, scope, signed_list, sighex);
}

void s3m_resp_free(s3m_resp *r)
{
    free(r->body);
    free(r->hdrs);
    r->body = r->hdrs = NULL;
}

bool s3m_resp_header(const s3m_resp *r, const char *name,
                     char *out, size_t outsz)
{
    if (!r->hdrs)
        return false;
    size_t nl = strlen(name);
    for (const char *p = r->hdrs; *p; ) {
        const char *eol = strchr(p, '\n');
        size_t linelen = eol ? (size_t)(eol - p) : strlen(p);
        if (linelen > nl + 1 && !strncasecmp(p, name, nl) && p[nl] == ':') {
            const char *v = p + nl + 1;
            while (*v == ' ' || *v == '\t')
                v++;
            size_t vl = linelen - (size_t)(v - p);
            while (vl && (v[vl - 1] == '\r' || v[vl - 1] == ' '))
                vl--;
            if (vl >= outsz)
                vl = outsz - 1;
            memcpy(out, v, vl);
            out[vl] = '\0';
            return true;
        }
        if (!eol)
            break;
        p = eol + 1;
    }
    return false;
}

void s3m_resp_errstr(const s3m_resp *r, char *out, size_t outsz)
{
    if (r->code[0])
        snprintf(out, outsz, "%s: %s (HTTP %ld)", r->code,
                 r->msg[0] ? r->msg : "-", r->status);
    else if (r->status)
        snprintf(out, outsz, "HTTP %ld", r->status);
    else
        snprintf(out, outsz, "%s", r->msg[0] ? r->msg : "transport failure");
}

static void parse_error_xml(s3m_resp *r)
{
    if (!r->body || !r->body_len)
        return;
    s3m_xml x;
    s3m_xml_init(&x, r->body, r->body_len);
    while (s3m_xml_next(&x)) {
        if (x.kind != S3M_XML_ELEM)
            continue;
        if (!strcmp(x.tag, "Code") && !r->code[0])
            snprintf(r->code, sizeof r->code, "%s", x.text);
        else if (!strcmp(x.tag, "Message") && !r->msg[0])
            snprintf(r->msg, sizeof r->msg, "%s", x.text);
    }
    s3m_xml_free(&x);
}

#define S3M_MAX_ATTEMPTS 8

struct reqspec {
    const char  *method;
    const char  *bucket;
    const char  *key;                 /* raw, not encoded; may be NULL */
    const char  *query;               /* canonical or NULL */
    const void  *body;                /* memory body (POST/PUT) */
    size_t       body_len;
    const char  *content_type;
    bool         md5_body;
    const char **xhdrs;
    size_t       nxhdrs;
    int          upload_fd;           /* -1 when unused */
    uint64_t     up_off, up_len;
    int          download_fd;         /* -1 when unused */
    _Atomic uint64_t *ctr;
};

static int do_request(s3m_http *h, const struct reqspec *q, s3m_resp *r)
{
    memset(r, 0, sizeof *r);

    /* ---- URL + canonical path ---- */
    bool vhost = !s3m_cfg.path_style && q->bucket && q->bucket[0];
    char host[560];
    if (vhost)
        snprintf(host, sizeof host, "%s.%s", q->bucket, s3m_cfg.hostport);
    else
        snprintf(host, sizeof host, "%s", s3m_cfg.hostport);

    const char *key = q->key ? q->key : "";
    char keyenc[3 * 1100];
    s3m_urlenc(key, true, keyenc, sizeof keyenc);

    char path[4096];
    if (vhost)
        snprintf(path, sizeof path, "/%s", keyenc);
    else if (q->bucket && q->bucket[0])
        snprintf(path, sizeof path, "/%s%s%s", q->bucket,
                 keyenc[0] ? "/" : "", keyenc);
    else
        snprintf(path, sizeof path, "/");

    char url[8192];
    snprintf(url, sizeof url, "%s://%s%s%s%s", s3m_cfg.scheme, host, path,
             (q->query && q->query[0]) ? "?" : "",
             (q->query && q->query[0]) ? q->query : "");

    /* ---- payload hash + Content-MD5 (attempt-invariant) ---- */
    char payhash[68];
    if (q->upload_fd >= 0)
        snprintf(payhash, sizeof payhash, "UNSIGNED-PAYLOAD");
    else if (q->body && q->body_len)
        sha256hex(q->body, q->body_len, payhash);
    else
        snprintf(payhash, sizeof payhash,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

    char md5hdr[64] = "";
    if (q->md5_body && q->body) {
        char b64[32];
        md5_b64(q->body, q->body_len, b64);
        snprintf(md5hdr, sizeof md5hdr, "Content-MD5: %s", b64);
    }

    CURLcode rc = CURLE_OK;
    long status = 0;
    struct reqctx cx;
    memset(&cx, 0, sizeof cx);

    for (int attempt = 0; attempt < S3M_MAX_ATTEMPTS; attempt++) {
        atomic_fetch_add_explicit(&s3m_nrequests, 1, memory_order_relaxed);
        if (attempt > 0)
            atomic_fetch_add_explicit(&s3m_nretries, 1, memory_order_relaxed);

        free(cx.body.p);
        free(cx.hdrs.p);
        memset(&cx, 0, sizeof cx);
        cx.download_fd = q->download_fd;
        cx.upload_fd   = q->upload_fd;
        cx.up_off      = q->up_off;
        cx.up_len      = q->up_len;
        cx.ctr         = q->ctr;
        if (q->download_fd >= 0 && attempt > 0) {
            /* restart a partially-written download */
            if (ftruncate(q->download_fd, 0) != 0 ||
                lseek(q->download_fd, 0, SEEK_SET) < 0) {
                cx.local_err = true;
                break;
            }
        }

        CURL *cl = h->curl;
        curl_easy_reset(cl);
        curl_easy_setopt(cl, CURLOPT_URL, url);
        curl_easy_setopt(cl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(cl, CURLOPT_ERRORBUFFER, h->errbuf);
        curl_easy_setopt(cl, CURLOPT_CONNECTTIMEOUT, 15L);
        curl_easy_setopt(cl, CURLOPT_LOW_SPEED_LIMIT, 1024L);
        curl_easy_setopt(cl, CURLOPT_LOW_SPEED_TIME, 60L);
        curl_easy_setopt(cl, CURLOPT_WRITEFUNCTION, write_cb);
        curl_easy_setopt(cl, CURLOPT_WRITEDATA, &cx);
        curl_easy_setopt(cl, CURLOPT_HEADERFUNCTION, hdr_cb);
        curl_easy_setopt(cl, CURLOPT_HEADERDATA, &cx);
        if (s3m_cfg.insecure) {
            curl_easy_setopt(cl, CURLOPT_SSL_VERIFYPEER, 0L);
            curl_easy_setopt(cl, CURLOPT_SSL_VERIFYHOST, 0L);
        }
        if (s3m_cfg.ca_bundle[0])
            curl_easy_setopt(cl, CURLOPT_CAINFO, s3m_cfg.ca_bundle);

        if (!strcmp(q->method, "HEAD")) {
            curl_easy_setopt(cl, CURLOPT_NOBODY, 1L);
        } else if (q->upload_fd >= 0) {
            curl_easy_setopt(cl, CURLOPT_UPLOAD, 1L);
            curl_easy_setopt(cl, CURLOPT_READFUNCTION, read_cb);
            curl_easy_setopt(cl, CURLOPT_READDATA, &cx);
            curl_easy_setopt(cl, CURLOPT_INFILESIZE_LARGE,
                             (curl_off_t)q->up_len);
        } else if (q->body) {
            curl_easy_setopt(cl, CURLOPT_POSTFIELDS, (const char *)q->body);
            curl_easy_setopt(cl, CURLOPT_POSTFIELDSIZE_LARGE,
                             (curl_off_t)q->body_len);
            curl_easy_setopt(cl, CURLOPT_CUSTOMREQUEST, q->method);
        } else if (strcmp(q->method, "GET") != 0) {
            curl_easy_setopt(cl, CURLOPT_CUSTOMREQUEST, q->method);
        }

        /* ---- headers + signature (re-signed each attempt) ---- */
        char amzdate[20];
        time_t now = time(NULL);
        struct tm tm;
        gmtime_r(&now, &tm);
        strftime(amzdate, sizeof amzdate, "%Y%m%dT%H%M%SZ", &tm);

        struct curl_slist *hl = NULL;
        char datehdr[48], shahdr[112], tokhdr[2700], auth[2048];
        char cthdr[160];

        if (!s3m_cfg.no_sign) {
            snprintf(datehdr, sizeof datehdr, "x-amz-date: %s", amzdate);
            snprintf(shahdr, sizeof shahdr, "x-amz-content-sha256: %s",
                     payhash);

            struct shdr sh[24];
            size_t nsh = 0;
            sh[nsh++] = (struct shdr){ "host", host };
            sh[nsh++] = (struct shdr){ "x-amz-content-sha256", payhash };
            sh[nsh++] = (struct shdr){ "x-amz-date", amzdate };
            if (s3m_cfg.session_token[0]) {
                snprintf(tokhdr, sizeof tokhdr, "x-amz-security-token: %s",
                         s3m_cfg.session_token);
                sh[nsh++] = (struct shdr){ "x-amz-security-token",
                                           s3m_cfg.session_token };
            }
            /* extra x-amz-* headers are signed; "name:value", lowercase */
            for (size_t i = 0; i < q->nxhdrs && nsh < 24; i++) {
                const char *colon = strchr(q->xhdrs[i], ':');
                if (colon && !strncmp(q->xhdrs[i], "x-amz-", 6)) {
                    static __thread char names[8][64], vals[8][2048];
                    size_t xi = i & 7;
                    size_t nl = (size_t)(colon - q->xhdrs[i]);
                    if (nl >= sizeof names[0])
                        nl = sizeof names[0] - 1;
                    memcpy(names[xi], q->xhdrs[i], nl);
                    names[xi][nl] = '\0';
                    snprintf(vals[xi], sizeof vals[xi], "%s", colon + 1);
                    sh[nsh++] = (struct shdr){ names[xi], vals[xi] };
                }
            }
            sign_request(q->method, path, q->query, sh, nsh, payhash,
                         amzdate, auth, sizeof auth);

            hl = curl_slist_append(hl, datehdr);
            hl = curl_slist_append(hl, shahdr);
            if (s3m_cfg.session_token[0])
                hl = curl_slist_append(hl, tokhdr);
            hl = curl_slist_append(hl, auth);
        }
        for (size_t i = 0; i < q->nxhdrs; i++)
            hl = curl_slist_append(hl, q->xhdrs[i]);
        if (q->content_type) {
            snprintf(cthdr, sizeof cthdr, "Content-Type: %s",
                     q->content_type);
            hl = curl_slist_append(hl, cthdr);
        }
        if (md5hdr[0])
            hl = curl_slist_append(hl, md5hdr);
        hl = curl_slist_append(hl, "Expect:");
        curl_easy_setopt(cl, CURLOPT_HTTPHEADER, hl);

        rc = curl_easy_perform(cl);
        status = 0;
        curl_easy_getinfo(cl, CURLINFO_RESPONSE_CODE, &status);
        curl_slist_free_all(hl);

        bool retry;
        if (cx.oom || cx.local_err)
            retry = false;
        else if (rc != CURLE_OK)
            retry = true;
        else
            retry = (status == 408 || status == 429 ||
                     (status >= 500 && status <= 504));
        if (!retry || attempt == S3M_MAX_ATTEMPTS - 1)
            break;

        /* exponential backoff with jitter: 0.1–0.2s, 0.2–0.4s, … cap 10s */
        long ms = 100L << (attempt < 7 ? attempt : 7);
        if (ms > 10000)
            ms = 10000;
        ms += (long)((double)ms * (double)rand_r(&h->seed) / RAND_MAX);
        struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };
        nanosleep(&ts, NULL);
    }

    r->status   = status;
    r->body     = cx.body.p;
    r->body_len = cx.body.len;
    r->hdrs     = cx.hdrs.p;

    if (cx.oom) {
        snprintf(r->code, sizeof r->code, "local");
        snprintf(r->msg, sizeof r->msg, "out of memory");
        r->status = 0;
        return -1;
    }
    if (cx.local_err) {
        snprintf(r->code, sizeof r->code, "local");
        snprintf(r->msg, sizeof r->msg, "local file I/O error: %s",
                 strerror(errno));
        r->status = 0;
        return -1;
    }
    if (rc != CURLE_OK) {
        snprintf(r->code, sizeof r->code, "transport");
        snprintf(r->msg, sizeof r->msg, "%.191s",
                 h->errbuf[0] ? h->errbuf : curl_easy_strerror(rc));
        r->status = 0;
        return -1;
    }
    if (status >= 400)
        parse_error_xml(r);
    return 0;
}

int s3m_req(s3m_http *h, const char *method, const char *bucket,
            const char *key, const char *query,
            const void *body, size_t body_len, const char *content_type,
            bool md5_body, const char **xhdrs, size_t nxhdrs,
            s3m_resp *r)
{
    struct reqspec q = {
        .method = method, .bucket = bucket, .key = key, .query = query,
        .body = body, .body_len = body_len, .content_type = content_type,
        .md5_body = md5_body, .xhdrs = xhdrs, .nxhdrs = nxhdrs,
        .upload_fd = -1, .download_fd = -1,
    };
    return do_request(h, &q, r);
}

int s3m_req_upload(s3m_http *h, const char *bucket, const char *key,
                   const char *query, int fd, uint64_t off, uint64_t len,
                   const char *content_type, const char **xhdrs,
                   size_t nxhdrs, _Atomic uint64_t *ctr, s3m_resp *r)
{
    struct reqspec q = {
        .method = "PUT", .bucket = bucket, .key = key, .query = query,
        .content_type = content_type, .xhdrs = xhdrs, .nxhdrs = nxhdrs,
        .upload_fd = fd, .up_off = off, .up_len = len,
        .download_fd = -1, .ctr = ctr,
    };
    return do_request(h, &q, r);
}

int s3m_req_download(s3m_http *h, const char *bucket, const char *key,
                     const char *query, int fd, _Atomic uint64_t *ctr,
                     s3m_resp *r)
{
    struct reqspec q = {
        .method = "GET", .bucket = bucket, .key = key, .query = query,
        .upload_fd = -1, .download_fd = fd, .ctr = ctr,
    };
    return do_request(h, &q, r);
}

/* ------------------------------------------------------------------ */
/* server-side copy (CopyObject / UploadPartCopy)                       */
/* ------------------------------------------------------------------ */

#define COPY_PLAIN_MAX ((uint64_t)5 << 30)     /* CopyObject size limit */
#define COPY_PARTSIZE  ((uint64_t)1 << 30)

/* CopyObject and friends can return 200 with an <Error> body */
static bool copy_body_error(const s3m_resp *r)
{
    return r->body && strstr(r->body, "<Error>") != NULL;
}

static void xml_find_elem(const char *body, size_t len, const char *tag,
                          char *out, size_t outsz)
{
    out[0] = '\0';
    s3m_xml x;
    s3m_xml_init(&x, body, len);
    while (s3m_xml_next(&x)) {
        if (x.kind == S3M_XML_ELEM && !strcmp(x.tag, tag)) {
            snprintf(out, outsz, "%s", x.text);
            break;
        }
    }
    s3m_xml_free(&x);
}

int s3m_copy_object(s3m_http *h, const char *src_bucket,
                    const char *src_key, const char *dst_bucket,
                    const char *dst_key, uint64_t size,
                    _Atomic uint64_t *ctr, char *err, size_t errsz)
{
    char senc[3 * 1100];
    s3m_urlenc(src_key, true, senc, sizeof senc);
    char *cs_hdr = s3m_strdupf("x-amz-copy-source:/%s/%s", src_bucket,
                               senc);
    if (!cs_hdr) {
        snprintf(err, errsz, "out of memory");
        return -1;
    }

    s3m_resp r = { 0 };
    int rc = -1;

    if (size <= COPY_PLAIN_MAX) {
        const char *xh[1] = { cs_hdr };
        if (s3m_req(h, "PUT", dst_bucket, dst_key, NULL, NULL, 0, NULL,
                    false, xh, 1, &r) == 0 && r.status == 200 &&
            !copy_body_error(&r)) {
            rc = 0;
            if (ctr)
                atomic_fetch_add_explicit(ctr, size,
                                          memory_order_relaxed);
        } else {
            s3m_resp_errstr(&r, err, errsz);
        }
        s3m_resp_free(&r);
        free(cs_hdr);
        return rc;
    }

    /* multipart copy */
    if (s3m_req(h, "POST", dst_bucket, dst_key, "uploads=", NULL, 0,
                NULL, false, NULL, 0, &r) != 0 || r.status != 200) {
        s3m_resp_errstr(&r, err, errsz);
        s3m_resp_free(&r);
        free(cs_hdr);
        return -1;
    }
    char upid[300];
    xml_find_elem(r.body, r.body_len, "UploadId", upid, sizeof upid);
    s3m_resp_free(&r);
    if (!upid[0]) {
        snprintf(err, errsz, "no UploadId in initiate response");
        free(cs_hdr);
        return -1;
    }
    char upid_enc[900];
    s3m_urlenc(upid, false, upid_enc, sizeof upid_enc);

    uint64_t partsz = COPY_PARTSIZE;
    while (size / partsz + 1 > 10000)
        partsz *= 2;
    size_t nparts = (size_t)((size + partsz - 1) / partsz);

    char *etags = calloc(nparts, 68);
    size_t done = 0;
    if (etags) {
        for (; done < nparts; done++) {
            uint64_t off = (uint64_t)done * partsz;
            uint64_t len = (off + partsz <= size) ? partsz : size - off;
            char range_hdr[96], q[1024];
            snprintf(range_hdr, sizeof range_hdr,
                     "x-amz-copy-source-range:bytes=%llu-%llu",
                     (unsigned long long)off,
                     (unsigned long long)(off + len - 1));
            snprintf(q, sizeof q, "partNumber=%zu&uploadId=%s",
                     done + 1, upid_enc);
            const char *xh[2] = { cs_hdr, range_hdr };
            if (s3m_req(h, "PUT", dst_bucket, dst_key, q, NULL, 0, NULL,
                        false, xh, 2, &r) != 0 || r.status != 200 ||
                copy_body_error(&r)) {
                s3m_resp_errstr(&r, err, errsz);
                s3m_resp_free(&r);
                break;
            }
            /* UploadPartCopy returns the part etag in the body */
            xml_find_elem(r.body, r.body_len, "ETag", etags + done * 68,
                          68);
            s3m_resp_free(&r);
            if (!etags[done * 68]) {
                snprintf(err, errsz, "no ETag in CopyPartResult");
                break;
            }
            if (ctr)
                atomic_fetch_add_explicit(ctr, len, memory_order_relaxed);
        }
        if (done == nparts) {
            size_t bl = 128 + nparts * 160;
            char *body = malloc(bl);
            if (body) {
                size_t o = (size_t)snprintf(body, bl,
                                            "<CompleteMultipartUpload>");
                for (size_t j = 0; j < nparts; j++)
                    o += (size_t)snprintf(body + o, bl - o,
                        "<Part><PartNumber>%zu</PartNumber>"
                        "<ETag>%s</ETag></Part>", j + 1, etags + j * 68);
                o += (size_t)snprintf(body + o, bl - o,
                                      "</CompleteMultipartUpload>");
                char q[1024];
                snprintf(q, sizeof q, "uploadId=%s", upid_enc);
                if (s3m_req(h, "POST", dst_bucket, dst_key, q, body, o,
                            "application/xml", false, NULL, 0, &r) == 0 &&
                    r.status == 200 && !copy_body_error(&r))
                    rc = 0;
                else
                    s3m_resp_errstr(&r, err, errsz);
                s3m_resp_free(&r);
                free(body);
            } else {
                snprintf(err, errsz, "out of memory");
            }
        }
    } else {
        snprintf(err, errsz, "out of memory");
    }
    free(etags);

    if (rc != 0) {
        s3m_resp ab;
        char q[1024];
        snprintf(q, sizeof q, "uploadId=%s", upid_enc);
        s3m_req(h, "DELETE", dst_bucket, dst_key, q, NULL, 0, NULL,
                false, NULL, 0, &ab);
        s3m_resp_free(&ab);
    }
    free(cs_hdr);
    return rc;
}

/* ------------------------------------------------------------------ */
/* minimal XML reader                                                   */
/* ------------------------------------------------------------------ */

void s3m_xml_init(s3m_xml *x, const char *doc, size_t len)
{
    memset(x, 0, sizeof *x);
    x->p = doc;
    x->end = doc + len;
}

void s3m_xml_free(s3m_xml *x)
{
    free(x->text);
    x->text = NULL;
    x->text_cap = 0;
}

static bool xml_settext(s3m_xml *x, const char *s, size_t n)
{
    if (n + 1 > x->text_cap) {
        size_t nc = x->text_cap ? x->text_cap : 256;
        while (nc < n + 1)
            nc *= 2;
        char *np = realloc(x->text, nc);
        if (!np)
            return false;
        x->text = np;
        x->text_cap = nc;
    }
    /* copy, decoding entities */
    char *o = x->text;
    for (size_t i = 0; i < n; ) {
        if (s[i] == '&') {
            if      (n - i >= 4 && !strncmp(s + i, "&lt;", 4))  { *o++ = '<';  i += 4; continue; }
            else if (n - i >= 4 && !strncmp(s + i, "&gt;", 4))  { *o++ = '>';  i += 4; continue; }
            else if (n - i >= 5 && !strncmp(s + i, "&amp;", 5)) { *o++ = '&';  i += 5; continue; }
            else if (n - i >= 6 && !strncmp(s + i, "&quot;", 6)){ *o++ = '"';  i += 6; continue; }
            else if (n - i >= 6 && !strncmp(s + i, "&apos;", 6)){ *o++ = '\''; i += 6; continue; }
            else if (n - i >= 4 && s[i + 1] == '#') {
                long v;
                char *e;
                if (s[i + 2] == 'x' || s[i + 2] == 'X')
                    v = strtol(s + i + 3, &e, 16);
                else
                    v = strtol(s + i + 2, &e, 10);
                if (*e == ';' && v > 0 && v < 256) {
                    *o++ = (char)v;
                    i = (size_t)(e - s) + 1;
                    continue;
                }
            }
        }
        *o++ = s[i++];
    }
    *o = '\0';
    return true;
}

bool s3m_xml_next(s3m_xml *x)
{
    for (;;) {
        const char *lt = memchr(x->p, '<', (size_t)(x->end - x->p));
        if (!lt)
            return false;
        const char *p = lt + 1;
        if (p >= x->end)
            return false;
        if (*p == '?') {                       /* <?xml … ?> */
            const char *e = memchr(p, '>', (size_t)(x->end - p));
            if (!e)
                return false;
            x->p = e + 1;
            continue;
        }
        if (*p == '!') {                       /* comments, doctype */
            const char *e = memchr(p, '>', (size_t)(x->end - p));
            if (!e)
                return false;
            x->p = e + 1;
            continue;
        }
        bool closing = (*p == '/');
        if (closing)
            p++;
        size_t tl = 0;
        while (p + tl < x->end && !strchr(" \t\r\n/>", p[tl]))
            tl++;
        if (tl >= sizeof x->tag)
            tl = sizeof x->tag - 1;
        memcpy(x->tag, p, tl);
        x->tag[tl] = '\0';
        const char *gt = memchr(p, '>', (size_t)(x->end - p));
        if (!gt)
            return false;
        bool selfclose = (gt > p && gt[-1] == '/');
        x->p = gt + 1;

        if (closing) {
            x->kind = S3M_XML_CLOSE;
            return true;
        }
        if (selfclose) {
            x->kind = S3M_XML_ELEM;
            if (!xml_settext(x, "", 0))
                return false;
            return true;
        }
        /* look ahead: simple element (<K>text</K>) or container? */
        const char *nx = memchr(x->p, '<', (size_t)(x->end - x->p));
        if (!nx)
            return false;
        if (nx + 1 < x->end && nx[1] == '/') {
            if (!xml_settext(x, x->p, (size_t)(nx - x->p)))
                return false;
            const char *ce = memchr(nx, '>', (size_t)(x->end - nx));
            if (!ce)
                return false;
            x->p = ce + 1;
            x->kind = S3M_XML_ELEM;
            return true;
        }
        x->kind = S3M_XML_OPEN;
        return true;
    }
}

/* ------------------------------------------------------------------ */
/* listing engine                                                       */
/* ------------------------------------------------------------------ */

static void qadd(char *q, size_t qsz, size_t *o, const char *name,
                 const char *raw_value)
{
    char enc[3 * 1100];
    s3m_urlenc(raw_value, false, enc, sizeof enc);
    *o += (size_t)snprintf(q + *o, qsz - *o, "%s%s=%s",
                           *o ? "&" : "", name, enc);
}

int s3m_list_job(s3m_http *h, s3m_stack *stk, const char *bucket,
                 const char *prefix, int depth, int shard_depth,
                 bool versions, bool fetch_owner,
                 s3m_obj_cb cb, void *ctx)
{
    bool delim = depth < shard_depth;
    char token[2600] = "", kmark[1100] = "", vmark[300] = "";
    bool more = true;

    while (more) {
        /* canonical query: parameters appended in alphabetical order */
        char q[8192];
        size_t o = 0;
        if (versions) {
            if (delim)
                o += (size_t)snprintf(q + o, sizeof q - o, "delimiter=%%2F");
            if (kmark[0])
                qadd(q, sizeof q, &o, "key-marker", kmark);
            if (prefix[0])
                qadd(q, sizeof q, &o, "prefix", prefix);
            if (vmark[0])
                qadd(q, sizeof q, &o, "version-id-marker", vmark);
            o += (size_t)snprintf(q + o, sizeof q - o, "%sversions=",
                                  o ? "&" : "");
        } else {
            if (token[0])
                qadd(q, sizeof q, &o, "continuation-token", token);
            if (delim)
                o += (size_t)snprintf(q + o, sizeof q - o, "%sdelimiter=%%2F",
                                      o ? "&" : "");
            if (fetch_owner)
                o += (size_t)snprintf(q + o, sizeof q - o,
                                      "%sfetch-owner=true", o ? "&" : "");
            o += (size_t)snprintf(q + o, sizeof q - o, "%slist-type=2",
                                  o ? "&" : "");
            if (prefix[0])
                qadd(q, sizeof q, &o, "prefix", prefix);
        }

        s3m_resp r;
        if (s3m_req(h, "GET", bucket, "", q, NULL, 0, NULL, false,
                    NULL, 0, &r) != 0 || r.status != 200) {
            char es[256];
            s3m_resp_errstr(&r, es, sizeof es);
            s3m_note_error(prefix[0] ? prefix : bucket, "list", es);
            s3m_resp_free(&r);
            return -1;
        }

        /* parse the page */
        s3m_xml x;
        s3m_xml_init(&x, r.body, r.body_len);
        s3m_obj obj;
        char keybuf[1100], ownid[128], owndisp[128];
        char ntoken[2600] = "", nkmark[1100] = "", nvmark[300] = "";
        bool trunc = false, in_entry = false, in_cp = false,
             in_owner = false;
        memset(&obj, 0, sizeof obj);
        keybuf[0] = ownid[0] = owndisp[0] = '\0';

        while (s3m_xml_next(&x)) {
            if (x.kind == S3M_XML_OPEN) {
                if (!strcmp(x.tag, "Contents") ||
                    !strcmp(x.tag, "Version") ||
                    !strcmp(x.tag, "DeleteMarker")) {
                    memset(&obj, 0, sizeof obj);
                    keybuf[0] = ownid[0] = owndisp[0] = '\0';
                    obj.is_marker = !strcmp(x.tag, "DeleteMarker");
                    in_entry = true;
                } else if (!strcmp(x.tag, "CommonPrefixes")) {
                    in_cp = true;
                } else if (!strcmp(x.tag, "Owner")) {
                    in_owner = true;
                }
                continue;
            }
            if (x.kind == S3M_XML_CLOSE) {
                if (in_entry && (!strcmp(x.tag, "Contents") ||
                                 !strcmp(x.tag, "Version") ||
                                 !strcmp(x.tag, "DeleteMarker"))) {
                    obj.key = keybuf;
                    snprintf(obj.owner, sizeof obj.owner, "%s",
                             owndisp[0] ? owndisp : ownid);
                    cb(ctx, &obj);
                    in_entry = false;
                } else if (!strcmp(x.tag, "Owner")) {
                    in_owner = false;
                } else if (!strcmp(x.tag, "CommonPrefixes")) {
                    in_cp = false;
                }
                continue;
            }
            /* S3M_XML_ELEM */
            if (in_owner) {
                if (!strcmp(x.tag, "ID"))
                    snprintf(ownid, sizeof ownid, "%s", x.text);
                else if (!strcmp(x.tag, "DisplayName"))
                    snprintf(owndisp, sizeof owndisp, "%s", x.text);
            } else if (in_cp) {
                if (!strcmp(x.tag, "Prefix") && stk)
                    s3m_push_job(stk, bucket, x.text, depth + 1);
            } else if (in_entry) {
                if (!strcmp(x.tag, "Key"))
                    snprintf(keybuf, sizeof keybuf, "%s", x.text);
                else if (!strcmp(x.tag, "Size"))
                    obj.size = strtoull(x.text, NULL, 10);
                else if (!strcmp(x.tag, "LastModified"))
                    obj.mtime = s3m_parse_iso8601(x.text);
                else if (!strcmp(x.tag, "ETag")) {
                    const char *t = x.text;
                    size_t tl = strlen(t);
                    if (tl >= 2 && t[0] == '"' && t[tl - 1] == '"') {
                        t++;
                        tl -= 2;
                    }
                    if (tl >= sizeof obj.etag)
                        tl = sizeof obj.etag - 1;
                    memcpy(obj.etag, t, tl);
                    obj.etag[tl] = '\0';
                } else if (!strcmp(x.tag, "StorageClass"))
                    snprintf(obj.storclass, sizeof obj.storclass, "%s",
                             x.text);
                else if (!strcmp(x.tag, "VersionId"))
                    snprintf(obj.version_id, sizeof obj.version_id, "%s",
                             x.text);
                else if (!strcmp(x.tag, "IsLatest"))
                    obj.is_latest = !strcmp(x.text, "true");
            } else {
                if (!strcmp(x.tag, "IsTruncated"))
                    trunc = !strcmp(x.text, "true");
                else if (!strcmp(x.tag, "NextContinuationToken"))
                    snprintf(ntoken, sizeof ntoken, "%s", x.text);
                else if (!strcmp(x.tag, "NextKeyMarker"))
                    snprintf(nkmark, sizeof nkmark, "%s", x.text);
                else if (!strcmp(x.tag, "NextVersionIdMarker"))
                    snprintf(nvmark, sizeof nvmark, "%s", x.text);
            }
        }
        s3m_xml_free(&x);
        s3m_resp_free(&r);

        more = trunc;
        if (versions) {
            if (trunc && !nkmark[0])
                more = false;                     /* defensive */
            snprintf(kmark, sizeof kmark, "%s", nkmark);
            snprintf(vmark, sizeof vmark, "%s", nvmark);
        } else {
            if (trunc && !ntoken[0])
                more = false;                     /* defensive */
            snprintf(token, sizeof token, "%s", ntoken);
        }
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* batched DeleteObjects                                                */
/* ------------------------------------------------------------------ */

void s3m_delbatch_init(s3m_delbatch *b, const char *bucket,
                       s3m_del_cb done, void *ctx)
{
    memset(b, 0, sizeof *b);
    b->bucket = bucket;
    b->done = done;
    b->ctx = ctx;
}

static void xml_escape_cat(struct rbuf *rb, const char *s)
{
    for (const char *p = s; *p; p++) {
        switch (*p) {
        case '&':  rbuf_add(rb, "&amp;", 5);  break;
        case '<':  rbuf_add(rb, "&lt;", 4);   break;
        case '>':  rbuf_add(rb, "&gt;", 4);   break;
        case '"':  rbuf_add(rb, "&quot;", 6); break;
        default:   rbuf_add(rb, p, 1);        break;
        }
    }
}

int s3m_delbatch_flush(s3m_delbatch *b, s3m_http *h)
{
    if (b->n == 0)
        return 0;

    struct rbuf body = { 0 };
    rbuf_add(&body, "<Delete>", 8);
    for (int i = 0; i < b->n; i++) {
        rbuf_add(&body, "<Object><Key>", 13);
        xml_escape_cat(&body, b->keys[i]);
        rbuf_add(&body, "</Key>", 6);
        if (b->vids[i]) {
            rbuf_add(&body, "<VersionId>", 11);
            xml_escape_cat(&body, b->vids[i]);
            rbuf_add(&body, "</VersionId>", 12);
        }
        rbuf_add(&body, "</Object>", 9);
    }
    rbuf_add(&body, "</Delete>", 9);

    s3m_resp r;
    int rc = s3m_req(h, "POST", b->bucket, "", "delete=", body.p, body.len,
                     "application/xml", true, NULL, 0, &r);
    free(body.p);

    int ret = 0;
    if (rc != 0 || r.status != 200) {
        char es[256];
        s3m_resp_errstr(&r, es, sizeof es);
        for (int i = 0; i < b->n; i++)
            b->done(b->ctx, b->keys[i], b->vids[i] ? b->vids[i] : "",
                    false, r.code[0] ? r.code : "error", es);
        ret = -1;
    } else {
        s3m_xml x;
        s3m_xml_init(&x, r.body, r.body_len);
        char key[1100] = "", vid[300] = "", code[64] = "", msg[192] = "";
        bool in_del = false, in_err = false;
        while (s3m_xml_next(&x)) {
            if (x.kind == S3M_XML_OPEN) {
                if (!strcmp(x.tag, "Deleted")) {
                    in_del = true;
                    key[0] = vid[0] = code[0] = msg[0] = '\0';
                } else if (!strcmp(x.tag, "Error")) {
                    in_err = true;
                    key[0] = vid[0] = code[0] = msg[0] = '\0';
                }
            } else if (x.kind == S3M_XML_ELEM && (in_del || in_err)) {
                if (!strcmp(x.tag, "Key"))
                    snprintf(key, sizeof key, "%s", x.text);
                else if (!strcmp(x.tag, "VersionId"))
                    snprintf(vid, sizeof vid, "%s", x.text);
                else if (!strcmp(x.tag, "Code"))
                    snprintf(code, sizeof code, "%s", x.text);
                else if (!strcmp(x.tag, "Message"))
                    snprintf(msg, sizeof msg, "%s", x.text);
            } else if (x.kind == S3M_XML_CLOSE) {
                if (in_del && !strcmp(x.tag, "Deleted")) {
                    b->done(b->ctx, key, vid, true, "", "");
                    in_del = false;
                } else if (in_err && !strcmp(x.tag, "Error")) {
                    b->done(b->ctx, key, vid, false, code, msg);
                    in_err = false;
                }
            }
        }
        s3m_xml_free(&x);
    }
    s3m_resp_free(&r);

    for (int i = 0; i < b->n; i++) {
        free(b->keys[i]);
        free(b->vids[i]);
        b->keys[i] = b->vids[i] = NULL;
    }
    b->n = 0;
    return ret;
}

int s3m_delbatch_add(s3m_delbatch *b, s3m_http *h,
                     const char *key, const char *vid)
{
    b->keys[b->n] = strdup(key);
    b->vids[b->n] = vid && vid[0] ? strdup(vid) : NULL;
    if (!b->keys[b->n] || (vid && vid[0] && !b->vids[b->n])) {
        free(b->keys[b->n]);
        free(b->vids[b->n]);
        b->keys[b->n] = b->vids[b->n] = NULL;
        s3m_note_error(key, "delete", "out of memory");
        return -1;
    }
    b->n++;
    if (b->n == S3M_DEL_MAX)
        return s3m_delbatch_flush(b, h);
    return 0;
}
