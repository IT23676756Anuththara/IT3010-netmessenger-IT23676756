/*
 * server_6756.c - NetMessenger server (IE3010)
 * Registration number : IT23676756
 * Port                : 6000 + 6756 = 12756
 * NID tag             : NID:6767 (digits 3-6 of 23676756)
 * Concurrency model   : one POSIX thread per client (pthreads)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <stdarg.h>
#include <ctype.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* ---------- personalised values (from registration number) ---------- */
#define REG_NO    "IT23676756"
#define PORT      12756
#define NID       "NID:6767"
#define LOG_FILE  "netmsg_" REG_NO ".log"

/* ---------- limits ---------- */
#define MAX_CLIENTS 64
#define MAX_NAME    32
#define BUF_SIZE    4096      /* per-client receive buffer = longest allowed line */
#define OUT_SIZE    8192      /* outgoing line buffer */
#define MAX_ROOMS_PER_CLIENT 8   /* a client can be in at most 8 rooms */
#define MAX_FILE_SIZE (10ULL * 1024 * 1024)   /* 10 MB limit for SENDFILE */
#define STORAGE_ROOT  "./storage/" REG_NO       /* ./storage/IT23676756 */

/* ---------- per-client state ---------- */
typedef struct {
    int    fd;                /* socket descriptor */
    int    in_use;            /* slot occupied? */
    int    registered;        /* has a username? */
    char   name[MAX_NAME];
    char   ip[INET_ADDRSTRLEN];
    int    port;
    char   buf[BUF_SIZE];     /* bytes received but not yet processed */
    size_t buf_len;
    char   rooms[MAX_ROOMS_PER_CLIENT][MAX_NAME]; /* rooms this client has joined */
    int    nrooms;
    pthread_mutex_t send_lock;/* stops two threads mixing bytes on one socket */
} Client;

static Client clients[MAX_CLIENTS];
static pthread_mutex_t g_lock   = PTHREAD_MUTEX_INITIALIZER; /* protects clients[] */
static pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER; /* protects log file  */

/* ================= logging ================= */
static void log_event(const char *fmt, ...)
{
    char msg[512], ts[32];
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    strftime(ts, sizeof ts, "%Y-%m-%d %H:%M:%S", &tmv);

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof msg, fmt, ap);
    va_end(ap);

    pthread_mutex_lock(&log_lock);
    FILE *f = fopen(LOG_FILE, "a");
    if (f) {
        fprintf(f, "[%s] %s\n", ts, msg);
        fclose(f);
    }
    printf("[%s] %s\n", ts, msg);
    fflush(stdout);
    pthread_mutex_unlock(&log_lock);
}

/* ================= sending ================= */
/* send every byte on a socket (send() may send only part of the data) */
static int send_raw(int fd, const char *data, size_t len)
{
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        sent += (size_t)n;
    }
    return 0;
}

/* same, but takes the client's send lock so lines never get mixed up */
static int send_all(Client *c, const char *data, size_t len)
{
    pthread_mutex_lock(&c->send_lock);
    int r = send_raw(c->fd, data, len);
    pthread_mutex_unlock(&c->send_lock);
    return r;
}

/* OK / ERR responses: always end with " NID:6767\n" */
static void reply(Client *c, const char *fmt, ...)
{
    char body[OUT_SIZE - 32], line[OUT_SIZE];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(body, sizeof body, fmt, ap);
    va_end(ap);
    int n = snprintf(line, sizeof line, "%s " NID "\n", body);
    if (n >= (int)sizeof line) n = (int)sizeof line - 1;
    send_all(c, line, (size_t)n);
}

/* send ONE MSG line to ONE client: NO NID tag */
static void send_msg(Client *c, const char *fmt, ...)
{
    char body[OUT_SIZE - 2], line[OUT_SIZE];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(body, sizeof body, fmt, ap);
    va_end(ap);
    int n = snprintf(line, sizeof line, "%s\n", body);
    if (n >= (int)sizeof line) n = (int)sizeof line - 1;
    send_all(c, line, (size_t)n);
}

/* send a MSG line to every registered client except 'skip' (may be NULL) */
static void broadcast_except(const Client *skip, const char *fmt, ...)
{
    char body[OUT_SIZE - 2], line[OUT_SIZE];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(body, sizeof body, fmt, ap);
    va_end(ap);
    int n = snprintf(line, sizeof line, "%s\n", body);
    if (n >= (int)sizeof line) n = (int)sizeof line - 1;

    pthread_mutex_lock(&g_lock);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].in_use && clients[i].registered && &clients[i] != skip)
            send_all(&clients[i], line, (size_t)n);
    }
    pthread_mutex_unlock(&g_lock);
}

/* ================= receiving / framing ================= */
/*
 * Reads ONE line (ending in '\n') into 'out'.
 * Handles: partial lines (keeps reading), several lines in one recv()
 * (leftover stays in c->buf for the next call).
 * Returns 1 = got a line, 0 = connection closed/error, -1 = line too long.
 */
static int read_line(Client *c, char *out, size_t outsz)
{
    for (;;) {
        char *nl = memchr(c->buf, '\n', c->buf_len);
        if (nl) {
            size_t len = (size_t)(nl - c->buf);
            size_t copy = (len < outsz - 1) ? len : outsz - 1;
            memcpy(out, c->buf, copy);
            out[copy] = '\0';
            if (copy > 0 && out[copy - 1] == '\r') out[copy - 1] = '\0';
            size_t used = len + 1;                       /* line + '\n' */
            memmove(c->buf, c->buf + used, c->buf_len - used);
            c->buf_len -= used;
            return 1;
        }
        if (c->buf_len == BUF_SIZE) {                    /* no '\n' and buffer full */
            c->buf_len = 0;
            return -1;
        }
        ssize_t n = recv(c->fd, c->buf + c->buf_len, BUF_SIZE - c->buf_len, 0);
        if (n == 0) return 0;                            /* peer closed */
        if (n < 0) {
            if (errno == EINTR) continue;
            return 0;                                    /* error / reset */
        }
        c->buf_len += (size_t)n;
    }
}

/* ================= command handlers ================= */
static int valid_name(const char *s)
{
    size_t len = strlen(s);
    if (len == 0 || len >= MAX_NAME) return 0;
    for (size_t i = 0; i < len; i++)
        if (!isalnum((unsigned char)s[i]) && s[i] != '_' && s[i] != '-') return 0;
    return 1;   /* only safe characters: also protects the storage path */
}

static void cmd_register(Client *c, const char *name)
{
    if (c->registered) { reply(c, "ERR 008 ALREADY_REGISTERED"); return; }
    if (!valid_name(name)) { reply(c, "ERR 009 INVALID_USERNAME"); return; }

    pthread_mutex_lock(&g_lock);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].in_use && clients[i].registered &&
            strcmp(clients[i].name, name) == 0) {
            pthread_mutex_unlock(&g_lock);
            reply(c, "ERR 001 USERNAME_TAKEN");
            return;
        }
    }
    snprintf(c->name, sizeof c->name, "%s", name);
    c->registered = 1;                /* check + set inside ONE lock: no race */
    pthread_mutex_unlock(&g_lock);

    reply(c, "OK REGISTERED %s", name);
    log_event("REGISTER user=%s from %s:%d", name, c->ip, c->port);
    broadcast_except(c, "MSG PRESENCE JOINED %s", name);
}

static void cmd_list(Client *c)
{
    char list[BUF_SIZE];
    size_t len = 0;
    int first = 1;
    list[0] = '\0';

    pthread_mutex_lock(&g_lock);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].in_use && clients[i].registered) {
            len += (size_t)snprintf(list + len, sizeof list - len, "%s%s",
                                    first ? "" : ",", clients[i].name);
            first = 0;
        }
    }
    pthread_mutex_unlock(&g_lock);

    reply(c, "OK USERS %s", list);
}

/* split "word rest of line": returns the first word, *rest = what follows */
static char *split_arg(char *s, char **rest)
{
    char *sp = strchr(s, ' ');
    if (sp) {
        *sp++ = '\0';
        while (*sp == ' ') sp++;
        *rest = sp;
    } else {
        *rest = s + strlen(s);
    }
    return s;
}

/* --- room helpers: the caller MUST already hold g_lock --- */
static int in_room(const Client *c, const char *room)
{
    for (int j = 0; j < c->nrooms; j++)
        if (strcmp(c->rooms[j], room) == 0) return j;
    return -1;
}

/* a room exists as long as at least one client is inside it */
static int room_exists(const char *room)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (clients[i].in_use && clients[i].registered &&
            in_room(&clients[i], room) >= 0) return 1;
    return 0;
}

static void cmd_bcast(Client *c, const char *msg)
{
    if (*msg == '\0') { reply(c, "ERR 015 MISSING_ARGUMENT"); return; }
    broadcast_except(c, "MSG BCAST %s %s", c->name, msg);
    reply(c, "OK SENT");
    log_event("BCAST from=%s bytes=%zu", c->name, strlen(msg));
}

static void cmd_pmsg(Client *c, char *args)
{
    char *msg;
    char *target = split_arg(args, &msg);
    if (*target == '\0' || *msg == '\0') { reply(c, "ERR 015 MISSING_ARGUMENT"); return; }

    int found = 0;
    pthread_mutex_lock(&g_lock);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].in_use && clients[i].registered &&
            strcmp(clients[i].name, target) == 0) {
            send_msg(&clients[i], "MSG PRIV %s %s", c->name, msg);
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&g_lock);

    if (!found) { reply(c, "ERR 002 USER_NOT_FOUND"); return; }
    reply(c, "OK SENT");
    log_event("PMSG from=%s to=%s bytes=%zu", c->name, target, strlen(msg));
}

static void cmd_join(Client *c, const char *room)
{
    if (!valid_name(room)) { reply(c, "ERR 013 INVALID_ROOM_NAME"); return; }

    int too_many = 0;
    pthread_mutex_lock(&g_lock);
    if (in_room(c, room) < 0) {                    /* not a member yet */
        if (c->nrooms >= MAX_ROOMS_PER_CLIENT)
            too_many = 1;
        else
            snprintf(c->rooms[c->nrooms++], MAX_NAME, "%s", room);
    }                                              /* room is created by its first member */
    pthread_mutex_unlock(&g_lock);

    if (too_many) { reply(c, "ERR 014 TOO_MANY_ROOMS"); return; }
    reply(c, "OK JOINED %s", room);
    log_event("JOIN user=%s room=%s", c->name, room);
}

static void cmd_leave(Client *c, const char *room)
{
    pthread_mutex_lock(&g_lock);
    int idx = in_room(c, room);
    if (idx >= 0) {
        for (int j = idx; j < c->nrooms - 1; j++)  /* close the gap in the array */
            memcpy(c->rooms[j], c->rooms[j + 1], MAX_NAME);
        c->nrooms--;
    }
    pthread_mutex_unlock(&g_lock);

    if (idx < 0) { reply(c, "ERR 003 ROOM_NOT_FOUND"); return; }
    reply(c, "OK LEFT %s", room);
    log_event("LEAVE user=%s room=%s", c->name, room);
}

static void cmd_rooms(Client *c)
{
    char list[BUF_SIZE];
    const char *seen[MAX_CLIENTS * MAX_ROOMS_PER_CLIENT];
    int nseen = 0;
    size_t len = 0;
    list[0] = '\0';

    pthread_mutex_lock(&g_lock);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (!clients[i].in_use || !clients[i].registered) continue;
        for (int j = 0; j < clients[i].nrooms; j++) {
            const char *r = clients[i].rooms[j];
            int dup = 0;
            for (int k = 0; k < nseen; k++)
                if (strcmp(seen[k], r) == 0) { dup = 1; break; }
            if (dup) continue;                     /* list each room only once */
            seen[nseen++] = r;
            len += (size_t)snprintf(list + len, sizeof list - len, "%s%s",
                                    nseen > 1 ? "," : "", r);
        }
    }
    pthread_mutex_unlock(&g_lock);

    if (nseen == 0) reply(c, "OK ROOMS");
    else            reply(c, "OK ROOMS %s", list);
}

static void cmd_rmsg(Client *c, char *args)
{
    char *msg;
    char *room = split_arg(args, &msg);
    if (*room == '\0' || *msg == '\0') { reply(c, "ERR 015 MISSING_ARGUMENT"); return; }

    pthread_mutex_lock(&g_lock);
    int exists = room_exists(room);
    int member = (in_room(c, room) >= 0);
    if (exists && member) {
        for (int i = 0; i < MAX_CLIENTS; i++)
            if (clients[i].in_use && clients[i].registered &&
                &clients[i] != c && in_room(&clients[i], room) >= 0)
                send_msg(&clients[i], "MSG ROOM %s %s %s", room, c->name, msg);
    }
    pthread_mutex_unlock(&g_lock);

    if (!exists)      { reply(c, "ERR 003 ROOM_NOT_FOUND"); return; }
    if (!member)      { reply(c, "ERR 011 NOT_IN_ROOM");    return; }
    reply(c, "OK SENT");
    log_event("RMSG from=%s room=%s bytes=%zu", c->name, room, strlen(msg));
}

/* ================= SENDFILE ================= */
/* digits only, fits in unsigned long long */
static int parse_size(const char *s, unsigned long long *out)
{
    if (*s == '\0') return 0;
    for (const char *p = s; *p; p++)
        if (!isdigit((unsigned char)*p)) return 0;
    errno = 0;
    unsigned long long v = strtoull(s, NULL, 10);
    if (errno == ERANGE) return 0;
    *out = v;
    return 1;
}

/* safe file names only: stops "../../etc/passwd" style path tricks */
static int valid_filename(const char *s)
{
    size_t len = strlen(s);
    if (len == 0 || len > 100 || s[0] == '.') return 0;
    for (size_t i = 0; i < len; i++)
        if (!isalnum((unsigned char)s[i]) && s[i] != '.' && s[i] != '_' && s[i] != '-')
            return 0;
    return 1;
}

/*
 * Consume EXACTLY n bytes from the client: first the bytes that already
 * arrived together with the command line (c->buf), then more recv() calls
 * (as many as it takes).  If 'out' != NULL the bytes are written to it,
 * otherwise they are thrown away (used when we must reject the file but
 * still keep the byte stream in sync).
 * Returns 1 = all n bytes consumed, 0 = connection lost.
 */
static int consume_bytes(Client *c, unsigned long long n, FILE *out, int *werr)
{
    char chunk[BUF_SIZE];
    while (n > 0) {
        size_t got;
        if (c->buf_len > 0) {
            got = (c->buf_len < n) ? c->buf_len : (size_t)n;
            if (out && !*werr && fwrite(c->buf, 1, got, out) != got) *werr = 1;
            memmove(c->buf, c->buf + got, c->buf_len - got);
            c->buf_len -= got;
        } else {
            size_t want = (n < sizeof chunk) ? (size_t)n : sizeof chunk;
            ssize_t r = recv(c->fd, chunk, want, 0);
            if (r == 0) return 0;
            if (r < 0) {
                if (errno == EINTR) continue;
                return 0;
            }
            got = (size_t)r;
            if (out && !*werr && fwrite(chunk, 1, got, out) != got) *werr = 1;
        }
        n -= got;
    }
    return 1;
}

/* send header line + the stored file to one client, as ONE uninterrupted block */
static void deliver_file(Client *dst, const char *header, const char *path)
{
    FILE *fp = fopen(path, "rb");
    if (!fp) return;
    char chunk[BUF_SIZE];
    size_t n;

    pthread_mutex_lock(&dst->send_lock);
    int ok = (send_raw(dst->fd, header, strlen(header)) == 0);
    while (ok && (n = fread(chunk, 1, sizeof chunk, fp)) > 0)
        ok = (send_raw(dst->fd, chunk, n) == 0);
    pthread_mutex_unlock(&dst->send_lock);
    fclose(fp);
}

/* returns 1 if the connection was lost while receiving the file */
static int cmd_sendfile(Client *c, char *args)
{
    char *r1, *sizestr;
    char *target = split_arg(args, &r1);
    char *fname  = split_arg(r1, &sizestr);
    unsigned long long size;

    if (*target == '\0' || *fname == '\0' || *sizestr == '\0') {
        reply(c, "ERR 015 MISSING_ARGUMENT");
        return 0;
    }
    if (!parse_size(sizestr, &size)) {      /* cannot know how many bytes follow */
        reply(c, "ERR 016 INVALID_FILESIZE");
        return 0;
    }

    /* ---- checks that reject the file: the bytes must still be read and dropped ---- */
    int err = 0;
    const char *errmsg = NULL;
    int kind = 0;                            /* 1 = user, 2 = room */

    if (size > MAX_FILE_SIZE) {
        err = 1; errmsg = "ERR 004 FILE_TOO_LARGE";
    } else if (!valid_filename(fname)) {
        err = 1; errmsg = "ERR 017 INVALID_FILENAME";
    } else {
        pthread_mutex_lock(&g_lock);
        for (int i = 0; i < MAX_CLIENTS; i++)
            if (clients[i].in_use && clients[i].registered &&
                strcmp(clients[i].name, target) == 0) { kind = 1; break; }
        if (!kind && room_exists(target)) {
            if (in_room(c, target) >= 0) kind = 2;
            else { err = 1; errmsg = "ERR 011 NOT_IN_ROOM"; }
        } else if (!kind) {
            err = 1; errmsg = "ERR 002 USER_NOT_FOUND";
        }
        pthread_mutex_unlock(&g_lock);
    }

    int werr = 0;
    if (err) {
        if (!consume_bytes(c, size, NULL, &werr)) return 1;   /* drop the bytes */
        reply(c, "%s", errmsg);
        log_event("SENDFILE rejected from=%s target=%s file=%s: %s", c->name, target, fname, errmsg);
        return 0;
    }

    /* ---- store ./storage/IT23676756/<sender>/<filename> ---- */
    char dir[256], path[512];
    (void)mkdir("./storage", 0755);
    (void)mkdir(STORAGE_ROOT, 0755);
    snprintf(dir, sizeof dir, "%s/%s", STORAGE_ROOT, c->name);
    snprintf(path, sizeof path, "%s/%s", dir, fname);
    if (mkdir(dir, 0755) < 0 && errno != EEXIST) path[0] = '\0';

    FILE *fp = path[0] ? fopen(path, "wb") : NULL;
    if (!fp) {
        if (!consume_bytes(c, size, NULL, &werr)) return 1;
        reply(c, "ERR 018 STORAGE_ERROR");
        log_event("SENDFILE storage error from=%s file=%s", c->name, fname);
        return 0;
    }
    int alive = consume_bytes(c, size, fp, &werr);   /* exactly 'size' bytes */
    fclose(fp);
    if (!alive) {                                    /* sender died mid-transfer */
        unlink(path);
        log_event("SENDFILE aborted (sender lost) from=%s file=%s", c->name, fname);
        return 1;
    }
    if (werr) {
        unlink(path);
        reply(c, "ERR 018 STORAGE_ERROR");
        return 0;
    }

    /* ---- forward to the target(s) ---- */
    char header[512];
    int delivered = 0;
    pthread_mutex_lock(&g_lock);
    if (kind == 1) {
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].in_use && clients[i].registered &&
                strcmp(clients[i].name, target) == 0) {
                snprintf(header, sizeof header, "MSG FILE PRIV %s %s %llu\n",
                         c->name, fname, size);
                deliver_file(&clients[i], header, path);
                delivered = 1;
                break;
            }
        }
    } else {
        snprintf(header, sizeof header, "MSG FILE ROOM %s %s %s %llu\n",
                 target, c->name, fname, size);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].in_use && clients[i].registered &&
                &clients[i] != c && in_room(&clients[i], target) >= 0) {
                deliver_file(&clients[i], header, path);
            }
        }
        delivered = 1;   /* an empty room (only the sender) is still a successful upload */
    }
    pthread_mutex_unlock(&g_lock);

    if (!delivered) { reply(c, "ERR 002 USER_NOT_FOUND"); return 0; }
    reply(c, "OK FILE_RECEIVED %s", fname);
    log_event("SENDFILE from=%s target=%s file=%s bytes=%llu stored=%s",
              c->name, target, fname, size, path);
    return 0;
}

/* returns 1 if the connection should be closed */
static int handle_command(Client *c, char *line)
{
    char *rest = strchr(line, ' ');
    if (rest) {
        *rest++ = '\0';
        while (*rest == ' ') rest++;
    } else {
        rest = line + strlen(line);   /* empty argument string */
    }
    const char *cmd = line;

    if (strcmp(cmd, "QUIT") == 0) {
        reply(c, "OK BYE");
        return 1;
    }
    if (strcmp(cmd, "REGISTER") == 0) {
        cmd_register(c, rest);
        return 0;
    }
    if (!c->registered) {             /* REGISTER must be the first command */
        reply(c, "ERR 006 NOT_REGISTERED");
        return 0;
    }
    if (strcmp(cmd, "LIST") == 0)           cmd_list(c);
    else if (strcmp(cmd, "BCAST") == 0)     cmd_bcast(c, rest);
    else if (strcmp(cmd, "PMSG") == 0)      cmd_pmsg(c, rest);
    else if (strcmp(cmd, "JOIN") == 0)      cmd_join(c, rest);
    else if (strcmp(cmd, "LEAVE") == 0)     cmd_leave(c, rest);
    else if (strcmp(cmd, "ROOMS") == 0)     cmd_rooms(c);
    else if (strcmp(cmd, "RMSG") == 0)      cmd_rmsg(c, rest);
    else if (strcmp(cmd, "SENDFILE") == 0)  return cmd_sendfile(c, rest);
    else {
        reply(c, "ERR 007 UNKNOWN_COMMAND");
    }
    return 0;
}

/* ================= one thread per client ================= */
static void *client_thread(void *arg)
{
    Client *c = (Client *)arg;
    char line[BUF_SIZE];
    int quit = 0;

    log_event("CONNECT %s:%d", c->ip, c->port);

    while (!quit) {
        int r = read_line(c, line, sizeof line);
        if (r == 0) break;                         /* disconnect (graceful or not) */
        if (r < 0) { reply(c, "ERR 005 LINE_TOO_LONG"); continue; }
        if (line[0] == '\0') continue;             /* ignore empty lines */
        quit = handle_command(c, line);
    }

    /* ---- cleanup: free the slot, tell the others ---- */
    pthread_mutex_lock(&g_lock);
    int was_registered = c->registered;
    char name[MAX_NAME];
    snprintf(name, sizeof name, "%s", c->name);
    c->registered = 0;
    c->name[0] = '\0';
    c->nrooms = 0;                  /* leaves every room */
    close(c->fd);
    c->fd = -1;
    c->in_use = 0;
    pthread_mutex_unlock(&g_lock);

    log_event("DISCONNECT %s:%d user=%s", c->ip, c->port,
              was_registered ? name : "(unregistered)");
    if (was_registered)
        broadcast_except(NULL, "MSG PRESENCE LEFT %s", name);
    return NULL;
}

/* ================= main: listen + accept loop ================= */
int main(void)
{
    signal(SIGPIPE, SIG_IGN);         /* writing to a dead client must not kill us */

    for (int i = 0; i < MAX_CLIENTS; i++) {
        clients[i].fd = -1;
        clients[i].in_use = 0;
        pthread_mutex_init(&clients[i].send_lock, NULL);
    }

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { perror("socket"); return 1; }

    int yes = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(PORT);

    if (bind(lfd, (struct sockaddr *)&addr, sizeof addr) < 0) { perror("bind"); return 1; }
    if (listen(lfd, 16) < 0) { perror("listen"); return 1; }

    log_event("SERVER START reg=%s port=%d tag=%s", REG_NO, PORT, NID);

    for (;;) {
        struct sockaddr_in ca;
        socklen_t cl = sizeof ca;
        int cfd = accept(lfd, (struct sockaddr *)&ca, &cl);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }

        pthread_mutex_lock(&g_lock);
        int slot = -1;
        for (int i = 0; i < MAX_CLIENTS; i++)
            if (!clients[i].in_use) { slot = i; break; }
        if (slot >= 0) {
            Client *c = &clients[slot];
            c->in_use = 1;
            c->fd = cfd;
            c->registered = 0;
            c->name[0] = '\0';
            c->buf_len = 0;
            c->nrooms = 0;
            inet_ntop(AF_INET, &ca.sin_addr, c->ip, sizeof c->ip);
            c->port = ntohs(ca.sin_port);
        }
        pthread_mutex_unlock(&g_lock);

        if (slot < 0) {
            const char *m = "ERR 010 SERVER_FULL " NID "\n";
            send(cfd, m, strlen(m), MSG_NOSIGNAL);
            close(cfd);
            continue;
        }

        pthread_t tid;
        if (pthread_create(&tid, NULL, client_thread, &clients[slot]) != 0) {
            pthread_mutex_lock(&g_lock);
            close(clients[slot].fd);
            clients[slot].fd = -1;
            clients[slot].in_use = 0;
            pthread_mutex_unlock(&g_lock);
            continue;
        }
        pthread_detach(tid);
    }
    return 0;
}
