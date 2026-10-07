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
/* send every byte (send() may send only part of the data) */
static int send_all(Client *c, const char *data, size_t len)
{
    size_t sent = 0;
    pthread_mutex_lock(&c->send_lock);
    while (sent < len) {
        ssize_t n = send(c->fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            pthread_mutex_unlock(&c->send_lock);
            return -1;
        }
        sent += (size_t)n;
    }
    pthread_mutex_unlock(&c->send_lock);
    return 0;
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
    if (strcmp(cmd, "LIST") == 0) {
        cmd_list(c);
    } else {
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
