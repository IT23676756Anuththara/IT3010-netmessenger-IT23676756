/*
 * client_6756.c - NetMessenger client (IE3010)
 * Registration number : IT23676756
 * Port                : 6000 + 6756 = 12756
 *
 * Usage: ./client_6756 [server-ip]      (default 127.0.0.1)
 * Type protocol commands directly (REGISTER amal, BCAST hi, PMSG amal hi,
 * JOIN room, RMSG room hi, ROOMS, LIST, QUIT).
 * Extra helper:  /sendfile <target> <path>   sends a local file with SENDFILE.
 * Files sent to you are saved in ./downloads/
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include <poll.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netdb.h>

#define REG_NO "IT23676756"
#define PORT   "12756"            /* 6000 + 6756 */
#define RBUF   65536
#define LINE   4096

static int  sockfd = -1;
static char rbuf[RBUF];           /* bytes received from the server, not yet processed */
static size_t rlen = 0;
static int  quitting = 0;

/* ---------- sending ---------- */
static int send_all(const char *data, size_t len)
{
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(sockfd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        sent += (size_t)n;
    }
    return 0;
}

/* /sendfile <target> <path>  ->  SENDFILE <target> <name> <size>\n + raw bytes */
static void do_sendfile(char *args)
{
    char *sp = strchr(args, ' ');
    if (!sp) { printf("usage: /sendfile <user-or-room> <path>\n"); return; }
    *sp++ = '\0';
    while (*sp == ' ') sp++;
    const char *target = args, *path = sp;

    FILE *fp = fopen(path, "rb");
    if (!fp) { printf("cannot open '%s': %s\n", path, strerror(errno)); return; }
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    if (size < 0) { fclose(fp); printf("cannot read file size\n"); return; }

    const char *base = strrchr(path, '/');
    base = base ? base + 1 : path;

    char header[LINE];
    int n = snprintf(header, sizeof header, "SENDFILE %s %s %ld\n", target, base, size);
    if (send_all(header, (size_t)n) < 0) { fclose(fp); return; }

    char chunk[8192];
    size_t got;
    long total = 0;
    while ((got = fread(chunk, 1, sizeof chunk, fp)) > 0) {
        if (send_all(chunk, got) < 0) break;
        total += (long)got;
    }
    fclose(fp);
    printf("[sent %ld of %ld bytes of '%s', waiting for server reply]\n", total, size, base);
}

/* ---------- receiving ---------- */
/* a file arrives as:  MSG FILE ... <size>\n  followed by exactly <size> raw bytes */
static int receive_file(const char *name, unsigned long long size, const char *from)
{
    (void)mkdir("downloads", 0755);
    char path[512];
    int safe = (name[0] != '.' && strchr(name, '/') == NULL);
    FILE *fp = NULL;
    if (safe) {
        snprintf(path, sizeof path, "downloads/%s", name);
        fp = fopen(path, "wb");
    }
    unsigned long long left = size;
    while (left > 0) {
        if (rlen == 0) {                              /* need more bytes from the socket */
            ssize_t n = recv(sockfd, rbuf, sizeof rbuf, 0);
            if (n == 0) { if (fp) fclose(fp); return -1; }
            if (n < 0) {
                if (errno == EINTR) continue;
                if (fp) fclose(fp);
                return -1;
            }
            rlen = (size_t)n;
        }
        size_t take = (rlen < left) ? rlen : (size_t)left;
        if (fp) fwrite(rbuf, 1, take, fp);
        memmove(rbuf, rbuf + take, rlen - take);
        rlen -= take;
        left -= take;
    }
    if (fp) {
        fclose(fp);
        printf("[FILE] received '%s' (%llu bytes) from %s -> %s\n", name, size, from, path);
    } else {
        printf("[FILE] refused unsafe file name '%s'\n", name);
    }
    return 0;
}

/* returns -1 if the connection was lost while reading a file */
static int handle_line(const char *line)
{
    char a[64], b[64], name[256];
    unsigned long long size;

    if (sscanf(line, "MSG FILE PRIV %63s %255s %llu", a, name, &size) == 3)
        return receive_file(name, size, a);
    if (sscanf(line, "MSG FILE ROOM %63s %63s %255s %llu", a, b, name, &size) == 4)
        return receive_file(name, size, b);

    printf("%s\n", line);
    if (strncmp(line, "OK BYE", 6) == 0) quitting = 1;
    return 0;
}

/* read from the socket, split into lines, handle each line */
static int process_incoming(void)
{
    ssize_t n = recv(sockfd, rbuf + rlen, sizeof rbuf - rlen, 0);
    if (n == 0) return -1;
    if (n < 0) return (errno == EINTR) ? 0 : -1;
    rlen += (size_t)n;

    for (;;) {
        char *nl = memchr(rbuf, '\n', rlen);
        if (!nl) {
            if (rlen == sizeof rbuf) rlen = 0;      /* absurdly long line: drop it */
            return 0;
        }
        size_t len = (size_t)(nl - rbuf);
        char line[LINE];
        size_t copy = (len < sizeof line - 1) ? len : sizeof line - 1;
        memcpy(line, rbuf, copy);
        line[copy] = '\0';
        memmove(rbuf, rbuf + len + 1, rlen - len - 1);
        rlen -= len + 1;
        if (handle_line(line) < 0) return -1;
    }
}

/* ---------- keyboard ---------- */
static void handle_stdin_line(char *line)
{
    size_t len = strlen(line);
    while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) line[--len] = '\0';
    if (len == 0) return;

    if (strncmp(line, "/sendfile ", 10) == 0) { do_sendfile(line + 10); return; }
    if (strcmp(line, "/help") == 0) {
        printf("Commands: REGISTER <name> | LIST | BCAST <msg> | PMSG <user> <msg>\n"
               "          JOIN <room> | LEAVE <room> | ROOMS | RMSG <room> <msg>\n"
               "          /sendfile <user-or-room> <path> | QUIT\n");
        return;
    }
    for (char *p = line; *p && *p != ' '; p++)     /* 'list' works like 'LIST' */
        *p = (char)toupper((unsigned char)*p);

    char out[LINE + 2];
    int n = snprintf(out, sizeof out, "%s\n", line);
    if (n >= (int)sizeof out) n = (int)sizeof out - 1;
    send_all(out, (size_t)n);
}

int main(int argc, char **argv)
{
    const char *host = (argc > 1) ? argv[1] : "127.0.0.1";
    signal(SIGPIPE, SIG_IGN);

    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof hints);
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    int rc = getaddrinfo(host, PORT, &hints, &res);
    if (rc != 0) { fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rc)); return 1; }

    sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sockfd < 0) { perror("socket"); return 1; }
    if (connect(sockfd, res->ai_addr, res->ai_addrlen) < 0) {
        perror("connect");
        fprintf(stderr, "Is the server running on %s port %s?\n", host, PORT);
        return 1;
    }
    freeaddrinfo(res);
    printf("Connected to %s:%s (%s). Type REGISTER <name> first, /help for help.\n",
           host, PORT, REG_NO);

    struct pollfd fds[2];
    fds[0].fd = STDIN_FILENO; fds[0].events = POLLIN;
    fds[1].fd = sockfd;       fds[1].events = POLLIN;
    int stdin_open = 1;

    while (!quitting) {
        fds[0].fd = stdin_open ? STDIN_FILENO : -1;   /* poll ignores fd = -1 */
        if (poll(fds, 2, -1) < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }
        if (fds[1].revents & (POLLIN | POLLHUP | POLLERR)) {
            if (process_incoming() < 0) {
                printf("Server closed the connection.\n");
                break;
            }
        }
        if (stdin_open && (fds[0].revents & (POLLIN | POLLHUP))) {
            char line[LINE];
            if (!fgets(line, sizeof line, stdin)) {   /* Ctrl+D / end of input */
                stdin_open = 0;
                send_all("QUIT\n", 5);
            } else {
                handle_stdin_line(line);
            }
        }
    }
    close(sockfd);
    return 0;
}
