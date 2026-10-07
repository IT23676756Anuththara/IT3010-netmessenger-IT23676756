# NetMessenger (IE3010 Network Programming)

Multi-client chat and file-sharing platform over TCP/IP, written in C with BSD sockets.

**Registration number:** IT23676756

| Item | Value | Calculation |
|---|---|---|
| Port | **12756** | 6000 + last four digits (6756) |
| NID tag | **NID:6767** | numeric part 23676756, digits 3-6 |
| Source files | server_6756.c, client_6756.c, Makefile_6756 | last four digits = 6756 |
| Log file | netmsg_IT23676756.log | netmsg_<regno>.log |
| Storage path | ./storage/IT23676756/<sender_username>/<filename> | |
| ZIP archive | IE3010_IT23676756.zip | |

## Build
    make -f Makefile_6756

## Run
    ./server_6756                 # terminal 1
    ./client_6756                 # terminal 2, 3, ... (optional argument: server IP)

In the client, type protocol commands directly (lower case is accepted):
REGISTER, LIST, BCAST, PMSG, JOIN, LEAVE, ROOMS, RMSG, QUIT.
Extra helper: `/sendfile <user-or-room> <path>`. Received files are saved in `./downloads/`.

## Architecture
- One server process, **one POSIX thread per client** (pthreads).
- Shared state (client table, room membership) protected by a mutex; a per-client send mutex keeps lines from interleaving.
- Line framing: a per-client buffer handles partial lines and several lines in one recv().
- SENDFILE reads exactly <filesize> bytes (as many recv() calls as needed), streams them to disk, then forwards the stored file.
- Client uses poll() on stdin and the socket (single thread).

## Protocol notes and assumptions
- Every OK/ERR response ends with ` NID:6767`; forwarded MSG lines do not.
- Presence notices: `MSG PRESENCE JOINED <user>` / `MSG PRESENCE LEFT <user>`.
- File delivered to a user: `MSG FILE PRIV <sender> <filename> <size>` + raw bytes.
- File delivered to a room: `MSG FILE ROOM <room> <sender> <filename> <size>` + raw bytes.
- A room exists while it has at least one member; only members can send to a room.
- Max file size 10 MB; file names limited to letters, digits, `.`, `_`, `-`.
- Error codes: 001 USERNAME_TAKEN, 002 USER_NOT_FOUND, 003 ROOM_NOT_FOUND, 004 FILE_TOO_LARGE,
  005 LINE_TOO_LONG, 006 NOT_REGISTERED, 007 UNKNOWN_COMMAND, 008 ALREADY_REGISTERED,
  009 INVALID_USERNAME, 010 SERVER_FULL, 011 NOT_IN_ROOM, 013 INVALID_ROOM_NAME,
  014 TOO_MANY_ROOMS, 015 MISSING_ARGUMENT, 016 INVALID_FILESIZE, 017 INVALID_FILENAME, 018 STORAGE_ERROR.

## Known limitations
- While a file is forwarded the server holds the client-table lock, so other clients may wait briefly.
- No encryption or authentication (optional extensions not implemented).

## Files
- `DESIGN_DIARY.md` - design decisions and obstacles
