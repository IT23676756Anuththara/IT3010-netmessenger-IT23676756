# Design Diary - NetMessenger (IT23676756)

This assignment was done between 6 and 7 October 2026, so the work was compressed into about
two days. This diary was written up from my notes right after the main implementation was working,
not step by step every day. I used an AI assistant (Claude) for guidance; the details are in my prompt log.

## Setup and obstacles
- Calculated my personalised values from IT23676756: port 12756, NID:6767, files *_6756,
  log netmsg_IT23676756.log, storage ./storage/IT23676756/<sender>/<filename>.
- I had never used Git or GitHub before. Problems I hit and fixed:
  - Git rejects the account password; a personal access token is needed.
  - I typed the token into the username prompt at first, and I used my Linux user name
    instead of my GitHub user name in the repository URL.
  - I pasted a token into a chat, so I deleted it and made a new one.
- Installed WSL (Ubuntu) to get a Linux environment. Compilation first failed because the C headers
  were missing; fixed by installing build-essential.
- Linux file names are case sensitive: my repository folder was IT3010-netmessenger-..., not the name I typed.
- I ran commands in the wrong terminal (PowerShell instead of Ubuntu) and in the wrong folder a few times.

## Design decisions
- **Concurrency model:** one pthread per client. Each client has a simple blocking recv() loop, which
  makes the exact-byte-count file reception easy to write and to explain. Alternative considered:
  a single-threaded select()/poll() server (no locks, but more per-connection state to track).
- **Shared state:** one global mutex protects the client table; a send mutex per client stops two
  threads mixing bytes on the same socket (important when a file is being forwarded).
- **Framing:** each client has a receive buffer. read_line() splits on '\n' and keeps leftover bytes,
  so partial lines and several lines in one recv() both work. SENDFILE uses the leftover bytes first,
  then calls recv() until exactly <filesize> bytes have been read.
- **Rooms:** each client stores the names of the rooms it has joined. A room exists while at least one
  client is in it, so there is no separate room table to clean up when a client disconnects.
- **Safety:** user names, room names and file names accept only letters, digits, '_', '-' (and '.' in
  file names), so a client cannot write outside ./storage/IT23676756/<user>/ (path traversal).
- **Rejected files:** when SENDFILE is rejected (unknown target, too large, bad file name) the server
  still reads and discards the file bytes so the byte stream stays in sync with the next command.
- **Incomplete transfers:** if the sender disconnects in the middle of a file, the partial file is deleted.
- **Assumptions:** the brief does not define the presence notices or the format used to deliver a file to
  the receiver, so I defined them (see README): MSG PRESENCE JOINED/LEFT, MSG FILE PRIV, MSG FILE ROOM.

## Testing notes
- First tested the server with netcat (nc), then with my own client program.
- Checked partial lines, several commands in one packet, duplicate user names, unknown users and rooms,
  malformed commands, killing a client with Ctrl+C (server stayed up and cleaned up).
- Checked file integrity with sha256sum: original, stored copy and downloaded copy are identical.

## What I would improve with more time
- Do not hold the client-table lock while forwarding large files.
- Add TLS or token authentication, chat history, rate limiting.
