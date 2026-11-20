# EmberDB — M6: complete binary protocol implementation

A learning-stage, Redis-like in-memory TCP key/value store implemented in Rust.
This is **the complete file set** needed for M6 (not just a patch). It uses
length-prefixed binary framing, a typed request/response codec, a shared
`RwLock<HashMap<...>>` store, a thread-per-connection server, a text CLI, and
HTTP-*equivalent* numeric error codes on the binary wire.

## Requirements

- Stable Rust with edition 2024 support (`rustc --version`, `cargo --version`)
- Cargo access to the `dotenvy` crate on first build
- Linux/macOS/Windows with standard Rust networking support

## Install into an existing EmberDB Cargo repository

The archive contains **files only**, without a `.git/` directory. Back up or
commit any local changes before extracting because source files and
`Cargo.toml` will be replaced. Extract from the root of your existing repo:

```bash
cd ~/dev/emberdb
# (after putting the archive in ~/Downloads/)
tar -xzf ~/Downloads/emberdb-m6-complete.tar.gz -C .
```

You can also create a fresh directory and extract into it, then run Cargo.
The optional `.env` file already contains the local development address.

## Build, test, run

```bash
cargo fmt --all
cargo check --all-targets
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

Start a server in one terminal:

```bash
cargo run --bin ember-server
```

Connect from another terminal:

```bash
cargo run --bin ember-cli
```

Try:

```text
ember> PING
PONG
ember> SET name rudra
OK
ember> GET name
VALUE rudra
ember> SET greeting hello world
OK
ember> GET greeting
VALUE hello world
ember> EXISTS name
INTEGER 1
ember> DEL name
INTEGER 1
ember> GET name
NIL
ember> SET name
ERR 400 Bad Request: invalid number of arguments
ember> UNKNOWN
ERR 501 Not Implemented: unknown command
ember> QUIT
```

The CLI text parser accepts a one-word key and joins multiple value words
with spaces. Binary arguments must be sent through the codec itself (the TCP
integration test demonstrates this). QUIT is a CLI-local instruction.

## Configuration

The server and CLI currently use the same address for local development:

```dotenv
EMBERDB_ADDR=127.0.0.1:6380
```

Configuration precedence is process environment > `.env` > built-in default.
You can override it with `EMBERDB_ADDR=127.0.0.1:6390 cargo run --bin ember-server`.
Do **not** configure `0.0.0.0` as the CLI destination: a bind address and a
client destination will be split in a later milestone.

`.env` is ignored by Git. `.env.example` is intended to be committed.
`Cargo.lock` is generated on first Cargo build (and should be committed for
this executable project).

## Protocol v2

Frame envelope (both directions):

```text
u32 payload_length (big-endian), followed by payload_length bytes
maximum payload_length = 1,048,576 (1 MiB)
```

Request payload: `[version: u8 = 2][opcode: u8][arguments]`.
Blob arguments: `[byte_count: u32 BE][raw bytes]`.

| Opcode | Command | Arguments |
|---:|---|---|
| 0x01 | PING | none |
| 0x02 | SET | key blob, value blob |
| 0x03 | GET | key blob |
| 0x04 | DEL | key blob |
| 0x05 | EXISTS | key blob |

Response payload: `[version: u8 = 2][tag: u8][data]`.

| Tag | Response | Data |
|---:|---|---|
| 0x01 | PONG | none |
| 0x02 | OK | none |
| 0x03 | VALUE | value blob |
| 0x04 | NIL | none |
| 0x05 | INTEGER | u64 BE |
| 0xFF | ERROR | status code: u16 BE |

Error codes are **inspired by HTTP, but the protocol is not HTTP**:

| Decimal | Meaning | When generated |
|---:|---|---|
| 400 | Bad Request | Malformed request payload or CLI argument count |
| 413 | Content Too Large | Payload exceeds the 1 MiB limit |
| 500 | Internal Server Error | Storage lock operation failed |
| 501 | Not Implemented | Unknown command opcode or CLI command |
| 505 | Version Not Supported | Unsupported protocol payload version |

An error response with status 400 has payload bytes `02 FF 01 90`, and
its complete TCP frame is `00 00 00 04 02 FF 01 90`. A frame with an
oversized length header receives 413 and then the server closes the socket;
the oversized body is **not** consumed. A malformed command *within* a valid
frame is answered with an error while the connection remains open.

Missing keys return `NIL` for GET, `INTEGER 0` for DEL and EXISTS; they are
not represented by HTTP 404. Success is conveyed through typed response tags
rather than an HTTP 200 code.

## Directory layout

```text
Cargo.toml
.env
.env.example
.gitignore
README.md
src/
  lib.rs
  protocol.rs
  codec.rs
  store.rs
  command.rs
  config.rs
  bin/
    ember-server.rs
    ember-cli.rs
tests/
  tcp_integration.rs
```

## Test coverage

- Binary and multiple TCP frames, exact wire bytes, clean EOF and truncation
- Oversized incoming/outgoing frames
- Binary keys/values, overwrite and concurrent writes
- Codec round trips, malformed payloads, error codes and version checking
- Typed command execution, CLI parsing
- Two independent clients sharing binary values over real TCP connections
- 400/501/505 server errors and 413 oversized-frame disconnect

## Scope limitations

This is a deliberately local-only learning implementation. It does **not**
have persistence, TTL, authentication, TLS, connection limits, read deadlines,
rate limiting, or a graceful coordinated shutdown. Thread-per-connection is
not yet bounded and an idle client can occupy a thread. Keep the server on
`127.0.0.1` and do not expose it to untrusted networks.
