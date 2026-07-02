# HIP/1 — Haruki Ingest Protocol v1

HIP/1 is a length-prefixed binary protocol used by the Haruki Sekai Asset
Updater (client) to submit deduplicated bundles to the pjsk.moe gateway
(server). It exists because HTTP/JSON is a poor fit for the workload:

- long-lived session across many uploads
- multi-plexed uploads inside one TCP connection
- server-authoritative dedup decisions (`SKIP` vs `UPLOAD`) issued on-the-fly
- one session == one atomic version commit

This document defines the wire format and semantics. Client implementation
lives in `src/core/hip/`; a compatible mock server used in integration tests
lives in `tests/hip_mock.rs`.

## 1. Transport

- TCP (default) or TCP + TLS (via `hip.tls.enabled: true`). TLS uses rustls
  with either a supplied CA PEM file or the platform trust store.
- Default port `7420/tcp`. Not required — endpoint is fully configurable.
- One TCP connection == one **session** == one region's one version.

## 2. Frame format

```
+--------------------+--------+----------------------+
|  length (u32 BE)   |  type  |       payload        |
+--------------------+--------+----------------------+
      4 bytes          1 byte   (length - 1) bytes
```

- `length` counts the type byte plus payload, **not** itself.
- Default maximum frame size: `16 * 1024 * 1024` bytes. Peer's cap is
  negotiated in `HELLO` / `HELLO_ACK`; both sides use the minimum.
- All structured messages are msgpack. The single exception is
  `UPLOAD_CHUNK`, whose payload is a raw `[u32 be stream_id][bytes...]`
  binary layout.

## 3. Frame types

| Value | Name          | Direction | Description                                                                         |
|-------|---------------|-----------|-------------------------------------------------------------------------------------|
| 0x01  | `HELLO`       | C → S     | Handshake + version declaration                                                     |
| 0x02  | `HELLO_ACK`   | S → C     | Server accepts the session and returns limits                                       |
| 0x03  | `CHECK_BATCH` | C → S     | A batch of bundles to check against server state                                    |
| 0x04  | `CHECK_ACK`   | S → C     | Per-item `SKIP` / `UPLOAD` decision                                                 |
| 0x05  | `UPLOAD_BEGIN`| C → S     | Announce an incoming upload stream                                                  |
| 0x06  | `UPLOAD_CHUNK`| C → S     | One chunk of bytes for a stream (raw layout, not msgpack)                           |
| 0x07  | `UPLOAD_END`  | C → S     | End of stream, client-computed sha256                                               |
| 0x08  | `UPLOAD_ACK`  | S → C     | Server-verified sha256 result and placement                                         |
| 0x09  | `COMMIT`      | C → S     | Finalize this session's version                                                     |
| 0x0A  | `COMMIT_ACK`  | S → C     | Version id assigned and override index rebuilt                                      |
| 0x0B  | `BYE`         | C → S     | Client-initiated graceful close                                                     |
| 0x0C  | `WINDOW`      | S → C     | Dynamic `max_in_flight_uploads` update                                              |
| 0x0E  | `PING`        | ↔         | Heartbeat                                                                           |
| 0x0F  | `PONG`        | ↔         | Heartbeat response                                                                  |
| 0x1F  | `ERROR`       | S → C or C → S | Structured error, optionally fatal                                             |

Reserved for future use: `0x10`-`0x1E`.

## 4. Message schemas

All fields below are msgpack, encoded via `rmp-serde` on the client side.
Field names use `snake_case`. Optional fields are `Option<T>` in the client.

### 4.1 `HELLO` (0x01, C → S)

```
{
  "proto": "hip",
  "version": 1,
  "bearer_token": "<opaque>",
  "region": "jp",             // one of jp / en / tw / kr / cn
  "app_version": "6.6.0",
  "asset_version": "6.6.0.20",
  "asset_hash": "e1f2ec17-...",
  "run_id": "01J8...ULID",    // client-generated unique per run
  "unpacker_version": "6.0.5",
  "expected_max_frame": 16777216
}
```

The (region, app_version, asset_version, asset_hash) tuple is bound to the
entire session. All subsequent `CHECK_BATCH` / `UPLOAD_BEGIN` are recorded
against this version. Reject with `ERROR{code:"AUTH_FAILED", fatal:true}` if
bearer_token is invalid.

### 4.2 `HELLO_ACK` (0x02, S → C)

```
{
  "session_id": "<opaque>",
  "server_version": "hip-gateway/1.2.3",
  "max_frame": 16777216,
  "max_in_flight_uploads": 8,
  "sha256_required": true,
  "known_version": false      // optional: true if the server already saw
                              //   this (region, asset_version, asset_hash)
}
```

Client MUST use `min(hello.expected_max_frame, hello_ack.max_frame)` for
subsequent frames.

Client MUST NOT exceed `max_in_flight_uploads` concurrent uploads (that is,
unacknowledged `UPLOAD_BEGIN..UPLOAD_END` pairs). It MAY reduce it further
on receipt of `WINDOW`.

### 4.3 `CHECK_BATCH` (0x03, C → S)

```
{
  "batch_id": 42,
  "items": [
    {
      "path": "<bundle_path>",     // AssetBundleInfo key
      "fingerprint": "3735928559", // decimal string of Unity CRC32
      "size": 1048576,
      "provider": "jp"             // informational, mirrors region
    },
    ...
  ]
}
```

Recommended batch size ≤ 512 items. Server responds with a `CHECK_ACK`
carrying the same `batch_id`.

### 4.4 `CHECK_ACK` (0x04, S → C)

```
{
  "batch_id": 42,
  "results": [
    {"path": "...", "action": "SKIP"},
    {"path": "...", "action": "UPLOAD", "placement": "SHARED"},
    {"path": "...", "action": "UPLOAD", "placement": "OVERRIDE"}
  ]
}
```

- `SKIP`: server already has `(bundle_path, fingerprint)` (from any region).
  Client MUST NOT download or upload this bundle.
- `UPLOAD` + `SHARED`: no baseline exists for this `bundle_path` yet.
  Server plans to write it to `/shared-assets/{path}`.
- `UPLOAD` + `OVERRIDE`: a baseline with a different fingerprint already
  exists; the server will place this region's version at
  `/overrides/{server}/{path}`.

### 4.5 `UPLOAD_BEGIN` (0x05, C → S)

```
{
  "stream_id": 7,              // u32, unique within session
  "bundle_path": "music/foo",  // the bundle path this artefact belongs to
  "path": "character/xxx.png", // relative asset path (what the CDN serves)
  "fingerprint": "3735928559", // bundle_path's CRC32; used for version binding
  "size": 1048576,
  "content_type": "application/octet-stream"
}
```

Note: one bundle produces N artefacts (extracted PNGs, WAVs, JSONs, …).
Each artefact is its own upload stream. All streams from one bundle carry
the same `bundle_path` and `fingerprint`.

### 4.6 `UPLOAD_CHUNK` (0x06, C → S)

Not msgpack. Raw wire layout:

```
+-----------------+-------------------------+
| stream_id (u32) |     raw bytes...        |
+-----------------+-------------------------+
        4 bytes            remaining bytes
```

Client chooses chunk size (default 1 MiB, min 64 KiB). Chunks for the same
`stream_id` MUST arrive in file order. Chunks for different `stream_id`s
MAY be interleaved.

### 4.7 `UPLOAD_END` (0x07, C → S)

```
{
  "stream_id": 7,
  "total_bytes": 1048576,
  "sha256": "<64-hex-chars>"
}
```

Client streams the payload while computing sha256 incrementally.
`total_bytes` MUST equal the `size` from `UPLOAD_BEGIN`.

### 4.8 `UPLOAD_ACK` (0x08, S → C)

```
{
  "stream_id": 7,
  "status": "OK",              // or "SHA_MISMATCH" / "SIZE_MISMATCH" / "REJECTED"
  "placement": "SHARED",       // matches CHECK_ACK's placement
  "server_sha256": "<64-hex>", // server-computed sha256 (always echoed on OK)
  "storage_key": "/shared-assets/character/xxx.png",
  "message": null
}
```

`sha256_required=true` (see HELLO_ACK) means the server verifies sha256 by
recomputing it while receiving chunks. On mismatch it MUST return
`status=SHA_MISMATCH` and MUST NOT alter the baseline.

### 4.9 `COMMIT` (0x09, C → S)

```
{
  "bundle_count": 92124,
  "stats": {
    "skipped_by_layer1": 78901,   // client-local diff hits
    "skipped_by_check": 11834,    // server SKIP hits
    "uploaded_shared": 200,
    "uploaded_override": 45
  }
}
```

Version binding is already implicit — `HELLO` established it. `COMMIT`
contains only the outcome statistics.

Server actions on `COMMIT`:

1. Persist `(server, asset_version, asset_hash)` to the versions table with
   all uploaded artefacts of this session mapped to this version.
2. Rebuild in-memory override index so subsequent read traffic sees new
   placements.
3. Return `COMMIT_ACK`.

### 4.10 `COMMIT_ACK` (0x0A, S → C)

```
{
  "version_id": 1234,
  "override_index_rebuilt": true
}
```

### 4.11 `BYE` (0x0B, C → S)

Empty payload. Client-initiated graceful close after `COMMIT_ACK`. Server
should finish any pending writes then close the TCP connection.

### 4.12 `WINDOW` (0x0C, S → C)

```
{
  "max_in_flight_uploads": 4
}
```

Server can shrink the client's in-flight cap dynamically. Client MUST
respect it starting from the next new `UPLOAD_BEGIN`; existing streams may
complete.

### 4.13 `PING` (0x0E) / `PONG` (0x0F)

Both are empty payload. Any peer may send `PING` at any time. Recipient
MUST reply with `PONG` immediately.

Reference implementation heartbeat: the writer side sends `PING` every 30 s
of write idleness. A session with no `PONG` for 60 s is treated as broken.

### 4.14 `ERROR` (0x1F)

```
{
  "code": "AUTH_FAILED" | "PROTO_VIOLATION" | "SHA_MISMATCH" | "STORAGE_FULL" | "INTERNAL",
  "message": "<human-readable>",
  "fatal": true
}
```

`fatal=true` means the peer MUST close the connection after receiving it.
`fatal=false` is an out-of-band diagnostic; the session may continue.

## 5. State machine (server-side view)

```
CLOSED
  │
  ├── recv HELLO (auth ok) ──▶  HANDSHAKED
  │
HANDSHAKED
  │
  ├── send HELLO_ACK
  │
  ├── recv CHECK_BATCH ──▶ RUNNING
  ├── recv UPLOAD_BEGIN ─▶ RUNNING
  │
RUNNING
  │  (loop)
  │   ├─ CHECK_BATCH  → CHECK_ACK
  │   ├─ UPLOAD_BEGIN → UPLOAD_CHUNK* → UPLOAD_END → UPLOAD_ACK
  │
  ├── recv COMMIT ──▶ COMMITTING
COMMITTING
  ├── send COMMIT_ACK ──▶ FINALIZED
FINALIZED
  ├── recv BYE / TCP FIN ──▶ CLOSED
```

At any state, `ERROR{fatal:true}` in either direction leads to CLOSED.

## 6. Concurrency & pipelining

- Client MAY interleave `CHECK_BATCH` and `UPLOAD_BEGIN`. There is no
  required "check-all-then-upload-all" phase ordering.
- Client's outstanding uploads (from `UPLOAD_BEGIN` sent to `UPLOAD_ACK`
  received) MUST be ≤ current window (initially `HELLO_ACK.max_in_flight_uploads`,
  optionally reduced by `WINDOW`).
- Client SHOULD serialize its writer side (single writer, ordered frame
  emission). The reference Rust client does this with an mpsc-fed writer
  task.

## 7. Failure semantics

- **Session is all-or-nothing.** Any disconnect before `COMMIT_ACK` means
  the whole `run_id`'s state is discarded server-side. The client should
  retry from `HELLO` with a fresh `run_id` at its next scheduling window.
- **Duplicate `run_id` from the same client** SHOULD be rejected with
  `ERROR{code:"PROTO_VIOLATION"}`.
- **Partial upload** (client abandons mid-stream): server discards the
  temporary bytes and does not touch the baseline / assets table.

## 8. Client-side dedup layers

For context: the client applies three layers of pruning before/during a
HIP session to minimise redundant work:

1. **Layer 0 — watermark**: skip the whole poll tick if
   `current_version.json` matches the last committed watermark for this
   region. No network to the gateway at all.
2. **Layer 1 — local AssetBundleInfo diff**: compare the freshly-fetched
   AssetBundleInfo to the msgpack+zstd snapshot from the last successful
   commit. Only bundles that were added or whose fingerprint changed enter
   the HIP session at all.
3. **Layer 2 — HIP CHECK**: send `CHECK_BATCH` to the gateway for the
   surviving set. Server-side hits (from any region) return `SKIP`.

## 9. Interoperability notes

- Fingerprint dimension is unified to Unity CRC32 (decimal string). Both
  ColorfulPalette (JP/EN) and Nuverse (TW/KR/CN) `AbCacheEntry` structures
  carry `crc`.
- `bundle_path` is the raw key from AssetBundleInfo, not any post-export
  relative path.
- `asset_path` inside `UPLOAD_BEGIN.path` is the CDN-facing path — what
  `pjsk.moe/{server}/{path}` reverse-proxies to.
