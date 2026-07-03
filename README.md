> [!Caution]
> This project was rewritten in Rust.  
> Go edition is not maintained anymore.   
> If you want to use Go edition, please go to [old go branch](https://github.com/Team-Haruki/Haruki-Sekai-Asset-Updater/tree/old-go).

# Haruki Sekai Asset Updater
**Haruki Sekai Asset Updater** is a companion project for [HarukiBot](https://github.com/Team-Haruki), it's a high performance game asset extractor and exporter of the game `Project Sekai`.

## Scope

- Loads v3 YAML config
- Exposes `GET /healthz`
- Exposes `POST /v2/assets/update`
- Exposes `GET /v2/jobs/{id}`
- Exposes `POST /v2/jobs/{id}/cancel`
- Uses [`cridecoder`](https://crates.io/crates/cridecoder) as the codec backend
- Supports bundle download, deobfuscation, export post-processing, S3-compatible upload, and Git CLI chart sync
- Uses the Rust image backend for PNG/JPG/WebP output from AssetStudio RGBA payloads
- Uses the double-FFI production path by default: AssetStudio FFI worker
  pool plus FFmpeg/rsmpeg FFI. FFmpeg CLI remains available as a media fallback
  for platforms where FFI is unavailable.
- The native AssetStudioFFI library is built from
  [`Team-Haruki/AssetStudio`](https://github.com/Team-Haruki/AssetStudio)'s
  `sekai-modified` branch, which is the fork's default branch.

## Layout

- `src/`: application code
- `crates/assetstudio-ffi/`: AssetStudio FFI ABI and worker binary
- `tests/`: integration tests
- `docs/migration/v2-api.md`: current HTTP API notes

## Secret Config

- Sensitive config fields support `${env:VAR_NAME}` references instead of checked-in plaintext.
- The main service only accepts the current v3 config shape. Use
  `haruki-asset-configs.example.yaml` as the current config template.
- The loader resolves this syntax for:
  `server.auth.bearer_token`,
  `backends.asset_studio.library_path`,
  `backends.asset_studio.worker_path`,
  `storage.providers[].access_key`,
  `storage.providers[].secret_key`,
  `git_sync.chart_hashes.password`,
  `regions.*.crypto.aes_key_hex`,
  `regions.*.crypto.aes_iv_hex`.
- Tracked config templates expect values such as:
  `HARUKI_MEDIA_BACKEND`,
  `HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH`,
  `HARUKI_ASSET_STUDIO_FFI_WORKER_PATH`,
  `HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY`,
  `HARUKI_ASSET_STUDIO_FFI_WORKER_MAX_CALLS`,
  `HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE`,
  `HARUKI_ASSET_STUDIO_FFI_IMAGE_FORMAT`,
  `HARUKI_ASSET_STUDIO_FFI_WORKER_IDLE_TIMEOUT_SECONDS`,
  `HARUKI_ASSET_STUDIO_FFI_WORKER_GC_HEAP_HARD_LIMIT_MB`,
  `HARUKI_ASSET_STUDIO_FFI_WORKER_GC_CONSERVE_MEMORY`,
  `HARUKI_ASSET_STUDIO_FFI_IMAGE_FLUSH_BYTES`,
  `HARUKI_ASSET_HTTP_VERSION`,
  `HARUKI_CPU_BUDGET_AUTO`,
  `HARUKI_CPU_BUDGET_RATIO`,
  `HARUKI_CPU_RESERVED`,
  `HARUKI_SHARED_AES_KEY_HEX`,
  `HARUKI_SHARED_AES_IV_HEX`,
  `HARUKI_EN_AES_KEY_HEX`,
  `HARUKI_EN_AES_IV_HEX`.

## Run locally

1. Copy the example config:

```bash
cp haruki-asset-configs.example.yaml haruki-asset-configs.yaml
```

2. Fill the environment values used by your local config:

```bash
cp .env.example .env
export HARUKI_MEDIA_BACKEND=ffi
export HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH=/path/to/HarukiAssetStudioFFI.so
export HARUKI_ASSET_STUDIO_FFI_WORKER_PATH=/path/to/assetstudio_ffi_worker
export HARUKI_SHARED_AES_KEY_HEX=...
export HARUKI_SHARED_AES_IV_HEX=...
export HARUKI_EN_AES_KEY_HEX=...
export HARUKI_EN_AES_IV_HEX=...
```

3. Start the service:

```bash
cargo run --features media-ffi
```

Or run it with Docker Compose:

```bash
docker compose up --build
```

4. Check health:

```bash
curl http://127.0.0.1:8080/healthz
```

5. Submit a dry-run job:

```bash
curl -X POST http://127.0.0.1:8080/v2/assets/update \
  -H 'Content-Type: application/json' \
  -H 'User-Agent: HarukiInternal/1.0' \
  -H 'Authorization: Bearer change-me' \
  -d '{"region":"jp","asset_version":"6.0.0","asset_hash":"deadbeef","dry_run":true}'
```

### AssetStudioFFI Runtime

The Rust service talks to AssetStudio through `assetstudio_ffi_worker`, while
the native `HarukiAssetStudioFFI` dynamic library comes from the
[`Team-Haruki/AssetStudio`](https://github.com/Team-Haruki/AssetStudio)
`sekai-modified` branch. Release and Docker builds use that branch by default.

Platform release archives include the matching AssetStudioFFI files under
`assetstudio/`. For local development with a release archive, point the service
at that bundled library and the worker binary:

```bash
export HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH=./assetstudio/HarukiAssetStudioFFI.so
export HARUKI_ASSET_STUDIO_FFI_WORKER_PATH=./assetstudio_ffi_worker
```

Use the platform-specific library extension for your host: `.so` on Linux,
`.dylib` on macOS, and `.dll` on Windows; Windows releases use
`./assetstudio/HarukiAssetStudioFFI.dll` and `./assetstudio_ffi_worker.exe`.
You can also download the standalone AssetStudioFFI archive or build the
`AssetStudioFFI` project yourself, then set the same variables to those paths.

## Runtime Tuning

- AssetStudio exports use the `assetstudio_ffi_worker` pool. Set
  `HARUKI_ASSET_STUDIO_FFI_LIBRARY_PATH` and, when the worker cannot be inferred
  from the service binary directory, `HARUKI_ASSET_STUDIO_FFI_WORKER_PATH`.
  `HARUKI_ASSET_STUDIO_FFI_PROCESS_CONCURRENCY`,
  `HARUKI_ASSET_STUDIO_FFI_WORKER_MAX_CALLS`, and
  `HARUKI_ASSET_STUDIO_FFI_READ_BATCH_SIZE` tune worker pool throughput.
- `backends.asset_studio.mode` defaults to `worker_pool` for production crash
  isolation. Set it to `direct`, or set `HARUKI_ASSET_STUDIO_FFI_MODE=direct`,
  only for local throughput benchmarks where the service process may load and
  call `HarukiAssetStudioFFI` directly.
- `concurrency.*` only bounds how many *bundles*/*encode workers* run at once —
  it does **not** bound the memory each bundle's AssetStudio FFI export uses.
  The two knobs that actually matter for total memory footprint are:
  - `backends.asset_studio.process_concurrency`: each unit is a standalone
    .NET NativeAOT worker process. `0` (auto) defaults to the CPU budget, so
    on a many-core, memory-constrained host it can spawn far more worker
    processes than you expect. Set it explicitly (e.g. `2`-`4`) instead of
    leaving it at auto when memory is tight.
  - `backends.asset_studio.image_flush_bytes`: bounds how much
    decoded-but-not-yet-encoded raw RGBA texture data a single bundle can
    buffer before it's flushed to disk. Without this, a bundle with many/large
    textures buffers *all* of them uncompressed (a 4096x4096 texture alone is
    64 MiB) regardless of `concurrency.images`. Defaults to 128 MiB; `0`
    restores the old "flush once, at the end of the bundle" behaviour.
  Additional per-worker knobs bound each spawned .NET process directly:
  `backends.asset_studio.worker_idle_timeout_seconds` (default `120`; kills
  pooled workers that have sat idle that long, so a traffic burst doesn't
  leave `process_concurrency` .NET processes permanently resident) and
  `backends.asset_studio.worker_gc_heap_hard_limit_mb` /
  `worker_gc_conserve_memory` (opt-in `DOTNET_GCHeapHardLimit` /
  `DOTNET_GCConserveMemory` caps — useful because each worker's GC otherwise
  sizes itself against the *whole* container's memory, an assumption that
  breaks down once more than one worker runs concurrently).
- `resources.memory.max_in_flight_bundle_bytes` is a soft memory guard. The default
  `0` disables it. On small Linux hosts, set it to the amount of bundle work the
  process may keep in memory, for example
  `HARUKI_MAX_IN_FLIGHT_BUNDLE_BYTES=4294967296`.
- `resources.cpu.budget_auto` and `resources.cpu.budget_ratio` size the
  CPU-heavy worker pools. The default uses the available CPU budget for
  full-throughput export runs; lower it on shared or memory-constrained hosts.
- `resources.cpu.throttle.enabled` is optional and defaults to `false`. Enable
  it only when the process should actively wait based on sampled process-tree
  CPU usage; leave it disabled for full-throughput export runs.
- `backends.image` controls Rust-side image encoding. Keep
  `png_compression: fast` for high-throughput exports unless smaller PNG output
  is more important than CPU time.
- `concurrency.post_process` limits bundle post-processing. Keep it near the
  CPU budget for production full exports, and raise `concurrency.images` for
  image-heavy paths such as `character/member`.
- `concurrency.media_encode` is the legacy aggregate FFmpeg/rsmpeg cap, while
  `concurrency.audio_encode` and `concurrency.video_encode` split audio and
  video encode pressure. Keep video encoding lower on memory-constrained hosts
  because x264 keeps per-encoder frame queues; audio encoding can usually run
  much wider.
- Normal progress logging emits bundle-level start/completion/failure lines.
  Use debug logging for detailed download, native FFI, export, and post-process
  phase traces.

## Benchmark Snapshot

The following local comparison was run on an Apple Mac mini M4 with OrbStack
Linux arm64 containers, using cached CN bundles where noted. It compares the
current Rust FFI pipeline against the old Rust v5.2.2 AssetStudio CLI pipeline
and the retired Go CLI pipeline.

| Rule | Current Rust FFI | Rust v5.2.2 CLI | Old Go CLI |
| --- | ---: | ---: | ---: |
| `^character/member/` images | `71.5s` with local bundle HTTP, `1250/1250` | `272.9s`, `1250/1250` | `298.3s`, `1250/1250` |
| `^music/short` audio MP3 | `57.4s`, `1547/1547` | `113.0s`, `1547/1547` | `120.3s`, `1547/1547` |
| `^movie/gacha` video MP4 | `370.2s`, `448/448` | `401.8s`, `445/448` | `415.0s`, `448/448` |

Notes:

- The image run is the most CPU-bound comparison for the current pipeline; the
  same current image rule through the normal CDN path took `108.6s`.
- The v5.2.2 video result shown uses the best stable direct-FFmpeg rerun
  (`download: 8`, `usm: 4`, `cridecoder@0.2.3`). The original v5 video path only
  completed `154/448` because many USM files failed extraction.
- Output file counts are not fully path-contract comparable across versions:
  current Rust writes final semantic outputs, while older CLI pipelines may keep
  extra exported or intermediate files.

## Verification

- Run the Rust test suite with `cargo test --workspace`.
- Real codec sample baselines are opt-in. Put `0703.usm` and
  `se_0126_01.acb` in an external directory and run with
  `HARUKI_CODEC_SAMPLE_DIR=/path/to/codec-samples`; otherwise those sample
  checks skip while the rest of the suite still runs.
