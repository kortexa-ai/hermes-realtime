# Changelog

All notable changes to **hermes-realtime** are documented here, in the format
of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.1] — 2026-10-29

### Fixed

- Allow Hermes to select compatible NumPy, Pillow and aiohttp versions in its
  shared Python environment. Exact plugin pins could conflict with an updated
  host and cause the updater to disable both voice transports. Distribution
  and native plugin requirements now use matching bounded ranges; transport
  SDK pins stay fixed.
- Check shared requirements against the actual Hermes host dependency pins
  in both vanilla and Kortexa test lanes.
- Pass the selected interpreter to the current Hermes PM workspace builder
  when preparing the isolated test environment.

## [0.5.0] — 2026-10-22

### Added

- Added authenticated `GET /v1/realtime/discovery` metadata so trusted routers
  can map a Hermes profile name to the fixed-profile Direct Realtime listener
  without loading or switching profiles inside hermes-realtime.

### Changed

- Renamed the repository and distribution from `hermes-livekit` to
  `hermes-realtime`, the Python package/native install path from
  `hermes_livekit` to `hermes_realtime`, and the Hermes plugin/entry point
  from `livekit` to `realtime`.
- Renamed explicit plugin toolsets to `hermes-realtime` and
  `hermes-realtime-tools`; updated installation, CI, imports and documentation.
- Existing installs must remove the old plugin, migrate its settings and
  reinstall under the new name. See the README upgrade and rollback steps.
  LiveKit platform settings, transport protocols and SDK dependencies retain
  their existing names and behavior.
- Silero VAD is the default speech detector on both transports. The pinned
  v6.2 model and its license ship inside the package, and `onnxruntime` and
  `numpy` are base dependencies; the `vad` extra is gone. The energy gate
  stays available as `vad_backend: rms`. In a noisy room the energy gate
  turned bursts into hallucinated "Okay." turns.
- The Direct Realtime adapter drops the gateway's first-turn `/sethome`
  onboarding notice instead of showing it as assistant text on every new call.
  A call's chat id is ephemeral, so a Realtime home channel could never receive
  deliveries; no home channel is forced.
- Replaced the legacy Hermes data topics with the shared Conference contract.
  Conversation events use `conference.events`; participant tool catalogs and
  acknowledgements use `conference.tools`; optional camera/runtime controls
  use `conference.extensions`. Hermes native RPC and bounded byte streams
  remain negotiated transport extensions.
- Replaced custom remote-tool call/result correlation over JSON data messages
  with LiveKit native RPC. Clients advertise the same
  `conference.tools.register` catalog used by `api.server`, then serve an RPC
  method with the same name. LiveKit owns correlation, response timeout, and
  error transport.
- Updated the pinned SDKs to `livekit==1.1.14`, `livekit-api==1.2.0`, and
  `Pillow==12.3.0`; raised the minimum Hermes package to `0.20.0`.
- Moved platform dependency and credential checks onto Hermes' current passive
  `check_fn` / `validate_config` contract and added direct YAML-key bridging.
- Removed private LiveKit FFI teardown now that the SDK owns remote-disconnect
  queue cleanup.

### Fixed

- Limited disposable test-workspace resolution to the host OS and architecture,
  retaining Hermes' Python constraints. CI no longer tries to resolve Android
  dependencies that have no compatible native wheels.
- Updated the packaging contract test to use Hermes' current
  `installs_packages` property after upstream removed `is_member`.
- Rebuilt local and CI validation around vanilla Hermes while retaining an
  explicit, origin- and branch-verified Kortexa pipeline lane. Plugin import no
  longer eagerly loads gateway adapters during Hermes config initialization,
  and native streaming tolerates the older vanilla audible-state behavior.
- Hermes `/stop` and running `/new` interruptions now cancel LiveKit native
  RPC calls scheduled on the gateway loop instead of leaving them alive until
  the SDK response timeout.
- Kept Direct Realtime capture suppressed for a short playout guard after TTS
  leaves the server RTP queue, preventing the remote speaker/jitter-buffer tail
  from being transcribed as a second user turn.

## [0.4.4] — 2026-10-22

### Fixed

- Make the voice-observer integration fixture install a native plugin in its
  temporary Hermes home. Clean CI checkouts now test actual plugin discovery
  and async hook delivery without relying on leftover pip entry-point metadata.
  Runtime behavior is unchanged from 0.4.3.

## [0.4.3] — 2026-10-22

### Fixed

- Read connection settings from the owning Hermes profile when several
  profiles share one gateway. LiveKit rooms, direct Realtime listeners,
  and credential checks now use Hermes' scoped configuration reader.
- Defer media SDK imports until a configured transport is probed or
  created, keeping plugin registration within Hermes' startup deadline.

## [0.4.2] — 2026-10-22

### Fixed

- Aligned both distribution and native-plugin dependencies with Hermes'
  `aiohttp==3.14.5` pin. The previous `3.14.3` pin made the shared environment
  unsatisfiable and caused Hermes updates to disable LiveKit. Reinstall this
  version with `--enable --yes-deps` for each affected profile, then restart
  the shared gateway.

## [0.4.0] — 2026-07-28

### Added

- **`client:control` action `end-of-turn`.** Closes the sending participant's
  utterance and dispatches it immediately, for clients that endpoint locally.
  Without it the adapter cannot tell you have stopped until it has seen
  `SILENCE_THRESHOLD_SECONDS` of silence, and that delay is paid on every
  reply. Scoped to the sender, unlike the global `pause`/`resume`.

### Fixed

- **Leaked UDP sockets on every room teardown, ending in
  `[Errno 24] Too many open files`.** A connected room holds ~15 sockets from
  ICE gathering, and the gateway had accumulated 209 — about 14 rooms alive at
  once in a process that should hold one. It surfaced far from the cause: STT
  could not write its temp WAV and the kanban dispatcher failed its tick.

  `Room.disconnect()` returns early when the room is already disconnected,
  skipping both `await self._task` and the FFI queue unsubscribe. That task is
  a bound coroutine, so while it lives the event loop keeps the Room alive, its
  handle is never dropped, and the sockets stay open for the life of the
  process. Rooms end that way routinely — the server closes them, the last
  participant leaves, a reconnect replaces them. Teardown now cancels the task,
  unsubscribes the queue and disposes the handle itself. `_join_room()` also
  assigned a fresh `rtc.Room()` unconditionally, orphaning any existing one;
  it now releases first and warns, since reaching that branch means a race.

  All private-attribute access is best-effort, so an SDK shape change degrades
  to the previous behaviour rather than breaking teardown.

- **Silent presence failures.** A failing remote-participant count logged at
  `DEBUG`, so a room that never saw its participants looked idle. It now warns,
  throttled to once per 5 minutes.

## [0.3.2] — 2026-07-27

### Fixed

Two adapter overrides had drifted from the `BasePlatformAdapter` contract,
each breaking a whole path with a `TypeError`.

- **Every voice reply failed.** The gateway's auto-TTS path calls
  `play_tts(..., caption=...)` on every platform, but the override pinned a
  fixed signature without it — so each voice turn died *after* the TTS audio
  had already been generated, and the room got "Sorry, I encountered an
  error (TypeError)" instead of the reply. `play_tts` now takes `caption`
  (accepted and ignored: LiveKit delivers reply text separately on
  `hermes-chat`, and leaving the caption unconsumed is exactly what makes
  the caller still send that text) plus `**kwargs`, matching the base
  signature, which takes `**kwargs` and can grow keywords at any time.
- **The platform stayed down permanently after any disconnect.**
  hermes-agent's reconnection watcher calls `connect(is_reconnect=True)`;
  without the kwarg every reconnect attempt raised a `TypeError`.

## [0.3.1] — 2026-05-17

### Fixed

- **Silence-only participants could trigger STT on silence.**
  `_on_track_subscribed` seeded `_last_audio_time` on subscribe, which
  defeated the `_check_silence_loop` guard that treats a missing entry
  as "this participant has never spoken" (and discards accumulated
  noise instead of transcribing it). The timestamp is now set only on
  the first chunk above the RMS floor.
- **Last words could be dropped when a track ended abruptly.** The
  track-end utterance flush in `_cleanup_participant` subtracted a
  fixed silence window (`len(buf) - silence_bytes`) before transcribing,
  which chopped real speech — or zeroed the flush entirely — when a
  track ended right after a word with no trailing silence buffered. The
  flush now transcribes the whole buffer; trailing silence handed to
  STT is harmless, lost words are not.
- Corrected a stale docstring on `prepare_tts_text` — upstream
  `BasePlatformAdapter` now calls the hook (landed via
  NousResearch/hermes-agent#27308), so the override is live, not a
  no-op.

Both adapter fixes address review feedback from the now-closed core
PR NousResearch/hermes-agent#3894.

## [0.3.0] — 2026-05-16

### Added

- **Remote tools over the data channel.** Connected clients can register
  tools that the hermes agent's LLM can invoke. The agent calls them
  via a targeted `agent:tool-call` JSON message; the client runs the
  tool locally (whatever that means — pop a notification, drive a
  browser tab, move a robot arm) and replies with `client:tool-result`.
  Three new inbound types on the `hermes-control` topic
  (`client:tool-register`, `client:tool-unregister`, `client:tool-result`)
  and matching outbound `agent:tool-*` events. Full design in
  `docs/remote-tools-design.md`.
- Tools register into the hermes registry under the
  `hermes-realtime-tools` toolset. **Operators must add
  `hermes-realtime-tools` to the active toolsets in `~/.hermes/config.yaml`
  (`platform_toolsets.livekit`)** for the LLM to see client-registered
  tools. The plugin does not auto-activate.
- `HERMES_LIVEKIT_TOOL_TIMEOUT_SEC` env override (default 30s) for the
  per-call timeout when waiting for a `client:tool-result`.
- Cleanup: tools and pending calls owned by a disconnecting client are
  deregistered automatically; full adapter teardown wipes all tool state;
  agent-loop cancellation propagates `agent:tool-call-cancelled` to the
  owner.

### Scope notes (v1 design choices)

- Single-client assumption — collisions between distinct clients
  registering the same tool name are undefined.
- Trust-on-connect — any participant in the room can register any tool.
  No allowlist; the LiveKit room's existing auth is the consent surface.
- Large / binary tool results (e.g. camera snapshots) are Phase 1.5 work
  via LiveKit byte streams; v0.3.0 supports small JSON-shaped results
  only.

## [0.2.1] — 2026-05-15

### Fixed

- **Adapter operational logs now actually reach `gateway.log`**. v0.2.0's
  `logger.setLevel(INFO)` was the right idea but the wrong fix: hermes
  core installs a component filter on the gateway log handler that only
  admits records from loggers whose name starts with one of the registered
  component prefixes (`gateway`, `agent`, `tools`, `cli`, `cron`). Our
  `hermes_realtime.adapter` logger fell outside the allowlist and records
  were dropped at the handler stage regardless of level. The logger is
  now created with the explicit name `gateway.platforms.livekit`,
  matching the convention every built-in platform adapter uses (and
  what the kortexa branch's core-resident version uses). Output is
  byte-identical whether the platform lives in core or as this plugin.
- `HERMES_LIVEKIT_LOG_LEVEL` semantics adjusted accordingly: unset →
  inherit from hermes's root logger config (INFO under the standard
  gateway setup); a value override-sets the level only when present.

## [0.2.0] — 2026-05-15

### Added

- **Inbound data-channel protocol** (`hermes-control` topic, JSON
  `{"type": "client:<...>"}`). Single dispatcher routes by `type`; unknown
  types are ignored silently to stay friendly with apps that share the data
  channel for other traffic.
- **`client:capture-frame`** — sample the very next video frame from the
  sender's published video track, JPEG-encode (Pillow, quality 85), queue
  for attachment to the next `MessageEvent` (voice or text). The hermes
  vision pipeline picks up the file via `media_urls`. Frames not claimed by
  any message are cleaned up on disconnect.
- **`client:message`** — typed text from the client (skips STT). Pending
  captures attach automatically, same path as voice. Useful for IM-style
  fallback or accessibility.
- **`client:control`** — runtime hooks. `action: "pause"` / `"resume"`
  for explicit "mute me" UX. Placeholder for future controls.
- **Outbound `agent:frame-captured`** / **`agent:frame-capture-failed`** —
  acknowledge captures to clients so UIs can flash a "snapshot taken"
  indicator. Failure reasons: `no-video-track`, `timeout`, `encode-error`,
  `pillow-not-installed`.
- **Video track subscription** — `_on_track_subscribed` now branches on
  `KIND_VIDEO` and stores `rtc.VideoStream(track)`. Streams are NOT iterated
  eagerly; frames are only pulled on `client:capture-frame` trigger
  (sampling-on-demand, no continuous decoding cost).
- **`HERMES_LIVEKIT_LOG_LEVEL`** env var (`DEBUG` / `INFO` / `WARNING` / `ERROR`
  or numeric, default `INFO`). Lets users dial adapter verbosity without
  touching code. Mirrors hermes-agent's logging conventions so operational
  logs land in `gateway.log`.
- **Pillow** dependency, pinned to `==12.2.0` (2026-04-01 release, no yanks).
  Used to encode sampled frames.
- **LICENSE** (MIT), **CONTRIBUTING.md**, **CHANGELOG.md** (this file).
- README: full inbound/outbound protocol tables + sampling semantics.

### Changed

- Plugin default logger level is now `INFO` (was implicitly `WARNING` —
  Python's default for unconfigured loggers). All existing operational log
  lines should now be visible without further config.
- `_process_voice_input` drains pending captures into `MessageEvent`'s
  `media_urls` + `media_types`. Voice-only flow is unchanged when no
  captures are queued.

### Reserved (not yet implemented)

- `client:tool-register` / `client:tool-unregister` / `client:tool-result`
  message types — placeholder for future client-published tools (think:
  teleoperated hardware controlled by the agent over WebRTC). Don't ship
  clients that send these types; the schema may change.

## [0.1.0] — 2026-05-14

### Added

- Initial release. LiveKit WebRTC voice gateway plugin for hermes-agent.
- `LiveKitAdapter` — joins a LiveKit room as a participant, transcribes
  inbound audio via hermes's STT pipeline (qwen3-asr / OpenAI / whisper /
  faster-whisper, whichever hermes is configured for), publishes TTS replies
  back to the room.
- Presence-aware join: room empty → presence-watch poll, joins as soon as a
  real participant arrives, leaves when the last human leaves. Doesn't burn
  a participant slot on empty rooms.
- Capped exponential-backoff reconnect on disconnect.
- Outbound data-channel events (`agent:listening-start` / `-stop`,
  `agent:thinking-start`, `agent:speaking-start` / `-stop`,
  `agent:user-transcript`, `agent:agent-transcript`).
- Env-driven auto-configuration via `LIVEKIT_URL` / `LIVEKIT_API_KEY` /
  `LIVEKIT_API_SECRET` / `LIVEKIT_ROOM` / `LIVEKIT_AGENT_NAME` etc. —
  plugin auto-enables the platform when env is present.
- Hooks into hermes's `register_platform()` surface: `env_enablement_fn`,
  `is_connected`, `cron_deliver_env_var`, `allowed_users_env`,
  `allow_all_env`, `platform_hint`, `setup_fn`, plus a `hermes-realtime`
  toolset registration. Zero core hermes-agent edits required to install.
- Pinned dependencies: `livekit==1.1.7`, `livekit-api==1.1.0`.

[0.2.1]: https://github.com/kortexa-ai/hermes-realtime/releases/tag/v0.2.1
[0.2.0]: https://github.com/kortexa-ai/hermes-realtime/releases/tag/v0.2.0
[0.1.0]: https://github.com/kortexa-ai/hermes-realtime/releases/tag/v0.1.0
