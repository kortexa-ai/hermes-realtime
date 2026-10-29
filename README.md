# hermes-realtime

<p align="center">
  <img src="docs/assets/hermes-gateway.png" width="220" alt="Hermes realtime gateway mark">
</p>

Realtime voice gateway plugin for [hermes-agent](https://github.com/NousResearch/hermes-agent).

It serves an OpenAI-compatible direct WebRTC endpoint and can join a LiveKit
room as a Realtime Conference agent. Both transports transcribe speech through
Hermes, run the same agent loop, return TTS audio, and share one conversation
event contract.

For telephone integration, see the [outbound phone configuration and design
guide](docs/outbound-phone-calls.md). It covers Twilio Elastic SIP Trunking,
LiveKit Cloud setup, verification, and the proposed Hermes call controller.
Outbound dialing is not yet implemented by this plugin.

The gateway mark combines a transport arch, messenger wings, and an audio
pulse. `docs/assets/hermes-gateway-master.png` is the opaque lossless source;
`./scripts/export-readme-mark.sh` regenerates the optimized README image. The
original artwork was generated with OpenAI's built-in image generation tool on
2026-08-27 with text, third-party marks, and transparency prohibited.

## Requirements

- An existing `hermes-agent` install (this plugin attaches to it; it does not
  vendor hermes itself).
- `ffmpeg` on `PATH` — used to decode TTS audio for the WebRTC publish path.
  - macOS: `brew install ffmpeg`
  - Debian / Ubuntu: `sudo apt install ffmpeg`
- A reachable LiveKit server and API key/secret pair only when using the
  Conference transport. Direct WebRTC has no LiveKit dependency at runtime.

## Install

For current Hermes, install the native plugin through its dependency manager:

```bash
hermes plugins install kortexa-ai/hermes-realtime/hermes_realtime --enable --yes-deps
```

The subdirectory is intentional: it contains the native manifest, loader and
dependency declaration. Hermes PM resolves the pinned SDKs with the core and
other enabled plugins, then publishes a new Python environment. The plugin and
its dependencies survive `hermes update`. Python 3.14 is supported.

Install separately for each profile that uses voice:

```bash
hermes --profile mira plugins install kortexa-ai/hermes-realtime/hermes_realtime --enable --yes-deps
```

Each profile retains its own settings and plugin registration. The native
dependency project has no build backend, so PM can combine multiple profile
copies without duplicate Python package names. No core source edits are needed.
The manifest requires Hermes Agent 0.20.0 or newer and checks the runtime
version. Current source installs use `0.0.0` for Python distribution metadata;
the plugin therefore does not depend on a PyPI `hermes-agent` package.

After installation, restart the affected gateway with `hermes gateway restart`
or `hermes --profile mira gateway restart`. Use `hermes plugins list` to check
discovery. Existing pip-only installs must migrate to this native install:
packages added manually to a previous environment are not an enabled PM member.

If an update disabled `realtime` because its dependency environment no longer
builds, install version 0.5.1 or newer with `--enable --yes-deps` for each affected
profile, then restart the shared gateway. These releases let Hermes select
compatible NumPy, Pillow and aiohttp versions within bounded ranges instead of
requiring the older plugin's exact shared-library versions. Hermes records an
automatic disable and its reason under `plugins.disabled_by_update`; enabling
the compatible replacement clears that record.

### Upgrade from hermes-livekit

Version 0.5.0 renames the distribution to `hermes-realtime`, the Python package
and native install subdirectory to `hermes_realtime`, and the plugin name from
`livekit` to `realtime`. GitHub redirects the old repository URL, but an existing
installation must be replaced because its plugin name and subdirectory changed.

For each affected profile, back up its configuration. Move any
`plugins.entries.livekit` settings to `plugins.entries.realtime`, and replace
explicit `hermes-livekit` / `hermes-livekit-tools` toolset selections with
`hermes-realtime` / `hermes-realtime-tools`. Then run:

```bash
hermes plugins remove livekit
hermes plugins install kortexa-ai/hermes-realtime/hermes_realtime --enable --yes-deps
```

Repeat with `hermes --profile <name>` for each additional profile, then restart
the shared gateway once and check `hermes plugins list --plain --no-bundled`.
Remove the old plugin before enabling its replacement so both copies cannot
register the same transports. `platforms.livekit`, `platforms.realtime`,
`LIVEKIT_*`, `HERMES_LIVEKIT_*`, and `HERMES_REALTIME_*` keep their existing
names and values. LiveKit remains the Conference transport and SDK.

For legacy pip installs, uninstall `hermes-livekit` from that independently
managed environment before installing `hermes-realtime`; update imports from
`hermes_livekit` to `hermes_realtime`. There is no old-name import alias or
second plugin entry point.

To roll back, remove `realtime`, install the previous reviewed commit from
`kortexa-ai/hermes-realtime/hermes_livekit` with `--ref <old-full-commit-sha>`,
restore the saved plugin settings and toolset selections, and restart the
gateway. Old commits retain their original subdirectory.

### Legacy pip / editable install

For an independently managed Python environment, the pip entry point remains
available. Use the interpreter from that environment:

```bash
python -m pip install git+https://github.com/kortexa-ai/hermes-realtime.git
# Or, for a local checkout:
python -m pip install -e ~/src/hermes-realtime
```

Use the native install above for a PM-managed Hermes environment. Re-run the
editable install after pulling a change to `pyproject.toml`: editable installs
reflect Python source edits immediately, but package metadata such as the
version, dependencies, and plugin entry point is generated at install time.
Hermes must already be installed; the plugin never installs or upgrades its host.

## Enable

After install, add `realtime` to the enabled-plugins list:

```bash
hermes plugins enable realtime
```

(Or edit `~/.hermes/config.yaml` and add `realtime` to `plugins.enabled`.)

Then enable either or both platforms in the same config:

```yaml
platforms:
  livekit:
    enabled: true
  realtime:
    enabled: true
plugins:
  enabled:
    - realtime
```

## Configure

### Direct Realtime / WebRTC

The direct adapter implements OpenAI-style SDP signalling at
`POST /v1/realtime/calls` (with `/realtime/calls` as an alias), RTP audio, and
the `oai-events` data channel. The preferred setup request is multipart with a
required `sdp` field and optional `session` field. It returns `201
application/sdp` with the call resource in `Location`; raw `application/sdp`
offers remain accepted for older clients. Every listener requires a Bearer
token:

```yaml
platforms:
  realtime:
    enabled: true
    host: 127.0.0.1
    port: 8091
    api_key: ${HERMES_REALTIME_API_KEY}
```

The equivalent environment-only setup is:

```bash
HERMES_REALTIME_ENABLED=true
HERMES_REALTIME_HOST=127.0.0.1
HERMES_REALTIME_PORT=8091
HERMES_REALTIME_API_KEY=choose-a-long-random-token
HERMES_REALTIME_ALLOW_ALL_USERS=true
```

Send `HERMES_REALTIME_API_KEY` as a Bearer token. Startup fails closed if it is
missing. `HERMES_REALTIME_MAX_CALLS` defaults to 8 and
`HERMES_REALTIME_MAX_CALL_SECONDS` defaults to 7200. Offer upload and ICE
negotiation share a 30-second setup deadline; unsuccessful setup releases its
call slot, peer, and tools. The current direct edge
advertises host ICE candidates; deployments across NAT still need a TURN-aware
front door before they are internet-ready.

The gateway's first-turn `/sethome` onboarding notice is dropped on direct
calls. A call's chat id is ephemeral, so a Realtime home channel could never
receive cron results or cross-platform messages; no home channel is forced.

Each listener is permanently bound to the Hermes profile of its gateway
process. Trusted routers can discover that fixed binding from
`GET /v1/realtime/discovery` with the same Bearer token:

```bash
curl -H "Authorization: Bearer $HERMES_REALTIME_API_KEY" \
  http://127.0.0.1:8091/v1/realtime/discovery
```

The bounded response contains only `version`, `profile`, and the relative
`realtime_path`. It does not enumerate other profiles or expose credentials,
filesystem paths, rooms, models, or provider configuration.

### Realtime Conference / LiveKit

Set these env vars. Current Hermes also accepts the matching lowercase keys
either directly under `platforms.livekit` or inside `platforms.livekit.extra`:

```yaml
platforms:
  livekit:
    enabled: true
    url: wss://your-project.livekit.cloud
    api_key: your-project-key
    api_secret: your-project-secret
    room: hermes
```

| Var                              | Required | Notes                                                              |
|----------------------------------|----------|--------------------------------------------------------------------|
| `LIVEKIT_URL`                    | yes      | `wss://your-project.livekit.cloud` or `wss://your-self-hosted/`    |
| `LIVEKIT_API_KEY`                | yes      | from your LiveKit project / server config                          |
| `LIVEKIT_API_SECRET`             | yes      | from your LiveKit project / server config                          |
| `LIVEKIT_ROOM`                   | no       | room the agent joins; default `hermes`                             |
| `LIVEKIT_AGENT_NAME`             | no       | display name; default `Hermes` (asks the LLM if unset)             |
| `LIVEKIT_AGENT_AVATAR`           | no       | avatar URL or local image path (encoded as data URI)               |
| `LIVEKIT_HOME_CHANNEL`           | no       | cron / cross-platform delivery target; defaults to `LIVEKIT_ROOM`  |
| `LIVEKIT_ALLOWED_USERS`          | no       | comma-separated participant identities                             |
| `LIVEKIT_ALLOW_ALL_USERS`        | no       | `1`/`true` allows any participant (dev only)                       |
| `LIVEKIT_PRESENCE_POLL_INTERVAL` | no       | seconds; auto-picked (cloud 30s, local 5s)                         |
| `HERMES_LIVEKIT_TOOL_TIMEOUT_SEC` | no      | native RPC response timeout; default 30 seconds                    |
| `HERMES_LIVEKIT_REMOTE_TOOL_POLICY` | no    | bounded JSON policy; absent denies tools without a signed API admission |

Or run the interactive prompt:

```bash
hermes config
```

### Voice endpointing and room noise

Set `silence_duration` in each platform's configuration, in seconds:

```yaml
platforms:
  realtime:
    extra:
      silence_duration: 0.7
  livekit:
    extra:
      silence_duration: 0.7
```

The default remains 1.5 seconds. Values from 0.2 to 5.0 are accepted; invalid
values fail configuration instead of silently changing voice behavior. Restart
the affected profile's gateway after changing it. Hermes CLI's
`voice.silence_duration` is a separate recorder setting and does not affect
these transports. Direct WebRTC checks on received audio frames; LiveKit checks
every 200 ms, so it can add up to one polling interval.

The energy gate calibrates from the quieter half of the initial 400 ms of
actual audio, using 20 ms windows regardless of received packet size. Buffered
calibration audio is retained for speech detection. The RMS detector checks all
new audio in short windows, so speech before a quiet poll tail is not lost. It then
tracks background noise continuously during idle audio and confident quiet
pauses. It has separate speech-start and speech-stop thresholds. Noise rises
slowly and falls faster, using audio duration rather than frame count. Detected
speech and near-threshold phonemes during a turn are excluded from adaptation.
Mute/unmute recalibrates the gate against the real microphone.

This is still energy-based detection, not semantic turn detection. A loud new
noise above the speech threshold can look like speech; a shorter timeout can
split thinking pauses. Validate with the actual room and microphone before
lowering it further. Endpoint logs include observed silence, configured target,
and learned noise RMS; they do not contain microphone recordings.

Speech/noise discrimination uses the pinned Silero v6.2 model by default. The
model and its license ship inside the package (`hermes_realtime/models`) and
`onnxruntime` is a base dependency, so a plain install runs it with no extra
step. Tune or switch it under either platform's `extra` configuration, then
restart only the affected gateway:

```yaml
vad_threshold: 0.5                      # 0.2–0.9; lower is more sensitive
vad_backend: rms                        # energy gate only, for rooms without a working runtime
vad_model_path: /path/to/silero.onnx    # a different verified copy of the pinned model
```

Silero uses only CPU, one inference thread and independent state per input.
The model is shared between inputs, loaded at adapter startup and checked
against a pinned SHA-256. A missing runtime or an invalid model fails startup;
there is no automatic download, GPU fallback or silent change back to energy
VAD. The startup log names the active backend.
The existing silence timeout still applies: this is not semantic endpointing.
The timer starts at the detector's last positive decision, which can occur
after audible speech has ended. Equal timer values do not guarantee equal
endpoint latency across backends. Measure total endpoint delay and test
mid-sentence pauses before lowering the timer; a shorter timer can split turns.
The [model and its license](https://github.com/snakers4/silero-vad/tree/be95df9152c0d7618fa1edfeb296fc3dae32376f)
are MIT-licensed by the Silero Team. `tools/prepare_vad.py <destination>`
fetches a verified copy when the bundled release is bumped.

Both backends retain 500 ms of pre-roll to protect word onsets, without waiting
longer to dispatch the utterance. Continuous
capture is limited to two minutes per input. Overflow discards the whole
unsubmitted utterance and reports `input_audio_too_long`; no truncated command
is sent to ASR or Hermes. Pause for the configured silence interval, or mute
then unmute, before trying again. Other callers and ongoing replies continue.

For an offline noise-step and quiet-speech check through the actual direct
capture path, run:

```sh
python tools/vad_probe.py --model /absolute/path/to/silero-v6.2.onnx \
  --speech-fixture /absolute/path/to/mono-pcm16.wav
```

It reports
CPU timings and endpoint events without recording, playback or service calls.
Synthetic fixtures do not replace qualification with the actual room/mic.

To overlap transcription with that silence window, optionally set
`asr_prefetch_silence: 0.35` beside `silence_duration: 0.7` in either platform's
`extra` configuration. The default is `0` (disabled); enabled values must be
at least 0.1 seconds and below `silence_duration`. Restart the affected gateway.
This uses the existing Hermes STT provider, model and prompt. It is speculative
batch ASR, not a persistent streaming-ASR connection.

After a qualifying quiet pause, one early request runs in the background. The
gateway still waits for its normal endpoint before dispatching a user turn.
It reuses the early result only if speech has not resumed and the final input
differs solely by quiet PCM at the tail; otherwise it transcribes the final
audio normally. Empty or failed early results also fall back. Each input has
at most one speculative worker in flight; invalidating a candidate does not
cancel an active server request. A pause followed by more speech can therefore
cost an extra ASR request. This is an energy-based safety check, not proof that
very quiet speech was absent. Validate accuracy and load with the real mic.
Gateway logs indicate whether each final transcription reused an early result.

### Streaming speech

Direct WebRTC keeps a paced 20 ms audio stream open, including silent frames
between replies and between generated clauses. This keeps the receiver's
media clock running instead of restarting an audio burst after every pause.
Idle frames do not create response events or delay speech-queue draining;
they add a small ongoing Opus/network cost while a call is connected.

Both transports implement Hermes Agent's streaming-TTS audio sink. For voice
turns with auto-TTS enabled and a supported streaming provider, completed text
clauses can play while the rest of the reply is still being generated. The
configured provider and voice are preserved; no TTS-server or client changes
are needed. Unsupported providers retain whole-file playback.

PCM is resampled incrementally to 48 kHz mono and published in paced 20 ms
frames. Direct WebRTC queues at most 500 ms of audio ahead of the sender;
LiveKit uses its audio source's playout backpressure. A failure before playable
audio permits whole-file fallback. After playback starts, failure or cancellation
must not replay the reply from the beginning.

Streaming requires a Hermes Agent consumer that preserves adapter-owned
`handle.audible` and supports `tts.streaming.completion_timeout` (default 120
seconds, accepted range 1–600). This bounds the wait for a long reply to finish;
it does not delay the start of playback. The companion fixes are tracked in
[the streaming integration issue](https://github.com/kortexa-ai/hermes-realtime/issues/40).

Generated PCM can itself start with a long near-silent prefix. To shorten that
pause, set `platforms.realtime.extra.tts_trim_leading_silence: true` (or the
equivalent `platforms.livekit.extra` key) and restart that profile's gateway.
This boolean defaults to `false`. It removes only near-silent frames at the
start of each streamed reply, retaining 80 ms before the first sample above
PCM16 peak 16 (about -66 dBFS). It scans at most one second and buffers at most
four 20 ms frames. Internal pauses, later clauses, and retained samples are
unchanged. This is conservative amplitude trimming, not speech recognition;
disable it when exact leading timing or unusually faint output must be kept.
The provider's output contract and whole-file fallback are unchanged.

Run the transport tests with
`scripts/run_tests.sh -q tests/test_streaming_tts.py`. The mandatory first lane
uses vanilla Hermes. When `.env.development.local` configures the verified
adjacent Kortexa fork, the same command also runs the optimized pipeline cases.
Create or refresh the vanilla environment first with
`scripts/bootstrap_tests.sh`. It keeps upstream Hermes in the ignored
`.development/hermes-agent` checkout, verifies its origin, updates it to
upstream `main`, and recreates `.venv` with the plugin's complete dependencies.

Hermes releases before the session-owned toolset API proposed in
[NousResearch/hermes-agent#110515](https://github.com/NousResearch/hermes-agent/issues/110515)
have no public way to bind client-supplied Realtime tools to one conversation.
On those versions, `hermes_realtime.direct_tools` deliberately uses a compatibility
path that mutates `toolsets.TOOLSETS`, copies `toolsets._HERMES_CORE_TOOLS`,
extends `tools.tool_search._DIRECT_SURFACE_TOOLSETS`, and registers scoped names
through `tools.registry`. When `PluginContext.session_toolset` is available, the
plugin feature-detects it and uses the public, session-isolated lifecycle instead.
Issue [#77](https://github.com/kortexa-ai/hermes-realtime/issues/77) tracks removal
of the compatibility path after the upstream API is released.

To test the configured TTS service through real local RTP peers, explicitly opt
in with `HERMES_TTS_CANARY_CONFIG=/path/to/profile/config.yaml` and select
`-k smarty-opt-in -s`. The canary prints timings, not credentials, and does not
play sound on a physical device.

For an end-to-end measurement from the Pi, run the isolated voice probe:

```bash
uv run --no-project --no-build \
  --with aiortc==1.15.0 --with aiohttp==3.14.3 --with av==17.0.0 \
  python tools/voice_latency_probe.py \
  --gateway http://192.168.2.6:8092 --config-host snappy --profile mira --case greeting
```

It fetches profile credentials over passwordless SSH into memory, synthesizes
one fixed question, and sends a real voice turn through ASR, Hermes, and TTS.
Choose `--case greeting`, `fact`, `calculation`, or `first_sentence`. The session requests no tools,
but that does not disable Hermes's own tools: inspect gateway API counts before
treating any case as a single model request.
The probe rejects split or premature endpoint detections instead of reporting
latency for mixed turns. Keep failed samples when evaluating endpoint quality;
do not treat them as valid latency results.
To compare reasoning effort on the same model, add `--reasoning medium` or
`--reasoning low`. The probe sends Hermes's native session-only `/reasoning`
command in its own new call and waits for an English acknowledgement before
speaking. It never sends `--global`, changes the kiosk call, or changes the model
or service tier. Command errors, unexpected audio and missing acknowledgements
fail the probe. The default `--reasoning profile` sends no command. Add
`--show-answer` to include at most 500 characters of the fixture answer for
manual review; otherwise only its word count is reported. Counterbalance effort
order and compare several serial runs. These simple questions do not establish
general reasoning/tool quality, so do not promote a default from timing alone.
Use `--turns 4` to compare the first reply with three later replies on the same
connection (1–10 turns; default 1). The probe synthesizes the question once and
replays identical PCM without resetting the RTP clock. Multi-turn output has a
`turns` array, one shared `call_id` and `fixture_sha256`, and a `response_id` per
reply. There is a one-second settling interval between completed replies and
the next utterance; recent residual audio, early response audio, split fixtures
and multiple audio replies invalidate the measurement. The interval is outside
the measured latency. Correlate these identities with the opt-in gateway metrics
to distinguish fresh-call prompt-cache misses from warm-session model time.
Repeated requests share conversation history, so later answers can differ;
this measures the conversation path, not identical model inputs or pure prefill.
Native transcript frames add `caption_after_speech_s`, `caption_to_audio_event_s`
and `caption_to_audible_rtp_s`. These use the first nonempty incremental frame
or full-snapshot replacement for the answer's response ID. Empty frames,
final-only setup notices and other responses do not start this clock. Older
transports without these frames omit the fields. These are client receipt times,
not the provider's first-token timestamps; a negative caption-to-audio interval
means that audio started before the caption arrived.
The `first_sentence` case requests a fixed two-sentence answer starting with
“Yes.” and rejects a different final answer. Use it to measure the delay from
buffering a short opening sentence into the next one. Its instructions belong
only to the isolated test call; it does not change Mira's profile or speech
chunking settings. Timing and text preservation do not establish voice quality.
It neither captures the microphone nor plays audio. The configuration host
must have Hermes at `/Users/francip/src/hermes-agent` with its existing venv;
the speech fixture expects the configured service's 24 kHz mono PCM format.
Run probes serially and avoid concurrent service benchmarks. JSON timings
measure from the last voiced source frame to endpoint detection, transcript,
audio-start event, and first audible RTP received by the Pi. The latter includes
network and receiver buffering, but not the kiosk's physical speaker latency.
Compare several runs: model generation time can vary independently of the
silence timeout or TTS transport.

For tool continuations, use `tools/tool_latency_probe.py --input voice --case dependent`.
This sends a synthetic spoken request through ASR and the streaming-TTS voice
path. Two side-effect-free client functions return fresh values; the second
requires the first result as an integer argument. The probe rejects missing,
out-of-order or incorrect arguments, incomplete utterances and unmatched audio.
Other cases cover independent tools (`parallel`), partial failure (`mixed`),
the real server deadline (`timeout`), and cancellation (`cancel`). The default
`--input text` retains the typed-input canary and its whole-file TTS path.
Timings explicitly identify `speech_end` or `text_submitted` as their reference.
An optional `--model gpt-5.6-terra` (also Astra or Sol) sends a native
`/model ... --provider openai-codex --session` command and requires confirmation
of the exact model, provider and session-only scope before the test starts.
The default `--model profile` sends no override. Profile reasoning is unchanged.
For matched comparisons, the Python `probe` function accepts the same in-memory
48 kHz mono int16 `fixture` across calls; compare the returned SHA-256 hashes,
counterbalance model order, and verify server-side model IDs and tool rounds.
Do not infer general agent quality from these bounded tool fixtures or change
the production model based only on faster canned answers.

For local model timing logs, set
`plugins.entries.realtime.settings.voice_latency_metrics: true` in the Hermes
profile and restart its gateway. It defaults to off and uses existing plugin
hooks; it does not rewrite requests or change model configuration. Hermes
treats stream observers as stream consumers, so enabling this in a mixed-channel
profile can also enable internal model streaming for other channels. Logs remain
limited to voice turns. `voice_timing` JSON logs contain the
first observed visible text and each completed API request, token/cache counts,
and tool-call count. Correlate `(session_id, turn_id, iteration)`; separate hook
workers can log out of order. First-text time includes observer queue delay.
Provider first-chunk time is `null` when unavailable (including current Codex
Responses); neither metric isolates provider queueing, network or model prefill.
Only timings and bounded identifiers are logged; no text, audio or credentials
are retained by this observer. Deduplication holds at most 1,024 request keys.
Existing audio logs distinguish first provider PCM entering the sink from the
first published PCM frame, so resampling/onset gating is measured separately.
The `streaming TTS non-quiet PCM` log also records the first sample above absolute
PCM16 amplitude 40 in queued, post-resample/post-trim audio. Its sample offset
measures the retained quiet prefix; its queue time measures when that frame was
accepted, not when the speaker played it. Correlate by `response_id`. Observation
stops after that sample or one second of queued PCM and never changes audio.
The final log includes `onset_sample` and `onset_scanned_samples`; a missing onset
means no threshold crossing was observed within that prefix, not that the entire
reply was silent. This amplitude threshold matches the silent RTP probe but is
not speech detection, and lossy encoding can change the receiver's threshold
crossing. Do not attribute every residual millisecond to receiver buffering.

Use the same isolated dependencies with `python tools/tts_onset_probe.py` to
measure provider first bytes versus audible PCM, then compare the identical
waveform with and without onset trimming. Its sample-offset difference is a
PCM-domain saving, not an end-to-end playback measurement. It also reports
filter CPU cost on the machine running the probe and verifies that every
retained sample is unchanged.

## Verify

```bash
hermes gateway restart
hermes gateway status      # should show 🎙️ LiveKit as connected
```

For direct WebRTC, status also shows `⚡ Realtime` and the signalling endpoint
is `http://127.0.0.1:8091/v1/realtime/calls` with the defaults above.

Join the configured room from any LiveKit client (web, mobile, voice-agent
desktop). The agent watches the room when empty and joins as soon as a real
participant arrives, then transcribes incoming audio and replies via TTS.

## Data channel protocol

Reliable JSON messages on `conference.events` use the same OpenAI-compatible
session and conversation contract as `api.server` Conference calls. Audio
stays on LiveKit tracks. Every participant in the room shares one Hermes
conversation session; participant identity still scopes errors and tools.

The agent sends a targeted `session.created` snapshot when each participant
joins. Room lifecycle events are broadcast:

- `input_audio_buffer.speech_started` / `speech_stopped`
- `conversation.item.added` / `conversation.item.done`
- `conversation.item.input_audio_transcription.completed`
- `response.created` / `response.done`
- `response.output_item.added` / `response.output_item.done`
- `response.content_part.added` / `response.content_part.done`
- `response.output_audio_transcript.delta` / `.done`
- `output_audio_buffer.started` / `stopped` / `cleared`
- correlated `error` events

When Hermes text streaming is enabled, captions arrive during generation,
independently of audio. Append `delta` to the item identified by `item_id`;
if a delta event includes the optional `transcript` snapshot, replace that
item's draft instead (native text can be revised at a tool boundary). The
final `.done` transcript replaces the draft, not a second message. Neither
text update nor text completion indicates that audio playback has stopped.

The adapter capability does not override a profile's disabled text-streaming
policy. Current Hermes requires the master switch as well as platform policy:

```sh
hermes --profile mira config set streaming.enabled true
hermes --profile mira config set display.platforms.realtime.streaming true
hermes --profile mira config set display.platforms.livekit.streaming true
```

Set `display.platforms.<other-platform>.streaming` to false for clients that
should stay final-only, then restart that profile's gateway. The top-level
`streaming.enabled` is a master switch; a platform cannot enable it on its own.
`display.streaming` controls the CLI, not gateway delivery. A consumer created for interim messages alone does not
receive model text deltas. Qualify a real voice turn, not just adapter methods.

Clients send supported Realtime events on the same topic. Typed input uses a
normal user conversation item:

```json
{
  "type": "conversation.item.create",
  "item": {
    "type": "message",
    "role": "user",
    "content": [{"type": "input_text", "text": "Hello"}]
  }
}
```

`response.create` starts the pending typed turn or requests a new response
through the normal Hermes message pipeline. `response.cancel` cancels the
active Hermes room turn and clears queued output audio. Unsupported events
receive an explicit error targeted to the sending participant. The old
`hermes-chat` and raw conversation `agent:*` streams are not part of the new
contract.

Hermes adds one namespaced input-state extension because the portable OpenAI
Realtime event set has no microphone mute/unmute signal:

```json
{"type":"hermes.input_audio.state","muted":true}
```

Direct WebRTC clients send it on `oai-events`. Conference clients send the
same envelope reliably on `conference.extensions`. The server responds on the
same transport with `hermes.input_audio.state_updated`. Muting immediately
finalizes any active utterance and then ignores media frames; unmuting clears
stale audio and recalibrates the participant's adaptive noise gate. Clients
that omit this optional extension retain the configured server-side endpointing.

### Conference extensions

Portable tool discovery uses `conference.tools`, exactly as it does with
`api.server` Conference. Native RPC invocation and bounded byte streams are
negotiated LiveKit extensions. Triggered camera and runtime controls use the
separate `conference.extensions` topic.

Frame status messages use the extension topic:

- `agent:frame-captured`
- `agent:frame-capture-failed`

Portable registration acknowledgements on `conference.tools` are targeted to
the owning participant:

- `conference.tools.registered`
- `conference.tools.rejected`

Binary-result lifecycle messages are also targeted on `conference.tools`:

- `agent:tool-result-stream-ready` — the targeted binary-result receiver is
  installed; `{stream_id, topic}`
- `agent:tool-result-stream-cancel` — stop and close that targeted binary
  stream; `{stream_id, topic}`

#### Inbound extension controls

Reliable JSON payloads on `conference.extensions`:

```jsonc
// sample the next frame from this client's published video track
{"type": "conference.capture_frame"}

// runtime control hooks
{"type": "conference.control", "action": "pause"}    // stop sampling audio
{"type": "conference.control", "action": "resume"}   // resume sampling audio

// this speaker is done talking; close the utterance and dispatch it now (0.4.0+)
{"type": "conference.control", "action": "end-of-turn"}

// participant-scoped mute boundary and VAD recalibration
{"type": "hermes.input_audio.state", "muted": true}
{"type": "hermes.input_audio.state", "muted": false}
```

`end-of-turn` is for clients that endpoint locally. Without it the adapter can
only notice you stopped once it has seen `SILENCE_THRESHOLD_SECONDS` of silence,
and that wait lands on every single reply. Unlike `pause`/`resume` — which are
global — this is scoped to the sending participant, so one client ending its
turn cannot affect anyone else in the room.

Tool catalogs use the portable Conference envelope on `conference.tools`:

```jsonc
{
  "type": "conference.tools.register",
  "tools": [{
    "type": "function",
    "function": {
      "name": "desktop_notify",
      "description": "Show a desktop notification.",
      "parameters": {
        "type": "object",
        "properties": {"title": {"type": "string"}, "body": {"type": "string"}},
        "required": ["title", "body"]
      }
    }
  }]
}

// replace this participant's catalog with no tools
{"type": "conference.tools.register", "tools": []}
```

Remote tools require either a signed API admission for the live participant
or an exact participant/name entry in `HERMES_LIVEKIT_REMOTE_TOOL_POLICY`.
Tier 1 permits the exact entry. Tier 2 also requires a future Unix
`consent_expires_at`. Tier 3 is always denied:

```json
{"tools":[{"participant_identity":"desktop-client","tool_name":"desktop_notify","tier":2,"consent_expires_at":1798761600}]}
```

The policy is loaded once when the adapter is constructed. See
[`docs/remote-tools-design.md`](docs/remote-tools-design.md) for the closed
classification, bounds, and audit contract.

An authorized Kortexa API join can carry `kortexa_voice_client` metadata signed
with the existing `HERMES_REALTIME_PROXY_SIGNING_KEY`. The verifier binds the
room, exact participant, one-hour admission window and voice context. It does
not trust an unsigned metadata assertion or expose the account identity.
Admission permits only that live owner's declared client tools; explicit tier 3
denials, expired tier 2 entries and malformed policy remain denied. Reconnecting
participants must be admitted again, while the unchanged live participant keeps
its connection lease. Captured handlers and in-flight results cannot cross a
participant or room replacement.

The signed context adds API-owned speech guidance alongside session instructions
for both typed and spoken Conference turns. Companion-card guidance is added only
when the owner's `display_card` tool is registered and enabled. Direct Realtime
accepts this context only inside its verified, body-bound API proxy request;
ordinary direct clients cannot assert it through an unsigned session field.

Voice clients can send `session.update` with `model` and optional
`reasoning_effort` (`low`, `medium`, `high`, `xhigh`, or `null` for the model
default). The owning profile's `kortexa.ai` provider supplies the authenticated
`/v1/agent/models` catalog, including the API-owned `fast` and `smart` aliases.
An unavailable target fails explicitly. Session controls use Hermes's native
model resolution, selection guards and next-turn commit; they do not rewrite
profile defaults or change an agent already answering. Conference model controls
require the signed API participant admission.

Wait for `session.updated.client_event_id` to acknowledge a selection. Errors
correlate through `error.event_id` and retain the preceding model/effort.

The same acknowledged `session.update` supports a replacement flat `tools` list
on both transports. Send tool and model changes separately. Registration retains
the existing call/participant admission and tool policy; disabling a tool revokes
pending Direct output and stale Conference handlers/results. Registry generations
refresh the next Hermes turn without rewriting profile configuration.

Client snapshot tools can return a compact `client_image` JSON result with
`version: 1`, `mime_type: "image/jpeg"`, canonical base64 `data`, actual integer
`width` and `height`, and a short `text` summary. The limit is 10,000 JPEG bytes
and 640 pixels per edge, which fits the small RPC/data result path. Both transports
validate the envelope and pass a native Hermes multimodal result; bytes are never
sent as ordinary model text. The core vision capability policy owns any text-only
fallback. The existing byte-stream protocol remains available for larger results.
Overlapping updates return `session_update_busy`; connection closure cancels
pending selection work. Model changes that require native Hermes confirmation
return `model_confirmation_required` instead of accepting silently. Existing
clients that send no model selection keep their native Hermes session model.

Advertised RPC method names use case-sensitive ASCII identifier segments
separated by single dots, with a 64-character total limit. For example,
`camera.snapshot` is valid; `.camera`, `camera.`, `camera..snapshot`, path
separators, whitespace, Unicode, and normalized aliases are not.

Before advertising a tool, the client registers a LiveKit RPC method with the
same name. The agent calls that method with the tool arguments encoded as a
JSON object. The method returns either a JSON-shaped result or the bounded
byte-stream reference documented below, encoded as a JSON string, or raises
`RpcError`. LiveKit owns request correlation, response timeout, and error
transport. These calls do not use custom `agent:tool-call` or
`client:tool-result` data messages.

`examples/test_client.py` registers both `desktop_notify` and
`camera.snapshot`. The camera tool deliberately returns a built-in 1x1 PNG, so
the complete reference → targeted ready → byte stream → cleanup contract can be
tested without camera hardware. A real client can replace those fixture bytes
after applying the same 12 MiB bound. The stream header, chunks, and reference
target only the agent participant whose RPC invocation requested the snapshot.

For tools to be visible to the LLM, add `hermes-realtime-tools` to the
livekit toolset list in `~/.hermes/config.yaml`
(`platform_toolsets.livekit`). The plugin does not auto-activate the
toolset.

Tools are removed automatically when the registering participant disconnects.
Full JSON, bounded-binary, and participant-scoped multi-client design in
[`docs/remote-tools-design.md`](docs/remote-tools-design.md).

Unknown `type` values are ignored silently — keeps the topic compatible
with apps that share the same data channel for unrelated control traffic.

### Native tool approval cards

Voice approvals use the existing Hermes execution guard. The client's
`agent.approval.subscribe` (`version: 1`) enables structured requests on both
Realtime and Conference. A request is sent only to the subscribed participant
that owns the active native turn. The pending native request stays paused until
that exact request receives an offered choice.

`agent.approval.requested` carries the outer voice `session_id` and an `approval`
with opaque request/session/turn/tool-call bindings, redacted full command details,
expiry and choices. Send `agent.approval.response` with the same outer session,
an `event_id`, and `decision` containing all four bindings plus `choice`.
`agent.approval.resolved` closes the card. Native once/session/always/deny choices
are exposed only when the execution guard offers them; Hermes retains persistence.
Clients cannot override profile policy with `approval_mode`.

Disconnect, replacement, expiry and turn cancellation withdraw the exact pending
native request. A stale, foreign or replayed answer cannot grant permission.
If another native surface settles the request, the voice card closes without
inventing an approval outcome. Commands over 8 KiB are declined rather than
truncated for review. Automatic native execution mode remains unchanged.

### Video / camera-frame semantics

The LiveKit SDK receives and decodes a subscribed camera continuously. The
adapter keeps only the newest frame, so idle video does not accumulate an
unbounded queue or make later snapshots stale. Send
`{"type": "conference.capture_frame"}` on `conference.extensions` and the
agent samples the **latest available** frame (or waits for one), encodes
it as JPEG (quality 85), and queues it locally.

The frame attaches to **the next user message** dispatched by the adapter
(either a closed voice utterance or a standard typed Realtime turn). The Hermes agent
loop then processes it through its existing `image_input_mode: auto`
vision path — exactly the same code path used by image attachments on
other platforms.

Frames captured but never claimed by a message are cleaned up on
disconnect. Frames attached to a message stay on disk through the agent
turn (the agent loop is fire-and-forget after `handle_message`).

For source-review findings and deferred verification, see
[`docs/code-review.md`](docs/code-review.md).
