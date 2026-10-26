"""LiveKit voice platform adapter using WebRTC.

Joins a LiveKit room as a participant, transcribes inbound audio via
Hermes's STT pipeline, feeds transcripts into the agent loop, and publishes
TTS replies back as audio.

Carved out of hermes-agent's kortexa/gateway-livekit branch so it can be
installed as a pip plugin on top of upstream main without core patches.

Requires:
    pip install hermes-realtime
    LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET env vars
"""

import asyncio
import hashlib
import logging
import os
import subprocess
import tempfile
import time
import uuid
import weakref
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    from livekit import rtc
    LIVEKIT_AVAILABLE = True
except ImportError:
    LIVEKIT_AVAILABLE = False
    rtc = None  # type: ignore[assignment]

try:
    from livekit.api import AccessToken, VideoGrants, LiveKitAPI
    from livekit.protocol.room import ListParticipantsRequest
    LIVEKIT_API_AVAILABLE = True
except ImportError:
    LIVEKIT_API_AVAILABLE = False
    AccessToken = None  # type: ignore[assignment,misc]
    VideoGrants = None  # type: ignore[assignment,misc]
    LiveKitAPI = None  # type: ignore[assignment,misc]
    ListParticipantsRequest = None  # type: ignore[assignment,misc]

# Pillow is used to JPEG-encode sampled video frames before handing them to
# hermes's vision pipeline. The plugin still loads (and voice still works)
# if Pillow is missing; only frame capture is disabled.
try:
    from io import BytesIO
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    Image = None  # type: ignore[assignment,misc]
    BytesIO = None  # type: ignore[assignment,misc]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    ProcessingOutcome,
    SendResult,
)
from gateway.session import build_session_key
from gateway.platforms._shared import get_scoped_secret

from . import TOOLSET_NAME

from .realtime_protocol import (
    HERMES_INPUT_AUDIO_STATE,
    HERMES_INPUT_AUDIO_STATE_UPDATED,
    RealtimeProtocol,
)
from .tool_result_protocol import (
    DRAIN_TIMEOUT_SEC,
    REFERENCE_TYPE,
    BinaryResultProtocolError,
    BinaryResultReference,
    bounded_next_size,
    format_completed_result,
    parse_reference,
    release_stream,
    reserve_stream,
    stream_cancel_message,
    stream_ready_message,
    validate_completed_size,
    validate_stream_header,
)
from .direct_tools import resolve_gateway_session_key
from .client_image import decode_client_image
from .approval_bridge import ApprovalOrigin, approval_bridge, remember_approval_origin, forget_approval_origin
from .client_admission import ClientAdmission, verify_client_admission
from .model_controls import select_model, ModelControlError
from .tool_model import (
    FunctionToolDefinition,
    ToolDefinitionError,
    parse_function_tools,
)
from .tool_safety import (
    PolicyDecision,
    ToolAuditLog,
    ToolPolicy,
    ToolPolicyError,
    valid_participant_identity,
    valid_tool_name,
)
from .vad import (AdaptiveRmsGate, DEFAULT_SILENCE_DURATION, configured_silence_duration,
                  configured_vad_factory)
from .media import (EarlyTranscription, configured_asr_prefetch, pcm_rms,
                    transcribe_pcm, transcribe_with_prefetch, append_capture, PREROLL_BYTES)
from .streaming_tts import LiveKitStreamingTTSMixin
from .native_transcript import NativeTranscriptMixin

# Use the ``gateway.platforms.livekit`` namespace rather than ``__name__``.
# Hermes core's gateway.log handler installs a component filter that only
# admits records from loggers whose name starts with one of the component
# prefixes (``gateway`` is one of them — see ``hermes_logging.py``
# ``COMPONENT_PREFIXES``). Loggers outside that allowlist get dropped at
# the handler stage regardless of their own level. Adopting the
# ``gateway.platforms.<adapter>`` convention is also what the kortexa
# branch's core-resident version does, so the log output is
# byte-identical whether the LiveKit platform lives in core or here.
logger = logging.getLogger("gateway.platforms.livekit")


_LIVE_ADAPTERS: "weakref.WeakSet[LiveKitAdapter]" = weakref.WeakSet()


# Allow operators to dial verbosity without editing code:
#   HERMES_LIVEKIT_LOG_LEVEL=DEBUG    # noisy
#   HERMES_LIVEKIT_LOG_LEVEL=WARNING  # quiet
#   HERMES_LIVEKIT_LOG_LEVEL=20       # numeric also accepted
# Unset → inherit from hermes's root logger config (INFO under the standard
# gateway setup), matching every other built-in adapter.
def _apply_env_log_level() -> None:
    raw = os.getenv("HERMES_LIVEKIT_LOG_LEVEL", "").strip()
    if not raw:
        return
    try:
        logger.setLevel(int(raw))
        return
    except ValueError:
        pass
    level = logging.getLevelName(raw.upper())
    if isinstance(level, int):
        logger.setLevel(level)


_apply_env_log_level()

# Voice detection
SILENCE_THRESHOLD_SECONDS = DEFAULT_SILENCE_DURATION
MIN_SPEECH_DURATION = 0.5         # minimum seconds to process (skip noise)
RMS_SILENCE_FLOOR = 50            # PCM RMS below this is silence
POLL_INTERVAL = 0.2               # silence check interval when active

# LiveKit audio defaults
SAMPLE_RATE = 48000
NUM_CHANNELS = 1

# Reconnection
RECONNECT_BACKOFF = [2, 5, 10, 30, 60]
MAX_RECONNECT_ATTEMPTS = 10       # give up after this many consecutive failures
EMPTY_ROOM_GRACE_SECONDS = 2.0    # tolerate transient participant loss during reconnect

# Presence polling (when no humans in room, we stay out and poll).
# Defaults differ by deployment:
#   - LiveKit Cloud has real API rate limits and we pay per-minute, so
#     30s keeps headroom while still waking fast enough for normal UX.
#   - Self-hosted LiveKit has no limits and no cost pressure, so we poll
#     aggressively enough that the first speaker doesn't wait noticeably.
# Override with LIVEKIT_PRESENCE_POLL_INTERVAL (seconds) in the env if
# neither default fits.
PRESENCE_POLL_INTERVAL_CLOUD = 30.0
PRESENCE_POLL_INTERVAL_LOCAL = 5.0

# Remote tools (client-offered, callable by the agent). See
# docs/remote-tools-design.md. v0.3.0 ships protocol + desktop_notify-style
# small JSON results and bounded byte-stream results.
SCOPED_TOOL_PREFIX = "lk_"
TOOL_CALL_TIMEOUT_DEFAULT = 30.0
MAX_PENDING_BINARY_RESULTS = 8
MAX_IGNORED_BINARY_DRAINS = 4


@dataclass
class _BinaryTransfer:
    reference: BinaryResultReference
    room: Any
    generation: int
    result: asyncio.Future[bytes]
    buffer: bytearray = field(default_factory=bytearray)
    reader_task: Optional[asyncio.Task[None]] = None
    drain_task: Optional[asyncio.Task[None]] = None
    trailer_received: asyncio.Event = field(default_factory=asyncio.Event)
    terminal: bool = False
    handler_registered: bool = False

def check_livekit_requirements() -> bool:
    """Passively report whether the pinned LiveKit SDKs are importable.

    Hermes calls platform ``check_fn`` from status and config-loading paths, so
    this probe must not inspect credentials or perform installation. Credential
    validation belongs to the platform registration's ``validate_config`` hook.
    """
    return LIVEKIT_AVAILABLE and LIVEKIT_API_AVAILABLE


class LiveKitAdapter(NativeTranscriptMixin, LiveKitStreamingTTSMixin, BasePlatformAdapter):
    """LiveKit voice adapter using WebRTC.

    Joins a LiveKit room, captures participant audio, transcribes to text,
    and sends TTS replies back to the room.
    """

    def __init__(self, config: PlatformConfig):
        # PluginContext registers the name before the factory runs, allowing
        # Hermes to create its dynamic Platform member through the public
        # platform registry contract.
        super().__init__(config, Platform("livekit"))

        extra = config.extra or {}
        self.config.extra = extra
        self._silence_duration = configured_silence_duration(extra)
        self._asr_prefetch_silence = configured_asr_prefetch(extra, self._silence_duration)
        self._vad_factory = configured_vad_factory(extra)
        self._early_asr: Dict[str, EarlyTranscription] = {}
        # Realtime Conference owns one conversation per room. Participant
        # identity remains available on MessageEvent for attribution and tool
        # ownership, but it must not split the Hermes conversation history.
        self.config.extra["group_sessions_per_user"] = False
        self._url: str = extra.get("url") or get_scoped_secret("LIVEKIT_URL", "")
        self._api_key: str = extra.get("api_key") or get_scoped_secret("LIVEKIT_API_KEY", "")
        self._api_secret: str = extra.get("api_secret") or get_scoped_secret("LIVEKIT_API_SECRET", "")
        self._room_name: str = extra.get("room") or get_scoped_secret("LIVEKIT_ROOM", "hermes")
        self._agent_name: str = extra.get("agent_name") or get_scoped_secret("LIVEKIT_AGENT_NAME", "Hermes")
        self._agent_avatar: str = extra.get("agent_avatar") or get_scoped_secret("LIVEKIT_AGENT_AVATAR", "") or self._find_default_avatar()

        self._room: Optional["rtc.Room"] = None
        self._realtime_protocol: Optional[RealtimeProtocol] = None
        self._audio_source: Optional["rtc.AudioSource"] = None
        self._local_track: Optional["rtc.LocalAudioTrack"] = None
        self._silence_task: Optional[asyncio.Task] = None
        self._connect_task: Optional[asyncio.Task] = None
        self._presence_task: Optional[asyncio.Task] = None
        self._empty_room_task: Optional[asyncio.Task] = None
        self._graceful_leave: bool = False  # set while intentionally leaving

        # Per-participant audio buffers: identity -> (pcm bytearray, last_audio_time)
        self._audio_buffers: Dict[str, bytearray] = {}
        self._last_audio_time: Dict[str, float] = {}
        self._audio_streams: Dict[str, asyncio.Task] = {}
        self._audio_gates: Dict[str, AdaptiveRmsGate] = {}
        self._audio_processed: Dict[str, tuple[int, bool | None]] = {}
        self._audio_overflowed: set[str] = set()
        self._muted_inputs: set[str] = set()
        self._audio_ready = asyncio.Event()

        # Pause audio capture during TTS playback
        self._paused = False
        # ponytail: client intent is independent of playback's echo pause.
        self._client_paused = False

        # Throttle for presence-check failure warnings (see
        # _count_remote_participants).
        self._presence_error_logged = 0.0

        # Per-participant speech state (for listening-start/stop events)
        self._speaking_participants: set[str] = set()
        self._deferred_internal_wakes: set[asyncio.Task[None]] = set()

        # The SDK receives video continuously; retain only the newest frame.
        # JPEG encoding runs only on conference.capture_frame requests.
        self._video_streams: Dict[str, "rtc.VideoStream"] = {}

        # Frames captured-but-not-yet-dispatched. Drained into the next
        # MessageEvent built by _process_voice_input or _handle_client_message.
        # Each entry is a (path, mime_type) tuple. Paths are temp files written
        # under <tempdir>/hermes_realtime/; cleanup happens on disconnect (the
        # agent loop reads the file after handle_message returns, so we can't
        # unlink at dispatch time).
        self._pending_captures: list[tuple[str, str]] = []

        # Remote tools registered by connected clients over the data channel.
        # Registry names are participant-scoped; RPC method names remain the
        # names advertised by each client.
        self._client_tools: Dict[str, set[str]] = {}  # identity -> registry names
        self._tool_owners: Dict[str, str] = {}  # registry name -> owner identity
        self._tool_methods: Dict[str, str] = {}  # registry name -> RPC method
        self._tool_policy = self._resolve_tool_policy()
        self._api_client_key = str(get_scoped_secret("HERMES_REALTIME_PROXY_SIGNING_KEY", "") or "").strip()
        self._api_client_admissions = {}
        self._tool_audit = ToolAuditLog()
        self._pending_native_rpc_tasks: dict[str, set[asyncio.Task[Any]]] = {}
        _LIVE_ADAPTERS.add(self)

        # Binary RPC results use one reserved LiveKit topic per pending call.
        # State is also initialized lazily for contract tests that construct the
        # adapter with object.__new__.
        self._binary_topics: set[str] = set()
        self._binary_transfers: Dict[str, _BinaryTransfer] = {}
        self._room_generation = 0
        self._room_replacement_started = False
        self._binary_ignored_drains: set[asyncio.Task[None]] = set()
        self._binary_replacement_scheduled = False

        self._presence_poll_interval: float = self._resolve_presence_poll_interval()
        self._tool_call_timeout: float = self._resolve_tool_call_timeout()

    def _should_auto_tts_for_chat(self, chat_id: str) -> bool:
        """LiveKit is voice-first — always auto-TTS unless the chat opted out.

        On text platforms the default is gated by ``voice.auto_tts`` (off by
        default). On LiveKit the channel itself is audio, so a typed-only
        reply gives the user nothing. Per-chat ``/voice off`` still wins.
        """
        if chat_id in self._auto_tts_disabled_chats:
            return False
        return True

    def _resolve_presence_poll_interval(self) -> float:
        """Pick the presence-poll interval: env override > cloud/local default.

        LiveKit Cloud hosts on ``*.livekit.cloud``; anything else is treated
        as a self-hosted deployment and gets the faster default.
        """
        override = get_scoped_secret("LIVEKIT_PRESENCE_POLL_INTERVAL", "").strip()
        if override:
            try:
                parsed = float(override)
                if parsed > 0:
                    logger.info("[%s] presence poll interval=%.1fs (LIVEKIT_PRESENCE_POLL_INTERVAL)", self.name, parsed)
                    return parsed
            except ValueError:
                logger.warning("[%s] LIVEKIT_PRESENCE_POLL_INTERVAL=%r is not a number; using default", self.name, override)

        is_cloud = ".livekit.cloud" in self._url.lower()
        interval = PRESENCE_POLL_INTERVAL_CLOUD if is_cloud else PRESENCE_POLL_INTERVAL_LOCAL
        logger.info("[%s] presence poll interval=%.1fs (%s default)", self.name, interval, "cloud" if is_cloud else "local")
        return interval

    def _resolve_tool_call_timeout(self) -> float:
        """Per-call timeout for remote tools. HERMES_LIVEKIT_TOOL_TIMEOUT_SEC overrides."""
        raw = get_scoped_secret("HERMES_LIVEKIT_TOOL_TIMEOUT_SEC", "").strip()
        if raw:
            try:
                parsed = float(raw)
                if parsed > 0:
                    return parsed
            except ValueError:
                logger.warning("[%s] HERMES_LIVEKIT_TOOL_TIMEOUT_SEC=%r is not a number; using default", self.name, raw)
        return TOOL_CALL_TIMEOUT_DEFAULT

    def _resolve_tool_policy(self) -> ToolPolicy:
        """Load the closed operator policy; malformed input denies every tool."""
        raw = get_scoped_secret("HERMES_LIVEKIT_REMOTE_TOOL_POLICY", "")
        self._tool_policy_invalid = False
        try:
            return ToolPolicy.parse(raw)
        except ToolPolicyError:
            self._tool_policy_invalid = True
            logger.warning(
                "[%s] remote-tool policy invalid; all remote tools denied",
                self.name,
            )
            return ToolPolicy()

    @staticmethod
    def _find_default_avatar() -> str:
        """Look for a default avatar image in ~/.hermes/."""
        from pathlib import Path
        hermes_home = Path.home() / ".hermes"
        for name in ("agent.png", "agent.jpg"):
            path = hermes_home / name
            if path.is_file():
                return str(path)
        return ""

    def _resolve_avatar_url(self) -> str:
        """Convert avatar to a URL suitable for LiveKit metadata.

        If it's already a URL, use as-is. If it's a local file, encode
        as a data URI so it works without a web server.
        """
        avatar = self._agent_avatar
        if not avatar:
            return ""
        if avatar.startswith(("http://", "https://", "data:")):
            return avatar
        # Local file — base64 encode as data URI
        try:
            import base64
            from pathlib import Path
            path = Path(avatar).expanduser()
            if not path.is_file():
                return ""
            suffix = path.suffix.lower()
            mime = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg"}.get(suffix, "image/png")
            data = base64.b64encode(path.read_bytes()).decode("ascii")
            return f"data:{mime};base64,{data}"
        except Exception:
            return ""

    # -- Connection lifecycle -----------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Start the LiveKit adapter.

        Presence-aware: if the room already has at least one remote
        participant, join immediately. Otherwise stay out and run a
        presence watcher that joins as soon as someone arrives. Either
        way the adapter is "connected" from the gateway's point of view.

        ``is_reconnect`` is part of the BasePlatformAdapter.connect
        contract (the gateway's reconnection watcher passes it); this
        adapter has no cold-boot vs reconnect distinction, so it is
        accepted and ignored.
        """
        if not LIVEKIT_AVAILABLE:
            logger.warning("[%s] livekit SDK not installed. Run: pip install hermes-realtime", self.name)
            return False
        if not LIVEKIT_API_AVAILABLE:
            logger.warning("[%s] livekit-api not installed. Run: pip install hermes-realtime", self.name)
            return False
        if not self._url or not self._api_key or not self._api_secret:
            logger.warning("[%s] LIVEKIT_URL, LIVEKIT_API_KEY, and LIVEKIT_API_SECRET required", self.name)
            return False

        self._running = True

        # Check if anyone is in the room already. If not, don't consume a
        # participant slot — just watch.
        count = await self._count_remote_participants()
        if count > 0:
            logger.info("[%s] %d participant(s) already in '%s', joining", self.name, count, self._room_name)
            return await self._join_room()

        logger.info("[%s] Room '%s' empty, watching for participants (poll %.1fs)", self.name, self._room_name, self._presence_poll_interval)
        self._mark_connected()
        self._presence_task = asyncio.create_task(self._presence_watch_loop())
        return True

    async def _count_remote_participants(self) -> int:
        """Count non-local participants currently in the room via the Server API.

        Returns 0 on any error (room missing, network blip, etc.) — callers
        treat that as "nobody here, keep polling".
        """
        try:
            # Server API expects http(s):// scheme; convert from ws(s)://.
            http_url = self._url
            if http_url.startswith("wss://"):
                http_url = "https://" + http_url[6:]
            elif http_url.startswith("ws://"):
                http_url = "http://" + http_url[5:]
            http_url = http_url.rstrip("/")

            client = LiveKitAPI(url=http_url, api_key=self._api_key, api_secret=self._api_secret)
            try:
                resp = await client.room.list_participants(
                    ListParticipantsRequest(room=self._room_name)
                )
                return len(resp.participants)
            finally:
                await client.aclose()
        except Exception as e:
            # A failing presence check is indistinguishable from an empty room:
            # we return 0 either way and the agent simply never joins, silently,
            # forever. Say so out loud the first time and then periodically,
            # rather than hiding it at DEBUG (which the gateway's log handler
            # drops anyway).
            now = time.time()
            if now - self._presence_error_logged > 300:
                self._presence_error_logged = now
                logger.warning(
                    "[%s] presence check failed — the agent cannot see participants in '%s': %s: %s",
                    self.name, self._room_name, type(e).__name__, e,
                )
            else:
                logger.debug("[%s] presence check failed: %s", self.name, e)
            return 0

    async def _presence_watch_loop(self) -> None:
        """Poll the room; join as soon as a remote participant appears."""
        try:
            while self._running:
                await asyncio.sleep(self._presence_poll_interval)
                if not self._running:
                    return
                if self._room is not None:
                    # Something else joined us (manual reconnect?); stop polling.
                    return
                count = await self._count_remote_participants()
                if count > 0:
                    logger.info("[%s] Participant detected in '%s', joining", self.name, self._room_name)
                    if await self._join_room():
                        return  # joined — done polling
        except asyncio.CancelledError:
            return

    async def _join_room(self) -> bool:
        """Actually establish the LiveKit room connection and start audio I/O."""
        try:
            # Overwriting self._room would orphan the previous one. Its FFI
            # listen task keeps it alive, so its ICE sockets would never be
            # released -- see _release_room.
            if self._room is not None:
                logger.warning(
                    "[%s] joining with a room already set; releasing the old one",
                    self.name,
                )
                stale, self._room = self._room, None
                self._fail_binary_generation(self._room_generation, "room_replaced")
                await self._close_capture_streams()
                self._discard_pending_captures()
                self._graceful_leave = True
                try:
                    await self._release_room(stale, why="stale-on-join")
                finally:
                    self._graceful_leave = False
                if self._realtime_protocol:
                    await self._realtime_protocol.close()

            self._audio_source = None
            self._local_track = None
            self._paused = False
            self._room = rtc.Room()
            self._realtime_protocol = self._new_realtime_protocol()
            self._room_generation += 1
            self._room_replacement_started = False
            self._binary_ignored_drains = set()
            self._binary_replacement_scheduled = False

            # Register event handlers
            self._room.on("track_subscribed", self._on_track_subscribed)
            self._room.on("track_unsubscribed", self._on_track_unsubscribed)
            self._room.on("participant_connected", self._on_participant_connected)
            self._room.on("participant_disconnected", self._on_participant_disconnected)
            self._room.on("disconnected", self._on_disconnected)
            # Inbound data-channel: portable Realtime events and tools use the
            # shared topics; optional controls use conference.extensions.
            receiving_room = self._room
            receiving_generation = self._room_generation
            self._room.on(
                "data_received",
                lambda packet: self._on_data_received(
                    packet,
                    receiving_room=receiving_room,
                    receiving_generation=receiving_generation,
                ),
            )

            # Create access token
            import json as _json
            token = (
                AccessToken(api_key=self._api_key, api_secret=self._api_secret)
                .with_identity(f"hermes-{self._agent_name.lower()}")
                .with_name(self._agent_name)
                .with_grants(VideoGrants(
                    room_join=True,
                    room=self._room_name,
                    can_publish=True,
                    can_subscribe=True,
                    can_update_own_metadata=True,
                ))
            )
            jwt_token = token.to_jwt()

            # Connect to room
            await self._room.connect(self._url, jwt_token)

            # Set metadata (including avatar) after connecting — avoids JWT size limits
            metadata = {}
            avatar_url = self._resolve_avatar_url()
            if avatar_url:
                metadata["avatar"] = avatar_url
            if metadata:
                await self._room.local_participant.set_metadata(_json.dumps(metadata))

            # Publish a local audio track for TTS playback
            self._audio_source = rtc.AudioSource(SAMPLE_RATE, NUM_CHANNELS)
            self._local_track = rtc.LocalAudioTrack.create_audio_track(
                "hermes-voice", self._audio_source
            )
            options = rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE)
            await self._room.local_participant.publish_track(self._local_track, options)

            for participant in self._room.remote_participants.values():
                await self._realtime_protocol.client_connected(participant.identity)

            # Start silence detection loop
            self._silence_task = asyncio.create_task(self._check_silence_loop())

            self._mark_connected()
            logger.info("[%s] Connected to room '%s' at %s", self.name, self._room_name, self._url)

            # If no explicit agent name was configured, ask the LLM and reconnect
            if not get_scoped_secret("LIVEKIT_AGENT_NAME") and not (self.config.extra or {}).get("agent_name"):
                asyncio.create_task(self._resolve_agent_name())

            return True
        except Exception as e:
            if self._realtime_protocol:
                await self._realtime_protocol.close()
                self._realtime_protocol = None
            # A failed join must not leave a non-None room that makes the
            # presence watcher believe it has connected successfully.
            failed, self._room = self._room, None
            self._graceful_leave = True
            try:
                await self._release_room(failed, why="failed-join")
            finally:
                self._graceful_leave = False
            logger.error("[%s] Failed to connect: %s", self.name, e)
            return False

    async def disconnect(self) -> None:
        """Disconnect from LiveKit room."""
        self._running = False
        self._mark_disconnected()

        if self._connect_task and self._connect_task is not asyncio.current_task():
            self._connect_task.cancel()
            await asyncio.gather(self._connect_task, return_exceptions=True)
            self._connect_task = None

        if self._presence_task:
            self._presence_task.cancel()
            try:
                await self._presence_task
            except asyncio.CancelledError:
                pass
            self._presence_task = None

        if self._empty_room_task:
            self._empty_room_task.cancel()
            try:
                await self._empty_room_task
            except asyncio.CancelledError:
                pass
            self._empty_room_task = None

        await self._close_capture_streams()

        if self._room:
            self._graceful_leave = True
            room, self._room = self._room, None
            try:
                self._fail_binary_generation(self._room_generation, "room_replaced")
                await self._release_room(room, why="disconnect")
            finally:
                self._graceful_leave = False

        if self._realtime_protocol:
            await self._realtime_protocol.close()
            self._realtime_protocol = None

        self._audio_source = None
        self._local_track = None
        self._audio_buffers.clear()
        self._last_audio_time.clear()
        self._audio_gates.clear()
        getattr(self, "_audio_processed", {}).clear()
        getattr(self, "_audio_overflowed", set()).clear()
        self._muted_inputs.clear()
        self._speaking_participants.clear()
        for task in list(self._deferred_internal_wakes):
            task.cancel()
        self._deferred_internal_wakes.clear()

        # Unlink any frame files that were captured but never dispatched
        # (no MessageEvent ever drained them). Dispatched-but-not-yet-read
        # files live on — the agent loop may still be processing.
        self._discard_pending_captures()

        # Drop every client-registered tool from the hermes registry.
        self._cleanup_all_client_tools()


        logger.info("[%s] Disconnected", self.name)

    # -- LiveKit event handlers ---------------------------------------------

    def _new_realtime_protocol(self) -> RealtimeProtocol:
        return RealtimeProtocol(
            session_id=self._room_name,
            model="hermes",
            voice="hermes",
            publish=self._publish_realtime_event,
            on_text_input=self._handle_realtime_text_input,
            on_response_requested=self._handle_realtime_response_requested,
            on_response_cancelled=self._cancel_realtime_response,
            on_model_selection=self._select_conference_model,
            on_tools_updated=self._replace_realtime_client_tools,
            on_approval_subscribe=self._subscribe_voice_approvals,
            on_approval_response=lambda event, identity: approval_bridge(self).respond(event, identity),
            on_approval_disconnect=lambda identity: approval_bridge(self).unsubscribe(identity, self._room_name),
        )

    def _subscribe_voice_approvals(self, identity):
        if self._api_client_admission(identity) is None:
            raise ValueError("Authenticated participant admission required")
        approval_bridge(self).subscribe(identity, self._room_name)

    async def _replace_realtime_client_tools(self, tools, identity):
        room, generation = self._room, self._room_generation
        if not self._tool_message_is_current(identity, room, generation):
            raise RuntimeError("Tool owner unavailable")
        await self._register_client_tools({"tools": [
            {"type": "function", "function": {"name": tool.name, "description": tool.description, "parameters": tool.parameters}}
            for tool in tools]}, identity, receiving_room=room, receiving_generation=generation)
        actual = {self._tool_methods.get(name) for name in self._client_tools.get(identity, set())}
        if actual != {tool.name for tool in tools}:
            raise RuntimeError("Tool registration denied")

    async def _select_conference_model(self, model, effort, identity):
        room, generation = self._room, self._room_generation
        participant = room.remote_participants.get(identity) if room else None
        if self._api_client_admission(identity) is None:
            raise ModelControlError("model_controls_denied", "Join through the authenticated API to change this model.")
        source = self.build_source(chat_id=self._room_name, chat_name=self._room_name,
                                   chat_type="group", user_id=identity, user_name=identity)
        return await select_model(self, source, model, effort, is_current=lambda: (
            self._room is room and self._room_generation == generation
            and room.remote_participants.get(identity) is participant))

    async def _publish_realtime_event(
        self,
        event: Dict[str, Any],
        recipient: Optional[str],
    ) -> bool:
        return await self._publish_typed(
            event,
            identity=recipient,
            topic=self.DATA_CHANNEL_EVENTS_TOPIC,
        )

    async def _handle_realtime_text_input(self, text: str, identity: str) -> None:
        await self._handle_client_message(
            {"text": text},
            identity,
            publish_transcript=False,
        )

    async def _handle_realtime_response_requested(self, identity: str) -> None:
        await self._handle_realtime_text_input(
            "Follow the session instructions and respond now.",
            identity,
        )

    async def _cancel_realtime_response(self, identity: str) -> None:
        await self._abort_tts_for_chat(self._room_name)
        source = self.build_source(
            chat_id=self._room_name,
            chat_name=self._room_name,
            chat_type="group",
            user_id=identity,
            user_name=identity,
        )
        session_key = build_session_key(
            source,
            group_sessions_per_user=False,
            thread_sessions_per_user=False,
            profile=self._session_key_profile(source),
        )
        if self._audio_source:
            self._audio_source.clear_queue()
        await self.cancel_session_processing(session_key)
        if self._audio_source:
            self._audio_source.clear_queue()
        self._paused = False

    def _on_participant_connected(self, participant: "rtc.RemoteParticipant") -> None:
        self._api_client_admission(participant.identity)
        empty_room_task = getattr(self, "_empty_room_task", None)
        if empty_room_task and not empty_room_task.done():
            empty_room_task.cancel()
        self._empty_room_task = None
        protocol = getattr(self, "_realtime_protocol", None)
        if not protocol:
            return
        try:
            asyncio.create_task(protocol.client_connected(participant.identity))
        except RuntimeError:
            pass

    def _on_track_subscribed(
        self,
        track: "rtc.Track",
        publication: "rtc.RemoteTrackPublication",
        participant: "rtc.RemoteParticipant",
    ):
        """Start capturing media when a participant's track is subscribed.

        Audio tracks are buffered continuously for VAD/STT. Video streams keep
        their latest frame — JPEGs are encoded on demand when a
        client sends ``conference.capture_frame`` on the
        ``conference.extensions`` data-channel topic.
        """
        identity = participant.identity

        if track.kind == rtc.TrackKind.KIND_AUDIO:
            logger.info("[%s] Audio track subscribed: %s", self.name, identity)
            old_task = self._audio_streams.pop(identity, None)
            if old_task is not None:
                old_task.cancel()
            self._last_audio_time.pop(identity, None)
            self._speaking_participants.discard(identity)
            self._audio_buffers[identity] = bytearray()
            self._audio_ready.set()
            self._audio_gates[identity] = self._new_audio_gate()
            self._audio_processed.pop(identity, None)
            self._audio_overflowed.discard(identity)
            # Deliberately do NOT seed _last_audio_time here. _check_silence_loop
            # sets it on the first chunk above RMS_SILENCE_FLOOR, and treats a
            # missing entry as "this participant has never spoken" — discarding
            # accumulated noise instead of feeding silence to STT. Seeding it on
            # subscribe defeats that guard: a participant who only ever publishes
            # silence would accrue a stale timestamp and eventually trip an
            # utterance.
            stream = rtc.AudioStream(track)
            task = asyncio.create_task(self._audio_receive_loop(stream, identity))
            self._audio_streams[identity] = task
            return

        if track.kind == rtc.TrackKind.KIND_VIDEO:
            if not PIL_AVAILABLE:
                logger.warning(
                    "[%s] Video track from %s ignored — Pillow not installed",
                    self.name, identity,
                )
                return
            # Replace any prior stream for this participant (e.g. camera toggled).
            old = self._video_streams.pop(identity, None)
            if old is not None:
                try:
                    asyncio.create_task(old.aclose())
                except Exception:
                    pass
            self._video_streams[identity] = rtc.VideoStream(track, capacity=1)
            logger.info("[%s] Video track subscribed: %s (sampling-on-trigger)", self.name, identity)
            return

    def _on_track_unsubscribed(
        self,
        track: "rtc.Track",
        publication: "rtc.RemoteTrackPublication",
        participant: "rtc.RemoteParticipant",
    ):
        """Clean up when a participant's track is unsubscribed."""
        identity = participant.identity

        if track.kind == rtc.TrackKind.KIND_AUDIO:
            logger.debug("[%s] Audio track unsubscribed: %s", self.name, identity)
            self._cleanup_participant(identity)
            return

        if track.kind == rtc.TrackKind.KIND_VIDEO:
            logger.debug("[%s] Video track unsubscribed: %s", self.name, identity)
            stream = self._video_streams.pop(identity, None)
            if stream is not None:
                try:
                    asyncio.create_task(stream.aclose())
                except Exception:
                    pass
            return

    def _on_participant_disconnected(self, participant: "rtc.RemoteParticipant"):
        """Clean up when a participant leaves the room.

        If we're now alone in the room, drop the connection and go back
        to presence polling — no need to consume a participant slot while
        nobody's here to talk to.
        """
        identity = participant.identity
        logger.info("[%s] Participant disconnected: %s", self.name, identity)
        self._cleanup_participant(identity)
        # Drop any tools this client had registered.
        self._cleanup_client_tools(identity)
        getattr(self, "_api_client_admissions", {}).pop(identity, None)
        self._fail_binary_owner(identity)
        protocol = getattr(self, "_realtime_protocol", None)
        if protocol:
            protocol.client_disconnected(identity)

        if self._room and not self._room.remote_participants:
            logger.info(
                "[%s] Last participant left '%s', waiting %.1fs for reconnect",
                self.name,
                self._room_name,
                EMPTY_ROOM_GRACE_SECONDS,
            )
            pending = getattr(self, "_empty_room_task", None)
            if pending is None or pending.done():
                room = self._room
                generation = self._room_generation
                self._empty_room_task = asyncio.create_task(
                    self._leave_after_empty_grace(room, generation)
                )

    async def _leave_after_empty_grace(self, room: "rtc.Room", generation: int) -> None:
        """Leave only when a room stays empty through the reconnect window."""
        try:
            await asyncio.sleep(EMPTY_ROOM_GRACE_SECONDS)
            if (
                self._room is room
                and self._room_generation == generation
                and not room.remote_participants
            ):
                logger.info(
                    "[%s] Room '%s' remained empty, leaving",
                    self.name,
                    self._room_name,
                )
                await self._leave_and_watch()
        except asyncio.CancelledError:
            return
        finally:
            if getattr(self, "_empty_room_task", None) is asyncio.current_task():
                self._empty_room_task = None

    async def _release_room(self, room: Optional["rtc.Room"], *, why: str) -> None:
        """Disconnect through LiveKit's public API and let the SDK own FFI cleanup.

        LiveKit 1.1.10 added a listen-task completion callback that unsubscribes
        the FFI queue even when ``Room.disconnect()`` returns early after a remote
        disconnect. Reaching into ``_task``, ``_ffi_queue``, and ``_ffi_handle``
        here would now duplicate SDK cleanup and couple the adapter to internals.
        """
        if room is None:
            return
        try:
            await room.disconnect()
        except Exception as e:
            logger.debug("[%s] disconnect during %s: %s", self.name, why, e)

    async def _close_capture_streams(self) -> None:
        """Stop silence detection and release the SDK's media receivers."""
        await self._abort_tts_for_chat(getattr(self, "_room_name", ""))
        tasks = list(getattr(self, "_audio_streams", {}).values())
        silence = getattr(self, "_silence_task", None)
        if silence is not None:
            tasks.append(silence)
        self._silence_task = None
        getattr(self, "_audio_streams", {}).clear()
        tasks = [task for task in tasks if task is not asyncio.current_task()]
        # Cancel all audio stream tasks before waiting on any of them.
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        # Close the SDK's background video receivers.
        streams = list(getattr(self, "_video_streams", {}).values())
        getattr(self, "_video_streams", {}).clear()
        if streams:
            await asyncio.gather(*(stream.aclose() for stream in streams), return_exceptions=True)
        getattr(self, "_audio_buffers", {}).clear()
        getattr(self, "_last_audio_time", {}).clear()
        getattr(self, "_audio_gates", {}).clear()
        getattr(self, "_audio_processed", {}).clear()
        getattr(self, "_audio_overflowed", set()).clear()
        for prefetch in getattr(self, "_early_asr", {}).values():
            prefetch.discard()
        getattr(self, "_early_asr", {}).clear()
        getattr(self, "_muted_inputs", set()).clear()
        getattr(self, "_speaking_participants", set()).clear()

    def _discard_pending_captures(self) -> None:
        for path, _mime in getattr(self, "_pending_captures", []):
            try:
                os.unlink(path)
            except OSError:
                pass
        self._pending_captures = []

    async def _leave_and_watch(self) -> None:
        """Tear down the room connection and resume presence polling."""
        # Stop silence detection and audio streams, but keep self._running
        # so the presence loop can resume us later.
        await self._close_capture_streams()
        self._discard_pending_captures()
        self._audio_buffers.clear()
        self._last_audio_time.clear()
        self._audio_gates.clear()
        getattr(self, "_audio_processed", {}).clear()
        getattr(self, "_audio_overflowed", set()).clear()
        self._muted_inputs.clear()
        self._speaking_participants.clear()

        # Clients will be gone after we drop the room — clear their tools.
        self._cleanup_all_client_tools()
        self._fail_binary_generation(self._room_generation, "room_replaced")

        if self._room:
            self._graceful_leave = True
            room, self._room = self._room, None
            try:
                await self._release_room(room, why="leave")
            finally:
                self._graceful_leave = False
        if self._realtime_protocol:
            await self._realtime_protocol.close()
            self._realtime_protocol = None
        self._audio_source = None
        self._local_track = None
        # Client pause belongs to the departed session, not the next arrival.
        self._client_paused = False

        if self._running and (self._presence_task is None or self._presence_task.done()):
            self._presence_task = asyncio.create_task(self._presence_watch_loop())

    def _on_disconnected(self, reason: str = ""):
        """Handle unexpected room disconnection — schedule reconnection.

        Graceful leaves (empty room, full teardown) set ``_graceful_leave``
        so we don't fight with ``_leave_and_watch`` / ``disconnect``.
        """
        if not self._running or self._graceful_leave:
            return
        if self._connect_task and not self._connect_task.done():
            return
        self._fail_binary_generation(self._room_generation, "room_replaced")
        self._cleanup_all_client_tools()
        logger.warning("[%s] Disconnected from room: %s. Will reconnect.", self.name, reason)
        self._connect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self):
        """Reconnect to LiveKit with exponential backoff.

        Caps at MAX_RECONNECT_ATTEMPTS consecutive failures — beyond that the
        adapter stays disconnected rather than spamming a misconfigured URL
        forever. The user can restart the gateway to retry.
        """
        backoff_idx = 0
        attempts = 0
        while self._running:
            if attempts >= MAX_RECONNECT_ATTEMPTS:
                logger.error(
                    "[%s] Giving up after %d reconnect attempts. Restart the gateway to try again.",
                    self.name, attempts,
                )
                self._running = False
                self._mark_disconnected()
                return
            delay = RECONNECT_BACKOFF[min(backoff_idx, len(RECONNECT_BACKOFF) - 1)]
            logger.info("[%s] Reconnecting in %ds (attempt %d/%d)...", self.name, delay, attempts + 1, MAX_RECONNECT_ATTEMPTS)
            await asyncio.sleep(delay)
            if not self._running:
                return
            try:
                if await self._join_room():
                    logger.info("[%s] Reconnected successfully", self.name)
                    return
            except Exception as e:
                logger.warning("[%s] Reconnect attempt failed: %s", self.name, e)
            backoff_idx += 1
            attempts += 1

    async def _resolve_agent_name(self):
        """Ask the LLM for the agent's name, then update the display name in-place."""
        try:
            from openai import AsyncOpenAI
            from hermes_cli.config import load_config

            config = load_config()
            model_config = config.get("model", {})
            provider = model_config.get("provider", "")
            model = model_config.get("default", "")

            # Use the runtime provider resolution to get the right client
            from hermes_cli.runtime_provider import resolve_requested_provider
            resolved = resolve_requested_provider(provider, model)
            if not resolved or not resolved.get("api_key"):
                return

            client = AsyncOpenAI(
                api_key=resolved["api_key"],
                base_url=resolved.get("base_url"),
            )
            resp = await client.chat.completions.create(
                model=resolved.get("model", model),
                messages=[{"role": "user", "content": "What is your name? Reply with ONLY your first name — no quotes, no punctuation, no explanation. It will be used as your on-screen display label in a video call."}],
                max_tokens=20,
            )
            name = resp.choices[0].message.content.strip().strip('"').strip("'").split()[0] if resp.choices else ""
            if not name or name.lower() == "hermes" or len(name) > 30:
                return

            logger.info("[%s] LLM says agent name is '%s', updating display name", self.name, name)
            self._agent_name = name
            await self._room.local_participant.set_name(name)
        except Exception as e:
            logger.debug("[%s] Could not resolve agent name from LLM: %s", self.name, e)

    def _cleanup_participant(self, identity: str):
        """Remove buffers and cancel audio stream for a participant.

        If the participant was mid-utterance when the track went away
        (e.g. their mic dropped or — for file-based publishers — the
        clip ended), flush whatever speech has been buffered before
        discarding it, so the user's last words still reach STT.
        """
        # Flush a pending utterance, if any, before tearing buffers down.
        buf = self._audio_buffers.get(identity)
        if buf is not None and identity in self._speaking_participants and len(buf) > 0:
            # Use the whole buffer: the track just ended, so there is no
            # "ongoing silence" to trim — the buffer ends at (or very close
            # to) the last spoken word. The steady-state path in
            # _check_silence_loop trims trailing silence because the
            # participant is still connected; here, subtracting a fixed
            # silence window would chop real speech (or zero the flush
            # entirely) when the track ends right after a word. Trailing
            # silence in the audio handed to STT is harmless; lost words
            # are not.
            self._flush_utterance(identity, len(buf))

        task = self._audio_streams.pop(identity, None)
        if task:
            task.cancel()
        self._audio_buffers.pop(identity, None)
        self._last_audio_time.pop(identity, None)
        getattr(self, "_audio_gates", {}).pop(identity, None)
        getattr(self, "_audio_processed", {}).pop(identity, None)
        getattr(self, "_audio_overflowed", set()).discard(identity)
        prefetch = getattr(self, "_early_asr", {}).pop(identity, None)
        if prefetch is not None:
            prefetch.discard()
        getattr(self, "_muted_inputs", set()).discard(identity)
        self._speaking_participants.discard(identity)

    # -- Audio capture and processing ---------------------------------------

    def _new_audio_gate(self):
        factory = getattr(self, "_vad_factory", AdaptiveRmsGate)
        return factory(minimum_floor=RMS_SILENCE_FLOOR)

    async def _capture_overflow(self, identity: str) -> None:
        self._audio_overflowed.add(identity)
        self._audio_processed.pop(identity, None)
        self._last_audio_time[identity] = time.monotonic()
        self._speaking_participants.discard(identity)
        prefetch = getattr(self, "_early_asr", {}).get(identity)
        if prefetch is not None:
            prefetch.discard()
        if self._realtime_protocol is not None:
            await self._realtime_protocol.input_audio_overflow(identity)
        await self._publish_agent_event("agent:listening-stop", {"identity": identity})
        await self._publish_typed({"type": "error", "error": {
            "code": "input_audio_too_long",
            "message": "Speech exceeded two minutes. Please pause and try again.",
        }}, identity=identity, topic=self.DATA_CHANNEL_EXTENSIONS_TOPIC)

    async def _audio_receive_loop(
        self,
        stream: "rtc.AudioStream",
        identity: str,
    ):
        """Receive audio frames from a participant and buffer them.

        This loop must drain the SDK's internal queue as fast as possible
        to avoid 'native audio stream queue overflow' warnings.  All
        heavy processing (RMS, silence detection) happens in
        _check_silence_loop instead.
        """
        try:
            self._audio_processed = getattr(self, "_audio_processed", {})
            self._audio_overflowed = getattr(self, "_audio_overflowed", set())
            async for event in stream:
                if self._paused or getattr(self, "_client_paused", False) or identity in self._muted_inputs:
                    continue
                if identity not in self._audio_buffers:
                    break

                pcm = event.frame.data.tobytes()
                buffer = self._audio_buffers[identity]
                if identity in self._audio_overflowed:
                    # Keep enough fresh sound to recognize a recovery pause,
                    # even if the polling task is delayed by other work.
                    buffer.extend(pcm[-PREROLL_BYTES * 2:])
                    del buffer[:-PREROLL_BYTES * 2]
                elif not append_capture(buffer, pcm):
                    await self._capture_overflow(identity)
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.warning("[%s] Audio receive error for %s: %s", self.name, identity, e)
        finally:
            await stream.aclose()

    async def _check_silence_loop(self):
        """Periodically check for completed utterances (silence after speech).

        Each tick, we classify the new audio in every participant's buffer to
        decide whether they are currently speaking or silent. When
        silence exceeds the threshold, we extract the utterance and
        send it for transcription.

        Waits for an audio subscription when no participants are buffered.
        A joining participant wakes the detector immediately, so a short
        first utterance cannot hide inside a multi-second idle polling gap.
        """
        try:
            self._audio_processed = getattr(self, "_audio_processed", {})
            self._audio_overflowed = getattr(self, "_audio_overflowed", set())
            while self._running:
                # No one to listen to — wake on subscription, without polling.
                if not self._audio_buffers:
                    self._audio_ready.clear()
                    await self._audio_ready.wait()
                    continue
                await asyncio.sleep(POLL_INTERVAL)
                if self._paused or getattr(self, "_client_paused", False):
                    continue

                for identity in list(self._audio_buffers.keys()):
                    buf = self._audio_buffers.get(identity)
                    if buf is None:
                        continue

                    buf_len = len(buf)
                    if buf_len == 0:
                        continue

                    processed, speech = self._audio_processed.get(identity, (0, None))
                    pending = bytes(buf[processed:])
                    if not pending and speech is not False:
                        continue  # Never replay stale speech into a stateful detector.
                    self._audio_processed[identity] = (buf_len, speech)

                    gate = self._audio_gates.get(identity)
                    if gate is None:
                        gate = self._new_audio_gate()
                        self._audio_gates[identity] = gate
                    if not gate.ready:
                        if not gate.calibrate_pcm(pending):
                            continue
                        pending = bytes(buf)  # Classifier has not seen calibration audio yet.
                        logger.info(
                            "[%s] adaptive VAD for %s: noise_rms=%.1f start=%.1f stop=%.1f",
                            self.name,
                            identity,
                            gate.noise_rms,
                            gate.start_threshold,
                            gate.stop_threshold,
                        )

                    if pending:
                        speech = gate.is_speech(
                            pcm_rms(pending),
                            speaking=identity in self._speaking_participants,
                            frame_seconds=len(pending) / (SAMPLE_RATE * NUM_CHANNELS * 2),
                            pcm=pending,
                        )
                        self._audio_processed[identity] = (buf_len, speech)
                    if identity in self._audio_overflowed:
                        now = time.monotonic()
                        if speech:
                            self._last_audio_time[identity] = now
                        elif now - self._last_audio_time[identity] >= self._silence_duration:
                            self._audio_overflowed.discard(identity)
                            self._last_audio_time.pop(identity, None)
                        buf.clear()
                        self._audio_processed.pop(identity, None)
                        continue
                    if speech:
                        prefetch = getattr(self, "_early_asr", {}).get(identity)
                        if prefetch is not None:
                            prefetch.discard()
                        # Active speech — update timestamp
                        self._last_audio_time[identity] = time.monotonic()
                        # Emit listening-start on first loud chunk of an utterance
                        if identity not in self._speaking_participants:
                            self._speaking_participants.add(identity)
                            asyncio.create_task(
                                self._publish_agent_event(
                                    "agent:listening-start", {"identity": identity}
                                )
                            )
                        continue

                    # Silent — check if silence has lasted long enough
                    last_time = self._last_audio_time.get(identity)
                    if last_time is None:
                        # Preserve word onsets before the classifier crosses
                        # its threshold; keep idle room audio strictly bounded.
                        del buf[:-PREROLL_BYTES]
                        self._audio_processed[identity] = (len(buf), False)
                        continue

                    elapsed_silence = time.monotonic() - last_time
                    prefetch_silence = getattr(self, "_asr_prefetch_silence", 0.0)
                    if prefetch_silence and elapsed_silence >= prefetch_silence:
                        prefetch = self._early_asr.get(identity)
                        if prefetch is None:
                            prefetch = self._early_asr[identity] = EarlyTranscription(
                                lambda pcm: transcribe_pcm(pcm, SAMPLE_RATE, NUM_CHANNELS),
                                SAMPLE_RATE, NUM_CHANNELS,
                            )
                        prefetch.start(buf, last_time)
                    if elapsed_silence < self._silence_duration:
                        continue

                    # Keep a short tail for quiet word endings. Trimming uses
                    # this platform's configured endpoint duration as well.
                    silence_bytes = int(max(0, self._silence_duration - 0.1) * SAMPLE_RATE * NUM_CHANNELS * 2)
                    logger.info("[%s] voice endpoint: silence=%.3fs target=%.3fs noise_rms=%.1f",
                                identity, elapsed_silence, self._silence_duration, gate.noise_rms)
                    self._flush_utterance(identity, max(0, buf_len - silence_bytes))
        except asyncio.CancelledError:
            return

    def _flush_utterance(self, identity: str, speech_end: int) -> bool:
        """Close a buffered utterance and hand it to STT. Returns True if sent.

        Shared by the silence detector and by ``client:control end-of-turn``.
        A client doing its own endpointing already knows the turn is over;
        making it wait for us to rediscover that costs SILENCE_THRESHOLD_SECONDS
        on every single reply.

        Deliberately synchronous up to the dispatch: the buffer is reset before
        any await, so the silence loop and an inbound control frame racing each
        other cannot transcribe the same audio twice.
        """
        buf = self._audio_buffers.get(identity)
        if not buf or speech_end <= 0 or identity in getattr(self, "_audio_overflowed", ()):
            return False

        duration = speech_end / (SAMPLE_RATE * NUM_CHANNELS * 2)
        if duration < MIN_SPEECH_DURATION:
            # Too short — discard as noise
            prefetch = getattr(self, "_early_asr", {}).get(identity)
            if prefetch is not None:
                prefetch.discard()
            self._audio_buffers[identity] = bytearray()
            getattr(self, "_audio_processed", {}).pop(identity, None)
            self._last_audio_time.pop(identity, None)
            # False alarm — revert the listening-start we sent
            if identity in self._speaking_participants:
                self._speaking_participants.discard(identity)
                asyncio.create_task(
                    self._publish_agent_event("agent:listening-stop", {"identity": identity})
                )
            return False

        # Extract the utterance (speech portion only) and reset
        pcm_data = bytes(buf[:speech_end])
        prefetch = getattr(self, "_early_asr", {}).get(identity)
        gate = getattr(self, "_audio_gates", {}).get(identity)
        prefetched = (prefetch.take(
            pcm_data, self._last_audio_time.get(identity), gate.stop_threshold,
        ) if prefetch is not None and gate is not None else None)
        self._audio_buffers[identity] = bytearray()
        getattr(self, "_audio_processed", {}).pop(identity, None)
        self._last_audio_time.pop(identity, None)
        self._speaking_participants.discard(identity)
        logger.info("[%s] Utterance from %s: %.1fs audio", self.name, identity, duration)
        asyncio.create_task(self._process_voice_input(identity, pcm_data, prefetched=prefetched))
        return True

    async def _process_voice_input(self, identity: str, pcm_data: bytes, *, prefetched=None):
        """Transcribe audio and feed into the agent loop."""
        protocol = self._realtime_protocol
        room = self._room
        if room is None or protocol is None:
            return
        try:
            # Transcribe using hermes STT pipeline. transcribe_audio resolves
            # the model from stt config internally when called with no model
            # arg — same pattern other gateway adapters use.
            item_id = await protocol.speech_stopped(identity)
            transcript, reused = await transcribe_with_prefetch(
                pcm_data, SAMPLE_RATE, NUM_CHANNELS, prefetched=prefetched, transcribe=transcribe_pcm,
            )
            if room is not self._room or protocol is not self._realtime_protocol:
                return
            logger.info("[%s] ASR for %s reused early result: %s", self.name, identity, reused)
            if not transcript:
                logger.info("[%s] Empty transcript from %s, skipping", self.name, identity)
                return

            logger.info("[%s] Transcript from %s: %s", self.name, identity, transcript[:80])

            # Publish the final user transcript so clients can update their UI.
            await protocol.user_transcript(transcript, identity, item_id=item_id)

            # Drain any captured frames into this message so the agent's
            # vision pipeline sees them alongside the transcript.
            media_urls, media_types = self._drain_pending_captures()

            # Build message event
            source = self.build_source(
                chat_id=self._room_name,
                chat_name=self._room_name,
                chat_type="group",
                user_id=identity,
                user_name=identity,
            )

            event = MessageEvent(
                text=transcript,
                message_type=MessageType.VOICE,
                source=source,
                message_id=uuid.uuid4().hex[:12],
                media_urls=media_urls,
                media_types=media_types,
                timestamp=datetime.now(tz=timezone.utc),
                channel_prompt=self._voice_prompt_for_client(identity),
            )

            # Agent is about to invoke the LLM.
            await self._publish_agent_event("agent:thinking-start")
            await self.handle_message(event)
        except Exception as e:
            logger.error("[%s] Error processing voice from %s: %s", self.name, identity, e)

    # -- Inbound data channel + frame capture -------------------------------

    # Realtime conversation events and participant-owned tools use the same
    # topics as api.server Conference. Hermes-only controls stay isolated on
    # one explicitly named extension topic.
    DATA_CHANNEL_EVENTS_TOPIC = "conference.events"
    DATA_CHANNEL_TOOLS_TOPIC = "conference.tools"
    DATA_CHANNEL_EXTENSIONS_TOPIC = "conference.extensions"

    def _on_data_received(
        self,
        packet,
        *,
        receiving_room: Any = None,
        receiving_generation: Optional[int] = None,
    ) -> None:
        """Route inbound data-channel packets.

        Called synchronously by the SDK's event thread; heavy work is
        kicked off as asyncio tasks. Portable tool registration uses
        ``conference.tools``. Hermes-only controls use
        ``conference.extensions``. Anything else is ignored silently.
        """
        participant = getattr(packet, "participant", None)
        participant_identity = (
            getattr(participant, "identity", "") if participant is not None else ""
        )
        if receiving_room is None:
            receiving_room = self._room
        if receiving_generation is None:
            receiving_generation = getattr(self, "_room_generation", 0)
        if (
            receiving_room is not self._room
            or receiving_generation != getattr(self, "_room_generation", 0)
            or receiving_room is None
            or participant_identity not in receiving_room.remote_participants
        ):
            return

        async def dispatch_current(handler):
            # The SDK callback schedules work; the room can change before that
            # task starts. Check ownership again at the execution boundary.
            if self._tool_message_is_current(
                participant_identity, receiving_room, receiving_generation
            ):
                await handler()

        topic = getattr(packet, "topic", None) or ""
        if topic == self.DATA_CHANNEL_EVENTS_TOPIC:
            protocol = getattr(self, "_realtime_protocol", None)
            if not protocol or not participant_identity:
                return
            try:
                asyncio.create_task(
                    dispatch_current(lambda: protocol.handle_client_message(packet.data, participant_identity))
                )
            except RuntimeError:
                pass
            return
        if topic not in (
            self.DATA_CHANNEL_TOOLS_TOPIC,
            self.DATA_CHANNEL_EXTENSIONS_TOPIC,
        ):
            return

        try:
            import json as _json
            msg = _json.loads(packet.data.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            logger.warning(
                "[%s] %s: undecodable payload from %s: %s",
                self.name, topic,
                participant_identity or "?", exc,
            )
            return

        msg_type = msg.get("type", "") if isinstance(msg, dict) else ""
        if not msg_type:
            logger.debug("[%s] %s: payload missing 'type'", self.name, topic)
            return

        if topic == self.DATA_CHANNEL_TOOLS_TOPIC:
            handlers = {
                "conference.tools.register": lambda: self._register_client_tools(
                    msg,
                    participant_identity,
                    receiving_room=receiving_room,
                    receiving_generation=receiving_generation,
                ),
            }
        else:
            handlers = {
                "conference.capture_frame": lambda: self._capture_next_frame(participant_identity),
                "conference.message": lambda: self._handle_client_message(msg, participant_identity),
                "conference.control": lambda: self._handle_client_control(msg, participant_identity),
                HERMES_INPUT_AUDIO_STATE: lambda: self._handle_input_audio_state(
                    msg,
                    participant_identity,
                ),
            }
        handler = handlers.get(msg_type)
        if handler is None:
            logger.debug("[%s] unknown control type %r from %s", self.name, msg_type, participant_identity or "?")
            return

        try:
            asyncio.create_task(dispatch_current(handler))
        except RuntimeError:
            # No running loop (callback fired during teardown). Drop quietly.
            pass

    async def _capture_next_frame(self, identity: str) -> None:
        """Sample the latest video frame from ``identity`` and queue it.

        Only one JPEG per call; the SDK continuously receives video. If the
        participant has no video track subscribed yet,
        emit ``agent:frame-capture-failed`` so the client knows.
        """
        if not PIL_AVAILABLE:
            await self._publish_agent_event(
                "agent:frame-capture-failed",
                {"reason": "pillow-not-installed"},
            )
            return

        stream = self._video_streams.get(identity)
        room = self._room
        if stream is None:
            logger.info("[%s] capture-frame from %s but no video track subscribed", self.name, identity)
            await self._publish_agent_event(
                "agent:frame-capture-failed",
                {"reason": "no-video-track", "identity": identity},
            )
            return

        try:
            # The capacity-one queue supplies the newest buffered frame, or
            # waits for the next one when it is empty.
            frame_event = await asyncio.wait_for(stream.__anext__(), timeout=2.0)
        except (asyncio.TimeoutError, StopAsyncIteration) as exc:
            logger.warning("[%s] capture-frame from %s timed out: %s", self.name, identity, exc)
            await self._publish_agent_event(
                "agent:frame-capture-failed",
                {"reason": "timeout", "identity": identity},
            )
            return

        frame = frame_event.frame
        try:
            jpeg_bytes = await asyncio.to_thread(self._encode_video_frame, frame)
        except Exception as exc:
            logger.error("[%s] frame encode failed for %s: %s", self.name, identity, exc)
            await self._publish_agent_event(
                "agent:frame-capture-failed",
                {"reason": "encode-error", "identity": identity, "detail": str(exc)},
            )
            return

        if room is not self._room or self._video_streams.get(identity) is not stream:
            return
        tmp_dir = os.path.join(tempfile.gettempdir(), "hermes_realtime")
        os.makedirs(tmp_dir, exist_ok=True)
        path = os.path.join(tmp_dir, f"frame_{uuid.uuid4().hex[:12]}.jpg")
        with open(path, "wb") as f:
            f.write(jpeg_bytes)

        self._pending_captures.append((path, "image/jpeg"))
        logger.info(
            "[%s] captured %dx%d frame from %s (%d bytes) — pending=%d",
            self.name, frame.width, frame.height, identity,
            len(jpeg_bytes), len(self._pending_captures),
        )
        await self._publish_agent_event(
            "agent:frame-captured",
            {
                "identity": identity,
                "width": frame.width,
                "height": frame.height,
                "bytes": len(jpeg_bytes),
                "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            },
        )

    @staticmethod
    def _encode_video_frame(frame: Any) -> bytes:
        # Convert directly to RGB so Pillow can ingest the raw buffer without
        # allocating an intermediate RGBA image. JPEG does not carry alpha.
        from livekit.rtc import VideoBufferType

        rgb = frame.convert(VideoBufferType.RGB24)
        img = Image.frombytes("RGB", (rgb.width, rgb.height), bytes(rgb.data))
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=85)
        return buf.getvalue()

    async def _handle_client_message(
        self,
        msg: Dict[str, Any],
        identity: str,
        *,
        publish_transcript: bool = True,
    ) -> None:
        """Inject a typed text message as if it were a transcribed voice utterance.

        Useful for clients that want to text-chat with the agent over the
        LiveKit data channel (no STT needed). Any pending captures attach
        to this message, same as the voice path.
        """
        text = (msg.get("text") or "").strip()
        if not text:
            return

        media_urls, media_types = self._drain_pending_captures()

        source = self.build_source(
            chat_id=self._room_name,
            chat_name=self._room_name,
            chat_type="group",
            user_id=identity or "client",
            user_name=identity or "client",
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=uuid.uuid4().hex[:12],
            media_urls=media_urls,
            media_types=media_types,
            timestamp=datetime.now(tz=timezone.utc),
            channel_prompt=self._voice_prompt_for_client(identity),
        )

        if publish_transcript:
            await self._publish_agent_event(
                "agent:user-transcript",
                {"transcript": text, "final": True, "identity": identity, "source": "text"},
            )
        await self._publish_agent_event("agent:thinking-start")
        await self.handle_message(event)

    async def _handle_client_control(self, msg: Dict[str, Any], identity: str) -> None:
        """Runtime control hooks from the client. Placeholder for now.

        Currently recognized actions:
          - ``pause``  — stop sampling inbound audio until the client resumes.
          - ``resume`` — allow audio sampling once TTS echo suppression ends.
          - ``end-of-turn`` — the client has finished speaking and says so, so
            transcribe what is buffered now instead of waiting for the silence
            detector to reach the same conclusion a second and a half later.
            Scoped to the sender: it flushes only that participant's buffer.
        """
        action = (msg.get("action") or "").strip().lower()
        if action in ("end-of-turn", "end_of_turn"):
            buf = self._audio_buffers.get(identity)
            if self._flush_utterance(identity, len(buf) if buf else 0):
                logger.info("[%s] end-of-turn from %s", self.name, identity)
            else:
                logger.debug("[%s] end-of-turn from %s with nothing buffered", self.name, identity)
        elif action == "pause":
            self._client_paused = True
            logger.info("[%s] paused by client %s", self.name, identity)
        elif action == "resume":
            self._client_paused = False
            logger.info("[%s] resumed by client %s", self.name, identity)
        else:
            logger.debug("[%s] unknown client:control action %r", self.name, action)

    async def _handle_input_audio_state(
        self,
        msg: Dict[str, Any],
        identity: str,
    ) -> None:
        """Apply the shared Hermes mute extension to one participant."""
        muted = msg.get("muted")
        if not isinstance(muted, bool):
            logger.debug("[%s] invalid input audio state from %s", self.name, identity)
            return
        await self._set_input_audio_state(identity, muted)
        await self._publish_typed(
            {
                "type": HERMES_INPUT_AUDIO_STATE_UPDATED,
                "muted": muted,
            },
            identity=identity,
            topic=self.DATA_CHANNEL_EXTENSIONS_TOPIC,
        )

    async def _set_input_audio_state(self, identity: str, muted: bool) -> None:
        getattr(self, "_audio_processed", {}).pop(identity, None)
        getattr(self, "_audio_overflowed", set()).discard(identity)
        muted_inputs = getattr(self, "_muted_inputs", None)
        if muted_inputs is None:
            muted_inputs = self._muted_inputs = set()
        if muted:
            muted_inputs.add(identity)
            buf = self._audio_buffers.get(identity)
            if (
                identity in self._speaking_participants
                and self._flush_utterance(identity, len(buf) if buf else 0)
            ):
                logger.info("[%s] input muted by %s; utterance finalized", self.name, identity)
            else:
                prefetch = getattr(self, "_early_asr", {}).get(identity)
                if prefetch is not None:
                    prefetch.discard()
                self._audio_buffers[identity] = bytearray()
                self._last_audio_time.pop(identity, None)
                self._speaking_participants.discard(identity)
                logger.info("[%s] input muted by %s", self.name, identity)
            return

        muted_inputs.discard(identity)
        prefetch = getattr(self, "_early_asr", {}).get(identity)
        if prefetch is not None:
            prefetch.discard()
        self._audio_buffers[identity] = bytearray()
        self._last_audio_time.pop(identity, None)
        self._speaking_participants.discard(identity)
        audio_gates = getattr(self, "_audio_gates", None)
        if audio_gates is None:
            audio_gates = self._audio_gates = {}
        audio_gates[identity] = self._new_audio_gate()
        logger.info("[%s] input unmuted by %s; adaptive VAD reset", self.name, identity)

    # -- Remote tools (client-registered) -----------------------------------

    async def _publish_typed(
        self,
        msg: Dict[str, Any],
        *,
        identity: Optional[str] = None,
        topic: str = "",
    ) -> bool:
        """Publish a flat-envelope JSON message; optionally target one participant.

        Unlike _publish_agent_event (which wraps payload in {type, payload}),
        the remote-tool protocol is flat by spec — every field at top level.
        """
        if not self._room:
            return False
        import json as _json
        try:
            data = _json.dumps(msg).encode("utf-8")
            dest = [identity] if identity else []
            await self._room.local_participant.publish_data(
                data, reliable=True, topic=topic, destination_identities=dest,
            )
            return True
        except Exception as exc:
            logger.debug("[%s] typed publish failed (%s): %s", self.name, msg.get("type"), exc)
            return False

    def _ensure_binary_state(self) -> None:
        if not hasattr(self, "_binary_topics"):
            self._binary_topics = set()
            self._binary_transfers = {}
            self._room_generation = 1
            self._room_replacement_started = False
        if not hasattr(self, "_binary_ignored_drains"):
            self._binary_ignored_drains = set()
            self._binary_replacement_scheduled = False

    @staticmethod
    def _binary_error(code: str) -> RuntimeError:
        return RuntimeError(f"binary tool result failed: {code}")

    def _unregister_binary_handler(self, transfer: _BinaryTransfer) -> None:
        if not transfer.handler_registered:
            return
        transfer.handler_registered = False
        try:
            transfer.room.unregister_byte_stream_handler(transfer.reference.topic)
        except Exception:
            pass

    def _cleanup_binary_transfer(self, transfer: _BinaryTransfer) -> None:
        self._unregister_binary_handler(transfer)
        transfer.buffer.clear()
        release_stream(transfer.reference, self._binary_topics)
        current = self._binary_transfers.get(transfer.reference.topic)
        if current is transfer:
            self._binary_transfers.pop(transfer.reference.topic, None)

    def _fail_binary_transfer(
        self,
        transfer: _BinaryTransfer,
        code: str,
        *,
        cancel_waiter: bool = False,
    ) -> None:
        if transfer.terminal:
            return
        transfer.terminal = True
        transfer.buffer.clear()
        self._unregister_binary_handler(transfer)
        if not transfer.result.done():
            if cancel_waiter:
                transfer.result.cancel()
            else:
                transfer.result.set_exception(self._binary_error(code))

    def _fail_binary_owner(self, identity: str) -> None:
        self._ensure_binary_state()
        for transfer in list(self._binary_transfers.values()):
            if transfer.reference.owner_identity != identity:
                continue
            self._fail_binary_transfer(transfer, "owner_disconnected")
            self._schedule_binary_terminal_cleanup(transfer, send_cancel=False)

    def _fail_binary_generation(self, generation: int, code: str) -> None:
        self._ensure_binary_state()
        current_task = asyncio.current_task()
        for transfer in list(self._binary_transfers.values()):
            if transfer.generation != generation:
                continue
            self._fail_binary_transfer(transfer, code)
            for task in (transfer.reader_task, transfer.drain_task):
                if task is not None and task is not current_task and not task.done():
                    task.cancel()
            self._cleanup_binary_transfer(transfer)
        if generation == self._room_generation:
            for task in list(self._binary_ignored_drains):
                if task is not current_task and not task.done():
                    task.cancel()
            self._binary_ignored_drains.clear()

    def _schedule_binary_room_replacement(self, generation: int) -> None:
        if (
            generation != self._room_generation
            or self._room_replacement_started
            or self._binary_replacement_scheduled
        ):
            return
        self._binary_replacement_scheduled = True
        asyncio.create_task(self._replace_binary_room_generation(generation))

    def _track_unclaimed_reader(self, reader: Any, generation: int) -> None:
        self._ensure_binary_state()
        if generation != self._room_generation:
            return
        self._binary_ignored_drains = {
            task for task in self._binary_ignored_drains if not task.done()
        }
        if len(self._binary_ignored_drains) >= MAX_IGNORED_BINARY_DRAINS:
            self._schedule_binary_room_replacement(generation)
            return
        task = asyncio.create_task(self._drain_unclaimed_reader(reader, generation))
        self._binary_ignored_drains.add(task)
        task.add_done_callback(self._binary_ignored_drains.discard)

    def _schedule_binary_terminal_cleanup(
        self, transfer: _BinaryTransfer, *, send_cancel: bool
    ) -> None:
        if transfer.reader_task is None:
            self._cleanup_binary_transfer(transfer)
            return
        if transfer.drain_task is not None:
            return
        transfer.drain_task = asyncio.create_task(
            self._finish_binary_terminal(transfer, send_cancel=send_cancel)
        )

    async def _publish_binary_control(
        self, transfer: _BinaryTransfer, message: Dict[str, Any]
    ) -> bool:
        """Publish only through the exact room generation that owns a transfer."""
        if (
            transfer.room is not self._room
            or transfer.generation != self._room_generation
        ):
            return False
        import json as _json

        try:
            await transfer.room.local_participant.publish_data(
                _json.dumps(message).encode("utf-8"),
                reliable=True,
                topic=self.DATA_CHANNEL_TOOLS_TOPIC,
                destination_identities=[transfer.reference.owner_identity],
            )
            return (
                transfer.room is self._room
                and transfer.generation == self._room_generation
            )
        except Exception:
            return False

    async def _finish_binary_terminal(
        self, transfer: _BinaryTransfer, *, send_cancel: bool
    ) -> None:
        try:
            if send_cancel and transfer.room is self._room:
                await self._publish_binary_control(
                    transfer, stream_cancel_message(transfer.reference)
                )
            try:
                await asyncio.wait_for(
                    transfer.trailer_received.wait(), timeout=DRAIN_TIMEOUT_SEC
                )
            except asyncio.TimeoutError:
                await self._replace_binary_room_generation(transfer.generation)
                return
            self._cleanup_binary_transfer(transfer)
        except asyncio.CancelledError:
            return

    async def _replace_binary_room_generation(self, generation: int) -> None:
        self._ensure_binary_state()
        if generation != self._room_generation or self._room_replacement_started:
            return
        self._room_replacement_started = True
        room, self._room = self._room, None
        self._fail_binary_generation(generation, "room_replaced")
        self._cleanup_all_client_tools()
        await self._close_capture_streams()
        self._discard_pending_captures()
        self._audio_source = None
        self._local_track = None
        if room is not None:
            self._graceful_leave = True
            try:
                await self._release_room(room, why="binary-drain-timeout")
            finally:
                self._graceful_leave = False
        if self._running:
            joined = await self._join_room()
            if not joined and (self._connect_task is None or self._connect_task.done()):
                self._connect_task = asyncio.create_task(self._reconnect_loop())

    async def _consume_binary_stream(
        self,
        transfer: _BinaryTransfer,
        reader: Any,
        sender_identity: str,
    ) -> None:
        try:
            info = reader.info
            validate_stream_header(
                transfer.reference,
                sender_identity=sender_identity,
                stream_id=info.stream_id,
                topic=info.topic,
                mime_type=info.mime_type,
                total_size=info.size,
            )
        except BinaryResultProtocolError as exc:
            self._fail_binary_transfer(transfer, exc.code)
            self._schedule_binary_terminal_cleanup(transfer, send_cancel=True)
        except Exception:
            self._fail_binary_transfer(transfer, "header_mismatch")
            self._schedule_binary_terminal_cleanup(transfer, send_cancel=True)

        try:
            async for chunk in reader:
                if transfer.terminal:
                    continue
                try:
                    bounded_next_size(
                        transfer.reference, len(transfer.buffer), len(chunk)
                    )
                except BinaryResultProtocolError as exc:
                    self._fail_binary_transfer(transfer, exc.code)
                    self._schedule_binary_terminal_cleanup(transfer, send_cancel=True)
                    continue
                transfer.buffer.extend(chunk)
        except asyncio.CancelledError:
            return
        except Exception:
            self._fail_binary_transfer(transfer, "transfer_incomplete")
            self._schedule_binary_terminal_cleanup(transfer, send_cancel=True)
            return

        transfer.trailer_received.set()
        if transfer.terminal:
            self._cleanup_binary_transfer(transfer)
            current = asyncio.current_task()
            if (
                transfer.drain_task is not None
                and transfer.drain_task is not current
                and not transfer.drain_task.done()
            ):
                transfer.drain_task.cancel()
            return
        try:
            validate_completed_size(transfer.reference, len(transfer.buffer))
        except BinaryResultProtocolError as exc:
            self._fail_binary_transfer(transfer, exc.code)
            self._schedule_binary_terminal_cleanup(transfer, send_cancel=False)
            return
        payload = bytes(transfer.buffer)
        transfer.buffer.clear()
        if not transfer.result.done():
            transfer.result.set_result(payload)

    def _on_binary_stream(
        self,
        transfer: _BinaryTransfer,
        reader: Any,
        sender_identity: str,
    ) -> None:
        if (
            transfer.generation != self._room_generation
            or self._binary_transfers.get(transfer.reference.topic) is not transfer
        ):
            self._track_unclaimed_reader(reader, transfer.generation)
            return
        if sender_identity != transfer.reference.owner_identity:
            self._track_unclaimed_reader(reader, transfer.generation)
            return
        if transfer.reader_task is not None:
            self._fail_binary_transfer(transfer, "stream_collision")
            self._schedule_binary_terminal_cleanup(transfer, send_cancel=True)
            self._track_unclaimed_reader(reader, transfer.generation)
            return
        transfer.reader_task = asyncio.create_task(
            self._consume_binary_stream(transfer, reader, sender_identity)
        )

    async def _drain_unclaimed_reader(self, reader: Any, generation: int) -> None:
        try:
            async def drain() -> None:
                async for _chunk in reader:
                    pass

            await asyncio.wait_for(drain(), timeout=DRAIN_TIMEOUT_SEC)
        except asyncio.TimeoutError:
            await self._replace_binary_room_generation(generation)
        except (asyncio.CancelledError, Exception):
            return

    async def _receive_binary_result(
        self,
        payload: str,
        *,
        owner_identity: str,
        room: Any,
        generation: int,
    ) -> Any:
        self._ensure_binary_state()
        try:
            reference = parse_reference(
                payload,
                rpc_owner_identity=owner_identity,
                configured_timeout_sec=self._tool_call_timeout,
            )
            if len(self._binary_transfers) >= MAX_PENDING_BINARY_RESULTS:
                raise BinaryResultProtocolError("too_many_pending")
            reserve_stream(reference, self._binary_topics)
        except BinaryResultProtocolError as exc:
            raise self._binary_error(exc.code) from None

        loop = asyncio.get_running_loop()
        transfer = _BinaryTransfer(
            reference=reference,
            room=room,
            generation=generation,
            result=loop.create_future(),
        )
        self._binary_transfers[reference.topic] = transfer
        try:
            room.register_byte_stream_handler(
                reference.topic,
                lambda reader, identity: self._on_binary_stream(
                    transfer, reader, identity
                ),
            )
            transfer.handler_registered = True
        except Exception:
            self._cleanup_binary_transfer(transfer)
            raise self._binary_error("handler_registration_failed") from None

        deadline = loop.time() + reference.transfer_timeout_sec
        try:
            ready = await asyncio.wait_for(
                self._publish_binary_control(transfer, stream_ready_message(reference)),
                timeout=max(0.0, deadline - loop.time()),
            )
            current_task = asyncio.current_task()
            if current_task is not None and current_task.cancelling():
                raise asyncio.CancelledError
            if not ready or room is not self._room or generation != self._room_generation:
                raise self._binary_error("room_replaced")
            payload_bytes = await asyncio.wait_for(
                asyncio.shield(transfer.result),
                timeout=max(0.0, deadline - loop.time()),
            )
            result = format_completed_result(reference, payload_bytes)
            self._cleanup_binary_transfer(transfer)
            return result
        except asyncio.CancelledError:
            self._fail_binary_transfer(
                transfer, "transfer_cancelled", cancel_waiter=True
            )
            self._schedule_binary_terminal_cleanup(transfer, send_cancel=True)
            raise
        except asyncio.TimeoutError:
            self._fail_binary_transfer(
                transfer, "transfer_timeout", cancel_waiter=True
            )
            self._schedule_binary_terminal_cleanup(transfer, send_cancel=True)
            raise self._binary_error("transfer_timeout") from None
        except Exception:
            if not transfer.terminal:
                self._fail_binary_transfer(transfer, "room_replaced")
                self._schedule_binary_terminal_cleanup(transfer, send_cancel=False)
            raise

    async def _register_client_tools(
        self,
        msg: Dict[str, Any],
        identity: str,
        *,
        receiving_room: Any = None,
        receiving_generation: Optional[int] = None,
    ) -> None:
        """Replace one participant's tools using the portable Conference envelope."""
        if receiving_room is None:
            receiving_room = self._room
        if receiving_generation is None:
            receiving_generation = self._room_generation
        if not self._tool_message_is_current(
            identity, receiving_room, receiving_generation
        ):
            return

        values = msg.get("tools")
        try:
            converted = parse_function_tools(
                values, nested=True, allow_dotted_names=True
            )
        except ToolDefinitionError:
            converted = []
            accepted = False
        else:
            accepted = True
        if accepted:
            self._cleanup_client_tools(identity)
            for definition in converted:
                if not await self._register_client_tool(
                    {
                        "name": definition.name,
                        "description": definition.description,
                        "input_schema": definition.parameters,
                    },
                    identity,
                    receiving_room=receiving_room,
                    receiving_generation=receiving_generation,
                ):
                    accepted = False
                    self._cleanup_client_tools(identity)
                    break

        await self._publish_typed(
            {
                "type": (
                    "conference.tools.registered"
                    if accepted
                    else "conference.tools.rejected"
                )
            },
            identity=identity,
            topic=self.DATA_CHANNEL_TOOLS_TOPIC,
        )

    async def _register_client_tool(
        self,
        msg: Dict[str, Any],
        identity: str,
        *,
        receiving_room: Any = None,
        receiving_generation: Optional[int] = None,
    ) -> bool:
        if receiving_room is None:
            receiving_room = self._room
        if receiving_generation is None:
            receiving_generation = self._room_generation
        if not self._tool_message_is_current(
            identity, receiving_room, receiving_generation
        ):
            return False
        raw_name = msg.get("name")
        name = raw_name if isinstance(raw_name, str) else ""
        description = msg.get("description") or ""
        input_schema = msg.get("input_schema")

        if not self._valid_tool_owner_identity(identity):
            self._audit_tool("registration", identity, name, 0, "denied")
            return False

        if not valid_tool_name(name):
            self._audit_tool("registration", identity, name, 0, "denied")
            return False

        if not isinstance(input_schema, dict) or input_schema.get("type") != "object":
            self._audit_tool("registration", identity, name, 0, "denied")
            return False

        decision = self._tool_policy_decision(identity, name)
        if not decision.allowed:
            self._audit_tool("registration", identity, name, decision.tier, "denied")
            return False

        try:
            from tools.registry import registry
        except Exception:
            logger.error("[%s] tool registry unavailable", self.name)
            self._audit_tool("registration", identity, name, decision.tier, "error")
            return False

        self._ensure_client_tool_maps()
        registry_name = self._scoped_tool_name(identity, name)
        owner = self._tool_owners.get(registry_name)
        method = self._tool_methods.get(registry_name)
        if owner is not None and (owner, method) != (identity, name):
            self._audit_tool("registration", identity, name, decision.tier, "denied")
            return False
        existing = registry.get_entry(registry_name)
        if existing is not None and owner is None:
            self._audit_tool("registration", identity, name, decision.tier, "denied")
            return False

        handler = self._build_tool_handler(identity, name)
        # The registry's `schema` is the OpenAI function-envelope shape
        # (`{name, description, parameters}`), not a bare JSON Schema. Wrap
        # the client-supplied input_schema accordingly.
        definition = FunctionToolDefinition(name, str(description), input_schema)
        registry_schema = definition.registry_schema(registry_name)
        try:
            # override=True permits the exact owner/method pair to re-register
            # after reconnect. Collision checks above protect every other slot.
            registry.register(
                name=registry_name,
                toolset=TOOLSET_NAME,
                schema=registry_schema,
                handler=handler,
                is_async=True,
                description=description,
                override=True,
            )
        except Exception:
            logger.warning("[%s] tool register %r failed", self.name, name)
            self._audit_tool("registration", identity, name, decision.tier, "error")
            return False

        self._client_tools.setdefault(identity, set()).add(registry_name)
        self._tool_owners[registry_name] = identity
        self._tool_methods[registry_name] = name
        self._audit_tool("registration", identity, name, decision.tier, "accepted")
        logger.info(
            "[%s] client %s registered tool %r as %r",
            self.name,
            identity,
            name,
            registry_name,
        )
        return True

    def _build_tool_handler(self, owner_identity: str, registered_name: str):
        """Return an async fn that hermes will call when the LLM picks this tool.

        Signature matches the hermes ToolRegistry contract:
        ``handler(args_dict, **kwargs)`` — first positional is the LLM-supplied
        arguments object, kwargs are framework extras we pass through.
        """

        self._ensure_binary_state()
        revisions = self.__dict__.setdefault("_client_tool_revisions", {})
        revision = object()
        revisions[(owner_identity, registered_name)] = revision
        owner_room = self._room
        owner_generation = getattr(self, "_room_generation", 0)
        owner_participant = owner_room.remote_participants.get(owner_identity) if owner_room else None
        try:
            owner_loop = asyncio.get_running_loop()
        except RuntimeError:
            owner_loop = None

        async def invoke(
            args: dict[str, Any] | None, invocation_session_key: str
        ) -> Any:
            import json as _json

            if (self._room is not owner_room or getattr(self, "_room_generation", 0) != owner_generation
                    or owner_room is None or owner_room.remote_participants.get(owner_identity) is not owner_participant
                    or revisions.get((owner_identity, registered_name)) is not revision):
                raise RuntimeError("remote tool owner was replaced")

            decision = self._tool_policy_decision(owner_identity, registered_name)
            if not decision.allowed:
                self._audit_tool(
                    "invocation", owner_identity, registered_name, decision.tier, "denied"
                )
                raise RuntimeError("remote tool invocation denied by policy")
            task = asyncio.current_task()
            pending = self.__dict__.setdefault("_pending_native_rpc_tasks", {})
            if task is not None and invocation_session_key:
                pending.setdefault(invocation_session_key, set()).add(task)
            try:
                arguments: Dict[str, Any] = dict(args or {})
                if not self._room or owner_identity not in self._room.remote_participants:
                    raise RuntimeError(
                        f"client {owner_identity!r} who registered {registered_name!r} is not connected"
                    )
                room = self._room
                self._ensure_binary_state()
                generation = self._room_generation
                result = await room.local_participant.perform_rpc(
                    destination_identity=owner_identity,
                    method=registered_name,
                    payload=_json.dumps(arguments),
                    response_timeout=self._tool_call_timeout,
                )
                if room is not self._room or generation != self._room_generation:
                    raise RuntimeError("remote tool room was replaced")
                if room.remote_participants.get(owner_identity) is not owner_participant:
                    raise RuntimeError("remote tool owner was replaced")
                if revisions.get((owner_identity, registered_name)) is not revision:
                    raise RuntimeError("remote tool was disabled or replaced")
                decoded = _json.loads(result)
                decoded = decode_client_image(decoded)
                if isinstance(decoded, dict) and decoded.get("type") == REFERENCE_TYPE:
                    decoded = await self._receive_binary_result(
                        result,
                        owner_identity=owner_identity,
                        room=room,
                        generation=generation,
                    )
                    if room.remote_participants.get(owner_identity) is not owner_participant:
                        raise RuntimeError("remote tool owner was replaced")
                if revisions.get((owner_identity, registered_name)) is not revision:
                    raise RuntimeError("remote tool was disabled or replaced")
            except asyncio.CancelledError:
                self._audit_tool(
                    "invocation", owner_identity, registered_name, decision.tier, "cancelled"
                )
                self._audit_tool(
                    "cancellation", owner_identity, registered_name, decision.tier, "cancelled"
                )
                raise
            except Exception:
                self._audit_tool(
                    "invocation", owner_identity, registered_name, decision.tier, "error"
                )
                raise
            finally:
                if task is not None and invocation_session_key:
                    tasks = pending.get(invocation_session_key)
                    if tasks is not None:
                        tasks.discard(task)
                        if not tasks:
                            pending.pop(invocation_session_key, None)
            self._audit_tool(
                "invocation", owner_identity, registered_name, decision.tier, "success"
            )
            if (
                isinstance(decoded, dict)
                and decoded.get("_multimodal") is True
                and isinstance(decoded.get("content"), list)
            ):
                return decoded
            # Hermes accepts normal tool results as strings; keep the client
            # result structured by returning validated JSON text. The only
            # supported dictionary result is the multimodal envelope above.
            return _json.dumps(decoded, ensure_ascii=False, separators=(",", ":"))

        async def proxy(args: dict[str, Any] | None = None, **kwargs: Any) -> Any:
            # Hermes dispatches async tools from worker loops. Binary stream
            # callbacks and room cleanup run on the gateway loop; their futures
            # and transfer maps must have that same owner.
            nonlocal owner_loop
            invocation_session_key = self._tool_invocation_session_key(
                kwargs.get("session_id")
            )
            running_loop = asyncio.get_running_loop()
            if owner_loop is None:
                owner_loop = running_loop
            if running_loop is owner_loop:
                return await invoke(args, invocation_session_key)
            result = asyncio.run_coroutine_threadsafe(
                invoke(args, invocation_session_key), owner_loop
            )
            return await asyncio.wrap_future(result)

        return proxy

    @staticmethod
    def _tool_invocation_session_key(session_id: Any) -> str:
        """Resolve Hermes's persisted session ID to the gateway routing key."""
        if not isinstance(session_id, str) or not session_id:
            return ""
        return resolve_gateway_session_key(session_id) or session_id

    def cancel_native_rpc_for_session(self, session_key: str) -> int:
        """Cancel native RPC tasks owned by one interrupted Hermes session."""
        tasks = list(
            self.__dict__.setdefault("_pending_native_rpc_tasks", {}).get(
                session_key, ()
            )
        )
        cancelled = 0
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        for task in tasks:
            if task.done():
                continue
            loop = task.get_loop()
            if loop is running_loop:
                task.cancel()
            elif loop.is_running():
                loop.call_soon_threadsafe(task.cancel)
            else:
                continue
            cancelled += 1
        return cancelled

    @staticmethod
    def _valid_tool_owner_identity(identity: str) -> bool:
        """Return whether a participant identity has one unambiguous encoding."""
        return valid_participant_identity(identity)

    def _ensure_tool_safety_state(self) -> None:
        """Initialize deny-by-default safety state for lightweight adapters."""
        if not hasattr(self, "_tool_policy"):
            self._tool_policy = ToolPolicy()
        if not hasattr(self, "_tool_audit"):
            self._tool_audit = ToolAuditLog()

    def _tool_policy_decision(
        self, participant_identity: str, tool_name: str
    ) -> PolicyDecision:
        self._ensure_tool_safety_state()
        decision = self._tool_policy.decide(participant_identity, tool_name)
        # Only an API-authenticated live owner can extend its own declared client
        # tools. Explicit operator denials/expired consent remain authoritative.
        if (decision.reason == "not-allowlisted" and not getattr(self, "_tool_policy_invalid", False)
                and self._api_client_admission(participant_identity)):
            decision = PolicyDecision(True, 2, "consent-valid")
        self._audit_tool(
            "policy",
            participant_identity,
            tool_name,
            decision.tier,
            decision.reason,
        )
        return decision

    def _api_client_admission(self, identity: str) -> ClientAdmission | None:
        room = getattr(self, "_room", None)
        participant = room.remote_participants.get(identity) if room else None
        if participant is None:
            return None
        metadata = getattr(participant, "metadata", "")
        generation = getattr(self, "_room_generation", 0)
        cache = self.__dict__.setdefault("_api_client_admissions", {})
        prior = cache.get(identity)
        # Like JWT room admission, expiry gates a new connection. The exact live
        # participant retains its lease; metadata or room/owner changes recheck it.
        if prior and prior[0] is room and prior[1] == generation and prior[2] is participant and prior[3] == metadata:
            return prior[4]
        admission = verify_client_admission(metadata, room=getattr(self, "_room_name", ""),
            participant=identity, key=getattr(self, "_api_client_key", ""))
        if admission:
            cache[identity] = (room, generation, participant, metadata, admission)
        else:
            cache.pop(identity, None)
        return admission

    def _voice_prompt_for_client(self, identity: str) -> str | None:
        protocol = getattr(self, "_realtime_protocol", None)
        instructions = getattr(protocol, "instructions", "")
        admission = self._api_client_admission(identity)
        prompts = [admission.voice.base if admission else "", instructions]
        if admission and getattr(protocol, "tool_choice", "auto") != "none":
            if any(owner == identity and self._tool_methods.get(name) == "display_card"
                   for name, owner in getattr(self, "_tool_owners", {}).items()):
                prompts.append(admission.voice.display)
        return "\n\n".join(prompt for prompt in prompts if prompt) or None

    def _audit_tool(
        self,
        event: str,
        participant_identity: str,
        tool_name: str,
        tier: int,
        outcome: str,
    ) -> None:
        self._ensure_tool_safety_state()
        self._tool_audit.append(
            event, participant_identity, tool_name, tier, outcome
        )

    def _tool_audit_snapshot(self) -> tuple[dict[str, object], ...]:
        """Return a detached read-only view for diagnostics and tests."""
        self._ensure_tool_safety_state()
        return self._tool_audit.snapshot()

    def _tool_message_is_current(
        self,
        identity: str,
        receiving_room: Any,
        receiving_generation: int,
    ) -> bool:
        """Reject delayed tool mutations from departed clients or old rooms."""
        return bool(
            receiving_room is not None
            and receiving_room is self._room
            and receiving_generation == self._room_generation
            and identity in receiving_room.remote_participants
        )

    @staticmethod
    def _scoped_tool_name(identity: str, advertised_name: str) -> str:
        """Build the bounded Hermes registry name for one advertised RPC method."""
        identity_bytes = identity.encode("utf-8")
        digest = hashlib.sha256(
            len(identity_bytes).to_bytes(2, "big")
            + identity_bytes
            + advertised_name.encode("ascii")
        ).hexdigest()[:16]
        readable_limit = 64 - len(SCOPED_TOOL_PREFIX) - len(digest) - 1
        readable_name = advertised_name.replace(".", "_")
        return f"{SCOPED_TOOL_PREFIX}{digest}_{readable_name[:readable_limit]}"

    def _ensure_client_tool_maps(self) -> None:
        """Initialize maps for lightweight contract-test adapter instances."""
        if not hasattr(self, "_client_tools"):
            self._client_tools = {}
        if not hasattr(self, "_tool_owners"):
            self._tool_owners = {}
        if not hasattr(self, "_tool_methods"):
            self._tool_methods = {}

    def _deregister_tool(self, name: str, identity: str) -> None:
        """Remove a single tool from the hermes registry and our maps."""
        self._ensure_client_tool_maps()
        self.__dict__.setdefault("_client_tool_revisions", {}).pop((identity, self._tool_methods.get(name)), None)
        try:
            from tools.registry import registry
            registry.deregister(name)
        except Exception:
            logger.debug("[%s] tool deregister %r failed", self.name, name)
        self._tool_owners.pop(name, None)
        self._tool_methods.pop(name, None)
        tools = self._client_tools.get(identity)
        if tools:
            tools.discard(name)
            if not tools:
                self._client_tools.pop(identity, None)

    def _cleanup_client_tools(self, identity: str) -> None:
        """Deregister all tools owned by ``identity``."""
        revisions = self.__dict__.setdefault("_client_tool_revisions", {})
        for key in list(revisions):
            if key[0] == identity:
                revisions.pop(key, None)
        self._ensure_client_tool_maps()
        for name in list(self._client_tools.get(identity, set())):
            method = self._tool_methods.get(name, "")
            decision = self._tool_policy_decision(identity, method)
            self._audit_tool(
                "owner_disconnect", identity, method, decision.tier, "removed"
            )
            self._deregister_tool(name, identity)

    def _cleanup_all_client_tools(self) -> None:
        """Deregister every client-offered tool."""
        self._ensure_client_tool_maps()
        for identity in list(self._client_tools.keys()):
            self._cleanup_client_tools(identity)
        self._client_tools.clear()
        self._tool_owners.clear()
        self._tool_methods.clear()
        getattr(self, "_api_client_admissions", {}).clear()

    def _drain_pending_captures(self) -> tuple[list[str], list[str]]:
        """Pop all buffered frame paths into parallel (urls, types) lists.

        Temp files are NOT unlinked here — the hermes agent loop reads them
        after handle_message returns (the dispatch is fire-and-forget). The
        files live under <tempdir>/hermes_realtime/ and are cleaned up on
        disconnect; OS tempdir housekeeping handles anything we miss.
        """
        urls: list[str] = []
        types: list[str] = []
        pending, self._pending_captures = self._pending_captures, []
        for path, mime in pending:
            urls.append(path)
            types.append(mime)
        return urls, types

    # -- Outbound messaging -------------------------------------------------

    async def handle_message(self, event: MessageEvent) -> None:
        """Defer background completion wakes until the room is quiet.

        Normal user turns retain the base adapter's interruption semantics.
        Only internal events (such as a Kanban completion wake) are delayed,
        and each event is dispatched exactly once if the room remains joined.
        """
        if event.internal and getattr(self, "_speaking_participants", set()):
            task = asyncio.create_task(self._deliver_internal_when_silent(event))
            pending = getattr(self, "_deferred_internal_wakes", None)
            if pending is None:
                pending = self._deferred_internal_wakes = set()
            pending.add(task)
            task.add_done_callback(pending.discard)
            return
        await super().handle_message(event)

    async def _deliver_internal_when_silent(self, event: MessageEvent) -> None:
        while self._room is not None and getattr(self, "_speaking_participants", set()):
            await asyncio.sleep(0.05)
        if self._room is not None:
            await super().handle_message(event)

    async def _publish_agent_event(
        self, event_type: str, payload: Optional[Dict[str, Any]] = None
    ) -> None:
        """Translate Hermes lifecycle hooks into the shared Realtime contract."""
        if not self._room:
            return
        protocol = getattr(self, "_realtime_protocol", None)
        event_payload = payload or {}
        if protocol:
            identity = str(event_payload.get("identity") or "client")
            if event_type == "agent:listening-start":
                await protocol.speech_started(identity)
                return
            if event_type == "agent:listening-stop":
                await protocol.speech_stopped(identity)
                return
            if event_type == "agent:user-transcript":
                transcript = event_payload.get("transcript")
                if event_payload.get("final") is True and isinstance(transcript, str):
                    await protocol.user_transcript(transcript, identity)
                return
            if event_type == "agent:thinking-start":
                await protocol.response_started()
                return
            if event_type == "agent:agent-transcript":
                transcript = event_payload.get("transcript")
                if isinstance(transcript, str):
                    await protocol.assistant_transcript(transcript)
                return
            if event_type == "agent:speaking-start":
                await protocol.output_started()
                return
            if event_type == "agent:speaking-stop":
                await protocol.output_playback_stopped()
                return

        # Video and native-tool lifecycle messages remain bounded Conference
        # extensions. They no longer share the conversation event topic.
        try:
            await self._publish_typed(
                {"type": event_type, "payload": event_payload},
                topic=self.DATA_CHANNEL_EXTENSIONS_TOPIC,
            )
        except Exception as e:
            logger.debug("[%s] agent event publish failed (%s): %s", self.name, event_type, e)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send text via data channel (best-effort for connected web clients)."""
        if not self._room:
            return SendResult(success=False, error="Not connected to room")

        try:
            await self._publish_agent_event(
                "agent:agent-transcript", {"transcript": content, "final": True}
            )
            if self._realtime_protocol:
                await self._realtime_protocol.text_delivery_complete()
            return SendResult(success=True, message_id=uuid.uuid4().hex[:12])
        except Exception as e:
            logger.debug("[%s] Data channel send failed (non-critical): %s", self.name, e)
            # Not a failure — voice is the primary channel
            return SendResult(success=True, message_id=uuid.uuid4().hex[:12])

    async def play_tts(
        self,
        chat_id: str,
        audio_path: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        caption: Optional[str] = None,
        **kwargs: Any,
    ) -> SendResult:
        """Play TTS audio into the LiveKit room via the published audio track.

        ``caption`` is the Telegram-style "attach the text to the voice message"
        hook; the gateway passes it on every platform. LiveKit delivers reply
        text through Realtime transcript events, so it is accepted and ignored —
        returning without consuming it leaves the caller's
        ``_tts_caption_delivered`` False, which is what sends the text. The
        ``**kwargs`` keeps this override compatible with the base signature,
        which takes ``**kwargs`` and can grow new keywords at any time.
        """
        if not self._audio_source or not self._room:
            return SendResult(success=False, error="Not connected to room")

        source = self._audio_source
        room = self._room
        try:
            # Pause capture to avoid echo
            self._paused = True

            # Decode audio file to raw PCM using ffmpeg
            pcm_data = await asyncio.to_thread(
                self._decode_audio_to_pcm, audio_path
            )
            if source is not self._audio_source or room is not self._room:
                return SendResult(success=False, error="Room changed during audio decode")
            if not pcm_data:
                self._paused = False
                return SendResult(success=False, error="Failed to decode audio")

            # Publish PCM frames to the audio source
            # LiveKit expects frames of a specific size
            samples_per_frame = SAMPLE_RATE // 50  # 20ms frames
            bytes_per_frame = samples_per_frame * NUM_CHANNELS * 2  # 16-bit

            await self._publish_agent_event("agent:speaking-start")

            offset = 0
            while offset < len(pcm_data):
                if source is not self._audio_source or room is not self._room:
                    source.clear_queue()
                    return SendResult(success=False, error="Room changed during playback")
                chunk = pcm_data[offset:offset + bytes_per_frame]
                if len(chunk) < bytes_per_frame:
                    # Pad the last frame with silence
                    chunk = chunk + b"\x00" * (bytes_per_frame - len(chunk))

                frame = rtc.AudioFrame(
                    data=chunk,
                    sample_rate=SAMPLE_RATE,
                    num_channels=NUM_CHANNELS,
                    samples_per_channel=samples_per_frame,
                )
                await source.capture_frame(frame)
                offset += bytes_per_frame

            # Brief pause after playback before resuming capture
            await source.wait_for_playout()
            await asyncio.sleep(0.3)
            if source is not self._audio_source or room is not self._room:
                return SendResult(success=False, error="Room changed during playback")
            await self._publish_agent_event("agent:speaking-stop")

            return SendResult(success=True, message_id=uuid.uuid4().hex[:12])
        except asyncio.CancelledError:
            source.clear_queue()
            raise
        except Exception as e:
            source.clear_queue()
            if self._realtime_protocol and room is self._room:
                await self._realtime_protocol.response_failed()
            logger.error("[%s] TTS playback error: %s", self.name, e)
            return SendResult(success=False, error=str(e))
        finally:
            if source is self._audio_source:
                self._paused = False

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        """Deliver voice attachments on the room's native audio track."""
        return await self.play_tts(
            chat_id=chat_id,
            audio_path=audio_path,
            caption=caption,
            reply_to=reply_to,
            metadata=metadata,
            **kwargs,
        )

    @staticmethod
    def _decode_audio_to_pcm(audio_path: str) -> Optional[bytes]:
        """Decode an audio file to raw 16-bit PCM using ffmpeg."""
        try:
            result = subprocess.run(
                [
                    "ffmpeg", "-i", audio_path,
                    "-f", "s16le",        # raw 16-bit little-endian PCM
                    "-acodec", "pcm_s16le",
                    "-ar", str(SAMPLE_RATE),
                    "-ac", str(NUM_CHANNELS),
                    "-loglevel", "error",
                    "pipe:1",
                ],
                capture_output=True,
                timeout=30,
            )
            if result.returncode != 0:
                logger.warning("ffmpeg decode failed: %s", result.stderr.decode()[:200])
                return None
            return result.stdout
        except FileNotFoundError:
            logger.warning("ffmpeg not found — required for LiveKit TTS playback")
            return None
        except Exception as e:
            logger.warning("Audio decode error: %s", e)
            return None

    def prepare_tts_text(self, text: str) -> str:
        """Strip tool output, code blocks, URLs, and file paths for voice.

        The full response is already sent via data channel — TTS should
        only speak the conversational parts.

        Overrides ``BasePlatformAdapter.prepare_tts_text``, which upstream
        calls in the auto-TTS path (the hook landed via NousResearch/
        hermes-agent#27308). The base default does a basic markdown strip;
        this override additionally removes code fences, inline code, URLs,
        file paths, and MEDIA: tags.
        """
        import re as _re

        # Remove fenced code blocks (```...```)
        text = _re.sub(r'```[\s\S]*?```', '', text)

        # Remove inline code (`...`)
        text = _re.sub(r'`[^`]+`', '', text)

        # Remove URLs
        text = _re.sub(r'https?://\S+', '', text)

        # Remove file paths (/foo/bar, ~/foo, C:\foo)
        text = _re.sub(r'(?:~|/|[A-Z]:\\)[\w./\\-]+', '', text)

        # Remove MEDIA: tags
        text = _re.sub(r'MEDIA:\S+', '', text)

        # Remove markdown formatting
        text = _re.sub(r'[*_`#\[\]()]', '', text)

        # Collapse whitespace
        text = _re.sub(r'\n{3,}', '\n\n', text)
        text = _re.sub(r'  +', ' ', text)

        return text[:4000].strip()

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """No typing indicator for voice — no-op."""
        pass

    async def on_processing_start(self, event: MessageEvent) -> None:
        protocol = self._realtime_protocol
        if protocol is None or event.source.chat_id != self._room_name:
            return
        await protocol.processing_started()
        event._hermes_realtime_response = (protocol, protocol.processing_turn_id)
        if callable(getattr(getattr(self, "gateway_runner", None), "_session_key_for_source", None)):
            room, generation, turn = self._room, self._room_generation, protocol.processing_turn_id
            identity = event.source.user_id
            participant = room.remote_participants.get(identity) if room else None
            remember_approval_origin(self, event, ApprovalOrigin(identity, self._room_name, event.message_id or uuid.uuid4().hex,
                lambda: self._room is room and self._room_generation == generation and room is not None
                    and room.remote_participants.get(identity) is participant and protocol.processing_turn_id is turn
                    and self._api_client_admission(identity) is not None,
                lambda value: self._publish_typed(value, identity=identity, topic=self.DATA_CHANNEL_EVENTS_TOPIC)))

    async def _send_exec_approval_prompt(self, prompt):
        return await approval_bridge(self).request(prompt)

    async def on_processing_complete(self, event: MessageEvent, outcome: ProcessingOutcome) -> None:
        """Finish turns that deliver only audio, fail, or are cancelled by Hermes."""
        forget_approval_origin(self, event)
        protocol = self._realtime_protocol
        if protocol is None or event.source.chat_id != self._room_name:
            return
        # An old Hermes task can finish after a replacement response has
        # started. Its cleanup must not complete or cancel that newer reply.
        if getattr(event, "_hermes_realtime_response", None) != (
            protocol, protocol.processing_turn_id
        ):
            return
        if outcome == ProcessingOutcome.CANCELLED:
            await protocol.response_cancelled()
        elif outcome == ProcessingOutcome.FAILURE:
            await protocol.response_failed()
        else:
            await protocol.output_stopped()

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return info about the LiveKit room."""
        participants = []
        if self._room:
            for p in self._room.remote_participants.values():
                participants.append(p.identity)
        return {
            "name": self._room_name,
            "type": "group",
            "chat_id": chat_id,
            "participants": participants,
        }
