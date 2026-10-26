"""Direct WebRTC transport contract and real aiortc negotiation."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from aiohttp import ClientSession, FormData, web
from aiortc import RTCPeerConnection, RTCSessionDescription

from gateway.config import PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, ProcessingOutcome
from gateway.platform_registry import PlatformEntry, platform_registry
from hermes_realtime.realtime_webrtc import (
    AdaptiveRmsGate,
    CLIENT_IDENTITY,
    OUTPUT_ECHO_GUARD_SECONDS,
    PROXY_CALL_HEADER,
    PROXY_PRINCIPAL_HEADER,
    PROXY_SIGNATURE_HEADER,
    PROXY_TIMESTAMP_HEADER,
    QueuedAudioTrack,
    RealtimeCall,
    RealtimeWebRTCAdapter,
    _gateway_profile_name,
    _parse_ice_servers,
    _proxy_signature_payload,
    check_realtime_requirements,
)
from tools.registry import registry
from hermes_realtime.client_admission import VoiceContext
from gateway.platforms.base import MessageType
import hermes_realtime.realtime_webrtc as webrtc_module


@pytest.fixture
def realtime_platform() -> None:
    entry = PlatformEntry(
        name="realtime",
        label="Realtime",
        adapter_factory=lambda config: RealtimeWebRTCAdapter(config),
        check_fn=check_realtime_requirements,
    )
    platform_registry.register(entry, scope=platform_registry.current_scope_key())
    try:
        yield
    finally:
        platform_registry.unregister("realtime")


@pytest.mark.asyncio
@pytest.mark.parametrize("signed", [False, True])
async def test_direct_listener_negotiates_and_sends_session_created(
    realtime_platform: None, signed: bool,
) -> None:
    adapter = RealtimeWebRTCAdapter(
        PlatformConfig(
            enabled=True,
            extra={"host": "127.0.0.1", "port": 0, "api_key": "test-token", "silence_duration": 0.7,
                "proxy_signing_key": "fixture-secret"},
        )
    )
    peer = RTCPeerConnection()
    created = asyncio.Event()
    updated = asyncio.Event()
    received: list[dict[str, object]] = []
    channel = peer.createDataChannel("oai-events")
    peer.addTransceiver("audio", direction="recvonly")
    adapter.process_text = AsyncMock()
    registered_name: str | None = None

    @channel.on("message")
    def on_message(raw: str) -> None:
        event = json.loads(raw)
        received.append(event)
        if event.get("type") == "session.created":
            created.set()
        if event.get("type") == "session.updated":
            updated.set()

    try:
        assert await adapter.connect() is True
        port = adapter._site._server.sockets[0].getsockname()[1]
        await peer.setLocalDescription(await peer.createOffer())
        form = FormData(default_to_multipart=True)
        form.add_field("sdp", peer.localDescription.sdp)
        session_json = json.dumps({
                "type": "realtime",
                "instructions": "Initial instructions.",
                "_kortexa_voice": {"version": 1, "base": "Voice guidance.", "display": "Supporting detail."},
                "tools": [{
                    "type": "function",
                    "name": "fixture_echo",
                    "description": "Return a value.",
                    "parameters": {"type": "object", "properties": {}},
                }],
                "tool_choice": "auto",
            })
        form.add_field("session", session_json)
        headers = {"Authorization": "Bearer test-token"}
        if signed:
            headers.update(_proxy_headers(principal="user-a", call_id="rt_fixture-call", sdp=peer.localDescription.sdp,
                session_json=session_json, secret="fixture-secret"))
        async with ClientSession() as client:
            async with client.post(
                f"http://127.0.0.1:{port}/v1/realtime/calls",
                data=form,
                headers=headers,
            ) as response:
                assert response.status == 201
                assert response.content_type == "application/sdp"
                assert response.headers["Location"].startswith("/v1/realtime/calls/call_")
                answer = await response.text()
        await peer.setRemoteDescription(RTCSessionDescription(sdp=answer, type="answer"))
        await asyncio.wait_for(created.wait(), timeout=10)

        assert received[0]["type"] == "session.created"
        assert received[0]["session"]["model"] == "hermes"
        assert received[0]["session"]["tool_choice"] == "auto"
        assert received[0]["session"]["instructions"] == "Initial instructions."
        assert len(adapter._calls) == 1
        active_call = next(iter(adapter._calls.values()))
        assert active_call.server_voice_context == (VoiceContext("Voice guidance.", "Supporting detail.") if signed else None)
        assert active_call.silence_duration == 0.7
        assert active_call.tool_bridge is not None
        registered_name = next(iter(active_call.tool_bridge._registered))
        assert registry.get_entry(registered_name) is not None
        channel.send(json.dumps({
            "type": "session.update",
            "event_id": "fixture-session-update",
            "session": {
                "type": "realtime",
                "instructions": "Reply briefly.",
            },
        }))
        await asyncio.wait_for(updated.wait(), timeout=5)
        assert active_call.protocol.instructions == "Reply briefly."
        channel.send(json.dumps({"type": "response.create"}))
        await asyncio.wait_for(
            _wait_until(lambda: adapter.process_text.await_count == 1),
            timeout=5,
        )
        requested_call, prompt = adapter.process_text.await_args.args
        assert requested_call.call_id.startswith("call_")
        assert prompt == "Follow the session instructions and respond now."
    finally:
        await peer.close()
        await adapter.disconnect()
    if registered_name is not None:
        assert registry.get_entry(registered_name) is None


async def _wait_until(predicate, interval: float = 0.01) -> None:
    while not predicate():
        await asyncio.sleep(interval)


@pytest.mark.asyncio
async def test_direct_voice_prompt_survives_session_instruction_changes():
    adapter = object.__new__(RealtimeWebRTCAdapter)
    adapter._source_for_call = lambda call: SimpleNamespace(chat_id="fixture")
    adapter.handle_message = AsyncMock()
    call = SimpleNamespace(server_voice_context=VoiceContext("Voice guidance.", "Supporting detail."),
        client_tool_names={"display_card"}, tool_bridge=None,
        protocol=SimpleNamespace(instructions="User preference.", tool_choice="auto", response_started=AsyncMock()))
    await adapter._dispatch_text(call, "Hello", MessageType.TEXT)
    event = adapter.handle_message.await_args.args[0]
    assert event.channel_prompt == "Voice guidance.\n\nUser preference.\n\nSupporting detail."
    call.protocol.instructions = ""
    call.protocol.tool_choice = "none"
    await adapter._dispatch_text(call, "Hello again", MessageType.VOICE)
    assert adapter.handle_message.await_args.args[0].channel_prompt == "Voice guidance."


def test_gateway_profile_name_is_derived_from_process_home(tmp_path) -> None:
    assert _gateway_profile_name(tmp_path / ".hermes") == "default"
    assert _gateway_profile_name(tmp_path / ".hermes" / "profiles" / "mira") == "mira"
    assert _gateway_profile_name(tmp_path / ".hermes" / "profiles" / "Mira") == "default"


@pytest.mark.asyncio
async def test_direct_listener_exposes_authenticated_fixed_profile_discovery(
    realtime_platform: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes" / "profiles" / "mira"))
    adapter = RealtimeWebRTCAdapter(
        PlatformConfig(
            enabled=True,
            extra={"host": "127.0.0.1", "port": 0, "api_key": "test-token"},
        )
    )
    try:
        assert await adapter.connect() is True
        port = adapter._site._server.sockets[0].getsockname()[1]
        async with ClientSession() as client:
            async with client.get(
                f"http://127.0.0.1:{port}/v1/realtime/discovery",
            ) as response:
                assert response.status == 401
            async with client.get(
                f"http://127.0.0.1:{port}/v1/realtime/discovery",
                headers={"Authorization": "Bearer test-token"},
            ) as response:
                assert response.status == 200
                assert response.headers["Cache-Control"] == "no-store"
                assert await response.json() == {
                    "version": 1,
                    "profile": "mira",
                    "realtime_path": "/v1/realtime/calls",
                }
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_queued_audio_track_paces_pcm_in_realtime() -> None:
    track = QueuedAudioTrack()
    samples_per_frame = 48_000 // 50
    frame_bytes = b"\x00\x00" * samples_per_frame
    await track.enqueue_pcm(frame_bytes * 3)

    started_at = asyncio.get_running_loop().time()
    frames = [await track.recv() for _ in range(3)]
    elapsed = asyncio.get_running_loop().time() - started_at

    # The first frame is ready immediately; the next two are spaced 20 ms
    # apart instead of being returned as a single RTP burst.
    assert elapsed >= 0.035
    assert [frame.pts for frame in frames] == [
        0,
        samples_per_frame,
        samples_per_frame * 2,
    ]
    assert all(frame.sample_rate == 48_000 for frame in frames)


@pytest.mark.asyncio
async def test_idle_audio_keeps_clock_running_and_preserves_burst_onsets():
    track = QueuedAudioTrack()
    frames = []
    started = asyncio.get_running_loop().time()
    try:
        # Keep an idle receiver primed before and between two short utterances.
        for burst in range(2):
            for _ in range(6):
                frame = await asyncio.wait_for(track.recv(), 0.3)
                assert bytes(frame.planes[0]) == b"\x00\x00" * 960
                frames.append(frame)
            onset = b"\x00\x10" * 960
            tail = b"\x00\x08" * 960
            await track.enqueue_pcm(onset + tail)
            for expected in (onset, tail):
                frame = await asyncio.wait_for(track.recv(), 0.3)
                assert bytes(frame.planes[0]) == expected
                frames.append(frame)
            await asyncio.wait_for(track.drained(), 0.1)
        assert [frame.pts for frame in frames] == list(range(0, 16 * 960, 960))
        assert asyncio.get_running_loop().time() - started >= 0.28
        # Background silence is not unfinished speech and cannot delay drain.
        await asyncio.wait_for(track.drained(), 0.1)
    finally:
        track.stop()


@pytest.mark.asyncio
async def test_stopped_idle_audio_track_ends_promptly():
    from aiortc.mediastreams import MediaStreamError

    track = QueuedAudioTrack()
    track.stop()
    with pytest.raises(MediaStreamError):
        await asyncio.wait_for(track.recv(), 0.1)


def test_adaptive_rms_gate_learns_fan_noise_without_calling_it_speech() -> None:
    gate = AdaptiveRmsGate()

    # The Pi fan measures around RMS 150, well above the legacy fixed gate of
    # 50. Include loud samples to model the user starting to talk while the
    # initial 400 ms calibration window is still open.
    for rms in [150.0] * 12 + [900.0] * 8:
        gate.calibrate(rms)

    assert gate.ready is True
    assert gate.noise_rms == 150.0
    assert gate.is_speech(175.0, speaking=False) is False
    assert gate.is_speech(900.0, speaking=False) is True
    assert gate.is_speech(175.0, speaking=True) is False


def test_adaptive_rms_gate_keeps_safe_floor_after_muted_track_calibration() -> None:
    gate = AdaptiveRmsGate()
    for _ in range(20):
        gate.calibrate(0.0)

    # WPE sends digital silence before replaceTrack installs the real mic.
    # The measured Pi fan (RMS ~150, short-window peaks below 200) must not
    # become speech merely because calibration happened while muted.
    assert gate.noise_rms == 1.0
    assert gate.start_threshold == 300.0
    assert gate.stop_threshold == 220.0
    assert gate.is_speech(200.0, speaking=False) is False
    assert gate.is_speech(700.0, speaking=False) is True


def test_adaptive_rms_gate_tracks_gradual_idle_noise_changes() -> None:
    gate = AdaptiveRmsGate()
    for _ in range(20):
        gate.calibrate(100.0)
    initial_floor = gate.noise_rms

    for _ in range(100):
        assert gate.is_speech(175.0, speaking=False) is False

    assert gate.noise_rms is not None
    assert initial_floor is not None
    assert gate.noise_rms > initial_floor
    assert gate.noise_rms < 175.0


def _proxy_headers(
    *,
    principal: str,
    call_id: str,
    sdp: str,
    session_json: str,
    secret: str,
    timestamp: str | None = None,
) -> dict[str, str]:
    encoded_principal = base64.urlsafe_b64encode(principal.encode()).decode().rstrip("=")
    signed_at = timestamp or str(int(time.time()))
    signature = hmac.new(
        secret.encode(),
        _proxy_signature_payload(
            signed_at,
            call_id,
            encoded_principal,
            sdp,
            session_json,
        ),
        hashlib.sha256,
    ).hexdigest()
    return {
        PROXY_PRINCIPAL_HEADER: encoded_principal,
        PROXY_CALL_HEADER: call_id,
        PROXY_TIMESTAMP_HEADER: signed_at,
        PROXY_SIGNATURE_HEADER: f"v1={signature}",
    }


def test_internal_proxy_signature_matches_the_api_cross_language_fixture() -> None:
    signature = hmac.new(
        b"fixture-secret",
        _proxy_signature_payload(
            "1787900000",
            "rt_12345678",
            "dXNlci1h",
            "v=0\r\n",
            '{"type":"realtime"}',
        ),
        hashlib.sha256,
    ).hexdigest()

    assert signature == "c1cdd493ff293612f1140d867040dccdcd9b17478c300be6108ec7ee754da6e1"


def test_internal_proxy_metadata_is_body_bound_scoped_and_replay_safe() -> None:
    adapter = RealtimeWebRTCAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "host": "127.0.0.1",
                "port": 0,
                "api_key": "service-token",
                "proxy_signing_key": "signing-secret",
            },
        )
    )
    headers = _proxy_headers(
        principal="user-a",
        call_id="rt_12345678",
        sdp="v=0\r\n",
        session_json='{"type":"realtime"}',
        secret="signing-secret",
    )
    request = SimpleNamespace(headers=headers)

    identity = adapter._client_identity(
        request,
        "v=0\r\n",
        '{"type":"realtime"}',
    )

    assert identity.startswith("kortexa-")
    assert identity != "user-a"
    with pytest.raises(web.HTTPConflict):
        adapter._client_identity(request, "v=0\r\n", '{"type":"realtime"}')

    altered = RealtimeWebRTCAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "api_key": "service-token",
                "proxy_signing_key": "signing-secret",
            },
        )
    )
    with pytest.raises(web.HTTPUnauthorized):
        altered._client_identity(request, "v=1\r\n", '{"type":"realtime"}')


def test_unsigned_direct_requests_keep_the_legacy_opaque_identity() -> None:
    adapter = RealtimeWebRTCAdapter(
        PlatformConfig(enabled=True, extra={"api_key": "service-token"})
    )

    assert adapter._client_identity(SimpleNamespace(headers={}), "v=0\r\n", "{}") == CLIENT_IDENTITY


def test_direct_ice_servers_are_bounded_and_scheme_checked() -> None:
    servers = _parse_ice_servers(json.dumps([
        {"urls": "stun:stun.example.test:3478"},
        {
            "urls": ["turn:turn.example.test:3478?transport=udp"],
            "username": "user",
            "credential": "secret",
        },
    ]))

    assert len(servers) == 2
    assert servers[1].username == "user"
    with pytest.raises(ValueError, match="urls is invalid"):
        _parse_ice_servers('[{"urls":"https://example.test"}]')


@pytest.mark.asyncio
async def test_listener_requires_api_key(realtime_platform: None) -> None:
    adapter = RealtimeWebRTCAdapter(
        PlatformConfig(enabled=True, extra={"host": "127.0.0.1", "port": 0})
    )

    assert await adapter.connect() is False


@pytest.mark.asyncio
async def test_listener_accepts_required_tool_choice(
    realtime_platform: None,
) -> None:
    adapter = RealtimeWebRTCAdapter(
        PlatformConfig(
            enabled=True,
            extra={"host": "127.0.0.1", "port": 0, "api_key": "test-token"},
        )
    )
    peer = RTCPeerConnection()
    created = asyncio.Event()
    channel = peer.createDataChannel("oai-events")
    peer.addTransceiver("audio", direction="recvonly")

    @channel.on("message")
    def on_message(raw: str) -> None:
        if json.loads(raw).get("type") == "session.created":
            created.set()

    try:
        assert await adapter.connect() is True
        port = adapter._site._server.sockets[0].getsockname()[1]
        await peer.setLocalDescription(await peer.createOffer())
        form = FormData(default_to_multipart=True)
        form.add_field("sdp", peer.localDescription.sdp)
        form.add_field(
            "session",
            json.dumps({
                "type": "realtime",
                "tools": [{
                    "type": "function",
                    "name": "fixture_echo",
                    "parameters": {"type": "object", "properties": {}},
                }],
                "tool_choice": "required",
            }),
        )
        async with ClientSession() as client:
            async with client.post(
                f"http://127.0.0.1:{port}/v1/realtime/calls",
                data=form,
                headers={"Authorization": "Bearer test-token"},
            ) as response:
                assert response.status == 201
                answer = await response.text()
        await peer.setRemoteDescription(RTCSessionDescription(sdp=answer, type="answer"))
        await asyncio.wait_for(created.wait(), timeout=10)
        call = next(iter(adapter._calls.values()))
        assert call.protocol.tool_choice == "required"
    finally:
        await peer.close()
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_direct_send_voice_uses_native_audio_track() -> None:
    adapter = object.__new__(RealtimeWebRTCAdapter)
    adapter.play_tts = AsyncMock(return_value="delivered")

    result = await adapter.send_voice(
        chat_id="call",
        audio_path="reply.wav",
        caption="caption",
        reply_to="message",
        metadata={"turn": 1},
    )

    assert result == "delivered"
    adapter.play_tts.assert_awaited_once_with(
        chat_id="call",
        audio_path="reply.wav",
        caption="caption",
        reply_to="message",
        metadata={"turn": 1},
    )


@pytest.mark.asyncio
async def test_direct_play_tts_keeps_capture_paused_for_output_echo_tail() -> None:
    protocol = AsyncMock()
    output_track = SimpleNamespace(enqueue_pcm=AsyncMock(), drained=AsyncMock())
    call = SimpleNamespace(
        protocol=protocol,
        output_track=output_track,
        paused=False,
        closed=False,
    )
    adapter = object.__new__(RealtimeWebRTCAdapter)
    adapter._calls = {"call": call}

    async def guarded_sleep(delay: float) -> None:
        assert delay == OUTPUT_ECHO_GUARD_SECONDS
        assert call.paused is True
        protocol.output_playback_stopped.assert_not_awaited()

    with (
        patch(
            "hermes_realtime.adapter.LiveKitAdapter._decode_audio_to_pcm",
            return_value=b"\x00\x00" * 320,
        ),
        patch(
            "hermes_realtime.realtime_webrtc.asyncio.sleep",
            side_effect=guarded_sleep,
        ) as sleep,
    ):
        result = await adapter.play_tts("call", "reply.wav")

    assert result.success is True
    sleep.assert_awaited_once_with(OUTPUT_ECHO_GUARD_SECONDS)
    output_track.enqueue_pcm.assert_awaited_once()
    output_track.drained.assert_awaited_once_with()
    protocol.output_started.assert_awaited_once_with()
    protocol.output_playback_stopped.assert_awaited_once_with()
    protocol.output_stopped.assert_not_awaited()
    assert call.paused is False


@pytest.mark.asyncio
async def test_direct_send_completes_transcript_response() -> None:
    from hermes_realtime.realtime_protocol import RealtimeProtocol

    events = []

    async def publish(event, recipient):
        events.append(event)
        return True

    protocol = RealtimeProtocol(
        session_id="standalone", model="test", voice="test", publish=publish,
    )
    adapter = object.__new__(RealtimeWebRTCAdapter)
    call = SimpleNamespace(protocol=protocol)
    adapter._calls = {"call": call}

    result = await adapter.send(chat_id="call", content="hello")

    assert result.success is True
    done = [e["response"] for e in events if e["type"] == "response.done"]
    assert len(done) == 1
    assert done[0]["status"] == "completed"
    assert done[0]["output"][0]["content"][0]["transcript"] == "hello"
    assert protocol.active_response_id is None


@pytest.mark.asyncio
async def test_direct_send_drops_home_channel_prompt() -> None:
    from hermes_realtime.realtime_protocol import RealtimeProtocol

    events = []

    async def publish(event, recipient):
        events.append(event)
        return True

    protocol = RealtimeProtocol(
        session_id="standalone", model="test", voice="test", publish=publish,
    )
    adapter = object.__new__(RealtimeWebRTCAdapter)
    adapter._calls = {"call": SimpleNamespace(protocol=protocol)}
    prompt = (
        "📬 No home channel is set for Realtime. A home channel is where Hermes "
        "delivers cron job results and cross-platform messages.\n\n"
        "Type /sethome to make this chat your home channel, or ignore to skip."
    )

    result = await adapter.send(chat_id="call", content=prompt)

    assert result.success is True
    assert events == []
    assert protocol.active_response_id is None


def test_direct_adapter_supports_async_completion_delivery() -> None:
    assert RealtimeWebRTCAdapter.supports_async_delivery is True


@pytest.mark.asyncio
async def test_direct_internal_wake_waits_for_silence() -> None:
    adapter = object.__new__(RealtimeWebRTCAdapter)
    call = SimpleNamespace(
        call_id="call",
        speaking=True,
        closed=False,
        tasks=set(),
    )

    def spawn(coroutine: object) -> None:
        task = asyncio.create_task(coroutine)
        call.tasks.add(task)
        task.add_done_callback(call.tasks.discard)

    call.spawn = spawn
    adapter._calls = {"call": call}
    event = SimpleNamespace(internal=True, source=SimpleNamespace(chat_id="call"))

    with patch.object(BasePlatformAdapter, "handle_message", new=AsyncMock()) as dispatch:
        await adapter.handle_message(event)
        await asyncio.sleep(0)
        dispatch.assert_not_awaited()

        call.speaking = False
        await asyncio.gather(*call.tasks)
        dispatch.assert_awaited_once_with(event)


@pytest.mark.asyncio
async def test_direct_mute_finalizes_and_unmute_resets_vad() -> None:
    protocol = AsyncMock()
    adapter = SimpleNamespace(process_voice=AsyncMock(), cancel_call_response=AsyncMock())
    call = RealtimeCall(
        adapter=adapter,
        call_id="call-a",
        peer=AsyncMock(),
        output_track=QueuedAudioTrack(),
        protocol=protocol,
    )
    call.speaking = True
    call.audio_buffer.extend(b"\x00\x00" * 48_000)
    old_vad = call.vad

    await call.set_input_audio_state(True)
    await asyncio.sleep(0)
    assert call.input_muted is True
    assert call.audio_buffer == b""
    protocol.speech_stopped.assert_awaited_once_with("webrtc-client")
    adapter.process_voice.assert_awaited_once()

    await call.set_input_audio_state(False)
    assert call.input_muted is False
    assert call.vad is not old_vad
    assert call.vad.ready is False

    for task in list(call.tasks):
        task.cancel()


@pytest.mark.asyncio
async def test_audio_clear_discards_the_frame_already_waiting_for_its_deadline(monkeypatch):
    track = QueuedAudioTrack()
    await track.enqueue_pcm(b"\x01\x00" * 960)
    track._next_frame_at = asyncio.get_running_loop().time() + 10

    async def clear_during_pacing(delay):
        track.clear()

    monkeypatch.setattr(webrtc_module.asyncio, "sleep", clear_during_pacing)
    frame = await track.recv()
    assert bytes(frame.planes[0]) == b"\x00\x00" * 960
    await track.drained()


@pytest.mark.asyncio
async def test_call_close_does_not_transcribe_buffered_audio():
    entered = asyncio.Event()

    async def receive():
        entered.set()
        await asyncio.Future()

    adapter = SimpleNamespace(process_voice=AsyncMock(), cancel_call_response=AsyncMock())
    call = RealtimeCall(adapter, "call", AsyncMock(), QueuedAudioTrack(), AsyncMock())
    call.speaking = True
    call.audio_buffer.extend(b"\x00\x00" * 48_000)
    call.spawn(call.consume_audio(SimpleNamespace(recv=receive)))
    await entered.wait()
    tasks = list(call.tasks)
    await call.close()
    adapter.process_voice.assert_not_awaited()
    assert call.audio_buffer == b""
    assert all(task.done() for task in tasks)
    call.peer.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancelled_tts_clears_queued_audio():
    track = SimpleNamespace(
        enqueue_pcm=AsyncMock(),
        drained=AsyncMock(side_effect=asyncio.CancelledError),
        clear=Mock(),
    )
    call = SimpleNamespace(protocol=AsyncMock(), output_track=track, paused=False, closed=False)
    adapter = object.__new__(RealtimeWebRTCAdapter)
    adapter._calls = {"call": call}
    with patch("hermes_realtime.adapter.LiveKitAdapter._decode_audio_to_pcm", return_value=b"\x00\x00" * 960):
        with pytest.raises(asyncio.CancelledError):
            await adapter.play_tts("call", "unused.wav")
    track.clear.assert_called_once()
    assert not call.paused


@pytest.mark.asyncio
async def test_completed_transcription_is_dropped_after_call_closes(monkeypatch):
    call = SimpleNamespace(protocol=AsyncMock(), closed=False, client_identity="client")
    adapter = object.__new__(RealtimeWebRTCAdapter)
    adapter._dispatch_text = AsyncMock()

    def transcribe(*args):
        call.closed = True
        return "late transcript"

    monkeypatch.setattr(webrtc_module, "transcribe_pcm", transcribe)
    await adapter.process_voice(call, b"\x00\x00", item_id="first")
    adapter._dispatch_text.assert_not_awaited()
    call.protocol.user_transcript.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("bad offer"), asyncio.CancelledError()])
async def test_failed_or_cancelled_setup_releases_call_and_peer(monkeypatch, realtime_platform, failure):
    adapter = RealtimeWebRTCAdapter(PlatformConfig(extra={"api_key": "test-token"}))
    adapter._read_offer = AsyncMock(return_value=("sdp", {}, "{}"))
    peer = SimpleNamespace(
        on=lambda event: lambda callback: callback,
        addTrack=Mock(),
        setRemoteDescription=AsyncMock(side_effect=failure),
        close=AsyncMock(),
    )
    monkeypatch.setattr(webrtc_module, "RTCPeerConnection", lambda config: peer)
    request = SimpleNamespace(headers={"Authorization": "Bearer test-token"})
    if isinstance(failure, asyncio.CancelledError):
        with pytest.raises(asyncio.CancelledError):
            await adapter._create_call(request)
    else:
        response = await adapter._create_call(request)
        assert response.status == 400
    assert adapter._calls == {}
    assert adapter._pending_calls == 0
    peer.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_setup_deadline_releases_pending_slot(monkeypatch, realtime_platform):
    adapter = RealtimeWebRTCAdapter(PlatformConfig(extra={"api_key": "test-token"}))

    async def read_forever(request):
        await asyncio.Future()

    adapter._read_offer = read_forever
    monkeypatch.setattr(webrtc_module, "CALL_SETUP_TIMEOUT_SECONDS", 0)
    response = await adapter._create_call(SimpleNamespace(headers={"Authorization": "Bearer test-token"}))
    assert response.status == 400
    assert adapter._pending_calls == 0
    assert adapter._calls == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome,method", [
    (ProcessingOutcome.SUCCESS, "output_stopped"),
    (ProcessingOutcome.FAILURE, "response_failed"),
    (ProcessingOutcome.CANCELLED, "response_cancelled"),
])
async def test_direct_processing_hook_finishes_audio_only_and_failed_turns(outcome, method):
    adapter = object.__new__(RealtimeWebRTCAdapter)
    protocol = AsyncMock()
    adapter._calls = {"call": SimpleNamespace(protocol=protocol)}
    event = SimpleNamespace(source=SimpleNamespace(chat_id="call"))
    await adapter.on_processing_start(event)
    await adapter.on_processing_complete(event, outcome)
    getattr(protocol, method).assert_awaited_once()


@pytest.mark.asyncio
async def test_shutdown_covers_calls_added_by_finishing_http_handlers():
    adapter = object.__new__(RealtimeWebRTCAdapter)
    adapter._mark_disconnected = Mock()
    adapter._calls = {}
    call = SimpleNamespace(close=AsyncMock())

    async def finish_handlers():
        adapter._calls["last"] = call

    adapter._runner = SimpleNamespace(cleanup=finish_handlers)
    await adapter.disconnect()
    call.close.assert_awaited_once()
    assert adapter._calls == {}


@pytest.mark.asyncio
async def test_setup_releases_peer_if_audio_track_construction_fails(monkeypatch, realtime_platform):
    adapter = RealtimeWebRTCAdapter(PlatformConfig(extra={"api_key": "test-token"}))
    adapter._read_offer = AsyncMock(return_value=("sdp", {}, "{}"))
    peer = SimpleNamespace(close=AsyncMock())
    monkeypatch.setattr(webrtc_module, "RTCPeerConnection", lambda config: peer)
    monkeypatch.setattr(webrtc_module, "QueuedAudioTrack", Mock(side_effect=RuntimeError("track failure")))
    response = await adapter._create_call(SimpleNamespace(headers={"Authorization": "Bearer test-token"}))
    assert response.status == 400
    assert adapter._pending_calls == 0
    peer.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_queued_transcription_does_not_start_after_call_close(monkeypatch):
    adapter = object.__new__(RealtimeWebRTCAdapter)
    transcribe = Mock()
    monkeypatch.setattr(webrtc_module, "transcribe_pcm", transcribe)
    await adapter.process_voice(SimpleNamespace(closed=True), b"\x00\x00")
    transcribe.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("resume", [False, True])
async def test_early_asr_waits_for_endpoint_and_discards_resumed_speech(monkeypatch, resume):
    from hermes_realtime.media import EarlyTranscription

    clock = [1.0]
    monkeypatch.setattr(webrtc_module, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    worker = Mock(return_value="candidate")
    prefetch = EarlyTranscription(worker, 48_000, 1)
    adapter = SimpleNamespace(process_voice=AsyncMock(), cancel_call_response=AsyncMock())
    call = RealtimeCall(adapter, "early-asr", AsyncMock(), QueuedAudioTrack(), AsyncMock(),
                        vad=AdaptiveRmsGate(noise_rms=150), silence_duration=0.7,
                        asr_prefetch_silence=0.35, early_asr=prefetch)
    speech = b"\x00\x10" * 9600
    quiet = b"\x10\x00" * 9600
    try:
        await call.accept_pcm(speech)
        clock[0] = 1.2
        await call.accept_pcm(quiet)
        worker.assert_not_called()
        clock[0] = 1.4
        await call.accept_pcm(quiet)
        candidate = prefetch._task
        assert await candidate == "candidate"
        adapter.process_voice.assert_not_awaited()
        call.protocol.speech_stopped.assert_not_awaited()
        if resume:
            clock[0] = 1.5
            await call.accept_pcm(speech)
            # Mute can precede another silence tick; it must not reuse a prefix.
            await call.set_input_audio_state(True)
        else:
            clock[0] = 1.71
            await call.accept_pcm(quiet)
        await asyncio.gather(*list(call.tasks))
        adapter.process_voice.assert_awaited_once()
        dispatched = adapter.process_voice.call_args.kwargs["prefetched"]
        assert dispatched is (None if resume else candidate)
        call.protocol.speech_stopped.assert_awaited_once()
    finally:
        await call.close()
