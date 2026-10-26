from __future__ import annotations

import json
import asyncio

import pytest

from hermes_realtime.realtime_protocol import RealtimeProtocol


def protocol_fixture(**callbacks):
    sent: list[tuple[dict, str | None]] = []

    async def publish(event: dict, recipient: str | None) -> bool:
        sent.append((event, recipient))
        return True

    protocol = RealtimeProtocol(
        session_id="room-a",
        model="local-model",
        voice="local-voice",
        publish=publish,
        **callbacks,
    )
    return protocol, sent


@pytest.mark.asyncio
async def test_tool_replacement_is_correlated_and_revokes_a_pending_camera():
    changes = []
    protocol, sent = protocol_fixture(on_tools_updated=lambda tools, identity: changes.append((tools, identity)))
    await protocol.client_connected("client")
    tools = [{"type": "function", "name": "camera_snapshot", "description": "Snapshot", "parameters": {"type": "object"}}]
    await protocol.handle_client_message(json.dumps({"type": "session.update", "event_id": "enable", "session": {"type": "realtime", "tools": tools}}), "client")
    assert changes[0][0][0].name == "camera_snapshot"
    assert changes[0][1] == "client"
    assert sent[-1][0]["client_event_id"] == "enable"
    assert sent[-1][0]["session"]["tools"] == tools
    request = asyncio.create_task(protocol.request_client_tool("camera_snapshot", {}))
    for _ in range(10):
        await asyncio.sleep(0)
        if protocol._pending_tool is not None:
            break
    await protocol.handle_client_message(json.dumps({"type": "session.update", "event_id": "disable", "session": {"type": "realtime", "tools": []}}), "client")
    with pytest.raises(RuntimeError, match="disabled"):
        await request
    assert sent[-1][0]["session"]["tools"] == []
    await protocol.close()


@pytest.mark.asyncio
async def test_playout_then_transcript_completes_one_response():
    protocol, sent = protocol_fixture()
    await protocol.output_started()
    response_id = protocol.active_response_id
    await protocol.output_playback_stopped()
    assert protocol.active_response_id == response_id
    assert not any(event["type"] == "response.done" for event, _ in sent)
    await protocol.assistant_transcript("hello")
    await protocol.output_stopped()
    responses = [event["response"] for event, _ in sent if event["type"] == "response.done"]
    assert len(responses) == 1
    assert responses[0]["id"] == response_id
    assert responses[0]["output"][0]["content"][0]["transcript"] == "hello"
    assert sum(event["type"] == "response.created" for event, _ in sent) == 1


@pytest.mark.asyncio
async def test_transcription_keeps_utterance_id_when_next_speech_has_started():
    protocol, sent = protocol_fixture()
    await protocol.speech_started("client")
    first = await protocol.speech_stopped("client")
    await protocol.speech_started("client")
    second = await protocol.speech_stopped("client")
    # Even out-of-order worker completion must not consume another input ID.
    await protocol.user_transcript("second", "client", item_id=second)
    await protocol.user_transcript("first", "client", item_id=first)
    transcripts = [event for event, _ in sent if event["type"] == "conversation.item.input_audio_transcription.completed"]
    assert [(event["transcript"], event["item_id"]) for event in transcripts] == [
        ("second", second), ("first", first)
    ]
    assert first != second


@pytest.mark.asyncio
async def test_item_publish_cannot_be_overtaken_by_response_create():
    blocked = asyncio.Event()
    release = asyncio.Event()
    inputs = []
    protocol, sent = protocol_fixture(on_text_input=lambda text, identity: inputs.append(text))
    publish = protocol._publish

    async def slow_publish(event, recipient):
        if event["type"] == "conversation.item.added":
            blocked.set()
            await release.wait()
        return await publish(event, recipient)

    protocol._publish = slow_publish
    item = asyncio.create_task(protocol.handle_client_message(json.dumps({
        "type": "conversation.item.create",
        "item": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]},
    }), "client"))
    await asyncio.wait_for(blocked.wait(), timeout=1)
    response = asyncio.create_task(protocol.handle_client_message('{"type":"response.create"}', "client"))
    await asyncio.sleep(0)
    assert inputs == []
    release.set()
    await asyncio.gather(item, response)
    assert inputs == ["hello"]
    assert not any(event["type"] == "error" for event, _ in sent)


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [
    '\ud800',
    '{"type":"session.update","session":{"instructions":"\\ud800"}}',
    '{"type":"session.update","session":{"instructions":NaN}}',
    '[' * 2000 + ']' * 2000,
])
async def test_malformed_unicode_and_nested_json_get_protocol_errors(raw):
    protocol, sent = protocol_fixture()
    await protocol.handle_client_message(raw, "client")
    assert sent[-1][0]["error"]["code"] == "invalid_event_json"
    await protocol.handle_client_message('{"type":"session.update","session":{"instructions":"hello"}}', "client")
    assert protocol.instructions == "hello"


@pytest.mark.asyncio
async def test_response_failure_stops_playback_state():
    protocol, sent = protocol_fixture()
    await protocol.output_started()
    await protocol.response_failed()
    assert not protocol._speaking
    assert sent[-1][0]["response"]["status"] == "failed"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("publish failed"), asyncio.CancelledError()])
async def test_tool_emission_failure_releases_pending_call(failure):
    protocol, sent = protocol_fixture()

    async def publish(event, recipient):
        if event["type"] == "response.output_item.added":
            raise failure
        return True

    protocol._publish = publish
    with pytest.raises(type(failure)):
        await protocol.request_client_tool("fixture", {})
    assert protocol._pending_tool is None


@pytest.mark.asyncio
async def test_routes_namespaced_input_audio_state_and_acknowledges_it() -> None:
    states: list[tuple[bool, str]] = []

    async def on_state(muted: bool, identity: str) -> None:
        states.append((muted, identity))

    protocol, sent = protocol_fixture(on_input_audio_state=on_state)
    await protocol.handle_client_message(
        json.dumps({
            "type": "hermes.input_audio.state",
            "event_id": "mute-1",
            "muted": True,
        }),
        "client-a",
    )

    assert states == [(True, "client-a")]
    assert sent[-1][0]["type"] == "hermes.input_audio.state_updated"
    assert sent[-1][0]["muted"] is True
    assert sent[-1][1] == "client-a"


@pytest.mark.asyncio
async def test_rejects_invalid_input_audio_state() -> None:
    protocol, sent = protocol_fixture(on_input_audio_state=lambda *_args: None)
    await protocol.handle_client_message(
        json.dumps({"type": "hermes.input_audio.state", "muted": "yes"}),
        "client-a",
    )

    assert sent[-1][0]["error"]["code"] == "invalid_input_audio_state"


@pytest.mark.asyncio
async def test_targets_session_snapshot_and_correlated_client_error() -> None:
    protocol, sent = protocol_fixture()

    await protocol.client_connected("client-a")
    await protocol.handle_client_message(
        json.dumps({"type": "nope", "event_id": "client-event-1"}),
        "client-b",
    )

    assert sent[0][0]["type"] == "session.created"
    assert sent[0][1] == "client-a"
    assert sent[1][0]["error"]["event_id"] == "client-event-1"
    assert sent[1][1] == "client-b"


@pytest.mark.asyncio
async def test_accepts_bounded_session_update_and_targets_snapshot() -> None:
    protocol, sent = protocol_fixture(instructions="initial")

    await protocol.handle_client_message(
        json.dumps({
            "type": "session.update",
            "event_id": "session-update-1",
            "session": {
                "type": "realtime",
                "instructions": "Reply briefly.",
                "tool_choice": "none",
            },
        }),
        "client-a",
    )

    assert protocol.instructions == "Reply briefly."
    assert protocol.tool_choice == "none"
    assert sent[0][0]["event_id"].startswith("evt_room-a_1_")
    assert sent[0][0]["type"] == "session.updated"
    assert sent[0][0]["session"] == {
        "id": "room-a",
        "type": "realtime",
        "model": "local-model",
        "reasoning_effort": None,
        "instructions": "Reply briefly.",
        "tool_choice": "none",
        "audio": {"output": {"voice": "local-voice"}},
    }
    assert sent[0][1] == "client-a"


@pytest.mark.asyncio
async def test_rejects_unsupported_session_update_fields() -> None:
    protocol, sent = protocol_fixture()

    await protocol.handle_client_message(
        json.dumps({
            "type": "session.update",
            "event_id": "session-update-unsupported",
            "session": {"type": "realtime", "temperature": 0.5},
        }),
        "client-a",
    )

    assert sent[-1][0]["error"] == {
        "type": "invalid_request_error",
        "code": "unsupported_session_field",
        "message": "Unsupported session field: temperature",
        "param": "session.temperature",
        "event_id": "session-update-unsupported",
    }


@pytest.mark.asyncio
async def test_maps_audio_and_response_lifecycle_to_openai_events() -> None:
    protocol, sent = protocol_fixture()

    await protocol.speech_started("client-a")
    await protocol.speech_stopped("client-a")
    await protocol.user_transcript("hello", "client-a")
    await protocol.response_started()
    await protocol.output_started()
    await protocol.assistant_transcript("hi there")
    await protocol.output_stopped()

    assert [event["type"] for event, _ in sent] == [
        "input_audio_buffer.speech_started",
        "input_audio_buffer.speech_stopped",
        "conversation.item.added",
        "conversation.item.input_audio_transcription.completed",
        "conversation.item.done",
        "response.created",
        "response.output_item.added",
        "conversation.item.added",
        "response.content_part.added",
        "output_audio_buffer.started",
        "response.output_audio_transcript.done",
        "response.content_part.done",
        "conversation.item.done",
        "response.output_item.done",
        "response.done",
        "output_audio_buffer.stopped",
    ]
    response = sent[-2][0]["response"]
    assert response["status"] == "completed"
    assert response["output"][0]["content"] == [
        {"type": "output_audio", "transcript": "hi there"}
    ]
    assert all(event["event_id"].startswith("evt_room-a_") for event, _ in sent)


@pytest.mark.asyncio
async def test_routes_typed_input_and_cancellation_to_transport_callbacks() -> None:
    inputs: list[tuple[str, str]] = []
    cancelled: list[str] = []

    async def on_text(text: str, identity: str) -> None:
        inputs.append((text, identity))

    async def on_cancel(identity: str) -> None:
        cancelled.append(identity)

    protocol, sent = protocol_fixture(
        on_text_input=on_text,
        on_response_cancelled=on_cancel,
    )
    await protocol.handle_client_message(
        json.dumps(
            {
                "type": "conversation.item.create",
                "item": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": " hello "}],
                },
            }
        ),
        "client-a",
    )
    assert inputs == []
    assert [event["type"] for event, _ in sent[:2]] == [
        "conversation.item.added",
        "conversation.item.done",
    ]
    assert all(recipient == "client-a" for _, recipient in sent[:2])
    await protocol.handle_client_message(
        json.dumps({"type": "response.create", "event_id": "response-1"}),
        "client-a",
    )
    await protocol.response_started()
    await protocol.output_started()
    await protocol.handle_client_message(
        json.dumps({"type": "response.cancel", "event_id": "cancel-1"}),
        "client-a",
    )

    assert inputs == [("hello", "client-a")]
    assert cancelled == ["client-a"]
    assert [event["type"] for event, _ in sent[-2:]] == [
        "response.done",
        "output_audio_buffer.cleared",
    ]
    assert sent[-2][0]["response"]["status"] == "cancelled"


@pytest.mark.asyncio
@pytest.mark.parametrize("result_received", [False, True])
@pytest.mark.parametrize("hook_completes", [False, True])
async def test_wire_cancel_stops_pending_tool_and_rejects_late_result(result_received, hook_completes):
    cancelled = []

    async def on_cancel(identity):
        cancelled.append(identity)
        if hook_completes:
            await protocol.response_cancelled()

    protocol, sent = protocol_fixture(on_response_cancelled=on_cancel)
    await protocol.processing_started()
    pending = asyncio.create_task(protocol.request_client_tool("fixture", {}))
    try:
        async def sealed():
            while not any(e["type"] == "response.done" for e, _ in sent):
                await asyncio.sleep(0)
            return next(e["response"] for e, _ in sent if e["type"] == "response.done")
        first = await asyncio.wait_for(sealed(), 1)
        result = json.dumps({"type": "conversation.item.create", "event_id": "fixture-result",
            "item": {"type": "function_call_output", "call_id": first["output"][0]["call_id"], "output": "ok"}})
        if result_received:
            await protocol.handle_client_message(result, "client")
        assert protocol.active_response_id is None
        await protocol.handle_client_message('{"type":"response.cancel"}', "client")
        assert cancelled == ["client"]
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(pending, 1)
        done = [e["response"] for e, _ in sent if e["type"] == "response.done"]
        assert len(done) == 2
        assert done[-1]["status"] == "cancelled"
        assert done[-1]["id"] != first["id"]  # Do not re-seal the completed function response.
        assert protocol.active_response_id is None
        assert protocol.processing_turn_id is None
        assert protocol._pending_tool is None
        await protocol.handle_client_message(result, "client")
        assert sent[-1][0]["error"]["code"] == "unknown_tool_call"
        assert sent[-1][0]["error"]["event_id"] == "fixture-result"
        # The same transport can own a fresh turn after cancellation.
        await protocol.processing_started()
        await protocol.assistant_transcript("New answer.")
        await protocol.output_stopped()
        assert sent[-1][0]["response"]["output"][0]["content"][0]["transcript"] == "New answer."
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.asyncio
async def test_delayed_cancel_callback_cannot_cancel_a_replacement_turn():
    entered, release = asyncio.Event(), asyncio.Event()
    async def on_cancel(identity):
        entered.set()
        await release.wait()

    protocol, sent = protocol_fixture(on_response_cancelled=on_cancel)
    await protocol.processing_started()
    cancel = asyncio.create_task(protocol.handle_client_message('{"type":"response.cancel"}', "client"))
    replacement = None
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await protocol.response_cancelled()
        await protocol.processing_started()
        replacement_turn = protocol.processing_turn_id
        replacement = asyncio.create_task(protocol.request_client_tool("new", {}))
        async def tool_pending():
            while protocol._pending_tool is None:
                await asyncio.sleep(0)
            return protocol._pending_tool["call_id"]
        call_id = await asyncio.wait_for(tool_pending(), 1)
        release.set()
        await asyncio.wait_for(cancel, 1)
        assert protocol.processing_turn_id is replacement_turn
        assert protocol._pending_tool is not None
        assert not replacement.done()
        await protocol.handle_client_message(json.dumps({"type": "conversation.item.create",
            "item": {"type": "function_call_output", "call_id": call_id, "output": "new result"}}), "client")
        await protocol.handle_client_message('{"type":"response.create"}', "client")
        assert await asyncio.wait_for(replacement, 1) == "new result"
    finally:
        release.set()
        for task in (cancel, replacement):
            if task is not None:
                task.cancel()
        await asyncio.gather(*(task for task in (cancel, replacement) if task is not None), return_exceptions=True)


@pytest.mark.asyncio
async def test_idle_wire_cancel_does_not_create_a_response():
    cancelled = []
    protocol, sent = protocol_fixture(on_response_cancelled=lambda identity: cancelled.append(identity))
    await protocol.handle_client_message('{"type":"response.cancel","event_id":"idle-cancel"}', "client")
    assert cancelled == []
    assert len(sent) == 1
    assert sent[0][0]["error"]["code"] == "no_active_response"
    assert sent[0][0]["error"]["event_id"] == "idle-cancel"


@pytest.mark.asyncio
async def test_response_create_requires_a_new_queued_input() -> None:
    inputs: list[tuple[str, str]] = []

    async def on_text(text: str, identity: str) -> None:
        inputs.append((text, identity))

    protocol, sent = protocol_fixture(on_text_input=on_text)
    item = {
        "type": "conversation.item.create",
        "item": {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "one turn"}],
        },
    }
    await protocol.handle_client_message(json.dumps(item), "client-a")
    await protocol.handle_client_message(json.dumps({"type": "response.create"}), "client-a")
    await protocol.handle_client_message(json.dumps({"type": "response.create"}), "client-a")

    assert inputs == [("one turn", "client-a")]
    assert sent[-1][0]["type"] == "error"
    assert sent[-1][0]["error"]["code"] == "response_create_unsupported"


@pytest.mark.asyncio
async def test_response_create_without_new_input_uses_request_callback() -> None:
    requested: list[str] = []

    async def on_response(identity: str) -> None:
        requested.append(identity)

    protocol, sent = protocol_fixture(on_response_requested=on_response)
    await protocol.handle_client_message(
        json.dumps({"type": "response.create"}),
        "client-a",
    )

    assert requested == ["client-a"]
    assert sent == []


@pytest.mark.asyncio
async def test_does_not_replace_a_pending_typed_input() -> None:
    protocol, sent = protocol_fixture()

    def item(text: str) -> str:
        return json.dumps({
            "type": "conversation.item.create",
            "item": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": text}],
            },
        })

    await protocol.handle_client_message(item("first"), "client-a")
    await protocol.handle_client_message(item("second"), "client-a")

    assert sent[-1][0]["type"] == "error"
    assert sent[-1][0]["error"]["code"] == "conversation_item_pending"


@pytest.mark.asyncio
async def test_rejects_oversized_and_non_object_client_events() -> None:
    protocol, sent = protocol_fixture()

    await protocol.handle_client_message(b"x" * (256 * 1024 + 1), "client-a")
    await protocol.handle_client_message("[]", "client-a")

    assert [event["error"]["code"] for event, _ in sent] == [
        "event_too_large",
        "invalid_event_json",
    ]
    assert all(recipient == "client-a" for _, recipient in sent)


@pytest.mark.asyncio
async def test_function_call_waits_for_output_and_response_create() -> None:
    protocol, sent = protocol_fixture()
    await protocol.client_connected("client-a")
    await protocol.response_started()

    result = asyncio.create_task(
        protocol.request_client_tool("fixture_echo", {"value": "ready"})
    )
    await asyncio.sleep(0)
    argument_event = next(
        event for event, _ in sent
        if event["type"] == "response.function_call_arguments.done"
    )
    call_id = argument_event["call_id"]

    assert argument_event["name"] == "fixture_echo"
    assert json.loads(argument_event["arguments"]) == {"value": "ready"}
    assert [
        event["response"]["status"] for event, _ in sent
        if event["type"] == "response.done"
    ] == ["completed"]

    await protocol.handle_client_message(
        json.dumps({
            "type": "conversation.item.create",
            "event_id": "tool-output",
            "item": {
                "type": "function_call_output",
                "call_id": call_id,
                "output": "client-result",
            },
        }),
        "client-a",
    )
    assert result.done() is False
    assert any(
        event["type"] == "conversation.item.done"
        and event["item"]["type"] == "function_call_output"
        for event, _ in sent
    )

    await protocol.handle_client_message(
        json.dumps({"type": "response.create", "event_id": "tool-continue"}),
        "client-a",
    )

    assert await result == "client-result"
    assert sent[-1][0]["type"] == "response.created"


@pytest.mark.asyncio
async def test_function_output_rejects_unknown_call_ids() -> None:
    protocol, sent = protocol_fixture()
    await protocol.handle_client_message(
        json.dumps({
            "type": "conversation.item.create",
            "event_id": "unknown-output",
            "item": {
                "type": "function_call_output",
                "call_id": "call_unknown",
                "output": "nope",
            },
        }),
        "client-a",
    )

    assert sent[-1][0]["error"]["code"] == "unknown_tool_call"
    assert sent[-1][0]["error"]["event_id"] == "unknown-output"


@pytest.mark.asyncio
async def test_function_output_rejects_duplicates_and_oversized_values() -> None:
    protocol, sent = protocol_fixture()
    await protocol.client_connected("client-a")
    pending = asyncio.create_task(protocol.request_client_tool("fixture_echo", {}))
    await asyncio.sleep(0)
    call_id = next(
        event["call_id"]
        for event, _ in sent
        if event["type"] == "response.function_call_arguments.done"
    )

    def output(value: str) -> str:
        return json.dumps({
            "type": "conversation.item.create",
            "item": {
                "type": "function_call_output",
                "call_id": call_id,
                "output": value,
            },
        })

    await protocol.handle_client_message(output("x" * (64 * 1024 + 1)), "client-a")
    assert sent[-1][0]["error"]["code"] == "invalid_tool_output"
    await protocol.handle_client_message(output("ok"), "client-a")
    await protocol.handle_client_message(output("again"), "client-a")
    assert sent[-1][0]["error"]["code"] == "duplicate_tool_output"
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending


@pytest.mark.asyncio
async def test_oversized_tool_arguments_fail_before_pending_state() -> None:
    protocol, _sent = protocol_fixture()
    with pytest.raises(RuntimeError, match="too large"):
        await protocol.request_client_tool("fixture_echo", {"value": "x" * (64 * 1024)})
    assert protocol._pending_tool is None


@pytest.mark.asyncio
async def test_cancelled_tool_wait_clears_pending_state_and_rejects_late_output() -> None:
    protocol, sent = protocol_fixture(tool_names={"fixture_echo"})
    await protocol.client_connected("client-a")
    pending = asyncio.create_task(
        protocol.request_client_tool("fixture_echo", {"value": "ready"})
    )
    await asyncio.sleep(0)
    call_id = next(
        event["call_id"]
        for event, _ in sent
        if event["type"] == "response.function_call_arguments.done"
    )

    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert protocol._pending_tool is None

    await protocol.handle_client_message(
        json.dumps({
            "type": "conversation.item.create",
            "item": {
                "type": "function_call_output",
                "call_id": call_id,
                "output": "late",
            },
        }),
        "client-a",
    )
    assert sent[-1][0]["error"]["code"] == "unknown_tool_call"


@pytest.mark.asyncio
async def test_tool_wait_times_out_and_clears_pending_state() -> None:
    protocol, sent = protocol_fixture(
        tool_names={"fixture_echo"}, tool_timeout_seconds=0.001
    )
    await protocol.client_connected("client-a")

    with pytest.raises(RuntimeError, match="timeout"):
        await protocol.request_client_tool("fixture_echo", {})

    assert protocol._pending_tool is None
    assert sent[-1][0]["error"]["code"] == "tool_timeout"
    assert sent[-1][1] == "client-a"


@pytest.mark.asyncio
async def test_protocol_close_cancels_pending_tool_wait() -> None:
    protocol, _sent = protocol_fixture()
    pending = asyncio.create_task(protocol.request_client_tool("fixture_echo", {}))
    await asyncio.sleep(0)
    await protocol.close()

    with pytest.raises(asyncio.CancelledError):
        await pending
    assert protocol._pending_tool is None


@pytest.mark.asyncio
async def test_session_update_accepts_required_and_named_function_choices() -> None:
    protocol, sent = protocol_fixture(tool_names={"fixture_echo"})
    for choice in (
        "required",
        {"type": "function", "name": "fixture_echo"},
    ):
        await protocol.handle_client_message(
            json.dumps({
                "type": "session.update",
                "session": {"type": "realtime", "tool_choice": choice},
            }),
            "client-a",
        )
        assert protocol.tool_choice == choice
        assert sent[-1][0]["type"] == "session.updated"
