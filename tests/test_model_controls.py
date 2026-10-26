from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock
import asyncio
import json

import pytest

from gateway.config import Platform
from hermes_realtime.model_controls import ModelControlError, resolve_selection, select_model
from hermes_realtime.realtime_protocol import RealtimeProtocol

CATALOG = [{"id": "fast", "alias_for": "fixture-fast", "available": True, "capabilities": {"reasoning_effort": True}},
    {"id": "smart", "alias_for": "fixture-smart", "available": False},
    {"id": "plain", "available": True, "capabilities": {}}]


def test_catalog_resolution_preserves_intent_and_rejects_unavailable_or_unsupported_targets():
    assert resolve_selection(CATALOG, "fast", "low") == "fixture-fast"
    for model, effort, code in [("smart", None, "model_unavailable"), ("absent", None, "model_unavailable"),
        ("plain", "high", "unsupported_reasoning_effort"), ("fast --global", None, "invalid_model"),
        ("fast", "ultra", "unsupported_reasoning_effort")]:
        with pytest.raises(ModelControlError) as error:
            resolve_selection(CATALOG, model, effort)
        assert error.value.code == code


@pytest.fixture
def native_runner(monkeypatch):
    from gateway.slash_commands_model import _ModelSwitchContext
    monkeypatch.setattr(_ModelSwitchContext, "read_config", lambda ctx: setattr(ctx, "user_provs", {
        "kortexa.ai": {"api": "https://api.example.test/v1", "api_key": "fixture-key"}}))
    monkeypatch.setattr("hermes_realtime.model_controls._catalog", AsyncMock(return_value=CATALOG))
    monkeypatch.setattr("hermes_cli.model_selection_guards.combined_selection_warning", lambda *a, **kw: None)
    monkeypatch.setattr("hermes_cli.model_selection_guards.selection_context_for_agent", lambda agent: None)
    result = SimpleNamespace(success=True, new_model="fixture-fast", target_provider="custom:kortexa.ai",
        base_url="https://api.example.test/v1", api_key="fixture-key", model_info=None)
    runner = SimpleNamespace(_async_profile_scope_for_source=lambda source: nullcontext(),
        _model_switch_lock=lambda: asyncio.Lock(), _session_key_for_source=lambda source: "fixture-route",
        _session_model_override=lambda route: None, _cached_agent_for=lambda route: None,
        _perform_model_switch=AsyncMock(return_value=(result, None)), _record_model_switch=AsyncMock(),
        _apply_reasoning_selection=lambda *a, **kw: None)
    return runner


@pytest.mark.asyncio
async def test_selection_uses_the_native_next_turn_commit_and_never_global_config(native_runner):
    source = SimpleNamespace(platform=Platform.LOCAL)
    result = await select_model(SimpleNamespace(gateway_runner=native_runner), source, "fast", "low")
    assert result == {"model": "fixture-fast", "reasoning_effort": "low"}
    args, kwargs = native_runner._record_model_switch.await_args
    assert args[1].persist_global is False
    assert kwargs == {"source": source, "one_turn": False, "picker": True}
    assert native_runner._perform_model_switch.await_args.args[1:3] == ("fixture-fast", "kortexa.ai")


@pytest.mark.asyncio
async def test_stale_selection_and_native_guard_never_commit(native_runner, monkeypatch):
    adapter, source = SimpleNamespace(gateway_runner=native_runner), SimpleNamespace(platform=Platform.LOCAL)
    with pytest.raises(ModelControlError, match="replaced"):
        await select_model(adapter, source, "fast", None, is_current=lambda: False)
    native_runner._record_model_switch.assert_not_awaited()
    monkeypatch.setattr("hermes_cli.model_selection_guards.combined_selection_warning", lambda *a, **kw: object())
    with pytest.raises(ModelControlError) as error:
        await select_model(adapter, source, "fast", None)
    assert error.value.code == "model_confirmation_required"
    native_runner._record_model_switch.assert_not_awaited()


@pytest.mark.asyncio
async def test_model_ack_is_deferred_correlated_and_does_not_block_other_controls():
    sent = []
    ready = asyncio.get_running_loop().create_future()
    async def publish(event, recipient):
        sent.append(event)
        return True
    async def select(*args):
        return await ready
    protocol = RealtimeProtocol(session_id="fixture", model="previous", voice="fixture", publish=publish,
                                on_model_selection=select, instructions="Original")
    await protocol.client_connected("owner")
    await protocol.handle_client_message(json.dumps({"type": "session.update", "event_id": "pick",
        "session": {"model": "fast", "instructions": "New", "reasoning_effort": "low"}}), "owner")
    assert protocol.model == "previous" and protocol.instructions == "Original"
    await protocol.handle_client_message(json.dumps({"type": "session.update", "event_id": "racing",
        "session": {"model": "smart"}}), "owner")
    assert sent[-1]["error"]["code"] == "session_update_busy"
    assert sent[-1]["error"]["event_id"] == "racing"
    task = protocol._selection_task
    ready.set_result({"model": "fixture-fast", "reasoning_effort": "low"})
    await task
    assert sent[-1]["client_event_id"] == "pick"
    assert sent[-1]["session"]["reasoning_effort"] == "low"
    assert protocol.model == "fixture-fast" and protocol.instructions == "New"


@pytest.mark.asyncio
async def test_rejected_and_disconnected_picks_retain_the_old_state():
    sent = []
    async def publish(event, recipient):
        sent.append(event)
        return True
    async def reject(*args):
        raise ModelControlError("model_unavailable", "Unavailable")
    protocol = RealtimeProtocol(session_id="fixture", model="previous", voice="fixture", publish=publish,
                                on_model_selection=reject)
    await protocol.client_connected("owner")
    event = json.dumps({"type": "session.update", "event_id": "pick", "session": {"model": "smart"}})
    await protocol.handle_client_message(event, "owner")
    await protocol._selection_task
    assert protocol.model == "previous" and sent[-1]["error"]["event_id"] == "pick"
    wait = asyncio.Event()
    protocol._on_model_selection = lambda *args: wait.wait()
    await protocol.handle_client_message(event, "owner")
    task = protocol._selection_task
    await asyncio.sleep(0)
    protocol.client_disconnected("owner")
    await asyncio.gather(task, return_exceptions=True)
    assert protocol.model == "previous"
    await protocol.close()
