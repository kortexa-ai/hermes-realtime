"""Direct Realtime client tools remain call-owned at the Hermes registry edge."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from hermes_realtime.direct_tools import (
    DIRECT_TOOLSET_NAME,
    DirectToolBridge,
    DirectToolError,
    parse_direct_tools,
)
from tools.registry import registry
import base64
import json


def direct_session(*, choice: str = "auto") -> dict:
    return {
        "type": "realtime",
        "tools": [{
            "type": "function",
            "name": "fixture_echo",
            "description": "Return a value.",
            "parameters": {
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
            },
        }],
        "tool_choice": choice,
    }


@pytest.mark.asyncio
async def test_compact_snapshot_direct_result_and_disabled_handler():
    protocol = AsyncMock()
    protocol.tool_choice = "auto"
    data = base64.b64encode(bytes([255,216,255,192,0,8,8,0,4,0,6,1,255,217])).decode()
    protocol.request_client_tool.return_value = json.dumps({"type": "client_image", "version": 1, "mime_type": "image/jpeg", "data": data, "width": 6, "height": 4, "text": "Synthetic snapshot"})
    tools, _ = parse_direct_tools(direct_session())
    bridge = DirectToolBridge(session_id="owned", protocol=protocol)
    bridge.register(tools)
    entry = registry.get_entry(next(iter(bridge._registered)))
    try:
        result = await entry.handler({}, session_id="owned")
        assert result["_multimodal"] is True
        assert result["content"][1]["image_url"]["url"] == "data:image/jpeg;base64," + data
        bridge.replace([])
        with pytest.raises(RuntimeError, match="disabled"):
            await entry.handler({}, session_id="owned")
    finally:
        bridge.close()


def test_parses_openai_flat_function_tools_and_supported_choices() -> None:
    tools, choice = parse_direct_tools(direct_session())

    assert choice == "auto"
    assert tools[0].name == "fixture_echo"
    assert tools[0].parameters["type"] == "object"

    required_tools, required = parse_direct_tools(direct_session(choice="required"))
    assert required == "required"
    assert required_tools == tools

    disabled_tools, disabled = parse_direct_tools(direct_session(choice="none"))
    assert disabled == "none"
    assert disabled_tools == tools

    named_session = direct_session()
    named_session["tool_choice"] = {"type": "function", "name": "fixture_echo"}
    _, named = parse_direct_tools(named_session)
    assert named == {"type": "function", "name": "fixture_echo"}


@pytest.mark.parametrize(
    ("tools", "message"),
    [
        ("bad", "tools must be an array"),
        ([{"type": "function"}], "name is invalid"),
        ([{
            "type": "function",
            "name": "fixture_echo",
            "parameters": {"type": "string"},
        }], "object schema"),
        ([{
            "type": "function",
            "name": "fixture_echo",
            "parameters": {"type": "object", "default": float("nan")},
        }], "valid JSON"),
        ([direct_session()["tools"][0]] * 2, "duplicate tool"),
    ],
)
def test_rejects_malformed_and_duplicate_direct_tools(tools, message: str) -> None:
    session = direct_session()
    session["tools"] = tools

    with pytest.raises(DirectToolError, match=message):
        parse_direct_tools(session)


def test_bounds_direct_tool_catalog_and_choice() -> None:
    session = direct_session()
    session["tools"][0]["description"] = "x" * (4 * 1024 + 1)
    with pytest.raises(DirectToolError, match="description is too large"):
        parse_direct_tools(session)

    session = direct_session()
    session["tools"] = [
        {**session["tools"][0], "name": f"tool_{index}"}
        for index in range(17)
    ]
    with pytest.raises(DirectToolError, match="too many tools"):
        parse_direct_tools(session)

    session = direct_session()
    session["tools"][0]["parameters"]["properties"] = {
        "value": {"type": "string", "description": "x" * (64 * 1024)}
    }
    with pytest.raises(DirectToolError, match="schemas are too large"):
        parse_direct_tools(session)

    with pytest.raises(DirectToolError, match="needs at least one tool"):
        parse_direct_tools({"tools": [], "tool_choice": "required"})
    session = direct_session()
    session["tool_choice"] = {"type": "function", "name": "missing"}
    with pytest.raises(DirectToolError, match="declared function"):
        parse_direct_tools(session)


@pytest.mark.asyncio
async def test_scoped_handler_checks_hermes_session_before_proxying(monkeypatch) -> None:
    protocol = AsyncMock()
    protocol.tool_choice = "auto"
    protocol.request_client_tool.return_value = "client-result"
    tools, _choice = parse_direct_tools(direct_session())
    bridge = DirectToolBridge(session_id="agent:main:realtime:dm:call-a", protocol=protocol)
    bridge.register(tools)
    registry_name = next(iter(bridge._registered))
    entry = registry.get_entry(registry_name)
    assert entry is not None
    assert "fixture_echo" in bridge.prompt_hint()
    assert registry_name in bridge.prompt_hint()

    try:
        with pytest.raises(RuntimeError, match="outside its owning session"):
            await entry.handler(
                {"value": "wrong"},
                session_id="agent:main:realtime:dm:call-b",
            )
        monkeypatch.setattr(
            bridge,
            "_lookup_session_key",
            lambda session_id: (
                "agent:main:realtime:dm:call-a"
                if session_id == "20260827_185359_e806d4ac"
                else None
            ),
        )
        assert await asyncio.to_thread(
            lambda: asyncio.run(entry.handler(
                {"value": "ready"},
                session_id="20260827_185359_e806d4ac",
            ))
        ) == "client-result"
        protocol.request_client_tool.assert_awaited_once_with(
            "fixture_echo", {"value": "ready"}
        )
    finally:
        bridge.close()

    assert registry.get_entry(registry_name) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("replace", [False, True])
async def test_queued_tools_cannot_resume_after_their_turn_is_cancelled(replace):
    from hermes_realtime.realtime_protocol import RealtimeProtocol

    events, sealed = [], asyncio.Event()
    async def publish(event, recipient):
        events.append(event)
        if event["type"] == "response.done":
            sealed.set()
        return True

    protocol = RealtimeProtocol(session_id="cancel", model="test", voice="test", publish=publish)
    bridge = DirectToolBridge(session_id="owned", protocol=protocol)
    await protocol.processing_started()
    first = asyncio.create_task(bridge._request("first", {}))
    second = None
    try:
        await asyncio.wait_for(sealed.wait(), 1)
        second = asyncio.create_task(bridge._request("second", {}))
        await asyncio.sleep(0)  # Second invocation is now queued on the bridge lock.
        await protocol.response_cancelled()
        # Core cancellation also stops the first worker; this isolates the
        # queued invocation's turn fence from the protocol's pending future.
        first.cancel()
        if replace:
            await protocol.processing_started()
        response_id = protocol.active_response_id
        results = await asyncio.wait_for(asyncio.gather(first, second, return_exceptions=True), 1)
        assert all(isinstance(result, asyncio.CancelledError) for result in results)
        calls = [e for e in events if e["type"] == "response.function_call_arguments.done"]
        assert [e["name"] for e in calls] == ["first"]
        assert protocol.active_response_id == response_id
        assert protocol._pending_tool is None
    finally:
        for task in (first, second):
            if task is not None:
                task.cancel()
        await asyncio.gather(*(task for task in (first, second) if task is not None), return_exceptions=True)


@pytest.mark.asyncio
async def test_parallel_direct_invocations_are_serialized_deterministically() -> None:
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    calls: list[str] = []

    async def request(name: str, _arguments: dict) -> str:
        calls.append(name)
        if name == "first":
            first_started.set()
            await release_first.wait()
        return name

    protocol = AsyncMock()
    protocol.tool_choice = "auto"
    protocol.request_client_tool.side_effect = request
    session = direct_session()
    session["tools"] = [
        {**session["tools"][0], "name": "first"},
        {**session["tools"][0], "name": "second"},
    ]
    tools, _ = parse_direct_tools(session)
    bridge = DirectToolBridge(session_id="owned", protocol=protocol)
    bridge.register(tools)
    entries = {
        advertised: registry.get_entry(registry_name)
        for registry_name, (advertised, _entry, _scope) in bridge._registered.items()
    }
    try:
        first = asyncio.create_task(entries["first"].handler({}, session_id="owned"))
        await first_started.wait()
        second = asyncio.create_task(entries["second"].handler({}, session_id="owned"))
        await asyncio.sleep(0)
        assert calls == ["first"]
        release_first.set()
        assert await asyncio.gather(first, second) == ["first", "second"]
        assert calls == ["first", "second"]
    finally:
        bridge.close()


def test_required_and_named_choices_are_injected_into_the_turn_prompt() -> None:
    protocol = AsyncMock()
    protocol.tool_choice = "auto"
    tools, _ = parse_direct_tools(direct_session())
    bridge = DirectToolBridge(session_id="owned", protocol=protocol)
    bridge.register(tools)
    try:
        assert "must invoke at least one" in bridge.prompt_hint("required")
        named = bridge.prompt_hint({"type": "function", "name": "fixture_echo"})
        assert "must invoke the function" in named
        assert next(iter(bridge._registered)) in named
    finally:
        bridge.close()


def test_tool_choice_limits_the_exposed_hermes_registry_surface() -> None:
    protocol = AsyncMock()
    session = direct_session()
    session["tools"] = [
        session["tools"][0],
        {**session["tools"][0], "name": "other"},
    ]
    tools, _ = parse_direct_tools(session)
    bridge = DirectToolBridge(session_id="owned", protocol=protocol)
    bridge.register(tools)
    try:
        from toolsets import TOOLSETS

        static_names = TOOLSETS[DIRECT_TOOLSET_NAME]["tools"]
        bridge.set_tool_choice("none")
        assert not set(bridge._registered).intersection(static_names)
        bridge.set_tool_choice({"type": "function", "name": "fixture_echo"})
        exposed = set(bridge._registered).intersection(static_names)
        assert len(exposed) == 1
        assert bridge._definitions[next(iter(exposed))].name == "fixture_echo"
        bridge.set_tool_choice("required")
        assert set(bridge._registered).issubset(static_names)
    finally:
        bridge.close()


@pytest.mark.asyncio
async def test_handler_enforces_none_and_named_function_choices() -> None:
    protocol = AsyncMock()
    session = direct_session()
    session["tools"] = [
        session["tools"][0],
        {**session["tools"][0], "name": "other"},
    ]
    tools, _ = parse_direct_tools(session)
    bridge = DirectToolBridge(session_id="owned", protocol=protocol)
    bridge.register(tools)
    entries = {
        advertised: registry.get_entry(registry_name)
        for registry_name, (advertised, _entry, _scope) in bridge._registered.items()
    }
    try:
        protocol.tool_choice = "none"
        with pytest.raises(RuntimeError, match="disabled"):
            await entries["fixture_echo"].handler({}, session_id="owned")
        protocol.tool_choice = {"type": "function", "name": "fixture_echo"}
        with pytest.raises(RuntimeError, match="does not match"):
            await entries["other"].handler({}, session_id="owned")
    finally:
        bridge.close()


def test_registered_direct_toolset_is_selected_for_realtime_platform() -> None:
    from hermes_cli.tools_config import _get_platform_tools

    protocol = AsyncMock()
    tools, _choice = parse_direct_tools(direct_session())
    bridge = DirectToolBridge(session_id="agent:main:realtime:dm:call-a", protocol=protocol)
    bridge.register(tools)
    try:
        assert DIRECT_TOOLSET_NAME in _get_platform_tools({}, "realtime")
    finally:
        bridge.close()


@pytest.mark.kortexa_hermes
def test_patched_hermes_uses_public_session_toolset_api() -> None:
    from hermes_cli.plugin_session_toolsets import session_toolset_names
    from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest

    session_key = "agent:main:realtime:dm:public-api"
    manager = PluginManager(scope_key=registry.current_scope_key())
    context = PluginContext(
        PluginManifest(name="livekit-public-api", key="livekit-public-api"),
        manager,
    )
    protocol = AsyncMock()
    protocol.tool_choice = "auto"
    protocol.processing_turn_id = object()
    protocol.request_client_tool.return_value = "public-result"
    tools, _choice = parse_direct_tools(direct_session())
    bridge = DirectToolBridge(
        session_id=session_key,
        protocol=protocol,
        session_toolset_factory=context.session_toolset,
    )
    bridge.register(tools)
    registry_name = next(iter(bridge._registered))
    toolset_name = bridge._session_toolset.name
    try:
        assert session_toolset_names(
            session_key, scope=registry.current_scope_key()
        ) == [toolset_name]
        assert registry.dispatch(
            registry_name, {}, gateway_session_key="another-session"
        ).startswith('{"error"')
        assert registry.dispatch(
            registry_name, {"value": "ready"}, gateway_session_key=session_key,
            session_id=session_key,
        ) == "public-result"
    finally:
        bridge.close()
    assert registry.get_entry(registry_name) is None
