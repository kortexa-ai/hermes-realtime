"""Session-owned client function tools for the direct Realtime transport."""

from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any, Iterable

from .tool_model import (
    FunctionToolDefinition,
    ToolDefinitionError,
    parse_function_tools,
    parse_tool_choice,
)
from .client_image import decode_client_image


DIRECT_TOOLSET_NAME = "realtime-client-tools"
DIRECT_TOOL_PREFIX = "rt_"
class DirectToolError(ToolDefinitionError):
    """The OpenAI Realtime tool setup is invalid or unsupported."""


DirectToolDefinition = FunctionToolDefinition


def resolve_gateway_session_key(session_id: Any) -> str | None:
    """Resolve Hermes's persisted session ID to its gateway routing key."""
    if not isinstance(session_id, str) or not session_id:
        return None
    try:
        from hermes_state import SessionDB

        database = SessionDB(read_only=True)
        try:
            session = database.get_session(session_id)
        finally:
            database.close()
    # A missing/corrupt session database must not block the remote tool call;
    # the persisted ID remains a safe fallback for cancellation ownership.
    except Exception:  # noqa: BLE001
        return None
    key = session.get("session_key") if isinstance(session, dict) else None
    return key if isinstance(key, str) and key else None


def parse_direct_tools(
    session: dict[str, Any],
) -> tuple[list[DirectToolDefinition], str | dict[str, str]]:
    """Validate the portable OpenAI function-tool subset used by Hermes Direct."""
    raw_tools = session.get("tools", [])
    try:
        parsed = parse_function_tools(
            raw_tools, nested=False, allow_dotted_names=False
        )
        choice = parse_tool_choice(
            session.get("tool_choice", "auto"), {tool.name for tool in parsed}
        )
    except ToolDefinitionError as exc:
        raise DirectToolError(str(exc)) from exc
    return parsed, choice


def install_direct_toolsets() -> None:
    """Declare the direct platform bundle without requiring a Hermes patch."""
    from toolsets import TOOLSETS, _HERMES_CORE_TOOLS

    TOOLSETS.setdefault(
        DIRECT_TOOLSET_NAME,
        {
            "description": "Tools offered by the active OpenAI Realtime client",
            "tools": [],
            "includes": [],
        },
    )
    # Hermes defers non-core plugin tools behind tool_search by default. A
    # client-provided Realtime function is an active-session surface, like a
    # desktop UI callback, and must stay in the model's native tools array to
    # preserve OpenAI tool semantics. Extend the existing runtime category;
    # no hermes-agent source change is required.
    try:
        from tools import tool_search

        tool_search._DIRECT_SURFACE_TOOLSETS = frozenset({
            *tool_search._DIRECT_SURFACE_TOOLSETS,
            DIRECT_TOOLSET_NAME,
        })
    except Exception:
        pass
    TOOLSETS.setdefault(
        "hermes-realtime",
        {
            "description": "Direct Realtime voice toolset",
            "tools": list(_HERMES_CORE_TOOLS),
            "includes": [DIRECT_TOOLSET_NAME],
        },
    )


class DirectToolBridge:
    """Register one call's tools and proxy invocations over its data channel."""

    def __init__(
        self, *, session_id: str, protocol: Any, session_toolset_factory: Any = None,
    ) -> None:
        self.session_id = session_id
        self.protocol = protocol
        self._session_toolset_factory = session_toolset_factory
        self._session_toolset = None
        self._registered: dict[str, tuple[str, Any, str | None]] = {}
        self._definitions: dict[str, DirectToolDefinition] = {}
        self._call_lock = asyncio.Lock()
        try:
            self._owner_loop: asyncio.AbstractEventLoop | None = (
                asyncio.get_running_loop()
            )
        except RuntimeError:
            self._owner_loop = None

    def register(self, tools: Iterable[DirectToolDefinition]) -> None:
        if callable(self._session_toolset_factory):
            self._register_public(tools)
            return
        from tools.registry import registry
        from toolsets import TOOLSETS

        install_direct_toolsets()
        static_names = TOOLSETS[DIRECT_TOOLSET_NAME]["tools"]
        try:
            for tool in tools:
                registry_name = self._scoped_name(self.session_id, tool.name)
                handler = self._handler(tool.name)
                scope = registry.plugin_scope_for_callable(handler)
                if registry.get_entry(registry_name, scope=scope) is not None:
                    raise DirectToolError(f"tool registry collision: {tool.name}")
                if registry_name not in static_names:
                    static_names.append(registry_name)
                registry.register(
                    name=registry_name,
                    toolset=DIRECT_TOOLSET_NAME,
                    schema=tool.registry_schema(registry_name),
                    handler=handler,
                    is_async=True,
                    description=tool.description,
                    scope=scope,
                )
                entry = registry.get_entry(registry_name, scope=scope)
                if entry is None:
                    raise DirectToolError(f"failed to register tool: {tool.name}")
                self._registered[registry_name] = (tool.name, entry, scope)
                self._definitions[registry_name] = tool
        except Exception:
            self.close()
            raise

    def _register_public(self, tools: Iterable[DirectToolDefinition]) -> None:
        lease = self._session_toolset_factory(
            self.session_id,
            name=DIRECT_TOOLSET_NAME,
            description="Tools offered by the active OpenAI Realtime client",
            direct=True,
        )
        self._session_toolset = lease
        try:
            for tool in tools:
                registry_name = lease.register_tool(
                    tool.name,
                    schema=tool.registry_schema(tool.name),
                    handler=self._handler(tool.name),
                    is_async=True,
                    description=tool.description,
                )
                self._registered[registry_name] = (tool.name, None, None)
                self._definitions[registry_name] = tool
        except Exception:
            self.close()
            raise

    def replace(self, tools: Iterable[DirectToolDefinition]) -> None:
        previous = list(self._definitions.values())
        self.close()
        try:
            self.register(tools)
        except Exception:
            self.register(previous)
            raise

    def set_tool_choice(self, tool_choice: str | dict[str, str]) -> None:
        """Expose only the registry entries allowed for the next Hermes turn."""
        if self._session_toolset is not None:
            # Public session toolsets are cache-stable after their first turn.
            # The handler and prompt enforce tool_choice without rewriting the
            # model's cached schema mid-conversation.
            return
        from toolsets import TOOLSETS

        static_names = TOOLSETS[DIRECT_TOOLSET_NAME]["tools"]
        selected_name = tool_choice.get("name") if isinstance(tool_choice, dict) else None
        for registry_name, definition in self._definitions.items():
            exposed = tool_choice != "none" and (
                selected_name is None or definition.name == selected_name
            )
            if exposed and registry_name not in static_names:
                static_names.append(registry_name)
            while not exposed and registry_name in static_names:
                static_names.remove(registry_name)

    def close(self) -> None:
        if self._session_toolset is not None:
            self._session_toolset.dispose()
            self._session_toolset = None
            self._registered.clear()
            self._definitions.clear()
            return
        from tools.registry import registry
        from toolsets import TOOLSETS

        static_names = TOOLSETS.get(DIRECT_TOOLSET_NAME, {}).get("tools", [])
        for registry_name, (_advertised_name, entry, scope) in tuple(
            self._registered.items()
        ):
            try:
                registry.restore_registration(
                    registry_name, entry, None, scope=scope
                )
            finally:
                while registry_name in static_names:
                    static_names.remove(registry_name)
        self._registered.clear()
        self._definitions.clear()

    def prompt_hint(self, tool_choice: str | dict[str, str] = "auto") -> str:
        """Describe deferred client tools in an ephemeral per-turn prompt."""
        selected_name = tool_choice.get("name") if isinstance(tool_choice, dict) else None
        tools = [
            {
                "client_name": definition.name,
                "internal_name": registry_name,
                "description": definition.description,
                "parameters": definition.parameters,
            }
            for registry_name, definition in self._definitions.items()
            if selected_name is None or definition.name == selected_name
        ]
        if not tools:
            return ""
        catalog = json.dumps(tools, ensure_ascii=False, separators=(",", ":"))
        choice_instruction = ""
        if tool_choice == "required":
            choice_instruction = (
                "You must invoke at least one supplied function before answering. "
            )
        elif isinstance(tool_choice, dict):
            advertised_name = tool_choice["name"]
            registry_name = next(
                name
                for name, definition in self._definitions.items()
                if definition.name == advertised_name
            )
            choice_instruction = (
                f"You must invoke the function with internal_name {registry_name!r} "
                "before answering. "
            )
        return (
            "This Realtime call has client-provided function tools. Their exact "
            "schemas and call-scoped internal names are in the JSON below. When "
            "a function is appropriate, invoke the function with its exact "
            "internal_name and matching arguments. If the user explicitly asks "
            "to use a supplied function, you must invoke it before answering. "
            "In a phrase of the form 'with <parameter-name> <value>', the "
            "word after 'with' names the argument and the remaining text is "
            "its value; for example, 'with value ready' means "
            "{\"value\":\"ready\"}. Do not ask for clarification when all "
            "required arguments are present in the request. "
            "Never reveal the internal name. "
            f"{choice_instruction}\n"
            f"{catalog}"
        )

    def _handler(self, advertised_name: str):
        async def proxy(args: dict[str, Any] | None = None, **kwargs: Any) -> Any:
            if not any(definition.name == advertised_name for definition in self._definitions.values()):
                raise RuntimeError("realtime client tool was disabled")
            invocation_session = kwargs.get("session_id")
            if not self._owns_invocation(invocation_session):
                raise RuntimeError("realtime tool invoked outside its owning session")
            tool_choice = self.protocol.tool_choice
            if tool_choice == "none":
                raise RuntimeError("realtime tool invocation disabled by tool_choice")
            if isinstance(tool_choice, dict) and tool_choice["name"] != advertised_name:
                raise RuntimeError("realtime tool invocation does not match tool_choice")
            arguments = dict(args or {})
            running_loop = asyncio.get_running_loop()
            owner_loop = self._owner_loop
            if owner_loop is None:
                self._owner_loop = running_loop
                owner_loop = running_loop
            if owner_loop is running_loop:
                return await self._request(advertised_name, arguments)
            result = asyncio.run_coroutine_threadsafe(
                self._request(advertised_name, arguments), owner_loop
            )
            return await asyncio.wrap_future(result)

        return proxy

    def _owns_invocation(self, session_id: Any) -> bool:
        if session_id == self.session_id:
            return True
        if not isinstance(session_id, str) or not session_id:
            return False
        return self._lookup_session_key(session_id) == self.session_id

    @staticmethod
    def _lookup_session_key(session_id: str) -> str | None:
        """Resolve Hermes's persisted session ID to its gateway routing key."""
        return resolve_gateway_session_key(session_id)

    async def _request(self, name: str, arguments: dict[str, Any]) -> Any:
        processing_turn = self.protocol.processing_turn_id
        async with self._call_lock:
            # A sibling may have been waiting behind a cancelled tool. Never
            # let that queued invocation reopen or join a replacement turn.
            if processing_turn is None or self.protocol.processing_turn_id is not processing_turn:
                raise asyncio.CancelledError()
            result = await self.protocol.request_client_tool(name, arguments)
            if not any(definition.name == name for definition in self._definitions.values()):
                raise RuntimeError("realtime client tool was disabled")
            return decode_client_image(result)

    @staticmethod
    def _scoped_name(session_id: str, advertised_name: str) -> str:
        digest = hashlib.sha256(
            session_id.encode("utf-8") + b"\0" + advertised_name.encode("ascii")
        ).hexdigest()[:16]
        readable_limit = 64 - len(DIRECT_TOOL_PREFIX) - len(digest) - 1
        return f"{DIRECT_TOOL_PREFIX}{digest}_{advertised_name[:readable_limit]}"
