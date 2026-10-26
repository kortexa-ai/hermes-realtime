"""Transport-neutral OpenAI-compatible Realtime session state."""

from __future__ import annotations

import asyncio
import inspect
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, Optional

from .tool_model import ToolDefinitionError, parse_tool_choice, parse_function_tools
from .model_controls import ModelControlError, validate_selection


MAX_EVENT_BYTES = 256 * 1024
MAX_TEXT_BYTES = 64 * 1024
MAX_EVENT_ID_BYTES = 128
MAX_INSTRUCTIONS_BYTES = 64 * 1024
MAX_TOOL_ARGUMENT_BYTES = 64 * 1024
MAX_TOOL_OUTPUT_BYTES = 64 * 1024
DEFAULT_TOOL_TIMEOUT_SECONDS = 30.0

Publish = Callable[[dict[str, Any], Optional[str]], Awaitable[bool]]
ClientCallback = Callable[[str], Awaitable[None] | None]
TextCallback = Callable[[str, str], Awaitable[None] | None]
InputAudioStateCallback = Callable[[bool, str], Awaitable[None] | None]
ToolChoiceCallback = Callable[[str | dict[str, str]], Awaitable[None] | None]
HERMES_INPUT_AUDIO_STATE = "hermes.input_audio.state"
HERMES_INPUT_AUDIO_STATE_UPDATED = "hermes.input_audio.state_updated"


async def _call(callback: Callable[..., Any] | None, *args: Any) -> bool:
    if callback is None:
        return False
    result = callback(*args)
    if inspect.isawaitable(result):
        await result
    return True


class RealtimeProtocol:
    """Own one realtime conversation independently of WebRTC or LiveKit."""

    def __init__(
        self,
        *,
        session_id: str,
        model: str,
        voice: str,
        publish: Publish,
        on_text_input: TextCallback | None = None,
        on_response_requested: ClientCallback | None = None,
        on_response_cancelled: ClientCallback | None = None,
        on_input_audio_state: InputAudioStateCallback | None = None,
        on_tool_choice_updated: ToolChoiceCallback | None = None,
        on_model_selection: Callable[..., Awaitable[dict]] | None = None,
        on_tools_updated: Callable[..., Any] | None = None,
        instructions: str = "",
        tool_choice: str | dict[str, str] = "auto",
        tool_names: set[str] | None = None,
        tool_timeout_seconds: float = DEFAULT_TOOL_TIMEOUT_SECONDS,
    ) -> None:
        self.session_id = session_id
        self.model = model
        self.voice = voice
        self._publish = publish
        self._on_text_input = on_text_input
        self._on_response_requested = on_response_requested
        self._on_response_cancelled = on_response_cancelled
        self._on_input_audio_state = on_input_audio_state
        self._on_tool_choice_updated = on_tool_choice_updated
        self._on_model_selection = on_model_selection
        self._on_tools_updated = on_tools_updated
        self.reasoning_effort = None
        self._selection_task = None
        self._selection_identity = None
        self.instructions = instructions
        self.tool_choice = tool_choice
        self._tool_names = set(tool_names or ())
        self._tool_timeout_seconds = tool_timeout_seconds
        self._started_at = time.monotonic()
        self._event_sequence = 0
        self._input_sequence = 0
        self._response_sequence = 0
        self._input_items: dict[str, str] = {}
        self._pending_text_inputs: dict[str, str] = {}
        self._clients: set[str] = set()
        self._active_response_id: str | None = None
        self._processing_turn_id: object | None = None
        self._active_output_item_id: str | None = None
        self._active_transcript: str | None = None
        self._transcript_turn_id: str | None = None
        self._output_item_announced = False
        self._speaking = False
        self._pending_tool: dict[str, Any] | None = None
        self._closed = False
        # Both transports schedule a task per incoming packet. Keep item
        # creation and response requests in arrival order across publish awaits.
        # Transport callbacks dispatch Hermes work and return; tool execution
        # waits independently, so client tool results can still arrive here.
        self._client_message_lock = asyncio.Lock()

    @property
    def active_response_id(self) -> str | None:
        return self._active_response_id

    @property
    def processing_turn_id(self) -> object | None:
        return self._processing_turn_id

    async def client_connected(self, identity: str) -> None:
        if self._closed or not identity or identity in self._clients:
            return
        self._clients.add(identity)
        await self._emit(
            {
                "type": "session.created",
                "session": self._session_snapshot(),
            },
            recipient=identity,
        )

    def client_disconnected(self, identity: str) -> None:
        if self._selection_identity == identity and self._selection_task is not None:
            self._selection_task.cancel()
        self._clients.discard(identity)
        self._input_items.pop(identity, None)
        self._pending_text_inputs.pop(identity, None)

    async def handle_client_message(self, raw: bytes | str, identity: str) -> None:
        async with self._client_message_lock:
            await self._handle_client_message(raw, identity)

    async def _handle_client_message(self, raw: bytes | str, identity: str) -> None:
        if self._closed:
            return
        if isinstance(raw, bytes):
            if len(raw) > MAX_EVENT_BYTES:
                await self._error("event_too_large", "Client event is too large", identity, param="event")
                return
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                await self._error("invalid_event_json", "Client event must be UTF-8 JSON", identity, param="event")
                return
        else:
            text = raw
            try:
                size = len(text.encode("utf-8"))
            except UnicodeEncodeError:
                await self._error("invalid_event_json", "Client event must be UTF-8 JSON", identity, param="event")
                return
            if size > MAX_EVENT_BYTES:
                await self._error("event_too_large", "Client event is too large", identity, param="event")
                return
        try:
            event = json.loads(text)
            # JSON escapes can contain lone surrogates even in valid UTF-8
            # packets. Reject them (and NaN/Infinity) before field validation.
            json.dumps(event, ensure_ascii=False, allow_nan=False).encode("utf-8")
        except (RecursionError, TypeError, ValueError, UnicodeError):
            event = None
        if not isinstance(event, dict):
            await self._error("invalid_event_json", "Client event must be a JSON object", identity, param="event")
            return

        event_id = event.get("event_id")
        if not isinstance(event_id, str) or len(event_id.encode("utf-8")) > MAX_EVENT_ID_BYTES:
            event_id = None
        event_type = event.get("type")
        if event_type == "session.update":
            await self._accept_session_update(event, identity, event_id)
        elif event_type == "conversation.item.create":
            await self._accept_conversation_item(event, identity, event_id)
        elif event_type == "response.create":
            await self._accept_response_create(identity, event_id)
        elif event_type == "response.cancel":
            await self._accept_response_cancel(identity, event_id)
        elif event_type == HERMES_INPUT_AUDIO_STATE:
            await self._accept_input_audio_state(event, identity, event_id)
        else:
            label = event_type if isinstance(event_type, str) else "unknown"
            await self._error(
                "unsupported_client_event",
                f"Unsupported client event: {label}",
                identity,
                param="type",
                triggering_event_id=event_id,
            )

    async def _accept_input_audio_state(
        self,
        event: dict[str, Any],
        identity: str,
        event_id: str | None,
    ) -> None:
        muted = event.get("muted")
        if not isinstance(muted, bool):
            await self._error(
                "invalid_input_audio_state",
                "muted must be a boolean",
                identity,
                param="muted",
                triggering_event_id=event_id,
            )
            return
        if not await _call(self._on_input_audio_state, muted, identity):
            await self._error(
                "unsupported_client_event",
                "Input audio state is not supported by this transport",
                identity,
                param="type",
                triggering_event_id=event_id,
            )
            return
        await self._emit(
            {
                "type": HERMES_INPUT_AUDIO_STATE_UPDATED,
                "muted": muted,
            },
            recipient=identity,
        )

    def _session_snapshot(self) -> dict[str, Any]:
        return {
            "id": self.session_id,
            "type": "realtime",
            "model": self.model,
            "reasoning_effort": self.reasoning_effort,
            "instructions": self.instructions,
            "tool_choice": self.tool_choice,
            "audio": {"output": {"voice": self.voice}},
        }

    async def _accept_session_update(
        self,
        event: dict[str, Any],
        identity: str,
        event_id: str | None,
    ) -> None:
        session = event.get("session")
        if not isinstance(session, dict):
            await self._error(
                "invalid_session",
                "session must be an object",
                identity,
                param="session",
                triggering_event_id=event_id,
            )
            return
        if self._selection_task is not None:
            await self._error("session_update_busy", "Wait for the pending model selection.", identity,
                              triggering_event_id=event_id)
            return
        allowed = {"type", "instructions", "tool_choice", "model", "reasoning_effort", "tools"}
        unknown = next((key for key in session if key not in allowed), None)
        if unknown is not None:
            await self._error(
                "unsupported_session_field",
                f"Unsupported session field: {unknown}",
                identity,
                param=f"session.{unknown}",
                triggering_event_id=event_id,
            )
            return
        if session.get("type", "realtime") != "realtime":
            await self._error(
                "unsupported_session_type",
                "Only realtime sessions are supported",
                identity,
                param="session.type",
                triggering_event_id=event_id,
            )
            return
        instructions = session.get("instructions", self.instructions)
        if not isinstance(instructions, str) or len(instructions.encode("utf-8")) > MAX_INSTRUCTIONS_BYTES:
            await self._error(
                "invalid_session",
                "session.instructions is invalid",
                identity,
                param="session.instructions",
                triggering_event_id=event_id,
            )
            return
        replacement_tools = None
        if "tools" in session:
            try:
                if self._on_tools_updated is None or "model" in session or "reasoning_effort" in session:
                    raise ToolDefinitionError("Update tools separately from the model")
                replacement_tools = parse_function_tools(session["tools"], nested=False, allow_dotted_names=False)
            except ToolDefinitionError:
                await self._error("invalid_tools", "Tool updates are unavailable or invalid; change tools separately from the model.", identity, param="session.tools", triggering_event_id=event_id)
                return
        try:
            tool_choice = parse_tool_choice(
                session.get("tool_choice", self.tool_choice), {tool.name for tool in replacement_tools} if replacement_tools is not None else self._tool_names
            )
        except ToolDefinitionError:
            await self._error(
                "unsupported_tool_choice",
                "Unsupported tool choice",
                identity,
                param="session.tool_choice",
                triggering_event_id=event_id,
            )
            return
        if "model" in session or "reasoning_effort" in session:
            model = session.get("model", self.model)
            effort = session.get("reasoning_effort", None if "model" in session else self.reasoning_effort)
            try:
                validate_selection(model, effort)
                if self._on_model_selection is None:
                    raise ModelControlError("model_controls_unavailable", "Model controls are unavailable on this transport.")
            except ModelControlError as exc:
                await self._error(exc.code, str(exc), identity, triggering_event_id=event_id)
                return
            self._selection_identity = identity
            self._selection_task = asyncio.create_task(self._finish_model_selection(
                model, effort, instructions, tool_choice, identity, event_id))
            return
        if replacement_tools is not None:
            try:
                await _call(self._on_tools_updated, replacement_tools, identity)
            except Exception:
                await self._error("tool_update_failed", "The client tools could not be updated. Reconnect to try again.", identity, triggering_event_id=event_id)
                return
            self._tool_names = {tool.name for tool in replacement_tools}
            pending = self._pending_tool
            if pending is not None and pending["name"] not in self._tool_names:
                self._pending_tool = None
                if not pending["future"].done():
                    pending["future"].set_exception(RuntimeError("Client tool was disabled"))
        await self._commit_session_update(instructions, tool_choice, identity, event_id, replacement_tools)

    async def _finish_model_selection(self, model, effort, instructions, tool_choice, identity, event_id):
        try:
            selected = await self._on_model_selection(model, effort, identity)
            if self._closed or identity not in self._clients:
                return
            self.model, self.reasoning_effort = selected["model"], selected["reasoning_effort"]
            await self._commit_session_update(instructions, tool_choice, identity, event_id)
        except ModelControlError as exc:
            if not self._closed:
                await self._error(exc.code, str(exc), identity, triggering_event_id=event_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            if not self._closed:
                await self._error("model_selection_failed", "Hermes could not change the model. Try again.",
                                  identity, triggering_event_id=event_id)
        finally:
            self._selection_task = None
            self._selection_identity = None

    async def _commit_session_update(self, instructions, tool_choice, identity, event_id, tools=None):
        self.instructions = instructions
        self.tool_choice = tool_choice
        await _call(self._on_tool_choice_updated, tool_choice)
        await self._emit(
            {"type": "session.updated", "session": {**self._session_snapshot(), **({"tools": [
                {"type": "function", "name": tool.name, "description": tool.description, "parameters": tool.parameters}
                for tool in tools]} if tools is not None else {})},
             **({"client_event_id": event_id} if event_id else {})},
            recipient=identity,
        )

    async def speech_started(self, identity: str) -> None:
        item_id = self._new_input_item(identity)
        await self._emit(
            {
                "type": "input_audio_buffer.speech_started",
                "audio_start_ms": self._elapsed_ms(),
                "item_id": item_id,
            }
        )

    async def speech_stopped(self, identity: str) -> str:
        item_id = self._input_items.get(identity) or self._new_input_item(identity)
        await self._emit(
            {
                "type": "input_audio_buffer.speech_stopped",
                "audio_end_ms": self._elapsed_ms(),
                "item_id": item_id,
            }
        )
        return item_id

    async def input_audio_overflow(self, identity: str) -> None:
        """Retire an unsubmitted input without manufacturing a partial command."""
        if identity in self._input_items:
            await self.speech_stopped(identity)
        self._input_items.pop(identity, None)
        await self._error("input_audio_too_long",
                          "Speech exceeded two minutes. Please pause and try again.", identity)

    async def user_transcript(
        self, transcript: str, identity: str, *, item_id: str | None = None
    ) -> None:
        if item_id is None:
            item_id = self._input_items.pop(identity, None) or self._new_item_id("input")
        elif self._input_items.get(identity) == item_id:
            self._input_items.pop(identity, None)
        item = {
            "id": item_id,
            "type": "message",
            "status": "completed",
            "role": "user",
            "content": [{"type": "input_audio", "transcript": transcript}],
        }
        await self._emit(
            {
                "type": "conversation.item.added",
                "previous_item_id": None,
                "item": item,
            }
        )
        await self._emit(
            {
                "type": "conversation.item.input_audio_transcription.completed",
                "item_id": item_id,
                "content_index": 0,
                "transcript": transcript,
                "usage": {
                    "type": "tokens",
                    "total_tokens": 0,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "input_token_details": {"audio_tokens": 0, "text_tokens": 0},
                },
            }
        )
        await self._emit(
            {
                "type": "conversation.item.done",
                "previous_item_id": None,
                "item": item,
            }
        )

    async def response_started(self) -> None:
        if self._closed or self._active_response_id:
            return
        self._response_sequence += 1
        self._active_response_id = f"resp_{self.session_id}_{self._response_sequence}"
        self._active_output_item_id = None
        self._active_transcript = None
        self._output_item_announced = False
        await self._emit(
            {
                "type": "response.created",
                "response": {"id": self._active_response_id, "status": "in_progress", "output": []},
            }
        )

    async def output_started(self) -> None:
        await self.response_started()
        self._speaking = True
        if self._active_response_id:
            self._active_output_item_id = self._active_output_item_id or f"item_{self._active_response_id}_audio"
            await self._announce_audio_output_item()
        await self._emit({"type": "output_audio_buffer.started", "response_id": self._active_response_id})

    async def processing_started(self) -> None:
        await self.response_started()
        if self._active_response_id:
            self._processing_turn_id = object()

    async def text_delivery_complete(self) -> None:
        # Notices and interim messages use the same adapter.send() as final
        # text. Only the base processing hook knows when that turn is finished.
        # Standalone sends (for example an idle slash command) still complete.
        if self._processing_turn_id is None:
            await self.output_stopped()

    async def stream_transcript(self, transcript: str, *, turn_id: str, finalize: bool = False) -> None:
        """Bind the consumer's seed to this response; drop late/replaced frames.

        Text completion does not stop audio or complete the response. The TTS
        sink and processing lifecycle still own those transitions.
        """
        if self._closed or not self._active_response_id or not turn_id:
            return
        if not transcript and not finalize:
            self._transcript_turn_id = self._transcript_turn_id or turn_id
            return
        if turn_id != self._transcript_turn_id:
            return
        if finalize:
            await self.assistant_transcript(transcript)
            return
        previous = self._active_transcript or ""
        if transcript == previous:
            return
        self._active_output_item_id = self._active_output_item_id or f"item_{self._active_response_id}_audio"
        self._active_transcript = transcript
        response_id, item_id = self._active_response_id, self._active_output_item_id
        await self._announce_audio_output_item()
        if self._active_response_id != response_id or self._transcript_turn_id != turn_id:
            return
        event = {
            "type": "response.output_audio_transcript.delta",
            "response_id": response_id,
            "item_id": item_id,
            "output_index": 0,
            "content_index": 0,
            "delta": transcript[len(previous):] if transcript.startswith(previous) else "",
        }
        if not transcript.startswith(previous):
            # Native frames are snapshots: tool boundaries or display cleanup
            # can revise a draft. This extension lets clients replace it safely.
            event["transcript"] = transcript
        await self._emit(event)

    async def assistant_transcript(self, transcript: str) -> None:
        await self.response_started()
        if not self._active_response_id:
            return
        self._active_output_item_id = self._active_output_item_id or f"item_{self._active_response_id}_audio"
        self._active_transcript = transcript
        response_id, item_id = self._active_response_id, self._active_output_item_id
        await self._announce_audio_output_item()
        if self._active_response_id != response_id:
            return
        await self._emit(
            {
                "type": "response.output_audio_transcript.done",
                "response_id": response_id,
                "item_id": item_id,
                "output_index": 0,
                "content_index": 0,
                "transcript": transcript,
            }
        )

    async def output_stopped(self) -> None:
        response_id = self._active_response_id
        await self._finish_audio_output_item("completed")
        await self._complete_response("completed")
        await self.output_playback_stopped(response_id)

    async def output_playback_stopped(self, response_id: str | None = None) -> None:
        """Finish playout while keeping the response open for Hermes's text.

        Hermes sends its transcript after play_tts returns. Completing the
        response here would turn that transcript into a second response.
        """
        if self._speaking:
            self._speaking = False
            await self._emit({"type": "output_audio_buffer.stopped", "response_id": response_id or self._active_response_id})

    async def response_failed(self) -> None:
        await self._finish_audio_output_item("incomplete")
        await self.output_playback_stopped()
        await self._complete_response("failed")

    async def request_client_tool(self, name: str, arguments: dict[str, Any]) -> str:
        """Emit one OpenAI function call and await its client-owned result."""
        try:
            return await self._request_client_tool(name, arguments)
        finally:
            pending = self._pending_tool
            if pending is not None and pending["task"] is asyncio.current_task():
                self._pending_tool = None
                if not pending["future"].done():
                    pending["future"].cancel()

    async def _request_client_tool(self, name: str, arguments: dict[str, Any]) -> str:
        if self._closed:
            raise RuntimeError("Realtime session is closed")
        if self._pending_tool is not None:
            raise RuntimeError("A client tool call is already pending")
        processing_turn = self._processing_turn_id
        try:
            encoded_arguments = json.dumps(
                arguments, ensure_ascii=False, separators=(",", ":")
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Tool arguments are not valid JSON") from exc
        if len(encoded_arguments.encode("utf-8")) > MAX_TOOL_ARGUMENT_BYTES:
            raise RuntimeError("Tool arguments are too large")

        await self.response_started()
        response_id = self._active_response_id
        if not response_id:
            raise RuntimeError("Could not start a function-call response")
        wire_call_id = f"call_{uuid.uuid4().hex}"
        item_id = f"item_{wire_call_id}"
        future: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        pending = {
            "call_id": wire_call_id,
            "name": name,
            "future": future,
            "output": None,
            "task": asyncio.current_task(),
        }
        self._pending_tool = pending
        item = {
            "id": item_id,
            "type": "function_call",
            "status": "in_progress",
            "call_id": wire_call_id,
            "name": name,
            "arguments": "",
        }
        await self._emit({
            "type": "response.output_item.added",
            "response_id": response_id,
            "output_index": 0,
            "item": item,
        })
        await self._emit({"type": "conversation.item.added", "previous_item_id": None, "item": item})
        await self._emit({
            "type": "response.function_call_arguments.done",
            "response_id": response_id,
            "item_id": item_id,
            "output_index": 0,
            "call_id": wire_call_id,
            "name": name,
            "arguments": encoded_arguments,
        })
        done_item = {**item, "status": "completed", "arguments": encoded_arguments}
        await self._emit({"type": "conversation.item.done", "previous_item_id": None, "item": done_item})
        await self._emit({
            "type": "response.output_item.done",
            "response_id": response_id,
            "output_index": 0,
            "item": done_item,
        })
        await self._complete_response(
            "completed",
            continue_processing=True,
            explicit_output=[{
                "id": item_id,
                "type": "function_call",
                "call_id": wire_call_id,
                "name": name,
                "arguments": encoded_arguments,
            }],
        )
        try:
            return await asyncio.wait_for(
                asyncio.shield(future), timeout=self._tool_timeout_seconds
            )
        except asyncio.CancelledError:
            if self._pending_tool is pending:
                self._pending_tool = None
            if not future.done():
                future.cancel()
            raise
        except asyncio.TimeoutError:
            if self._pending_tool is pending:
                self._pending_tool = None
            if not future.done():
                future.cancel()
            # A timeout resumes Hermes without a client response.create. Open
            # its continuation before native text can arrive ahead of TTS PCM,
            # but never reopen a cancelled or replaced processing turn.
            if processing_turn is not None and self._processing_turn_id is processing_turn:
                await self.response_started()
            await self._error(
                "tool_timeout",
                "Timed out while waiting for client tool result",
                next(iter(self._clients), ""),
                param="item.output",
            )
            raise RuntimeError("Client tool result timeout") from None

    async def close(self) -> None:
        self._closed = True
        task, self._selection_task = self._selection_task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._cancel_pending_tool()
        self._clients.clear()
        self._input_items.clear()
        self._pending_text_inputs.clear()
        self._active_response_id = None
        self._processing_turn_id = None
        self._transcript_turn_id = None
        self._active_output_item_id = None
        self._active_transcript = None
        self._output_item_announced = False
        self._speaking = False

    def _cancel_pending_tool(self) -> None:
        pending, self._pending_tool = self._pending_tool, None
        if pending is not None:
            future = pending["future"]
            if not future.done():
                future.cancel()

    async def _accept_conversation_item(
        self,
        event: dict[str, Any],
        identity: str,
        event_id: str | None,
    ) -> None:
        item = event.get("item")
        if isinstance(item, dict) and item.get("type") == "function_call_output":
            await self._accept_tool_output(item, identity, event_id)
            return
        if not isinstance(item, dict) or item.get("type") != "message" or item.get("role") != "user":
            await self._error(
                "unsupported_conversation_item",
                "Only user messages and function_call_output are supported",
                identity,
                param="item",
                triggering_event_id=event_id,
            )
            return
        content = item.get("content")
        if not isinstance(content, list) or len(content) != 1 or not isinstance(content[0], dict):
            await self._error("invalid_conversation_item", "item.content must contain one input", identity, param="item.content", triggering_event_id=event_id)
            return
        part = content[0]
        text = part.get("text") if part.get("type") == "input_text" else None
        if not isinstance(text, str) or not text.strip() or len(text.encode("utf-8")) > MAX_TEXT_BYTES:
            await self._error("invalid_conversation_item", "input_text is invalid", identity, param="item.content", triggering_event_id=event_id)
            return
        if identity in self._pending_text_inputs:
            await self._error(
                "conversation_item_pending",
                "Create a response for the pending input before adding another",
                identity,
                param="item",
                triggering_event_id=event_id,
            )
            return
        item_id = item.get("id")
        if not isinstance(item_id, str) or not item_id or len(item_id.encode("utf-8")) > MAX_EVENT_ID_BYTES:
            item_id = self._new_item_id("input")
        normalized = {
            "id": item_id,
            "type": "message",
            "status": "completed",
            "role": "user",
            "content": [{"type": "input_text", "text": text.strip()}],
        }
        await self._emit_conversation_item(normalized, recipient=identity)
        self._pending_text_inputs[identity] = text.strip()

    async def _accept_response_create(self, identity: str, event_id: str | None) -> None:
        if self._active_response_id:
            await self._error("conversation_already_has_active_response", "The conversation already has an active response", identity, param="response", triggering_event_id=event_id)
            return
        pending_tool = self._pending_tool
        if pending_tool is not None:
            if pending_tool["output"] is None:
                await self._error("tool_outputs_pending", "The tool output is required before response.create", identity, param="response", triggering_event_id=event_id)
                return
            await self.response_started()
            self._pending_tool = None
            future = pending_tool["future"]
            if not future.done():
                future.set_result(pending_tool["output"])
            return
        text = self._pending_text_inputs.pop(identity, None)
        if text is not None:
            if not await _call(self._on_text_input, text, identity):
                await self._error("text_input_unavailable", "Text input is unavailable", identity, triggering_event_id=event_id)
            return
        if not await _call(self._on_response_requested, identity):
            await self._error("response_create_unsupported", "response.create is unavailable", identity, param="response", triggering_event_id=event_id)

    async def _accept_tool_output(
        self,
        item: dict[str, Any],
        identity: str,
        event_id: str | None,
    ) -> None:
        call_id = item.get("call_id")
        output = item.get("output")
        if (
            not isinstance(call_id, str)
            or not call_id
            or len(call_id.encode("utf-8")) > MAX_EVENT_ID_BYTES
            or not isinstance(output, str)
            or len(output.encode("utf-8")) > MAX_TOOL_OUTPUT_BYTES
        ):
            await self._error("invalid_tool_output", "call_id or output is invalid", identity, param="item", triggering_event_id=event_id)
            return
        pending = self._pending_tool
        if pending is None or pending["call_id"] != call_id:
            await self._error("unknown_tool_call", "Tool call is unknown or no longer pending", identity, param="item.call_id", triggering_event_id=event_id)
            return
        if pending["output"] is not None:
            await self._error("duplicate_tool_output", "Tool output was already received", identity, param="item.call_id", triggering_event_id=event_id)
            return
        pending["output"] = output
        await self._emit_conversation_item(
            {
                "id": f"item_{call_id}_output",
                "type": "function_call_output",
                "call_id": call_id,
                "output": output,
            },
            recipient=identity,
        )

    async def _accept_response_cancel(self, identity: str, event_id: str | None) -> None:
        if not self._active_response_id and self._processing_turn_id is None and self._pending_tool is None:
            await self._error("no_active_response", "There is no active response to cancel", identity, param="response", triggering_event_id=event_id)
            return
        # Function-call responses are sealed while Hermes awaits the client.
        # Give cancellation its own terminal continuation, not a second seal
        # for the completed function response. The transport stops core work.
        processing_turn, pending_tool = self._processing_turn_id, self._pending_tool
        prior_response_id = self._active_response_id
        await self.response_started()
        if (self._processing_turn_id is not processing_turn
                or prior_response_id is not None and self._active_response_id != prior_response_id):
            return
        response_id = self._active_response_id
        await _call(self._on_response_cancelled, identity)
        if self._pending_tool is pending_tool:
            self._cancel_pending_tool()
        # Transport cleanup may yield while speech starts a replacement turn.
        if (self._active_response_id == response_id
                or processing_turn is not None and self._processing_turn_id is processing_turn):
            await self.response_cancelled()

    async def response_cancelled(self) -> None:
        response_id = self._active_response_id
        await self._finish_audio_output_item("incomplete")
        await self._complete_response("cancelled")
        if self._speaking:
            self._speaking = False
            await self._emit({"type": "output_audio_buffer.cleared", "response_id": response_id})

    async def _complete_response(
        self,
        status: str,
        *,
        explicit_output: list[dict[str, Any]] | None = None,
        continue_processing: bool = False,
    ) -> None:
        response_id = self._active_response_id
        # A client function call seals one wire response, not the Hermes turn.
        # Its result resumes the same text consumer and processing-complete hook.
        # Clear failed/cancelled ownership even while waiting between responses.
        if not continue_processing:
            self._processing_turn_id = None
            self._transcript_turn_id = None
        if not response_id:
            return
        output: list[dict[str, Any]] = list(explicit_output or [])
        if explicit_output is None and status == "completed" and self._active_transcript:
            output.append(
                {
                    "id": self._active_output_item_id or f"item_{response_id}_audio",
                    "type": "message",
                    "status": "completed",
                    "role": "assistant",
                    "content": [{"type": "output_audio", "transcript": self._active_transcript}],
                }
            )
        self._active_response_id = None
        self._active_output_item_id = None
        self._active_transcript = None
        self._output_item_announced = False
        await self._emit({"type": "response.done", "response": {"id": response_id, "status": status, "output": output}})

    async def _emit_conversation_item(
        self,
        item: dict[str, Any],
        *,
        recipient: str | None = None,
    ) -> None:
        await self._emit(
            {"type": "conversation.item.added", "previous_item_id": None, "item": item},
            recipient=recipient,
        )
        await self._emit(
            {"type": "conversation.item.done", "previous_item_id": None, "item": item},
            recipient=recipient,
        )

    def _audio_output_item(self, status: str) -> dict[str, Any] | None:
        if not self._active_response_id or not self._active_output_item_id:
            return None
        return {
            "id": self._active_output_item_id,
            "type": "message",
            "status": status,
            "role": "assistant",
            "content": [{"type": "output_audio", "transcript": self._active_transcript or ""}],
        }

    async def _announce_audio_output_item(self) -> None:
        if self._output_item_announced or not self._active_response_id:
            return
        item = self._audio_output_item("in_progress")
        if item is None:
            return
        response_id, item_id = self._active_response_id, self._active_output_item_id
        self._output_item_announced = True
        await self._emit({
            "type": "response.output_item.added",
            "response_id": response_id,
            "output_index": 0,
            "item": item,
        })
        if self._active_response_id != response_id:
            return
        await self._emit({"type": "conversation.item.added", "previous_item_id": None, "item": item})
        if self._active_response_id != response_id:
            return
        await self._emit({
            "type": "response.content_part.added",
            "response_id": response_id,
            "item_id": item_id,
            "output_index": 0,
            "content_index": 0,
            "part": {"type": "output_audio", "transcript": ""},
        })

    async def _finish_audio_output_item(self, status: str) -> None:
        if not self._output_item_announced or not self._active_response_id or not self._active_output_item_id:
            return
        item = self._audio_output_item(status)
        if item is None:
            return
        await self._emit({
            "type": "response.content_part.done",
            "response_id": self._active_response_id,
            "item_id": self._active_output_item_id,
            "output_index": 0,
            "content_index": 0,
            "part": {"type": "output_audio", "transcript": self._active_transcript or ""},
        })
        await self._emit({"type": "conversation.item.done", "previous_item_id": None, "item": item})
        await self._emit({
            "type": "response.output_item.done",
            "response_id": self._active_response_id,
            "output_index": 0,
            "item": item,
        })
        self._output_item_announced = False

    async def _error(
        self,
        code: str,
        message: str,
        recipient: str,
        *,
        param: str | None = None,
        triggering_event_id: str | None = None,
    ) -> None:
        await self._emit(
            {
                "type": "error",
                "error": {
                    "type": "invalid_request_error",
                    "code": code,
                    "message": message[:500],
                    "param": param,
                    "event_id": triggering_event_id,
                },
            },
            recipient=recipient,
        )

    async def _emit(self, event: dict[str, Any], recipient: str | None = None) -> None:
        if self._closed:
            return
        self._event_sequence += 1
        payload = {
            "event_id": f"evt_{self.session_id}_{self._event_sequence}_{uuid.uuid4().hex[:8]}",
            **event,
        }
        encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        if len(encoded) > MAX_EVENT_BYTES:
            self._closed = True
            raise RuntimeError("Realtime event exceeded the data message limit")
        await self._publish(payload, recipient)

    def _elapsed_ms(self) -> int:
        return max(0, int((time.monotonic() - self._started_at) * 1000))

    def _new_item_id(self, label: str) -> str:
        self._input_sequence += 1
        return f"item_{self.session_id}_{label}_{self._input_sequence}"

    def _new_input_item(self, identity: str) -> str:
        item_id = self._new_item_id("input")
        self._input_items[identity] = item_id
        return item_id
