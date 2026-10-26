"""Call-owned native exec approval cards; Hermes retains execution/persistence."""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from gateway.platforms.base import SendResult

_BINDINGS = ("request_id", "backend_session_id", "turn_id", "tool_call_id")
_SCOPES = {"once": "once", "session": "session", "always": "always", "deny": "once"}


@dataclass
class ApprovalOrigin:
    identity: str
    session_id: str
    turn_id: str
    current: Callable[[], bool]
    publish: Callable[..., Any]


class VoiceApprovalBridge:
    def __init__(self, *, native=None, clock=time.time, timeout=None):
        self.native = native
        self.clock = clock
        self.timeout = timeout
        self.subscribers: set[tuple[str, str]] = set()
        self.origins: dict[str, ApprovalOrigin] = {}
        self.pending: dict[str, dict] = {}

    def _native(self):
        if self.native is None:
            from tools import approval
            self.native = approval
        return self.native

    def subscribe(self, identity, session_id):
        self.subscribers.add((session_id, identity))

    def unsubscribe(self, identity, session_id):
        self.subscribers.discard((session_id, identity))
        for key, origin in list(self.origins.items()):
            if origin.identity == identity and origin.session_id == session_id:
                self.origins.pop(key, None)
        for request_id, entry in list(self.pending.items()):
            if entry["origin"].identity == identity and entry["origin"].session_id == session_id:
                self._native().withdraw_gateway_approval(entry["session_key"], entry["native_id"], "voice client disconnected")
                self._settle(request_id, "cancelled")

    def remember(self, session_key, origin):
        self.origins[session_key] = origin

    def forget(self, session_key, turn_id):
        origin = self.origins.get(session_key)
        if origin and origin.turn_id == turn_id:
            self.origins.pop(session_key, None)
        for request_id, entry in list(self.pending.items()):
            if entry["session_key"] == session_key and entry["origin"].turn_id == turn_id:
                self._native().withdraw_gateway_approval(session_key, entry["native_id"], "voice turn ended")
                self._settle(request_id, "cancelled")

    async def request(self, prompt):
        origin = self.origins.get(prompt.session_key)
        if (origin is None or origin.session_id != prompt.chat_id or not origin.current()
                or (origin.session_id, origin.identity) not in self.subscribers or len(self.pending) >= 8):
            return SendResult(success=False, error="Voice approval client unavailable")
        native = self._native()
        from gateway.run import _redact_approval_command
        claimed = {(entry["session_key"], entry["native_id"]) for entry in self.pending.values()}
        candidates = [value for value in native.list_gateway_approvals(prompt.session_key)
                      if (prompt.session_key, value.get("request_id")) not in claimed
                      and _redact_approval_command(value.get("command", "")) == prompt.command]
        if not candidates:
            return SendResult(success=False, error="Native approval is no longer pending")
        native_id = candidates[0].get("request_id")
        if not isinstance(native_id, str) or re.fullmatch(r"[A-Za-z0-9_:-]{1,128}", native_id) is None:
            return SendResult(success=False, error="Native approval has no request ID")
        choices = [{"id": choice, "label": label, "kind": "deny" if choice == "deny" else "allow", "scope": _SCOPES[choice]}
                   for label, choice, _style in prompt.actions if choice in _SCOPES]
        if {value["id"] for value in choices}.issuperset({"once", "deny"}) is False or len(choices) != len(prompt.actions):
            return SendResult(success=False, error="Unsupported native approval choices")
        if self.timeout is None:
            from gateway.platforms.base_exec_approval import approval_timeout_seconds
            seconds = approval_timeout_seconds()
        else:
            seconds = self.timeout
        expires = self.clock() + max(0, seconds)
        request_id = uuid.uuid4().hex
        request = {"request_id": request_id, "backend_session_id": "hermes_" + hashlib.sha256(prompt.session_key.encode()).hexdigest(),
                   "turn_id": origin.turn_id, "tool_call_id": native_id, "title": "Command approval", "description": prompt.description.encode()[:1024].decode(errors="ignore"),
                   "details": prompt.command, "choices": choices,
                   "expires_at": datetime.fromtimestamp(expires, timezone.utc).isoformat().replace("+00:00", "Z")}
        if len(prompt.command.encode()) > 8192 or len(json.dumps(request).encode()) > 12 * 1024 or seconds <= 0:
            native.withdraw_gateway_approval(prompt.session_key, native_id, "voice approval cannot be displayed")
            return SendResult(success=True, message_id=request_id)
        entry = {"request": request, "origin": origin, "session_key": prompt.session_key, "native_id": native_id, "expires": expires, "task": None}
        self.pending[request_id] = entry
        try:
            delivered = await origin.publish({"type": "agent.approval.requested", "session_id": origin.session_id, "approval": request})
        except Exception:
            delivered = False
        if not delivered:
            native.withdraw_gateway_approval(prompt.session_key, native_id, "voice approval delivery failed")
            self._settle(request_id, "cancelled")
            return SendResult(success=False, error="Voice approval delivery failed")
        native.ack_gateway_approval(prompt.session_key, native_id)
        entry["task"] = asyncio.create_task(self._watch(request_id))
        return SendResult(success=True, message_id=request_id)

    def _settle(self, request_id, status, choice=None, client_event_id=None):
        entry = self.pending.pop(request_id, None)
        if entry is None:
            return None
        task = entry["task"]
        if task and task is not asyncio.current_task():
            task.cancel()
        resolution = {key: entry["request"][key] for key in _BINDINGS}
        resolution["status"] = status
        if choice is not None:
            resolution["choice"] = choice
        if client_event_id is not None:
            resolution["client_event_id"] = client_event_id
        async def publish():
            try:
                await entry["origin"].publish({"type": "agent.approval.resolved", "session_id": entry["origin"].session_id, "resolution": resolution})
            except Exception:
                pass
        asyncio.create_task(publish())
        return resolution

    async def _watch(self, request_id):
        try:
            while entry := self.pending.get(request_id):
                if not entry["origin"].current() or (entry["origin"].session_id, entry["origin"].identity) not in self.subscribers:
                    self._native().withdraw_gateway_approval(entry["session_key"], entry["native_id"], "voice owner or turn replaced")
                    self._settle(request_id, "cancelled")
                    return
                if self.clock() >= entry["expires"]:
                    self._native().withdraw_gateway_approval(entry["session_key"], entry["native_id"], "voice approval expired")
                    self._settle(request_id, "expired")
                    return
                if not any(value.get("request_id") == entry["native_id"] for value in self._native().list_gateway_approvals(entry["session_key"])):
                    # Another native surface may have answered. Do not invent its outcome.
                    self._settle(request_id, "closed")
                    return
                await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            pass
        except Exception:
            entry = self.pending.get(request_id)
            if entry:
                self._native().withdraw_gateway_approval(entry["session_key"], entry["native_id"], "voice approval unavailable")
                self._settle(request_id, "cancelled")

    async def respond(self, event, identity):
        decision = event.get("decision")
        if not isinstance(decision, dict):
            raise ValueError("Invalid approval decision")
        entry = self.pending.get(decision.get("request_id"))
        if (entry is None or identity != entry["origin"].identity or (entry["origin"].session_id, identity) not in self.subscribers
                or event.get("session_id") != entry["origin"].session_id or not entry["origin"].current()
                or any(decision.get(key) != entry["request"][key] for key in _BINDINGS)
                or decision.get("choice") not in {value["id"] for value in entry["request"]["choices"]}):
            raise ValueError("Approval is unavailable or choice not offered")
        request_id = decision["request_id"]
        if self.clock() >= entry["expires"]:
            self._native().withdraw_gateway_approval(entry["session_key"], entry["native_id"], "voice approval expired")
            self._settle(request_id, "expired")
            raise ValueError("Approval expired")
        count = self._native().resolve_gateway_approval(entry["session_key"], decision["choice"], request_id=entry["native_id"])
        if count != 1:
            self._settle(request_id, "closed")
            raise ValueError("Approval is no longer pending")
        return self._settle(request_id, "denied" if decision["choice"] == "deny" else "approved", decision["choice"], event.get("event_id"))


def approval_bridge(adapter):
    bridge = adapter.__dict__.get("_voice_approval_bridge")
    if bridge is None:
        bridge = adapter._voice_approval_bridge = VoiceApprovalBridge()
    return bridge


def remember_approval_origin(adapter, event, origin):
    runner = getattr(adapter, "gateway_runner", None)
    route = getattr(runner, "_session_key_for_source", None)
    if callable(route):
        try:
            key = route(event.source)
        except Exception:
            # Optional card routing cannot break ordinary voice processing.
            return
        if isinstance(key, str) and key:
            approval_bridge(adapter).remember(key, origin)
            event._voice_approval_origin = (key, origin.turn_id)


def forget_approval_origin(adapter, event):
    origin = getattr(event, "_voice_approval_origin", None)
    if isinstance(origin, tuple) and len(origin) == 2:
        approval_bridge(adapter).forget(*origin)
