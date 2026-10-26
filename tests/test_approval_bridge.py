import asyncio
from types import SimpleNamespace

import pytest

from hermes_realtime.approval_bridge import ApprovalOrigin, VoiceApprovalBridge


class NativeApprovals:
    def __init__(self):
        self.queue = [{"request_id": "native-request", "command": "echo fixture"}]
        self.answers = []
        self.withdrawn = []

    def list_gateway_approvals(self, key):
        return list(self.queue)

    def resolve_gateway_approval(self, key, choice, *, request_id):
        if not any(value["request_id"] == request_id for value in self.queue):
            return 0
        self.answers.append((key, choice, request_id))
        self.queue = []
        return 1

    def withdraw_gateway_approval(self, key, request_id, cause):
        self.withdrawn.append((key, request_id, cause))
        self.queue = []
        return True

    def ack_gateway_approval(self, key, request_id):
        return True


async def fixture(*, permanent=False):
    events, native = [], NativeApprovals()
    async def publish(event):
        events.append(event)
        return True
    bridge = VoiceApprovalBridge(native=native, timeout=30)
    bridge.subscribe("owner", "call")
    bridge.remember("native-session", ApprovalOrigin("owner", "call", "turn", lambda: True, publish))
    choices = [("Allow once", "once", "primary"), ("Deny", "deny", "danger")]
    if permanent:
        choices[1:1] = [("Allow for session", "session", ""), ("Allow always", "always", "")]
    prompt = SimpleNamespace(session_key="native-session", chat_id="call", command="echo fixture", description="Review command", actions=choices)
    assert (await bridge.request(prompt)).success
    request = events[0]["approval"]
    decision = {key: request[key] for key in ("request_id", "backend_session_id", "turn_id", "tool_call_id")}
    return bridge, native, events, request, {"type": "agent.approval.response", "session_id": "call", "event_id": "answer", "decision": {**decision, "choice": "once"}}


@pytest.mark.asyncio
async def test_native_choices_are_offered_conditionally_and_persistence_stays_native():
    bridge, native, events, request, response = await fixture(permanent=True)
    assert [choice["id"] for choice in request["choices"]] == ["once", "session", "always", "deny"]
    response["decision"]["choice"] = "always"
    resolution = await bridge.respond(response, "owner")
    assert resolution["status"] == "approved"
    assert native.answers == [("native-session", "always", "native-request")]
    with pytest.raises(ValueError):
        await bridge.respond(response, "owner")
    await asyncio.sleep(0)
    assert events[-1]["resolution"]["client_event_id"] == "answer"


@pytest.mark.asyncio
async def test_cross_owner_turn_and_unoffered_decisions_cannot_resolve_native_approval():
    bridge, native, events, request, response = await fixture()
    with pytest.raises(ValueError):
        await bridge.respond(response, "intruder")
    with pytest.raises(ValueError):
        await bridge.respond({**response, "decision": {**response["decision"], "turn_id": "other"}}, "owner")
    with pytest.raises(ValueError):
        await bridge.respond({**response, "decision": {**response["decision"], "choice": "always"}}, "owner")
    assert native.answers == []
    bridge.unsubscribe("owner", "call")
    await asyncio.sleep(0)
    assert native.withdrawn
    assert bridge.pending == {}
    assert events[-1]["resolution"]["status"] == "cancelled"


@pytest.mark.asyncio
async def test_expired_decision_is_withdrawn_and_never_granted():
    bridge, native, events, request, response = await fixture()
    bridge.clock = lambda: 10**12
    with pytest.raises(ValueError, match="expired"):
        await bridge.respond(response, "owner")
    await asyncio.sleep(0)
    assert native.answers == []
    assert events[-1]["resolution"]["status"] == "expired"
